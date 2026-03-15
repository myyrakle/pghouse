use crate::core::interface::{GranuleRef, ScanBatch, ScanPlan, ScanRequest};
use anyhow::{Result, bail};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct MaterializedRow {
    pub(crate) pk_text: Option<String>,
    pub(crate) row: Value,
}

pub(crate) fn build_scan_plan(
    request: &ScanRequest,
    available_granules: &[GranuleRef],
) -> Result<ScanPlan> {
    let mut granules = Vec::new();
    let mut row_estimate = 0usize;

    for granule in available_granules {
        if let Some(snapshot_generation) = request.snapshot_generation
            && granule.generation > snapshot_generation
        {
            continue;
        }

        if !granule_overlaps(
            granule,
            request.pk_min.as_deref(),
            request.pk_max.as_deref(),
        ) {
            continue;
        }

        granules.push(granule.clone());

        let remaining = request
            .limit
            .map(|limit| limit.saturating_sub(row_estimate));
        let granule_rows = usize::try_from(granule.row_count.max(0)).unwrap_or_default();
        row_estimate += remaining.map_or(granule_rows, |limit| limit.min(granule_rows));

        if let Some(limit) = request.limit
            && row_estimate >= limit
        {
            break;
        }
    }

    Ok(ScanPlan {
        table: request.table.clone(),
        projection: request.projection.clone(),
        granules,
        pk_min: request.pk_min.clone(),
        pk_max: request.pk_max.clone(),
        limit: request.limit,
        row_estimate,
    })
}

pub(crate) fn materialize_row_entries(
    plan: &ScanPlan,
    batches: &[ScanBatch],
) -> Result<Vec<MaterializedRow>> {
    let mut rows = Vec::new();
    let projected_columns = plan
        .projection
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<BTreeMap<_, _>>();

    for batch in batches {
        let row_count = batch_row_count(batch)?;
        for row_index in 0..row_count {
            let pk_value = load_pk_value(plan, batch, row_index)?;
            if !pk_matches_bounds(
                pk_value.as_deref(),
                plan.pk_min.as_deref(),
                plan.pk_max.as_deref(),
            ) {
                continue;
            }

            let mut row = Map::new();
            for (column_name, column_descriptor) in &projected_columns {
                let column = batch
                    .columns
                    .iter()
                    .find(|candidate| candidate.column.name == *column_name)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "scan batch for generation {} is missing projected column {}",
                            batch.granule.generation,
                            column_name
                        )
                    })?;
                let value = column.values.get(row_index).ok_or_else(|| {
                    anyhow::anyhow!(
                        "scan batch for generation {} is missing row {} in column {}",
                        batch.granule.generation,
                        row_index,
                        column_descriptor.name
                    )
                })?;
                row.insert(column_descriptor.name.clone(), value.clone());
            }

            rows.push(MaterializedRow {
                pk_text: pk_value,
                row: Value::Object(row),
            });
            if let Some(limit) = plan.limit
                && rows.len() >= limit
            {
                return Ok(rows);
            }
        }
    }

    Ok(rows)
}

fn granule_overlaps(granule: &GranuleRef, pk_min: Option<&str>, pk_max: Option<&str>) -> bool {
    if let Some(pk_min) = pk_min
        && granule
            .pk_max
            .as_deref()
            .is_some_and(|value| value < pk_min)
    {
        return false;
    }

    if let Some(pk_max) = pk_max
        && granule
            .pk_min
            .as_deref()
            .is_some_and(|value| value > pk_max)
    {
        return false;
    }

    true
}

fn batch_row_count(batch: &ScanBatch) -> Result<usize> {
    let mut row_count = None;
    for column in &batch.columns {
        let column_row_count = column.values.len();
        match row_count {
            Some(existing) if existing != column_row_count => {
                bail!(
                    "scan batch for generation {} has inconsistent column lengths",
                    batch.granule.generation
                );
            }
            Some(_) => {}
            None => row_count = Some(column_row_count),
        }
    }

    Ok(row_count.unwrap_or_else(|| usize::try_from(batch.granule.row_count.max(0)).unwrap_or(0)))
}

fn load_pk_value(plan: &ScanPlan, batch: &ScanBatch, row_index: usize) -> Result<Option<String>> {
    let pk_column = batch
        .columns
        .iter()
        .find(|column| column.column.name == plan.table.pk_column)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "scan batch for generation {} is missing PK column {}",
                batch.granule.generation,
                plan.table.pk_column
            )
        })?;

    let value = pk_column.values.get(row_index).ok_or_else(|| {
        anyhow::anyhow!(
            "scan batch for generation {} is missing PK row {}",
            batch.granule.generation,
            row_index
        )
    })?;

    Ok(value_to_pk_text(value))
}

fn pk_matches_bounds(value: Option<&str>, pk_min: Option<&str>, pk_max: Option<&str>) -> bool {
    let Some(value) = value else {
        return pk_min.is_none() && pk_max.is_none();
    };

    if pk_min.is_some_and(|bound| value < bound) {
        return false;
    }

    if pk_max.is_some_and(|bound| value > bound) {
        return false;
    }

    true
}

fn value_to_pk_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_scan_plan, materialize_row_entries};
    use crate::core::interface::{
        ColumnDescriptor, ColumnVector, GranuleRef, ScanBatch, ScanRequest, TableDescriptor,
    };
    use serde_json::json;

    fn test_table() -> TableDescriptor {
        TableDescriptor {
            table_oid: 1,
            schema_name: "public".to_string(),
            table_name: "events".to_string(),
            pk_column: "id".to_string(),
            granule_rows: 2,
            compression: "zstd".to_string(),
            storage_root: "/tmp/pghouse".to_string(),
        }
    }

    #[test]
    fn scan_plan_prunes_granules_by_pk_bounds() {
        let request = ScanRequest {
            table: test_table(),
            projection: vec![ColumnDescriptor {
                ordinal: 1,
                name: "id".to_string(),
            }],
            pk_min: Some("15".to_string()),
            pk_max: Some("25".to_string()),
            limit: None,
            snapshot_generation: None,
        };
        let plan = build_scan_plan(
            &request,
            &[
                GranuleRef {
                    table_oid: 1,
                    generation: 1,
                    row_count: 10,
                    pk_min: Some("01".to_string()),
                    pk_max: Some("10".to_string()),
                    manifest_path: "g00000000000000000001/manifest.json".to_string(),
                },
                GranuleRef {
                    table_oid: 1,
                    generation: 2,
                    row_count: 10,
                    pk_min: Some("11".to_string()),
                    pk_max: Some("20".to_string()),
                    manifest_path: "g00000000000000000002/manifest.json".to_string(),
                },
                GranuleRef {
                    table_oid: 1,
                    generation: 3,
                    row_count: 10,
                    pk_min: Some("21".to_string()),
                    pk_max: Some("30".to_string()),
                    manifest_path: "g00000000000000000003/manifest.json".to_string(),
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.granules.len(), 2);
        assert_eq!(plan.granules[0].generation, 2);
        assert_eq!(plan.granules[1].generation, 3);
    }

    #[test]
    fn materialize_rows_applies_bounds_and_projection() {
        let request = ScanRequest {
            table: test_table(),
            projection: vec![ColumnDescriptor {
                ordinal: 2,
                name: "payload".to_string(),
            }],
            pk_min: Some("2".to_string()),
            pk_max: Some("2".to_string()),
            limit: Some(1),
            snapshot_generation: None,
        };
        let plan = build_scan_plan(
            &request,
            &[GranuleRef {
                table_oid: 1,
                generation: 1,
                row_count: 2,
                pk_min: Some("1".to_string()),
                pk_max: Some("2".to_string()),
                manifest_path: "g00000000000000000001/manifest.json".to_string(),
            }],
        )
        .unwrap();
        let rows = materialize_row_entries(
            &plan,
            &[ScanBatch {
                granule: plan.granules[0].clone(),
                columns: vec![
                    ColumnVector {
                        column: ColumnDescriptor {
                            ordinal: 1,
                            name: "id".to_string(),
                        },
                        values: vec![json!(1), json!(2)],
                    },
                    ColumnVector {
                        column: ColumnDescriptor {
                            ordinal: 2,
                            name: "payload".to_string(),
                        },
                        values: vec![json!({"a": 1}), json!({"a": 2})],
                    },
                ],
            }],
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row, json!({"payload": {"a": 2}}));
    }

    #[test]
    fn materialize_row_entries_keeps_pk_outside_projection() {
        let request = ScanRequest {
            table: test_table(),
            projection: vec![ColumnDescriptor {
                ordinal: 2,
                name: "payload".to_string(),
            }],
            pk_min: None,
            pk_max: None,
            limit: None,
            snapshot_generation: None,
        };
        let plan = build_scan_plan(
            &request,
            &[GranuleRef {
                table_oid: 1,
                generation: 1,
                row_count: 1,
                pk_min: Some("1".to_string()),
                pk_max: Some("1".to_string()),
                manifest_path: "g00000000000000000001/manifest.json".to_string(),
            }],
        )
        .unwrap();
        let rows = materialize_row_entries(
            &plan,
            &[ScanBatch {
                granule: plan.granules[0].clone(),
                columns: vec![
                    ColumnVector {
                        column: ColumnDescriptor {
                            ordinal: 1,
                            name: "id".to_string(),
                        },
                        values: vec![json!(1)],
                    },
                    ColumnVector {
                        column: ColumnDescriptor {
                            ordinal: 2,
                            name: "payload".to_string(),
                        },
                        values: vec![json!({"a": 1})],
                    },
                ],
            }],
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk_text.as_deref(), Some("1"));
        assert_eq!(rows[0].row, json!({"payload": {"a": 1}}));
    }
}
