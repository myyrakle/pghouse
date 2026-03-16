use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PkCompareMode {
    Lexical,
    I64,
}

impl PkCompareMode {
    pub(crate) fn from_sql_label(value: &str) -> Self {
        match value {
            "i64" => Self::I64,
            _ => Self::Lexical,
        }
    }

    pub(crate) fn as_sql_label(self) -> &'static str {
        match self {
            Self::Lexical => "lexical",
            Self::I64 => "i64",
        }
    }
}

pub(crate) fn compare_pk_text(mode: PkCompareMode, left: &str, right: &str) -> Ordering {
    match mode {
        PkCompareMode::Lexical => left.cmp(right),
        PkCompareMode::I64 => match (left.parse::<i64>(), right.parse::<i64>()) {
            (Ok(left_value), Ok(right_value)) => {
                left_value.cmp(&right_value).then_with(|| left.cmp(right))
            }
            (Ok(_), Err(_)) => Ordering::Less,
            (Err(_), Ok(_)) => Ordering::Greater,
            (Err(_), Err(_)) => left.cmp(right),
        },
    }
}

pub(crate) fn compare_optional_pk_text(
    mode: PkCompareMode,
    left: Option<&str>,
    right: Option<&str>,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_pk_text(mode, left, right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub(crate) fn pk_matches_bounds(
    mode: PkCompareMode,
    value: Option<&str>,
    pk_min: Option<&str>,
    pk_max: Option<&str>,
) -> bool {
    let Some(value) = value else {
        return pk_min.is_none() && pk_max.is_none();
    };

    if pk_min.is_some_and(|bound| compare_pk_text(mode, value, bound).is_lt()) {
        return false;
    }

    if pk_max.is_some_and(|bound| compare_pk_text(mode, value, bound).is_gt()) {
        return false;
    }

    true
}

pub(crate) fn granule_overlaps(
    mode: PkCompareMode,
    granule_min: Option<&str>,
    granule_max: Option<&str>,
    pk_min: Option<&str>,
    pk_max: Option<&str>,
) -> bool {
    if let Some(pk_min) = pk_min
        && granule_max.is_some_and(|value| compare_pk_text(mode, value, pk_min).is_lt())
    {
        return false;
    }

    if let Some(pk_max) = pk_max
        && granule_min.is_some_and(|value| compare_pk_text(mode, value, pk_max).is_gt())
    {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::{PkCompareMode, compare_pk_text, granule_overlaps, pk_matches_bounds};
    use std::cmp::Ordering;

    #[test]
    fn i64_mode_sorts_numeric_text_numerically() {
        assert_eq!(compare_pk_text(PkCompareMode::I64, "2", "10"), Ordering::Less);
        assert_eq!(
            compare_pk_text(PkCompareMode::Lexical, "2", "10"),
            Ordering::Greater
        );
    }

    #[test]
    fn i64_mode_uses_numeric_bounds() {
        assert!(pk_matches_bounds(
            PkCompareMode::I64,
            Some("10"),
            Some("2"),
            Some("20")
        ));
        assert!(!pk_matches_bounds(
            PkCompareMode::I64,
            Some("10"),
            Some("11"),
            Some("20")
        ));
    }

    #[test]
    fn i64_mode_prunes_non_overlapping_numeric_granules() {
        assert!(!granule_overlaps(
            PkCompareMode::I64,
            Some("10"),
            Some("20"),
            Some("2"),
            Some("9")
        ));
    }
}
