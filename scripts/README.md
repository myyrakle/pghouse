# scripts

이 디렉터리에는 `pghouse` 벤치 실행 스크립트가 있다.

현재 기준으로 유지하는 스크립트는 하나다.

- `benchmark_postgres_vs_pghouse.py`
  - 일반 PostgreSQL 테이블과 `USING pghouse` 테이블에 동일한 데이터를 적재
  - insert 시간 비교
  - aggregate 쿼리 반복 실행 시간 비교
  - 결과를 JSON 파일로 저장

## 전제 조건

- PostgreSQL 서버가 실행 중이어야 한다.
- `pghouse` extension이 설치 가능해야 한다.
- `python3`와 `psql`이 필요하다.
- 기본 예시는 `postgres` 사용자로 접속한다.

접속 테스트 예시:

```bash
psql -U postgres -Atqc "SELECT current_database(), current_user"
```

## 기본 실행

가장 단순한 실행:

```bash
python3 scripts/benchmark_postgres_vs_pghouse.py --user postgres
```

기본값:

- 행 수: `50,000,000`
- 배치 크기: `1,000,000`
- aggregate 반복 수: `3`
- 일반 PostgreSQL 테이블: `public.bench_events_postgres_50m`
- pghouse 테이블: `public.bench_events_pghouse_50m`
- 결과 파일: `bench/results/postgres_vs_pghouse_50000000.json`

## 주요 옵션

```bash
python3 scripts/benchmark_postgres_vs_pghouse.py \
  --user postgres \
  --db postgres \
  --rows 50000000 \
  --batch-rows 1000000 \
  --agg-repeats 5 \
  --postgres-table bench_events_postgres_50m \
  --pghouse-table bench_events_pghouse_50m \
  --result-path bench/results/postgres_vs_pghouse_50m.json
```

자주 쓰는 옵션:

- `--user`
  - 접속 사용자
- `--db`
  - 대상 데이터베이스
- `--host`
  - 원격 호스트
- `--port`
  - 포트
- `--rows`
  - 총 생성 행 수
- `--batch-rows`
  - 한 번에 insert할 행 수
- `--agg-repeats`
  - aggregate 쿼리 반복 횟수
- `--postgres-table`
  - 일반 PostgreSQL 테이블 이름
- `--pghouse-table`
  - pghouse 테이블 이름
- `--result-path`
  - 결과 JSON 출력 경로
- `--keep-existing`
  - 기존 테이블을 지우지 않고 유지

## 빠른 smoke test

작은 데이터로 빠르게 확인:

```bash
python3 scripts/benchmark_postgres_vs_pghouse.py \
  --user postgres \
  --rows 1000 \
  --batch-rows 1000 \
  --agg-repeats 1 \
  --postgres-table bench_events_postgres_smoke \
  --pghouse-table bench_events_pghouse_smoke \
  --result-path bench/results/smoke.json
```

## 결과 형식

결과는 JSON으로 저장된다. 주요 필드:

- `postgres_insert_ms`
- `pghouse_insert_ms`
- `postgres_aggregate_ms`
- `pghouse_aggregate_ms`
- `postgres_aggregate_avg_ms`
- `pghouse_aggregate_avg_ms`
- `postgres_row_count`
- `pghouse_row_count`
- `pghouse_granule_count`
- `pghouse_active_insert_buffers`

## 주의 사항

- 현재 `pghouse`는 regular SQL read path가 완전한 독립 엔진이 아니다.
- 그래서 aggregate 비교는 "현재 bootstrap 상태의 pghouse 테이블" 기준이다.
- insert 비교는 trigger, insert buffer, maintenance flush 비용까지 포함한다.
