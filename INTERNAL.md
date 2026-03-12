# pghouse Internal Guide

## 목적

이 문서는 `pghouse`의 현재 구조, 개발 원칙, 설치/검증 흐름, 그리고 PostgreSQL extension 특유의 주의점을 정리한다.

현재 `pghouse`는 다음 상태다.

- `CREATE TABLE ... USING pghouse` 가능
- 실제 table AM read/write 엔진은 아직 heap delegate
- 별도 sidecar metadata와 granule 파일 저장 경로를 사용
- background maintenance가 mutation을 흡수하고 granule snapshot을 재작성

## 상위 구조

코드는 두 층으로 나뉜다.

- `src/core`
  - PostgreSQL에 직접 의존하지 않는 내부 엔진 로직
  - 데이터 계약, granule 생성, 압축, 파일 레이아웃 규칙
- `src/pg`
  - PostgreSQL extension 통합 레이어
  - access method bootstrap, `extension_sql!`, SPI catalog 접근, SQL 함수, background worker

이 분리는 앞으로 실제 read/write 엔진을 붙일 때 핵심이다.

- 내부 포맷과 알고리즘을 바꿀 때는 가능하면 `src/core`만 수정한다.
- PostgreSQL catalog, SQL API, trigger, bgworker, access method 등록을 바꿀 때만 `src/pg`를 수정한다.

## 디렉터리 구조

### `src/lib.rs`

extension entry point다.

- `pgrx::pg_module_magic!()` 선언
- `core` 모듈 export
- 외부 호환용 `interface` re-export
- `pg` 모듈 로드

### `src/core`

#### `src/core/interface.rs`

내부 데이터 계약과 교체 가능한 trait를 정의한다.

- `TableDescriptor`
- `ColumnDescriptor`
- `RowVersion`
- `GranuleWriteRequest`
- `ScanRequest`
- `ChunkCodec`
- `SnapshotWriter`
- `ScanPlanner`
- `GranuleReader`

새 storage engine, read path, codec을 붙일 때 가장 먼저 보는 파일이다.

#### `src/core/codec.rs`

현재 기본 codec 구현.

- `JsonArrayZstdCodec`
- 각 column chunk를 JSON array로 직렬화 후 zstd 압축

실제 columnar binary format으로 바꾸려면 이 레이어부터 교체하면 된다.

#### `src/core/snapshot.rs`

live row 집합을 granule 단위 write request로 바꾸는 순수 로직.

- row -> granule batch 분할
- generation 할당
- column별 encode 요청 생성

이 파일은 PostgreSQL SPI를 몰라야 한다.

#### `src/core/file_layout.rs`

granule 파일 이름과 디렉터리 레이아웃 규칙.

- `g00000000000000000001/`
- `c0001_payload.zstd.bin`

파일명 규칙을 바꾸거나 파일 쓰기 유틸을 교체할 때 이 레이어를 수정한다.

### `src/pg`

#### `src/pg/am.rs`

`pghouse` table access method bootstrap.

현재는 `GetHeapamTableAmRoutine()`를 반환한다. 즉 `USING pghouse`는 가능하지만 물리 row 저장은 아직 heap이다.

#### `src/pg/catalog.rs`

PostgreSQL catalog/SPI 통합 레이어.

역할:

- `pghouse` schema/table/index 생성
- trigger function 정의
- relation lookup
- metadata upsert
- pending mutation 조회/적용
- granule metadata insert/delete

중요:

- 이 파일은 SQL 문자열이 많다.
- 가능하면 "PostgreSQL 상태 조회/변경"만 담당한다.
- granule 조립 로직이나 codec 세부사항은 넣지 않는다.

#### `src/pg/api.rs`

사용자 호출 SQL 함수.

- `pghouse_register_table(...)`
- `pghouse_schedule_merge(...)`

여기는 얇게 유지한다.

- 입력 검증
- `pg/catalog` 호출
- JSON 응답 조립

실제 엔진 로직은 넣지 않는다.

#### `src/pg/storage.rs`

PostgreSQL 컨텍스트에서 sidecar granule 파일을 쓰는 어댑터.

역할:

- `storage_root` 계산
- `$PGDATA/pghouse/...` 기본 경로 계산
- `GranuleWriteRequest`를 파일과 metadata insert로 반영

주의:

- 이 레이어는 `SnapshotWriter` 구현체다.
- 내부 포맷은 `core` 계약을 따르고, PostgreSQL metadata 기록은 `pg/catalog`를 사용한다.

#### `src/pg/worker.rs`

maintenance orchestration.

흐름:

1. candidate table 조회
2. `pending_rows` 적용
3. dirty row / merge queue 검사
4. live rows 조회
5. `core::snapshot`으로 granule write request 생성
6. `pg::storage` writer로 파일 및 metadata 반영

핵심 원칙:

- orchestration만 하고 세부 포맷은 몰라야 한다.
- SQL API와 별도 lifecycle을 가져야 한다.

## 데이터 흐름

현재 write path는 직접 table AM write가 아니라 sidecar pipeline이다.

1. 사용자가 `USING pghouse` 테이블에 DML 수행
2. trigger `pghouse.capture_dml()`가 `pghouse.pending_rows`에 mutation 적재
3. maintenance worker가 pending mutation을 `pghouse.row_versions`에 반영
4. dirty/live row를 PK 순서로 읽음
5. granule 단위로 column chunk 생성 및 압축
6. 파일을 `storage_root` 아래에 기록
7. granule/chunk metadata를 `pghouse.granules`, `pghouse.column_chunks`에 기록

즉 현재는:

- base row storage: heap
- analytics sidecar storage: pghouse file-backed granules

## `extension_sql!` 사용 기준

`pgrx::extension_sql!`은 "extension 설치 시 같이 실행할 SQL DDL"을 넣는 용도다.

이 프로젝트에서 필요한 이유:

- `CREATE ACCESS METHOD`는 Rust 함수 export만으로 만들 수 없다.
- `pghouse` schema/table/index/PLpgSQL trigger function도 install 시점 DDL이 필요하다.

사용 기준은 다음과 같다.

- `#[pg_extern]`
  - 사용자가 `SELECT ...`로 호출할 SQL 함수
- `extension_sql!`
  - extension install 시 생성되어야 하는 schema object

새 객체를 추가할 때 판단 기준:

- 호출형 API면 `#[pg_extern]`
- install-time catalog/DDL이면 `extension_sql!`

## 개발 원칙

### 1. `core`는 `pgrx`를 몰라야 한다

가능하면 `src/core`에서는 `pgrx`, `Spi`, `pg_sys`를 쓰지 않는다.

예외가 생기면 먼저 설계를 다시 본다. 대부분은 DTO나 trait boundary가 부족한 경우다.

### 2. `pg`는 orchestration과 adapter에 집중한다

`src/pg`는 PostgreSQL 연동 코드다.

- catalog 조회/변경
- SQL 함수 노출
- bgworker 연결
- PostgreSQL 경로/환경 조회

비즈니스 로직이 길어지면 `core`로 이동시킨다.

### 3. public interface는 신중하게 바꾼다

외부에서 재사용 가능한 계약은 주로 `crate::interface`로 re-export된다.

이 계약을 바꾸면:

- `pg` 어댑터
- 미래의 read/write 엔진
- 테스트 도우미

가 같이 영향을 받는다.

### 4. metadata schema 변경은 재설치를 전제한다

현재 extension upgrade script는 없다.

따라서 `extension_sql!` 안의 schema를 바꾸면 보통 다음 절차가 필요하다.

```bash
cargo pgrx install --features pg18 --sudo
```

그리고 PostgreSQL에서:

```sql
DROP EXTENSION pghouse CASCADE;
CREATE EXTENSION pghouse;
```

주의:

- `CASCADE`를 쓰면 `USING pghouse` 테이블도 함께 삭제된다.
- 테스트용 DB에서 작업하는 게 안전하다.

## 빌드와 검증

### 기본 빌드

```bash
cargo check --features pg18
```

### SQL schema 생성 확인

```bash
cargo pgrx schema --features pg18
```

이 명령은 `#[pg_extern]`, `extension_sql!`이 실제로 어떤 SQL로 풀리는지 확인할 때 중요하다.

### 설치

```bash
cargo pgrx install --features pg18 --sudo
```

### PostgreSQL에서 활성화

```sql
CREATE EXTENSION pghouse;
```

### 메타테이블 확인

```sql
\dt pghouse.*
\d+ pghouse.tables
SELECT * FROM pghouse.tables;
```

## 로컬 개발 절차

새 기능을 넣을 때 권장 순서는 이렇다.

1. `core/interface.rs`에서 필요한 계약이 있는지 확인
2. 순수 로직이면 `src/core`에 먼저 구현
3. PostgreSQL adapter가 필요하면 `src/pg`에 얇게 연결
4. `cargo check --features pg18`
5. `cargo pgrx schema --features pg18`
6. 필요시 reinstall 후 `CREATE EXTENSION pghouse`

## 현재 제약

현재는 아직 bootstrap 단계다.

- table AM는 heap delegate
- read path용 custom scan/planner는 없음
- WAL/crash recovery 전략 미구현
- update/delete는 logical async가 아니라 sidecar 반영이 지연되는 모델
- primary key는 SQL unique enforcement이 아니라 merge key semantics에 더 가까움

즉 지금 구현은 "독립 storage engine 완성본"이 아니라 "pghouse 전용 storage/control plane의 뼈대"다.

## 앞으로의 확장 지점

### 실제 write path

후보:

- heap delegate 제거
- table AM write callback 구현
- `SnapshotWriter` 대신 직접 granule append/flush 경로 구현

### 실제 read path

후보:

- `ScanPlanner` 구현
- `GranuleReader` 구현
- planner hook/custom scan 연동

### merge/compaction 정책

후보:

- delta + base granule 구조
- multi-level merge
- tombstone compaction
- codec selection per column

## 디버깅 팁

### extension 설치 후 함수/AM 확인

```sql
SELECT extname FROM pg_extension WHERE extname = 'pghouse';
SELECT oid, amname, amhandler::regprocedure FROM pg_am WHERE amname = 'pghouse';
SELECT oid::regprocedure, probin, prosrc FROM pg_proc WHERE proname LIKE 'pghouse%';
```

### 설치된 `.so` 심볼 확인

```bash
nm -D /usr/lib/postgresql/pghouse.so | rg 'pghouse|pg_finfo|Pg_magic'
```

### sidecar 파일 확인

```bash
find /var/lib/postgres/data/pghouse -maxdepth 4 -type f
```

경로는 `storage_root` 설정에 따라 달라질 수 있다.

## 문서 갱신 규칙

다음이 바뀌면 이 문서도 같이 갱신한다.

- 모듈 구조
- extension install 절차
- metadata schema
- sidecar storage layout
- public interface 계약
- worker lifecycle

이 문서는 README보다 내부 구현자 기준으로 쓴다. 사용자 기능 설명보다 "어디를 고쳐야 하는가"와 "무엇을 건드리면 위험한가"를 우선한다.
