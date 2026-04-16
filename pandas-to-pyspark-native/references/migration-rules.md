# Pandas -> PySpark 변환 규칙

## 빠른 매핑

- `pd.read_csv(...)` -> `spark.read.option(...).csv(...)`
- `pd.read_parquet(...)` -> `spark.read.parquet(...)`
- `df.to_parquet(...)` -> `df.write.mode(...).parquet(...)`
- `df["col"] = ...` -> `df.withColumn("col", ...)`
- `df.rename(columns={...})` -> `withColumnRenamed(...)` 또는 `select(...alias(...))`
- `df.drop(columns=[...])` -> `df.drop(...)`
- `df.merge(right, on=..., how=...)` -> `df.join(right, on=..., how=...)`
- `df.groupby(keys).agg(...)` -> `df.groupBy(keys).agg(...)`
- `df.sort_values(...)` -> `df.orderBy(...)`
- `df.fillna(...)` -> `df.fillna(...)` 또는 `F.coalesce(...)`
- `df.assign(...)` -> 연속된 `withColumn(...)`
- `df.query(...)` -> `df.filter(...)`
- `df.loc[mask]` -> `df.filter(...)`
- `df.drop_duplicates(...)` -> `df.dropDuplicates(...)`
- `df.apply(func, axis=1)` -> 컬럼 표현식, 조인, 윈도우 함수로 재작성
- `Series.map(dict)` -> `F.create_map`, `F.when`, 또는 lookup join

## 핵심 원칙

- PySpark native 구조를 유지한다.
- 비즈니스 로직은 DataFrame 변환으로 표현한다.
- `pyspark.sql.functions as F` 내장 함수를 우선 사용한다.
- 파일 상단에 `import pyspark.sql.functions as F`와 `import pyspark.sql.types as T`를 기본 선언으로 둔다.
- Python UDF와 pandas UDF는 기본적으로 금지한다.
- 명시적 schema, 명시적 정렬, 명시적 키를 우선한다.
- 대용량 파이프라인 리뷰가 가능하도록 읽기 쉬운 단계형 코드를 만든다.

## 필수 아키텍처

- 코드는 가능하면 아래 순서를 유지한다.
  1. session/bootstrap
  2. source read
  3. named transform stages
  4. validation/check
  5. sink write
- 긴 체인은 `orders_filtered`, `orders_enriched`, `daily_metrics` 같은 중간 이름으로 분리한다.
- helper 함수를 만들더라도 Spark `Column` 또는 DataFrame을 반환하도록 유지한다.

## UDF 금지 원칙

- 다음은 기본적으로 금지한다.
  - Python UDF
  - pandas UDF
  - `rdd.map`
  - driver-side row loop
- `udf`, `@udf`, `pandas_udf`, `rdd.map`은 모두 금지 패턴으로 간주한다.
- UDF가 필요해 보이면 먼저 아래 대안을 찾는다.
  - `F.when`, `F.coalesce`, `F.expr`
  - `F.concat_ws`, `F.regexp_replace`, `F.substring`
  - `F.to_date`, `F.to_timestamp`, `F.date_format`
  - `F.split`, `F.explode`, `F.from_json`, `F.element_at`
  - `Window.partitionBy(...).orderBy(...)`
  - 작은 매핑은 `F.create_map`
  - 큰 매핑은 join 또는 broadcast join

## 주요 패턴별 변환 가이드

### apply 변환

- `axis=1 apply`는 가장 우선적으로 제거한다.
- 조건문 기반이면 `F.when(...).otherwise(...)`로 바꾼다.
- 이전/다음 행 참조면 window 함수로 바꾼다.
- dictionary/lookup 성격이면 map 또는 join으로 바꾼다.
- 문자열/날짜 파싱이면 내장 함수 조합으로 바꾼다.

### map 변환

- 작은 static mapping은 `F.create_map` 사용
- 큰 mapping table은 join 사용
- 복잡한 조건 분기는 `F.when` 체인 사용

### groupby 변환

- `groupby(...).agg(...)`는 `groupBy(...).agg(...)`로 바꾼다.
- 집계 컬럼은 즉시 alias를 붙인다.
- pandas의 `as_index=False` 동작은 Spark에서는 group key 컬럼이 그대로 남는 점으로 대응한다.

### merge 변환

- `merge`는 `join`으로 바꾼다.
- join key, join type, cardinality를 명시적으로 확인한다.
- 작은 차원 테이블이면 `broadcast` 사용 가능성을 검토한다.
- 작은 참조 데이터 조인은 `F.broadcast(small_df)`를 우선 검토한다.
- 불필요한 shuffle을 만들지 않도록 join 전 projection/filter를 먼저 적용한다.

### 타입 캐스팅 변환

- `df['A'].astype(str)`는 `df.withColumn('A', F.col('A').cast(T.StringType()))`로 바꾼다.

### 불리언 필터 변환

- `df[df['A'] > 0]`는 `df.filter(F.col('A') > 0)`로 바꾼다.

## 성능 규칙

- 불필요한 shuffle을 피한다.
- 필요한 경우에만 broadcast join을 사용한다.
- wide transformation을 최소화한다.
- `collect()`, `count()`, `toPandas()`, `show()`는 transformation 중간 단계에서 사용하지 않는다.
- `collect()`는 검증용 소량 데이터 비교에서만 제한적으로 사용한다.
- production 경로에 `toPandas()`를 두지 않는다.

## Window 함수 규칙

- `shift()`, `rolling()`, `diff()` 같은 순차 의존 로직은 반드시 `Window.partitionBy(...).orderBy(...)` 기반으로 재작성한다.
- 이전 행/다음 행 참조는 `lag`, `lead`를 우선 사용한다.
- 누적 집계는 window aggregate를 사용하고 정렬 컬럼을 반드시 명시한다.

## Null / 스키마 규칙

- null 처리는 `fillna`, `na.fill`, `F.coalesce`를 명시적으로 사용한다.
- JSON/Array explode 전에는 `StructType`, `ArrayType` 등 명시적 스키마를 먼저 정의한다.
- Spark의 null/type 엄격성을 고려해 cast와 default 값을 명확히 남긴다.

## 검증 코드 규칙

- 검증 코드는 반드시 포함한다.
- 최소 검증 항목:
  - row count check
  - aggregation comparison
  - sample data comparison
- 가능하면 `assert` 또는 불일치 감지 로직을 사용한다.
- 가능하면 mismatch row를 보여주는 비교 코드를 작성한다.
- deterministic comparison을 위해 비교 전 정렬 기준을 명시한다.

## 자주 쓰는 예시

### import / session

```python
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("job-name").getOrCreate()
```

### 컬럼 생성

```python
df = df.withColumn("amount_usd", F.col("amount") * F.col("fx_rate"))
```

### 조건 분기

```python
df = df.withColumn(
    "status",
    F.when(F.col("amount") > 0, F.lit("paid")).otherwise(F.lit("free")),
)
```

### 집계

```python
agg = df.groupBy("customer_id").agg(F.sum("amount").alias("amount"))
```

### 윈도우

```python
window = Window.partitionBy("customer_id").orderBy("event_ts")
df = df.withColumn("prev_amount", F.lag("amount").over(window))
```

## 리스크 체크포인트

- null 처리 차이
- 문자열/숫자/decimal 타입 차이
- pandas index 제거로 인한 의미 변화
- 정렬이 보장되지 않는 문제
- join 중복 증폭
- timezone 차이
- floating-point 비교 차이

## 리뷰 체크리스트

- `import pandas` 또는 `pd.` 참조가 남아 있는가
- `apply()`가 제거되었는가
- Python UDF, pandas UDF, RDD API가 도입되었는가
- `F` 내장 함수로 충분히 바뀌었는가
- 검증 코드가 포함되었는가
- 결과 비교가 deterministic 한가
- 과도하게 복잡한 체인으로 가독성이 떨어지지 않는가
