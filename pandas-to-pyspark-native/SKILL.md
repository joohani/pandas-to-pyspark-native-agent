---
name: pandas-to-pyspark-native
description: Convert Python projects implemented with pandas into idiomatic native PySpark. Use when Codex needs to migrate `.py` application code, ETL jobs, notebooks translated into modules, or test fixtures away from pandas and toward `pyspark.sql` APIs, Spark SQL functions, and Spark-native I/O without relying on pandas-on-Spark compatibility layers.
---

# Pandas -> PySpark Native 변환 스킬

이 스킬은 pandas 코드를 Spark-native PySpark 코드로 변환하고, 반드시 검증 코드까지 함께 생성한다. 결과물은 단순 치환이 아니라 대용량 데이터 처리 관점에서 읽기 쉬운 Spark 파이프라인이어야 한다.

## 기본 임무

- 입력으로 pandas 코드가 주어진다고 가정한다.
- 출력은 반드시 다음 4개 섹션으로 구성한다.
  1. `PySpark Code`
  2. `Key Transformation Notes`
  3. `Validation Code`
  4. `Risk / Edge Cases`
- 변환 코드는 `pyspark.sql.functions as F`를 사용한다.
- UDF는 원칙적으로 금지한다. 정말 불가피한 경우가 아니면 사용하지 않는다.
- 검증 코드는 pandas 결과와 PySpark 결과를 비교할 수 있어야 한다.

## 출력 형식 규칙

응답은 항상 아래 순서를 지킨다.

### 1. PySpark Code

- pandas 코드를 fully converted 된 PySpark 코드로 제공한다.
- `SparkSession`, `DataFrame`, `F`, `Window`, `expr` 등 Spark-native 구성요소를 우선 사용한다.
- `withColumn`, `groupBy`, `join`, `Window`, `expr`, 내장 함수 중심으로 작성한다.
- 원본 로직과의 의미적 동등성을 최대한 유지한다.

### 2. Key Transformation Notes

- `apply`, `map`, `groupby`, `merge`, `pivot`, `loc`, `assign` 등이 어떤 Spark-native 패턴으로 바뀌었는지 설명한다.
- 로직 보존을 위해 둔 가정이 있으면 명시한다.
- UDF를 피하기 위해 어떤 내장 함수/윈도우/조인을 선택했는지 설명한다.

### 3. Validation Code

- pandas 결과와 PySpark 결과를 비교하는 검증 코드를 반드시 생성한다.
- 최소한 아래 비교를 포함한다.
  - row count check
  - aggregation comparison
  - sample data comparison
- 가능하면 assert 또는 명시적 비교 로직을 사용한다.
- mismatch row를 찾을 수 있으면 찾도록 작성한다.

### 4. Risk / Edge Cases

- null 처리 차이
- 타입 차이
- 정렬/순서 차이
- 분산 환경에서의 동작 차이
- 필요 시 timezone, decimal, duplicate key, join cardinality 위험

## 엄격 규칙

- `DO NOT use UDF unless unavoidable`
- 모든 `apply()` 패턴은 Spark-native 표현식으로 치환한다.
- 검증용 소량 데이터 비교 외에는 `collect()` 사용을 피한다.
- 컬럼 표현식 중심으로 작성한다.
- 출력 결과는 deterministic 하도록 정렬 기준 또는 키를 명시한다.

## 성능 규칙

- 불필요한 shuffle을 피한다.
- 필요한 경우에만 broadcast join을 사용한다.
- wide transformation을 최소화한다.
- 대용량 처리를 고려해 read -> transform -> validate -> write 구조를 유지한다.
- 긴 체인은 적절한 중간 DataFrame 이름으로 분리해 가독성을 높인다.

## 실행 절차

1. 입력 pandas 코드에서 `apply`, `map`, `groupby`, `merge`, `read_*`, `to_*`, row iteration, index 의존 로직을 찾는다.
2. [references/migration-rules.md](references/migration-rules.md)를 참고해 Spark-native 변환 패턴을 고른다.
3. 먼저 `PySpark Code`를 작성한다.
4. 이어서 변환 근거를 `Key Transformation Notes`에 정리한다.
5. 같은 로직을 비교할 수 있는 `Validation Code`를 작성한다.
6. 마지막에 `Risk / Edge Cases`를 정리한다.

## 가드레일

- `pyspark.pandas`, `koalas`, 하이브리드 호환 계층은 사용하지 않는다.
- Python UDF, pandas UDF, `rdd.map`, 드라이버 루프를 사용하지 않는다.
- pandas index 의존 동작은 명시적 키나 정렬 컬럼으로 치환한다.
- Spark 비용이 숨겨지는 과도하게 압축된 one-liner 체인을 만들지 않는다.
- 검증 섹션이 없으면 결과를 완료로 간주하지 않는다.

## 번들 리소스 사용

- `scripts/scan_pandas_usage.py`
  프로젝트 단위 변환 전, pandas 사용 지점을 빠르게 인벤토리화할 때 먼저 사용한다.
- [references/migration-rules.md](references/migration-rules.md)
  변환 규칙, UDF 금지 원칙, 검증 패턴, 가독성 규칙이 들어 있으므로 비단순 로직 변환 전에 읽는다.

## 기대 결과

- 결과 코드는 Spark-native 구조를 가진다.
- `apply` 기반 로직이 모두 제거된다.
- UDF 없이 내장 함수 중심으로 구성된다.
- pandas 대비 검증 가능한 코드가 함께 제공된다.
- 대용량 데이터 처리 관점에서 리뷰 가능한 수준의 가독성을 가진다.
