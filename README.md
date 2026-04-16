# pandas-to-pyspark-native-agent

pandas 기반 Python 코드를 Spark-native PySpark 코드로 변환하기 위한 Codex 스킬 저장소입니다.

이 에이전트는 단순 문법 치환이 아니라 아래 목표를 기준으로 동작하도록 설계되어 있습니다.

- `pyspark.sql` 기반의 native 구조 유지
- UDF 사용 금지
- `pyspark.sql.functions as F` 내장 함수 우선 사용
- 대용량 데이터 처리에 맞는 가독성과 유지보수성 확보
- pandas 결과와 PySpark 결과를 비교하는 검증 코드 자동 생성

## 저장소 구성

- [README.md](/Users/joohani/Documents/New%20project/README.md)
  사용 방법과 전체 개요
- [pandas-to-pyspark-native/SKILL.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/SKILL.md)
  Codex가 직접 읽는 스킬 본문
- [pandas-to-pyspark-native/references/migration-rules.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/references/migration-rules.md)
  pandas -> PySpark 변환 규칙, 성능 원칙, 검증 규칙
- [pandas-to-pyspark-native/scripts/scan_pandas_usage.py](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/scripts/scan_pandas_usage.py)
  프로젝트 내 pandas 사용 위치를 찾는 스캐너

## 이 에이전트가 하는 일

이 스킬은 pandas 코드가 입력되면 반드시 아래 4개 섹션으로 결과를 생성하도록 설계되어 있습니다.

1. `PySpark Code`
2. `Key Transformation Notes`
3. `Validation Code`
4. `Risk / Edge Cases`

즉, 변환 코드만 주는 것이 아니라 아래까지 함께 제공합니다.

- `apply`, `map`, `groupby`, `merge`가 어떻게 바뀌었는지 설명
- pandas 대비 검증 코드
- row count / aggregation / sample data 비교
- null, 타입, 정렬, 분산 처리 차이에 대한 리스크 정리

## 주요 원칙

- `pyspark.pandas` 또는 호환 계층을 사용하지 않습니다.
- Python UDF, pandas UDF, `rdd.map` 사용을 금지합니다.
- `apply()` 패턴은 Spark-native 표현식으로 치환합니다.
- `withColumn`, `groupBy`, `join`, `Window`, `expr`, `F.*` 내장 함수 중심으로 변환합니다.
- 검증용 소량 데이터 비교를 제외하면 `collect()`를 피합니다.
- deterministic 비교를 위해 정렬 기준 또는 키를 명시합니다.
- 대용량 데이터 처리 가독성을 위해 긴 체인을 중간 DataFrame 단계로 분리합니다.

## 사용 방법

### 1. Codex에서 직접 스킬로 호출하기

이 저장소의 스킬 경로는 아래입니다.

```text
/Users/joohani/Documents/New project/pandas-to-pyspark-native
```

Codex에서 가장 기본적으로는 아래처럼 요청하면 됩니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native to convert the following pandas code into Spark-native PySpark and include validation code.

<<<PANDAS_CODE_HERE>>>
```

### 2. 권장 프롬프트 형태

아래처럼 요구사항을 함께 적어주면 더 안정적으로 동작합니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

Convert the following pandas code to Spark-native PySpark.

Requirements:
- no UDF
- use pyspark.sql.functions as F
- replace all apply() patterns
- keep deterministic ordering
- include validation code
- explain how apply/map/groupby were converted
- highlight null/type/ordering risks

<<<PANDAS_CODE_HERE>>>
```

### 3. 한글 프롬프트 예시

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

아래 pandas 코드를 Spark-native PySpark 코드로 변환해줘.

조건:
- UDF 금지
- pyspark.sql.functions as F 사용
- apply()는 모두 제거
- 내장함수 중심으로 변환
- Validation Code 반드시 포함
- pandas와 PySpark 결과 비교 코드 포함
- 정렬 기준을 명시해서 deterministic 하게 비교
- null/type/order 차이 리스크도 정리

<<<PANDAS_CODE_HERE>>>
```

## 결과 형식

정상적으로 동작하면 결과는 항상 아래 구조를 따릅니다.

### 1. PySpark Code

- 완전히 변환된 PySpark 코드
- `SparkSession`, `F`, `Window`, `expr` 사용 가능
- Spark-native transformation 중심

### 2. Key Transformation Notes

- 어떤 pandas 로직이 어떤 Spark 패턴으로 바뀌었는지 설명
- 예:
  - `apply(axis=1)` -> `F.when`, `F.expr`, window 함수
  - `map(dict)` -> `F.create_map` 또는 join
  - `groupby().agg()` -> `groupBy().agg()`

### 3. Validation Code

- pandas 결과와 PySpark 결과 비교
- row count check
- aggregation comparison
- sample data comparison
- 가능하면 mismatch row 확인 코드 포함

### 4. Risk / Edge Cases

- null 처리 차이
- 타입 차이
- 순서/정렬 차이
- join cardinality 문제
- timezone / decimal / duplicate key 이슈

## 프로젝트 단위로 사용할 때

실제 프로젝트를 통째로 옮길 때는 먼저 pandas 사용 위치를 찾는 것이 좋습니다.

### pandas 사용 위치 스캔

아래 스크립트로 프로젝트 내부의 pandas 사용 지점을 빠르게 찾을 수 있습니다.

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/scan_pandas_usage.py /변환할/프로젝트/경로
```

이 스크립트는 다음 같은 항목을 찾아줍니다.

- `import pandas`
- `pd.read_csv`, `pd.read_parquet`
- `apply`, `groupby`, `merge`, `map`
- `sort_values`, `fillna`, `drop_duplicates`

### 프로젝트 변환 추천 순서

1. 스캐너로 pandas 사용 파일을 찾습니다.
2. I/O가 많은 파일부터 Spark read/write 구조로 바꿉니다.
3. `apply`, row loop, index 의존 로직을 우선 제거합니다.
4. `groupby`, `merge`, `window` 로직을 Spark-native로 재작성합니다.
5. pandas 결과와 비교하는 검증 코드를 붙입니다.
6. 마지막에 null, 타입, 정렬 리스크를 점검합니다.

## 이 에이전트가 특히 잘 다루는 패턴

- `apply(axis=1)` 제거
- `map(dict)`를 `create_map` 또는 join으로 전환
- `groupby().agg()`를 `groupBy().agg()`로 전환
- `merge()`를 `join()`으로 전환
- 누적 합계, 이전 행 참조, 순위 계산을 window 함수로 전환
- 문자열/날짜 파싱을 `F.*` 내장 함수로 전환

## 제한 사항

- 매우 복잡한 Python 로직은 바로 Spark 표현식으로 바꾸기 어려울 수 있습니다.
- 사용자 정의 파싱 함수가 많은 경우 built-in 함수 조합으로 재설계가 필요할 수 있습니다.
- pandas index 의미가 중요한 코드는 명시적 key/order column 설계가 필요합니다.
- 검증 코드는 입력 데이터 규모에 따라 샘플 기반 비교가 적절할 수 있습니다.

## 참고 문서

- 스킬 본문: [pandas-to-pyspark-native/SKILL.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/SKILL.md)
- 변환 규칙: [pandas-to-pyspark-native/references/migration-rules.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/references/migration-rules.md)

## 빠른 시작

가장 짧게는 아래 프롬프트 하나로 시작하면 됩니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

아래 pandas 코드를 PySpark native 코드로 바꾸고, 변환 노트와 validation code, risk까지 함께 작성해줘.

<<<PANDAS_CODE_HERE>>>
```
