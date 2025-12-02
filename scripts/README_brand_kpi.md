# 브랜드별 KPI 업데이트 스크립트

## 개요

브랜드별 분석 KPI를 계산하는 스크립트입니다.

## 계산 항목

1. **실판매출(현시점)**: ke30 Shop 파일에서 브랜드별 실판매액 합계
2. **직접이익(현시점)**: ke30 Shop 파일에서 브랜드별 직접이익 합계
3. **직접이익율(현시점)**: 직접이익/실판매출*1.1
4. **진척율**: plan 파일 내수합계의 실판매액[v+] 대비 ke30 실판매액

## 사용법

### 기본 사용

```bash
python scripts/update_brand_kpi.py 20251124
```

### 출력 파일 경로 지정

```bash
python scripts/update_brand_kpi.py 20251124 --output output/brand_kpi.csv
```

### 연월 지정

```bash
python scripts/update_brand_kpi.py 20251124 --year-month 202511
```

## 입력 파일

- **ke30 Shop 파일**: `raw/YYYYMM/current_year/YYYYMMDD/ke30_YYYYMMDD_YYYYMM_Shop.csv`
- **계획 파일**: `raw/YYYYMM/plan/plan_YYYYMM_전처리완료.csv`

## 출력 파일

기본 저장 위치: `public/brand_kpi_YYYYMMDD.js`

## 출력 형식

```csv
브랜드,실판매출(현시점),직접이익(현시점),직접이익율(현시점),진척율
I,7294283390,xxx,xx.xx,69.0
M,27674806896,xxx,xx.xx,79.0
ST,642574330,xxx,xx.xx,50.0
V,4584514760,xxx,xx.xx,74.0
W,173935269,xxx,xx.xx,48.0
X,51129081462,xxx,xx.xx,67.0
```

## 계산 로직

### 실판매출(현시점)
- ke30 Shop 파일에서 브랜드별로 '합계 : 실판매액' 컬럼 합계

### 직접이익(현시점)
- ke30 Shop 파일에서 브랜드별로 '직접이익' 컬럼 합계

### 직접이익율(현시점)
- 공식: (직접이익 / 실판매출) * 1.1 * 100
- 단위: 퍼센트(%)

### 진척율
- 공식: (ke30 실판매액 / plan 내수합계 실판매액[v+]) * 100
- 계획 파일에서 채널='내수합계'인 행의 '실판매액 [v+]' 값 사용
- 단위: 퍼센트(%)


