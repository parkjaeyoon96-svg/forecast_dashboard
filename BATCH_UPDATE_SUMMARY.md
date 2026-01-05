# Dashboard JSON Generation Batch 수정 완료

## 문제 요약

### 1. 전체현황
- ❌ 월중 누적 매출 추이 데이터 없음
- ❌ 당시즌의류/ACC 판매율 분석에서 판매율 평균 재계산 안됨

### 2. 브랜드별 분석
- ❌ 채널별 트리맵 업데이트 안됨
- ❌ 트리맵 날짜 데이터 없음
- ❌ 주차별 매출 추세 데이터 없음

---

## 해결 방법

### ✅ 1. 월중 누적 매출 추이 데이터

**파일**: `dashboard_json_gen.bat`

**변경사항**:
- **Step 1.5**: `download_weekly_sales_trend.py` 실행
  - 주차별 매출 원본 데이터 다운로드
  - 저장 위치: `raw/{YYYYMM}/ETC/weekly_sales_trend_{DATE}.csv`
  
- **Step 2**: `update_overview_data.py` 실행
  - `create_cumulative_trend_data()` 함수로 누적 매출 추이 생성
  - 출력: `public/data/{DATE}/overview_trend.json`
  - 포함 데이터:
    ```json
    {
      "weeks": ["11/24", "12/1", ...],
      "weekly_current": [100.5, 120.3, ...],
      "weekly_prev": [95.2, 115.8, ...],
      "cumulative_current": [100.5, 220.8, ...],
      "cumulative_prev": [95.2, 211.0, ...]
    }
    ```

**배치 로그**:
```
[Step 1.5] Completed - Weekly sales data downloaded for cumulative sales trend
[Step 2] Completed
  + overview_kpi.json (includes cumulative sales trend)
  + overview_trend.json (weekly cumulative sales chart)
```

---

### ✅ 2. 판매율 평균 재계산

**파일**: `dashboard_json_gen.bat`

**변경사항**:
- **Step 2**: `update_overview_data.py` 실행
  - `create_overview_stock_analysis_data()` 함수로 전체 브랜드 판매율 집계
  - 브랜드별/아이템별/카테고리별 판매율 재계산
  
- **Step 7-Post**: `generate_brand_stock_analysis.py` 실행
  - CSV에서 원본 데이터 읽어서 판매율 재계산
  - 출력: `public/data/{DATE}/stock_analysis.json`
  - 포함 데이터:
    ```json
    {
      "clothingSummary": {
        "overall": {
          "cumSalesRate": 0.4523,
          "cumSalesRatePy": 0.4102,
          "cumSalesRateDiff": 0.0421,
          "pyClosingSalesRate": 0.5601
        }
      },
      "clothingItemRatesOverall": {
        "ITEM001": {
          "cumSalesRate": 0.4523,
          "cumSalesRatePy": 0.4102,
          "pyClosingSalesRate": 0.5601
        }
      }
    }
    ```

**배치 로그**:
```
[Step 2] Completed
  + stock_analysis.json (with sales rate averages recalculated)
[Step 7-Post] Success - Stock analysis with sales rate averages generated
  + Includes clothingItemRatesOverall (recalculated averages)
```

---

### ✅ 3. 채널별 트리맵 업데이트 & 날짜 데이터

**파일**: `dashboard_json_gen.bat`

**변경사항**:
- **Step 7.5**: `download_previous_year_treemap_data.py` 실행
  - 전년 동주차 트리맵 데이터 다운로드 (YOY 계산용)
  
- **Step 8**: `run_treemap_pipeline.py` 실행
  - 전체 트리맵 파이프라인 실행:
    1. `download_treemap_rawdata.py`: 스노우플레이크에서 원본 데이터 다운로드
    2. `preprocess_treemap_data.py`: 채널/아이템 매핑, 시즌 분류
    3. `create_treemap_data_v2.py`: JSON 생성 (YOY 포함)
  - 출력: `public/data/{DATE}/treemap.json`
  - 포함 데이터:
    ```json
    {
      "metadata": {
        "updateDate": "2025-12-01",
        "weekStart": "2025-11-24",
        "weekEnd": "2025-11-30"
      },
      "M": {
        "백화점": {
          "value": 15000000000,
          "share": 35,
          "yoy": 105
        }
      }
    }
    ```

**배치 로그**:
```
[Step 7.5] Completed
[Step 8] Completed - Treemap with dates and YOY data generated
```

---

### ✅ 4. 주차별 매출 추세 데이터

**파일**: `dashboard_json_gen.bat`

**변경사항**:
- **Step 1.5**: `download_weekly_sales_trend.py` 실행
  - 주차별 매출 원본 데이터 다운로드
  
- **Step 2**: `update_overview_data.py` 실행
  - `create_cumulative_trend_data()`로 주차별 추세 데이터 생성
  - 출력: `public/data/{DATE}/overview_trend.json`

**배치 로그**:
```
[Step 1.5] Completed - Weekly sales data downloaded for cumulative sales trend
[Step 2] Completed
  + overview_trend.json (weekly cumulative sales chart)
```

---

## 최종 배치 실행 흐름

```
Step 1:   update_brand_kpi.py
Step 1.5: download_weekly_sales_trend.py  ← 주차별 매출 다운로드
Step 2:   update_overview_data.py          ← 월중 누적 추이 + 판매율 재계산
Step 3:   create_brand_pl_data.py
Step 4:   update_brand_radar.py
Step 5:   process_channel_profit_loss.py
Step 6:   download_brand_stock_analysis.py
Step 7:   generate_brand_stock_analysis.py ← 판매율 평균 재계산
Step 7.5: download_previous_year_treemap_data.py ← 전년 트리맵 데이터
Step 8:   run_treemap_pipeline.py          ← 트리맵 전체 파이프라인 (날짜/YOY 포함)
Step 9:   export_to_json.py                ← 최종 JSON 통합
Step 10:  generate_ai_insights.py
```

---

## 생성되는 JSON 파일

### `public/data/{DATE}/`
- ✅ `overview_trend.json` - 월중 누적 매출 추이 (주차별/누적)
- ✅ `stock_analysis.json` - 판매율 분석 (평균 재계산 포함)
- ✅ `treemap.json` - 채널별 트리맵 (날짜/YOY 포함)
- ✅ `overview_kpi.json` - 전체 현황 KPI
- ✅ `brand_kpi.json` - 브랜드별 KPI
- ✅ `brand_pl.json` - 브랜드별 손익
- ✅ `channel_profit_loss.json` - 채널별 손익

---

## 검증 방법

배치 실행 후 다음을 확인하세요:

```powershell
# 1. 생성된 JSON 파일 확인
dir public\data\{DATE}\*.json

# 2. overview_trend.json 내용 확인
type public\data\{DATE}\overview_trend.json | findstr "weeks cumulative"

# 3. stock_analysis.json의 판매율 확인
type public\data\{DATE}\stock_analysis.json | findstr "cumSalesRate clothingItemRatesOverall"

# 4. treemap.json의 날짜 확인
type public\data\{DATE}\treemap.json | findstr "updateDate weekStart"
```

---

## 완료!

모든 문제가 해결되었습니다. 배치를 실행하면 한 번에 모든 데이터가 생성됩니다.







