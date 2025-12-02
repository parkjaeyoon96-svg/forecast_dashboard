# 대시보드 JSON 파일 매핑표

## 📋 배치 파일 실행 시 생성되는 JSON 파일 목록

### ✅ 전체현황 (Overview) 섹션

| JSON 파일 | 생성 스크립트 | 대시보드 사용 위치 | 필수 여부 |
|-----------|--------------|------------------|----------|
| `overview_kpi.json` | `update_overview_data.py` | 전체현황 KPI 카드 | ✅ 필수 |
| `overview_by_brand.json` | `update_overview_data.py` | 브랜드별 매출/이익 | ✅ 필수 |
| `overview_pl.json` | `update_overview_data.py` | **전체현황 손익계산서** | ✅ 필수 |
| `overview_waterfall.json` | `update_overview_data.py` | 워터폴 차트 | ✅ 필수 |
| `overview_trend.json` | `update_overview_data.py` | 누적 추이 차트 | ✅ 필수 |
| `overview.json` | `update_overview_data.py` | 통합 파일 (하위 호환성) | ⚠️ 선택 |

### ✅ 브랜드별 분석 섹션

| JSON 파일 | 생성 스크립트 | 대시보드 사용 위치 | 필수 여부 |
|-----------|--------------|------------------|----------|
| `brand_kpi.json` | `update_brand_kpi.py` | 브랜드별 KPI 카드 | ✅ 필수 |
| `brand_pl.json` | `create_brand_pl_data.py` | **브랜드별 손익계산서** | ✅ 필수 |
| `radar_chart.json` | `update_brand_radar.py` | 레이더 차트 | ✅ 필수 |
| `channel_profit_loss.json` | `process_channel_profit_loss.py` | 채널별 손익 테이블 | ✅ 필수 |
| `weekly_trend.json` | `download_weekly_sales_trend.py` + `export_to_json.py` | **주차별 매출 추세** | ✅ 필수 |
| `stock_analysis.json` | `download_brand_stock_analysis.py` + `update_overview_data.py` | 재고 분석 | ✅ 필수 |
| `treemap.json` | `create_treemap_data_v2.py` + `export_to_json.py` | 트리맵 차트 | ✅ 필수 |
| `brand_plan.json` | `update_overview_data.py` | 전체 브랜드 레이더 차트 | ✅ 필수 |
| `ai_insights/insights_data_*.json` | `generate_ai_insights.py` | AI 인사이트 | ⚠️ 선택 |

### 📊 추가 파일

| JSON 파일 | 생성 스크립트 | 대시보드 사용 위치 | 필수 여부 |
|-----------|--------------|------------------|----------|
| `metrics.json` | `export_to_json.py` | 메트릭스 데이터 | ⚠️ 선택 |
| `channel_pl.json` | `export_to_json.py` | 채널 손익 (레거시) | ⚠️ 선택 |

---

## 🔄 배치 파일 실행 순서 및 생성 파일

```
대시보드_데이터_가공_JSON생성.bat 20251124
    ↓
[1/10] update_brand_kpi.py
    → brand_kpi.json ✅
    ↓
[2/10] update_overview_data.py
    → overview_kpi.json ✅
    → overview_by_brand.json ✅
    → overview_pl.json ✅
    → overview_waterfall.json ✅
    → overview_trend.json ✅
    → brand_plan.json ✅
    → stock_analysis.json ✅
    ↓
[3/10] create_brand_pl_data.py
    → brand_pl.json ✅
    ↓
[4/10] update_brand_radar.py
    → radar_chart.json ✅
    ↓
[5/10] process_channel_profit_loss.py
    → channel_profit_loss.json ✅
    ↓
[6/10] download_weekly_sales_trend.py
    → weekly_sales_trend_*.js 생성
    ↓
[7/10] download_brand_stock_analysis.py
    → brand_stock_analysis_*.js 생성
    ↓
[8/10] create_treemap_data_v2.py
    → treemap_data_v2_*.js 생성
    ↓
[9/10] export_to_json.py
    → weekly_trend.json ✅ (JS에서 변환)
    → treemap.json ✅ (JS에서 변환)
    → metrics.json (JS에서 변환)
    ↓
[10/10] generate_ai_insights.py
    → ai_insights/insights_data_*.json ✅
```

---

## ✅ 대시보드 반영 확인

### 대시보드가 로드하는 모든 JSON 파일 (14개)

1. ✅ `overview_kpi.json` - Step 2에서 생성
2. ✅ `overview_by_brand.json` - Step 2에서 생성
3. ✅ `overview_pl.json` - Step 2에서 생성
4. ✅ `overview_waterfall.json` - Step 2에서 생성
5. ✅ `overview_trend.json` - Step 2에서 생성
6. ✅ `brand_kpi.json` - Step 1에서 생성
7. ✅ `brand_pl.json` - Step 3에서 생성
8. ✅ `radar_chart.json` - Step 4에서 생성
9. ✅ `channel_profit_loss.json` - Step 5에서 생성
10. ✅ `weekly_trend.json` - Step 6 + Step 9에서 생성
11. ✅ `stock_analysis.json` - Step 2 + Step 7에서 생성
12. ✅ `treemap.json` - Step 8 + Step 9에서 생성
13. ✅ `brand_plan.json` - Step 2에서 생성
14. ✅ `ai_insights/insights_data_*.json` - Step 10에서 생성

---

## 🎯 결론

**✅ 네, 배치 파일을 실행하면 대시보드의 모든 데이터가 반영됩니다!**

배치 파일이 생성하는 JSON 파일들이 대시보드가 로드하는 모든 JSON 파일과 일치합니다.

### 대시보드 영역별 반영 내용

#### 전체현황 섹션
- ✅ KPI 카드 (overview_kpi.json)
- ✅ 브랜드별 매출/이익 (overview_by_brand.json)
- ✅ **전체 손익계산서** (overview_pl.json)
- ✅ 워터폴 차트 (overview_waterfall.json)
- ✅ 누적 추이 차트 (overview_trend.json)

#### 브랜드별 분석 섹션
- ✅ 브랜드별 KPI 카드 (brand_kpi.json)
- ✅ **브랜드별 손익계산서** (brand_pl.json)
- ✅ 레이더 차트 (radar_chart.json)
- ✅ 채널별 손익 테이블 (channel_profit_loss.json)
- ✅ **주차별 매출 추세** (weekly_trend.json)
- ✅ 재고 분석 (stock_analysis.json)
- ✅ 트리맵 차트 (treemap.json)
- ✅ AI 인사이트 (ai_insights/insights_data_*.json)

---

## 📝 사용 방법

```cmd
# 1단계: 전처리 (C:\ke30 파일 읽기)
당년데이터_처리실행.bat

# 2단계: 대시보드 데이터 가공 및 JSON 생성
대시보드_데이터_가공_JSON생성.bat 20251124
```

---

## ⚠️ 주의사항

1. **전처리 필수**: 배치 파일 실행 전에 `당년데이터_처리실행.bat`를 먼저 실행해야 합니다.
2. **날짜 형식**: YYYYMMDD 형식으로 입력 (예: 20251124)
3. **에러 처리**: 필수 단계에서 오류 발생 시 중단됩니다.
4. **선택적 단계**: 주간 매출 추세, 재고 분석, AI 인사이트는 실패해도 계속 진행됩니다.


