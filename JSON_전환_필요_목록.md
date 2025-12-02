# JSON 전환 필요 목록

## 📋 목표
모든 데이터를 `public/data/{날짜}/` 경로의 JSON 파일로만 사용

## ❌ 현재 JSON이 아닌 것들

### 1. JS 파일로 직접 로드되는 것들

#### 1.1 `treemap_data_v2_{날짜}.js`
- **현재 위치**: `public/treemap_data_v2_20251124.js`
- **대시보드 로드 위치**: 1932줄 (`loadTreemapV2()` 함수)
- **문제**: JS 파일로 직접 로드됨
- **해결 방법**: 
  - ✅ 이미 `public/data/{날짜}/treemap.json` 생성됨
  - ✅ `loadAllDashboardData()`에서 이미 로드함 (151줄)
  - ❌ `loadTreemapV2()` 함수 제거 필요

#### 1.2 `data_{날짜}.js` (중간 파일)
- **현재 위치**: `public/data_20251124.js`
- **용도**: `export_to_json.py`에서 읽어서 JSON으로 변환하는 중간 파일
- **문제**: 직접 로드는 안 하지만 생성됨
- **해결 방법**: 
  - 중간 파일이므로 유지 가능
  - 또는 스크립트를 수정하여 JS 파일 생성 없이 직접 JSON 생성

### 2. 잘못된 경로의 JSON 파일

#### 2.1 `brand_pl_data_{날짜}.json`
- **현재 위치**: `public/brand_pl_data_20251124.json` (루트)
- **대시보드 로드 위치**: 1804줄, 1849줄
- **문제**: 
  - `public/` 루트에 있음 (날짜별 폴더 아님)
  - 하위 호환성을 위해 남아있음
- **해결 방법**: 
  - ✅ 이미 `public/data/{날짜}/brand_pl.json` 생성됨
  - ❌ 대시보드에서 루트 경로 로드 코드 제거 필요
  - ❌ `create_brand_pl_data.py`에서 루트 경로 저장 코드 제거 필요 (900줄)

#### 2.2 `brand_stock_analysis_{날짜}.json`
- **현재 위치**: `public/brand_stock_analysis_20251124.json` (루트)
- **대시보드 로드**: `stock_analysis.json`으로 이미 로드됨 (150줄)
- **문제**: 
  - `public/` 루트에 있음
  - 사용되지 않음 (주석에만 언급)
- **해결 방법**: 
  - ✅ 이미 `public/data/{날짜}/stock_analysis.json` 사용 중
  - ❌ 루트 파일 삭제 가능

### 3. JS 파일 생성 스크립트

#### 3.1 `create_treemap_data_v2.py`
- **생성 파일**: 
  - `public/treemap_data_v2_{날짜}.js` (361줄)
  - `public/data/{날짜}/treemap.json` (377줄) ✅
- **문제**: JS 파일도 생성함
- **해결 방법**: 
  - JS 파일 생성 코드 제거 (361-366줄)
  - JSON 파일만 생성하도록 수정

#### 3.2 `create_brand_pl_data.py`
- **생성 파일**: 
  - `public/brand_pl_data_{날짜}.json` (900줄) ❌
  - `public/data/{날짜}/brand_pl.json` (911줄) ✅
- **문제**: 루트 경로에도 JSON 생성
- **해결 방법**: 
  - 루트 경로 저장 코드 제거 (900-906줄)

#### 3.3 `export_to_json.py`
- **읽는 JS 파일**: 
  - `data_{날짜}.js` (149줄)
  - `weekly_sales_trend_{날짜}.js` (205줄)
  - `brand_stock_analysis_{날짜}.js` (244줄)
- **문제**: JS 파일을 읽어서 JSON으로 변환
- **해결 방법**: 
  - 스크립트를 수정하여 JS 파일 생성 없이 직접 JSON 생성
  - 또는 중간 파일로 유지 (JSON 생성 후 삭제)

## ✅ 이미 JSON으로 전환된 것들

1. `overview_kpi.json` → `public/data/{날짜}/overview_kpi.json`
2. `overview_by_brand.json` → `public/data/{날짜}/overview_by_brand.json`
3. `overview_pl.json` → `public/data/{날짜}/overview_pl.json`
4. `overview_waterfall.json` → `public/data/{날짜}/overview_waterfall.json`
5. `overview_trend.json` → `public/data/{날짜}/overview_trend.json`
6. `brand_kpi.json` → `public/data/{날짜}/brand_kpi.json`
7. `brand_pl.json` → `public/data/{날짜}/brand_pl.json`
8. `channel_profit_loss.json` → `public/data/{날짜}/channel_profit_loss.json`
9. `radar_chart.json` → `public/data/{날짜}/radar_chart.json`
10. `weekly_trend.json` → `public/data/{날짜}/weekly_trend.json`
11. `stock_analysis.json` → `public/data/{날짜}/stock_analysis.json`
12. `treemap.json` → `public/data/{날짜}/treemap.json`
13. `brand_plan.json` → `public/data/{날짜}/brand_plan.json`
14. `ai_insights/insights_data_{날짜}.json` → `public/data/{날짜}/ai_insights/insights_data_{날짜}.json`

## 🔧 수정 필요 사항 요약

### 대시보드 (`Dashboard.html`)
1. ✅ `loadTreemapV2()` 함수 제거 완료
2. ✅ `brand_pl_data_{날짜}.json` 루트 경로 로드 코드 제거 완료
3. ✅ `treemap.json` 우선 사용 (이미 수정됨)

### Python 스크립트
1. ✅ `create_treemap_data_v2.py`: JS 파일 생성 코드 제거 완료
2. ✅ `create_brand_pl_data.py`: 루트 경로 저장 코드 제거 완료
3. ✅ `export_to_json.py`: JS 파일 의존성 선택적 처리 완료 (이미 JSON 파일이 있으면 스킵)

## ✅ 모든 수정 완료

이제 모든 데이터는 **파일 전처리 → 전처리 파일 읽고 py가공 → json으로 저장** 되는 일관된 로직을 따릅니다.

### 배치 파일
- ✅ 현재 배치 파일은 이미 JSON 생성에 맞춰져 있음

## 📝 작업 우선순위

1. **높음**: `treemap_data_v2_*.js` 제거 (대시보드에서 직접 로드)
2. **높음**: `brand_pl_data_*.json` 루트 경로 제거
3. **중간**: JS 파일 생성 스크립트 수정
4. **낮음**: 중간 파일(`data_*.js`) 정리 (선택적)

