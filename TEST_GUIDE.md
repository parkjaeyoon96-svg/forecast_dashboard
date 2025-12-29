# Dashboard 리팩토링 테스트 가이드

## 📋 개요

Dashboard.html이 다음과 같이 개선되었습니다:
- ✅ 전역 변수 274개 → 단일 `DashboardState` 객체로 통합
- ✅ 이중 로딩 파이프라인 → 단일 `loadAllDashboardData()` + `dashboardReady` 이벤트
- ✅ 2개 이벤트 (`jsonDataLoaded`, `dataLoaded`) → 1개 이벤트 (`dashboardReady`)
- ✅ 하위 호환성 유지 (기존 코드 동작)

## 🧪 테스트 시나리오

### 1단계: 배치 파일 실행 (데이터 생성)

#### 1-1. 전년/계획 데이터 업데이트
```batch
전년계획_업데이트.bat
```

**확인 사항:**
- ✅ Snowflake 연결 성공
- ✅ `raw/{YYYYMM}/previous_year/*.csv` 생성
- ✅ `raw/{YYYYMM}/plan/*.csv` 생성

#### 1-2. 당년 데이터 처리
```batch
당년데이터_처리실행.bat
```

**입력:**
- Use latest files? → `Y` 또는 `N`
- (N 선택 시) Analysis Month: `202512`
- (N 선택 시) Update Date: `20251223`

**확인 사항:**
- ✅ `raw/{YYYYMM}/current_year/{YYYYMMDD}/ke30_*_전처리완료.csv` 생성
- ✅ `raw/{YYYYMM}/current_year/{YYYYMMDD}/forecast_*_Shop.csv` 생성

#### 1-3. Dashboard JSON 생성
```batch
dashboard_json_gen.bat
```

**입력:**
- Use latest files? → `Y` 또는 `N`
- (N 선택 시) Analysis Month: `202512`
- (N 선택 시) Update Date: `20251223`

**확인 사항:**
- ✅ Step 1-10 모두 "Completed" 표시
- ✅ `public/data/{YYYYMMDD}/*.json` 파일 17개 생성
  - overview_kpi.json
  - overview_by_brand.json
  - overview_pl.json
  - overview_waterfall.json
  - overview_trend.json
  - brand_kpi.json
  - brand_pl.json
  - channel_profit_loss.json
  - channel_pl.json
  - radar_chart.json
  - weekly_trend.json (CSV에서 변환)
  - stock_analysis.json
  - treemap.json
  - brand_plan.json
  - metrics.json
  - ai_insights/insights_data_{YYYYMMDD}.json
  - ai_insights/ai_insights_*_{YYYYMMDD}.json (브랜드별)

---

### 2단계: 프론트엔드 로드 테스트

#### 2-1. 브라우저에서 Dashboard 열기

**URL:**
```
http://localhost:8000/Dashboard.html?date=20251223
```

또는 로컬 서버 실행:
```batch
python -m http.server 8000 --directory public
```

#### 2-2. 브라우저 콘솔 확인 (F12)

**정상 로그 예시:**
```
[DashboardState] 단일 상태 객체 초기화 완료
[데이터로드] JSON 로드 시작: 20251223
[데이터로드] baseUrl: /data/20251223
[데이터로드] overview_kpi.json 응답: 200 true
[데이터로드] overview_by_brand.json 응답: 200 true
[데이터로드] overview_pl.json 응답: 200 true
...
[데이터로드] ✅ 모든 데이터 로드 완료, dashboardReady 이벤트 발생
[Dashboard] ★ dashboardReady 이벤트 수신 - 초기화 시작 ★
```

#### 2-3. 콘솔에서 상태 확인

```javascript
// 단일 상태 객체 확인
window.DashboardState

// 데이터 로드 확인
window.DashboardState.meta.dataLoaded  // true여야 함

// 개별 데이터 확인
window.DashboardState.data.overview.pl  // 전체 손익계산서
window.DashboardState.data.brands.kpi   // 브랜드 KPI
window.DashboardState.data.channels.treemap  // 트리맵 데이터

// 하위 호환성 확인 (기존 방식도 동작)
window.overviewPL  // DashboardState.data.overview.pl과 동일
window.brandKPI    // DashboardState.data.brands.kpi와 동일
```

#### 2-4. UI 요소 확인

**전체현황 탭:**
- ✅ 손익계산서 테이블 표시
- ✅ KPI 카드 표시 (매출, 이익, 진척율 등)
- ✅ 워터폴 차트 표시
- ✅ 누적 추이 차트 표시

**브랜드 탭:**
- ✅ 브랜드 버튼 클릭 시 데이터 전환
- ✅ 브랜드별 손익계산서 표시
- ✅ 주차별 매출 추세 차트 표시
- ✅ 레이더 차트 표시
- ✅ 채널별 손익 테이블 표시

**채널 탭:**
- ✅ 트리맵 표시 (브랜드 > 채널 > 아이템)
- ✅ 트리맵 클릭 시 드릴다운 동작
- ✅ YoY 데이터 표시

**재고 탭:**
- ✅ 재고 분석 차트 표시
- ✅ 의류 아이템별 재고 비율 표시

**AI 인사이트:**
- ✅ 전체현황 인사이트 표시
- ✅ 브랜드별 인사이트 표시

---

### 3단계: 반복 테스트 (안정성 확인)

#### 3-1. 새로고침 테스트

**방법:**
1. 브라우저에서 `Ctrl + F5` (강제 새로고침) 10회 반복
2. 매번 다음 항목 확인:
   - ✅ 판매율 평균 표시
   - ✅ 트리맵 표시
   - ✅ 브랜드 KPI 카드 표시
   - ✅ 손익계산서 테이블 표시

**예상 결과:**
- 모든 새로고침에서 동일하게 표시되어야 함
- 콘솔 에러 없어야 함

#### 3-2. 브랜드 전환 테스트

**방법:**
1. MLB → DISCOVERY → DUVETICA → SERGIO → SUPRA → MLB KIDS 순서로 클릭
2. 각 브랜드에서 다음 항목 확인:
   - ✅ KPI 카드 데이터 변경
   - ✅ 손익계산서 데이터 변경
   - ✅ 주차별 매출 추세 변경
   - ✅ 레이더 차트 변경
   - ✅ 채널별 손익 변경

**예상 결과:**
- 브랜드 전환 시 즉시 데이터 업데이트
- 깜빡임 없음
- 콘솔 에러 없음

#### 3-3. 탭 전환 테스트

**방법:**
1. 전체현황 → 브랜드 → 채널 → 재고 → AI 인사이트 순서로 클릭
2. 각 탭에서 데이터 표시 확인

**예상 결과:**
- 모든 탭에서 정상 표시
- 탭 전환 시 지연 없음

---

### 4단계: 문제 발생 시 디버깅

#### 4-1. 데이터가 표시되지 않는 경우

**브라우저 콘솔 확인:**
```javascript
// 데이터 로드 상태 확인
window.DashboardState.meta.dataLoaded

// 특정 데이터 확인
window.DashboardState.data.overview.pl
window.DashboardState.data.brands.kpi

// 네트워크 탭에서 JSON 파일 로드 확인
// 모든 파일이 200 OK여야 함
```

**일반적인 원인:**
1. JSON 파일이 생성되지 않음 → `dashboard_json_gen.bat` 재실행
2. 날짜 파라미터 불일치 → URL의 `?date=` 파라미터 확인
3. 서버 경로 문제 → 로컬 서버 재시작

#### 4-2. 판매율 평균이 표시되지 않는 경우

**확인 사항:**
```javascript
// weekly_trend.json 로드 확인
window.DashboardState.data.weekly.salesTrend

// 또는 (하위 호환)
window.weeklySalesTrend
```

**해결 방법:**
1. `dashboard_json_gen.bat` Step 1.5 확인
2. `raw/{YYYYMM}/current_year/{YYYYMMDD}/weekly_sales_trend_{YYYYMMDD}.csv` 존재 확인
3. 브라우저 새로고침

#### 4-3. 트리맵이 표시되지 않는 경우

**확인 사항:**
```javascript
// treemap.json 로드 확인
window.DashboardState.data.channels.treemap

// 또는 (하위 호환)
window.channelItemSalesData
window.channelTreemapData
```

**해결 방법:**
1. `dashboard_json_gen.bat` Step 8 확인
2. `public/data/{YYYYMMDD}/treemap.json` 존재 확인
3. JSON 파일 내용 확인 (비어있지 않은지)
4. 브라우저 새로고침

---

## 🎯 성공 기준

### ✅ 모든 테스트 통과 조건

1. **배치 파일 실행**
   - 3개 배치 모두 에러 없이 완료
   - JSON 파일 17개 생성

2. **프론트엔드 로드**
   - 페이지 로드 시 콘솔 에러 없음
   - `dashboardReady` 이벤트 1회만 발생
   - 모든 UI 요소 정상 표시

3. **안정성**
   - 10회 새로고침 시 항상 동일하게 표시
   - 브랜드/탭 전환 시 즉시 반응
   - 판매율 평균, 트리맵 항상 표시

4. **하위 호환성**
   - `window.overviewPL` 등 기존 변수 접근 가능
   - 기존 코드 수정 없이 동작

---

## 🔄 롤백 방법

문제 발생 시 이전 버전으로 되돌리기:

```bash
git checkout HEAD -- public/Dashboard.html
```

또는 Git GUI에서:
1. `public/Dashboard.html` 우클릭
2. "Discard Changes" 선택

---

## 📝 변경 사항 요약

### 수정된 파일
- `public/Dashboard.html` (13,389 라인)

### 주요 변경 내용

1. **라인 14-160**: 단일 상태 객체 `DashboardState` 도입
   - 전역 변수 13개 → 1개 객체로 통합
   - 하위 호환성 getter/setter 추가

2. **라인 685-695**: 이벤트 통일
   - `jsonDataLoaded` → `dashboardReady`
   - 상태 정보를 이벤트 detail에 포함

3. **라인 2072-2508**: DOMContentLoaded 단순화
   - 중복 로딩 로직 제거
   - `dataLoaded` 이벤트 발생 제거

4. **라인 12280, 12362**: 이벤트 리스너 업데이트
   - `jsonDataLoaded` → `dashboardReady`
   - 상태 객체 참조로 변경

### 변경되지 않은 파일
- `전년계획_업데이트.bat`
- `당년데이터_처리실행.bat`
- `dashboard_json_gen.bat`
- `scripts/*.py` (모든 Python 스크립트)

---

## 🚀 다음 단계

1. 이 가이드에 따라 전체 파이프라인 테스트
2. 문제 발견 시 콘솔 로그 캡처
3. 1주일간 실제 업무에서 사용하며 안정성 확인
4. 문제 없으면 다른 팀원에게 공유

---

**작성일**: 2025-12-23  
**버전**: 2.0 (리팩토링 완료)



