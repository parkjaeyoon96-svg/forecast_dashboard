# 월중 손익예측 대시보드 — PRD (현재 기준)

> **작성일**: 2026년 2월 4일  
> **기준**: 현재 구현된 시스템 상태를 반영한 제품 요구사항 문서

---

## Ver1. 문제 정의

### 1. 현재 상황

- 당사의 **월 손익 보고**는 **마감 후 익월 15~17일경**에 이루어짐.
- 예: 8월 손익 자료는 9월 중순 이후 확인 가능 → **보고 시점의 시의성 부족**.
- 회계 마감 이전에는 **매출·비용 등 주요 손익 데이터를 월중 기준으로 종합 파악하기 어려움**.

---

### 2. 불편 사항

- **보고 시점 지연**: 월중 손익 현황을 바로 파악하기 어려워 **신속한 대응이 힘듦**.
- **의사결정 지연**: 매출 부진, 비용 급증 등을 **사후에만** 인지 가능.
- **운영 효율성 저하**: 경영진/실무진의 **빠른 의사결정 지원**이 제한적.
- **데이터 단절**: 회계 마감 전 데이터를 활용한 **집계·분석 체계가 없음**.

---

### 3. 목표 설정

- **프로젝트 비전**  
  “월중 손익을 **가능한 한 빠르게** 모니터링할 수 있는 시스템을 구축하여, 경영진의 **신속·정확한 의사결정**을 지원한다.”

- **추진 목표**
  - **시의성 확보**: 회계 마감 전에도 **월중 손익 지표** 확인 가능.
  - **선제적 대응 지원**: 이상 징후 조기 감지 및 **경영진 보고 체계** 강화.
  - **표준화**: 손익 집계 로직 **일관성** 확보, 데이터 **신뢰도** 향상.
  - **접근성 강화**: **직관적인 대시보드**로 누구나 활용 가능.

---

## Ver2. 솔루션 설계 (현재 구현 반영)

### 1. 핵심 개요

- **문제**: 월중 데이터 집계 → 예상 손익 산출 → 대시보드 반영까지 **매번 다단계 수작업** 필요.
- **해결**:  
  **Python ETL + 손익 로직** → **JSON 생성** → **정적/API 데이터** → **대시보드(HTML/Next.js)**  
  (데이터 수집·계산·변환·대시보드 반영은 **스크립트/배치 실행**으로 자동화, **사업부별 알림 발송은 미구현**.)

---

### 2. 도구 선정 (현재 사용 중)

| 단계 | 도구 | 용도 |
|------|------|------|
| **데이터 수집** | Python (pandas, openpyxl) | KE30 Excel → CSV 변환, `raw/` 폴더 내 월중·전년·계획 데이터 로드 |
| **추가 데이터** | Snowflake (Node.js SDK) | 트리맵/매출구성·할인내역·프로모션·재고주수 등 **실시간 조회** |
| **손익 계산** | pandas | 매출총이익·직접비·직접이익·월말 예상(forecast) 로직 적용 |
| **변환/전송** | Python → JSON | `public/data/YYYYMMDD/` 에 브랜드별·채널별·전체현황 JSON 생성 |
| **대시보드** | HTML + Chart.js/Plotly, Next.js | 정적 JSON + Next.js API(`/api/list-dates`, `/api/load-data-by-date`, Snowflake 등)로 데이터 공급 |
| **알림** | — | **미구현** (Slack/Teams/메일 등 사업부별 맞춤 알림 없음) |

---

### 3. 기능 정의 (현재 구현 범위)

1. **데이터 로딩**
   - **KE30**: `C:\ke30\ke30_YYYYMMDD_YYYYMM.xlsx` 수동 배치 후 → CSV 변환·전처리.
   - **raw 폴더**: `raw/YYYYMM/current_year/YYYYMMDD/`, `plan/`, `previous_year/`, `ETC/` 구조에서 **최신 또는 지정일** 데이터 탐색·적재.
   - **Snowflake**: Next.js API 경유로 트리맵·매출구성·할인·프로모션·재고주수 등 조회.

2. **손익·예측 계산**
   - **현시점 실적**: KE30 전처리 → 채널/아이템 집계 → 직접비 반영 → **직접이익** 산출.
   - **월말 예상**: `forecast_*_Shop.csv` 생성(진척률 기반 월말 추정).
   - **전년·계획 대비**: 전년 동월·계획 데이터와 비교한 KPI(매출·직접이익·진척률 등) 계산.

3. **데이터 가공**
   - 브랜드별/채널별/전체현황 JSON → `public/data/YYYYMMDD/` 에 저장.
   - `export_to_json.py`: 대시보드용 data.js → JSON 파일로 변환.

4. **대시보드 갱신**
   - **날짜 선택**: `?date=YYYYMMDD` 또는 Next.js 날짜 선택 → 해당일 JSON + API로 로드.
   - Chart.js/Plotly로 KPI·차트 실시간 표시(데이터 갱신은 **스크립트 재실행** 후 반영).

5. **알림 발송**
   - **미구현**: 사업부별 필터링·Slack/Teams/메일 발송 없음.

---

## Ver3. 완성 설계 (현재 아키텍처)

### 1. 최종 데이터/처리 흐름

```
[KE30 Excel] → 수동 배치 → [raw/YYYYMM/current_year/YYYYMMDD/]
                                    ↓
[Python ETL] process_ke30_full_pipeline → 전처리·직접비·집계
                                    ↓
[convert_ke30_to_forecast] → forecast_*_Shop.csv (월말 예상)
                                    ↓
[generate_dashboard_data] → update_brand_kpi, update_overview_data,
                            create_brand_pl_data, process_channel_profit_loss,
                            download_weekly_sales_trend, (재고/트리맵), export_to_json,
                            generate_ai_insights
                                    ↓
[JSON] public/data/YYYYMMDD/*.json
                                    ↓
[대시보드] Dashboard.html / Next.js → /data/{date}/ + /api/* (Snowflake 등)
                                    ↓
[알림] 미구현
```

---

### 2. 데이터 수집·전처리

| 구분 | 경로/스크립트 | 설명 |
|------|----------------|------|
| **KE30 입력** | `C:\ke30\ke30_YYYYMMDD_YYYYMM.xlsx` | SAP PA 1000 기준 월중 데이터 (수동 저장) |
| **전처리** | `scripts/process_ke30_full_pipeline.py` | CSV 변환, 스키마 표준화, 브랜드·채널·아이템 집계, 직접비 계산 |
| **raw 구조** | `raw/YYYYMM/current_year/YYYYMMDD/` | ke30_*_Shop.csv, forecast_*_Shop.csv, metadata.json 등 |
| **계획/전년** | `raw/YYYYMM/plan/`, `previous_year/` | 계획 전처리, 전년 동월 데이터 |

- **폴더 모니터링(watchdog)** 은 사용하지 않음. **배치(`당년데이터_처리실행.bat`) 또는 스크립트 직접 실행**으로 처리.

---

### 3. 손익 계산

| 모듈/스크립트 | 역할 |
|----------------|------|
| **process_ke30_full_pipeline.py** | 매출총이익 = 매출 − 원가(평가감 반영), 직접이익 = 매출총이익 − 직접비(마스터 기반) |
| **convert_ke30_to_forecast.py** | 현재 실적 + 진척률 기반 **월말 예상** 산출 → forecast_*_Shop.csv |
| **process_channel_profit_loss.py** | 채널별 당년/전년/계획 손익, 대시보드용 `channel_profit_loss.json` |
| **create_overview_kpi.py** | 브랜드별 KPI 합산 → `overview_kpi.json` |
| **update_brand_kpi.py** | 브랜드별 실판매출·직접이익·직접이익율·할인율·목표대비 진척률 → brand_kpi |

---

### 4. 데이터 변환·Export

| 구분 | 스크립트 | 출력 |
|------|----------|------|
| **대시보드 JSON** | `export_to_json.py` | `public/data/YYYYMMDD/` 하위 JSON (overview_*, brand_*, channel_*, weekly_trend, treemap 등) |
| **구조** | 브랜드별·채널별·전체현황 | overview_kpi, overview_pl, overview_by_brand, channel_profit_loss, brand_kpi 등 |

- **REST API `/api/dashboard`** 형태의 단일 엔드포인트는 없음.  
  **날짜별 JSON 디렉터리** + **Next.js API**(`list-dates`, `load-data-by-date`, Snowflake 쿼리 등)로 제공.

---

### 5. 대시보드 연동

| 구분 | 기술 | 비고 |
|------|------|------|
| **프론트** | `public/Dashboard.html` (Chart.js, Plotly) | 정적 HTML, `/data/{date}/*.json` fetch |
| **앱** | Next.js (app/dashboard, app/sales-rate 등) | 동일 데이터 소스 + API |
| **API** | `/api/list-dates`, `/api/load-data-by-date`, `/api/snowflake/query` 등 | 날짜 목록, 데이터 로드, Snowflake 실시간 조회 |
| **배포** | Vercel | 자동 배포 |

- **실시간 KPI·차트**: 데이터 갱신은 **파이프라인 재실행 → JSON 갱신** 후, 새로고침 또는 날짜 변경 시 반영.

---

### 6. 알림 전송

- **현재**: **미구현**.
- **원 설계**: 사업부 코드별 데이터 필터링 후 Slack/Teams/메일 발송.
- **추가 시**: `notifier.py` 또는 별도 서비스로 구현 필요.

---

### 7. 자동화·운영

| 구분 | 내용 |
|------|------|
| **당년 파이프라인** | `run_current_year_pipeline.py` (KE30 전처리 → forecast 변환) |
| **대시보드 데이터 일괄 생성** | `generate_dashboard_data.py <YYYYMMDD>` (KPI·overview·channel·treemap·JSON·AI 인사이트까지 일괄) |
| **배치** | `당년데이터_처리실행.bat` (최신 또는 지정 분석월/업데이트일) |
| **GitHub Actions** | `weekly-update.yml` — 매주 월요일 9시(KST), raw 변경 시 `generate_weekly_pages.py` 실행 |
| **AI 인사이트** | `generate_ai_insights.py` (전체현황·브랜드별), OpenAI 또는 로컬 규칙 기반 |

---

## 예상 결과 (현재 체계 기준)

- **시간 단축**: 수작업 집계 대비 **스크립트 실행 한 번**으로 전 단계 처리 (전처리 ~ JSON·AI 인사이트).
- **정확성**: 동일 로직·마스터 기반 계산으로 **일관성·신뢰도** 확보.
- **시의성**: 회계 마감 전 **월중 실적·월말 예상**을 대시보드에서 확인 가능.
- **접근성**: 날짜 선택만으로 **전체/브랜드별/채널별** KPI·차트 확인.
- **미구현**: **사업부별 맞춤 알림(Slack/Teams/메일)** 은 추후 도입 시 보완.

---

## 문서 이력

| 버전 | 일자 | 비고 |
|------|------|------|
| 1.0 | 2026-02-04 | 현재 구현 기준 PRD 최초 작성 (Ver1~3 통합) |
