# AI 인사이트 생성 전체 프로세스

## 📋 개요

전체현황과 브랜드별 분석의 AI 인사이트를 생성하는 전체 프로세스입니다.

---

## 🔧 두 개의 Python 스크립트

### 1. `scripts/generate_ai_insights.py` (메인 스크립트)
**역할**: 실제 AI 인사이트 분석 생성

#### 기능
- ✅ 전체 현황 인사이트 생성 (`--overview`)
- ✅ 브랜드별 인사이트 생성 (`--brand` 또는 `--all-brands`)
- ✅ 로컬 분석 (규칙 기반) 또는 OpenAI API 분석
- ✅ 각 그래프별 인사이트 생성:
  - 손익계산서 분석 (PL)
  - 트리맵 분석 (채널/아이템별 매출구성)
  - 레이더 차트 분석 (매출 계획/전년비)
  - 채널별 손익 분석
  - 주차별 매출추세 분석
  - 재고주수 분석
  - 판매율 분석

#### 생성되는 파일
개별 파일로 생성:
- `insights_data_overview_YYYYMMDD.json` - 전체 현황
- `insights_data_MLB_YYYYMMDD.json` - MLB 브랜드
- `insights_data_MLB_KIDS_YYYYMMDD.json` - MLB KIDS 브랜드
- `insights_data_DISCOVERY_YYYYMMDD.json` - DISCOVERY 브랜드
- `insights_data_DUVETICA_YYYYMMDD.json` - DUVETICA 브랜드
- `insights_data_SERGIO_YYYYMMDD.json` - SERGIO 브랜드
- `insights_data_SUPRA_YYYYMMDD.json` - SUPRA 브랜드

#### 사용법
```bash
# 전체 현황만 생성
python scripts/generate_ai_insights.py --date 20251117 --overview

# 특정 브랜드만 생성
python scripts/generate_ai_insights.py --date 20251117 --brand MLB

# 모든 브랜드 생성
python scripts/generate_ai_insights.py --date 20251117 --all-brands

# 전체 현황 + 모든 브랜드
python scripts/generate_ai_insights.py --date 20251117 --overview --all-brands
```

---

### 2. `scripts/merge_insights_data.py` (통합 스크립트)
**역할**: 개별 파일들을 하나의 통합 파일로 병합

#### 기능
- ✅ 개별 브랜드 파일들을 읽어서
- ✅ 하나의 통합 파일(`insights_data_YYYYMMDD.json`)로 병합
- ✅ HTML 대시보드에서 바로 사용 가능한 형식으로 생성

#### 생성되는 파일
- `insights_data_YYYYMMDD.json` - 모든 브랜드 + 전체 현황 통합 파일

#### 사용법
```bash
python scripts/merge_insights_data.py --date 20251117
```

---

## 📊 전체 프로세스 순서

### Step 1: 기본 JSON 데이터 생성
먼저 대시보드에 필요한 모든 JSON 파일을 생성해야 합니다.

```bash
# 대시보드_JSON생성.bat 실행
# 또는
python scripts/generate_dashboard_data.py
```

생성되는 파일:
- `overview_kpi.json`
- `overview_pl.json`
- `overview_by_brand.json`
- `brand_kpi.json`
- `brand_pl.json`
- `treemap.json`
- `radar_chart.json`
- `channel_pl.json`
- `weekly_trend.json`
- `stock_analysis.json`

---

### Step 2: AI 인사이트 생성 (개별 파일)

#### 2-1. 전체 현황 인사이트 생성
```bash
python scripts/generate_ai_insights.py --date 20251117 --overview
```

**결과:**
- `public/data/20251117/ai_insights/insights_data_overview_20251117.json` 생성

**포함 내용:**
- `content`: 주요내용 (줄글 형태)
- `keyPoints`: 핵심인사이트
- `plInsight`: 손익계산서 분석
- `treemapInsight`: 트리맵 분석
- `radarInsight`: 레이더 차트 분석
- `weeklyInsight`: 주차별 매출추세 분석
- `inventoryInsight`: 재고주수 분석
- `saleRateInsight`: 판매율 분석

#### 2-2. 브랜드별 인사이트 생성
```bash
python scripts/generate_ai_insights.py --date 20251117 --all-brands
```

**결과:**
- `public/data/20251117/ai_insights/insights_data_MLB_20251117.json`
- `public/data/20251117/ai_insights/insights_data_MLB_KIDS_20251117.json`
- `public/data/20251117/ai_insights/insights_data_DISCOVERY_20251117.json`
- `public/data/20251117/ai_insights/insights_data_DUVETICA_20251117.json`
- `public/data/20251117/ai_insights/insights_data_SERGIO_20251117.json`
- `public/data/20251117/ai_insights/insights_data_SUPRA_20251117.json`

**각 브랜드 파일 포함 내용:**
- `content`: 주요내용
- `keyPoints`: 핵심인사이트
- `treemapInsight`: 채널/아이템별 매출구성
- `radarInsight`: 매출 계획/전년비
- `channelPlInsight`: 주요 채널별 손익 분석
- `weeklyInsight`: 주차별 매출 추세
- `saleRateInsight`: 판매율 분석
- `inventoryInsight`: 재고주수 분석
- `part1`: 손익계산서 분석

**또는 한 번에:**
```bash
python scripts/generate_ai_insights.py --date 20251117 --overview --all-brands
```

---

### Step 3: 통합 파일 생성
개별 파일들을 하나의 통합 파일로 병합합니다.

```bash
python scripts/merge_insights_data.py --date 20251117
```

**결과:**
- `public/data/20251117/ai_insights/insights_data_20251117.json` 생성

**파일 구조:**
```json
{
  "overview": { ... },
  "MLB": { ... },
  "MLB_KIDS": { ... },
  "DISCOVERY": { ... },
  "DUVETICA": { ... },
  "SERGIO": { ... },
  "SUPRA": { ... }
}
```

이 파일이 HTML 대시보드에서 로드되어 사용됩니다.

---

## 🎯 실제 실행 예시

### 방법 1: 단계별 실행

```bash
# 1. 전체 현황 생성
python scripts/generate_ai_insights.py --date 20251117 --overview

# 2. 모든 브랜드 생성
python scripts/generate_ai_insights.py --date 20251117 --all-brands

# 3. 통합 파일 생성
python scripts/merge_insights_data.py --date 20251117
```

### 방법 2: 한 번에 실행 (권장)

```bash
# 전체 현황 + 모든 브랜드 한 번에 생성
python scripts/generate_ai_insights.py --date 20251117 --overview --all-brands

# 통합 파일 생성
python scripts/merge_insights_data.py --date 20251117
```

---

## ⚠️ 중요 사항

### 1. `generate_ai_insights.py`는 통합 파일도 생성합니다!

`generate_ai_insights.py`를 `--overview --all-brands` 옵션으로 실행하면:
- 개별 파일들을 생성하고
- **자동으로 통합 파일(`insights_data_YYYYMMDD.json`)도 생성**합니다.

따라서 **대부분의 경우 `merge_insights_data.py`를 별도로 실행할 필요가 없습니다!**

### 2. `merge_insights_data.py`가 필요한 경우

다음과 같은 상황에서만 필요합니다:
- 개별 파일을 수동으로 편집한 후 다시 통합하고 싶을 때
- 통합 파일이 누락되었거나 손상되었을 때
- 특정 브랜드만 다시 생성한 후 통합 파일을 업데이트하고 싶을 때

### 3. 실행 순서 요약

**일반적인 경우 (권장):**
```bash
# 한 번에 실행
python scripts/generate_ai_insights.py --date 20251117 --overview --all-brands
```
→ 개별 파일 + 통합 파일 모두 자동 생성됨 ✅

**개별 파일 수정 후 재통합이 필요한 경우:**
```bash
# 통합 파일만 다시 생성
python scripts/merge_insights_data.py --date 20251117
```

---

## 📁 생성되는 파일 구조

```
public/data/20251117/ai_insights/
├── insights_data_overview_20251117.json      (전체 현황)
├── insights_data_MLB_20251117.json          (MLB 브랜드)
├── insights_data_MLB_KIDS_20251117.json     (MLB KIDS 브랜드)
├── insights_data_DISCOVERY_20251117.json    (DISCOVERY 브랜드)
├── insights_data_DUVETICA_20251117.json     (DUVETICA 브랜드)
├── insights_data_SERGIO_20251117.json       (SERGIO 브랜드)
├── insights_data_SUPRA_20251117.json        (SUPRA 브랜드)
└── insights_data_20251117.json              (통합 파일) ⭐ 대시보드에서 사용
```

---

## 🔍 각 파일의 역할 비교

| 파일 | 역할 | 언제 실행? |
|------|------|-----------|
| `generate_ai_insights.py` | AI 인사이트 분석 생성 | 새 데이터로 인사이트 생성할 때 |
| `merge_insights_data.py` | 개별 파일 통합 | 개별 파일 수정 후 재통합이 필요할 때만 |

---

## ✅ 체크리스트

인사이트 생성 전 확인사항:

- [ ] 기본 JSON 파일들이 생성되어 있는가?
  - `overview_kpi.json`
  - `overview_pl.json`
  - `brand_kpi.json`
  - `treemap.json`
  - `radar_chart.json`
  - `channel_pl.json`
  - `weekly_trend.json`
  - `stock_analysis.json`

- [ ] 날짜 형식이 올바른가? (YYYYMMDD, 예: 20251117)

- [ ] 출력 디렉토리가 존재하는가?
  - `public/data/YYYYMMDD/ai_insights/`

---

## 📝 최종 요약

**대부분의 경우 이 한 줄만 실행하면 됩니다:**

```bash
python scripts/generate_ai_insights.py --date 20251117 --overview --all-brands
```

이 명령으로:
1. 전체 현황 인사이트 생성 ✅
2. 모든 브랜드별 인사이트 생성 ✅
3. 통합 파일 자동 생성 ✅

**`merge_insights_data.py`는 특별한 경우에만 사용합니다!**

