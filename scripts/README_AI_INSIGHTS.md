# AI 인사이트 생성 스크립트

JSON 파일을 읽어서 AI 분석을 생성하는 스크립트입니다.

## 설치

```bash
# 가상환경 활성화
Forcast_venv\Scripts\activate  # Windows
source Forcast_venv/bin/activate  # Linux/Mac

# 패키지 설치
pip install -r scripts/requirements.txt
```

## 사용법

### 1. 특정 브랜드 분석

```bash
python scripts/generate_ai_insights.py --date 20251124 --brand MLB
```

### 2. 모든 브랜드 분석

```bash
python scripts/generate_ai_insights.py --date 20251124 --all-brands
```

### 3. 전체 현황 분석

```bash
python scripts/generate_ai_insights.py --date 20251124 --overview
```

### 4. 전체 현황 + 모든 브랜드 분석

```bash
python scripts/generate_ai_insights.py --date 20251124 --overview --all-brands
```

### 3. OpenAI API 사용 (선택사항)

```bash
# 환경변수로 설정
set OPENAI_API_KEY=your-api-key-here  # Windows
export OPENAI_API_KEY=your-api-key-here  # Linux/Mac

# 또는 명령줄에서 직접 지정
python scripts/generate_ai_insights.py --date 20251124 --brand MLB --api-key your-api-key-here
```

### 4. 로컬 분석만 사용 (OpenAI API 사용 안 함)

```bash
python scripts/generate_ai_insights.py --date 20251124 --brand MLB --use-local
```

## 분석 영역

### 브랜드별 분석

스크립트는 다음 영역별로 JSON 파일을 읽어 AI 분석을 생성합니다:

1. **손익계산서** (`brand_pl.json`)
   - 매출 목표 대비 달성률
   - 전년 대비 성장률
   - 할인율 관리 상태
   - 직접비 효율성
   - 영업이익 달성률

2. **트리맵** (`treemap.json`)
   - 채널별 매출 비중 및 집중도
   - 아이템별 매출 비중 및 다양성
   - 주요 채널/아이템의 성과

3. **레이더 차트** (`radar_chart.json`)
   - 채널별 목표 대비 달성률
   - 전년 대비 성장률
   - 우수 성과 채널 및 개선 필요 채널

4. **채널별 손익** (`channel_pl.json`)
   - 채널별 매출 및 수익성
   - 매출총이익률 분석
   - 고수익 채널 및 저수익 채널

5. **주차별 매출추세** (`weekly_trend.json`)
   - 주차별 매출 추세
   - 전년 대비 성장률
   - 최근 추세 변화

6. **재고주수** (`stock_analysis.json`)
   - 재고주수 높은 상품
   - 전년 대비 재고 변화
   - 재고 관리 개선 필요 상품

7. **판매율** (`stock_analysis.json`)
   - 평균 판매율
   - 판매율 높은/낮은 상품
   - 전년 대비 판매율 변화

### 전체 현황 분석

전체 현황 분석은 다음 JSON 파일들을 통합하여 분석합니다:

1. **전체 KPI** (`overview_kpi.json`)
   - 전체 매출 목표 대비 달성률
   - 전년 대비 성장률
   - 직접이익률 및 영업이익률

2. **전체 손익계산서** (`overview_pl.json`)
   - 전체 손익 구조 분석
   - 목표 대비 달성률

3. **브랜드별 기여도** (`overview_by_brand.json`)
   - 브랜드별 매출 기여도
   - 주요 브랜드 성과

4. **월중누적매출추이** (`overview_trend.json`)
   - 월중 매출 추세
   - 누적 매출 분석

5. **전체 재고** (`overview_stock_analysis.json`)
   - 전체 재고 현황
   - 재고 관리 상태

## 출력 파일

### 개별 브랜드 파일
```
public/data/{date}/ai_insights/ai_insights_{brand}_{date}.json  # 원본 형식
public/data/{date}/ai_insights/insights_data_{brand}_{date}.json  # HTML 호환 형식
```

### 전체 현황 파일
```
public/data/{date}/ai_insights/ai_insights_overview_{date}.json  # 원본 형식
public/data/{date}/ai_insights/insights_data_overview_{date}.json  # HTML 호환 형식
```

### 통합 요약 파일
```
public/data/{date}/ai_insights/ai_insights_summary_{date}.json  # 원본 형식
public/data/{date}/ai_insights/insights_data_{date}.json  # HTML 호환 형식 (대시보드에서 자동 로드)
```

## HTML 연동

생성된 인사이트 파일은 HTML 대시보드에서 자동으로 로드됩니다:

1. **자동 로드**: `insights_data_{date}.json` 파일이 있으면 대시보드에서 자동으로 로드
2. **필드명 매핑**: 
   - `treemapInsight` → 채널별/아이템별 매출구성
   - `radarInsight` → 매출 계획/전년비
   - `channelPlInsight` → 주요 채널별 손익 분석
   - `weeklyInsight` → 주차별 매출 추세
   - `saleRateInsight` → 판매율 분석
   - `inventoryInsight` → 재고주수 분석
   - `part1` → 손익계산서 분석
   - `overview.content` → 전체 현황 내용
   - `overview.keyPoints` → 전체 현황 주요 포인트

## 출력 형식

```json
{
  "brand": "MLB",
  "date": "20251124",
  "generated_at": "2025-11-28T10:00:00",
  "insights": {
    "pl": "<strong>📊 손익계산서 분석</strong><br>• 실판매액...",
    "treemap": "<strong>📊 채널별 매출구성 분석</strong><br>• ...",
    "radar": "<strong>📊 매출 계획/전년비 분석</strong><br>• ...",
    "channelPl": "<strong>📊 주요 채널별 손익 분석</strong><br>• ...",
    "weekly": "<strong>📊 주차별 매출 추세 분석</strong><br>• ...",
    "inventory": "<strong>📊 재고주수 분석</strong><br>• ...",
    "saleRate": "<strong>📊 판매율 분석</strong><br>• ..."
  }
}
```

## 로컬 분석 vs OpenAI API

### 로컬 분석 (기본값)
- OpenAI API 키가 없어도 동작
- 규칙 기반 분석
- 빠른 실행 속도
- 기본적인 인사이트 제공

### OpenAI API 사용
- 더 상세하고 맥락적인 분석
- 실행 가능한 구체적인 제안
- API 키 필요 (유료)

## 예제

```bash
# MLB 브랜드만 분석 (로컬)
python scripts/generate_ai_insights.py --date 20251124 --brand MLB --use-local

# 모든 브랜드 분석 (OpenAI API 사용)
python scripts/generate_ai_insights.py --date 20251124 --all-brands

# 전체 현황 분석
python scripts/generate_ai_insights.py --date 20251124 --overview

# 전체 현황 + 모든 브랜드 분석
python scripts/generate_ai_insights.py --date 20251124 --overview --all-brands

# 특정 출력 디렉토리 지정
python scripts/generate_ai_insights.py --date 20251124 --brand MLB --output-dir ./output
```

## 문제 해결

### OpenAI API 오류
- API 키가 올바른지 확인
- `--use-local` 옵션으로 로컬 분석만 사용

### JSON 파일을 찾을 수 없음
- `public/data/{date}/` 디렉토리에 필요한 JSON 파일이 있는지 확인
- 날짜 형식이 YYYYMMDD인지 확인

### 패키지 설치 오류
```bash
pip install --upgrade pip
pip install -r scripts/requirements.txt
```

