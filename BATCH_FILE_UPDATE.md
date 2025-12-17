# dashboard_json_gen.bat 업데이트 완료 ✅

## 📋 변경 내용

트리맵 데이터 생성 로직을 새로운 파이프라인으로 완전히 교체했습니다.

## 🔄 배치 파일 실행 순서

### 전체 파이프라인

```batch
dashboard_json_gen.bat
```

**실행 단계:**

1. **Step 1**: `update_brand_kpi.py` - 브랜드별 KPI 업데이트
2. **Step 1.5**: `download_weekly_sales_trend.py` - 주간 매출 트렌드 다운로드
3. **Step 2**: `update_overview_data.py` - 개요 데이터 업데이트
4. **Step 3**: `create_brand_pl_data.py` - 브랜드별 손익 데이터 생성
5. **Step 4**: `update_brand_radar.py` - 브랜드 레이더 차트 업데이트
6. **Step 5**: `process_channel_profit_loss.py` - 채널별 손익 처리
7. **Step 6**: `download_brand_stock_analysis.py` - 재고 분석 다운로드
8. **Step 7-Post**: `generate_brand_stock_analysis.py` - 재고 분석 집계 생성

### 🆕 트리맵 파이프라인 (새로 추가/수정)

9. **Step 7.5**: `download_previous_year_treemap.py` - **전년 데이터 다운로드**
   - 전년 동주차 스노우플레이크 쿼리
   - 전년 데이터 전처리
   - YOY 계산용 데이터 준비
   - ⚠️ 실패해도 계속 진행 (YOY는 null로 표시)

10. **Step 8**: `run_treemap_pipeline.py` - **트리맵 통합 파이프라인**
    - **Step 8-1**: `download_treemap_rawdata.py` 
      - 스노우플레이크에서 당년 동주차 데이터 다운로드
    - **Step 8-2**: `preprocess_treemap_data.py`
      - 채널명 매핑 (RF 판정 포함)
      - 아이템 마스터 매핑
      - 아이템_중분류 생성 (시즌 로직)
    - **Step 8-3**: `create_treemap_data_v2.py`
      - 채널별/아이템별 집계
      - 할인율 계산
      - YOY 계산
      - JSON 생성

### 나머지 단계

11. **Step 9**: `export_to_json.py` - JSON 내보내기
12. **Step 10**: `generate_ai_insights.py` - AI 인사이트 생성

## 📊 트리맵 파이프라인 상세

### Step 7.5: 전년 데이터 다운로드

```batch
scripts\download_previous_year_treemap.py 20251215
```

**수행 작업:**
1. 전년 동일 날짜 계산 (2024-12-15)
2. 동주차 월요일 계산
3. 스노우플레이크 쿼리 실행
4. 전처리 (채널/아이템 매핑, 시즌 분류)
5. 저장: `raw/202512/previous_year/treemap_preprocessed_prev_20251215.csv`

**실패 시:**
- 경고 메시지 출력
- 계속 진행 (YOY 없이)

### Step 8: 당년 트리맵 생성

```batch
scripts\run_treemap_pipeline.py 20251215
```

**수행 작업:**

#### 8-1. 원본 데이터 다운로드
- 스노우플레이크 쿼리 (동주차 월요일~해당일)
- 저장: `raw/202512/current_year/20251215/treemap_raw_20251215.csv`

#### 8-2. 전처리
- 채널명 매핑:
  - 고객코드가 채널마스터 SAP_CD에 있으면 → RF
  - 없으면 채널코드로 채널명 매핑
- 아이템 마스터 매핑:
  - PH01-2, PH01-3 조인
  - PRDT_HRRC2_NM, PRDT_HRRC3_NM 가져오기
- 아이템_중분류 생성:
  - 의류(E0300): 시즌 로직 → 당/과/차시즌의류
  - ACC(E0200): PRDT_HRRC2_NM 사용
- 저장: `raw/202512/current_year/20251215/treemap_preprocessed_20251215.csv`

#### 8-3. JSON 생성
- 채널별 집계 (채널 → 아이템_중분류 → 아이템_소분류)
- 아이템별 집계 (아이템_중분류 → 채널)
- 할인율 계산: `(TAG - 실판매액) / TAG × 100`
- YOY 계산: `(당년 - 전년) / 전년 × 100`
- 저장: `public/data/20251215/treemap.json`

## 🎯 최종 출력물

### 생성되는 파일들

```
raw/202512/
├── current_year/20251215/
│   ├── treemap_raw_20251215.csv              # Step 8-1
│   └── treemap_preprocessed_20251215.csv     # Step 8-2
└── previous_year/
    ├── treemap_raw_prev_20251215.csv         # Step 7.5
    └── treemap_preprocessed_prev_20251215.csv # Step 7.5

public/data/20251215/
└── treemap.json                               # Step 8-3
```

### treemap.json 구조

```json
{
  "channelTreemapData": {
    "total": { "tag": 0, "sales": 0, "discountRate": 0, "yoy": 0 },
    "channels": {
      "RF": {
        "itemCategories": {
          "Headwear": {
            "discountRate": 16.0,  ← JSON 값 그대로 사용
            "yoy": 18.9,
            "subCategories": { ... }
          }
        }
      }
    },
    "byBrand": {
      "MLB": { "channel": {...}, "item": {...} }
    }
  },
  "itemTreemapData": {
    "items": {
      "Headwear": {
        "discountRate": 3.0,  ← 정확한 3.0%!
        "yoy": 28.5,
        "channels": { ... }
      }
    },
    "byBrand": { ... }
  }
}
```

## ✅ 검증

### 배치 파일 실행

```batch
dashboard_json_gen.bat
```

**입력:**
```
Use latest files? (Y/N): Y
```

**출력 확인:**
```
[Step 7.5] Downloading previous year treemap data for YOY calculation
...
[Step 7.5] Completed

[Step 8] Running treemap data pipeline (download, preprocess, generate JSON)
============================================================
Step 1: 원본 데이터 다운로드
============================================================
...
============================================================
Step 2: 데이터 전처리
============================================================
...
============================================================
Step 3: 트리맵 JSON 생성
============================================================
...
✅ 트리맵 파이프라인 완료!
[Step 8] Completed
```

### 할인율 검증

**PowerShell:**
```powershell
# Python으로 CSV 검증
python -c "
import pandas as pd
df = pd.read_csv('raw/202512/current_year/20251215/treemap_preprocessed_20251215.csv')
mlb = df[df['브랜드'] == 'MLB']
hw = mlb[mlb['아이템_중분류'] == 'Headwear']
tag = hw['TAG매출'].sum()
sales = hw['실판매출'].sum()
print(f'Headwear 할인율: {(tag-sales)/tag*100:.2f}%')
"

# JSON 검증
python -c "
import json
with open('public/data/20251215/treemap.json') as f:
    data = json.load(f)
    mlb = data['channelTreemapData']['byBrand']['MLB']
    hw = mlb['item']['items']['Headwear']
    print(f'JSON 할인율: {hw[\"discountRate\"]}%')
"
```

**기대 결과:**
```
Headwear 할인율: 3.04%
JSON 할인율: 3.0%
```

## 🔧 문제 해결

### Step 7.5 실패
**증상:** "Previous year data download failed"

**원인:**
- 스노우플레이크 연결 실패
- 전년 데이터 없음
- 마스터 파일 누락

**해결:**
- YOY 없이 진행됨 (정상)
- JSON의 yoy 필드는 null로 표시

### Step 8 실패

**증상:** "Step 8 Failed"

**원인:**
- 스노우플레이크 연결 실패
- 마스터 파일 누락 (`master/channel_master.csv`, `master/item_master.csv`)
- 전처리 에러

**해결:**
```bash
# 1. 마스터 파일 확인
dir master\channel_master.csv
dir master\item_master.csv

# 2. 수동 실행으로 에러 확인
cd scripts
python run_treemap_pipeline.py 20251215
```

## 📝 주의사항

1. **마스터 파일 필수**
   - `master/channel_master.csv` (SAP_CD, 채널코드, 채널명)
   - `master/item_master.csv` (PH01-2, PH01-3, PRDT_HRRC2_NM, PRDT_HRRC3_NM)

2. **전년 데이터 (선택)**
   - Step 7.5는 선택사항 (실패해도 계속 진행)
   - YOY가 필요하면 첫 실행 전 한 번만 실행

3. **실행 순서**
   - 첫 실행: Step 7.5 → Step 8
   - 이후: Step 8만 실행 (배치 파일 자동)

## 🎉 완료!

이제 `dashboard_json_gen.bat`를 실행하면 모든 새로운 트리맵 로직이 자동으로 실행됩니다!

**결과:**
- ✅ 스노우플레이크 직접 쿼리
- ✅ 채널/아이템 자동 매핑
- ✅ 시즌 자동 분류
- ✅ 정확한 할인율 계산
- ✅ YOY 자동 계산
- ✅ JSON 직접 사용 (HTML 재계산 없음)




