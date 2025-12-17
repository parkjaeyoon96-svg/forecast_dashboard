# 트리맵 데이터 생성 가이드

## 📋 개요

브랜드별 분석 화면의 채널별/아이템별 트리맵 데이터를 생성하는 파이프라인입니다.

## 🔄 데이터 흐름

```
1. 스노우플레이크 다운로드
   ↓
   (download_treemap_rawdata.py)
   ↓
   treemap_raw_YYYYMMDD.csv
   
2. 전처리
   ↓
   (preprocess_treemap_data.py)
   - 채널명 매핑
   - 아이템 마스터 매핑
   - 아이템_중분류 생성 (시즌 로직)
   ↓
   treemap_preprocessed_YYYYMMDD.csv
   
3. 트리맵 JSON 생성
   ↓
   (create_treemap_data_v2.py)
   - 채널별/아이템별 집계
   - 할인율 계산
   - YOY 계산
   ↓
   public/data/YYYYMMDD/treemap.json
```

## 🚀 사용 방법

### 방법 1: 전체 파이프라인 실행 (권장)

```bash
cd scripts
python run_treemap_pipeline.py 20251215
```

### 방법 2: 단계별 실행

```bash
# Step 1: 원본 데이터 다운로드
python download_treemap_rawdata.py 2024-12-02 2024-12-15

# Step 2: 데이터 전처리
python preprocess_treemap_data.py treemap_raw_20251215.csv --date 20251215

# Step 3: 트리맵 JSON 생성
python create_treemap_data_v2.py 20251215
```

## 📊 데이터 구조

### 1. 스노우플레이크 쿼리

- **테이블**: `fnf.sap_fnf.dw_copa_d`
- **기간**: 동주차 월요일 ~ 해당일
- **주요 컬럼**:
  - 브랜드코드, 시즌, 채널코드, 고객코드
  - prdt_hrrc_cd1, prdt_hrrc_cd2, prdt_hrrc_cd3
  - 아이템코드
  - TAG매출, 실판매출

### 2. 전처리 로직

#### 채널명 매핑
- **RF 판정**: 고객코드가 채널마스터의 SAP_CD에 있으면 RF
- **일반 채널**: 채널코드로 채널명 매핑

#### 아이템 마스터 매핑
- PH01-2, PH01-3를 키로 조인
- PRDT_HRRC2_NM, PRDT_HRRC3_NM 가져오기

#### 아이템_중분류 생성
**의류(E0300)인 경우 - 시즌 로직 적용:**

| 시즌 구분 | 기간 |
|-----------|------|
| SS시즌 | 3월 ~ 8월 |
| FW시즌 | 9월 ~ 내년 2월 |

**분류 로직:**
- **당시즌의류**: 현재 시즌과 동일 (예: 25F → 당시즌)
- **과시즌의류**: 현재 시즌 이전 (예: 25S, 24F → 과시즌)
- **차시즌의류**: 현재 시즌 초과 (예: 26S, 26F → 차시즌)
- **N 시즌**: 년도만 비교 (예: 25N → 당시즌, 24N → 과시즌)

**ACC(E0200)인 경우:**
- PRDT_HRRC2_NM 반환 (예: Headwear, Bag, Shoes)

### 3. 최종 출력 구조

#### treemap.json
```json
{
  "channelTreemapData": {
    "total": { "tag": 0, "sales": 0, "discountRate": 0, "yoy": 0 },
    "channels": {
      "RF": {
        "tag": 0,
        "sales": 0,
        "share": 0,
        "discountRate": 0,
        "yoy": 0,
        "itemCategories": {
          "Headwear": {
            "tag": 0,
            "sales": 0,
            "share": 0,
            "discountRate": 0,
            "yoy": 0,
            "subCategories": {
              "비니": { "tag": 0, "sales": 0, "share": 0, "discountRate": 0 }
            }
          }
        }
      }
    },
    "byBrand": {
      "MLB": {
        "channel": { ... },
        "item": { ... }
      }
    }
  },
  "itemTreemapData": {
    "total": { ... },
    "items": {
      "Headwear": {
        "tag": 0,
        "sales": 0,
        "share": 0,
        "discountRate": 0,
        "yoy": 0,
        "channels": {
          "RF": { "tag": 0, "sales": 0, "share": 0, "discountRate": 0, "yoy": 0 }
        }
      }
    }
  }
}
```

## 📝 할인율 계산

**전체 할인율 방식:**
```
할인율 = (TAG매출 - 실판매출) / TAG매출 × 100
```

**예시:**
- TAG매출: 6,063,130,000원
- 실판매출: 5,878,580,749원
- 할인율: (6,063,130,000 - 5,878,580,749) / 6,063,130,000 × 100 = **3.04% ≈ 3.0%**

## 📈 YOY 계산

```
YOY = (당년 실판매출 - 전년 실판매출) / 전년 실판매출 × 100
```

**전년 데이터 경로:**
```
raw/YYYYMM/previous_year/treemap_preprocessed_prev_YYYYMMDD.csv
```

## ⚠️ 주의사항

1. **마스터 파일 필수**:
   - `master/channel_master.csv`
   - `master/item_master.csv`

2. **전년 데이터**:
   - YOY 계산을 위해 전년 동주차 데이터 필요
   - 없으면 YOY는 null로 표시

3. **시즌 기준일**:
   - 전처리 시 지정한 날짜를 기준으로 시즌 분류
   - 보통 종료일(해당일)을 기준으로 사용

## 🔍 디버깅

### 데이터 확인
```bash
# 원본 데이터 행 수 확인
wc -l raw/202512/treemap_raw_20251215.csv

# 전처리 데이터 샘플 확인
head -20 raw/202512/treemap_preprocessed_20251215.csv

# JSON 구조 확인
python -m json.tool public/data/20251215/treemap.json | head -50
```

### 할인율 검증
```python
import pandas as pd

df = pd.read_csv('raw/202512/treemap_preprocessed_20251215.csv')

# MLB Headwear 할인율 계산
mlb_headwear = df[(df['브랜드'] == 'MLB') & (df['아이템_중분류'] == 'Headwear')]
tag_total = mlb_headwear['TAG매출'].sum()
sales_total = mlb_headwear['실판매출'].sum()
discount_rate = (tag_total - sales_total) / tag_total * 100

print(f"TAG: {tag_total:,}")
print(f"실판매출: {sales_total:,}")
print(f"할인율: {discount_rate:.2f}%")
```

## 📞 문의

문제 발생 시:
1. 로그 확인
2. 중간 파일(raw, preprocessed) 확인
3. 마스터 파일 업데이트 여부 확인




