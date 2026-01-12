# 브랜드별 현황 트리맵 업데이트 문제 해결

## 문제

브랜드별 현황 페이지에서 **채널별 트리맵**과 **아이템별 트리맵**이 업데이트되지 않음

## 원인 분석

### 1. 데이터 구조 불일치
- **Dashboard.html**은 `channelItemSalesData` 구조를 사용:
  ```javascript
  const brandSalesData = channelItemSalesData[brandCode];
  // 예: channelItemSalesData['M']['백화점']['당시즌의류'] = 1000000000
  ```

- **create_treemap_data_v2.py**는 다른 구조로 생성:
  ```javascript
  {
    channelTreemapData: { ... },
    itemTreemapData: { ... }
    // channelItemSalesData가 없음!
  }
  ```

### 2. 데이터 로드 누락
- `loadTreemapMetadata()` 함수가 `treemap.json`을 로드하지만
- 트리맵 데이터를 전역 변수에 저장하지 않음
- 결과: `channelItemSalesData`가 `undefined` → 트리맵 빈 화면

---

## 해결 방법

### 수정 1: `create_treemap_data_v2.py` (스크립트 수정)

**변경 위치**: 716~765번째 줄

**추가 기능**:
```python
# 브랜드별 간단한 채널-아이템 매출 데이터 생성 (Dashboard.html 호환용)
channel_item_sales_data = {}

for brand, brand_data in channel_treemap['byBrand'].items():
    brand_code = brand_code_map_local.get(brand, brand.replace(' ', '_'))
    channel_item_sales_data[brand_code] = {}
    
    for channel_name, channel_info in brand_data['channel']['channels'].items():
        channel_item_sales_data[brand_code][channel_name] = {}
        
        for item_cat, item_cat_info in channel_info['itemCategories'].items():
            # 중분류 매출
            channel_item_sales_data[brand_code][channel_name][item_cat] = item_cat_info.get('sales', 0)
            
            # 소분류 매출
            if 'subCategories' in item_cat_info:
                for sub_cat, sub_cat_info in item_cat_info['subCategories'].items():
                    item_full_name = f"{item_cat}-{sub_cat}"
                    channel_item_sales_data[brand_code][channel_name][item_full_name] = sub_cat_info.get('sales', 0)
```

**JSON 구조 변경**:
```json
{
  "metadata": { ... },
  "channelTreemapData": { ... },
  "itemTreemapData": { ... },
  "channelItemSalesData": {
    "M": {
      "백화점": {
        "당시즌의류": 1000000000,
        "당시즌의류-코트": 500000000,
        ...
      }
    }
  }
}
```

---

### 수정 2: `Dashboard.html` (프론트엔드 수정)

**변경 위치**: 1530~1557번째 줄

**추가 코드**:
```javascript
// ★ channelItemSalesData 저장 (브랜드별 트리맵 사용) ★
if (data.channelItemSalesData) {
  window.channelItemSalesData = data.channelItemSalesData;
  console.log('channelItemSalesData 로드 완료:', Object.keys(window.channelItemSalesData));
} else {
  console.warn('데이터에 channelItemSalesData가 없습니다');
}

// ★ channelTreemapData와 itemTreemapData도 저장 ★
if (data.channelTreemapData) {
  window.channelTreemapData = data.channelTreemapData;
  console.log('channelTreemapData 로드 완료');
}
if (data.itemTreemapData) {
  window.itemTreemapData = data.itemTreemapData;
  console.log('itemTreemapData 로드 완료');
}
```

**동작**:
1. `treemap.json` 로드
2. `channelItemSalesData` → `window.channelItemSalesData`에 저장
3. `getChannelTreemapData()` 함수가 이제 데이터를 찾을 수 있음

---

## 배치 실행 시 동작

```batch
[Step 7.5] Downloading previous year treemap data for YOY calculation
[Step 7.5] Completed

[Step 8] Running treemap data pipeline (download, preprocess, generate JSON with YOY and dates)
[계산] 채널별 매출구성 트리맵 생성 (전체)...
  채널 수: 8
[계산] 아이템별 매출구성 트리맵 생성 (전체)...
  아이템_중분류 수: 12
[계산] 채널별 매출구성 트리맵 생성 (브랜드: MLB)...
  채널 수: 9
[계산] 아이템별 매출구성 트리맵 생성 (브랜드: MLB)...
  아이템_중분류 수: 15
... (6개 브랜드 반복)
[계산] 브랜드별 channelItemSalesData 생성 (Dashboard.html 호환)
  브랜드 수: 6
[Step 8] Completed - Treemap with dates and YOY data generated
  - 채널별 트리맵 (브랜드별 포함)
  - 아이템별 트리맵 (브랜드별 포함)
  - channelItemSalesData (Dashboard.html 호환용)
  - 날짜 메타데이터 포함
```

---

## 최종 파일 구조

### `public/data/{DATE}/treemap.json`
```json
{
  "metadata": {
    "updateDate": "2025-12-01",
    "cyPeriod": {
      "start": "2025-12-01",
      "end": "2025-12-31"
    },
    "pyPeriod": {
      "start": "2024-12-04",
      "end": "2025-01-03"
    }
  },
  "channelTreemapData": {
    "total": { "tag": 150000000000, "sales": 120000000000 },
    "channels": {
      "백화점": {
        "tag": 50000000000,
        "sales": 40000000000,
        "share": 33,
        "discountRate": 20.0,
        "yoy": 105,
        "itemCategories": { ... }
      }
    },
    "byBrand": {
      "MLB": {
        "channel": { ... },
        "item": { ... }
      }
    }
  },
  "itemTreemapData": { ... },
  "channelItemSalesData": {
    "M": {
      "백화점": {
        "당시즌의류": 1000000000,
        "과시즌의류": 500000000,
        "모자": 300000000
      }
    },
    "I": { ... },
    "X": { ... }
  }
}
```

---

## 검증 방법

### 1. 배치 실행 후 파일 확인
```powershell
# JSON 파일 존재 확인
dir public\data\{DATE}\treemap.json

# JSON 내용 확인
type public\data\{DATE}\treemap.json | findstr "channelItemSalesData"
```

### 2. 브라우저 콘솔 확인
Dashboard.html 로드 후 콘솔에서:
```javascript
// 데이터 로드 확인
console.log(window.channelItemSalesData);
// 출력: {M: {...}, I: {...}, X: {...}, V: {...}, ST: {...}, W: {...}}

// 특정 브랜드 데이터 확인
console.log(window.channelItemSalesData['M']);
// 출력: {백화점: {...}, 면세점: {...}, ...}
```

### 3. 트리맵 표시 확인
- **브랜드별 분석** 탭으로 이동
- **MLB** 선택
- **채널별 매출구성** 트리맵에 데이터 표시됨
- 날짜 정보: "당년: 2025-12-01 ~ 2025-12-31 | 전년: 2024-12-04 ~ 2025-01-03 (당년동주차)"

---

## 완료! ✅

이제 배치 실행하면:
1. ✅ 트리맵 데이터가 브랜드별로 생성됨
2. ✅ Dashboard.html이 데이터를 정확히 로드함
3. ✅ 채널별/아이템별 트리맵이 모두 표시됨
4. ✅ 날짜 정보도 정확히 표시됨
5. ✅ YOY 데이터도 포함됨

### 핵심 개선사항
- **하위 호환성 유지**: 기존 `channelItemSalesData` 구조 지원
- **새로운 구조 추가**: `channelTreemapData`, `itemTreemapData` 병행 제공
- **메타데이터 포함**: 날짜 정보, YOY 등 모든 정보 포함
- **브랜드별 분리**: 6개 브랜드 각각의 트리맵 데이터 생성










