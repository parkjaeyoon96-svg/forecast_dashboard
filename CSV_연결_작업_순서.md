# 📊 CSV 데이터 연결 작업 순서

Next.js 대시보드에 Snowflake CSV, SAP CSV, 마스터파일, 계획 CSV를 연결하는 **순차적 가이드**입니다.

---

## 🎯 전체 작업 개요

```
STEP 1: 환경 확인 및 패키지 설치 ✅ (완료)
STEP 2: 데이터 파일 준비
STEP 3: 데이터 로딩 테스트
STEP 4: 대시보드 확인
STEP 5: 커스터마이징 (선택)
```

---

## ✅ STEP 1: 환경 확인 및 패키지 설치 (완료)

이미 완료된 작업:

```bash
✅ Next.js 프로젝트 설정
✅ npm install papaparse @types/papaparse xlsx
✅ lib/dataLoader.ts 생성 (CSV/Excel 파싱 유틸리티)
✅ app/api/load-data/route.ts 생성 (데이터 로드 API)
✅ app/dashboard/page.tsx 업데이트 (실제 데이터 연동)
```

**현재 상태:**
- 개발 서버: `http://localhost:3000` 실행 중
- 대시보드: `http://localhost:3000/dashboard` 접근 가능
- 샘플 데이터로 작동 중 ✅

---

## 📋 STEP 2: 데이터 파일 준비

### 2-1. 파일 위치 확인

```
Project_Forcast/
└── raw/
    ├── sap_ke30_sample.csv       (샘플 - 이미 있음) ✅
    ├── sap_ke30.csv              (실제 데이터 - 여기에 저장) ⬅️
    ├── snowflake_data.csv        (Snowflake 데이터)
    ├── master_data.xlsx          (마스터 데이터)
    └── plan_data.csv             (계획 데이터)
```

### 2-2. SAP KE30 데이터 준비 (필수)

#### 방법 1: SAP에서 다운로드

1. SAP 시스템 접속
2. 거래코드: **KE30** 실행
3. 필요한 조건 입력 (기간, 자재 등)
4. 결과를 **CSV로 내보내기**
5. 파일을 `raw/sap_ke30.csv`로 저장

#### 방법 2: 샘플 데이터 사용 (테스트용)

```powershell
# PowerShell에서 실행
cd "C:\Users\AD0283\Desktop\AIproject\Project_Forcast"

# 샘플 파일을 실제 파일로 복사
copy raw\sap_ke30_sample.csv raw\sap_ke30.csv
```

#### 필수 컬럼 형식:

```csv
자재코드,자재명,수량,매출액,매출원가,판매관리비,영업이익,고정비,변동비,일자
MAT001,제품A,100,10000000,6000000,2000000,2000000,1000000,1000000,2024-11-01
MAT002,제품B,80,8000000,5000000,1500000,1500000,800000,700000,2024-11-02
```

**주의사항:**
- ✅ 쉼표(,) 구분
- ✅ 숫자에 천단위 쉼표 있어도 됨 ("10,000,000")
- ✅ 인코딩: UTF-8 또는 CP949
- ✅ 날짜: YYYY-MM-DD 형식

### 2-3. Snowflake 데이터 준비 (선택사항)

`raw/snowflake_data.csv` 파일 생성:

```csv
날짜,브랜드,채널,매출액,방문자수,전환율
2024-11-01,브랜드A,온라인,5000000,1200,2.5
2024-11-01,브랜드B,오프라인,3000000,800,3.1
```

**또는** Snowflake에서 직접 추출:

```sql
-- Snowflake SQL 쿼리 예시
SELECT 
    날짜,
    브랜드,
    채널,
    매출액,
    방문자수,
    전환율
FROM 
    sales_data
WHERE 
    날짜 >= '2024-11-01'
ORDER BY 
    날짜 DESC;
```

결과를 CSV로 내보내기 → `raw/snowflake_data.csv` 저장

### 2-4. 마스터 데이터 준비 (선택사항)

Excel 파일 `raw/master_data.xlsx` 생성:

**시트1: 브랜드**
| 브랜드코드 | 브랜드명 | 목표매출 | 전년매출 |
|-----------|---------|---------|---------|
| BR001 | 브랜드A | 32000 | 31000 |
| BR002 | 브랜드B | 27000 | 26000 |

**시트2: 제품**
| 제품코드 | 제품명 | 브랜드코드 | 카테고리 |
|---------|--------|----------|---------|
| PROD001 | 제품A-1 | BR001 | CAT001 |
| PROD002 | 제품A-2 | BR001 | CAT001 |

**시트3: 카테고리**
| 카테고리코드 | 카테고리명 | 상위카테고리 |
|------------|----------|------------|
| CAT001 | 전자제품 | |
| CAT002 | 가전제품 | |

### 2-5. 계획 데이터 준비 (선택사항)

`raw/plan_data.csv` 파일 생성:

```csv
기간,브랜드,목표매출,목표이익,목표이익률
2024-11,브랜드A,32000,5000,15.6
2024-11,브랜드B,27000,4200,15.6
2024-12,브랜드A,35000,5500,15.7
2024-12,브랜드B,29000,4500,15.5
```

---

## 🧪 STEP 3: 데이터 로딩 테스트

### 3-1. API 직접 테스트

브라우저에서 API 엔드포인트 확인:

```
http://localhost:3000/api/load-data
```

**성공 응답 예시:**
```json
{
  "success": true,
  "data": {
    "overview": { ... },
    "brands": [ ... ],
    "weeklyData": { ... }
  },
  "metadata": {
    "loadedAt": "2024-11-14T10:30:00Z",
    "sources": {
      "sap": true,          ← SAP 파일 로드됨
      "snowflake": false,   ← Snowflake 파일 없음
      "master": false,      ← 마스터 파일 없음
      "plan": false         ← 계획 파일 없음
    }
  }
}
```

### 3-2. 터미널 로그 확인

개발 서버 터미널에서 확인:

```
✅ SAP KE30 데이터 로드 완료: 50 행
⚠️ Snowflake 파일을 찾을 수 없습니다.
⚠️ 마스터 파일을 찾을 수 없습니다.
⚠️ 계획 파일을 찾을 수 없습니다.
```

### 3-3. 브라우저 콘솔 테스트

1. 대시보드 열기: `http://localhost:3000/dashboard`
2. F12 (개발자 도구) 열기
3. Console 탭에서 확인:

```javascript
// 데이터 로드 확인
fetch('/api/load-data')
  .then(r => r.json())
  .then(data => {
    console.log('✅ 로드된 데이터:', data);
    console.log('📊 브랜드 수:', data.data.brands.length);
    console.log('📈 매출 데이터:', data.data.brands);
  });
```

---

## 📊 STEP 4: 대시보드 확인

### 4-1. 대시보드 접속

```
http://localhost:3000/dashboard
```

### 4-2. 확인 사항

#### ✅ 상단 헤더 확인
- **데이터 소스** 표시 확인
  - 예: "데이터 소스: sap" (SAP만 로드됨)
  - 예: "데이터 소스: sap, snowflake, master" (3개 로드됨)

#### ✅ KPI 카드 확인
- 실판매출 (현시점)
- 직접이익 (현시점)
- 직접이익율
- 목표달성율
- 실판매출 (월말 예상)
- 영업이익 (월말 예상)
- 영업이익율 (예상)
- 브랜드 수

#### ✅ 차트 확인
1. **브랜드별 매출 현황 & 영업이익** (막대 차트)
2. **전체 브랜드 매출 계획/전년비** (레이더 차트)
3. **주차별 매출 추이** (막대+선 혼합 차트)
4. **브랜드별 매출 비중** (파이 차트)

#### ✅ 재무 리포트 확인
- 실판매출, 영업이익 등의 표

#### ✅ 브랜드별 분석 탭
- 브랜드 선택 버튼
- 개별 브랜드 상세 정보

### 4-3. 데이터 새로고침

대시보드 상단의 **🔄 새로고침** 버튼 클릭:
- 새로운 데이터 파일 추가 후
- 파일 내용 수정 후

---

## 🔧 STEP 5: 커스터마이징 (선택사항)

### 5-1. 컬럼명이 다른 경우

실제 SAP CSV의 컬럼명이 다르면 수정:

**파일:** `lib/dataLoader.ts`

```typescript
export interface SAPKe30Row {
  // 실제 컬럼명으로 변경
  자재코드: string;    // Material_Code
  자재명: string;      // Material_Name
  수량: number;        // Quantity
  매출액: number;      // Revenue
  // ...
}
```

### 5-2. 브랜드 집계 방식 변경

**파일:** `lib/dataLoader.ts`

```typescript
export function aggregateSAPData(rows: SAPKe30Row[]): BrandData[] {
  // 자재명 대신 다른 필드로 그룹화
  const brandMap = new Map<string, {...}>();

  rows.forEach(row => {
    // 변경: 자재명 → 브랜드 코드
    const brand = row.브랜드코드; // 추가 컬럼 사용
    // ...
  });
}
```

### 5-3. 추가 차트 만들기

**파일:** `app/dashboard/page.tsx`

```typescript
// 새로운 차트 데이터
const myChartData = {
  labels: data.brands.map(b => b.name),
  datasets: [{
    label: '새로운 지표',
    data: data.brands.map(b => b.customMetric),
    backgroundColor: 'rgba(255, 99, 132, 0.8)'
  }]
};

// JSX에 추가
<div className="bg-white rounded-lg p-4">
  <h3 className="text-center font-bold">새로운 차트</h3>
  <div className="h-64">
    <Bar data={myChartData} options={chartOptions} />
  </div>
</div>
```

### 5-4. Snowflake 데이터 활용

**파일:** `app/api/load-data/route.ts`

```typescript
// Snowflake 데이터 로드 후
if (dashboardData.rawData!.snowflake) {
  const sfData = dashboardData.rawData!.snowflake;
  
  // 추가 분석
  const channelAnalysis = analyzeByChannel(sfData);
  dashboardData.channelData = channelAnalysis;
}
```

---

## 🔍 문제 해결

### ❌ 문제: 파일을 찾을 수 없습니다

**증상:**
```
⚠️ SAP KE30 파일을 찾을 수 없습니다. 샘플 데이터를 사용합니다.
```

**해결:**
1. 파일 경로 확인
   ```powershell
   ls raw\sap_ke30.csv
   ```
2. 파일명 확인 (대소문자, 확장자)
3. 파일이 `raw/` 폴더에 있는지 확인

### ❌ 문제: 데이터가 표시되지 않음

**해결:**
1. 브라우저 새로고침 (Ctrl + Shift + R)
2. 개발 서버 재시작
   ```powershell
   # Ctrl+C로 서버 종료 후
   npm run dev
   ```
3. 브라우저 콘솔(F12)에서 오류 확인

### ❌ 문제: 한글이 깨짐

**해결:**
CSV 파일을 UTF-8로 저장:

1. Excel에서 열기
2. 다른 이름으로 저장
3. 인코딩: **UTF-8 (BOM 포함)** 선택
4. 저장

### ❌ 문제: 숫자가 이상하게 표시됨

**원인:**
- 쉼표가 잘못 해석됨
- 숫자가 문자열로 저장됨

**해결:**
`lib/dataLoader.ts`에서 이미 처리됨:

```typescript
function cleanValue(value: string): any {
  // 쉼표 제거 후 숫자 변환
  const numericValue = value.replace(/,/g, '');
  if (!isNaN(Number(numericValue))) {
    return Number(numericValue);
  }
  return value;
}
```

---

## ✅ 완료 체크리스트

데이터 연결이 제대로 되었는지 확인:

- [ ] **환경 확인**
  - [ ] Node.js 설치됨
  - [ ] `npm install` 완료
  - [ ] 개발 서버 실행 중

- [ ] **파일 준비**
  - [ ] `raw/sap_ke30.csv` 파일 존재
  - [ ] CSV 형식 확인 (필수 컬럼 있음)
  - [ ] 선택사항 파일들 (snowflake, master, plan)

- [ ] **API 테스트**
  - [ ] `/api/load-data` 접속됨
  - [ ] `success: true` 응답 받음
  - [ ] 터미널에 "✅ 데이터 로드 완료" 표시

- [ ] **대시보드 확인**
  - [ ] http://localhost:3000/dashboard 접속
  - [ ] "데이터 소스: sap" 표시됨
  - [ ] KPI 카드에 실제 데이터 표시
  - [ ] 차트가 정상 렌더링
  - [ ] 브랜드별 분석 탭 작동

- [ ] **기능 테스트**
  - [ ] 🔄 새로고침 버튼 작동
  - [ ] 브랜드 선택 버튼 작동
  - [ ] 전체 현황 / 브랜드별 분석 탭 전환

---

## 📚 추가 참고 자료

### 공식 문서
- [CSV_DATA_GUIDE.md](./CSV_DATA_GUIDE.md) - 상세 가이드
- [DATA_UPDATE_GUIDE.md](./DATA_UPDATE_GUIDE.md) - 기존 Python 스크립트 가이드

### 파일 위치
- 데이터 로더: `lib/dataLoader.ts`
- API: `app/api/load-data/route.ts`
- 대시보드: `app/dashboard/page.tsx`

### 유용한 명령어
```powershell
# 개발 서버 시작
npm run dev

# 프로덕션 빌드
npm run build
npm run start

# 패키지 재설치
rm -rf node_modules
npm install

# 캐시 삭제
rm -rf .next
```

---

## 🎉 완료!

이제 CSV 데이터가 Next.js 대시보드에 연결되었습니다!

**다음 단계:**
1. 실제 SAP 데이터로 테스트
2. 추가 분석 기능 개발
3. 차트 커스터마이징
4. 사용자 인터페이스 개선

**업데이트 주기:**
- 일일: SAP KE30 데이터만 업데이트
- 주간: 모든 데이터 파일 업데이트
- 월간: 마스터 데이터 및 계획 데이터 검토

---

**작성일**: 2024-11-14  
**버전**: 1.0.0  
**상태**: ✅ 완료





