# 📊 CSV 데이터 연결 가이드

Next.js 대시보드에 CSV/Excel 파일을 연결하는 전체 가이드입니다.

## 🎯 개요

이 프로젝트는 다음 데이터 소스들을 지원합니다:
- ✅ **SAP KE30 CSV** (자재별 손익 데이터)
- ✅ **Snowflake CSV** (추가 분석 데이터)
- ✅ **마스터 데이터 Excel** (제품/브랜드 기준정보)
- ✅ **계획 데이터 CSV** (목표 및 계획)

---

## 📁 파일 구조

```
Project_Forcast/
├── raw/                          # 원본 데이터 파일
│   ├── sap_ke30_sample.csv      # SAP 데이터 (샘플)
│   ├── sap_ke30.csv             # SAP 데이터 (실제) ← 여기에 저장
│   ├── snowflake_data.csv       # Snowflake 데이터 (선택)
│   ├── master_data.xlsx         # 마스터 데이터 (선택)
│   └── plan_data.csv            # 계획 데이터 (선택)
│
├── lib/
│   └── dataLoader.ts            # 데이터 로딩 유틸리티
│
├── app/
│   ├── api/
│   │   ├── data/route.ts        # 기본 API (샘플 데이터)
│   │   └── load-data/route.ts   # CSV 로드 API ★
│   └── dashboard/
│       └── page.tsx             # 대시보드 페이지
│
└── CSV_DATA_GUIDE.md           # 이 파일
```

---

## 🚀 빠른 시작

### STEP 1: 데이터 파일 준비

#### 1-1. SAP KE30 데이터 (필수)

`raw/sap_ke30.csv` 파일을 준비합니다.

**필수 컬럼:**
```csv
자재코드,자재명,수량,매출액,매출원가,판매관리비,영업이익,고정비,변동비,일자
MAT001,제품A,100,10000000,6000000,2000000,2000000,1000000,1000000,2024-11-01
MAT002,제품B,80,8000000,5000000,1500000,1500000,800000,700000,2024-11-01
```

**주의사항:**
- 숫자에 쉼표가 있어도 됩니다 (예: "10,000,000")
- 인코딩: UTF-8 또는 CP949
- 날짜 형식: YYYY-MM-DD

#### 1-2. Snowflake 데이터 (선택)

`raw/snowflake_data.csv` - 추가 분석용 데이터

#### 1-3. 마스터 데이터 (선택)

`raw/master_data.xlsx` - Excel 멀티시트 파일
- 시트1: 브랜드 정보
- 시트2: 제품 정보
- 시트3: 카테고리 정보

#### 1-4. 계획 데이터 (선택)

`raw/plan_data.csv` - 월별/브랜드별 목표 데이터

---

### STEP 2: 개발 서버 실행

```powershell
# 프로젝트 디렉토리로 이동
cd "C:\Users\AD0283\Desktop\AIproject\Project_Forcast"

# 개발 서버 실행
npm run dev
```

서버가 시작되면:
- 메인 페이지: http://localhost:3000
- 대시보드: http://localhost:3000/dashboard

---

### STEP 3: 데이터 확인

대시보드에서 상단에 **"데이터 소스: sap, snowflake, master, plan"** 표시를 확인합니다.

- ✅ **sap**: SAP KE30 데이터 로드됨
- ✅ **snowflake**: Snowflake 데이터 로드됨
- ✅ **master**: 마스터 데이터 로드됨
- ✅ **plan**: 계획 데이터 로드됨

파일이 없으면 샘플 데이터가 사용됩니다.

---

## 🔧 상세 설정

### API 엔드포인트

#### 1. `/api/load-data` - 실제 CSV 데이터 로드

```typescript
// GET /api/load-data
{
  success: true,
  data: {
    overview: { ... },
    brands: [ ... ],
    weeklyData: { ... },
    financialReport: { ... },
    rawData: {
      sap: [ ... ],      // 원본 SAP 데이터
      snowflake: [ ... ], // 원본 Snowflake 데이터
      master: { ... },    // 원본 마스터 데이터
      plan: [ ... ]       // 원본 계획 데이터
    }
  },
  metadata: {
    loadedAt: "2024-11-14T10:30:00Z",
    sources: {
      sap: true,
      snowflake: false,
      master: false,
      plan: false
    }
  }
}
```

#### 2. `/api/data` - 샘플 데이터 (폴백)

파일 로드 실패시 자동으로 사용됩니다.

---

## 📝 데이터 포맷 상세

### SAP KE30 CSV 포맷

```csv
자재코드,자재명,수량,매출액,매출원가,판매관리비,영업이익,고정비,변동비,일자
MAT001,제품A,100,"10,000,000","6,000,000","2,000,000","2,000,000","1,000,000","1,000,000",2024-11-01
```

**컬럼 설명:**
- `자재코드`: 제품 코드 (문자열)
- `자재명`: 제품명 (문자열) - 브랜드로 사용됨
- `수량`: 판매 수량 (숫자)
- `매출액`: 매출액 (숫자, 원 단위)
- `매출원가`: 제조원가 (숫자, 원 단위)
- `판매관리비`: 판매관리비 (숫자, 원 단위)
- `영업이익`: 영업이익 (숫자, 원 단위)
- `고정비`: 고정비 (숫자, 원 단위, 선택)
- `변동비`: 변동비 (숫자, 원 단위, 선택)
- `일자`: 거래일자 (YYYY-MM-DD)

**데이터 처리:**
- 자재명별로 자동 집계
- 주차별로 자동 그룹화
- 억원 단위로 자동 변환

### Snowflake CSV 포맷 (자유 형식)

```csv
컬럼1,컬럼2,컬럼3,...
값1,값2,값3,...
```

### 마스터 데이터 Excel 포맷

**시트1: 브랜드**
```
브랜드코드,브랜드명,목표매출,전년매출
BR001,브랜드A,32000,31000
```

**시트2: 제품**
```
제품코드,제품명,브랜드코드,카테고리
PROD001,제품A-1,BR001,CAT001
```

### 계획 데이터 CSV 포맷

```csv
기간,브랜드,목표매출,목표이익,목표이익률
2024-11,브랜드A,32000,5000,15.6
```

---

## 🛠️ 커스터마이징

### 1. 컬럼명 변경

실제 SAP CSV의 컬럼명이 다른 경우 `lib/dataLoader.ts` 수정:

```typescript
export interface SAPKe30Row {
  자재코드: string;          // ← 실제 컬럼명으로 변경
  자재명: string;            // ← 실제 컬럼명으로 변경
  수량: number;
  매출액: number;
  // ...
}
```

### 2. 집계 로직 변경

`lib/dataLoader.ts`의 `aggregateSAPData` 함수 수정:

```typescript
export function aggregateSAPData(rows: SAPKe30Row[]): BrandData[] {
  // 브랜드별 집계 대신 카테고리별 집계
  const categoryMap = new Map<string, {...}>();
  
  rows.forEach(row => {
    const category = row.카테고리; // 추가 필드 사용
    // ...
  });
}
```

### 3. 새로운 차트 추가

`app/dashboard/page.tsx`에 차트 컴포넌트 추가:

```typescript
// 새로운 차트 데이터 정의
const myNewChartData = {
  labels: data.brands.map(b => b.name),
  datasets: [{ ... }]
};

// JSX에 차트 추가
<div className="bg-white rounded-lg p-4">
  <h3>새로운 차트</h3>
  <Bar data={myNewChartData} options={chartOptions} />
</div>
```

---

## 🔍 문제 해결

### 문제 1: "파일을 찾을 수 없습니다"

**증상:**
```
⚠️ SAP KE30 파일을 찾을 수 없습니다. 샘플 데이터를 사용합니다.
```

**해결:**
1. 파일 경로 확인: `raw/sap_ke30.csv` (또는 `sap_ke30_sample.csv`)
2. 파일명 확인 (대소문자, 철자)
3. 파일이 프로젝트 루트의 `raw/` 폴더에 있는지 확인

### 문제 2: "데이터 형식 오류"

**증상:**
차트가 제대로 표시되지 않음

**해결:**
1. CSV 컬럼명 확인
2. 숫자 필드에 문자 데이터 없는지 확인
3. 날짜 형식 확인 (YYYY-MM-DD)

브라우저 콘솔(F12) 확인:
```javascript
// 개발자 도구 콘솔에서
fetch('/api/load-data').then(r => r.json()).then(console.log)
```

### 문제 3: "인코딩 오류"

**증상:**
한글이 깨져서 표시됨

**해결:**
`lib/dataLoader.ts` 수정:

```typescript
// UTF-8-BOM 처리
const fileContent = fs.readFileSync(fullPath, 'utf-8')
  .replace(/^\uFEFF/, ''); // BOM 제거
```

또는 CSV를 UTF-8로 다시 저장

### 문제 4: "대시보드가 로딩 중에서 멈춤"

**해결:**
1. 터미널에서 오류 메시지 확인
2. `npm run dev` 재시작
3. 브라우저 캐시 삭제 (Ctrl+Shift+R)

---

## 📊 데이터 흐름

```
1. CSV/Excel 파일 준비
   └── raw/ 폴더에 저장

2. 대시보드 접속
   └── http://localhost:3000/dashboard

3. API 호출
   └── GET /api/load-data

4. 데이터 로딩 (lib/dataLoader.ts)
   ├── loadCSVFromFile() - CSV 파싱
   ├── loadExcelFromFile() - Excel 파싱
   ├── aggregateSAPData() - 데이터 집계
   ├── generateWeeklyData() - 주차별 집계
   └── generateFinancialReport() - 재무 리포트 생성

5. 차트 렌더링
   └── React Chart.js로 시각화
```

---

## 🔄 데이터 업데이트 프로세스

### 일일 업데이트

1. SAP에서 KE30 데이터 다운로드
2. `raw/sap_ke30.csv`로 저장 (기존 파일 덮어쓰기)
3. 대시보드에서 **🔄 새로고침** 버튼 클릭

### 주간 업데이트

1. 모든 데이터 파일 업데이트
   - `raw/sap_ke30.csv`
   - `raw/snowflake_data.csv`
   - `raw/master_data.xlsx`
   - `raw/plan_data.csv`

2. 대시보드 새로고침

### 자동화 (선택사항)

Python 스크립트로 자동 업데이트:

```python
# scripts/auto_update.py
import schedule
import time

def update_dashboard():
    # 1. SAP에서 데이터 다운로드
    download_sap_data()
    
    # 2. Snowflake에서 데이터 추출
    fetch_snowflake_data()
    
    # 3. 파일 저장
    save_to_raw_folder()
    
    print("데이터 업데이트 완료!")

# 매일 오전 9시 실행
schedule.every().day.at("09:00").do(update_dashboard)

while True:
    schedule.run_pending()
    time.sleep(60)
```

---

## 💡 베스트 프랙티스

### 1. 데이터 백업

중요한 데이터는 별도 백업:

```powershell
# 날짜별 백업 폴더 생성
mkdir raw\backup\2024-11-14

# 파일 복사
copy raw\*.csv raw\backup\2024-11-14\
copy raw\*.xlsx raw\backup\2024-11-14\
```

### 2. 대용량 데이터 처리

파일이 큰 경우 (>10MB):

```typescript
// lib/dataLoader.ts에서 스트리밍 처리
import { createReadStream } from 'fs';
import { parse } from 'csv-parse';

export async function loadLargeCSV(filePath: string) {
  return new Promise((resolve, reject) => {
    const rows: any[] = [];
    
    createReadStream(filePath)
      .pipe(parse({ columns: true }))
      .on('data', (row) => rows.push(row))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}
```

### 3. 데이터 검증

로드 전 데이터 유효성 검사:

```typescript
function validateSAPData(rows: SAPKe30Row[]): boolean {
  return rows.every(row => 
    row.자재코드 && 
    !isNaN(row.매출액) && 
    row.매출액 >= 0
  );
}
```

### 4. 성능 최적화

- 첫 로드 후 캐싱 사용
- 필요한 컬럼만 선택
- 집계 결과 메모이제이션

---

## 🎓 추가 학습 자료

- [Next.js API Routes](https://nextjs.org/docs/api-routes/introduction)
- [Chart.js 공식 문서](https://www.chartjs.org/docs/latest/)
- [PapaParse CSV 파서](https://www.papaparse.com/)
- [XLSX 라이브러리](https://www.npmjs.com/package/xlsx)

---

## 📞 문의 및 지원

문제가 해결되지 않으면:

1. 터미널 오류 메시지 캡처
2. 브라우저 콘솔(F12) 오류 확인
3. 샘플 데이터로 테스트

**유용한 명령어:**
```powershell
# Node.js 버전 확인
node --version

# 패키지 재설치
rm -rf node_modules
npm install

# 캐시 삭제
rm -rf .next
npm run dev
```

---

## ✅ 체크리스트

데이터 연결 전 확인사항:

- [ ] Node.js 설치됨 (v18 이상)
- [ ] 필요한 패키지 설치됨 (`npm install`)
- [ ] CSV 파일이 `raw/` 폴더에 있음
- [ ] CSV 인코딩이 UTF-8 또는 CP949
- [ ] 필수 컬럼이 모두 존재함
- [ ] 숫자 데이터가 올바른 형식
- [ ] 개발 서버가 실행 중 (`npm run dev`)
- [ ] 브라우저에서 http://localhost:3000/dashboard 접속됨

---

**마지막 업데이트**: 2024-11-14

**버전**: 1.0.0





