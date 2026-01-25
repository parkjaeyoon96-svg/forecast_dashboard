# 당시즌 의류 판매율 분석 기능

## 개요
브랜드별 당시즌 의류 판매 현황을 분석하는 기능입니다. Snowflake에서 실시간 데이터를 조회하여 당년, 전년, 전년마감 데이터를 비교 분석합니다.

## 주요 기능

### 1. 기본 정보 표시
- **업데이트 일자**: 데이터 조회 일자
- **분석 기간**:
  - 당년: 현재 시즌 누적 데이터 (예: 2026-01-23)
  - 전년: 동일 주차의 전년 누적 데이터 (예: 2025-01-23)
  - 마감: 전년 시즌 마감 데이터 (예: 2025-02-28)

### 2. 합계 표시
- 선택한 카테고리에 해당하는 전체 브랜드의 합계를 테이블 최상단에 표시
- 카테고리 선택 시 자동으로 재계산

### 3. 카테고리 필터링
- **전체**: 모든 카테고리 데이터 통합 (브랜드별 합계)
- **Outer**: 아우터 의류 (재킷, 코트, 베스트 등)
- **Bottom**: 하의 의류 (바지, 스커트, 데님 등)
- **Inner**: 상의 의류 (티셔츠, 니트, 셔츠, 블라우스, 원피스 등)

### 4. 브랜드별 데이터
각 브랜드별로 다음 정보를 표시:
- 발주 금액 (억 원 단위)
- 입고 금액 (억 원 단위)
- 판매 금액 (억 원 단위)
- 판매율 (판매/입고 × 100%)

## 파일 구조

```
app/
├── components/
│   └── SalesRateTable.tsx        # 판매율 분석 테이블 컴포넌트
├── sales-rate/
│   └── page.tsx                  # 판매율 분석 페이지
└── api/
    └── sales-rate/
        └── route.ts              # API 엔드포인트

scripts/
└── query_sales_rate.py           # Snowflake 쿼리 실행 스크립트
```

## 데이터 흐름

1. **프론트엔드** (`SalesRateTable.tsx`)
   - `/api/sales-rate` API 호출

2. **API** (`api/sales-rate/route.ts`)
   - Vercel Blob에서 캐시 확인
   - 캐시 없으면 Python 스크립트 실행
   - 결과를 Vercel Blob에 저장 (캐시)
   - 클라이언트에 JSON 반환

3. **Python 스크립트** (`query_sales_rate.py`)
   - Snowflake 연결
   - 당년(CUR), 전년(PY), 전년마감(PY_END) 데이터 조회
   - JSON 형식으로 출력

4. **프론트엔드 데이터 처리**
   - 브랜드-카테고리별 집계
   - 카테고리 필터링
   - 합계 계산
   - 판매율 계산

## 환경 설정

### 필수 환경 변수

`.env` 또는 `.env.local` 파일에 다음 변수 설정:

```bash
# Snowflake 연결 정보
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database

# Upstash Redis (선택 사항 - 강력 권장)
UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
UPSTASH_REDIS_REST_TOKEN=your_redis_token
```

### 캐시 전략 (3단계 폴백)

1. **Redis (Upstash)** - 프로덕션용 (최우선)
   - 빠른 속도 (메모리 기반)
   - Serverless 아키텍처
   - 24시간 TTL
   - **설정 안 하면 자동으로 로컬 캐시 사용**

2. **로컬 파일 캐시** - 개발/폴백용
   - `.cache/` 디렉토리에 JSON 저장
   - Redis 없어도 작동
   - 24시간 만료

3. **Snowflake 직접 조회** - 캐시 미스 시
   - 2-3분 소요
   - 결과는 자동으로 캐시에 저장

### Upstash Redis 설정 (권장)

#### 무료 플랜 사용 가능!
- 10,000 명령/일 무료
- 프로젝트에 충분함

#### 1. Upstash 회원가입
```
https://console.upstash.com
```

#### 2. Redis 데이터베이스 생성
1. "Create Database" 클릭
2. Name: `sales-rate-cache` (원하는 이름)
3. Region: 가장 가까운 지역 선택
4. Type: Regional (무료)
5. Create 클릭

#### 3. 환경 변수 복사
Redis 생성 후 대시보드에서:
- `UPSTASH_REDIS_REST_URL` 복사
- `UPSTASH_REDIS_REST_TOKEN` 복사

#### 4. 환경 변수 설정

**로컬 개발 (`.env.local`)**:
```bash
UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
UPSTASH_REDIS_REST_TOKEN=AXXXaaaaBBBBxxxxYYYY
```

**Vercel 배포**:
1. Vercel 대시보드 → Project Settings
2. Environment Variables
3. 위 두 변수 추가
4. 재배포

### Redis 없이 사용하기

Redis 환경 변수가 없으면 자동으로 **로컬 파일 캐시**를 사용합니다.

```bash
# Redis 변수 없이 실행 가능
npm run dev
```

로컬 파일 캐시:
- 위치: `.cache/sales-rate-YYYYMMDD.json`
- 만료: 24시간
- 자동 생성/삭제

## 사용 방법

### 개발 서버에서 테스트

```bash
npm run dev
```

브라우저에서 접속:
```
http://localhost:3000/sales-rate
```

### Vercel 배포

```bash
vercel deploy
```

환경 변수는 Vercel 대시보드에서 설정:
1. Project Settings
2. Environment Variables
3. 위의 환경 변수 추가

## 카테고리 매핑

| 카테고리 | 아이템 코드 |
|---------|-----------|
| Outer   | JK, CT, VT, CD, JP, PD |
| Bottom  | PT, SK, DP, LG, SP |
| Inner   | TS, KT, SH, BL, OP |

## 브랜드 코드 매핑

| 코드 | 브랜드명 |
|-----|---------|
| C | KUHO |
| D | CRES.E.DIM |
| E | ETC |
| F | SYSTEM |
| G | GUGGY |
| H | SJ |
| I | MLB |
| J | MLB KIDS |
| K | VPLUS |
| L | COLOMBO |
| M | MAJE |
| N | SANDRO |
| O | CLAUDIE |
| P | AP |
| Q | THE KOOPLES |
| R | ZADIG |
| S | JWPEI |
| T | TORY |
| U | MCM |
| V | METROCITY |
| W | TED BAKER |

## 성능 최적화

### 캐시 성능
- **Redis 캐시 적중**: ~50ms
- **파일 캐시 적중**: ~100ms
- **Snowflake 직접 조회**: ~2-3분

### 캐시 전략
1. **일일 캐시**: 날짜 기준 (YYYYMMDD)
2. **자동 만료**: 24시간 후 자동 삭제
3. **3단계 폴백**: Redis → 파일 → Snowflake
4. **병렬 저장**: Redis와 파일에 동시 저장

### 데이터 집계
- 브랜드-카테고리별 Map 자료구조 사용
- 메모리 효율적인 집계 로직

## 문제 해결

### 1. 캐시가 매번 리셋되는 문제
**증상**: 페이지 접속할 때마다 2-3분씩 기다려야 함

**원인**:
- Redis 환경 변수 미설정
- 파일 캐시가 삭제됨 (서버 재시작 등)

**해결**:
1. **Upstash Redis 설정** (가장 확실한 방법)
   ```bash
   # .env.local에 추가
   UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
   UPSTASH_REDIS_REST_TOKEN=your_token
   ```
   - 무료 플랜으로 충분
   - 한 번 설정하면 영구적으로 해결

2. **로컬 파일 캐시 확인**
   ```bash
   # .cache 디렉토리 확인
   ls -la .cache/
   
   # 캐시 파일 확인
   cat .cache/sales-rate-20260124.json
   ```

3. **.gitignore에 .cache 추가**
   ```
   .cache/
   ```

### 2. "UPSTASH_REDIS_REST_URL is not defined" 에러
- **정상입니다!** Redis 없어도 파일 캐시로 작동
- Redis 설정을 원하면 위 가이드 참조

### 3. Python 실행 오류
- 가상환경 활성화 확인: `Forcast_venv/Scripts/python.exe`
- 필요한 패키지 설치 확인: `snowflake-connector-python`, `pandas`, `python-dotenv`

### 4. Snowflake 연결 실패
- 환경 변수 확인 (`.env` 파일)
- Snowflake 계정 권한 확인
- 네트워크 연결 확인

### 5. 캐시 강제 삭제 (테스트용)
```bash
# Redis 캐시 삭제 (Upstash 대시보드에서)
# 또는
rm -rf .cache/

# 다음 요청 시 새로 조회됨
```

## API 응답 형식

```json
{
  "success": true,
  "date": "2026-01-24",
  "periodInfo": {
    "curDate": "2026-01-23",
    "pyDate": "2025-01-23",
    "pyEndDate": "2025-02-28"
  },
  "data": {
    "CUR": [
      {
        "BRD_CD": "I",
        "PRDT_CD": "PRD001",
        "ITEM_CD": "JK",
        "AC_ORD_TAG_AMT_KOR": 1000000000,
        "AC_STOR_TAG_AMT_KOR": 800000000,
        "SALE_TAG": 400000000,
        ...
      }
    ],
    "PY": [...],
    "PY_END": [...]
  },
  "rowCount": {
    "CUR": 150,
    "PY": 145,
    "PY_END": 148
  }
}
```

## 향후 개선 사항

- [ ] 엑셀 다운로드 기능
- [ ] 판매율 추이 그래프
- [ ] 브랜드별 상세 분석 페이지
- [ ] 실시간 데이터 새로고침 버튼
- [ ] 날짜 범위 선택 기능

