# 캐시 개선 완료 - Redis 도입

## 🎯 문제점
- **증상**: 페이지 접속할 때마다 2-3분 대기
- **원인**: Vercel Blob 토큰 미설정으로 캐시 저장 실패
- **결과**: 매번 Snowflake 직접 조회

## ✅ 해결 방법: Redis + 로컬 파일 캐시

### 3단계 폴백 전략
```
1순위: Redis (Upstash) → 프로덕션용, 50ms
2순위: 로컬 파일 캐시 → 개발/폴백용, 100ms
3순위: Snowflake 직접 조회 → 캐시 미스 시, 2-3분
```

### 캐시 정책
- **키**: `sales-rate-YYYYMMDD` (날짜별)
- **TTL**: 24시간 자동 만료
- **저장 방식**: Redis와 파일에 병렬 저장

## 📦 변경 사항

### 1. API 로직 개선
**파일**: `app/api/sales-rate/route.ts`

```typescript
// Redis 우선, 없으면 파일 캐시, 없으면 Snowflake
const redis = new Redis({ url, token }); // 선택 사항
```

**주요 기능**:
- Redis 조회/저장 (`@upstash/redis`)
- 로컬 파일 캐시 (`.cache/` 디렉토리)
- 24시간 TTL 관리
- 에러 핸들링 (Redis 없어도 작동)

### 2. 패키지 추가
**파일**: `package.json`

```json
"dependencies": {
  "@upstash/redis": "^1.34.3"
}
```

### 3. 문서 업데이트
- `docs/SALES_RATE_ANALYSIS.md` - 전체 가이드
- `docs/REDIS_SETUP_GUIDE.md` - Redis 설정 상세 가이드 (NEW)

### 4. .gitignore 업데이트
```
.cache/  # 로컬 캐시 디렉토리 제외
```

## 🚀 사용 방법

### 옵션 1: Redis 사용 (권장)

#### 1. Upstash Redis 생성 (5분, 무료)
```
https://console.upstash.com
→ Create Database
→ Regional (무료)
→ Copy REST URL & Token
```

#### 2. 환경 변수 추가 (`.env.local`)
```bash
UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
UPSTASH_REDIS_REST_TOKEN=your_token
```

#### 3. 서버 재시작
```bash
npm run dev
```

#### 4. 결과
- 첫 요청: 2-3분 (Snowflake 조회 + 캐시 저장)
- 이후 요청: ~50ms (Redis 캐시)
- 24시간 후: 자동 갱신

### 옵션 2: Redis 없이 사용 (로컬 개발)

환경 변수 설정하지 않으면 **자동으로 파일 캐시** 사용:

```bash
# 그냥 실행
npm run dev

# .cache/ 디렉토리에 자동 저장
ls .cache/
# → sales-rate-20260124.json
```

**동작**:
- 첫 요청: 2-3분 (Snowflake 조회 + 파일 저장)
- 이후 요청: ~100ms (파일 캐시)
- 24시간 후 또는 서버 재시작 시: 재조회

## 📊 성능 비교

| 방식 | 응답 시간 | 안정성 | 비용 |
|------|----------|--------|------|
| **Vercel Blob (기존)** | ❌ 실패 | ❌ 작동 안 함 | 무료 |
| **Snowflake 직접** | ⏰ 2-3분 | ✅ 항상 최신 | DB 쿼리 비용 |
| **파일 캐시** | 🟡 ~100ms | ⚠️ 서버 재시작 시 삭제 | 무료 |
| **Redis (Upstash)** | ✅ ~50ms | ✅ 영구 보존 | 무료 플랜 충분 |

## 🔍 동작 확인

### 콘솔 로그

**첫 번째 요청 (캐시 미스)**:
```
[캐시 미스] sales-rate-20260124 - 새로 조회합니다
[Snowflake 조회 시작]
[Redis 캐시 저장 완료] sales-rate-20260124
[파일 캐시 저장 완료] sales-rate-20260124
```

**두 번째 요청 (Redis 적중)**:
```
[Redis 캐시 적중] sales-rate-20260124
```

**Redis 없을 때 (파일 적중)**:
```
[파일 캐시 적중] sales-rate-20260124 (2.3시간 전)
```

### Upstash 대시보드 확인
1. https://console.upstash.com
2. Redis 선택 → Data Browser
3. 키 `sales-rate-20260124` 확인
4. TTL 표시 (86400초 = 24시간)

## 📁 파일 구조

```
Project_Forcast/
├── app/
│   └── api/
│       └── sales-rate/
│           └── route.ts          # ✨ Redis + 파일 캐시 구현
├── .cache/                        # 🆕 로컬 캐시 디렉토리
│   └── sales-rate-20260124.json  # 자동 생성
├── docs/
│   ├── SALES_RATE_ANALYSIS.md    # 📝 전체 가이드
│   └── REDIS_SETUP_GUIDE.md      # 🆕 Redis 설정 상세
├── .gitignore                     # ✨ .cache/ 추가
└── package.json                   # ✨ @upstash/redis 추가
```

## ⚙️ 환경 변수

### 필수 (기존)
```bash
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USERNAME=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=xxx
SNOWFLAKE_DATABASE=xxx
```

### 선택 (신규 - 권장)
```bash
UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
UPSTASH_REDIS_REST_TOKEN=your_token
```

### 제거 (더 이상 사용 안 함)
```bash
# BLOB_READ_WRITE_TOKEN=xxx  # 삭제해도 됨
```

## 🐛 문제 해결

### Q1. Redis 연결 안 됨
**증상**: `[Redis 조회 실패]` 로그
**해결**: 
- URL/Token 확인
- Upstash 대시보드에서 Redis 상태 확인
- 파일 캐시로 자동 폴백됨 (정상 작동)

### Q2. 파일 캐시가 삭제됨
**증상**: 서버 재시작 후 다시 느림
**해결**: 
- Redis 설정 (영구 보존)
- 또는 `.cache/` 디렉토리를 영구 저장소에 마운트

### Q3. 캐시를 강제로 갱신하고 싶음
**방법**:
```bash
# Upstash 대시보드 → Data Browser → Delete
# 또는
rm .cache/sales-rate-*.json

# 다음 요청 시 새로 조회
```

## 🎉 결과

### Before (Vercel Blob 실패)
```
매 요청: 2-3분 대기 ⏰
```

### After (Redis + 파일 캐시)
```
첫 요청: 2-3분 (1일 1회)
이후: ~50ms ⚡ (3600배 빠름!)
```

## 📚 더 알아보기

- 📖 [Redis 설정 가이드](./REDIS_SETUP_GUIDE.md) - 상세 설정 방법
- 📖 [판매율 분석 전체 문서](./SALES_RATE_ANALYSIS.md) - 기능 설명

## ✨ 다음 단계

### 즉시 적용 (권장)
1. Upstash Redis 생성 (5분)
2. 환경 변수 추가
3. 서버 재시작
4. 완료!

### 나중에 적용
- Redis 없이도 파일 캐시로 작동
- Vercel 배포 시 Redis 설정 권장










