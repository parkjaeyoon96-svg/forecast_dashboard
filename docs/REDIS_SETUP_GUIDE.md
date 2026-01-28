# Redis 캐시 설정 가이드 (Upstash)

## 왜 Redis가 필요한가?

현재 Vercel Blob 캐시가 작동하지 않아 매번 Snowflake를 조회하게 됩니다 (2-3분 소요).
Redis를 사용하면:
- ✅ 첫 조회 후 24시간 동안 즉시 응답 (~50ms)
- ✅ 서버 재시작해도 캐시 유지
- ✅ 무료 플랜으로 충분 (10,000 명령/일)

## 1단계: Upstash 회원가입

1. https://console.upstash.com 접속
2. GitHub 또는 Google 계정으로 로그인
3. 무료 (Free) 플랜 선택

## 2단계: Redis 데이터베이스 생성

1. Upstash 콘솔에서 **"Create Database"** 클릭
2. 설정:
   ```
   Name: sales-rate-cache (원하는 이름)
   Type: Regional (무료)
   Region: ap-northeast-2 (서울) 또는 가까운 지역
   ```
3. **Create** 클릭

## 3단계: 환경 변수 복사

Redis 생성 완료 후:

1. **".env" 탭** 클릭
2. **REST API** 섹션에서 다음 복사:
   ```
   UPSTASH_REDIS_REST_URL
   UPSTASH_REDIS_REST_TOKEN
   ```

예시:
```bash
UPSTASH_REDIS_REST_URL=https://us1-merry-firefly-12345.upstash.io
UPSTASH_REDIS_REST_TOKEN=AXXXaaaaBBBBxxxxYYYYzzzzz1234567890
```

## 4단계: 로컬 개발 환경 설정

`.env.local` 파일 생성 또는 수정:

```bash
# Snowflake (기존)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database

# Upstash Redis (새로 추가)
UPSTASH_REDIS_REST_URL=https://your-redis.upstash.io
UPSTASH_REDIS_REST_TOKEN=your_redis_token
```

## 5단계: 패키지 설치 및 서버 재시작

```bash
# Redis 클라이언트 설치
npm install @upstash/redis

# 개발 서버 재시작
npm run dev
```

## 6단계: 테스트

1. 브라우저에서 `/sales-rate` 접속
2. 첫 번째 요청: 2-3분 소요 (Snowflake 조회)
   - 콘솔 로그: `[Snowflake 조회 시작]`
   - 이후: `[Redis 캐시 저장 완료]`
3. 두 번째 요청: 즉시 응답 (~50ms)
   - 콘솔 로그: `[Redis 캐시 적중]`

## Vercel 배포 시 설정

### 1. Vercel 대시보드 접속
```
https://vercel.com/dashboard
```

### 2. 프로젝트 선택 → Settings → Environment Variables

### 3. 환경 변수 추가

| Name | Value | Environments |
|------|-------|--------------|
| `UPSTASH_REDIS_REST_URL` | `https://your-redis.upstash.io` | All (Production, Preview, Development) |
| `UPSTASH_REDIS_REST_TOKEN` | `your_token_here` | All |

### 4. 재배포

```bash
git push
# 또는
vercel --prod
```

## 캐시 동작 확인

### 로그 확인

**캐시 미스 (첫 요청)**:
```
[캐시 미스] sales-rate-20260124 - 새로 조회합니다
[Snowflake 조회 시작]
[Redis 캐시 저장 완료] sales-rate-20260124
[파일 캐시 저장 완료] sales-rate-20260124
```

**캐시 적중 (이후 요청)**:
```
[Redis 캐시 적중] sales-rate-20260124
```

### Upstash 대시보드에서 확인

1. Upstash 콘솔 → Redis 선택
2. **Data Browser** 탭
3. 키 `sales-rate-20260124` 확인
4. TTL 확인 (24시간 = 86400초)

## 캐시 강제 삭제 (테스트용)

### Upstash 대시보드에서:
1. Data Browser
2. 키 선택 → Delete

### 또는 Redis CLI:
```bash
# Upstash CLI 설치 (선택)
npm install -g @upstash/cli

# 연결
upstash redis connect

# 키 삭제
DEL sales-rate-20260124
```

## 비용

### 무료 플랜 한도:
- 요청: 10,000 명령/일
- 스토리지: 256MB
- 대역폭: 200MB/일

### 예상 사용량:
- 하루 1번 저장, 여러 번 조회
- 데이터 크기: ~100KB
- **무료 플랜으로 충분!**

## 문제 해결

### "Cannot find module '@upstash/redis'"
```bash
npm install @upstash/redis
```

### Redis 연결 안 됨
- URL과 Token 다시 확인
- Upstash 대시보드에서 Database 상태 확인
- 네트워크 방화벽 확인

### Redis 없이 사용하고 싶다면
환경 변수 설정하지 않으면 자동으로 로컬 파일 캐시 사용:
- 위치: `.cache/sales-rate-YYYYMMDD.json`
- 만료: 24시간
- **단점**: 서버 재시작하면 삭제될 수 있음

## 요약

1. Upstash 회원가입 (무료)
2. Redis 생성 (1분)
3. 환경 변수 복사
4. `.env.local`에 추가
5. `npm install @upstash/redis`
6. 서버 재시작
7. 완료! 🎉

**효과**: 2-3분 → 50ms (3600배 빨라짐)












