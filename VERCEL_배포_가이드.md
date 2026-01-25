# Vercel 배포 가이드

## 왜 Vercel인가?
현재 프로젝트는 Next.js로 구성되어 있으며, `/api/sales-rate` 같은 API 라우트가 있습니다.
GitHub Pages는 **정적 파일만 지원**하므로 API가 작동하지 않습니다.
Vercel은 Next.js의 공식 플랫폼으로 API 라우트를 완벽히 지원합니다.

## 배포 단계

### 1. Vercel 계정 생성
1. https://vercel.com 접속
2. "Sign Up" 클릭
3. **GitHub 계정으로 로그인** (추천)

### 2. 프로젝트 Import
1. Vercel 대시보드에서 "Add New..." → "Project" 클릭
2. GitHub 저장소 선택: `parkjaeyoon96-svg/forecast_dashboard`
3. "Import" 클릭

### 3. 환경 변수 설정 (중요!)
프로젝트 설정에서 다음 환경 변수들을 추가해야 합니다:

**Snowflake 연결:**
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
```

**Upstash Redis (캐시용):**
```
UPSTASH_REDIS_REST_URL=your_redis_url
UPSTASH_REDIS_REST_TOKEN=your_redis_token
```

### 4. 배포
1. "Deploy" 버튼 클릭
2. 자동으로 빌드 및 배포 진행 (2-3분 소요)
3. 배포 완료 후 URL 제공 (예: `https://forecast-dashboard-xxx.vercel.app`)

### 5. 자동 배포 설정
- GitHub에 push하면 자동으로 Vercel에 배포됨
- Pull Request마다 미리보기 URL 생성

## Upstash Redis 설정 (캐시용)

### 1. Upstash 계정 생성
1. https://upstash.com 접속
2. 무료 계정 생성

### 2. Redis 데이터베이스 생성
1. "Create Database" 클릭
2. 이름: `forecast-cache`
3. 지역: Seoul 선택 (한국)
4. "Create" 클릭

### 3. 연결 정보 복사
1. 생성된 DB 클릭
2. "REST API" 탭에서:
   - `UPSTASH_REDIS_REST_URL` 복사
   - `UPSTASH_REDIS_REST_TOKEN` 복사
3. Vercel 환경 변수에 추가

## 현재 .env 파일 확인

로컬에서 사용하는 `.env` 파일의 내용을 Vercel 환경 변수로 복사해야 합니다:

```bash
# .env 파일 내용 확인
type .env
```

## 배포 후 확인사항

✅ API 작동 확인:
```
https://your-app.vercel.app/api/sales-rate
```

✅ Dashboard 확인:
```
https://your-app.vercel.app/Dashboard.html
```

## 비용
- **Vercel**: 무료 플랜으로 충분 (Hobby Plan)
- **Upstash Redis**: 무료 10,000 commands/day

## 문제 해결

### 404 에러가 계속 나는 경우
1. Vercel 환경 변수가 제대로 설정되었는지 확인
2. Snowflake 연결 정보가 정확한지 확인
3. 배포 로그 확인: Vercel 대시보드 → Deployments → 최신 배포 → "View Function Logs"

### Python 스크립트 실행 오류
- Vercel은 Node.js 환경이므로 Python 실행이 제한적
- 대신 Redis 캐시를 활용하여 로컬에서 데이터 업데이트 후 캐시 사용

