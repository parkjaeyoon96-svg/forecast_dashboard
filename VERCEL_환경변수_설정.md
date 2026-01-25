# Vercel 환경 변수 설정 가이드

## 현재 .env 파일 확인
로컬의 .env 파일에 있는 값들을 Vercel에 복사해야 합니다.

## Vercel 웹 대시보드 방법 (가장 쉬움)

1. https://vercel.com 로그인
2. forecast_dashboard 프로젝트 선택
3. Settings → Environment Variables
4. 아래 변수들을 하나씩 추가:

### Snowflake 연결 (필수)
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
```

### Upstash Redis (선택 - 캐시 성능 향상)
```
UPSTASH_REDIS_REST_URL=https://...
UPSTASH_REDIS_REST_TOKEN=...
```

## Upstash Redis 무료 설정 (5분)

1. https://upstash.com 접속
2. 회사 이메일로 가입
3. "Create Database" 클릭
4. 설정:
   - Name: forecast-cache
   - Region: ap-northeast-2 (Seoul)
   - Type: Regional
5. 생성 후 "REST API" 탭에서:
   - UPSTASH_REDIS_REST_URL 복사
   - UPSTASH_REDIS_REST_TOKEN 복사
6. Vercel 환경 변수에 추가

## 환경 변수 추가 후

1. Vercel Deployments 탭으로 이동
2. 최신 배포 옆의 "..." 메뉴 클릭
3. "Redeploy" 선택
4. 재배포 완료 후 대시보드 접속하여 확인

## 주의사항

- Environment는 **Production, Preview, Development 모두 체크**
- SNOWFLAKE_ACCOUNT 형식: `계정명` (지역 제외)
  예: `abc123` 또는 `abc123.ap-northeast-2`
- 비밀번호에 특수문자 있으면 URL 인코딩 필요 없음 (Vercel이 자동 처리)

## 설정 확인

재배포 후 대시보드 접속:
- F12 (개발자 도구) 열기
- Console 탭에서 에러 확인
- Network 탭에서 /api/sales-rate 요청 확인

