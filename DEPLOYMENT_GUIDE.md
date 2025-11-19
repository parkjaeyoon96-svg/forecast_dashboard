# 🚀 배포 가이드 - 완전 초보자용

이 가이드는 GitHub와 Vercel을 처음 사용하는 분들을 위한 상세한 설명서입니다.

---

## 📋 사전 준비

### 필요한 것들
- ✅ GitHub 계정 (없으면 [여기서 가입](https://github.com/join))
- ✅ Git 설치됨 (이미 완료!)
- ✅ 현재 프로젝트 폴더

---

## 1단계: GitHub 저장소 만들기 (5분)

### 1-1. GitHub 웹사이트 접속
1. 브라우저에서 [https://github.com](https://github.com) 접속
2. 로그인 (계정이 없다면 회원가입)

### 1-2. 새 저장소 생성
1. 우측 상단의 **`+`** 버튼 클릭
2. **`New repository`** 선택
3. 다음 정보 입력:
   ```
   Repository name: project-forecast
   Description: 매주 업데이트되는 판매 예측 대시보드
   
   ⭐ 중요: Public 선택 (Private는 유료 배포 필요)
   ⭐ 중요: "Initialize this repository with a README" 체크 해제
   ```
4. **`Create repository`** 버튼 클릭

### 1-3. 저장소 URL 복사
생성 후 나오는 URL을 복사하세요:
```
https://github.com/당신의아이디/project-forecast.git
```

---

## 2단계: Git 설정 및 업로드 (5분)

### 2-1. PowerShell 열기
- VS Code에서 터미널이 이미 열려있으면 사용
- 또는 프로젝트 폴더에서 우클릭 → "터미널에서 열기"

### 2-2. Git 사용자 정보 설정 (처음 한 번만)
```powershell
git config --global user.name "본인이름"
git config --global user.email "본인이메일@gmail.com"
```

**예시:**
```powershell
git config --global user.name "Kim Minsu"
git config --global user.email "minsu.kim@gmail.com"
```

### 2-3. 프로젝트 폴더 확인
```powershell
# 현재 위치 확인
pwd

# 프로젝트 폴더로 이동 (필요시)
cd C:\Users\AD0283\Desktop\AIproject\Project_Forcast
```

### 2-4. Git 저장소 초기화
```powershell
git init
```

**결과:** `Initialized empty Git repository` 메시지가 나오면 성공!

### 2-5. 모든 파일 추가
```powershell
git add .
```

**설명:** 현재 폴더의 모든 파일을 Git에 추가합니다.

### 2-6. 첫 커밋 생성
```powershell
git commit -m "Initial commit: 프로젝트 예측 대시보드"
```

**결과:** 파일 개수와 변경 사항이 표시되면 성공!

### 2-7. GitHub 저장소 연결
```powershell
git remote add origin https://github.com/당신의아이디/project-forecast.git
```

**⚠️ 주의:** `당신의아이디`를 실제 GitHub 아이디로 바꾸세요!

**예시:**
```powershell
git remote add origin https://github.com/minsukim/project-forecast.git
```

### 2-8. 메인 브랜치로 변경
```powershell
git branch -M main
```

### 2-9. GitHub에 업로드
```powershell
git push -u origin main
```

**처음 푸시 시 GitHub 로그인 요청:**
- 브라우저가 열리면 로그인
- 또는 Username/Password 입력
- **2단계 인증이 있다면:** Personal Access Token 필요 (아래 참고)

---

### 🔐 Personal Access Token 만들기 (2단계 인증 사용 시)

1. GitHub 웹사이트 → 우측 상단 프로필 → **Settings**
2. 좌측 맨 아래 **Developer settings**
3. **Personal access tokens** → **Tokens (classic)**
4. **Generate new token** → **Generate new token (classic)**
5. 설정:
   ```
   Note: Project Forecast Push
   Expiration: 90 days
   Select scopes: ✅ repo (전체 선택)
   ```
6. **Generate token** 클릭
7. 생성된 토큰 복사 (한 번만 보여짐!)
8. PowerShell에서 비밀번호 대신 이 토큰 입력

---

## 3단계: GitHub에서 확인 (1분)

1. GitHub 저장소 페이지 새로고침
2. 파일들이 모두 업로드되었는지 확인
3. `README.md` 파일이 보이면 성공!

---

## 4단계: Vercel 연결 및 배포 (3분)

### 4-1. Vercel 가입
1. [https://vercel.com](https://vercel.com) 접속
2. **Sign Up** 클릭
3. **Continue with GitHub** 선택 (추천!)
4. GitHub 권한 승인

### 4-2. 프로젝트 Import
1. Vercel 대시보드에서 **Add New...** 클릭
2. **Project** 선택
3. **Import Git Repository** 섹션에서:
   - `project-forecast` 저장소 찾기
   - **Import** 클릭

### 4-3. 프로젝트 설정
```
Project Name: project-forecast (그대로 유지)
Framework Preset: Other (선택)
Root Directory: . (기본값)
Build Command: (비워두기)
Output Directory: (비워두기)
```

### 4-4. 환경 변수 (선택사항)
필요 없으면 스킵하고 **Deploy** 클릭!

### 4-5. 배포 시작! 🎉
- **Deploy** 버튼 클릭
- 약 30초~1분 대기
- 축하 화면이 나오면 성공!

---

## 5단계: 배포 완료 확인 (1분)

### 5-1. 배포된 URL 확인
```
https://project-forecast-xxx.vercel.app
```

### 5-2. 웹사이트 열기
1. **Visit** 버튼 클릭
2. 또는 URL 직접 입력
3. 인덱스 페이지가 보이면 성공!

---

## 🎯 이제 완료되었습니다!

### ✅ 확인 사항
- [x] GitHub에 코드 업로드 완료
- [x] Vercel 배포 완료
- [x] 웹사이트 접속 가능

### 🔄 이제부터 업데이트 방법

#### 방법 1: 파일 수정 후 자동 배포
```powershell
# 파일 수정 후...
git add .
git commit -m "업데이트 내용 설명"
git push
```
→ Vercel이 자동으로 배포합니다! (30초 소요)

#### 방법 2: 매주 자동 실행 (GitHub Actions)
- 매주 월요일 오전 9시 자동 실행
- 새로운 주차 페이지 자동 생성
- 자동으로 배포까지 완료

---

## 📱 URL 구조

배포 후 다음과 같은 URL로 접근할 수 있습니다:

```
메인 페이지 (인덱스):
https://project-forecast-xxx.vercel.app/

주차별 페이지:
https://project-forecast-xxx.vercel.app/pages/202411_week1.html
https://project-forecast-xxx.vercel.app/pages/202411_week2.html
https://project-forecast-xxx.vercel.app/pages/202411_week3.html
...
```

---

## 🎨 커스텀 도메인 연결 (선택사항)

### 본인 도메인이 있다면:
1. Vercel 프로젝트 → **Settings** → **Domains**
2. **Add** 클릭
3. 도메인 입력 (예: `dashboard.mycompany.com`)
4. DNS 설정 안내에 따라 진행

---

## 🐛 문제 해결

### Q: `git push` 시 권한 오류
**A:** Personal Access Token 사용 (위 가이드 참고)

### Q: Vercel 배포는 성공했는데 페이지가 안 보여요
**A:** 
1. `index.html` 파일이 프로젝트 루트에 있는지 확인
2. 브라우저 캐시 지우기 (Ctrl + Shift + R)
3. Vercel 대시보드 → Functions → Logs 확인

### Q: GitHub Actions가 실행 안 돼요
**A:**
1. GitHub 저장소 → **Settings** → **Actions** → **General**
2. "Allow all actions and reusable workflows" 선택
3. **Save** 클릭

### Q: 주간 페이지가 자동 생성 안 돼요
**A:**
```powershell
# 수동으로 실행해보기
cd scripts
python generate_weekly_pages.py --current
```

---

## 📞 추가 도움이 필요하신가요?

### 유용한 링크
- [GitHub 가이드 (한글)](https://docs.github.com/ko)
- [Vercel 문서](https://vercel.com/docs)
- [Git 튜토리얼](https://git-scm.com/book/ko/v2)

### 자주 사용하는 Git 명령어
```powershell
# 현재 상태 확인
git status

# 변경사항 확인
git diff

# 커밋 히스토리 보기
git log

# 최신 코드 가져오기
git pull

# 브랜치 확인
git branch
```

---

**🎉 축하합니다! 이제 전문가처럼 배포하고 관리할 수 있습니다!**















