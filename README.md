# forecast_dashboard

월중손익예측 대시보드

## 프로젝트 개요

이 프로젝트는 월중 손익 예측을 위한 대시보드 시스템입니다.

## 기술 스택

- **Frontend**: Next.js, React, TypeScript
- **Backend**: Python (데이터 전처리)
- **Data Processing**: Pandas

## 주요 기능

- 계획 데이터 전처리 및 분석
- 당년 데이터 전처리 및 분석
- 트리맵 시각화
- 대시보드 데이터 관리

## 시작하기

### 개발 서버 실행

```bash
npm run dev
```

브라우저에서 [http://localhost:3000](http://localhost:3000)을 열어 결과를 확인하세요.

## 데이터 처리

### 계획 데이터 전처리

```bash
python scripts/process_plan_data.py
```

### 당년 데이터 전처리

```bash
python scripts/process_ke30_current_year.py
```

## 배포

Vercel을 사용하여 배포할 수 있습니다.

[Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme)에서 배포하세요.
