# 프로젝트 파일 정리 가이드

## ✅ 삭제 가능한 파일 목록

### 1. HTML 파일 (루트)
```
❌ Dashboard.html (루트)           → public/Dashboard.html과 중복
❌ Dashboard_demo.html             → 데모용, 미사용
❌ test_clothing_item_rates.html   → 테스트용
❌ temp_git_version.html           → 임시 파일
❌ clear_localStorage.html         → 개발 도구
```

### 2. JavaScript 파일 (public/)
```
❌ public/data.js                             → JSON으로 대체됨
❌ public/data_20251117.js                    → JSON으로 대체됨
❌ public/data_20251124.js                    → JSON으로 대체됨
❌ public/data_20251201.js                    → JSON으로 대체됨
❌ public/data_20251208.js                    → JSON으로 대체됨
❌ public/data_20251215.js                    → JSON으로 대체됨
❌ public/data_20251222.js                    → JSON으로 대체됨
❌ public/brand_kpi_*.js (모든 날짜)          → JSON으로 대체됨
❌ public/brand_stock_analysis_*.js (모든 날짜) → JSON으로 대체됨
❌ public/treemap_data*.js (모든 날짜)        → JSON으로 대체됨
❌ public/weekly_sales_trend_*.js (모든 날짜)  → JSON으로 대체됨
```

### 3. Python 스크립트 (scripts/) - 중복/미사용
```
❌ scripts/check_data_structure.py          → 테스트용
❌ scripts/check_mlb_royalty_20251201.py    → 특정 날짜 디버그용
❌ scripts/check_mlb_royalty_detail.py      → 디버그용
❌ scripts/check_result.py                  → 테스트용
❌ scripts/cleanup_data_js.py               → 구버전 정리용
❌ scripts/convert_to_js.py                 → JSON으로 전환되어 불필요
❌ scripts/simplify_data_js.py              → 구버전
❌ scripts/simplify_data_js_v2.py           → 구버전
❌ scripts/rebuild_data_js.py               → 구버전
❌ scripts/inject_treemap_script.js         → 구버전
❌ scripts/create_treemap_data.py           → v2로 대체됨
❌ scripts/download_previous_year_treemap.py → download_previous_year_treemap_data.py로 대체
❌ scripts/process_treemap_data.py          → run_treemap_pipeline.py로 통합
```

### 4. 기타 폴더/파일
```
❌ forecast-/ (전체 폴더)           → 이전 프로젝트, 미사용
❌ README - 복사본.md               → 백업본
❌ README - 복사본 (2).md           → 백업본
❌ final_check.py (루트)            → 테스트용
❌ check_25f_calculation.py (루트)  → 테스트용
```

---

## ✅ 반드시 유지해야 하는 파일

### HTML (1개만!)
```
✅ public/Dashboard.html              → 실제 사용되는 대시보드
```

### Python 스크립트 (핵심 파이프라인)
```
✅ scripts/dashboard_json_gen.bat                    → 메인 배치 파일
✅ scripts/generate_dashboard_data.py                → 통합 생성 스크립트
✅ scripts/update_overview_data.py                   → 전체현황 데이터
✅ scripts/update_brand_kpi.py                       → 브랜드 KPI
✅ scripts/update_brand_radar.py                     → 레이더 차트
✅ scripts/create_brand_pl_data.py                   → 브랜드 손익
✅ scripts/process_channel_profit_loss.py            → 채널 손익
✅ scripts/download_weekly_sales_trend.py            → 주차별 매출
✅ scripts/download_brand_stock_analysis.py          → 재고 분석
✅ scripts/generate_brand_stock_analysis.py          → 재고 JSON 생성
✅ scripts/create_treemap_data_v2.py                 → 트리맵 생성 (최신)
✅ scripts/run_treemap_pipeline.py                   → 트리맵 파이프라인
✅ scripts/download_treemap_rawdata.py               → 트리맵 원본 다운
✅ scripts/download_previous_year_treemap_data.py    → 전년 트리맵
✅ scripts/preprocess_treemap_data.py                → 트리맵 전처리
✅ scripts/export_to_json.py                         → JSON 변환
✅ scripts/generate_ai_insights.py                   → AI 인사이트
✅ scripts/process_ke30_current_year.py              → 당년 전처리
✅ scripts/process_previous_year_rawdata.py          → 전년 전처리
✅ scripts/process_plan_data.py                      → 계획 전처리
✅ scripts/path_utils.py                             → 경로 유틸리티
✅ scripts/snowflake_connection.py                   → DB 연결
```

### JSON 데이터
```
✅ public/data/YYYYMMDD/*.json (모든 JSON 파일들)
```

---

## 🎯 정리 명령어

### 1. HTML 파일 정리
```powershell
Remove-Item -Path "Dashboard.html" -Force
Remove-Item -Path "Dashboard_demo.html" -Force
Remove-Item -Path "test_clothing_item_rates.html" -Force
Remove-Item -Path "temp_git_version.html" -Force
Remove-Item -Path "clear_localStorage.html" -Force
```

### 2. 구버전 JS 파일 정리
```powershell
Remove-Item -Path "public\data*.js" -Force
Remove-Item -Path "public\brand_kpi_*.js" -Force
Remove-Item -Path "public\brand_stock_analysis_*.js" -Force
Remove-Item -Path "public\treemap_data*.js" -Force
Remove-Item -Path "public\weekly_sales_trend_*.js" -Force
```

### 3. 불필요한 Python 스크립트 정리
```powershell
Remove-Item -Path "scripts\check_*.py" -Force
Remove-Item -Path "scripts\cleanup_data_js.py" -Force
Remove-Item -Path "scripts\convert_to_js.py" -Force
Remove-Item -Path "scripts\simplify_data_js*.py" -Force
Remove-Item -Path "scripts\rebuild_data_js.py" -Force
Remove-Item -Path "scripts\inject_treemap_script.js" -Force
Remove-Item -Path "scripts\create_treemap_data.py" -Force  # v2 사용
Remove-Item -Path "scripts\download_previous_year_treemap.py" -Force
Remove-Item -Path "scripts\process_treemap_data.py" -Force
```

### 4. 기타 정리
```powershell
Remove-Item -Path "forecast-" -Recurse -Force
Remove-Item -Path "README - 복사본*.md" -Force
Remove-Item -Path "final_check.py" -Force
Remove-Item -Path "check_25f_calculation.py" -Force
```

---

## ⚠️ 주의사항

**정리 전에 반드시:**
1. ✅ Git commit 또는 백업 생성
2. ✅ 배치 파일(`dashboard_json_gen.bat`)이 정상 작동하는지 확인
3. ✅ `public/Dashboard.html`이 올바르게 표시되는지 확인

**정리 후:**
1. ✅ 배치 파일 다시 실행하여 모든 기능 작동 확인
2. ✅ 개발 서버 재시작 (`npm run dev`)
3. ✅ 브라우저에서 대시보드 확인

---

## 📊 정리 효과

**Before:**
- HTML 파일: 6개 (중복)
- JS 파일: 50+ 개 (구버전)
- Python 파일: 60개 (중복 포함)

**After:**
- HTML 파일: 1개 (`public/Dashboard.html`)
- JS 파일: 0개 (JSON으로 대체)
- Python 파일: 30개 (핵심만)

**예상 공간 절약:** 약 200MB 이상
**유지보수 개선:** 파일 혼란 제거, 수정 시 명확한 타겟
