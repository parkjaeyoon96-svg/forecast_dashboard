# 가중치 진척율 계산 방식 변경 - 테스트 가이드

## 개요

전년 누적 매출 기반 진척율에서 요일/명절 계수 기반 가중치 진척율 방식으로 변경되었습니다.

## 변경 내용 요약

### AS-IS (기존)
- 전년 동일자 누적 매출 기반 진척일수 계산
- Snowflake에서 전년 누적 매출 다운로드 필요
- 브랜드별 진척일수 적용

### TO-BE (신규)
- 요일계수 + 명절계수 기반 가중치 진척율 계산
- 로컬 Master 파일만 사용 (Snowflake 조회 불필요)
- 전 브랜드 동일한 진척율 적용

## 테스트 시나리오

### 사전 준비
np
1. **Master 파일 확인**
   ```
   Master/명절계수.csv  - 2026년 설날/추석 날짜가 정확히 반영되어 있는지 확인
   Master/요일계수.csv  - Mon~Sun 7개 요일의 계수가 있는지 확인
   ```

2. **기존 데이터 백업** (선택사항)
   ```
   raw/202601/previous_year/progress_days_202601.csv (기존 방식)
   ```

### 테스트 1: 가중치 진척율 계산 단독 테스트

**목적**: 새로운 진척율 계산 스크립트가 정상 작동하는지 확인

**실행 명령**:
```bash
python scripts/calculate_weighted_progress_rate.py 202601
```

**예상 결과**:
```
✅ 명절계수 파일 로드 완료: XXX개 데이터
✅ 요일계수 파일 로드 완료: 7개 요일
✅ 진척율 계산 완료
   월말계수 합계: XX.XXXXXX
   명절 적용 일수: XX일
   요일 적용 일수: XX일
✅ CSV 파일 저장 완료: raw/202601/progress_rate/weighted_progress_rate_202601.csv
```

**검증 포인트**:
1. `raw/202601/progress_rate/weighted_progress_rate_202601.csv` 파일이 생성되었는가?
2. 파일을 열어서 다음을 확인:
   - 1월 1일~31일까지 31개 행이 있는가?
   - 진척율이 점진적으로 증가하는가? (1일: 약 3%, 31일: 100%)
   - 명절 날짜(1/1~1/19 설날 기간)에 '명절' 구분이 적용되었는가?
   - 그 외 날짜에 '요일' 구분이 적용되었는가?

**예상 출력 예시** (CSV 내용):
```
월,일,요일,계수구분,계수,월말계수,진척계수,진척율
1,1,Wed,명절,1.414577,31.234567,1.414577,0.045289
1,2,Thu,명절,1.380821,31.234567,2.795398,0.089512
...
1,31,Sat,요일,1.093155,31.234567,31.234567,1.000000
```

---

### 테스트 2: 전년계획 업데이트 배치 전체 실행

**목적**: 전체 워크플로우가 정상 작동하는지 확인

**실행 명령**:
```bash
전년계획_업데이트.bat
```

**입력값**: `202601` (또는 테스트할 분석월)

**예상 결과**:
```
[Step 1/4] Downloading previous year data (Snowflake)...
✅ Snowflake 연결 성공!
...

[Step 2/4] Processing previous year data...
✅ 전년 데이터 처리 완료
...

[Step 3/4] Processing plan data...
✅ 계획 데이터 처리 완료
...

[Step 4/4] Calculating weighted progress rate...
✅ 진척율 계산 완료
...

============================================================
  COMPLETED!
============================================================

Generated files are in raw/202601/previous_year/ and raw/202601/plan/
```

**검증 포인트**:
1. Step 4가 "Calculating weighted progress rate"로 표시되는가?
2. Step 5가 삭제되었는가? (기존 "Calculate progress days" 없어야 함)
3. 에러 없이 완료되었는가?
4. `raw/202601/progress_rate/weighted_progress_rate_202601.csv` 파일이 생성되었는가?

---

### 테스트 3: KE30 → Forecast 변환 테스트

**목적**: 새로운 진척율 방식으로 예측이 정상 작동하는지 확인

**전제 조건**:
- `raw/202601/current_year/20260112/` 폴더에 KE30 파일이 있어야 함
- `raw/202601/progress_rate/weighted_progress_rate_202601.csv` 파일이 있어야 함

**실행 명령**:
```bash
python scripts/convert_ke30_to_forecast.py 20260112
```

**예상 결과**:
```
============================================================
KE30 → Forecast 변환
============================================================
[INFO] 업데이트일자: 2026-01-12
[INFO] 실제 매출 기간 종료일: 2026-01-11
[INFO] 분석월: 202601
[INFO] 월 총 일수: 31일

[가중치 진척율 파일 읽기] 시작...
[OK] 진척율 로드 완료: XX.XXXX%
   전 브랜드 동일한 진척율 사용

[변환 시작] ke30_20260112_202601_Shop.csv -> forecast_20260112_202601_Shop.csv
...
[OK] 변환 완료
...
```

**검증 포인트**:
1. "전 브랜드 동일한 진척율 사용" 메시지가 출력되는가?
2. 브랜드별 진척일수 메시지가 없어졌는가? (기존에는 "M(면세): XX일" 등)
3. Forecast 파일이 정상 생성되었는가?
4. Forecast 매출액이 KE30 매출액보다 큰가? (진척율로 나누므로 증가해야 함)

**데이터 검증**:
```python
import pandas as pd

# KE30 원본
ke30 = pd.read_csv('raw/202601/current_year/20260112/ke30_20260112_202601_Shop.csv', encoding='utf-8-sig')

# Forecast 결과
forecast = pd.read_csv('raw/202601/current_year/20260112/forecast_20260112_202601_Shop.csv', encoding='utf-8-sig')

# 매출액 컬럼 비교 (예: '합계 : 실판매액(V-)')
print("KE30 총 매출:", ke30['합계 : 실판매액(V-)'].sum())
print("Forecast 총 매출:", forecast['합계 : 실판매액(V-)'].sum())
print("증가 배율:", forecast['합계 : 실판매액(V-)'].sum() / ke30['합계 : 실판매액(V-)'].sum())
# 증가 배율이 진척율의 역수와 유사해야 함 (예: 진척율 35% → 배율 약 2.86)
```

---

### 테스트 4: 에러 처리 테스트

#### 4-1. 진척율 파일 없을 때

**시나리오**: 전년계획 업데이트를 실행하지 않고 바로 Forecast 변환 시도

**실행 명령**:
```bash
# 진척율 파일 임시 이름 변경 (백업)
rename raw\202601\progress_rate\weighted_progress_rate_202601.csv weighted_progress_rate_202601.csv.bak

# Forecast 변환 시도
python scripts/convert_ke30_to_forecast.py 20260112
```

**예상 결과**:
```
[ERROR] 오류 발생: 진척율 정보가 없습니다. 전년 계획 업데이트 배치파일을 실행하여 진척율을 업데이트하세요.
필요 파일: raw\202601\progress_rate\weighted_progress_rate_202601.csv
```

**복구**:
```bash
# 파일 이름 복구
rename raw\202601\progress_rate\weighted_progress_rate_202601.csv.bak weighted_progress_rate_202601.csv
```

#### 4-2. 비정상 진척율 테스트 (선택사항)

**시나리오**: 진척율 파일에 비정상 값이 있을 때

이 테스트는 CSV를 수동으로 수정해야 하므로 선택사항입니다.

---

### 테스트 5: 월말 완료 케이스 테스트

**시나리오**: 2월 2일에 1월 전체 데이터 처리

**전제 조건**:
- 업데이트일자: 20260202 (2월 2일)
- 분석월: 202601 (1월)
- 실제 매출 기간: 1/1 ~ 1/31 (전체)

**실행 명령**:
```bash
python scripts/convert_ke30_to_forecast.py 20260202
```

**예상 결과**:
```
[INFO] 업데이트일자: 2026-02-02
[INFO] 실제 매출 기간 종료일: 2026-02-01  (실제로는 1월 전체 사용)
[INFO] 분석월: 202601
[OK] 진척율 로드 완료: 100.0000%
   전 브랜드 동일한 진척율 사용
```

**검증 포인트**:
1. 진척율이 100%로 표시되는가?
2. Forecast 매출액 = KE30 매출액 (진척율 100%이므로 동일해야 함)

---

### 테스트 6: 채널명 "미지정" 처리 확인

**시나리오**: 미지정 채널 데이터는 진척율 적용하지 않음

**검증 방법**:
```python
import pandas as pd

# KE30 원본
ke30 = pd.read_csv('raw/202601/current_year/20260112/ke30_20260112_202601_Shop.csv', encoding='utf-8-sig')

# Forecast 결과
forecast = pd.read_csv('raw/202601/current_year/20260112/forecast_20260112_202601_Shop.csv', encoding='utf-8-sig')

# 미지정 채널 필터링
ke30_unknown = ke30[ke30['채널명'] == '미지정']
forecast_unknown = forecast[forecast['채널명'] == '미지정']

# 매출액 비교 (동일해야 함)
print("미지정 채널:")
print("KE30 매출:", ke30_unknown['합계 : 실판매액(V-)'].sum())
print("Forecast 매출:", forecast_unknown['합계 : 실판매액(V-)'].sum())
print("동일 여부:", ke30_unknown['합계 : 실판매액(V-)'].sum() == forecast_unknown['합계 : 실판매액(V-)'].sum())
```

**예상 결과**: 미지정 채널의 매출액은 KE30와 Forecast가 동일해야 함

---

## 통합 테스트 체크리스트

### ✅ 필수 테스트
- [ ] 테스트 1: 가중치 진척율 계산 단독 테스트
- [ ] 테스트 2: 전년계획 업데이트 배치 전체 실행
- [ ] 테스트 3: KE30 → Forecast 변환 테스트
- [ ] 테스트 4-1: 진척율 파일 없을 때 에러 처리

### 📝 권장 테스트
- [ ] 테스트 5: 월말 완료 케이스 테스트
- [ ] 테스트 6: 채널명 "미지정" 처리 확인

### 🔍 추가 검증 (선택)
- [ ] 여러 분석월에 대해 테스트 (202601, 202602, 202603 등)
- [ ] 기존 방식과 신규 방식의 예측 결과 비교
- [ ] 성능 비교 (Snowflake 조회 제거로 인한 속도 향상 확인)

---

## 주요 변경 파일 목록

### 신규 생성
- `scripts/calculate_weighted_progress_rate.py` - 가중치 진척율 계산 스크립트

### 수정
- `전년계획_업데이트.bat` - Step 4/5 변경
- `scripts/run_previous_year_plan_update.py` - Step 4/5 변경
- `scripts/convert_ke30_to_forecast.py` - 진척율 방식 변경

### 삭제
- `scripts/download_previous_year_cumulative_sales.py` - Snowflake 조회 불필요
- `scripts/calculate_progress_days.py` - 전년 매출 기반 진척일수 계산 불필요

---

## 트러블슈팅

### 문제 1: 명절계수 파일을 찾을 수 없습니다
**원인**: Master/명절계수.csv 파일이 없거나 경로가 잘못됨
**해결**: Master 폴더에 명절계수.csv와 요일계수.csv 파일이 있는지 확인

### 문제 2: 진척율 파일에 XX일 데이터가 없습니다
**원인**: 윤년/평년 차이로 2월 29일이 없는 경우
**해결**: 명절계수.csv에 해당 연도의 올바른 날짜 데이터가 있는지 확인

### 문제 3: 비정상적인 진척율입니다
**원인**: 월말계수 합이 0이거나 데이터 오류
**해결**: 
1. Master/명절계수.csv와 Master/요일계수.csv 데이터 확인
2. 계수 값이 모두 양수인지 확인
3. 해당 월의 모든 날짜에 계수가 할당되었는지 확인

### 문제 4: Forecast 매출이 비정상적으로 크거나 작음
**원인**: 진척율 계산 오류 또는 업데이트일자 오류
**해결**:
1. 진척율 CSV 파일 확인 (진척율이 0~1 범위인지)
2. 업데이트일자와 분석월이 올바른지 확인
3. 실제 매출 기간이 올바른지 확인

---

## 배치 실행 순서 (중요)

### 월초 1회 실행
```
전년계획_업데이트.bat
↓
raw/{YYYYMM}/progress_rate/weighted_progress_rate_{YYYYMM}.csv 생성
```

### 주차별 반복 실행
```
당년데이터_처리실행.bat
↓
KE30 → Forecast 변환 시 진척율 파일 참조
```

**주의**: 전년계획 업데이트를 먼저 실행하지 않으면 당년 데이터 처리 시 에러 발생!

---

## 성공 기준

1. ✅ 전년계획 업데이트 배치가 에러 없이 완료됨
2. ✅ 진척율 CSV 파일이 정상 생성됨 (31개 행, 진척율 0~1 범위)
3. ✅ KE30 → Forecast 변환이 정상 작동함
4. ✅ Forecast 매출액이 합리적인 범위 내에 있음
5. ✅ 미지정 채널은 진척율이 적용되지 않음
6. ✅ 에러 처리가 명확한 메시지로 안내됨

---

## 문의 및 지원

테스트 중 문제가 발생하면 다음 정보를 수집하여 문의하세요:

1. 실행한 명령어
2. 에러 메시지 전문
3. 분석월 및 업데이트일자
4. 관련 CSV 파일 경로 및 내용 일부

테스트를 성공적으로 완료하시길 바랍니다! 🎉


















