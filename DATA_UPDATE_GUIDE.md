# 📊 대시보드 데이터 업데이트 가이드

## 🎯 데이터 업데이트 절차

### 간단 버전 (추천!)

1. **SAP KE30 CSV 다운로드**
   - SAP에서 KE30 거래 실행
   - 결과를 CSV로 내보내기
   - `raw/sap_ke30.csv` 파일로 저장

2. **배치 파일 실행**
   ```
   update_dashboard.bat 더블클릭
   ```

3. **완료!**
   - 1-2분 후 대시보드 자동 배포

---

## 📁 데이터 소스 상세

### 1. SAP KE30 데이터 (필수)
- **위치**: `raw/sap_ke30.csv`
- **내용**: 자재별 손익 데이터
- **업데이트**: 수동 (SAP에서 다운로드)
- **형식**: CSV (cp949 또는 UTF-8 인코딩)

**필수 컬럼 (예시):**
```
자재코드, 자재명, 수량, 매출액, 매출원가, 판매관리비, 영업이익
```

### 2. Snowflake 데이터 (선택)
- **위치**: `raw/snowflake_data.csv`
- **내용**: 추가 분석 데이터
- **업데이트**: 자동 (스크립트 실행 시)
- **형식**: CSV

### 3. 마스터 데이터 (선택)
- **위치**: `raw/master_data.xlsx` 또는 `.csv`
- **내용**: 제품, 고객, 부서 등 기준정보
- **업데이트**: 수동 (Excel 편집)
- **형식**: Excel 멀티시트 또는 CSV

---

## 🛠️ 수동 실행 방법

### Windows PowerShell
```powershell
# 1. 데이터 처리
python scripts\update_all.py

# 2. Git 푸시
git add .
git commit -m "데이터 업데이트"
git push
```

### 개별 스크립트 실행
```powershell
# KE30 데이터만 처리
python scripts\process_ke30.py

# Snowflake 데이터만 가져오기
python scripts\fetch_snowflake.py

# 마스터 데이터만 처리
python scripts\process_master_data.py
```

---

## ⚙️ KE30 전처리 커스터마이징

`scripts/process_ke30.py` 파일을 수정하여 전처리 로직을 변경할 수 있습니다.

### 1. 컬럼명 매칭
실제 SAP CSV 컬럼명에 맞게 수정:

```python
# scripts/process_ke30.py 파일 내
agg_config = {
    '매출액': 'sum',        # 실제 컬럼명으로 변경
    '매출원가': 'sum',      # 실제 컬럼명으로 변경
    '판매수량': 'sum',      # 실제 컬럼명으로 변경
}
```

### 2. 집계 기준 변경
자재별 외에 다른 기준으로 집계:

```python
# 자재 + 일자별 집계
group_columns = ['자재코드', '자재명', '일자']

# 카테고리별 집계
group_columns = ['카테고리', '제품군']
```

### 3. 계산 컬럼 추가
필요한 KPI 추가:

```python
# 이익률 계산
df['이익률'] = (df['영업이익'] / df['매출액'] * 100).round(2)

# 원가율 계산
df['원가율'] = (df['매출원가'] / df['매출액'] * 100).round(2)

# 누적 합계
df['누적매출'] = df.groupby('자재코드')['매출액'].cumsum()
```

---

## 🔍 문제 해결

### 오류: "파일을 찾을 수 없습니다"
```
❌ 파일을 찾을 수 없습니다: raw/sap_ke30.csv
```

**해결:**
1. SAP에서 KE30 데이터를 CSV로 다운로드
2. `raw/sap_ke30.csv` 경로에 저장
3. 파일명이 정확한지 확인

### 오류: "인코딩 오류"
```
❌ UnicodeDecodeError: 'utf-8' codec can't decode
```

**해결:**
`process_ke30.py`에서 인코딩 변경:
```python
df = pd.read_csv(file_path, encoding='cp949')  # 또는 'euc-kr'
```

### 오류: "컬럼을 찾을 수 없습니다"
```
❌ KeyError: '매출액'
```

**해결:**
1. `raw/sap_ke30_sample.csv` 샘플 파일 확인
2. 실제 CSV의 컬럼명 확인
3. `process_ke30.py`에서 컬럼명 수정

### Git 푸시 실패
```
❌ Git 푸시 실패
```

**해결:**
```powershell
# Git 상태 확인
git status

# 수동으로 푸시
git push

# 인증이 필요한 경우
# GitHub 로그인 창이 뜰 것입니다
```

---

## 📊 데이터 흐름

```
1. 원본 데이터
   ├── raw/sap_ke30.csv          (SAP에서 다운로드)
   ├── raw/snowflake_data.csv    (자동 다운로드)
   └── raw/master_data.xlsx      (Excel로 편집)
   
2. 전처리 스크립트 실행
   └── python scripts/update_all.py
   
3. 처리된 데이터
   ├── data/ke30_processed.json
   ├── data/snowflake_data.json
   ├── data/master_data.json
   └── data/metadata.json
   
4. Git 푸시
   └── git push
   
5. Vercel 자동 배포
   └── 1-2분 후 대시보드 업데이트
```

---

## 💡 팁

### 1. 정기 업데이트
매주/매월 같은 날에 업데이트하면 관리가 편합니다.

### 2. 데이터 백업
중요한 데이터는 Git에 자동으로 백업됩니다.
```powershell
# 이전 버전 확인
git log --oneline

# 특정 버전으로 되돌리기
git checkout <commit-id> -- data/
```

### 3. 샘플 데이터로 테스트
실제 데이터 업로드 전에 샘플로 테스트:
```powershell
# 샘플 파일 복사
copy raw\sap_ke30_sample.csv raw\sap_ke30.csv

# 테스트 실행
python scripts\process_ke30.py
```

### 4. 로그 확인
오류 발생 시 상세 로그 확인:
```powershell
python scripts\update_all.py > update_log.txt 2>&1
notepad update_log.txt
```

---

## 📞 문의

문제가 해결되지 않으면 다음 정보를 확인하세요:
- `data/metadata.json` - 마지막 업데이트 정보
- Python 버전: `python --version` (3.8 이상 필요)
- 필수 패키지: `pip install -r requirements.txt`














