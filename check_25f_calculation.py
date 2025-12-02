"""
25F 시즌 재고평가감 환입 계산 확인
"""
import pandas as pd
import numpy as np

# 데이터 로드
df = pd.read_csv('raw/202511/current_year/20251117/ke30_20251117_202511_전처리완료.csv', encoding='utf-8-sig')

# 브랜드 I, 25F 시즌 필터링
filtered = df[(df['브랜드'] == 'I') & (df['시즌'] == '25F')].copy()

print("=" * 80)
print("브랜드 I, 25F 시즌 재고평가감 환입 계산 확인")
print("=" * 80)
print(f"총 행 수: {len(filtered)}")
print()

# 평가율 확인
evaluation_master = pd.read_csv('Master/평가율마스터.csv', encoding='utf-8-sig')
eval_row = evaluation_master[(evaluation_master['브랜드'] == 'I') & (evaluation_master['시즌'] == '25F')]
if len(eval_row) > 0 and '2025.10' in eval_row.columns:
    eval_rate_str = str(eval_row['2025.10'].values[0])
    eval_rate = float(eval_rate_str.replace('%', '')) / 100 if '%' in eval_rate_str else float(eval_rate_str)
    print(f"평가율 (2025.10): {eval_rate_str} = {eval_rate}")
else:
    eval_rate = 0
    print(f"평가율: 찾을 수 없음 (0으로 설정)")

print()

# 컬럼명
TAG_col = '합계 : 판매금액(TAG가)'
표준매출원가_col = '합계 : 표준 매출원가'
재고평가감환입_col = '재고평가감 환입'

# 전체 합계
total_TAG = filtered[TAG_col].sum()
total_표준매출원가 = filtered[표준매출원가_col].sum()
actual_total = filtered[재고평가감환입_col].sum()

print("=" * 80)
print("전체 합계:")
print("=" * 80)
print(f"TAG매출 합계: {total_TAG:,.0f}")
print(f"표준매출원가 합계: {total_표준매출원가:,.0f}")
print(f"실제 재고평가감 환입 합계: {actual_total:,.0f}")
print()

# 이론적 계산
print("=" * 80)
print("이론적 계산 (전체 합계로):")
print("=" * 80)
theoretical = -total_표준매출원가 + total_TAG * eval_rate / 1.1
print(f"-표준매출원가 + TAG매출 * 평가율/1.1")
print(f"= -{total_표준매출원가:,.0f} + {total_TAG:,.0f} * {eval_rate}/1.1")
print(f"= {theoretical:,.2f}")
print()

# 각 행별 계산 후 합산
print("=" * 80)
print("각 행별 계산 후 합산:")
print("=" * 80)
calculated_total = 0
for idx, row in filtered.iterrows():
    TAG매출 = float(row[TAG_col]) if pd.notna(row[TAG_col]) else 0
    표준매출원가 = float(row[표준매출원가_col]) if pd.notna(row[표준매출원가_col]) else 0
    
    if TAG매출 == 0:
        result = 0
    else:
        part1 = (표준매출원가 / TAG매출 * 1.1) - eval_rate
        result = -TAG매출 * part1 / 1.1
        result = int(round(result, 1))
    
    calculated_total += result

print(f"각 행별 계산 후 합산: {calculated_total:,.0f}")
print(f"실제 저장된 값: {actual_total:,.0f}")
print(f"차이: {calculated_total - actual_total:,.0f}")
print()

# 샘플 데이터 (처음 10행)
print("=" * 80)
print("샘플 데이터 (처음 10행):")
print("=" * 80)
print(f"{'행':<5} {'TAG매출':>15} {'표준매출원가':>15} {'계산 결과':>15}")
print("-" * 50)
for idx, row in filtered.head(10).iterrows():
    TAG매출 = float(row[TAG_col]) if pd.notna(row[TAG_col]) else 0
    표준매출원가 = float(row[표준매출원가_col]) if pd.notna(row[표준매출원가_col]) else 0
    저장된값 = float(row[재고평가감환입_col]) if pd.notna(row[재고평가감환입_col]) else 0
    
    if TAG매출 == 0:
        result = 0
    else:
        part1 = (표준매출원가 / TAG매출 * 1.1) - eval_rate
        result = -TAG매출 * part1 / 1.1
        result = int(round(result, 1))
    
    print(f"{idx:<5} {TAG매출:>15,.0f} {표준매출원가:>15,.0f} {result:>15,}")






