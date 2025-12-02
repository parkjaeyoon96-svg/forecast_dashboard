import pandas as pd
import numpy as np

# 평가율 마스터 읽기
eval_master = pd.read_csv('Master/평가율마스터.csv', encoding='utf-8-sig')
print('평가율 마스터 컬럼:', list(eval_master.columns))
print()

# 분석월 202511의 평가감 환입월 = 2025.10
evaluation_month = '2025.10'
month_col = None

for col in eval_master.columns:
    if str(col).strip() == evaluation_month:
        month_col = col
        break
    elif evaluation_month in str(col):
        month_col = col
        break

print(f'평가감 환입월 컬럼: {month_col}')
print()

# 전처리완료 파일 읽기
df = pd.read_csv('raw/202511/current_year/20251124/ke30_20251124_202511_전처리완료.csv', encoding='utf-8-sig')

# 샘플 행 하나 선택 (I 브랜드, 21F 시즌)
sample = df[(df['브랜드'] == 'I') & (df['시즌'] == '21F')].iloc[0]

print('샘플 데이터 (I 브랜드, 21F 시즌):')
print(f"  TAG매출: {sample['합계 : 판매금액(TAG가)']:,.0f}")
print(f"  표준매출원가: {sample['합계 : 표준 매출원가']:,.0f}")
print(f"  재고평가감 환입 (실제값): {sample['재고평가감 환입']:,.0f}")
print()

# 평가율 찾기
if month_col:
    eval_row = eval_master[(eval_master['브랜드'] == 'I') & (eval_master['시즌'] == '21F')]
    if len(eval_row) > 0:
        eval_rate_str = str(eval_row[month_col].values[0]) if pd.notna(eval_row[month_col].values[0]) else '0'
        print(f'평가율 마스터에서 찾은 값: {eval_rate_str}')
        
        # 평가율 변환
        if '%' in eval_rate_str:
            eval_rate = float(eval_rate_str.replace('%', '').replace(' ', '')) / 100
        else:
            try:
                eval_rate = float(eval_rate_str)
                if eval_rate > 1:
                    eval_rate = eval_rate / 100
            except:
                eval_rate = 0
        
        print(f'평가율 (소수): {eval_rate}')
        print()
        
        # 계산
        tag_val = float(sample['합계 : 판매금액(TAG가)'])
        cost_val = float(sample['합계 : 표준 매출원가'])
        
        # 공식: -TAG매출 × ((표준매출원가 / TAG매출 × 1.1) - 평가율) / 1.1
        calculated = -tag_val * ((cost_val / tag_val * 1.1) - eval_rate) / 1.1
        
        print('계산 과정:')
        print(f'  표준매출원가 / TAG매출 × 1.1 = {cost_val / tag_val * 1.1:.6f}')
        print(f'  (표준매출원가 / TAG매출 × 1.1) - 평가율 = {(cost_val / tag_val * 1.1) - eval_rate:.6f}')
        print(f'  계산된 재고평가감 환입 = {calculated:,.0f}')
        print(f'  실제 재고평가감 환입 = {sample["재고평가감 환입"]:,.0f}')
        print()
        
        # 평가율이 0인 경우 계산
        if eval_rate == 0:
            print('⚠️  평가율이 0입니다!')
            print(f'  평가율이 0이면: -표준매출원가 = {-cost_val:,.0f}')
    else:
        print('평가율 마스터에서 I 브랜드, 21F 시즌을 찾을 수 없습니다.')
else:
    print(f'평가감 환입월 컬럼({evaluation_month})을 찾을 수 없습니다.')



