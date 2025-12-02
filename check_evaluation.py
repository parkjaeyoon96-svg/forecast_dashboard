import pandas as pd
import numpy as np

# 파일 읽기
df = pd.read_csv('raw/202511/current_year/20251124/ke30_20251124_202511_전처리완료.csv', encoding='utf-8-sig')

print('=' * 80)
print('재고평가감 환입 및 매출원가 확인')
print('=' * 80)
print(f'총 행 수: {len(df)}')
print()

# 재고평가감 환입 통계
print('재고평가감 환입 통계:')
print(f'  0인 행: {(df["재고평가감 환입"] == 0).sum()}')
print(f'  0이 아닌 행: {(df["재고평가감 환입"] != 0).sum()}')
print(f'  합계: {df["재고평가감 환입"].sum():,.0f}')
print(f'  최소값: {df["재고평가감 환입"].min():,.0f}')
print(f'  최대값: {df["재고평가감 환입"].max():,.0f}')
print()

# 매출원가(평가감환입반영) 통계
print('매출원가(평가감환입반영) 통계:')
print(f'  0인 행: {(df["매출원가(평가감환입반영)"] == 0).sum()}')
print(f'  0이 아닌 행: {(df["매출원가(평가감환입반영)"] != 0).sum()}')
print(f'  합계: {df["매출원가(평가감환입반영)"].sum():,.0f}')
print()

# 브랜드별 집계
print('브랜드별 집계:')
for brand in df['브랜드'].unique():
    if pd.isna(brand):
        continue
    brand_df = df[df['브랜드'] == brand]
    tag_sum = brand_df['합계 : 판매금액(TAG가)'].sum()
    cost_sum = brand_df['합계 : 표준 매출원가'].sum()
    jeonganbi_sum = brand_df['표준제간비'].sum()
    eval_sum = brand_df['재고평가감 환입'].sum()
    final_cost_sum = brand_df['매출원가(평가감환입반영)'].sum()
    
    print(f'\n{brand}:')
    print(f'  TAG매출: {tag_sum:,.0f}')
    print(f'  표준매출원가: {cost_sum:,.0f}')
    print(f'  표준제간비: {jeonganbi_sum:,.0f}')
    print(f'  재고평가감 환입: {eval_sum:,.0f}')
    print(f'  매출원가(평가감환입반영): {final_cost_sum:,.0f}')
    print(f'  계산값 (표준+제간비+환입): {cost_sum + jeonganbi_sum + eval_sum:,.0f}')

# 샘플 데이터 확인
print('\n' + '=' * 80)
print('샘플 데이터 (재고평가감 환입이 0이 아닌 행 5개):')
print('=' * 80)
non_zero = df[df['재고평가감 환입'] != 0].head(5)
if len(non_zero) > 0:
    print(non_zero[['브랜드', '시즌', '합계 : 판매금액(TAG가)', '합계 : 표준 매출원가', '표준제간비', '재고평가감 환입', '매출원가(평가감환입반영)']].to_string())
else:
    print('재고평가감 환입이 0이 아닌 행이 없습니다.')



