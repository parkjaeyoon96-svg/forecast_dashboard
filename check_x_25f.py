import pandas as pd
import numpy as np

# 평가율 마스터 읽기
eval_master = pd.read_csv('Master/평가율마스터.csv', encoding='utf-8-sig')

# 분석월 202511의 평가감 환입월 = 2025.10
evaluation_month = '2025.10'
month_col = None

# 컬럼 찾기 (2025.10 또는 2025.1)
for col in eval_master.columns:
    col_str = str(col).strip()
    if col_str == evaluation_month:
        month_col = col
        break
    elif evaluation_month in col_str:
        month_col = col
        break
    # "2025.10" -> "2025.1" 형식 매칭
    elif evaluation_month.replace('.10', '.1') == col_str:
        month_col = col
        break

print('=' * 80)
print('브랜드 X, 25F 시즌 재고평가감 환입 예상 계산')
print('=' * 80)
print()

# 평가율 찾기
if month_col:
    print(f'평가감 환입월 컬럼: {month_col}')
    eval_row = eval_master[(eval_master['브랜드'] == 'X') & (eval_master['시즌'] == '25F')]
    if len(eval_row) > 0:
        eval_rate_str = str(eval_row[month_col].values[0]) if pd.notna(eval_row[month_col].values[0]) else None
        print(f'평가율 마스터 값: {eval_rate_str}')
        
        if eval_rate_str and eval_rate_str != 'nan':
            # 평가율 변환
            if '%' in eval_rate_str:
                eval_rate = float(eval_rate_str.replace('%', '').replace(' ', '')) / 100
            else:
                try:
                    eval_rate = float(eval_rate_str)
                    if eval_rate > 1:
                        eval_rate = eval_rate / 100
                except:
                    eval_rate = None
        else:
            eval_rate = None
        
        print(f'평가율 (소수): {eval_rate}')
        print()
    else:
        eval_rate = None
        print('평가율 마스터에서 브랜드 X, 25F 시즌을 찾을 수 없습니다.')
        print()
else:
    eval_rate = None
    print(f'평가감 환입월 컬럼({evaluation_month})을 찾을 수 없습니다.')
    print()

# 전처리완료 파일에서 브랜드 X, 25F 시즌 데이터 확인
df = pd.read_csv('raw/202511/current_year/20251124/ke30_20251124_202511_전처리완료.csv', encoding='utf-8-sig')
x_25f = df[(df['브랜드'] == 'X') & (df['시즌'] == '25F')]

if len(x_25f) > 0:
    print('브랜드 X, 25F 시즌 데이터:')
    tag_sum = x_25f['합계 : 판매금액(TAG가)'].sum()
    cost_sum = x_25f['합계 : 표준 매출원가'].sum()
    current_eval = x_25f['재고평가감 환입'].sum()
    
    print(f'  TAG매출 합계: {tag_sum:,.0f}')
    print(f'  표준매출원가 합계: {cost_sum:,.0f}')
    print(f'  현재 재고평가감 환입 합계: {current_eval:,.0f}')
    print()
    
    # 예상 계산
    print('예상 계산:')
    if eval_rate is None:
        print('  평가율이 없음 → 재고평가감 환입 = 0')
        expected_eval = 0
    else:
        # 공식: -TAG매출 × ((표준매출원가 / TAG매출 × 1.1) - 평가율) / 1.1
        expected_eval = -tag_sum * ((cost_sum / tag_sum * 1.1) - eval_rate) / 1.1
        print(f'  평가율: {eval_rate * 100:.2f}%')
        print(f'  계산: -{tag_sum:,.0f} × (({cost_sum:,.0f} / {tag_sum:,.0f} × 1.1) - {eval_rate:.6f}) / 1.1')
        print(f'  = -{tag_sum:,.0f} × ({cost_sum / tag_sum * 1.1:.6f} - {eval_rate:.6f}) / 1.1')
    
    print(f'  예상 재고평가감 환입: {expected_eval:,.0f}')
    print()
    print(f'  현재 값과의 차이: {expected_eval - current_eval:,.0f}')
else:
    print('브랜드 X, 25F 시즌 데이터를 찾을 수 없습니다.')



