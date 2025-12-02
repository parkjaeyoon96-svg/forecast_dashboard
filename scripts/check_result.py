"""생성된 파일 확인"""
import pandas as pd
import os

base = 'raw/202511/current_year/20251117/'
files = [
    'ke30_20251117_202511_전처리완료.csv',
    'ke30_20251117_202511_Shop_item.csv',
    'ke30_20251117_202511_Shop.csv'
]

print('=' * 60)
print('생성된 파일 확인')
print('=' * 60)

for f in files:
    filepath = base + f
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        print(f'\n{os.path.basename(f)}:')
        print(f'  행 수: {len(df):,}')
        print(f'  컬럼 수: {len(df.columns)}')
        print(f'  컬럼 (처음 10개): {list(df.columns)[:10]}')
        if '직접비 합계' in df.columns:
            print(f'  직접비 합계 총합: {df["직접비 합계"].sum():,.0f}')
        if '직접이익' in df.columns:
            print(f'  직접이익 총합: {df["직접이익"].sum():,.0f}')
    else:
        print(f'\n{f}: 파일 없음')







