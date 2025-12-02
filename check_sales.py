import pandas as pd

# Forecast 파일 확인
df_forecast = pd.read_csv('raw/202511/current_year/20251124/forecast_20251124_202511_Shop.csv', encoding='utf-8-sig')
row_forecast = df_forecast[(df_forecast['브랜드'] == 'X') & (df_forecast['채널명'] == '백화점')]

if len(row_forecast) > 0:
    print('=== Forecast 파일 (디스커버리 백화점) ===')
    print(f"실판매액: {row_forecast['합계 : 실판매액'].values[0]:,.0f}")
    print(f"실판매액(V-): {row_forecast['합계 : 실판매액(V-)'].values[0]:,.0f}")
    print(f"계산값 (실판매액/1.1): {row_forecast['합계 : 실판매액'].values[0]/1.1:,.0f}")
    print()

# KE30 원본 파일 확인
df_ke30 = pd.read_csv('raw/202511/current_year/20251124/ke30_20251124_202511_Shop.csv', encoding='utf-8-sig')
row_ke30 = df_ke30[(df_ke30['브랜드'] == 'X') & (df_ke30['채널명'] == '백화점')]

if len(row_ke30) > 0:
    print('=== KE30 원본 파일 (디스커버리 백화점) ===')
    print(f"실판매액: {row_ke30['합계 : 실판매액'].values[0]:,.0f}")
    print(f"실판매액(V-): {row_ke30['합계 : 실판매액(V-)'].values[0]:,.0f}")
    print(f"계산값 (실판매액/1.1): {row_ke30['합계 : 실판매액'].values[0]/1.1:,.0f}")



