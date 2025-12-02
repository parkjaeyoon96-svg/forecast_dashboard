import pandas as pd

# KE30 원본 파일
df_ke30 = pd.read_csv('raw/202511/current_year/20251124/ke30_20251124_202511_Shop.csv', encoding='utf-8-sig')
row_ke30 = df_ke30[(df_ke30['브랜드'] == 'X') & (df_ke30['채널명'] == '백화점')]

# Forecast 파일
df_forecast = pd.read_csv('raw/202511/current_year/20251124/forecast_20251124_202511_Shop.csv', encoding='utf-8-sig')
row_forecast = df_forecast[(df_forecast['브랜드'] == 'X') & (df_forecast['채널명'] == '백화점')]

if len(row_ke30) > 0 and len(row_forecast) > 0:
    print('=== 디스커버리(X) 백화점 채널 금액 확인 ===\n')
    
    # KE30 원본
    ke30_sales = row_ke30['합계 : 실판매액'].values[0]
    ke30_sales_vm = row_ke30['합계 : 실판매액(V-)'].values[0]
    
    # Forecast
    forecast_sales = row_forecast['합계 : 실판매액'].values[0]
    forecast_sales_vm = row_forecast['합계 : 실판매액(V-)'].values[0]
    
    # 진척일수 (X 브랜드)
    progress_days = 22.01
    total_days = 30
    
    # 진척율 계산
    expected_sales = (ke30_sales / progress_days) * total_days
    expected_sales_vm = (ke30_sales_vm / progress_days) * total_days
    
    print(f'[KE30 원본]')
    print(f'  실판매액: {ke30_sales:,.0f}')
    print(f'  실판매액(V-): {ke30_sales_vm:,.0f}')
    print()
    
    print(f'[Forecast 결과]')
    print(f'  실판매액: {forecast_sales:,.0f}')
    print(f'  실판매액(V-): {forecast_sales_vm:,.0f}')
    print()
    
    print(f'[진척율 계산 기대값]')
    print(f'  실판매액: {expected_sales:,.0f} (진척일수: {progress_days}일)')
    print(f'  실판매액(V-): {expected_sales_vm:,.0f} (진척일수: {progress_days}일)')
    print()
    
    print(f'[검증]')
    sales_diff = abs(forecast_sales - expected_sales)
    sales_vm_diff = abs(forecast_sales_vm - expected_sales_vm)
    print(f'  실판매액 차이: {sales_diff:,.0f} ({"✓ 정확" if sales_diff < 1 else "✗ 오차"})')
    print(f'  실판매액(V-) 차이: {sales_vm_diff:,.0f} ({"✓ 정확" if sales_vm_diff < 1 else "✗ 오차"})')
    print()
    
    # 사용자가 확인한 값
    print(f'[사용자 확인값]')
    print(f'  실판매액: 17,121,404,980')
    print(f'  실판매액(V-): 11,419,456,425')
    print(f'  계산값 (실판매액/1.1): {forecast_sales/1.1:,.0f}')
    print()
    
    if abs(forecast_sales_vm - forecast_sales/1.1) < 1000:
        print('  ⚠️  실판매액(V-)가 실판매액/1.1과 거의 같습니다 (진척율 미적용 의심)')
    else:
        print('  ✓ 실판매액(V-)가 독립적으로 진척율 계산되었습니다')



