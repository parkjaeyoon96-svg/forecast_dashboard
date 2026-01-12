import pandas as pd

# KE30 데이터 읽기
ke30 = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_Shop.csv', encoding='utf-8-sig')
forecast = pd.read_csv('raw/202512/current_year/20251229/forecast_20251229_202512_Shop.csv', encoding='utf-8-sig')

# MLB 브랜드만 필터링
mlb_ke30 = ke30[ke30['브랜드'] == 'M']
mlb_forecast = forecast[forecast['브랜드'] == 'M']

print("=" * 80)
print("MLB (브랜드 M) - 20251229 데이터 분석")
print("=" * 80)

# KE30 실판매액
ke30_sales = mlb_ke30['합계 : 실판매액(V-)'].sum()
print(f"\n[KE30 현시점 데이터]")
print(f"실판매액(V-) 합계: {ke30_sales:,.0f}원")
print(f"실판매액(V-) 억원: {ke30_sales / 100000000:.2f}억원")

# Forecast 실판매액
forecast_sales = mlb_forecast['합계 : 실판매액(V-)'].sum()
print(f"\n[Forecast 월말예상 데이터]")
print(f"실판매액(V-) 합계: {forecast_sales:,.0f}원")
print(f"실판매액(V-) 억원: {forecast_sales / 100000000:.2f}억원")

# 면세/면세제외 구분
print(f"\n[유통채널별 KE30 실판매액]")
ke30_by_channel = mlb_ke30.groupby('유통채널')['합계 : 실판매액(V-)'].sum()
for channel, amount in ke30_by_channel.items():
    channel_name = "면세" if channel == 2 else "면세제외"
    print(f"  유통채널 {channel} ({channel_name}): {amount:,.0f}원 ({amount/100000000:.2f}억원)")

print(f"\n[유통채널별 Forecast 실판매액]")
forecast_by_channel = mlb_forecast.groupby('유통채널')['합계 : 실판매액(V-)'].sum()
for channel, amount in forecast_by_channel.items():
    channel_name = "면세" if channel == 2 else "면세제외"
    print(f"  유통채널 {channel} ({channel_name}): {amount:,.0f}원 ({amount/100000000:.2f}억원)")

# 진척일수 확인
progress_days = pd.read_csv('raw/202512/previous_year/progress_days_202512.csv', encoding='utf-8-sig')
print(f"\n[진척일수 정보 - 2024-12-23 기준]")
print(f"  M(면세): {progress_days[progress_days['브랜드'] == 'M(면세)']['2024-12-23'].values[0]}일")
print(f"  M(면세제외): {progress_days[progress_days['브랜드'] == 'M(면세제외)']['2024-12-23'].values[0]}일")
print(f"  월 총일수: 31일")

# 계산 검증
print(f"\n[계산 검증]")
duty_free_ke30 = mlb_ke30[mlb_ke30['유통채널'] == 2]['합계 : 실판매액(V-)'].sum()
duty_free_progress = 27.18
duty_free_forecast_calc = (duty_free_ke30 / duty_free_progress) * 31

non_duty_free_ke30 = mlb_ke30[mlb_ke30['유통채널'] != 2]['합계 : 실판매액(V-)'].sum()
non_duty_free_progress = 24.44
non_duty_free_forecast_calc = (non_duty_free_ke30 / non_duty_free_progress) * 31

print(f"\n면세 (유통채널 2):")
print(f"  KE30: {duty_free_ke30/100000000:.2f}억원")
print(f"  진척일수: {duty_free_progress}일")
print(f"  계산: ({duty_free_ke30/100000000:.2f} / {duty_free_progress}) × 31 = {duty_free_forecast_calc/100000000:.2f}억원")

print(f"\n면세제외 (유통채널 != 2):")
print(f"  KE30: {non_duty_free_ke30/100000000:.2f}억원")
print(f"  진척일수: {non_duty_free_progress}일")
print(f"  계산: ({non_duty_free_ke30/100000000:.2f} / {non_duty_free_progress}) × 31 = {non_duty_free_forecast_calc/100000000:.2f}억원")

print(f"\n합계 계산:")
print(f"  {duty_free_forecast_calc/100000000:.2f} + {non_duty_free_forecast_calc/100000000:.2f} = {(duty_free_forecast_calc + non_duty_free_forecast_calc)/100000000:.2f}억원")

print(f"\n실제 Forecast 파일 합계: {forecast_sales/100000000:.2f}억원")
print(f"차이: {(forecast_sales - (duty_free_forecast_calc + non_duty_free_forecast_calc))/100000000:.2f}억원")

print("=" * 80)






