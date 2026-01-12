import pandas as pd

print("=" * 80)
print("MLB 면세(유통채널 2) 상세 분석 - 20251229")
print("=" * 80)

# KE30 파일 읽기
ke30 = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_ke30 = ke30[ke30['브랜드'] == 'M']
duty_free_ke30 = mlb_ke30[mlb_ke30['유통채널'] == 2]

# Forecast 파일 읽기
forecast = pd.read_csv('raw/202512/current_year/20251229/forecast_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_forecast = forecast[forecast['브랜드'] == 'M']
duty_free_forecast = mlb_forecast[mlb_forecast['유통채널'] == 2]

print(f"\n[1] 면세 데이터 행 수:")
print(f"  KE30: {len(duty_free_ke30)}행")
print(f"  Forecast: {len(duty_free_forecast)}행")

# 채널명별 분석
print(f"\n[2] 채널명별 분석 (KE30):")
vplus_col = '합계 : 실판매액'
for channel_name in duty_free_ke30['채널명'].unique():
    channel_data = duty_free_ke30[duty_free_ke30['채널명'] == channel_name]
    sales = channel_data[vplus_col].sum()
    print(f"  {channel_name}: {sales:,.0f}원 ({sales/100000000:.2f}억원) - {len(channel_data)}행")

print(f"\n[3] 채널명별 분석 (Forecast):")
for channel_name in duty_free_forecast['채널명'].unique():
    channel_data = duty_free_forecast[duty_free_forecast['채널명'] == channel_name]
    sales = channel_data[vplus_col].sum()
    print(f"  {channel_name}: {sales:,.0f}원 ({sales/100000000:.2f}억원) - {len(channel_data)}행")

# 미지정 채널 확인
print(f"\n[4] '미지정' 채널 확인:")
ke30_unspecified = duty_free_ke30[duty_free_ke30['채널명'] == '미지정']
forecast_unspecified = duty_free_forecast[duty_free_forecast['채널명'] == '미지정']

print(f"  KE30 미지정: {len(ke30_unspecified)}행")
if len(ke30_unspecified) > 0:
    print(f"    실판매액: {ke30_unspecified[vplus_col].sum():,.0f}원 ({ke30_unspecified[vplus_col].sum()/100000000:.2f}억원)")

print(f"  Forecast 미지정: {len(forecast_unspecified)}행")
if len(forecast_unspecified) > 0:
    print(f"    실판매액: {forecast_unspecified[vplus_col].sum():,.0f}원 ({forecast_unspecified[vplus_col].sum()/100000000:.2f}억원)")

# 채널명 지정된 것만 계산
print(f"\n[5] 채널명 지정된 데이터만 계산:")
ke30_specified = duty_free_ke30[duty_free_ke30['채널명'] != '미지정']
forecast_specified = duty_free_forecast[duty_free_forecast['채널명'] != '미지정']

ke30_specified_sales = ke30_specified[vplus_col].sum()
forecast_specified_sales = forecast_specified[vplus_col].sum()

print(f"  KE30 지정: {ke30_specified_sales:,.0f}원 ({ke30_specified_sales/100000000:.2f}억원)")
print(f"  Forecast 지정: {forecast_specified_sales:,.0f}원 ({forecast_specified_sales/100000000:.2f}억원)")

# 진척율 계산
duty_free_progress = 27.18
expected_forecast = (ke30_specified_sales / duty_free_progress) * 31

print(f"\n[6] 진척일수 기반 예상 계산:")
print(f"  진척일수: {duty_free_progress}일")
print(f"  계산: ({ke30_specified_sales/100000000:.2f} / {duty_free_progress}) × 31 = {expected_forecast/100000000:.2f}억원")
print(f"  실제 Forecast: {forecast_specified_sales/100000000:.2f}억원")
print(f"  차이: {(forecast_specified_sales - expected_forecast)/100000000:.2f}억원")

# 실제 진척율 역산
actual_progress = (ke30_specified_sales / forecast_specified_sales) * 31
print(f"\n[7] 실제 적용된 진척일수 역산:")
print(f"  ({ke30_specified_sales/100000000:.2f} / {forecast_specified_sales/100000000:.2f}) × 31 = {actual_progress:.2f}일")
print(f"  예상 진척일수: {duty_free_progress}일")
print(f"  차이: {actual_progress - duty_free_progress:.2f}일")

# 개별 행 샘플 확인
print(f"\n[8] 샘플 데이터 확인 (첫 3행):")
print(f"\n  KE30:")
for idx, row in ke30_specified.head(3).iterrows():
    print(f"    채널명: {row['채널명']}, 실판매액: {row[vplus_col]:,.0f}원")

print(f"\n  Forecast:")
for idx, row in forecast_specified.head(3).iterrows():
    expected = (row[vplus_col] / forecast_specified_sales) * expected_forecast
    print(f"    채널명: {row['채널명']}, 실판매액: {row[vplus_col]:,.0f}원")

# 전체 합계 확인
print(f"\n[9] 전체 합계 (미지정 포함):")
ke30_total = duty_free_ke30[vplus_col].sum()
forecast_total = duty_free_forecast[vplus_col].sum()

print(f"  KE30: {ke30_total:,.0f}원 ({ke30_total/100000000:.2f}억원)")
print(f"  Forecast: {forecast_total:,.0f}원 ({forecast_total/100000000:.2f}억원)")
print(f"  진척율: {forecast_total/ke30_total:.4f}")
print(f"  역산 진척일수: {(ke30_total/forecast_total)*31:.2f}일")

print("=" * 80)






