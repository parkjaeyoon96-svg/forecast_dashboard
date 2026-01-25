import pandas as pd

print("=" * 80)
print("MLB 면세점 유통채널 코드 확인")
print("=" * 80)

# KE30 파일 읽기
ke30 = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_ke30 = ke30[ke30['브랜드'] == 'M']

print("\n[1] MLB 전체 유통채널 확인:")
print(mlb_ke30['유통채널'].value_counts().sort_index())

print("\n[2] 면세점 데이터 상세:")
duty_free = mlb_ke30[mlb_ke30['채널명'] == '면세점']
print(f"  행 수: {len(duty_free)}")
print(f"  유통채널 값: {duty_free['유통채널'].unique()}")
print(f"  유통채널 타입: {type(duty_free['유통채널'].iloc[0])}")
print(f"  유통채널 값 (str): '{str(duty_free['유통채널'].iloc[0])}'")

# 유통채널 2인 데이터
channel_2 = mlb_ke30[mlb_ke30['유통채널'] == 2]
print(f"\n[3] 유통채널 == 2인 데이터:")
print(f"  행 수: {len(channel_2)}")
if len(channel_2) > 0:
    print(f"  채널명: {channel_2['채널명'].unique()}")

# 유통채널 2.0인 데이터
channel_2_float = mlb_ke30[mlb_ke30['유통채널'] == 2.0]
print(f"\n[4] 유통채널 == 2.0인 데이터:")
print(f"  행 수: {len(channel_2_float)}")
if len(channel_2_float) > 0:
    print(f"  채널명: {channel_2_float['채널명'].unique()}")

# 문자열 비교
channel_2_str = mlb_ke30[mlb_ke30['유통채널'].astype(str) == '2']
print(f"\n[5] str(유통채널) == '2'인 데이터:")
print(f"  행 수: {len(channel_2_str)}")
if len(channel_2_str) > 0:
    print(f"  채널명: {channel_2_str['채널명'].unique()}")

channel_2_0_str = mlb_ke30[mlb_ke30['유통채널'].astype(str) == '2.0']
print(f"\n[6] str(유통채널) == '2.0'인 데이터:")
print(f"  행 수: {len(channel_2_0_str)}")
if len(channel_2_0_str) > 0:
    print(f"  채널명: {channel_2_0_str['채널명'].unique()}")

# Forecast 파일도 확인
forecast = pd.read_csv('raw/202512/current_year/20251229/forecast_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_forecast = forecast[forecast['브랜드'] == 'M']

print("\n[7] Forecast 파일 - 면세점 데이터:")
duty_free_forecast = mlb_forecast[mlb_forecast['채널명'] == '면세점']
print(f"  행 수: {len(duty_free_forecast)}")
print(f"  유통채널 값: {duty_free_forecast['유통채널'].unique()}")
print(f"  유통채널 타입: {type(duty_free_forecast['유통채널'].iloc[0])}")

# 진척일수 확인
progress_days = pd.read_csv('raw/202512/previous_year/progress_days_202512.csv', encoding='utf-8-sig')
print("\n[8] 진척일수 데이터:")
print(f"  M(면세): {progress_days[progress_days['브랜드'] == 'M(면세)']['2024-12-23'].values[0]}일")
print(f"  M(면세제외): {progress_days[progress_days['브랜드'] == 'M(면세제외)']['2024-12-23'].values[0]}일")

# 실제 계산 시뮬레이션
print("\n[9] 계산 시뮬레이션:")
vplus_col = '합계 : 실판매액'
ke30_sales = duty_free[vplus_col].iloc[0]
channel_code = duty_free['유통채널'].iloc[0]

print(f"  KE30 실판매액: {ke30_sales:,.0f}원 ({ke30_sales/100000000:.2f}억원)")
print(f"  유통채널 코드: {channel_code} (타입: {type(channel_code)})")
print(f"  str(channel_code): '{str(channel_code)}'")
print(f"  str(channel_code) == '2': {str(channel_code) == '2'}")
print(f"  str(channel_code) == '2.0': {str(channel_code) == '2.0'}")

# 조건 분기 테스트
brand = 'M'
if brand == 'M' and str(channel_code) == '2':
    used_progress = 27.18
    print(f"\n  ✓ 조건: brand=='M' and str(channel_code)=='2'")
    print(f"  → M(면세) 진척일수 사용: {used_progress}일")
elif brand == 'M' and str(channel_code) != '2':
    used_progress = 24.44
    print(f"\n  ✓ 조건: brand=='M' and str(channel_code)!='2'")
    print(f"  → M(면세제외) 진척일수 사용: {used_progress}일")

expected_forecast = (ke30_sales / used_progress) * 31
print(f"\n  계산: ({ke30_sales/100000000:.2f} / {used_progress}) × 31 = {expected_forecast/100000000:.2f}억원")

actual_forecast = duty_free_forecast[vplus_col].iloc[0]
print(f"  실제 Forecast: {actual_forecast:,.0f}원 ({actual_forecast/100000000:.2f}억원)")
print(f"  일치 여부: {'✅ 일치' if abs(expected_forecast - actual_forecast) < 1000 else '❌ 불일치'}")

print("=" * 80)














