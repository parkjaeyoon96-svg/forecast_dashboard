import pandas as pd
import json

print("=" * 80)
print("MLB 실판매액 V+ 기준 계산 검증 - 20251229")
print("=" * 80)

# KE30 파일 읽기
ke30 = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_ke30 = ke30[ke30['브랜드'] == 'M']

# Forecast 파일 읽기
forecast = pd.read_csv('raw/202512/current_year/20251229/forecast_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_forecast = forecast[forecast['브랜드'] == 'M']

# 컬럼 확인
print("\n[1] 사용 가능한 실판매액 컬럼:")
sales_cols = [col for col in ke30.columns if '실판매액' in str(col) and '합계' in str(col)]
for col in sales_cols:
    print(f"  - {col}")

# V+ 컬럼 찾기
vplus_col = None
for col in ke30.columns:
    col_str = str(col)
    if '실판매액' in col_str and '합계' in col_str:
        if '(V+)' in col_str or 'v+' in col_str.lower():
            vplus_col = col
            break
        # V-가 아니고 V+도 명시되지 않은 경우 (부가세 포함일 가능성)
        if '(V-)' not in col_str and vplus_col is None:
            vplus_col = col

print(f"\n[2] 선택된 실판매액 컬럼: {vplus_col}")

if vplus_col:
    # KE30 V+ 실판매액
    ke30_vplus = mlb_ke30[vplus_col].sum()
    print(f"\n[3] KE30 실판매액(V+):")
    print(f"  {ke30_vplus:,.0f}원 = {ke30_vplus/100000000:.2f}억원")
    
    # Forecast V+ 실판매액
    forecast_vplus = mlb_forecast[vplus_col].sum()
    print(f"\n[4] Forecast 실판매액(V+):")
    print(f"  {forecast_vplus:,.0f}원 = {forecast_vplus/100000000:.2f}억원")
    
    # brand_kpi.json 확인
    with open('public/data/20251229/brand_kpi.json', 'r', encoding='utf-8') as f:
        brand_kpi = json.load(f)
    
    mlb_kpi = brand_kpi['M']
    print(f"\n[5] brand_kpi.json의 MLB 데이터:")
    print(f"  revenue (KE30): {mlb_kpi['revenue']:,.0f}원 = {mlb_kpi['revenue']/100000000:.2f}억원")
    print(f"  revenueForecast: {mlb_kpi['revenueForecast']:,.0f}원 = {mlb_kpi['revenueForecast']/100000000:.2f}억원")
    
    # 비교
    print(f"\n[6] 비교:")
    print(f"  KE30 파일 V+: {ke30_vplus/100000000:.2f}억원")
    print(f"  brand_kpi revenue: {mlb_kpi['revenue']/100000000:.2f}억원")
    print(f"  차이: {(mlb_kpi['revenue'] - ke30_vplus)/100000000:.2f}억원")
    
    print(f"\n  Forecast 파일 V+: {forecast_vplus/100000000:.2f}억원")
    print(f"  brand_kpi revenueForecast: {mlb_kpi['revenueForecast']/100000000:.2f}억원")
    print(f"  차이: {(mlb_kpi['revenueForecast'] - forecast_vplus)/100000000:.2f}억원")
    
    # 진척일수 기반 계산 검증
    progress_days = pd.read_csv('raw/202512/previous_year/progress_days_202512.csv', encoding='utf-8-sig')
    duty_free_progress = progress_days[progress_days['브랜드'] == 'M(면세)']['2024-12-23'].values[0]
    non_duty_free_progress = progress_days[progress_days['브랜드'] == 'M(면세제외)']['2024-12-23'].values[0]
    
    print(f"\n[7] 진척일수 기반 계산 검증:")
    print(f"  진척일수 - 면세: {duty_free_progress}일, 면세제외: {non_duty_free_progress}일")
    print(f"  월 총일수: 31일")
    
    # 면세/면세제외 구분 계산
    duty_free_ke30 = mlb_ke30[mlb_ke30['유통채널'] == 2][vplus_col].sum()
    non_duty_free_ke30 = mlb_ke30[mlb_ke30['유통채널'] != 2][vplus_col].sum()
    
    duty_free_forecast_calc = (duty_free_ke30 / duty_free_progress) * 31
    non_duty_free_forecast_calc = (non_duty_free_ke30 / non_duty_free_progress) * 31
    
    print(f"\n  면세 KE30: {duty_free_ke30/100000000:.2f}억원")
    print(f"  면세 Forecast 계산: ({duty_free_ke30/100000000:.2f} / {duty_free_progress}) × 31 = {duty_free_forecast_calc/100000000:.2f}억원")
    
    print(f"\n  면세제외 KE30: {non_duty_free_ke30/100000000:.2f}억원")
    print(f"  면세제외 Forecast 계산: ({non_duty_free_ke30/100000000:.2f} / {non_duty_free_progress}) × 31 = {non_duty_free_forecast_calc/100000000:.2f}억원")
    
    print(f"\n  합계 계산: {duty_free_forecast_calc/100000000:.2f} + {non_duty_free_forecast_calc/100000000:.2f} = {(duty_free_forecast_calc + non_duty_free_forecast_calc)/100000000:.2f}억원")
    
    # 실제 Forecast 파일과 비교
    duty_free_forecast_actual = mlb_forecast[mlb_forecast['유통채널'] == 2][vplus_col].sum()
    non_duty_free_forecast_actual = mlb_forecast[mlb_forecast['유통채널'] != 2][vplus_col].sum()
    
    print(f"\n[8] 실제 Forecast 파일 값:")
    print(f"  면세: {duty_free_forecast_actual/100000000:.2f}억원")
    print(f"  면세제외: {non_duty_free_forecast_actual/100000000:.2f}억원")
    print(f"  합계: {forecast_vplus/100000000:.2f}억원")
    
    print(f"\n[9] 최종 결론:")
    print(f"  ✓ 사용자 질문: revenueForecast가 385억으로 나오는데 359.2억이 맞지 않냐?")
    print(f"  ✓ brand_kpi.json revenueForecast: {mlb_kpi['revenueForecast']/100000000:.2f}억원")
    print(f"  ✓ forecast 파일 실판매액(V+): {forecast_vplus/100000000:.2f}억원")
    print(f"  ✓ 진척일수 기반 계산값: {(duty_free_forecast_calc + non_duty_free_forecast_calc)/100000000:.2f}억원")
    
    if abs(mlb_kpi['revenueForecast'] - forecast_vplus) < 1000:
        print(f"\n  ✅ brand_kpi.json과 forecast 파일이 일치합니다!")
    else:
        print(f"\n  ⚠️ 차이 발생: {(mlb_kpi['revenueForecast'] - forecast_vplus)/100000000:.2f}억원")
        print(f"  → 원인 분석 필요")

else:
    print("\n[ERROR] 실판매액(V+) 컬럼을 찾을 수 없습니다.")

print("=" * 80)














