import pandas as pd
import json

# KPI 데이터 확인
with open('public/data/20251229/brand_kpi.json', 'r', encoding='utf-8') as f:
    brand_kpi = json.load(f)

print("=" * 80)
print("MLB 데이터 상세 분석 - 20251229")
print("=" * 80)

# Brand KPI에서 MLB 데이터
mlb_kpi = brand_kpi['M']
print("\n[1] brand_kpi.json의 MLB 데이터:")
print(f"  revenue (KE30): {mlb_kpi['revenue']:,.0f}원 = {mlb_kpi['revenue']/100000000:.2f}억원")
print(f"  revenueForecast: {mlb_kpi['revenueForecast']:,.0f}원 = {mlb_kpi['revenueForecast']/100000000:.2f}억원")

# Forecast 파일 읽기
forecast = pd.read_csv('raw/202512/current_year/20251229/forecast_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_forecast = forecast[forecast['브랜드'] == 'M']

print("\n[2] forecast_Shop.csv의 MLB 실판매액(V-):")
forecast_sales = mlb_forecast['합계 : 실판매액(V-)'].sum()
print(f"  {forecast_sales:,.0f}원 = {forecast_sales/100000000:.2f}억원")

# KE30 파일 읽기
ke30 = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_Shop.csv', encoding='utf-8-sig')
mlb_ke30 = ke30[ke30['브랜드'] == 'M']

print("\n[3] ke30_Shop.csv의 MLB 실판매액(V-):")
ke30_sales = mlb_ke30['합계 : 실판매액(V-)'].sum()
print(f"  {ke30_sales:,.0f}원 = {ke30_sales/100000000:.2f}억원")

# brand_kpi.json의 revenue와 ke30 파일 비교
print("\n[4] revenue 비교:")
print(f"  brand_kpi.json revenue: {mlb_kpi['revenue']/100000000:.2f}억원")
print(f"  ke30_Shop.csv 실판매액(V-): {ke30_sales/100000000:.2f}억원")
print(f"  차이: {(mlb_kpi['revenue'] - ke30_sales)/100000000:.2f}억원")

# brand_kpi.json의 revenueForecast와 forecast 파일 비교
print("\n[5] revenueForecast 비교:")
print(f"  brand_kpi.json revenueForecast: {mlb_kpi['revenueForecast']/100000000:.2f}억원")
print(f"  forecast_Shop.csv 실판매액(V-): {forecast_sales/100000000:.2f}억원")
print(f"  차이: {(mlb_kpi['revenueForecast'] - forecast_sales)/100000000:.2f}억원")

# 전처리완료 파일도 확인
preprocessed = pd.read_csv('raw/202512/current_year/20251229/ke30_20251229_202512_전처리완료.csv', encoding='utf-8-sig')
mlb_preprocessed = preprocessed[preprocessed['브랜드'] == 'M']

print("\n[6] ke30_전처리완료.csv의 MLB 실판매액:")
if '실판매액' in mlb_preprocessed.columns:
    preprocessed_sales = mlb_preprocessed['실판매액'].sum()
    print(f"  {preprocessed_sales:,.0f}원 = {preprocessed_sales/100000000:.2f}억원")
else:
    print("  '실판매액' 컬럼 없음")
    print(f"  컬럼 목록: {list(mlb_preprocessed.columns)[:10]}")

# 채널명이 "미지정"인 데이터 확인
print("\n[7] 채널명 '미지정' 데이터 확인:")
ke30_unspecified = mlb_ke30[mlb_ke30['채널명'] == '미지정']
forecast_unspecified = mlb_forecast[mlb_forecast['채널명'] == '미지정']

print(f"  KE30 미지정 행 수: {len(ke30_unspecified)}")
if len(ke30_unspecified) > 0:
    unspecified_ke30_sales = ke30_unspecified['합계 : 실판매액(V-)'].sum()
    print(f"  KE30 미지정 실판매액: {unspecified_ke30_sales:,.0f}원 = {unspecified_ke30_sales/100000000:.2f}억원")

print(f"  Forecast 미지정 행 수: {len(forecast_unspecified)}")
if len(forecast_unspecified) > 0:
    unspecified_forecast_sales = forecast_unspecified['합계 : 실판매액(V-)'].sum()
    print(f"  Forecast 미지정 실판매액: {unspecified_forecast_sales:,.0f}원 = {unspecified_forecast_sales/100000000:.2f}억원")

# 채널명 지정된 데이터만 계산
print("\n[8] 채널명 지정된 데이터만 계산:")
ke30_specified = mlb_ke30[mlb_ke30['채널명'] != '미지정']
forecast_specified = mlb_forecast[mlb_forecast['채널명'] != '미지정']

specified_ke30_sales = ke30_specified['합계 : 실판매액(V-)'].sum()
specified_forecast_sales = forecast_specified['합계 : 실판매액(V-)'].sum()

print(f"  KE30 지정 실판매액: {specified_ke30_sales:,.0f}원 = {specified_ke30_sales/100000000:.2f}억원")
print(f"  Forecast 지정 실판매액: {specified_forecast_sales:,.0f}원 = {specified_forecast_sales/100000000:.2f}억원")

# 진척율 계산
duty_free_ke30 = ke30_specified[ke30_specified['유통채널'] == 2]['합계 : 실판매액(V-)'].sum()
non_duty_free_ke30 = ke30_specified[ke30_specified['유통채널'] != 2]['합계 : 실판매액(V-)'].sum()

duty_free_forecast = forecast_specified[forecast_specified['유통채널'] == 2]['합계 : 실판매액(V-)'].sum()
non_duty_free_forecast = forecast_specified[forecast_specified['유통채널'] != 2]['합계 : 실판매액(V-)'].sum()

print("\n[9] 면세/면세제외 구분 계산 (채널명 지정된 것만):")
print(f"  면세 KE30: {duty_free_ke30/100000000:.2f}억원")
print(f"  면세 Forecast: {duty_free_forecast/100000000:.2f}억원")
print(f"  면세 진척율: {duty_free_forecast/duty_free_ke30:.4f} (예상: {31/27.18:.4f})")

print(f"\n  면세제외 KE30: {non_duty_free_ke30/100000000:.2f}억원")
print(f"  면세제외 Forecast: {non_duty_free_forecast/100000000:.2f}억원")
print(f"  면세제외 진척율: {non_duty_free_forecast/non_duty_free_ke30:.4f} (예상: {31/24.44:.4f})")

print("\n[10] 최종 정리:")
print(f"  brand_kpi.json에 저장된 revenueForecast: {mlb_kpi['revenueForecast']/100000000:.2f}억원")
print(f"  forecast 파일의 실제 합계: {forecast_sales/100000000:.2f}억원")
print(f"  차이: {(mlb_kpi['revenueForecast'] - forecast_sales)/100000000:.2f}억원")

print("=" * 80)






