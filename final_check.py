# -*- coding: utf-8 -*-
import pandas as pd

# 직접비율 추출 결과 읽기
df_rates = pd.read_csv(r'raw\202512\plan\직접비율_추출결과.csv', encoding='utf-8-sig')

# MLB 이천보관료 비율만 추출
mlb_storage_rates = df_rates[(df_rates['브랜드'] == 'M') & 
                              (df_rates['직접비항목'] == '지급수수료_이천보관료')].copy()

print("="*80)
print("MLB 이천보관료 비율 (직접비율 추출 결과)")
print("="*80)
for _, row in mlb_storage_rates.iterrows():
    channel = row['유통채널']
    rate_str = row['비율']
    rate = float(rate_str.replace('%', ''))
    print(f"유통채널 {channel}: {rate}%")

# Forecast 파일 읽기
df_forecast = pd.read_csv(r'raw\202512\current_year\20251208\forecast_20251208_202512_Shop.csv', 
                          encoding='utf-8-sig')

mlb = df_forecast[df_forecast['브랜드'] == 'M'].copy()

print("\n" + "="*80)
print("MLB Forecast 실제 적용 비율")
print("="*80)
print(f"{'채널명':<20} {'유통채널':>8} {'실판매액(V-)':>18} {'이천보관료':>18} {'실제비율':>10}")
print("-"*80)

for _, row in mlb.iterrows():
    channel_name = row['채널명']
    channel_num = int(row['유통채널']) if pd.notna(row['유통채널']) else 0
    sales = row['합계 : 실판매액(V-)']
    storage = row['지급수수료_이천보관료']
    
    if sales > 0:
        ratio = (storage / sales) * 100
        print(f"{channel_name:<20} {channel_num:>8} {sales:>18,.0f} {storage:>18,.0f} {ratio:>9.4f}%")

print("\n" + "="*80)
print("결론:")
print("  계획 파일의 비율: 2.10%")
print("  Forecast 적용 비율: 약 1.91%")
print("  차이: 약 0.19%p")
print("  비율: 1.91 / 2.10 = 0.909 (약 90.9%)")
print("\n  => 어딘가에서 1.1로 나누는 보정이 적용되고 있습니다!")
print("="*80)












