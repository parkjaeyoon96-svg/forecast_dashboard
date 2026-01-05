"""
20251201 MLB 채널 1 로열티 상세 계산 확인
"""
import pandas as pd
from pathlib import Path

project_root = Path(__file__).parent.parent

# 파일 경로
ke30_path = project_root / "raw/202511/current_year/20251201/ke30_20251201_202511_전처리완료.csv"
forecast_path = project_root / "raw/202511/current_year/20251201/forecast_20251201_202511_Shop.csv"
royalty_master_path = project_root / "Master/로열티율.csv"

# 로열티율 마스터 로드
df_royalty = pd.read_csv(royalty_master_path, encoding="utf-8-sig")
df_royalty = df_royalty.dropna(subset=['브랜드', '유통채널'])

royalty_master = {}
for _, row in df_royalty.iterrows():
    brand = str(row['브랜드']).strip()
    channel_num = row['유통채널']
    rate_str = str(row['%/원']).strip().replace('%', '')
    base = str(row['기준매출']).strip()
    
    if pd.isna(channel_num) or brand == '' or rate_str == '' or rate_str == 'nan':
        continue
    
    try:
        channel_num_int = int(float(channel_num))
        rate = float(rate_str) / 100
    except (ValueError, TypeError):
        continue
    
    key = (brand, channel_num_int)
    royalty_master[key] = {'rate': rate, 'base': base}

# MLB 채널 1 확인
mlb_ch1_info = royalty_master.get(('M', 1), None)
print("=" * 80)
print("MLB 채널 1 로열티율 마스터 정보")
print("=" * 80)
if mlb_ch1_info:
    print(f"브랜드: M, 채널: 1")
    print(f"로열티율: {mlb_ch1_info['rate']:.4f} ({mlb_ch1_info['rate']*100:.2f}%)")
    print(f"기준매출: {mlb_ch1_info['base']}")
    print(f"→ {'출고매출액' if '출고가' in mlb_ch1_info['base'] or '출고매출' in mlb_ch1_info['base'] else '실판매액'} 사용")
else:
    print("MLB 채널 1 정보를 찾을 수 없습니다.")
    exit(1)

# KE30 파일 로드
df_ke30 = pd.read_csv(ke30_path, encoding="utf-8-sig")
mlb_ch1_ke30 = df_ke30[(df_ke30['브랜드'].astype(str).str.strip() == 'M') & (df_ke30['유통채널'] == 1)].copy()

sales_col = '합계 : 실판매액(V-)'
shipping_col = '합계 : 출고매출액(V-) Actual'

print("\n" + "=" * 80)
print("KE30 MLB 채널 1 상세")
print("=" * 80)
print(f"행 수: {len(mlb_ch1_ke30)}")
print(f"실판매액(V-) 합계: {int(mlb_ch1_ke30[sales_col].sum()):,}")
print(f"출고매출액(V-) 합계: {int(mlb_ch1_ke30[shipping_col].sum()):,}")

# 로열티 계산
mlb_ch1_ke30['계산된_로열티'] = 0.0
for idx, row in mlb_ch1_ke30.iterrows():
    if '출고가' in mlb_ch1_info['base'] or '출고매출' in mlb_ch1_info['base']:
        base_value = row[shipping_col] if pd.notna(row[shipping_col]) else 0
    else:
        base_value = row[sales_col] if pd.notna(row[sales_col]) else 0
    
    if base_value > 0:
        cost_value = base_value * mlb_ch1_info['rate']
        mlb_ch1_ke30.at[idx, '계산된_로열티'] = int(round(cost_value))

print(f"계산된 로열티 합계: {int(mlb_ch1_ke30['계산된_로열티'].sum()):,}")
if '지급수수료_로열티' in mlb_ch1_ke30.columns:
    print(f"기존 로열티 컬럼 합계: {int(mlb_ch1_ke30['지급수수료_로열티'].sum()):,}")

# Forecast 파일 로드
df_forecast = pd.read_csv(forecast_path, encoding="utf-8-sig")
mlb_ch1_forecast = df_forecast[
    (df_forecast['브랜드'].astype(str).str.strip() == 'M') & 
    (df_forecast['유통채널'] == 1) &
    (df_forecast['채널명'].astype(str).str.strip() != '미지정')
].copy()

forecast_sales_col = '합계 : 실판매액(V-)'
forecast_shipping_col = '합계 : 출고매출액(V-) Actual'

print("\n" + "=" * 80)
print("Forecast MLB 채널 1 상세 (미지정 제외)")
print("=" * 80)
print(f"행 수: {len(mlb_ch1_forecast)}")
print(f"예상 실판매액(V-) 합계: {int(mlb_ch1_forecast[forecast_sales_col].sum()):,}")
print(f"예상 출고매출액(V-) 합계: {int(mlb_ch1_forecast[forecast_shipping_col].sum()):,}")

# 로열티 계산
mlb_ch1_forecast['계산된_로열티'] = 0.0
for idx, row in mlb_ch1_forecast.iterrows():
    if '출고가' in mlb_ch1_info['base'] or '출고매출' in mlb_ch1_info['base']:
        base_value = row[forecast_shipping_col] if pd.notna(row[forecast_shipping_col]) else 0
    else:
        base_value = row[forecast_sales_col] if pd.notna(row[forecast_sales_col]) else 0
    
    if base_value > 0:
        cost_value = base_value * mlb_ch1_info['rate']
        mlb_ch1_forecast.at[idx, '계산된_로열티'] = int(round(cost_value))

print(f"계산된 로열티 합계: {int(mlb_ch1_forecast['계산된_로열티'].sum()):,}")
if '지급수수료_로열티' in mlb_ch1_forecast.columns:
    print(f"기존 로열티 컬럼 합계: {int(mlb_ch1_forecast['지급수수료_로열티'].sum()):,}")

print("\n" + "=" * 80)
print("비교")
print("=" * 80)
print(f"KE30 출고매출액(V-) 합계: {int(mlb_ch1_ke30[shipping_col].sum()):,}")
print(f"Forecast 출고매출액(V-) 합계: {int(mlb_ch1_forecast[forecast_shipping_col].sum()):,}")
print(f"차이: {int(mlb_ch1_ke30[shipping_col].sum() - mlb_ch1_forecast[forecast_shipping_col].sum()):,}")
print()
print(f"KE30 로열티 합계: {int(mlb_ch1_ke30['계산된_로열티'].sum()):,}")
print(f"Forecast 로열티 합계: {int(mlb_ch1_forecast['계산된_로열티'].sum()):,}")
print(f"차이: {int(mlb_ch1_ke30['계산된_로열티'].sum() - mlb_ch1_forecast['계산된_로열티'].sum()):,}")
























