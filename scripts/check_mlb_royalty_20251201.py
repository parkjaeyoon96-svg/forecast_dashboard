"""
20251201 날짜의 KE30과 Forecast 파일에서 MLB 로열티 계산 비교
"""
import pandas as pd
import sys
from pathlib import Path

# 프로젝트 루트
project_root = Path(__file__).parent.parent

# 파일 경로
ke30_path = project_root / "raw/202511/current_year/20251201/ke30_20251201_202511_전처리완료.csv"
forecast_path = project_root / "raw/202511/current_year/20251201/forecast_20251201_202511_Shop.csv"

# 로열티율 마스터 로드
royalty_master_path = project_root / "Master/로열티율.csv"

print("=" * 80)
print("MLB 로열티 계산 비교 (20251201)")
print("=" * 80)

# 1. 로열티율 마스터 로드
print("\n[1] 로열티율 마스터 로드 중...")
if not royalty_master_path.exists():
    print(f"[ERROR] 로열티율 마스터 파일이 없습니다: {royalty_master_path}")
    sys.exit(1)

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

print(f"  로드 완료: {len(royalty_master)}개 매핑")
print("\n  MLB 관련 로열티율:")
mlb_rates = {k: v for k, v in royalty_master.items() if k[0] == 'M'}
for (brand, channel), info in sorted(mlb_rates.items()):
    print(f"    브랜드={brand}, 채널={channel}: rate={info['rate']:.4f}, base={info['base']}")

# 2. KE30 파일 로드
print("\n[2] KE30 파일 로드 중...")
if not ke30_path.exists():
    print(f"[ERROR] KE30 파일이 없습니다: {ke30_path}")
    sys.exit(1)

df_ke30 = pd.read_csv(ke30_path, encoding="utf-8-sig")
print(f"  전체 행 수: {len(df_ke30)}")

# 실판매액(V-) 컬럼 찾기
sales_col_ke30 = None
shipping_col_ke30 = None
for col in df_ke30.columns:
    if '실판매액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
        sales_col_ke30 = col
    if '출고매출액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
        shipping_col_ke30 = col

print(f"  실판매액(V-) 컬럼: {sales_col_ke30}")
print(f"  출고매출액(V-) 컬럼: {shipping_col_ke30 if shipping_col_ke30 else '없음'}")

# MLB 필터링
mlb_ke30 = df_ke30[df_ke30['브랜드'].astype(str).str.strip() == 'M'].copy()
print(f"  MLB 행 수: {len(mlb_ke30)}")

# KE30에서 로열티 계산 (로열티율 마스터 기준)
print("\n[3] KE30에서 MLB 로열티 계산 중...")
mlb_ke30['계산된_로열티'] = 0.0

for idx, row in mlb_ke30.iterrows():
    brand = str(row.get('브랜드', '')).strip()
    channel_num = row.get('유통채널', None)
    
    if pd.isna(channel_num):
        continue
    
    try:
        channel_num_int = int(float(channel_num))
    except (ValueError, TypeError):
        continue
    
    royalty_info = royalty_master.get((brand, channel_num_int), None)
    if royalty_info:
        rate = royalty_info['rate']
        base = royalty_info['base']
        
        # 기준매출에 따라 다른 컬럼 사용
        # "출고가(V-)" 또는 "출고매출"이 포함되면 출고매출액 사용
        if '출고가' in base or '출고매출' in base:
            base_value = row[shipping_col_ke30] if shipping_col_ke30 else (row[sales_col_ke30] if sales_col_ke30 else 0)
        elif '실판가' in base or '실판매' in base:
            base_value = row[sales_col_ke30] if sales_col_ke30 else 0
        else:
            base_value = row[sales_col_ke30] if sales_col_ke30 else 0
        
        if pd.notna(base_value) and base_value > 0:
            cost_value = base_value * rate
            mlb_ke30.at[idx, '계산된_로열티'] = int(round(cost_value))

# 기존 로열티 컬럼이 있는지 확인
if '지급수수료_로열티' in mlb_ke30.columns:
    ke30_royalty_sum = mlb_ke30['지급수수료_로열티'].sum()
    print(f"  기존 로열티 컬럼 합계: {int(ke30_royalty_sum):,}")
else:
    print(f"  [WARNING] '지급수수료_로열티' 컬럼이 없습니다.")

ke30_calculated_sum = mlb_ke30['계산된_로열티'].sum()
ke30_sales_sum = mlb_ke30[sales_col_ke30].sum() if sales_col_ke30 else 0
print(f"  계산된 로열티 합계: {int(ke30_calculated_sum):,}")
print(f"  실판매액(V-) 합계: {int(ke30_sales_sum):,}")

# 채널별 집계
print("\n  KE30 채널별 집계:")
ke30_by_channel = mlb_ke30.groupby('유통채널').agg({
    sales_col_ke30: 'sum',
    '계산된_로열티': 'sum'
}).round(0).astype(int)
ke30_by_channel.columns = ['실판매액(V-)', '계산된_로열티']
print(ke30_by_channel.to_string())

# 3. Forecast 파일 로드
print("\n[4] Forecast 파일 로드 중...")
if not forecast_path.exists():
    print(f"[ERROR] Forecast 파일이 없습니다: {forecast_path}")
    sys.exit(1)

df_forecast = pd.read_csv(forecast_path, encoding="utf-8-sig")
print(f"  전체 행 수: {len(df_forecast)}")

# 예상 실판매액(V-) 컬럼 찾기
forecast_sales_col = None
forecast_shipping_col = None
for col in df_forecast.columns:
    if '실판매액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
        forecast_sales_col = col
    if '출고매출액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
        forecast_shipping_col = col

print(f"  예상 실판매액(V-) 컬럼: {forecast_sales_col}")
print(f"  예상 출고매출액(V-) 컬럼: {forecast_shipping_col if forecast_shipping_col else '없음'}")

# MLB 필터링
mlb_forecast = df_forecast[df_forecast['브랜드'].astype(str).str.strip() == 'M'].copy()
print(f"  MLB 행 수 (전체): {len(mlb_forecast)}")

# 미지정 채널 제외
mlb_forecast_valid = mlb_forecast[mlb_forecast['채널명'].astype(str).str.strip() != '미지정'].copy()
print(f"  MLB 행 수 (미지정 제외): {len(mlb_forecast_valid)}")

# Forecast에서 로열티 계산
print("\n[5] Forecast에서 MLB 로열티 계산 중...")
mlb_forecast_valid['계산된_로열티'] = 0.0

for idx, row in mlb_forecast_valid.iterrows():
    brand = str(row.get('브랜드', '')).strip()
    channel_num = row.get('유통채널', None)
    
    if pd.isna(channel_num):
        continue
    
    try:
        channel_num_int = int(float(channel_num))
    except (ValueError, TypeError):
        continue
    
    royalty_info = royalty_master.get((brand, channel_num_int), None)
    if royalty_info:
        rate = royalty_info['rate']
        base = royalty_info['base']
        
        # 기준매출에 따라 다른 컬럼 사용
        # "출고가(V-)" 또는 "출고매출"이 포함되면 출고매출액 사용
        if '출고가' in base or '출고매출' in base:
            base_value = row[forecast_shipping_col] if forecast_shipping_col else (row[forecast_sales_col] if forecast_sales_col else 0)
        elif '실판가' in base or '실판매' in base:
            base_value = row[forecast_sales_col] if forecast_sales_col else 0
        else:
            base_value = row[forecast_sales_col] if forecast_sales_col else 0
        
        if pd.notna(base_value) and base_value > 0:
            cost_value = base_value * rate
            mlb_forecast_valid.at[idx, '계산된_로열티'] = int(round(cost_value))

# 기존 로열티 컬럼이 있는지 확인
if '지급수수료_로열티' in mlb_forecast_valid.columns:
    forecast_royalty_sum = mlb_forecast_valid['지급수수료_로열티'].sum()
    print(f"  기존 로열티 컬럼 합계: {int(forecast_royalty_sum):,}")
else:
    print(f"  [WARNING] '지급수수료_로열티' 컬럼이 없습니다.")

forecast_calculated_sum = mlb_forecast_valid['계산된_로열티'].sum()
forecast_sales_sum = mlb_forecast_valid[forecast_sales_col].sum() if forecast_sales_col else 0
print(f"  계산된 로열티 합계: {int(forecast_calculated_sum):,}")
print(f"  예상 실판매액(V-) 합계: {int(forecast_sales_sum):,}")

# 채널별 집계
print("\n  Forecast 채널별 집계 (미지정 제외):")
forecast_by_channel = mlb_forecast_valid.groupby('유통채널').agg({
    forecast_sales_col: 'sum',
    '계산된_로열티': 'sum'
}).round(0).astype(int)
forecast_by_channel.columns = ['예상_실판매액(V-)', '계산된_로열티']
print(forecast_by_channel.to_string())

# 4. 비교
print("\n" + "=" * 80)
print("[6] 비교 결과")
print("=" * 80)
print(f"KE30 실판매액(V-) 합계:     {int(ke30_sales_sum):,}")
print(f"Forecast 실판매액(V-) 합계: {int(forecast_sales_sum):,}")
print(f"차이: {int(ke30_sales_sum - forecast_sales_sum):,}")
print()
print(f"KE30 로열티 합계:           {int(ke30_calculated_sum):,}")
print(f"Forecast 로열티 합계:       {int(forecast_calculated_sum):,}")
print(f"차이: {int(ke30_calculated_sum - forecast_calculated_sum):,}")

# 미지정 채널 분석
if '채널명' in mlb_forecast.columns:
    mlb_forecast_미지정 = mlb_forecast[mlb_forecast['채널명'].astype(str).str.strip() == '미지정'].copy()
    if len(mlb_forecast_미지정) > 0:
        print(f"\n[참고] Forecast에서 제외된 '미지정' 채널 행 수: {len(mlb_forecast_미지정)}")
        if forecast_sales_col in mlb_forecast_미지정.columns:
            미지정_sales = mlb_forecast_미지정[forecast_sales_col].sum()
            print(f"  미지정 채널의 예상 실판매액(V-) 합계: {int(미지정_sales):,}")

print("\n" + "=" * 80)

