"""
KE30 파일을 진척일수를 사용하여 월말 예상 데이터로 변환

사용법:
    python scripts/convert_ke30_to_forecast.py [업데이트일자]
    
예시:
    python scripts/convert_ke30_to_forecast.py 20251117
    
설명:
    1. KE30 Shop 및 Shop_item 파일 읽기
    2. 진척일수 파일에서 전년동기간 진척일수 읽기
    3. 브랜드별 진척일수 매핑 (M(면세), M(면세제외) 처리)
    4. 각 필드별 계산:
       - 진척율 계산 필드: 필드금액/분석일수 * 총일수
       - 동일한 필드값: 그대로 유지
       - 직접비 계산 필드: 직접비 계산 로직 적용
       - 재계산 필드: 매출총이익, 직접비합계, 직접이익
    5. forecast 파일로 저장
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 기존 스크립트 import
scripts_dir = project_root / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

import extract_direct_cost_rates as extract_direct

# 직접비 항목 목록
DIRECT_COST_ITEMS = [
    '지급수수료_중간관리수수료',
    '지급수수료_중간관리수수료(직영)',
    '지급수수료_판매사원도급비(직영)',
    '지급수수료_판매사원도급비(면세)',
    '지급수수료_물류용역비',
    '지급수수료_물류운송비',
    '지급수수료_이천보관료',
    '지급수수료_카드수수료',
    '지급수수료_온라인위탁판매수수료',
    '지급수수료_로열티',
    '지급임차료_매장(변동)',
    '지급임차료_매장(고정)',
    '지급임차료_관리비',
    '감가상각비_임차시설물'
]

# 진척율 계산 필드
PROGRESS_RATE_FIELDS = [
    '합계 : 판매금액(TAG가)',
    '합계 : 실판매액',
    '합계 : 실판매액(V-)',
    '합계 : 출고매출액(V-) Actual',
    '매출원가(평가감환입반영)'
]

# 동일한 필드값 (그대로 유지)
IDENTICAL_FIELDS = [
    '브랜드',
    '유통채널',
    '채널명',
    '아이템_중분류',
    '아이템_소분류',
    '아이템코드',
    '지급수수료_이천보관료',
    '지급임차료_매장(고정)',
    '감가상각비_임차시설물'
]

# 직접비 계산 필드
DIRECT_COST_CALC_FIELDS = [
    '지급수수료_중간관리수수료',
    '지급수수료_판매사원도급비(직영)',
    '지급수수료_판매사원도급비(면세)',
    '지급수수료_물류용역비',
    '지급수수료_물류운송비',
    '지급수수료_카드수수료',
    '지급수수료_온라인위탁판매수수료',
    '지급수수료_로열티',
    '지급임차료_매장(변동)',
    '지급임차료_관리비'
]


def calculate_week_start_date(update_date_str: str) -> datetime:
    """
    업데이트일자로부터 분석기간 시작일(전주 월요일) 계산
    
    Args:
        update_date_str: YYYYMMDD 형식의 업데이트일자
    
    Returns:
        datetime: 분석기간 시작일 (전주 월요일)
    """
    year = int(update_date_str[:4])
    month = int(update_date_str[4:6])
    day = int(update_date_str[6:8])
    update_date = datetime(year, month, day)
    
    # 업데이트일자의 요일 (0=월요일, 6=일요일)
    day_of_week = update_date.weekday()
    
    # 전주 월요일까지의 일수 계산
    if day_of_week == 0:  # 월요일
        days_to_monday = 7
    elif day_of_week == 6:  # 일요일
        days_to_monday = 6
    else:  # 화~토요일
        days_to_monday = day_of_week + 7
    
    # 주차 시작일 계산 (전주 월요일)
    week_start_date = update_date - timedelta(days=days_to_monday)
    
    return week_start_date


def calculate_previous_year_date(week_start_date: datetime) -> datetime:
    """
    전년동기간 날짜 계산 (364일 전)
    
    Args:
        week_start_date: 분석기간 시작일
    
    Returns:
        datetime: 전년동기간 날짜
    """
    prev_year_date = week_start_date - timedelta(days=364)
    return prev_year_date


def load_progress_days(analysis_month: str, prev_year_date: datetime) -> dict:
    """
    진척일수 파일에서 전년동기간의 진척일수 읽기
    
    Args:
        analysis_month: 분석월 (YYYYMM 형식)
        prev_year_date: 전년동기간 날짜
    
    Returns:
        dict: 브랜드별 진척일수 {브랜드: 진척일수}
    """
    progress_days_path = project_root / "raw" / analysis_month / "previous_year" / f"progress_days_{analysis_month}.csv"
    
    if not progress_days_path.exists():
        raise FileNotFoundError(f"진척일수 파일을 찾을 수 없습니다: {progress_days_path}")
    
    df = pd.read_csv(progress_days_path, encoding='utf-8-sig')
    
    # 전년동기간 날짜를 컬럼명으로 찾기 (YYYY-MM-DD 형식)
    date_str = prev_year_date.strftime('%Y-%m-%d')
    
    if date_str not in df.columns:
        raise ValueError(f"진척일수 파일에 해당 날짜 컬럼이 없습니다: {date_str}")
    
    # 브랜드별 진척일수 딕셔너리 생성
    progress_days_dict = {}
    for idx, row in df.iterrows():
        brand = row['브랜드']
        progress_days = row[date_str]
        progress_days_dict[brand] = progress_days
    
    return progress_days_dict


def get_brand_progress_days(brand: str, channel_code: str, progress_days_dict: dict) -> float:
    """
    브랜드와 채널코드로부터 진척일수 가져오기
    
    Args:
        brand: 브랜드코드
        channel_code: 유통채널 코드
        progress_days_dict: 브랜드별 진척일수 딕셔너리
    
    Returns:
        float: 진척일수
    """
    # M(면세) = 브랜드코드가 M이며 유통코드가 2인건
    if brand == 'M' and str(channel_code) == '2':
        return progress_days_dict.get('M(면세)', 0.0)
    # M(면세제외) = 브랜드코드가 M이며 유통코드가 2가 아닌건
    elif brand == 'M' and str(channel_code) != '2':
        return progress_days_dict.get('M(면세제외)', 0.0)
    else:
        return progress_days_dict.get(brand, 0.0)


def calculate_forecast_value(current_value: float, progress_days: float, total_days: int) -> float:
    """
    진척율에 따라 월말 예상값 계산
    
    Args:
        current_value: 현재 값
        progress_days: 진척일수
        total_days: 월 총 일수
    
    Returns:
        float: 월말 예상값
    """
    if progress_days == 0:
        return 0.0
    
    forecast_value = (current_value / progress_days) * total_days
    return round(forecast_value, 0)


def calculate_direct_costs_for_forecast(
    df: pd.DataFrame, 
    plan_dir: str, 
    analysis_month: str,
    forecast_sales_col: str
) -> pd.DataFrame:
    """
    예상 매출액에 대해 직접비 계산 로직 적용
    
    Args:
        df: 데이터프레임 (예상 매출액이 이미 계산된 상태)
        plan_dir: 계획 파일 디렉토리
        analysis_month: 분석월 (YYYYMM 형식)
        forecast_sales_col: 예상 실판매액(V-) 컬럼명
    
    Returns:
        pd.DataFrame: 직접비가 계산된 데이터프레임
    """
    print("\n[직접비 계산] 예상 매출액에 대해 직접비 계산 로직 적용 중...")
    
    # 채널 마스터 로드
    channel_master = extract_direct.load_channel_master()
    
    # 계획 금액 추출 (지급임차료_매장(고정), 감가상각비_임차시설물 등)
    plan_amounts_df = extract_direct.extract_plan_amounts(plan_dir, channel_master)
    
    # 직접비율 추출
    rates_df = extract_direct.extract_direct_cost_rates(plan_dir, channel_master)
    
    # 로열티율 마스터 로드
    royalty_master = extract_direct.load_royalty_rate_master()
    
    # 직접비율 데이터를 브랜드/유통채널별로 딕셔너리로 변환
    rates_dict = {}
    for _, row in rates_df.iterrows():
        key = (row['브랜드'], row['유통채널'], row['직접비항목'])
        rates_dict[key] = row['비율'] / 100  # 퍼센트를 소수로 변환
    
    # 계획 파일 금액을 브랜드/유통채널별로 딕셔너리로 변환
    plan_amounts_dict = {}
    if not plan_amounts_df.empty:
        for _, row in plan_amounts_df.iterrows():
            key = (row['브랜드'], row['유통채널'], row['직접비항목'])
            plan_amounts_dict[key] = row['금액']
    
    # 출고매출액(V-) 컬럼 찾기
    shipping_col = None
    for col in df.columns:
        if '출고매출액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
            shipping_col = col
            break
    
    # 각 직접비 계산 필드별로 계산
    for cost_item in DIRECT_COST_CALC_FIELDS:
        df[cost_item] = 0.0
        print(f"  처리 중: {cost_item}")
        
        # 지급수수료_로열티는 로열티율 마스터 사용
        if cost_item == '지급수수료_로열티':
            for idx, row in df.iterrows():
                # 채널명이 "미지정"인 경우 스킵 (원본 데이터 유지)
                channel_name = str(row.get('채널명', '')).strip() if pd.notna(row.get('채널명')) else ''
                if channel_name == '미지정':
                    continue
                
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
                    
                    if '실판가' in base or '실판매' in base:
                        base_value = row[forecast_sales_col]
                    elif '출고가' in base or '출고매출' in base:
                        base_value = row[shipping_col] if shipping_col else row[forecast_sales_col]
                    else:
                        base_value = row[forecast_sales_col]
                    
                    if pd.notna(base_value) and base_value > 0:
                        cost_value = base_value * rate
                        df.at[idx, cost_item] = int(round(cost_value))
        
        # 나머지는 비율 기반 계산
        else:
            for idx, row in df.iterrows():
                # 채널명이 "미지정"인 경우 스킵 (원본 데이터 유지)
                channel_name = str(row.get('채널명', '')).strip() if pd.notna(row.get('채널명')) else ''
                if channel_name == '미지정':
                    continue
                
                brand = str(row.get('브랜드', '')).strip()
                channel_num = row.get('유통채널', None)
                sales_value = row[forecast_sales_col]
                
                if pd.isna(sales_value) or sales_value == 0:
                    continue
                
                if pd.isna(channel_num):
                    continue
                
                try:
                    channel_num_int = int(float(channel_num))
                except (ValueError, TypeError):
                    continue
                
                key = (brand, channel_num_int, cost_item)
                rate = rates_dict.get(key, 0)
                
                # 기타 채널(유통채널 99)이고 물류운송비 또는 물류용역비인 경우, 사입 채널(유통채널 8)의 비율 사용
                if channel_num_int == 99 and cost_item in ['지급수수료_물류운송비', '지급수수료_물류용역비']:
                    saip_key = (brand, 8, cost_item)
                    rate = rates_dict.get(saip_key, 0)
                
                if rate > 0:
                    cost_value = sales_value * rate
                    df.at[idx, cost_item] = int(round(cost_value))
    
    return df


def convert_ke30_to_forecast(
    update_date_str: str,
    input_file_path: Path,
    output_file_path: Path,
    progress_days_dict: dict,
    total_days: int,
    plan_dir: str,
    analysis_month: str
) -> pd.DataFrame:
    """
    KE30 파일을 월말 예상 데이터로 변환
    
    Args:
        update_date_str: 업데이트일자 (YYYYMMDD 형식)
        input_file_path: 입력 파일 경로
        output_file_path: 출력 파일 경로
        progress_days_dict: 브랜드별 진척일수 딕셔너리
        total_days: 월 총 일수
        plan_dir: 계획 파일 디렉토리
        analysis_month: 분석월 (YYYYMM 형식)
    
    Returns:
        pd.DataFrame: 변환된 데이터프레임
    """
    print(f"\n[변환 시작] {input_file_path.name} -> {output_file_path.name}")
    
    # CSV 파일 읽기
    df = pd.read_csv(input_file_path, encoding='utf-8-sig')
    print(f"  원본 데이터: {len(df)}행 × {len(df.columns)}열")
    
    # 브랜드와 유통채널 컬럼 확인
    if '브랜드' not in df.columns:
        raise ValueError("'브랜드' 컬럼이 없습니다.")
    if '유통채널' not in df.columns:
        raise ValueError("'유통채널' 컬럼이 없습니다.")
    
    # 결과 데이터프레임 복사
    df_forecast = df.copy()
    
    # 1) 진척율 계산 필드 처리
    print("\n[1단계] 진척율 계산 필드 처리 중...")
    for field in PROGRESS_RATE_FIELDS:
        # 유사한 컬럼명 찾기 (대소문자 무시)
        matching_cols = []
        field_lower = field.lower()
        for col in df.columns:
            col_str = str(col)
            # 정확한 매칭 우선, 그 다음 부분 매칭
            if col_str == field:
                matching_cols = [col]
                break
            elif field_lower in col_str.lower() or col_str.lower() in field_lower:
                # 부분 매칭이지만, 더 긴 필드명이 짧은 필드명에 포함되는 경우 제외
                # 예: '합계 : 실판매액'이 '합계 : 실판매액(V-)'에 포함되는 경우는 제외
                if field != '합계 : 실판매액' or '합계 : 실판매액(V-)' not in col_str:
                    matching_cols.append(col)
        
        if not matching_cols:
            print(f"  [WARNING] '{field}' 컬럼을 찾을 수 없습니다.")
            continue
        
        col_name = matching_cols[0]
        print(f"  처리 중: {col_name}")
        
        # 각 행에 대해 진척일수 가져오기 및 계산
        for idx, row in df.iterrows():
            # 채널명이 "미지정"인 경우 원본 데이터 그대로 유지
            channel_name = str(row.get('채널명', '')).strip() if pd.notna(row.get('채널명')) else ''
            if channel_name == '미지정':
                continue  # 원본 데이터 그대로 유지 (df_forecast는 이미 df.copy()로 복사됨)
            
            brand = str(row['브랜드']).strip()
            channel_code = str(row['유통채널']).strip() if pd.notna(row['유통채널']) else ''
            
            progress_days = get_brand_progress_days(brand, channel_code, progress_days_dict)
            current_value = row[col_name] if pd.notna(row[col_name]) else 0.0
            
            forecast_value = calculate_forecast_value(current_value, progress_days, total_days)
            df_forecast.at[idx, col_name] = forecast_value
    
    # 2) 동일한 필드값 유지 (이미 복사했으므로 그대로 사용)
    print("\n[2단계] 동일한 필드값 유지 (변경 없음)")
    
    # Shop_item 파일인지 확인 (직접비 계산 제외)
    is_shop_item = 'Shop_item' in str(input_file_path) or 'shop_item' in str(input_file_path).lower()
    
    # 3) 직접비 계산 필드 처리 (Shop 파일에만 적용)
    if is_shop_item:
        print("\n[3단계] 직접비 계산 필드 처리 건너뜀 (Shop_item 파일은 직접비 계산 제외)")
    else:
        print("\n[3단계] 직접비 계산 필드 처리 중...")
        # 예상 실판매액 컬럼 찾기 (V- 포함 우선, 없으면 일반 실판매액)
        forecast_sales_col = None
        for col in df_forecast.columns:
            if '실판매액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
                forecast_sales_col = col
                break
        
        # V-가 포함된 컬럼이 없으면 일반 실판매액 컬럼 사용
        if not forecast_sales_col:
            for col in df_forecast.columns:
                if '실판매액' in str(col):
                    forecast_sales_col = col
                    break
        
        if forecast_sales_col:
            print(f"  예상 실판매액 컬럼: {forecast_sales_col}")
            # 예상 매출액에 대해 직접비 재계산
            df_forecast = calculate_direct_costs_for_forecast(
                df_forecast,
                plan_dir,
                analysis_month,
                forecast_sales_col
            )
        else:
            print("  [WARNING] 예상 실판매액 컬럼을 찾을 수 없어 직접비 계산을 건너뜁니다.")
    
    # 4) 재계산 필드
    print("\n[4단계] 재계산 필드 처리 중...")
    
    # 매출총이익 = 합계 : 출고매출액(V-) Actual - 매출원가(평가감환입반영)
    print("  매출총이익 계산 중...")
    출고매출_col = None
    매출원가_col = None
    
    for col in df_forecast.columns:
        if '출고매출액(V-)' in str(col) or '출고매출액(V-) Actual' in str(col):
            출고매출_col = col
        if '매출원가(평가감환입반영)' in str(col):
            매출원가_col = col
    
    if 출고매출_col and 매출원가_col:
        df_forecast['매출총이익'] = (
            df_forecast[출고매출_col].fillna(0) - df_forecast[매출원가_col].fillna(0)
        ).astype(int)
    else:
        print("  [WARNING] 매출총이익 계산에 필요한 컬럼을 찾을 수 없습니다.")
    
    # 직접비합계 및 직접이익 계산 (Shop 파일에만 적용)
    if is_shop_item:
        print("  직접비합계 및 직접이익 계산 건너뜀 (Shop_item 파일은 직접비 계산 제외)")
    else:
        # 직접비합계 = 직접비 항목들의 합계
        print("  직접비합계 계산 중...")
        direct_cost_cols = [col for col in DIRECT_COST_ITEMS if col in df_forecast.columns]
        if direct_cost_cols:
            df_forecast['직접비 합계'] = df_forecast[direct_cost_cols].sum(axis=1).astype(int)
        else:
            print("  [WARNING] 직접비 항목 컬럼을 찾을 수 없습니다.")
            df_forecast['직접비 합계'] = 0
        
        # 직접이익 = 매출총이익 - 직접비합계
        print("  직접이익 계산 중...")
        if '매출총이익' in df_forecast.columns and '직접비 합계' in df_forecast.columns:
            df_forecast['직접이익'] = (
                df_forecast['매출총이익'].fillna(0) - df_forecast['직접비 합계'].fillna(0)
            ).astype(int)
        else:
            print("  [WARNING] 직접이익 계산에 필요한 컬럼을 찾을 수 없습니다.")
            df_forecast['직접이익'] = 0
    
    # CSV 저장
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    df_forecast.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    print(f"\n[OK] 변환 완료: {output_file_path}")
    print(f"   데이터: {len(df_forecast)}행 × {len(df_forecast.columns)}열")
    
    return df_forecast


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='KE30 파일을 진척일수를 사용하여 월말 예상 데이터로 변환',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/convert_ke30_to_forecast.py 20251117
  
설명:
  1. KE30 Shop 및 Shop_item 파일 읽기
  2. 진척일수 파일에서 전년동기간 진척일수 읽기
  3. 브랜드별 진척일수 매핑
  4. 각 필드별 계산
  5. forecast 파일로 저장
        """
    )
    
    parser.add_argument(
        'update_date',
        type=str,
        help='업데이트일자 (예: 20251117)'
    )
    
    args = parser.parse_args()
    
    update_date_str = args.update_date
    
    if len(update_date_str) != 8 or not update_date_str.isdigit():
        print("❌ 업데이트일자 형식이 올바르지 않습니다. (YYYYMMDD 형식 필요)")
        sys.exit(1)
    
    print("=" * 60)
    print("KE30 → Forecast 변환")
    print("=" * 60)
    print(f"📅 업데이트일자: {update_date_str}")
    print()
    
    try:
        # 분석기간 시작일 계산 (전주 월요일)
        week_start_date = calculate_week_start_date(update_date_str)
        print(f"📅 분석기간 시작일: {week_start_date.strftime('%Y-%m-%d')} (월요일)")
        
        # 전년동기간 날짜 계산
        prev_year_date = calculate_previous_year_date(week_start_date)
        print(f"📅 전년동기간 날짜: {prev_year_date.strftime('%Y-%m-%d')}")
        
        # 분석월 계산 (주차 시작일의 월)
        analysis_year = week_start_date.year
        analysis_month_num = week_start_date.month
        analysis_month = f"{analysis_year}{analysis_month_num:02d}"
        print(f"📅 분석월: {analysis_month}")
        
        # 월 총 일수 계산
        _, total_days = monthrange(analysis_year, analysis_month_num)
        print(f"📅 월 총 일수: {total_days}일")
        print()
        
        # 진척일수 파일 읽기
        print("[진척일수 파일 읽기] 시작...")
        progress_days_dict = load_progress_days(analysis_month, prev_year_date)
        print(f"✅ 진척일수 로드 완료: {len(progress_days_dict)}개 브랜드")
        for brand, days in progress_days_dict.items():
            print(f"   {brand}: {days}일")
        print()
        
        # 계획 파일 디렉토리
        plan_dir = project_root / "raw" / analysis_month / "plan"
        
        # 입력/출력 파일 경로
        date_output_dir = project_root / "raw" / analysis_month / "current_year" / update_date_str
        
        # Shop 파일 변환
        shop_input_path = date_output_dir / f"ke30_{update_date_str}_{analysis_month}_Shop.csv"
        shop_output_path = date_output_dir / f"forecast_{update_date_str}_{analysis_month}_Shop.csv"
        
        if shop_input_path.exists():
            print("=" * 60)
            convert_ke30_to_forecast(
                update_date_str,
                shop_input_path,
                shop_output_path,
                progress_days_dict,
                total_days,
                str(plan_dir),
                analysis_month
            )
        else:
            print(f"⚠️  Shop 파일을 찾을 수 없습니다: {shop_input_path}")
        
        # Shop_item 파일 변환
        shop_item_input_path = date_output_dir / f"ke30_{update_date_str}_{analysis_month}_Shop_item.csv"
        shop_item_output_path = date_output_dir / f"forecast_{update_date_str}_{analysis_month}_Shop_item.csv"
        
        if shop_item_input_path.exists():
            print("\n" + "=" * 60)
            convert_ke30_to_forecast(
                update_date_str,
                shop_item_input_path,
                shop_item_output_path,
                progress_days_dict,
                total_days,
                str(plan_dir),
                analysis_month
            )
        else:
            print(f"⚠️  Shop_item 파일을 찾을 수 없습니다: {shop_item_input_path}")
        
        print()
        print("=" * 60)
        print("✅ 변환 완료!")
        print("=" * 60)
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ 오류 발생: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

