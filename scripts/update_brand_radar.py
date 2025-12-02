"""
브랜드별 레이더 차트 데이터 업데이트 스크립트
==========================================

채널별/아이템별 계획, 전년, 당년 데이터를 추출하여 data.js에 추가합니다.

- 채널별: 당년(forecast), 전년(previous), 계획(plan)
- 아이템별: 당년(forecast), 전년(previous), 계획(plan_Item)

작성일: 2025-01
"""

import os
import sys
import json
import re
import pandas as pd
from typing import Dict, Optional
from path_utils import get_plan_file_path, get_previous_year_file_path, extract_year_month_from_date, get_current_year_file_path

ROOT = os.path.dirname(os.path.dirname(__file__))
PUBLIC_DIR = os.path.join(ROOT, "public")
RAW_DIR = os.path.join(ROOT, "raw")

def extract_numeric(value) -> float:
    """숫자 문자열(콤마 포함)을 float로 변환"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.replace(",", "").replace(" ", "").strip()
        if value == "" or value == "-":
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

# ============================================
# 채널별 데이터 처리 함수들
# ============================================

def load_plan_data(year_month: str) -> pd.DataFrame:
    """계획 전처리 완료 파일 로드"""
    filepath = get_plan_file_path(year_month)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_previous_year_data(year_month: str) -> pd.DataFrame:
    """전년 데이터 파일 로드"""
    filepath = get_previous_year_file_path(year_month, f"previous_rawdata_{year_month}_Shop.csv")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 전년 데이터 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_forecast_data(date_str: str, year_month: str) -> pd.DataFrame:
    """forecast 파일 로드 (당년 데이터)"""
    filename = f"forecast_{date_str}_{year_month}_Shop.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] forecast 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def extract_channel_plan_data(df_plan: pd.DataFrame) -> Dict:
    """
    계획 파일에서 브랜드별, 채널별 매출 추출
    
    Returns:
        Dict: {브랜드: {채널명: 매출액}}
    """
    print("\n[계산] 채널별 계획 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    channel_col = None
    sales_col = None
    
    for col in df_plan.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '채널' in col_str and channel_col is None:
            channel_col = col
        if '실판매액' in col_str and '[v+]' in col_str and sales_col is None:
            sales_col = col
    
    if not brand_col or not channel_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    채널 컬럼: {channel_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  채널 컬럼: {channel_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 채널명 매핑 (대시보드에서 사용하는 채널명으로 변환)
    channel_mapping = {
        '백화점': '백화점',
        '면세점': '면세점',
        '직영점': '직영점(가두)',
        '직영가두': '직영점(가두)',
        '직영점(가두)': '직영점(가두)',
        '대리점': '대리점',
        '자사몰': '자사몰',
        '제휴몰': '제휴몰',
        '온라인자사': '자사몰',
        '온라인제휴': '제휴몰',
        '온라인': '온라인',  # 온라인은 나중에 자사몰/제휴몰로 분할
        '직영몰': '직영몰',
        '아울렛': '아울렛',
        'RF': 'RF',
    }
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_plan.iterrows():
        brand_code = str(row[brand_col]).strip()
        channel_name = str(row[channel_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 내수합계는 제외
        if channel_name == '내수합계':
            continue
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 채널명 변환
        mapped_channel = channel_mapping.get(channel_name, channel_name)
        
        # 온라인 채널 처리 (자사몰/제휴몰로 분할하지 않고 온라인으로 유지)
        if mapped_channel == '온라인':
            if '온라인' not in result[brand_key]:
                result[brand_key]['온라인'] = 0
            result[brand_key]['온라인'] += sales
        else:
            if mapped_channel not in result[brand_key]:
                result[brand_key][mapped_channel] = 0
            result[brand_key][mapped_channel] += sales
    
    # 결과 출력
    for brand, channels in result.items():
        print(f"  {brand}:")
        for channel, sales in channels.items():
            print(f"    {channel}: {sales:,.0f}원")
    
    return result

def extract_channel_yoy_data(df_prev: pd.DataFrame) -> Dict:
    """
    전년 데이터 파일에서 브랜드별, 채널별 매출 추출
    
    Returns:
        Dict: {브랜드: {채널명: 매출액}}
    """
    print("\n[계산] 채널별 전년 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    channel_col = None
    sales_col = None
    
    for col in df_prev.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '채널명' in col_str and channel_col is None:
            channel_col = col
        if '실매출액' in col_str or '부가세제외 실판매액' in col_str:
            if sales_col is None:
                sales_col = col
            # '실매출액'이 부가세 포함이므로 우선 사용
            if '실매출액' in col_str and '부가세제외' not in col_str:
                sales_col = col
    
    if not brand_col or not channel_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    채널 컬럼: {channel_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  채널 컬럼: {channel_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 채널명 매핑
    channel_mapping = {
        '백화점': '백화점',
        '면세점': '면세점',
        '직영점': '직영점(가두)',
        '직영가두': '직영점(가두)',
        '직영점(가두)': '직영점(가두)',
        '대리점': '대리점',
        '자사몰': '자사몰',
        '제휴몰': '제휴몰',
        '온라인자사': '자사몰',
        '온라인제휴': '제휴몰',
        '온라인': '온라인',
        '직영몰': '직영몰',
        '아울렛': '아울렛',
        'RF': 'RF',
    }
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_prev.iterrows():
        brand_code = str(row[brand_col]).strip()
        channel_name = str(row[channel_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 채널명 변환
        mapped_channel = channel_mapping.get(channel_name, channel_name)
        
        # 온라인 채널 처리
        if mapped_channel == '온라인':
            if '온라인' not in result[brand_key]:
                result[brand_key]['온라인'] = 0
            result[brand_key]['온라인'] += sales
        else:
            if mapped_channel not in result[brand_key]:
                result[brand_key][mapped_channel] = 0
            result[brand_key][mapped_channel] += sales
    
    # 결과 출력
    for brand, channels in result.items():
        print(f"  {brand}:")
        for channel, sales in channels.items():
            print(f"    {channel}: {sales:,.0f}원")
    
    return result

def extract_channel_current_data(df_forecast: pd.DataFrame) -> Dict:
    """
    forecast 파일에서 브랜드별, 채널별 매출 추출 (당년 데이터)
    
    Returns:
        Dict: {브랜드: {채널명: 매출액}}
    """
    print("\n[계산] 채널별 당년 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    channel_col = None
    sales_col = None
    
    for col in df_forecast.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '채널명' in col_str and channel_col is None:
            channel_col = col
        # forecast 파일에서는 '합계 : 실판매액' 사용 (부가세 포함)
        if '실판매액' in col_str:
            # '합계 : 실판매액' 우선 (부가세 포함), '(V-)'가 없는 것
            if '(V-)' not in col_str and '(v-)' not in col_str:
                if sales_col is None:
                    sales_col = col
            # '(V-)'가 있는 것은 폴백으로만 사용
            elif sales_col is None:
                sales_col = col
    
    if not brand_col or not channel_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    채널 컬럼: {channel_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  채널 컬럼: {channel_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 채널명 매핑
    channel_mapping = {
        '백화점': '백화점',
        '면세점': '면세점',
        '직영점': '직영점(가두)',
        '직영점(가두)': '직영점(가두)',
        '직영가두': '직영점(가두)',
        '대리점': '대리점',
        '자사몰': '자사몰',
        '제휴몰': '제휴몰',
        '온라인자사': '자사몰',
        '온라인제휴': '제휴몰',
        '온라인': '온라인',
        '직영몰': '직영몰',
        '아울렛': '아울렛',
        'RF': 'RF',
    }
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_forecast.iterrows():
        brand_code = str(row[brand_col]).strip()
        channel_name = str(row[channel_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 채널명 변환
        mapped_channel = channel_mapping.get(channel_name, channel_name)
        
        # 온라인 채널 처리
        if mapped_channel == '온라인':
            if '온라인' not in result[brand_key]:
                result[brand_key]['온라인'] = 0
            result[brand_key]['온라인'] += sales
        else:
            if mapped_channel not in result[brand_key]:
                result[brand_key][mapped_channel] = 0
            result[brand_key][mapped_channel] += sales
    
    # 결과 출력
    for brand, channels in result.items():
        print(f"  {brand}:")
        for channel, sales in channels.items():
            print(f"    {channel}: {sales:,.0f}원")
    
    return result

def extract_brand_total_plan_data(df_plan: pd.DataFrame) -> Dict:
    """
    계획 파일에서 브랜드별 전체 계획 매출 추출 (내수합계)
    
    Returns:
        Dict: {브랜드: 전체계획매출액}
    """
    print("\n[계산] 브랜드별 전체 계획 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    channel_col = None
    sales_col = None
    
    for col in df_plan.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '채널' in col_str and channel_col is None:
            channel_col = col
        if '실판매액' in col_str and '[v+]' in col_str and sales_col is None:
            sales_col = col
    
    if not brand_col or not channel_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    채널 컬럼: {channel_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  채널 컬럼: {channel_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_plan.iterrows():
        brand_code = str(row[brand_col]).strip()
        channel_name = str(row[channel_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 내수합계만 추출
        if channel_name != '내수합계':
            continue
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        
        if brand_key not in result:
            result[brand_key] = 0
        result[brand_key] += sales
    
    # 결과 출력
    for brand, total_sales in result.items():
        print(f"  {brand}: {total_sales:,.0f}원")
    
    return result

def extract_brand_total_item_plan_data(df_plan: pd.DataFrame) -> Dict:
    """
    아이템 계획 파일에서 브랜드별 전체 계획 매출 추출
    
    Returns:
        Dict: {브랜드: 전체계획매출액}
    """
    print("\n[계산] 브랜드별 전체 아이템 계획 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    sales_col = None
    
    for col in df_plan.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '실판매액' in col_str and sales_col is None:
            sales_col = col
    
    if not brand_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_plan.iterrows():
        brand_code = str(row[brand_col]).strip()
        sales = extract_numeric(row[sales_col]) * 1000  # 1000 곱하기 (아이템 계획 파일 특성)
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        
        if brand_key not in result:
            result[brand_key] = 0
        result[brand_key] += sales
    
    # 결과 출력
    for brand, total_sales in result.items():
        print(f"  {brand}: {total_sales:,.0f}원")
    
    return result


# ============================================
# 아이템별 데이터 처리 함수들
# ============================================

# 아이템 중분류 매핑 (데이터 -> 표시명)
ITEM_MAPPING = {
    'Headwear': '모자',
    'Bag': '가방',
    'Shoes': '신발',
    'Acc_etc': '기타',
    '당시즌의류': '당시즌의류',
    '과시즌의류': '과시즌의류',
    '차시즌의류': '차시즌의류',
    '모자': '모자',
    '가방': '가방',
    '신발': '신발',
    '기타': '기타',
}

# 표시명 -> 데이터명 (역매핑)
ITEM_REVERSE_MAPPING = {
    '모자': 'Headwear',
    '가방': 'Bag',
    '신발': 'Shoes',
    '기타': 'Acc_etc',
}

def load_item_plan_data(year_month: str) -> pd.DataFrame:
    """아이템 계획 파일 로드"""
    filepath = os.path.join(RAW_DIR, year_month, "plan", "plan_Item.csv")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 아이템 계획 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_item_previous_year_data(year_month: str) -> pd.DataFrame:
    """아이템 전년 데이터 파일 로드"""
    filepath = os.path.join(RAW_DIR, year_month, "previous_year", f"previous_rawdata_{year_month}_Shop_Item.csv")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 아이템 전년 데이터 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_item_forecast_data(date_str: str, year_month: str) -> pd.DataFrame:
    """아이템 forecast 파일 로드 (당년 데이터)"""
    filename = f"forecast_{date_str}_{year_month}_Shop_item.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 아이템 forecast 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def extract_item_plan_data(df_plan: pd.DataFrame) -> Dict:
    """
    아이템 계획 파일에서 브랜드별, 아이템별 매출 추출
    계획 파일은 실판매액에 1000을 곱해야 함
    
    Returns:
        Dict: {브랜드: {아이템명: 매출액}}
    """
    print("\n[계산] 아이템별 계획 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    item_col = None
    sales_col = None
    
    for col in df_plan.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '아이템_중분류' in col_str and item_col is None:
            item_col = col
        if '실판매액' in col_str and sales_col is None:
            sales_col = col
    
    if not brand_col or not item_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    아이템 컬럼: {item_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  아이템 컬럼: {item_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_plan.iterrows():
        brand_code = str(row[brand_col]).strip()
        item_name = str(row[item_col]).strip()
        sales = extract_numeric(row[sales_col]) * 1000  # 1000 곱하기
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 아이템명 매핑
        mapped_item = ITEM_MAPPING.get(item_name, item_name)
        
        if mapped_item not in result[brand_key]:
            result[brand_key][mapped_item] = 0
        result[brand_key][mapped_item] += sales
    
    # 결과 출력
    for brand, items in result.items():
        print(f"  {brand}:")
        for item, sales in items.items():
            print(f"    {item}: {sales:,.0f}원")
    
    return result

def extract_item_yoy_data(df_prev: pd.DataFrame) -> Dict:
    """
    아이템 전년 데이터 파일에서 브랜드별, 아이템별 매출 추출
    
    Returns:
        Dict: {브랜드: {아이템명: 매출액}}
    """
    print("\n[계산] 아이템별 전년 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    item_col = None
    sales_col = None
    
    for col in df_prev.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '아이템_중분류' in col_str and item_col is None:
            item_col = col
        if '실매출액' in col_str:
            if sales_col is None:
                sales_col = col
            # '실매출액'이 부가세 포함이므로 우선 사용
            if '부가세제외' not in col_str:
                sales_col = col
    
    if not brand_col or not item_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    아이템 컬럼: {item_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  아이템 컬럼: {item_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_prev.iterrows():
        brand_code = str(row[brand_col]).strip()
        item_name = str(row[item_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 아이템명 매핑
        mapped_item = ITEM_MAPPING.get(item_name, item_name)
        
        if mapped_item not in result[brand_key]:
            result[brand_key][mapped_item] = 0
        result[brand_key][mapped_item] += sales
    
    # 결과 출력
    for brand, items in result.items():
        print(f"  {brand}:")
        for item, sales in items.items():
            print(f"    {item}: {sales:,.0f}원")
    
    return result

def extract_item_current_data(df_forecast: pd.DataFrame) -> Dict:
    """
    아이템 forecast 파일에서 브랜드별, 아이템별 매출 추출 (당년 데이터)
    
    Returns:
        Dict: {브랜드: {아이템명: 매출액}}
    """
    print("\n[계산] 아이템별 당년 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = None
    item_col = None
    sales_col = None
    
    for col in df_forecast.columns:
        col_str = str(col).strip()
        if '브랜드' in col_str and brand_col is None:
            brand_col = col
        if '아이템_중분류' in col_str and item_col is None:
            item_col = col
        # forecast 파일에서는 '합계 : 실판매액' 사용 (부가세 포함)
        if '실판매액' in col_str:
            if '(V-)' not in col_str and '(v-)' not in col_str:
                if sales_col is None:
                    sales_col = col
            elif sales_col is None:
                sales_col = col
    
    if not brand_col or not item_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    아이템 컬럼: {item_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  아이템 컬럼: {item_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    # 브랜드 코드 매핑
    brand_mapping = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    
    result = {}
    
    for _, row in df_forecast.iterrows():
        brand_code = str(row[brand_col]).strip()
        item_name = str(row[item_col]).strip()
        sales = extract_numeric(row[sales_col])
        
        # 브랜드 코드 변환
        brand_key = brand_mapping.get(brand_code, brand_code)
        if brand_key not in result:
            result[brand_key] = {}
        
        # 아이템명 매핑
        mapped_item = ITEM_MAPPING.get(item_name, item_name)
        
        if mapped_item not in result[brand_key]:
            result[brand_key][mapped_item] = 0
        result[brand_key][mapped_item] += sales
    
    # 결과 출력
    for brand, items in result.items():
        print(f"  {brand}:")
        for item, sales in items.items():
            print(f"    {item}: {sales:,.0f}원")
    
    return result


# ============================================
# data.js 업데이트 함수
# ============================================

def update_data_js_with_radar_data(
    data_js_path: str, 
    channel_plan: Dict, 
    channel_yoy: Dict, 
    channel_current: Dict,
    item_plan: Dict,
    item_yoy: Dict,
    item_current: Dict
):
    """data.js 파일에 채널별/아이템별 계획/전년/당년 데이터 추가"""
    
    if not os.path.exists(data_js_path):
        print(f"[WARNING] data.js 파일이 없습니다: {data_js_path}")
        return
    
    # 브랜드명 -> 브랜드 코드 매핑 (data.js에서 사용하는 코드)
    brand_name_to_code = {
        'MLB': 'M',
        'MLB_KIDS': 'I',
        'DISCOVERY': 'X',
        'DUVETICA': 'V',
        'SERGIO': 'ST',
        'SUPRA': 'W'
    }
    
    # 기존 파일 읽기
    with open(data_js_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # brandKPI 객체 찾기
    brand_kpi_match = re.search(r'var\s+brandKPI\s*=\s*({.*?});', content, re.DOTALL)
    if not brand_kpi_match:
        print("[WARNING] brandKPI 객체를 찾을 수 없습니다.")
        return
    
    # brandKPI 객체 파싱
    try:
        brand_kpi_str = brand_kpi_match.group(1)
        brand_kpi_obj = json.loads(brand_kpi_str)
    except:
        print("[WARNING] brandKPI 객체를 파싱할 수 없습니다.")
        return
    
    # 각 브랜드에 채널별/아이템별 데이터 추가
    for brand_code in brand_kpi_obj.keys():
        # 브랜드 코드에 해당하는 브랜드명 찾기
        brand_name = None
        for name, code in brand_name_to_code.items():
            if code == brand_code:
                brand_name = name
                break
        
        if brand_name:
            # 채널별 데이터
            if brand_name in channel_plan:
                brand_kpi_obj[brand_code]['channelPlan'] = channel_plan[brand_name]
            
            if brand_name in channel_yoy:
                brand_kpi_obj[brand_code]['channelYoy'] = channel_yoy[brand_name]
            
            if brand_name in channel_current:
                brand_kpi_obj[brand_code]['channelCurrent'] = channel_current[brand_name]
            
            # 아이템별 데이터
            if brand_name in item_plan:
                brand_kpi_obj[brand_code]['itemPlan'] = item_plan[brand_name]
            
            if brand_name in item_yoy:
                brand_kpi_obj[brand_code]['itemYoy'] = item_yoy[brand_name]
            
            if brand_name in item_current:
                brand_kpi_obj[brand_code]['itemCurrent'] = item_current[brand_name]
    
    # 파일 업데이트
    new_brand_kpi_str = json.dumps(brand_kpi_obj, ensure_ascii=False, indent=2)
    new_content = re.sub(
        r'var\s+brandKPI\s*=\s*{.*?};',
        f'var brandKPI = {new_brand_kpi_str};',
        content,
        flags=re.DOTALL
    )
    
    with open(data_js_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"\n[저장] {data_js_path}")
    print(f"  채널별 계획 데이터: {len(channel_plan)}개 브랜드")
    print(f"  채널별 전년 데이터: {len(channel_yoy)}개 브랜드")
    print(f"  채널별 당년 데이터: {len(channel_current)}개 브랜드")
    print(f"  아이템별 계획 데이터: {len(item_plan)}개 브랜드")
    print(f"  아이템별 전년 데이터: {len(item_yoy)}개 브랜드")
    print(f"  아이템별 당년 데이터: {len(item_current)}개 브랜드")


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="브랜드별 레이더 차트 데이터 업데이트 (채널별/아이템별)")
    parser.add_argument("date", help="YYYYMMDD 형식의 날짜 (예: 20251124)")
    
    args = parser.parse_args()
    
    date_str = args.date
    year_month = extract_year_month_from_date(date_str)
    
    print("=" * 60)
    print("브랜드별 레이더 차트 데이터 업데이트")
    print("=" * 60)
    print(f"날짜: {date_str}")
    print(f"연월: {year_month}")
    
    try:
        # ============================================
        # 채널별 데이터 처리
        # ============================================
        print("\n" + "=" * 40)
        print("채널별 데이터 처리")
        print("=" * 40)
        
        # 1. 계획 데이터 로드 및 추출
        df_plan = load_plan_data(year_month)
        channel_plan = extract_channel_plan_data(df_plan)
        
        # 2. 전년 데이터 로드 및 추출
        df_prev = load_previous_year_data(year_month)
        channel_yoy = extract_channel_yoy_data(df_prev)
        
        # 3. 당년 데이터 로드 및 추출 (forecast 파일)
        df_forecast = load_forecast_data(date_str, year_month)
        channel_current = extract_channel_current_data(df_forecast)
        
        # 4. 브랜드별 전체 계획 매출 추출 (채널별)
        brand_total_plan = extract_brand_total_plan_data(df_plan)
        
        # ============================================
        # 아이템별 데이터 처리
        # ============================================
        print("\n" + "=" * 40)
        print("아이템별 데이터 처리")
        print("=" * 40)
        
        # 5. 아이템 계획 데이터 로드 및 추출
        df_item_plan = load_item_plan_data(year_month)
        item_plan = extract_item_plan_data(df_item_plan)
        
        # 6. 아이템 전년 데이터 로드 및 추출
        df_item_prev = load_item_previous_year_data(year_month)
        item_yoy = extract_item_yoy_data(df_item_prev)
        
        # 7. 아이템 당년 데이터 로드 및 추출
        df_item_forecast = load_item_forecast_data(date_str, year_month)
        item_current = extract_item_current_data(df_item_forecast)
        
        # 8. 브랜드별 전체 아이템 계획 매출 추출
        brand_total_item_plan = extract_brand_total_item_plan_data(df_item_plan)
        
        # ============================================
        # data.js 파일 업데이트
        # ============================================
        data_js_path = os.path.join(PUBLIC_DIR, f"data_{date_str}.js")
        update_data_js_with_radar_data(
            data_js_path, 
            channel_plan, channel_yoy, channel_current,
            item_plan, item_yoy, item_current
        )
        
        print("\n[OK] 브랜드별 레이더 차트 데이터 업데이트 완료")
        
        # ★★★ JSON 파일로도 저장 ★★★
        json_dir = os.path.join(PUBLIC_DIR, "data", date_str)
        os.makedirs(json_dir, exist_ok=True)
        
        radar_data = {
            'channelPlan': channel_plan,
            'channelYoy': channel_yoy,
            'channelCurrent': channel_current,
            'itemPlan': item_plan,
            'itemYoy': item_yoy,
            'itemCurrent': item_current,
            'brandTotalPlan': brand_total_plan,  # 브랜드별 전체 계획 (채널별)
            'brandTotalItemPlan': brand_total_item_plan  # 브랜드별 전체 계획 (아이템별)
        }
        
        json_path = os.path.join(json_dir, "radar_chart.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(radar_data, f, ensure_ascii=False, indent=2)
        print(f"  ✅ JSON 저장: {json_path}")
        
        # ★★★ weekly_trend.json에도 브랜드별 계획 데이터 추가 ★★★
        weekly_trend_path = os.path.join(json_dir, "weekly_trend.json")
        if os.path.exists(weekly_trend_path):
            print(f"\n[업데이트] weekly_trend.json에 브랜드별 계획 데이터 추가...")
            with open(weekly_trend_path, 'r', encoding='utf-8') as f:
                weekly_trend_data = json.load(f)
            
            # 브랜드 코드 매핑 (MLB -> M 등)
            brand_code_mapping = {
                'MLB': 'M',
                'MLB_KIDS': 'I',
                'DISCOVERY': 'X',
                'DUVETICA': 'V',
                'SERGIO': 'ST',
                'SUPRA': 'W'
            }
            
            # 브랜드별 계획 데이터 추가
            if 'byBrand' not in weekly_trend_data:
                weekly_trend_data['byBrand'] = {}
            
            for brand_name, brand_code in brand_code_mapping.items():
                if brand_code in weekly_trend_data['byBrand']:
                    # 브랜드별 전체 계획 추가
                    if brand_name in brand_total_plan:
                        weekly_trend_data['byBrand'][brand_code]['totalPlan'] = brand_total_plan[brand_name]
                        print(f"  ✓ {brand_code} ({brand_name}): 전체 계획 {brand_total_plan[brand_name]:,.0f}원 추가")
            
            # 파일 저장
            with open(weekly_trend_path, 'w', encoding='utf-8') as f:
                json.dump(weekly_trend_data, f, ensure_ascii=False, indent=2)
            print(f"  ✅ weekly_trend.json 업데이트 완료: {weekly_trend_path}")
        else:
            print(f"  [WARNING] weekly_trend.json 파일을 찾을 수 없습니다: {weekly_trend_path}")
        
    except Exception as e:
        print(f"[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()



