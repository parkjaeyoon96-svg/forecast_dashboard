"""
트리맵 데이터 생성 스크립트 (v2)
================================

데이터 소스: ke30_YYYYMMDD_YYYYMM_Shop_item.csv

생성물:
1. 채널별 매출구성(현시점): 채널 → 아이템_중분류 → 아이템_소분류
2. 아이템별 매출구성(현시점): 아이템_중분류 → 채널

작성일: 2025-01
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from path_utils import get_current_year_file_path, extract_year_month_from_date

ROOT = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(ROOT, "public")

def find_treemap_preprocessed_file(date_str: str) -> str:
    """
    트리맵 데이터 파일 찾기 (ke30_Shop_item.csv 우선)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251215")
    
    Returns:
        str: 파일 경로
    """
    year_month = extract_year_month_from_date(date_str)
    
    # 1순위: ke30_YYYYMMDD_YYYYMM_Shop_item.csv
    filename = f"ke30_{date_str}_{year_month}_Shop_item.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if os.path.exists(filepath):
        print(f"[읽기] {filepath} (ke30_Shop_item)")
        return filepath
    
    # 2순위: treemap_preprocessed_{date}.csv (하위 호환)
    filename = f"treemap_preprocessed_{date_str}.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if os.path.exists(filepath):
        print(f"[읽기] {filepath} (전처리 파일)")
        return filepath
    
    raise FileNotFoundError(f"[ERROR] 트리맵 데이터 파일을 찾을 수 없습니다.\n"
                          f"  - ke30_{date_str}_{year_month}_Shop_item.csv\n"
                          f"  - treemap_preprocessed_{date_str}.csv")


def load_treemap_data(filepath: str) -> pd.DataFrame:
    """
    트리맵 데이터 로드 (ke30_Shop_item.csv 또는 전처리 파일)
    
    Args:
        filepath: 파일 경로
    
    Returns:
        pd.DataFrame: 데이터프레임
    """
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    # ke30_Shop_item.csv 파일의 컬럼명 통일
    column_mapping = {
        '합계 : 판매금액(TAG가)': 'TAG매출',
        '합계 : 실판매액': '실판매출',
        'PRDT_HRRC3_NM': '아이템_소분류'
    }
    df = df.rename(columns=column_mapping)
    
    # 필요한 컬럼 확인
    required_cols = ['브랜드', '채널명', '아이템_중분류', '아이템_소분류', 'TAG매출', '실판매출']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"  사용 가능한 컬럼: {list(df.columns)}")
        raise ValueError(f"[ERROR] 필수 컬럼 누락: {missing_cols}")
    
    # 숫자 변환
    df['TAG매출'] = pd.to_numeric(df['TAG매출'], errors="coerce").fillna(0)
    df['실판매출'] = pd.to_numeric(df['실판매출'], errors="coerce").fillna(0)
    
    # 실판매액으로 컬럼명 통일 (기존 로직 호환성)
    df = df.rename(columns={'실판매출': '실판매액'})
    
    print(f"  브랜드: {df['브랜드'].nunique()}개")
    print(f"  채널: {df['채널명'].nunique()}개")
    print(f"  아이템_중분류: {df['아이템_중분류'].nunique()}개")
    
    return df

def calculate_discount_rate(tag: float, sales: float) -> float:
    """할인율 계산: 1 - (실판매액 / TAG매출)"""
    if tag == 0:
        return 0.0
    return (1 - (sales / tag)) * 100

def calculate_share(value: float, total: float) -> int:
    """비중 계산 (정수%)"""
    if total == 0:
        return 0
    return int(round((value / total) * 100))

def calculate_yoy(current_value: float, previous_value: float) -> float:
    """
    YOY (전년대비 증감률) 계산
    
    Args:
        current_value: 당년 값
        previous_value: 전년 값
    
    Returns:
        float: YOY (%) - 소수점 1자리
    """
    if previous_value == 0:
        return 0.0 if current_value == 0 else 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 1)

def load_previous_year_treemap_data(date_str: str) -> pd.DataFrame:
    """
    전년 트리맵 데이터 로드 (전처리 완료된 데이터)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251215")
    
    Returns:
        pd.DataFrame: 전년 데이터프레임 (None if not exists)
    """
    year_month = extract_year_month_from_date(date_str)
    prev_filepath = os.path.join(ROOT, "raw", year_month, "previous_year", f"treemap_preprocessed_prev_{date_str}.csv")
    
    if not os.path.exists(prev_filepath):
        print(f"[경고] 전년 트리맵 데이터를 찾을 수 없습니다: {prev_filepath}")
        print("  YOY 계산 없이 진행합니다.")
        return None
    
    print(f"[읽기] {prev_filepath}")
    df = pd.read_csv(prev_filepath, encoding="utf-8-sig")
    print(f"  전년 데이터: {len(df)}행 × {len(df.columns)}열")
    
    # 컬럼명 통일 (전년 데이터는 판매금액(TAG가), 실판매액으로 저장됨)
    if '판매금액(TAG가)' in df.columns:
        df = df.rename(columns={'판매금액(TAG가)': 'TAG매출'})
    if '실판매출' in df.columns:
        df = df.rename(columns={'실판매출': '실판매액'})
    
    # 아이템소분류 컬럼명 통일 (전년 데이터는 언더스코어 없음)
    if '아이템소분류' in df.columns:
        df = df.rename(columns={'아이템소분류': '아이템_소분류'})
    
    # 브랜드 컬럼 통일 (전년 데이터는 '브랜드'가 브랜드코드)
    if '브랜드' in df.columns and '브랜드코드' not in df.columns:
        df['브랜드코드'] = df['브랜드']
    
    # 숫자 변환
    df['TAG매출'] = pd.to_numeric(df['TAG매출'], errors="coerce").fillna(0)
    df['실판매액'] = pd.to_numeric(df['실판매액'], errors="coerce").fillna(0)
    
    return df

def create_channel_treemap(df: pd.DataFrame, prev_df: pd.DataFrame = None, brand: str = None) -> dict:
    """
    채널별 매출구성 트리맵 생성 (YOY 포함)
    드릴다운: 채널 → 아이템_중분류 → 아이템_소분류
    
    Args:
        df: 당년 데이터프레임
        prev_df: 전년 데이터프레임 (YOY 계산용, None 가능)
        brand: 브랜드 필터 (None이면 전체)
    """
    if brand:
        print(f"\n[계산] 채널별 매출구성 트리맵 생성 (브랜드: {brand})...")
        df = df[df['브랜드'] == brand].copy()
        if prev_df is not None:
            # 전년 데이터에서 브랜드 코드 매핑
            brand_code_map = {'MLB': 'M', 'DISCOVERY': 'V', 'SUPRA': 'X', 'MLB_KIDS': 'I', 'SERGIO': 'ST', 'DUVETICA': 'W'}
            brand_code = brand_code_map.get(brand, brand)
            prev_df = prev_df[prev_df['브랜드코드'] == brand_code].copy()
    else:
        print("\n[계산] 채널별 매출구성 트리맵 생성 (전체)...")
    
    # 전체 합계 계산
    total_tag = df['TAG매출'].sum()
    total_sales = df['실판매액'].sum()
    
    # 전년 전체 합계
    prev_total_sales = 0
    prev_total_tag = 0
    if prev_df is not None:
        # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
        prev_total_tag = prev_df['TAG매출'].sum()
        prev_total_sales = prev_df['실판매액'].sum()
    
    result = {
        'total': {
            'tag': int(total_tag),
            'sales': int(total_sales),
            'discountRate': round(calculate_discount_rate(total_tag, total_sales), 1),
            'prevDiscountRate': round(calculate_discount_rate(prev_total_tag, prev_total_sales), 1) if prev_df is not None and prev_total_tag > 0 else None,
            'yoy': calculate_yoy(total_sales, prev_total_sales) if prev_df is not None else None
        },
        'channels': {}
    }
    
    # 1단계: 채널별 집계
    channel_sum = df.groupby('채널명', as_index=False).agg({
        'TAG매출': 'sum',
        '실판매액': 'sum'
    })
    
    # 전년 채널별 집계
    prev_channel_sales = {}
    prev_channel_tags = {}
    if prev_df is not None:
        # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
        prev_channel_sum = prev_df.groupby('채널명', as_index=False).agg({
            'TAG매출': 'sum',
            '실판매액': 'sum'
        })
        for _, prow in prev_channel_sum.iterrows():
            pchannel = str(prow['채널명']).strip()
            prev_channel_tags[pchannel] = float(prow['TAG매출'])
            prev_channel_sales[pchannel] = float(prow['실판매액'])
    
    for _, row in channel_sum.iterrows():
        channel = str(row['채널명']).strip()
        tag = float(row['TAG매출'])
        sales = float(row['실판매액'])
        
        # YOY 및 전년할인율 계산
        prev_sales = prev_channel_sales.get(channel, 0)
        prev_tag = prev_channel_tags.get(channel, 0)
        yoy = calculate_yoy(sales, prev_sales) if prev_df is not None else None
        prev_discount = round(calculate_discount_rate(prev_tag, prev_sales), 1) if prev_df is not None and prev_tag > 0 else None
        
        # 채널별 정보 저장
        result['channels'][channel] = {
            'tag': int(tag),
            'sales': int(sales),
            'share': calculate_share(sales, total_sales),
            'discountRate': round(calculate_discount_rate(tag, sales), 1),
            'prevDiscountRate': prev_discount,  # ★ 전년할인율 추가 ★
            'yoy': yoy,
            'itemCategories': {}  # 아이템_중분류별 데이터
        }
        
        # 2단계: 채널 내 아이템_중분류별 집계
        channel_df = df[df['채널명'] == channel]
        item_mid_sum = channel_df.groupby('아이템_중분류', as_index=False).agg({
            'TAG매출': 'sum',
            '실판매액': 'sum'
        })
        
        channel_total_sales = channel_df['실판매액'].sum()
        channel_total_tag = channel_df['TAG매출'].sum()
        
        # 전년 채널-아이템 데이터
        prev_channel_item_sales = {}
        prev_channel_item_tags = {}
        if prev_df is not None:
            prev_channel_df = prev_df[prev_df['채널명'] == channel]
            if not prev_channel_df.empty:
                # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
                prev_item_sum = prev_channel_df.groupby('아이템_중분류', as_index=False).agg({
                    'TAG매출': 'sum',
                    '실판매액': 'sum'
                })
                for _, pitem_row in prev_item_sum.iterrows():
                    pitem = str(pitem_row['아이템_중분류']).strip()
                    prev_channel_item_tags[pitem] = float(pitem_row['TAG매출'])
                    prev_channel_item_sales[pitem] = float(pitem_row['실판매액'])
        
        for _, item_row in item_mid_sum.iterrows():
            item_mid = str(item_row['아이템_중분류']).strip()
            item_tag = float(item_row['TAG매출'])
            item_sales = float(item_row['실판매액'])
            
            # YOY 및 전년할인율 계산
            prev_item_sales = prev_channel_item_sales.get(item_mid, 0)
            prev_item_tag = prev_channel_item_tags.get(item_mid, 0)
            item_yoy = calculate_yoy(item_sales, prev_item_sales) if prev_df is not None else None
            prev_item_discount = round(calculate_discount_rate(prev_item_tag, prev_item_sales), 1) if prev_df is not None and prev_item_tag > 0 else None
            
            # 아이템_중분류별 정보 저장
            result['channels'][channel]['itemCategories'][item_mid] = {
                'tag': int(item_tag),
                'sales': int(item_sales),
                'share': calculate_share(item_sales, channel_total_sales),  # 채널 내 비중
                'discountRate': round(calculate_discount_rate(item_tag, item_sales), 1),
                'prevDiscountRate': prev_item_discount,  # ★ 전년할인율 추가 ★
                'yoy': item_yoy,
                'subCategories': {}  # 아이템_소분류별 데이터
            }
            
            # 3단계: 채널-중분류 내 아이템_소분류별 집계
            item_mid_df = channel_df[channel_df['아이템_중분류'] == item_mid]
            item_sub_sum = item_mid_df.groupby('아이템_소분류', as_index=False).agg({
                'TAG매출': 'sum',
                '실판매액': 'sum'
            })
            
            item_mid_total_sales = item_mid_df['실판매액'].sum()
            item_mid_total_tag = item_mid_df['TAG매출'].sum()
            
            # 전년 채널-아이템중분류-소분류 데이터
            prev_sub_sales = {}
            prev_sub_tags = {}
            if prev_df is not None:
                prev_channel_df = prev_df[prev_df['채널명'] == channel]
                if not prev_channel_df.empty:
                    prev_item_mid_df = prev_channel_df[prev_channel_df['아이템_중분류'] == item_mid]
                    if not prev_item_mid_df.empty:
                        prev_sub_sum = prev_item_mid_df.groupby('아이템_소분류', as_index=False).agg({
                            'TAG매출': 'sum',
                            '실판매액': 'sum'
                        })
                        for _, psub_row in prev_sub_sum.iterrows():
                            psub = str(psub_row['아이템_소분류']).strip()
                            prev_sub_tags[psub] = float(psub_row['TAG매출'])
                            prev_sub_sales[psub] = float(psub_row['실판매액'])
            
            for _, sub_row in item_sub_sum.iterrows():
                item_sub = str(sub_row['아이템_소분류']).strip()
                sub_tag = float(sub_row['TAG매출'])
                sub_sales = float(sub_row['실판매액'])
                
                # YOY 및 전년할인율 계산
                prev_sub_sale = prev_sub_sales.get(item_sub, 0)
                prev_sub_tag = prev_sub_tags.get(item_sub, 0)
                sub_yoy = calculate_yoy(sub_sales, prev_sub_sale) if prev_df is not None else None
                prev_sub_discount = round(calculate_discount_rate(prev_sub_tag, prev_sub_sale), 1) if prev_df is not None and prev_sub_tag > 0 else None
                
                # 아이템_소분류별 정보 저장
                result['channels'][channel]['itemCategories'][item_mid]['subCategories'][item_sub] = {
                    'tag': int(sub_tag),
                    'sales': int(sub_sales),
                    'share': calculate_share(sub_sales, item_mid_total_sales),  # 중분류 내 비중
                    'discountRate': round(calculate_discount_rate(sub_tag, sub_sales), 1),
                    'prevDiscountRate': prev_sub_discount,  # ★ 전년할인율 추가 ★
                    'yoy': sub_yoy  # ★ YOY 추가 ★
                }
    
    print(f"  채널 수: {len(result['channels'])}")
    return result

def create_item_treemap(df: pd.DataFrame, prev_df: pd.DataFrame = None, brand: str = None) -> dict:
    """
    아이템별 매출구성 트리맵 생성 (YOY 포함)
    드릴다운: 아이템_중분류 → 채널
    
    Args:
        df: 당년 데이터프레임
        prev_df: 전년 데이터프레임 (YOY 계산용, None 가능)
        brand: 브랜드 필터 (None이면 전체)
    """
    if brand:
        print(f"\n[계산] 아이템별 매출구성 트리맵 생성 (브랜드: {brand})...")
        df = df[df['브랜드'] == brand].copy()
        if prev_df is not None:
            brand_code_map = {'MLB': 'M', 'DISCOVERY': 'V', 'SUPRA': 'X', 'MLB_KIDS': 'I', 'SERGIO': 'ST', 'DUVETICA': 'W'}
            brand_code = brand_code_map.get(brand, brand)
            prev_df = prev_df[prev_df['브랜드코드'] == brand_code].copy()
    else:
        print("\n[계산] 아이템별 매출구성 트리맵 생성 (전체)...")
    
    # 전체 합계 계산
    total_tag = df['TAG매출'].sum()
    total_sales = df['실판매액'].sum()
    
    # 전년 전체 합계
    prev_total_sales = 0
    prev_total_tag = 0
    if prev_df is not None:
        # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
        prev_total_tag = prev_df['TAG매출'].sum()
        prev_total_sales = prev_df['실판매액'].sum()
    
    result = {
        'total': {
            'tag': int(total_tag),
            'sales': int(total_sales),
            'discountRate': round(calculate_discount_rate(total_tag, total_sales), 1),
            'prevDiscountRate': round(calculate_discount_rate(prev_total_tag, prev_total_sales), 1) if prev_df is not None and prev_total_tag > 0 else None,
            'yoy': calculate_yoy(total_sales, prev_total_sales) if prev_df is not None else None
        },
        'items': {}
    }
    
    # 1단계: 아이템_중분류별 집계
    item_mid_sum = df.groupby('아이템_중분류', as_index=False).agg({
        'TAG매출': 'sum',
        '실판매액': 'sum'
    })
    
    # 전년 아이템별 집계
    prev_item_sales = {}
    prev_item_tags = {}
    if prev_df is not None:
        # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
        prev_item_sum = prev_df.groupby('아이템_중분류', as_index=False).agg({
            'TAG매출': 'sum',
            '실판매액': 'sum'
        })
        for _, prow in prev_item_sum.iterrows():
            pitem = str(prow['아이템_중분류']).strip()
            prev_item_tags[pitem] = float(prow['TAG매출'])
            prev_item_sales[pitem] = float(prow['실판매액'])
    
    for _, row in item_mid_sum.iterrows():
        item_mid = str(row['아이템_중분류']).strip()
        tag = float(row['TAG매출'])
        sales = float(row['실판매액'])
        
        # YOY 및 전년할인율 계산
        prev_sales = prev_item_sales.get(item_mid, 0)
        prev_tag = prev_item_tags.get(item_mid, 0)
        yoy = calculate_yoy(sales, prev_sales) if prev_df is not None else None
        prev_discount = round(calculate_discount_rate(prev_tag, prev_sales), 1) if prev_df is not None and prev_tag > 0 else None
        
        result['items'][item_mid] = {
            'tag': int(tag),
            'sales': int(sales),
            'share': calculate_share(sales, total_sales),
            'discountRate': round(calculate_discount_rate(tag, sales), 1),
            'prevDiscountRate': prev_discount,  # ★ 전년할인율 추가 ★
            'yoy': yoy,
            'channels': {}
        }
        
        # 2단계: 아이템_중분류 내 채널별 집계
        item_mid_df = df[df['아이템_중분류'] == item_mid]
        channel_sum = item_mid_df.groupby('채널명', as_index=False).agg({
            'TAG매출': 'sum',
            '실판매액': 'sum'
        })
        
        item_mid_total_sales = item_mid_df['실판매액'].sum()
        
        # 전년 아이템-채널 데이터
        prev_item_channel_sales = {}
        prev_item_channel_tags = {}
        if prev_df is not None:
            prev_item_df = prev_df[prev_df['아이템_중분류'] == item_mid]
            if not prev_item_df.empty:
                # load_previous_year_treemap_data에서 이미 컬럼명 통일됨
                prev_ch_sum = prev_item_df.groupby('채널명', as_index=False).agg({
                    'TAG매출': 'sum',
                    '실판매액': 'sum'
                })
                for _, pch_row in prev_ch_sum.iterrows():
                    pch = str(pch_row['채널명']).strip()
                    prev_item_channel_tags[pch] = float(pch_row['TAG매출'])
                    prev_item_channel_sales[pch] = float(pch_row['실판매액'])
        
        for _, ch_row in channel_sum.iterrows():
            channel = str(ch_row['채널명']).strip()
            ch_tag = float(ch_row['TAG매출'])
            ch_sales = float(ch_row['실판매액'])
            
            # YOY 및 전년할인율 계산
            prev_ch_sales = prev_item_channel_sales.get(channel, 0)
            prev_ch_tag = prev_item_channel_tags.get(channel, 0)
            ch_yoy = calculate_yoy(ch_sales, prev_ch_sales) if prev_df is not None else None
            prev_ch_discount = round(calculate_discount_rate(prev_ch_tag, prev_ch_sales), 1) if prev_df is not None and prev_ch_tag > 0 else None
            
            result['items'][item_mid]['channels'][channel] = {
                'tag': int(ch_tag),
                'sales': int(ch_sales),
                'share': calculate_share(ch_sales, item_mid_total_sales),
                'discountRate': round(calculate_discount_rate(ch_tag, ch_sales), 1),
                'prevDiscountRate': prev_ch_discount,  # ★ 전년할인율 추가 ★
                'yoy': ch_yoy
            }
    
    print(f"  아이템_중분류 수: {len(result['items'])}")
    return result

def save_treemap_js(channel_treemap: dict, item_treemap: dict, output_path: str):
    """
    트리맵 데이터를 JS 파일로 저장
    
    Args:
        channel_treemap: 채널별 트리맵 데이터
        item_treemap: 아이템별 트리맵 데이터
        output_path: 저장 경로
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("// 트리맵 데이터 (채널별/아이템별 매출구성)\n")
        f.write(f"// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("(function(){\n")
        f.write("  var channelTreemapData = ")
        f.write(json.dumps(channel_treemap, ensure_ascii=False, indent=2))
        f.write(";\n")
        f.write("  var itemTreemapData = ")
        f.write(json.dumps(item_treemap, ensure_ascii=False, indent=2))
        f.write(";\n")
        f.write("  if (typeof window !== 'undefined') {\n")
        f.write("    window.channelTreemapData = channelTreemapData;\n")
        f.write("    window.itemTreemapData = itemTreemapData;\n")
        f.write("  }\n")
        f.write("  console.log('[Treemap Data] 트리맵 데이터 로드 완료');\n")
        f.write("})();\n")
    
    file_size = os.path.getsize(output_path) / 1024  # KB
    print(f"\n[저장] {output_path}")
    print(f"  파일 크기: {file_size:.2f} KB")

def export_item_treemap_to_csv(item_treemap: dict, date_str: str, prev_df: pd.DataFrame = None):
    """
    아이템별 트리맵 전년 데이터를 CSV로 저장
    
    Args:
        item_treemap: 아이템별 트리맵 데이터
        date_str: YYYYMMDD 형식의 날짜
        prev_df: 전년 데이터프레임 (있을 경우)
    """
    if prev_df is None:
        print("\n[경고] 전년 데이터가 없어 CSV 내보내기를 건너뜁니다.")
        return
    
    print("\n[CSV 내보내기] 아이템별 트리맵 전년 데이터...")
    
    # CSV 데이터 생성
    csv_rows = []
    
    # 브랜드별 데이터 추출
    if 'byBrand' in item_treemap:
        for brand, brand_data in item_treemap['byBrand'].items():
            if 'item' not in brand_data:
                continue
            
            item_data = brand_data['item']
            
            # 아이템별 데이터
            if 'items' in item_data:
                for item_name, item_info in item_data['items'].items():
                    # 아이템 전체 정보
                    csv_rows.append({
                        '브랜드': brand,
                        '구분': '아이템',
                        '아이템명': item_name,
                        '채널명': '전체',
                        'TAG매출': item_info.get('tag', 0),
                        '실판매출': item_info.get('sales', 0),
                        '비중': item_info.get('share', 0),
                        '할인율': item_info.get('discountRate', 0),
                        'YOY': item_info.get('yoy', 0) if item_info.get('yoy') is not None else 0
                    })
                    
                    # 아이템 내 채널별 데이터
                    if 'channels' in item_info:
                        for channel_name, channel_info in item_info['channels'].items():
                            csv_rows.append({
                                '브랜드': brand,
                                '구분': '아이템-채널',
                                '아이템명': item_name,
                                '채널명': channel_name,
                                'TAG매출': channel_info.get('tag', 0),
                                '실판매출': channel_info.get('sales', 0),
                                '비중': channel_info.get('share', 0),
                                '할인율': channel_info.get('discountRate', 0),
                                'YOY': channel_info.get('yoy', 0) if channel_info.get('yoy') is not None else 0
                            })
    
    if not csv_rows:
        print("  [경고] CSV로 저장할 데이터가 없습니다.")
        return
    
    # DataFrame 생성
    df_csv = pd.DataFrame(csv_rows)
    
    # CSV 저장 경로 설정
    year_month = extract_year_month_from_date(date_str)
    csv_dir = os.path.join(ROOT, "raw", year_month, "previous_year")
    os.makedirs(csv_dir, exist_ok=True)
    
    csv_path = os.path.join(csv_dir, f"item_treemap_prev_{date_str}.csv")
    
    # CSV 저장
    df_csv.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    file_size = os.path.getsize(csv_path) / 1024  # KB
    print(f"  ✅ CSV 저장 완료: {csv_path}")
    print(f"  파일 크기: {file_size:.2f} KB")
    print(f"  데이터 행 수: {len(df_csv):,}건")
    
    # 요약 통계
    print("\n  📊 CSV 데이터 요약:")
    print(f"    브랜드 수: {df_csv['브랜드'].nunique()}개")
    print(f"    아이템 수: {df_csv[df_csv['구분']=='아이템']['아이템명'].nunique()}개")
    print(f"    총 실판매출: {df_csv[df_csv['구분']=='아이템']['실판매출'].sum() / 100000000:.1f}억원")

def calculate_date_periods(update_date_str: str):
    """
    트리맵 날짜 기간 계산
    
    당년: 분석월의 1일 ~ 말일 (전체 월 데이터)
    전년: 전년도 동일 월의 1일 ~ 말일
    
    Args:
        update_date_str: YYYYMMDD 형식 (예: 20251215)
    
    Returns:
        dict: 날짜 정보
    """
    from calendar import monthrange
    
    update_date = datetime.strptime(update_date_str, '%Y%m%d')
    
    # ★ 분석월 계산: metadata.json에서 가져오기 ★
    analysis_month_str = update_date_str[:6]  # YYYYMM (기본값)
    
    # metadata.json에서 실제 분석월 확인
    try:
        year_month = extract_year_month_from_date(update_date_str)
        metadata_path = get_current_year_file_path(update_date_str, 'metadata.json')
        if os.path.exists(metadata_path):
            import json
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                if 'analysis_month' in metadata:
                    analysis_month_str = metadata['analysis_month']
                    print(f"  [메타데이터] 분석월: {analysis_month_str}")
    except:
        pass
    
    # 분석월의 년월로 당년 기간 설정
    analysis_year = int(analysis_month_str[:4])
    analysis_month = int(analysis_month_str[4:6])
    
    # 당년 기간: 분석월의 1일 ~ 말일
    cy_start = datetime(analysis_year, analysis_month, 1)
    last_day = monthrange(analysis_year, analysis_month)[1]
    cy_end = datetime(analysis_year, analysis_month, last_day)
    
    # 전년 기간: 전년도 동일 월의 1일 ~ 말일
    prev_year = analysis_year - 1
    prev_month_start = datetime(prev_year, analysis_month, 1)
    prev_last_day = monthrange(prev_year, analysis_month)[1]
    prev_month_end = datetime(prev_year, analysis_month, prev_last_day)
    
    return {
        'cy_start': cy_start.strftime('%Y-%m-%d'),
        'cy_end': cy_end.strftime('%Y-%m-%d'),
        'py_start': prev_month_start.strftime('%Y-%m-%d'),
        'py_end': prev_month_end.strftime('%Y-%m-%d'),
        'update_date': update_date.strftime('%Y-%m-%d')
    }

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="트리맵 데이터 생성 (v2)")
    parser.add_argument("date", help="YYYYMMDD 형식의 날짜 (예: 20251124)")
    parser.add_argument("--output", help="출력 파일 경로 (선택사항)")
    
    args = parser.parse_args()
    date_str = args.date
    
    # 날짜 형식 검증
    if len(date_str) != 8 or not date_str.isdigit():
        print("[ERROR] 날짜 형식이 올바르지 않습니다. YYYYMMDD 형식이어야 합니다.")
        return 1
    
    try:
        print("=" * 60)
        print("트리맵 데이터 생성 (v2 - YOY 포함)")
        print("=" * 60)
        print(f"날짜: {date_str}")
        
        # 1. 당년 데이터 로드
        filepath = find_treemap_preprocessed_file(date_str)
        df = load_treemap_data(filepath)
        
        # 2. 전년 데이터 로드 (전처리 완료된 데이터)
        prev_df = load_previous_year_treemap_data(date_str)
        
        # 3. 채널별 트리맵 생성 (YOY 포함)
        channel_treemap = create_channel_treemap(df, prev_df)
        
        # 4. 아이템별 트리맵 생성 (YOY 포함)
        item_treemap = create_item_treemap(df, prev_df)
        
        # 5. 브랜드별 트리맵 생성 (YOY 포함)
        if '브랜드' in df.columns:
            brands = df['브랜드'].unique()
            brand_treemaps = {}
            for brand in brands:
                brand_str = str(brand).strip()
                brand_treemaps[brand_str] = {
                    'channel': create_channel_treemap(df, prev_df, brand_str),
                    'item': create_item_treemap(df, prev_df, brand_str)
                }
            # 브랜드별 데이터도 포함
            channel_treemap['byBrand'] = brand_treemaps
            item_treemap['byBrand'] = brand_treemaps
        
        # 6. 날짜 기간 계산
        date_periods = calculate_date_periods(date_str)
        print(f"\n[날짜 정보]")
        print(f"  당년: {date_periods['cy_start']} ~ {date_periods['cy_end']}")
        print(f"  전년: {date_periods['py_start']} ~ {date_periods['py_end']}")
        
        # 7. JSON 파일 저장 (JS 파일 제거, JSON만 사용)
        json_dir = os.path.join(OUTPUT_DIR, "data", date_str)
        os.makedirs(json_dir, exist_ok=True)
        
        treemap_json = {
            'metadata': {
                'updateDate': date_periods['update_date'],
                'cyPeriod': {
                    'start': date_periods['cy_start'],
                    'end': date_periods['cy_end']
                },
                'pyPeriod': {
                    'start': date_periods['py_start'],
                    'end': date_periods['py_end']
                }
            },
            'channelTreemapData': channel_treemap,
            'itemTreemapData': item_treemap
        }
        
        json_path = os.path.join(json_dir, "treemap.json")
        import json
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(treemap_json, f, ensure_ascii=False, indent=2)
        print(f"  ✅ JSON 저장: {json_path}")
        
        # 8. ★ 아이템별 트리맵 전년 데이터를 CSV로 내보내기 ★
        export_item_treemap_to_csv(item_treemap, date_str, prev_df)
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())

