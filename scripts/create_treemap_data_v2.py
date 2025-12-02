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
from datetime import datetime
from path_utils import get_current_year_file_path, extract_year_month_from_date

ROOT = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(ROOT, "public")

def find_ke30_shop_item_file(date_str: str) -> str:
    """
    ke30 Shop_item 파일 찾기
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251124")
    
    Returns:
        str: 파일 경로
    """
    year_month = extract_year_month_from_date(date_str)
    filename = f"ke30_{date_str}_{year_month}_Shop_item.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] ke30 Shop_item 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    return filepath

def load_ke30_data(filepath: str) -> pd.DataFrame:
    """
    ke30 Shop_item 데이터 로드
    
    Args:
        filepath: 파일 경로
    
    Returns:
        pd.DataFrame: 데이터프레임
    """
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    # 필요한 컬럼 확인
    required_cols = ['채널명', '아이템_중분류', '아이템_소분류']
    tag_col = None
    sales_col = None
    
    # TAG 컬럼 찾기
    for col in df.columns:
        if '판매금액' in str(col) and 'TAG' in str(col):
            tag_col = col
    
    # 실판매액 컬럼 찾기 (우선순위: 실판매액(V+) > 합계 : 실판매액 > 실판매액(V-))
    # 부가세 포함 실판매액을 사용해야 함
    for col in df.columns:
        col_str = str(col)
        if '실판매액' in col_str or '실판매' in col_str:
            if sales_col is None:
                sales_col = col
            # 부가세 포함 (V+) 우선
            if '(V+)' in col_str or 'v+' in col_str.lower():
                sales_col = col
                break
            # "합계 : 실판매액" (부가세 포함일 가능성)
            if '합계' in col_str and '(V-)' not in col_str:
                sales_col = col
    
    if not tag_col:
        raise ValueError(f"[ERROR] 판매금액(TAG가) 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df.columns)}")
    if not sales_col:
        raise ValueError(f"[ERROR] 실판매액 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df.columns)}")
    
    print(f"  TAG 컬럼: {tag_col}")
    print(f"  실판매액 컬럼: {sales_col}")
    
    # 숫자 변환
    df[tag_col] = pd.to_numeric(df[tag_col], errors="coerce").fillna(0)
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce").fillna(0)
    
    # 컬럼명 정규화
    df = df.rename(columns={tag_col: 'TAG매출', sales_col: '실판매액'})
    
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

def create_channel_treemap(df: pd.DataFrame, brand: str = None) -> dict:
    """
    채널별 매출구성 트리맵 생성
    드릴다운: 채널 → 아이템_중분류 → 아이템_소분류
    
    Args:
        df: 데이터프레임
        brand: 브랜드 필터 (None이면 전체)
    """
    if brand:
        print(f"\n[계산] 채널별 매출구성 트리맵 생성 (브랜드: {brand})...")
        df = df[df['브랜드'] == brand].copy()
    else:
        print("\n[계산] 채널별 매출구성 트리맵 생성 (전체)...")
    
    # 전체 합계 계산
    total_tag = df['TAG매출'].sum()
    total_sales = df['실판매액'].sum()
    
    result = {
        'total': {
            'tag': int(total_tag),
            'sales': int(total_sales),
            'discountRate': round(calculate_discount_rate(total_tag, total_sales), 1)
        },
        'channels': {}
    }
    
    # 1단계: 채널별 집계
    channel_sum = df.groupby('채널명', as_index=False).agg({
        'TAG매출': 'sum',
        '실판매액': 'sum'
    })
    
    for _, row in channel_sum.iterrows():
        channel = str(row['채널명']).strip()
        tag = float(row['TAG매출'])
        sales = float(row['실판매액'])
        
        # 채널별 정보 저장
        result['channels'][channel] = {
            'tag': int(tag),
            'sales': int(sales),
            'share': calculate_share(sales, total_sales),
            'discountRate': round(calculate_discount_rate(tag, sales), 1),
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
        
        for _, item_row in item_mid_sum.iterrows():
            item_mid = str(item_row['아이템_중분류']).strip()
            item_tag = float(item_row['TAG매출'])
            item_sales = float(item_row['실판매액'])
            
            # 아이템_중분류별 정보 저장
            result['channels'][channel]['itemCategories'][item_mid] = {
                'tag': int(item_tag),
                'sales': int(item_sales),
                'share': calculate_share(item_sales, channel_total_sales),  # 채널 내 비중
                'discountRate': round(calculate_discount_rate(item_tag, item_sales), 1),
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
            
            for _, sub_row in item_sub_sum.iterrows():
                item_sub = str(sub_row['아이템_소분류']).strip()
                sub_tag = float(sub_row['TAG매출'])
                sub_sales = float(sub_row['실판매액'])
                
                # 아이템_소분류별 정보 저장
                result['channels'][channel]['itemCategories'][item_mid]['subCategories'][item_sub] = {
                    'tag': int(sub_tag),
                    'sales': int(sub_sales),
                    'share': calculate_share(sub_sales, item_mid_total_sales),  # 중분류 내 비중
                    'discountRate': round(calculate_discount_rate(sub_tag, sub_sales), 1)
                }
    
    print(f"  채널 수: {len(result['channels'])}")
    return result

def create_item_treemap(df: pd.DataFrame, brand: str = None) -> dict:
    """
    아이템별 매출구성 트리맵 생성
    드릴다운: 아이템_중분류 → 채널
    
    Args:
        df: 데이터프레임
        brand: 브랜드 필터 (None이면 전체)
    """
    if brand:
        print(f"\n[계산] 아이템별 매출구성 트리맵 생성 (브랜드: {brand})...")
        df = df[df['브랜드'] == brand].copy()
    else:
        print("\n[계산] 아이템별 매출구성 트리맵 생성 (전체)...")
    
    # 전체 합계 계산
    total_tag = df['TAG매출'].sum()
    total_sales = df['실판매액'].sum()
    
    result = {
        'total': {
            'tag': int(total_tag),
            'sales': int(total_sales),
            'discountRate': round(calculate_discount_rate(total_tag, total_sales), 1)
        },
        'items': {}
    }
    
    # 1단계: 아이템_중분류별 집계
    item_mid_sum = df.groupby('아이템_중분류', as_index=False).agg({
        'TAG매출': 'sum',
        '실판매액': 'sum'
    })
    
    for _, row in item_mid_sum.iterrows():
        item_mid = str(row['아이템_중분류']).strip()
        tag = float(row['TAG매출'])
        sales = float(row['실판매액'])
        
        result['items'][item_mid] = {
            'tag': int(tag),
            'sales': int(sales),
            'share': calculate_share(sales, total_sales),
            'discountRate': round(calculate_discount_rate(tag, sales), 1),
            'channels': {}
        }
        
        # 2단계: 아이템_중분류 내 채널별 집계
        item_mid_df = df[df['아이템_중분류'] == item_mid]
        channel_sum = item_mid_df.groupby('채널명', as_index=False).agg({
            'TAG매출': 'sum',
            '실판매액': 'sum'
        })
        
        item_mid_total_sales = item_mid_df['실판매액'].sum()
        
        for _, ch_row in channel_sum.iterrows():
            channel = str(ch_row['채널명']).strip()
            ch_tag = float(ch_row['TAG매출'])
            ch_sales = float(ch_row['실판매액'])
            
            result['items'][item_mid]['channels'][channel] = {
                'tag': int(ch_tag),
                'sales': int(ch_sales),
                'share': calculate_share(ch_sales, item_mid_total_sales),
                'discountRate': round(calculate_discount_rate(ch_tag, ch_sales), 1)
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
        print("트리맵 데이터 생성 (v2)")
        print("=" * 60)
        print(f"날짜: {date_str}")
        
        # 1. 데이터 로드
        filepath = find_ke30_shop_item_file(date_str)
        df = load_ke30_data(filepath)
        
        # 2. 채널별 트리맵 생성 (전체)
        channel_treemap = create_channel_treemap(df)
        
        # 3. 아이템별 트리맵 생성 (전체)
        item_treemap = create_item_treemap(df)
        
        # 4. 브랜드별 트리맵 생성 (선택적)
        if '브랜드' in df.columns:
            brands = df['브랜드'].unique()
            brand_treemaps = {}
            for brand in brands:
                brand_str = str(brand).strip()
                brand_treemaps[brand_str] = {
                    'channel': create_channel_treemap(df, brand_str),
                    'item': create_item_treemap(df, brand_str)
                }
            # 브랜드별 데이터도 포함
            channel_treemap['byBrand'] = brand_treemaps
            item_treemap['byBrand'] = brand_treemaps
        
        # 4. JSON 파일 저장 (JS 파일 제거, JSON만 사용)
        json_dir = os.path.join(OUTPUT_DIR, "data", date_str)
        os.makedirs(json_dir, exist_ok=True)
        
        treemap_json = {
            'channelTreemapData': channel_treemap,
            'itemTreemapData': item_treemap
        }
        
        json_path = os.path.join(json_dir, "treemap.json")
        import json
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(treemap_json, f, ensure_ascii=False, indent=2)
        print(f"  ✅ JSON 저장: {json_path}")
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())

