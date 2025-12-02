"""
트리맵 데이터 정제 스크립트
=================================

작업 흐름:
1. 전처리 완료된 CSV 파일 읽기
2. 브랜드, 채널명, 아이템분류별 집계
3. JSON 형식으로 변환 (Dashboard.html 형식에 맞게)
4. public/treemap_data.js 파일 생성

작성일: 2025-11-14
"""

import pandas as pd
import json
import os
from datetime import datetime
import sys

# ================================
# 설정 (Configuration)
# ================================

# 경로 설정
INPUT_DIR = r"C:\Users\AD0283\Desktop\AIproject\Project_Forcast\raw"
OUTPUT_DIR = r"C:\Users\AD0283\Desktop\AIproject\Project_Forcast\public"

# 입력 파일 패턴 (전처리완료 파일)
INPUT_FILE_PATTERN = "_전처리완료.csv"

# 출력 파일명
OUTPUT_JS_FILE = "treemap_data.js"

# ================================
# 브랜드 코드 매핑
# ================================

BRAND_CODE_MAP = {
    'M': 'M',           # MLB
    'MLB': 'M',
    'I': 'I',           # MLB KIDS
    'MLB KIDS': 'I',
    'MLB_KIDS': 'I',
    'X': 'X',           # DISCOVERY
    'DISCOVERY': 'X',
    'V': 'V',           # DUVETICA
    'DUVETICA': 'V',
    'ST': 'ST',         # SERGIO
    'SERGIO': 'ST',
    'W': 'W',           # SUPRA
    'SUPRA': 'W'
}

# 브랜드 이름 매핑
BRAND_NAME_MAP = {
    'M': 'MLB',
    'I': 'MLB KIDS',
    'X': 'DISCOVERY',
    'V': 'DUVETICA',
    'ST': 'SERGIO',
    'W': 'SUPRA'
}

# ================================
# 함수 정의
# ================================

def find_latest_processed_file():
    """
    raw 폴더에서 최신 전처리완료 CSV 파일 찾기
    
    Returns:
        str: 파일 전체 경로
    """
    if not os.path.exists(INPUT_DIR):
        raise FileNotFoundError(f"❌ 폴더가 없습니다: {INPUT_DIR}")
    
    # _전처리완료.csv로 끝나는 모든 파일 찾기
    files = []
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith(INPUT_FILE_PATTERN):
            filepath = os.path.join(INPUT_DIR, filename)
            mtime = os.path.getmtime(filepath)
            files.append((filepath, mtime, filename))
    
    if not files:
        raise FileNotFoundError(f"❌ {INPUT_DIR} 폴더에 전처리완료 파일이 없습니다!")
    
    # 최신 파일 선택 (수정 시간 기준)
    latest_file = sorted(files, key=lambda x: x[1], reverse=True)[0]
    print(f"✅ 최신 파일 발견: {latest_file[2]}")
    
    return latest_file[0]


def load_and_aggregate_data(csv_path):
    """
    CSV 파일을 읽고 브랜드, 채널명, 아이템분류별 집계
    
    Args:
        csv_path (str): 입력 CSV 파일 경로
    
    Returns:
        pd.DataFrame: 집계된 데이터
    """
    print(f"\n📥 CSV 파일 읽는 중: {csv_path}")
    
    # CSV 읽기
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"   원본 데이터: {len(df)}행 × {len(df.columns)}열")
    
    # 필요한 컬럼 확인
    required_cols = ['브랜드', '유통채널', 'PH01-3']
    value_cols_candidates = [
        '합계 : 판매금액(TAG가) Actual',
        '판매금액(TAG가) Actual',
        '판매금액(TAG가)',
        '합계 : 실판매액 Actual',
        '실판매액 Actual',
        '실판매액'
    ]
    
    # 컬럼명 유연한 매칭
    column_map = {}
    for req_col in required_cols:
        found = False
        for col in df.columns:
            if req_col in str(col) or str(col) in req_col:
                column_map[col] = req_col
                found = True
                break
        if not found:
            print(f"⚠️  '{req_col}' 컬럼을 찾을 수 없습니다")
    
    # 값 컬럼 찾기
    tag_col = None
    sales_col = None
    
    for candidate in value_cols_candidates[:3]:  # TAG가 먼저
        for col in df.columns:
            if candidate in str(col):
                tag_col = col
                break
        if tag_col:
            break
    
    for candidate in value_cols_candidates[3:]:  # 실판매액
        for col in df.columns:
            if candidate in str(col):
                sales_col = col
                break
        if sales_col:
            break
    
    if not tag_col:
        print(f"⚠️  '판매금액(TAG가)' 컬럼을 찾을 수 없습니다")
        print(f"   사용 가능한 컬럼: {list(df.columns)}")
        raise ValueError("필수 컬럼 누락: 판매금액(TAG가)")
    
    if not sales_col:
        print(f"⚠️  '실판매액' 컬럼을 찾을 수 없습니다")
        sales_col = tag_col  # 폴백: TAG가 사용
    
    print(f"   TAG가 컬럼: {tag_col}")
    print(f"   실판매액 컬럼: {sales_col}")
    
    # 컬럼명 표준화
    df = df.rename(columns=column_map)
    
    # 집계 컬럼 설정
    group_cols = []
    if '브랜드' in df.columns:
        group_cols.append('브랜드')
    if '유통채널' in df.columns:
        group_cols.append('유통채널')
    if 'PH01-3' in df.columns:
        group_cols.append('PH01-3')
    
    if len(group_cols) == 0:
        raise ValueError("❌ 집계할 컬럼이 없습니다!")
    
    print(f"   집계 기준: {group_cols}")
    
    # 숫자 타입 변환
    df[tag_col] = pd.to_numeric(df[tag_col], errors='coerce').fillna(0)
    df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
    
    # 집계
    agg_dict = {
        tag_col: 'sum',
        sales_col: 'sum'
    }
    
    df_agg = df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    # 컬럼명 단순화
    df_agg = df_agg.rename(columns={
        tag_col: 'TAG가',
        sales_col: '실판매액'
    })
    
    print(f"✅ 집계 완료: {len(df_agg)}행")
    print(f"   총 TAG가: {df_agg['TAG가'].sum():,.0f}")
    print(f"   총 실판매액: {df_agg['실판매액'].sum():,.0f}")
    
    return df_agg


def convert_to_treemap_json(df_agg):
    """
    집계된 데이터를 트리맵 JSON 형식으로 변환
    
    Args:
        df_agg (pd.DataFrame): 집계된 데이터
    
    Returns:
        dict: 트리맵 데이터 (브랜드 코드별)
    """
    print(f"\n🔄 트리맵 JSON 변환 중...")
    
    treemap_data = {}
    
    # 컬럼명 확인
    has_brand = '브랜드' in df_agg.columns
    has_channel = '유통채널' in df_agg.columns
    has_item = 'PH01-3' in df_agg.columns
    
    if not (has_brand and has_channel and has_item):
        print(f"⚠️  필수 컬럼 누락")
        print(f"   브랜드: {has_brand}, 유통채널: {has_channel}, PH01-3: {has_item}")
        return treemap_data
    
    # 브랜드별로 그룹화
    for brand in df_agg['브랜드'].unique():
        brand_df = df_agg[df_agg['브랜드'] == brand].copy()
        
        # 브랜드 코드 변환
        brand_code = BRAND_CODE_MAP.get(str(brand).strip().upper(), str(brand).strip())
        
        if brand_code not in treemap_data:
            treemap_data[brand_code] = {}
        
        # 채널별로 그룹화
        for channel in brand_df['유통채널'].unique():
            channel_df = brand_df[brand_df['유통채널'] == channel]
            
            channel_name = str(channel).strip()
            if channel_name not in treemap_data[brand_code]:
                treemap_data[brand_code][channel_name] = {}
            
            # 아이템별로 그룹화
            for item in channel_df['PH01-3'].unique():
                item_df = channel_df[channel_df['PH01-3'] == item]
                
                # 실판매액 합계 (원 단위)
                item_value = int(item_df['실판매액'].sum())
                
                item_name = str(item).strip()
                
                if item_value > 0:  # 양수인 경우만 추가
                    treemap_data[brand_code][channel_name][item_name] = item_value
        
        print(f"   ✅ {brand} ({brand_code}): {len(treemap_data[brand_code])}개 채널")
    
    return treemap_data


def generate_js_file(treemap_data, output_path):
    """
    JavaScript 파일 생성
    
    Args:
        treemap_data (dict): 트리맵 데이터
        output_path (str): 출력 파일 경로
    """
    print(f"\n📝 JavaScript 파일 생성 중...")
    
    # JSON 문자열 생성 (들여쓰기 포함)
    json_str = json.dumps(treemap_data, indent=2, ensure_ascii=False)
    
    # JavaScript 파일 내용 생성
    js_content = f"""// 트리맵 데이터 (자동 생성)
// 생성 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 형식: {{ '브랜드코드': {{ '채널명': {{ '아이템명': 실판매액(원) }} }} }}

window.channelItemSalesData = {json_str};

// 브랜드 코드 매핑
window.brandCodeMap = {{
  'MLB': 'M',
  'MLB KIDS': 'I',
  'MLB_KIDS': 'I',
  'DISCOVERY': 'X',
  'DUVETICA': 'V',
  'SERGIO': 'ST',
  'SUPRA': 'W',
  'M': 'M',
  'I': 'I',
  'X': 'X',
  'V': 'V',
  'ST': 'ST',
  'W': 'W'
}};

console.log('✅ 트리맵 데이터 로드 완료:', Object.keys(window.channelItemSalesData));
"""
    
    # 파일 저장
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    print(f"✅ JavaScript 파일 저장: {output_path}")


def generate_summary_report(treemap_data):
    """
    요약 리포트 생성
    
    Args:
        treemap_data (dict): 트리맵 데이터
    """
    print(f"\n" + "=" * 60)
    print("📊 트리맵 데이터 요약")
    print("=" * 60)
    
    for brand_code, channels in treemap_data.items():
        brand_name = BRAND_NAME_MAP.get(brand_code, brand_code)
        total_sales = 0
        total_items = 0
        
        for channel_name, items in channels.items():
            channel_sales = sum(items.values())
            total_sales += channel_sales
            total_items += len(items)
        
        print(f"\n🏷️  {brand_name} ({brand_code})")
        print(f"   채널 수: {len(channels)}개")
        print(f"   아이템 수: {total_items}개")
        print(f"   총 매출: {total_sales:,.0f}원 ({total_sales/100000000:.1f}억원)")
        
        # 채널별 상세
        for channel_name, items in sorted(channels.items(), key=lambda x: sum(x[1].values()), reverse=True)[:5]:
            channel_sales = sum(items.values())
            print(f"     - {channel_name}: {channel_sales/100000000:.1f}억원 ({len(items)}개 아이템)")


def main():
    """
    메인 실행 함수
    """
    print("=" * 60)
    print("트리맵 데이터 정제 스크립트")
    print("=" * 60)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # ----------------
        # Step 1: 최신 전처리완료 파일 찾기
        # ----------------
        csv_path = find_latest_processed_file()
        
        # ----------------
        # Step 2: 데이터 로드 및 집계
        # ----------------
        df_agg = load_and_aggregate_data(csv_path)
        
        # ----------------
        # Step 3: 트리맵 JSON 변환
        # ----------------
        treemap_data = convert_to_treemap_json(df_agg)
        
        # ----------------
        # Step 4: JavaScript 파일 생성
        # ----------------
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_JS_FILE)
        generate_js_file(treemap_data, output_path)
        
        # ----------------
        # Step 5: 요약 리포트
        # ----------------
        generate_summary_report(treemap_data)
        
        print(f"\n🎉 완료!")
        print(f"   생성된 파일: {output_path}")
        
        return treemap_data
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ================================
# 직접 실행 시
# ================================

if __name__ == "__main__":
    result = main()
    
    print(f"\n📋 브랜드별 데이터 개수:")
    for brand_code, channels in result.items():
        brand_name = BRAND_NAME_MAP.get(brand_code, brand_code)
        channel_count = len(channels)
        item_count = sum(len(items) for items in channels.values())
        print(f"   {brand_name} ({brand_code}): {channel_count}개 채널, {item_count}개 아이템")






