"""
트리맵 데이터 전처리
====================

1. 채널명 매핑
2. 아이템 마스터 매핑
3. 아이템_중분류 생성 (시즌 로직 적용)

작성일: 2025-01
"""

import os
import pandas as pd
from datetime import datetime
from path_utils import get_current_year_file_path, extract_year_month_from_date

ROOT = os.path.dirname(os.path.dirname(__file__))

def load_channel_master() -> pd.DataFrame:
    """채널 마스터 로드"""
    master_path = os.path.join(ROOT, "master", "channel_master.csv")
    if not os.path.exists(master_path):
        raise FileNotFoundError(f"채널 마스터를 찾을 수 없습니다: {master_path}")
    
    df = pd.read_csv(master_path, encoding='utf-8-sig')
    print(f"[로드] 채널 마스터: {len(df)}개")
    return df

def load_item_master() -> pd.DataFrame:
    """아이템 마스터 로드"""
    master_path = os.path.join(ROOT, "master", "item_master.csv")
    if not os.path.exists(master_path):
        raise FileNotFoundError(f"아이템 마스터를 찾을 수 없습니다: {master_path}")
    
    df = pd.read_csv(master_path, encoding='utf-8-sig')
    print(f"[로드] 아이템 마스터: {len(df)}개")
    return df

def map_channel_name(row, channel_master):
    """
    채널명 매핑
    - 고객코드가 채널마스터의 SAP_CD에 있으면 RF
    - 없으면 채널코드로 채널명 매핑
    """
    cust_cd = str(row['고객코드']).strip()
    chnl_cd = str(row['채널코드']).strip()
    
    # RF 체크 (고객코드가 SAP_CD에 있는지)
    if cust_cd in channel_master['SAP_CD'].astype(str).values:
        return 'RF'
    
    # 채널코드로 매핑
    channel_row = channel_master[channel_master['채널코드'].astype(str) == chnl_cd]
    if not channel_row.empty:
        return channel_row.iloc[0]['채널명']
    
    return f"채널{chnl_cd}"  # 매핑 실패 시 기본값

def get_current_season(ref_date: datetime) -> tuple:
    """
    현재 시즌 반환
    
    SS시즌 = 3월 ~ 8월
    FW시즌 = 9월 ~ 내년 2월
    
    Returns:
        tuple: (년도, 시즌타입 'S' or 'F')
    """
    month = ref_date.month
    year = ref_date.year
    
    if 3 <= month <= 8:
        # SS 시즌
        return (year % 100, 'S')  # 25, 'S'
    else:
        # FW 시즌 (9월~12월은 당해, 1~2월은 전년)
        if month >= 9:
            return (year % 100, 'F')  # 25, 'F'
        else:
            return ((year - 1) % 100, 'F')  # 1~2월은 전년도 FW

def classify_season_category(season_code: str, ref_date: datetime) -> str:
    """
    시즌 분류: 당시즌의류, 과시즌의류, 차시즌의류
    
    Args:
        season_code: 시즌코드 (예: '25F', '25S', '25N')
        ref_date: 기준일
    
    Returns:
        str: 당시즌의류, 과시즌의류, 차시즌의류
    """
    if pd.isna(season_code) or season_code == '':
        return '당시즌의류'
    
    season_code = str(season_code).strip()
    
    # N 포함 시즌 (년도만 카운팅)
    if 'N' in season_code.upper():
        try:
            season_year = int(season_code[:2])
            current_year, _ = get_current_season(ref_date)
            
            if season_year == current_year:
                return '당시즌의류'
            elif season_year < current_year:
                return '과시즌의류'
            else:
                return '차시즌의류'
        except:
            return '당시즌의류'
    
    # 일반 시즌 (25F, 25S 등)
    try:
        if len(season_code) < 2:
            return '당시즌의류'
        
        season_year = int(season_code[:2])
        season_type = season_code[2].upper() if len(season_code) > 2 else 'S'
        
        current_year, current_type = get_current_season(ref_date)
        
        # 현재 시즌과 비교
        if season_year < current_year:
            return '과시즌의류'
        elif season_year > current_year:
            return '차시즌의류'
        else:
            # 같은 년도
            if season_type == current_type:
                return '당시즌의류'
            elif season_type == 'S' and current_type == 'F':
                return '과시즌의류'
            else:
                return '차시즌의류'
    except:
        return '당시즌의류'

def create_item_mid_category(row, ref_date: datetime) -> str:
    """
    아이템_중분류 생성
    
    - prdt_hrrc_cd1이 의류: 시즌 로직 적용
    - prdt_hrrc_cd1이 ACC: PRDT_HRRC2_NM 반환
    """
    prdt_hrrc_cd1 = str(row.get('prdt_hrrc_cd1', '')).strip()
    
    # ACC인 경우
    if prdt_hrrc_cd1 == 'E0200':  # ACC 코드
        return row.get('PRDT_HRRC2_NM', 'Acc_etc')
    
    # 의류인 경우 (E0300)
    if prdt_hrrc_cd1 == 'E0300':
        season_code = row.get('시즌', '')
        return classify_season_category(season_code, ref_date)
    
    # 기타
    return row.get('PRDT_HRRC2_NM', '기타')

def preprocess_treemap_data(input_path: str, output_path: str, ref_date: datetime):
    """
    트리맵 데이터 전처리
    
    Args:
        input_path: 입력 파일 경로
        output_path: 출력 파일 경로
        ref_date: 기준일 (시즌 분류용)
    """
    print(f"\n[전처리] 트리맵 데이터")
    print(f"  입력: {input_path}")
    print(f"  기준일: {ref_date.strftime('%Y-%m-%d')}")
    
    # 1. 원본 데이터 로드
    df = pd.read_csv(input_path, encoding='utf-8-sig')
    print(f"  원본 데이터: {len(df)}행")
    
    # 2. 마스터 로드
    channel_master = load_channel_master()
    item_master = load_item_master()
    
    # 3. 채널명 매핑
    print("\n[매핑] 채널명...")
    df['채널명'] = df.apply(lambda row: map_channel_name(row, channel_master), axis=1)
    
    # 4. 아이템 마스터 매핑
    print("[매핑] 아이템 정보...")
    
    # prdt_hrrc_cd2, prdt_hrrc_cd3를 키로 조인
    df = df.merge(
        item_master[['PH01-2', 'PH01-3', 'PRDT_HRRC2_NM', 'PRDT_HRRC3_NM']],
        left_on=['prdt_hrrc_cd2', 'prdt_hrrc_cd3'],
        right_on=['PH01-2', 'PH01-3'],
        how='left'
    )
    
    # 5. 아이템_중분류 생성
    print("[생성] 아이템_중분류...")
    df['아이템_중분류'] = df.apply(lambda row: create_item_mid_category(row, ref_date), axis=1)
    
    # 6. 브랜드명 매핑
    brand_map = {
        'M': 'MLB',
        'I': 'MLB_KIDS',
        'X': 'DISCOVERY',
        'V': 'DUVETICA',
        'ST': 'SERGIO',
        'W': 'SUPRA'
    }
    df['브랜드'] = df['브랜드코드'].map(brand_map).fillna(df['브랜드코드'])
    
    # 7. 컬럼 순서 정리
    output_columns = [
        '브랜드코드', '브랜드', '시즌', '채널코드', '채널명', '고객코드',
        'prdt_hrrc_cd1', 'prdt_hrrc_cd2', 'PRDT_HRRC2_NM',
        'prdt_hrrc_cd3', 'PRDT_HRRC3_NM', '아이템코드',
        '아이템_중분류', 'TAG매출', '실판매출'
    ]
    
    df = df[output_columns]
    
    # 8. 저장
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n[저장] {output_path}")
    print(f"  처리 완료: {len(df)}행")
    print(f"  브랜드: {df['브랜드'].nunique()}개")
    print(f"  채널: {df['채널명'].nunique()}개")
    print(f"  아이템_중분류: {df['아이템_중분류'].nunique()}개")
    
    return df

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="트리맵 데이터 전처리")
    parser.add_argument("input", help="입력 파일 경로")
    parser.add_argument("--output", help="출력 파일 경로")
    parser.add_argument("--date", help="기준일 (YYYYMMDD)", required=True)
    
    args = parser.parse_args()
    
    # 기준일 파싱
    ref_date = datetime.strptime(args.date, '%Y%m%d')
    
    # 출력 경로 설정
    if args.output:
        output_path = args.output
    else:
        date_str = args.date
        year_month = extract_year_month_from_date(date_str)
        filename = f"treemap_preprocessed_{date_str}.csv"
        output_path = get_current_year_file_path(date_str, filename)
    
    # 전처리
    preprocess_treemap_data(args.input, output_path, ref_date)
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())



