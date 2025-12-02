"""
계획 파일에서 직접비율 추출 및 ke30 파일에 적용하는 스크립트
===============================================================

작업 흐름:
1. 계획 파일에서 직접비율 추출 (RF 파일 제외)
2. 브랜드별/채널별 직접비율 계산
3. 행열 전환
4. 기타 채널 추가
5. 채널번호 추가
6. ke30 전처리 완료 파일에 직접비 계산 적용

작성일: 2025-11-24
"""

import pandas as pd
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 경로 설정
MASTER_DIR = project_root / "Master"
CHANNEL_MASTER_PATH = MASTER_DIR / "채널마스터.csv"
ROYALTY_RATE_MASTER_PATH = MASTER_DIR / "로열티율.csv"

# 제외할 직접비 항목
EXCLUDED_COSTS = [
    # 지급수수료_로열티: 로열티율 마스터로 별도 처리
    # 지급임차료_매장(고정), 감가상각비_임차시설물: 계획 파일 금액으로 처리
]

# 계획 파일 채널명 -> ke30 채널명 매핑
CHANNEL_NAME_MAPPING = {
    '(브랜드) 백화점': '백화점',
    '(브랜드) 면세점': '면세점',
    '직영점(가두)': '직영점(가두)',
    '온라인(자사)': '자사몰',
    '온라인(제휴)': '제휴몰',
    '대리점': '대리점',
    '아울렛(직영)': '아울렛',
    '사입': '사입',
    '수출': '수출',
    '직영(가두)2': '직영몰'
}

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


def load_channel_master() -> Dict[str, int]:
    """
    채널 마스터 파일 로드하여 채널명 -> 채널번호 매핑 반환
    
    Returns:
        Dict[str, int]: 채널명 -> 채널번호 매핑
    """
    if not CHANNEL_MASTER_PATH.exists():
        raise FileNotFoundError(f"[ERROR] 채널 마스터 파일이 없습니다: {CHANNEL_MASTER_PATH}")
    
    df = pd.read_csv(CHANNEL_MASTER_PATH, encoding="utf-8-sig")
    
    # 채널sap 컬럼 찾기
    channel_sap_col = None
    channel_num_col = None
    
    for col in df.columns:
        col_str = str(col).strip()
        if channel_sap_col is None and ("채널sap" in col_str.lower() or "채널 sap" in col_str.lower()):
            channel_sap_col = col
        if channel_num_col is None and ("채널번호" in col_str or "채널 번호" in col_str):
            channel_num_col = col
    
    if not channel_sap_col or not channel_num_col:
        # 기본값: 첫 번째와 세 번째 컬럼
        if len(df.columns) >= 3:
            channel_num_col = df.columns[0]
            channel_sap_col = df.columns[2]
        else:
            raise ValueError(f"[ERROR] 채널 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
    
    mapping = {}
    for _, row in df[[channel_num_col, channel_sap_col]].dropna().iterrows():
        channel_name = str(row[channel_sap_col]).strip()
        channel_num = row[channel_num_col]
        
        # 채널번호를 정수로 변환
        try:
            if pd.notna(channel_num):
                if isinstance(channel_num, str) and channel_num.strip() == '':
                    continue
                channel_num_int = int(float(channel_num)) if channel_num != 'RF' else 0
            else:
                continue
        except (ValueError, TypeError):
            continue
        
        if channel_name and channel_name != '':
            mapping[channel_name] = channel_num_int
    
    print(f"[OK] 채널 마스터 로드: {len(mapping)}개 매핑")
    return mapping


def load_royalty_rate_master() -> Dict[tuple, Dict]:
    """
    로열티율 마스터 파일 로드
    
    Returns:
        Dict: {(브랜드, 유통채널): {'rate': 비율, 'base': 기준매출}} 형태의 딕셔너리
    """
    if not ROYALTY_RATE_MASTER_PATH.exists():
        raise FileNotFoundError(f"[ERROR] 로열티율 마스터 파일이 없습니다: {ROYALTY_RATE_MASTER_PATH}")
    
    df = pd.read_csv(ROYALTY_RATE_MASTER_PATH, encoding="utf-8-sig")
    
    # 빈 행 제거
    df = df.dropna(subset=['브랜드', '유통채널'])
    
    royalty_dict = {}
    for _, row in df.iterrows():
        brand = str(row['브랜드']).strip()
        channel_num = row['유통채널']
        rate_str = str(row['%/원']).strip().replace('%', '')
        base = str(row['기준매출']).strip()
        
        if pd.isna(channel_num) or brand == '' or rate_str == '' or rate_str == 'nan':
            continue
        
        try:
            channel_num_int = int(float(channel_num))
            rate = float(rate_str) / 100  # 퍼센트를 소수로 변환
        except (ValueError, TypeError):
            continue
        
        key = (brand, channel_num_int)
        royalty_dict[key] = {
            'rate': rate,
            'base': base  # '실판가(V-)' 또는 '출고가(V-)'
        }
    
    print(f"[OK] 로열티율 마스터 로드: {len(royalty_dict)}개 매핑")
    return royalty_dict


def is_rf_file(filename: str) -> bool:
    """RF 파일인지 확인"""
    return "RF" in filename.upper() and "_RF" in filename.upper()


def read_plan_file(filepath: str):
    """
    계획 파일 읽기
    
    Args:
        filepath: 계획 파일 경로
    
    Returns:
        tuple: (데이터프레임, 채널목록, 브랜드코드)
    """
    df = pd.read_csv(filepath, encoding="utf-8-sig", header=None)
    
    # 첫 3행이 헤더 정보
    if len(df) < 3:
        raise ValueError(f"[ERROR] 파일 형식이 올바르지 않습니다: {filepath}")
    
    # 브랜드 코드 추출 (1행)
    brand_row = df.iloc[0]
    brand_code = None
    for val in brand_row.values[1:]:
        if pd.notna(val) and str(val).strip() in ['M', 'I', 'ST', 'V', 'W', 'X']:
            brand_code = str(val).strip()
            break
    
    if not brand_code:
        # 파일명에서 추출 시도
        filename = os.path.basename(filepath)
        if '_M.csv' in filename and '_M_RF' not in filename:
            brand_code = 'M'
        elif '_I' in filename:
            brand_code = 'I'
        elif '_ST' in filename:
            brand_code = 'ST'
        elif '_V' in filename:
            brand_code = 'V'
        elif '_W' in filename:
            brand_code = 'W'
        elif '_X' in filename:
            brand_code = 'X'
        else:
            brand_code = "UNKNOWN"
    
    # 채널명 추출 (3행)
    channel_row = df.iloc[2]
    channels = [str(val).strip() if pd.notna(val) else "" for val in channel_row.values[1:]]
    
    # 데이터 부분 (4행부터)
    data_df = df.iloc[3:].copy()
    data_df.columns = ["구분"] + channels
    
    # 구분 컬럼 정리
    data_df["구분"] = data_df["구분"].astype(str).str.strip()
    data_df = data_df[data_df["구분"].notna() & (data_df["구분"] != "")]
    data_df = data_df.reset_index(drop=True)
    
    return data_df, channels, brand_code


def extract_plan_amounts(plan_dir: str, channel_master: Dict[str, int]) -> pd.DataFrame:
    """
    계획 파일에서 지급임차료_매장(고정), 감가상각비_임차시설물 금액 추출
    
    Args:
        plan_dir: 계획 파일 디렉토리
        channel_master: 채널 마스터 매핑
    
    Returns:
        pd.DataFrame: 브랜드별/유통채널별 금액 데이터프레임
    """
    print("\n" + "=" * 60)
    print("계획 파일에서 금액 추출 시작 (지급임차료_매장(고정), 감가상각비_임차시설물)")
    print("=" * 60)
    
    if not os.path.exists(plan_dir):
        raise FileNotFoundError(f"[ERROR] 계획 데이터 폴더가 없습니다: {plan_dir}")
    
    # 계획 파일 찾기 (RF 파일 제외)
    plan_files = []
    for filename in os.listdir(plan_dir):
        if filename.endswith(".csv") and not is_rf_file(filename):
            filepath = os.path.join(plan_dir, filename)
            plan_files.append(filepath)
    
    if not plan_files:
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {plan_dir}")
    
    all_amounts = []
    cost_items = ['지급임차료_매장(고정)', '감가상각비_임차시설물']
    
    for filepath in plan_files:
        filename = os.path.basename(filepath)
        
        try:
            df, channels, brand_code = read_plan_file(filepath)
            
            for channel in channels:
                if not channel or channel == "Unassigned" or channel == "수출" or channel.strip() == "":
                    continue
                
                if channel not in df.columns:
                    continue
                
                # 채널번호 변환
                channel_num = channel_master.get(channel, None)
                if channel_num is None:
                    for key, val in channel_master.items():
                        if channel in key or key in channel:
                            channel_num = val
                            break
                
                if channel_num is None:
                    continue
                
                # 각 직접비 항목별로 금액 추출
                for cost_item in cost_items:
                    cost_row_idx = None
                    for idx, row in df.iterrows():
                        if str(row["구분"]).strip() == cost_item:
                            cost_row_idx = idx
                            break
                    
                    if cost_row_idx is None:
                        continue
                    
                    cost_val = df.at[cost_row_idx, channel]
                    if pd.isna(cost_val) or cost_val == "":
                        continue
                    
                    try:
                        cost_amount = float(cost_val)
                        # 계획 파일의 금액은 *1000하여 가져옴 (1275 -> 1,275,000)
                        cost_amount = cost_amount * 1000
                    except (ValueError, TypeError):
                        continue
                    
                    all_amounts.append({
                        '브랜드': brand_code,
                        '유통채널': channel_num,
                        '직접비항목': cost_item,
                        '금액': cost_amount
                    })
        
        except Exception as e:
            print(f"  [ERROR] 파일 처리 실패: {filename} - {e}")
            continue
    
    if not all_amounts:
        print("[WARNING] 추출된 금액이 없습니다.")
        return pd.DataFrame()
    
    amounts_df = pd.DataFrame(all_amounts)
    print(f"[OK] 금액 추출 완료: {len(amounts_df)}건")
    
    return amounts_df


def extract_direct_cost_rates(plan_dir: str, channel_master: Dict[str, int]) -> pd.DataFrame:
    """
    계획 파일에서 직접비율 추출
    
    Args:
        plan_dir: 계획 파일 디렉토리
        channel_master: 채널 마스터 매핑 (채널명 -> 채널번호)
    
    Returns:
        pd.DataFrame: 브랜드별/유통채널별 직접비율 데이터프레임
    """
    print("=" * 60)
    print("계획 파일에서 직접비율 추출 시작")
    print("=" * 60)
    
    if not os.path.exists(plan_dir):
        raise FileNotFoundError(f"[ERROR] 계획 데이터 폴더가 없습니다: {plan_dir}")
    
    # 계획 파일 찾기 (RF 파일 제외)
    plan_files = []
    for filename in os.listdir(plan_dir):
        if filename.endswith(".csv") and not is_rf_file(filename):
            filepath = os.path.join(plan_dir, filename)
            plan_files.append(filepath)
    
    if not plan_files:
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {plan_dir}")
    
    print(f"[INFO] 처리 대상 파일 수: {len(plan_files)}")
    
    all_rates = []
    
    for filepath in plan_files:
        filename = os.path.basename(filepath)
        print(f"\n[처리 중] {filename}")
        
        try:
            df, channels, brand_code = read_plan_file(filepath)
            
            # 실판매액 [v-] 행 찾기
            sales_row_idx = None
            for idx, row in df.iterrows():
                if '실판매액' in str(row["구분"]) and '[v-]' in str(row["구분"]).lower():
                    sales_row_idx = idx
                    break
            
            if sales_row_idx is None:
                print(f"  [WARNING] 실판매액 [v-] 행을 찾을 수 없습니다. 스킵합니다.")
                continue
            
            # 각 채널별로 직접비율 계산
            for channel in channels:
                if not channel or channel == "Unassigned" or channel == "수출" or channel.strip() == "":
                    continue
                
                # 채널 컬럼이 있는지 확인
                if channel not in df.columns:
                    continue
                
                # 실판매액 [v-] 값 가져오기
                sales_val = df.at[sales_row_idx, channel]
                if pd.isna(sales_val) or sales_val == "":
                    continue
                
                try:
                    sales_value = float(sales_val)
                except (ValueError, TypeError):
                    continue
                
                if sales_value == 0:
                    continue
                
                # 각 직접비 항목별로 비율 계산
                for cost_item in DIRECT_COST_ITEMS:
                    # 제외 항목은 스킵
                    if cost_item in EXCLUDED_COSTS:
                        continue
                    
                    # 지급임차료_매장(고정), 감가상각비_임차시설물은 비율 계산 대상이 아님 (금액으로 처리)
                    if cost_item in ['지급임차료_매장(고정)', '감가상각비_임차시설물']:
                        continue
                    
                    # 직접비 행 찾기
                    cost_row_idx = None
                    for idx, row in df.iterrows():
                        if str(row["구분"]).strip() == cost_item:
                            cost_row_idx = idx
                            break
                    
                    if cost_row_idx is None:
                        continue
                    
                    
                    # 직접비 값 가져오기
                    cost_val = df.at[cost_row_idx, channel]
                    if pd.isna(cost_val) or cost_val == "":
                        continue
                    
                    try:
                        cost_value = float(cost_val)
                    except (ValueError, TypeError):
                        continue
                    
                    # 비율 계산
                    rate = (cost_value / sales_value) * 100 if sales_value != 0 else 0
                    
                    # 계획 파일의 채널명을 채널번호로 변환
                    channel_num = channel_master.get(channel, None)
                    if channel_num is None:
                        # 부분 매칭 시도
                        for key, val in channel_master.items():
                            if channel in key or key in channel:
                                channel_num = val
                                break
                    
                    # 채널번호를 찾지 못하면 스킵
                    if channel_num is None:
                        continue
                    
                    all_rates.append({
                        '브랜드': brand_code,
                        '유통채널': channel_num,
                        '채널': channel,  # 참고용으로 유지
                        '직접비항목': cost_item,
                        '직접비값': cost_value,
                        '실판매액V-': sales_value,
                        '비율': rate
                    })
        
        except Exception as e:
            print(f"  [ERROR] 파일 처리 실패: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_rates:
        raise ValueError("[ERROR] 추출된 직접비율이 없습니다.")
    
    # 데이터프레임으로 변환
    rates_df = pd.DataFrame(all_rates)
    
    print(f"\n[OK] 직접비율 추출 완료: {len(rates_df)}건")
    
    return rates_df


def pivot_and_format_rates(rates_df: pd.DataFrame, channel_master: Dict[str, int]) -> pd.DataFrame:
    """
    직접비율 데이터를 피벗하여 브랜드/채널별로 정리
    
    Args:
        rates_df: 직접비율 데이터프레임
        channel_master: 채널 마스터 매핑
    
    Returns:
        pd.DataFrame: 피벗된 직접비율 데이터프레임
    """
    print("\n[처리 중] 행열 전환 및 포맷팅...")
    
    # 브랜드별/유통채널별로 피벗
    pivot_df = rates_df.pivot_table(
        index=['브랜드', '유통채널'],
        columns='직접비항목',
        values='비율',
        aggfunc='mean'
    ).reset_index()
    
    # 직접비 항목 컬럼 순서 정리 (로열티, 지급임차료_매장(고정), 감가상각비_임차시설물 제외)
    cost_columns = [
        item for item in DIRECT_COST_ITEMS 
        if item not in EXCLUDED_COSTS 
        and item != '지급수수료_로열티'
        and item not in ['지급임차료_매장(고정)', '감가상각비_임차시설물']
    ]
    
    # 존재하는 컬럼만 선택
    available_cost_cols = [col for col in cost_columns if col in pivot_df.columns]
    
    # 컬럼 순서: 브랜드, 유통채널, 채널(참고용), 직접비 항목들
    # 채널 컬럼이 있으면 포함
    base_cols = ['브랜드', '유통채널']
    if '채널' in pivot_df.columns:
        base_cols.append('채널')
    final_columns = base_cols + available_cost_cols
    pivot_df = pivot_df[final_columns]
    
    # 비율을 퍼센트 형식으로 변환 (소수점 2번째 자리까지 표시)
    for col in available_cost_cols:
        if col in pivot_df.columns:
            pivot_df[col] = pivot_df[col].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 and not pd.isna(x) else "0%"
            )
    
    # 채널번호 추가
    def get_channel_number(channel_name):
        # 채널명 정리
        channel_clean = str(channel_name).strip()
        # 채널 마스터에서 찾기
        channel_num = channel_master.get(channel_clean, None)
        if channel_num is None:
            # 부분 매칭 시도
            for key, val in channel_master.items():
                if channel_clean in key or key in channel_clean:
                    return val
            return 99  # 기본값 99 (기타)
        return channel_num
    
    # 채널번호는 유통채널과 동일하므로 별도로 추가하지 않음
    # 대신 유통채널 컬럼명을 채널번호로 변경하거나 유지
    # (이미 유통채널이 채널번호이므로)
    
    # 기타 채널 추가 (각 브랜드별로)
    print("\n[처리 중] 기타 채널 추가...")
    
    other_rows = []
    for brand in pivot_df['브랜드'].unique():
        # 사입 채널 찾기 (유통채널 8)
        saip_row = pivot_df[(pivot_df['브랜드'] == brand) & (pivot_df['유통채널'] == 8)]
        
        if len(saip_row) > 0:
            other_row = saip_row.iloc[0].copy()
            other_row['유통채널'] = 99
            if '채널' in other_row.index:
                other_row['채널'] = '기타'
            
            # 물류운송비와 물류용역비만 유지, 나머지는 0% 또는 빈 값
            for col in available_cost_cols:
                if col not in ['지급수수료_물류운송비', '지급수수료_물류용역비']:
                    other_row[col] = "0%"
            
            other_rows.append(other_row)
    
    if other_rows:
        other_df = pd.DataFrame(other_rows)
        pivot_df = pd.concat([pivot_df, other_df], ignore_index=True)
    
    # 브랜드, 유통채널 순으로 정렬
    pivot_df = pivot_df.sort_values(['브랜드', '유통채널'], ascending=[True, True])
    pivot_df = pivot_df.reset_index(drop=True)
    
    print(f"[OK] 포맷팅 완료: {len(pivot_df)}행 × {len(pivot_df.columns)}열")
    
    return pivot_df


def apply_direct_costs_to_ke30(ke30_file: str, rates_df: pd.DataFrame, plan_amounts_df: pd.DataFrame, royalty_master: Dict) -> pd.DataFrame:
    """
    ke30 전처리 완료 파일에 직접비 계산 적용
    
    Args:
        ke30_file: ke30 전처리 완료 파일 경로
        rates_df: 직접비율 데이터프레임 (피벗 전)
        plan_amounts_df: 계획 파일에서 추출한 금액 데이터프레임
        royalty_master: 로열티율 마스터 딕셔너리
    
    Returns:
        pd.DataFrame: 직접비가 계산된 데이터프레임
    """
    print("\n" + "=" * 60)
    print("ke30 파일에 직접비 계산 적용 시작")
    print("=" * 60)
    
    print(f"[읽기] {ke30_file}")
    df_ke30 = pd.read_csv(ke30_file, encoding="utf-8-sig")
    print(f"  원본 데이터: {len(df_ke30)}행 × {len(df_ke30.columns)}열")
    
    # 실판매액(V-) 컬럼 찾기
    sales_col = None
    for col in df_ke30.columns:
        if '실판매액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
            sales_col = col
            break
    
    if sales_col is None:
        raise ValueError("[ERROR] 실판매액(V-) 컬럼을 찾을 수 없습니다.")
    
    print(f"  실판매액(V-) 컬럼: {sales_col}")
    
    # 출고매출액(V-) 컬럼 찾기
    shipping_col = None
    for col in df_ke30.columns:
        if '출고매출액' in str(col) and ('v-' in str(col).lower() or 'V-' in str(col)):
            shipping_col = col
            break
    
    if shipping_col:
        print(f"  출고매출액(V-) 컬럼: {shipping_col}")
    
    # 직접비율 데이터를 브랜드/유통채널별로 딕셔너리로 변환
    rates_dict = {}
    for _, row in rates_df.iterrows():
        # 유통채널 기준으로 키 생성
        key = (row['브랜드'], row['유통채널'], row['직접비항목'])
        rates_dict[key] = row['비율'] / 100  # 퍼센트를 소수로 변환
    
    # 계획 파일 금액을 브랜드/유통채널별로 딕셔너리로 변환
    plan_amounts_dict = {}
    if not plan_amounts_df.empty:
        for _, row in plan_amounts_df.iterrows():
            key = (row['브랜드'], row['유통채널'], row['직접비항목'])
            plan_amounts_dict[key] = row['금액']
    
    # 각 직접비 항목별로 계산
    for cost_item in DIRECT_COST_ITEMS:
        if cost_item in EXCLUDED_COSTS:
            continue
        
        cost_col_name = cost_item
        df_ke30[cost_col_name] = 0.0
        
        print(f"  처리 중: {cost_item}")
        
        # 지급임차료_매장(고정), 감가상각비_임차시설물은 계획 파일 금액을 실판매출(v-) 대비 비율로 분배
        if cost_item in ['지급임차료_매장(고정)', '감가상각비_임차시설물']:
            # 브랜드/유통채널별로 집계
            for brand in df_ke30['브랜드'].unique():
                brand_str = str(brand).strip()
                for channel_num in df_ke30[df_ke30['브랜드'] == brand]['유통채널'].dropna().unique():
                    try:
                        channel_num_int = int(float(channel_num))
                    except (ValueError, TypeError):
                        continue
                    
                    key = (brand_str, channel_num_int, cost_item)
                    plan_amount = plan_amounts_dict.get(key, 0)
                    
                    if plan_amount > 0:
                        # 해당 브랜드/유통채널의 행 필터링
                        mask = (df_ke30['브랜드'] == brand) & (df_ke30['유통채널'] == channel_num)
                        filtered_df = df_ke30[mask].copy()
                        
                        if len(filtered_df) > 0:
                            # 실판매출(v-) 컬럼은 이미 위에서 찾았으므로 재사용
                            if sales_col is None:
                                # 기본값으로 행 수로 분배
                                row_count = mask.sum()
                                amount_per_row = int(plan_amount / row_count)
                                df_ke30.loc[mask, cost_col_name] = amount_per_row
                                current_total = amount_per_row * row_count
                                remainder = int(plan_amount - current_total)
                                if remainder != 0 and row_count > 0:
                                    first_idx = mask[mask].index[0]
                                    df_ke30.at[first_idx, cost_col_name] = df_ke30.at[first_idx, cost_col_name] + remainder
                            else:
                                # 실판매출(v-) 대비 비율로 분배
                                # 각 행의 실판매출(v-) 값 가져오기
                                sales_values = filtered_df[sales_col].fillna(0).astype(float)
                                total_sales = sales_values.sum()
                                
                                if total_sales > 0:
                                    # 각 행에 비율에 따라 금액 할당
                                    allocated_amounts = (sales_values / total_sales * plan_amount).round().astype(int)
                                    
                                    # 총합이 정확히 plan_amount가 되도록 조정
                                    current_total = allocated_amounts.sum()
                                    difference = int(plan_amount - current_total)
                                    
                                    if difference != 0 and len(allocated_amounts) > 0:
                                        # 차이를 마지막 행에 추가/차감
                                        last_idx = allocated_amounts.index[-1]
                                        allocated_amounts.iloc[-1] = allocated_amounts.iloc[-1] + difference
                                    
                                    # KE30 데이터프레임에 할당
                                    for idx, amount in allocated_amounts.items():
                                        df_ke30.at[idx, cost_col_name] = amount
                                else:
                                    # 실판매출이 0이면 행 수로 분배
                                    row_count = mask.sum()
                                    amount_per_row = int(plan_amount / row_count)
                                    df_ke30.loc[mask, cost_col_name] = amount_per_row
                                    current_total = amount_per_row * row_count
                                    remainder = int(plan_amount - current_total)
                                    if remainder != 0 and row_count > 0:
                                        first_idx = mask[mask].index[0]
                                        df_ke30.at[first_idx, cost_col_name] = df_ke30.at[first_idx, cost_col_name] + remainder
        
        # 지급수수료_로열티는 로열티율 마스터 사용
        elif cost_item == '지급수수료_로열티':
            for idx, row in df_ke30.iterrows():
                brand = str(row.get('브랜드', '')).strip()
                channel_num = row.get('유통채널', None)
                
                if pd.isna(channel_num):
                    continue
                
                try:
                    channel_num_int = int(float(channel_num))
                except (ValueError, TypeError):
                    continue
                
                # 로열티율 마스터에서 찾기
                royalty_info = royalty_master.get((brand, channel_num_int), None)
                if royalty_info:
                    rate = royalty_info['rate']
                    base = royalty_info['base']
                    
                    # 기준매출에 따라 다른 컬럼 사용
                    if '실판가' in base or '실판매' in base:
                        base_value = row[sales_col]
                    elif '출고가' in base or '출고매출' in base:
                        base_value = row[shipping_col] if shipping_col else row[sales_col]
                    else:
                        base_value = row[sales_col]  # 기본값
                    
                    if pd.notna(base_value) and base_value > 0:
                        cost_value = base_value * rate
                        df_ke30.at[idx, cost_col_name] = int(round(cost_value))
        
        # 나머지는 비율 기반 계산
        else:
            for idx, row in df_ke30.iterrows():
                brand = str(row.get('브랜드', '')).strip()
                channel_num = row.get('유통채널', None)
                sales_value = row[sales_col]
                
                if pd.isna(sales_value) or sales_value == 0:
                    continue
                
                if pd.isna(channel_num):
                    continue
                
                try:
                    channel_num_int = int(float(channel_num))
                except (ValueError, TypeError):
                    continue
                
                # 직접비율 찾기 (브랜드, 유통채널 기준)
                key = (brand, channel_num_int, cost_item)
                rate = rates_dict.get(key, 0)
                
                # 기타 채널(유통채널 99)이고 물류운송비 또는 물류용역비인 경우, 사입 채널(유통채널 8)의 비율 사용
                if channel_num_int == 99 and cost_item in ['지급수수료_물류운송비', '지급수수료_물류용역비']:
                    saip_key = (brand, 8, cost_item)
                    rate = rates_dict.get(saip_key, 0)
                
                if rate > 0:
                    # 직접비 계산 (정수로 반올림)
                    cost_value = sales_value * rate
                    df_ke30.at[idx, cost_col_name] = int(round(cost_value))
    
    print(f"[OK] 직접비 계산 완료: {len(df_ke30)}행")
    
    return df_ke30


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='계획 파일에서 직접비율 추출 및 ke30 파일에 적용',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--plan-dir',
        type=str,
        default=r'C:\Users\AD0283\Desktop\AIproject\Project_Forcast\raw\202511\plan',
        help='계획 파일 디렉토리 경로'
    )
    
    parser.add_argument(
        '--ke30-file',
        type=str,
        default=r'C:\Users\AD0283\Desktop\AIproject\Project_Forcast\raw\202511\current_year\20251117\ke30_20251117_202511_전처리완료.csv',
        help='ke30 전처리 완료 파일 경로'
    )
    
    parser.add_argument(
        '--output-rates',
        type=str,
        default=None,
        help='직접비율 출력 파일 경로 (지정하지 않으면 자동 생성)'
    )
    
    parser.add_argument(
        '--output-ke30',
        type=str,
        default=None,
        help='직접비 계산된 ke30 파일 출력 경로 (지정하지 않으면 자동 생성)'
    )
    
    args = parser.parse_args()
    
    try:
        # 1. 마스터 파일 로드
        print("\n[준비] 마스터 파일 로드 중...")
        channel_master = load_channel_master()
        royalty_master = load_royalty_rate_master()
        
        # 2. 계획 파일에서 직접비율 추출
        rates_df = extract_direct_cost_rates(args.plan_dir, channel_master)
        
        # 3. 계획 파일에서 금액 추출 (지급임차료_매장(고정), 감가상각비_임차시설물, 지급임차료_관리비)
        plan_amounts_df = extract_plan_amounts(args.plan_dir, channel_master)
        
        # 3. 행열 전환 및 포맷팅
        pivot_df = pivot_and_format_rates(rates_df, channel_master)
        
        # 4. 직접비율 파일 저장
        if args.output_rates is None:
            plan_dir = Path(args.plan_dir)
            output_rates_path = plan_dir / "직접비율_추출결과.csv"
        else:
            output_rates_path = Path(args.output_rates)
        
        output_rates_path.parent.mkdir(parents=True, exist_ok=True)
        pivot_df.to_csv(output_rates_path, index=False, encoding='utf-8-sig')
        print(f"\n[저장] 직접비율 파일 저장: {output_rates_path}")
        print(f"  데이터: {len(pivot_df)}행 × {len(pivot_df.columns)}열")
        
        # 5. ke30 파일에 직접비 계산 적용
        if args.ke30_file and os.path.exists(args.ke30_file):
            df_ke30_with_costs = apply_direct_costs_to_ke30(args.ke30_file, rates_df, plan_amounts_df, royalty_master)
            
            # 6. 결과 파일 저장
            if args.output_ke30 is None:
                ke30_path = Path(args.ke30_file)
                output_ke30_path = ke30_path.parent / f"{ke30_path.stem}_직접비계산.csv"
            else:
                output_ke30_path = Path(args.output_ke30)
            
            output_ke30_path.parent.mkdir(parents=True, exist_ok=True)
            df_ke30_with_costs.to_csv(output_ke30_path, index=False, encoding='utf-8-sig')
            print(f"\n[저장] 직접비 계산된 ke30 파일 저장: {output_ke30_path}")
            print(f"  데이터: {len(df_ke30_with_costs)}행 × {len(df_ke30_with_costs.columns)}열")
        
        print("\n" + "=" * 60)
        print("[COMPLETE] 모든 작업 완료!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

