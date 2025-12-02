"""
KE30 전체 전처리 파이프라인
===============================================================

[전체 전처리]
1. SAP에서 PA 1000 다운
2. C:\ke30\ke30_YYYYMMDD_YYYYMM.xlsx 파일 읽기
3. CSV로 변환하여 저장 (원본 파일명 유지)
4. 데이터 전처리 실행
5. 피벗 집계
6. 마스터 파일 기반 컬럼 추가
7. 표준제간비, 재고평가감환입, 매출원가(평가감반영), 매출총이익 필드추가

[채널별/아이템별 전처리]
1. 2차 집계 (브랜드, 유통채널, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드)
2. 매출총이익까지 집계 (직접비 제외)

[채널별 전처리]
1. 2차 집계 (브랜드, 유통채널, 채널명)
2. 매출총이익까지 집계 (직접비 제외)

[직접비 계산]
1. 직접비율 및 정액 추출
2. 직접비율 전처리
3. 집계된 데이터(채널/아이템별, 채널별)에 직접비 계산 적용
4. 직접비 합계 및 직접이익 계산 (세부 항목 유지)
5. 직접비 합계필드 추가
6. 직접이익 계산

작성일: 2025-11-24
수정일: 2025-12-01 (직접비 계산 순서 변경: 집계 후 계산)
"""

import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import re
from pathlib import Path
from typing import Dict, List, Optional
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 기존 스크립트 import
# 경로를 추가하여 모듈 import 가능하도록 설정
scripts_dir = project_root / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

# 모듈 import
import process_ke30_current_year as process_ke30
import extract_direct_cost_rates as extract_direct
import aggregate_direct_costs_by_master as aggregate_direct

# 경로 설정
KE30_INPUT_DIR = r"C:\ke30"
CSV_OUTPUT_DIR = project_root / "raw"
MASTER_DIR = project_root / "Master"
PLAN_DIR = CSV_OUTPUT_DIR / "202511" / "plan"

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


def aggregate_direct_costs_by_master(df, direct_cost_master):
    """
    직접비 항목을 마스터 항목으로 집계
    
    Args:
        df: 직접비가 계산된 데이터프레임
        direct_cost_master: 직접비 마스터 매핑 딕셔너리
    
    Returns:
        pd.DataFrame: 마스터 항목으로 집계된 데이터프레임
    """
    print("\n[직접비 마스터 집계] 직접비 항목을 마스터 항목으로 집계 중...")
    
    # 직접비 항목 컬럼 찾기
    direct_cost_columns = []
    for col in df.columns:
        if col in direct_cost_master:
            direct_cost_columns.append(col)
    
    if not direct_cost_columns:
        print("  [WARNING] 직접비 항목 컬럼을 찾을 수 없습니다.")
        return df
    
    # 모든 직접비 항목을 마스터 항목명으로 변환
    # 1단계: 마스터 항목별로 그룹화
    master_categories = {}
    for cost_col in direct_cost_columns:
        if cost_col in df.columns:
            master_category = direct_cost_master.get(cost_col, '기타')
            if master_category not in master_categories:
                master_categories[master_category] = []
            master_categories[master_category].append(cost_col)
    
    # 2단계: 각 마스터 항목별로 합산하여 마스터 항목명 컬럼 생성
    columns_to_drop = []  # 제거할 원본 세부 항목 컬럼들
    for master_cat, cost_items in master_categories.items():
        # cost_items에 있는 컬럼 중 실제로 존재하는 컬럼만 사용
        existing_items = [item for item in cost_items if item in df.columns]
        if existing_items:
            # 마스터 항목명 컬럼 생성 (기존 컬럼이 있으면 합산, 없으면 새로 생성)
            if master_cat in df.columns:
                # 이미 마스터 항목명 컬럼이 있으면 기존 값과 합산
                df[master_cat] = df[master_cat].fillna(0) + df[existing_items].sum(axis=1).fillna(0)
            else:
                # 마스터 항목명 컬럼이 없으면 새로 생성
                df[master_cat] = df[existing_items].sum(axis=1).fillna(0)
            df[master_cat] = df[master_cat].astype(int)
            
            # 원본 세부 항목 컬럼들은 제거 대상에 추가
            columns_to_drop.extend(existing_items)
    
    # 3단계: 원본 세부 항목 컬럼 제거
    if columns_to_drop:
        # 중복 제거
        columns_to_drop = list(set(columns_to_drop))
        # 마스터 항목명 컬럼은 제거 대상에서 제외
        columns_to_drop = [col for col in columns_to_drop if col not in master_categories.keys()]
        df = df.drop(columns=columns_to_drop, errors='ignore')
        print(f"  [INFO] 원본 세부 항목 컬럼 제거: {len(columns_to_drop)}개")
    
    # 4단계: 직접비 마스터 딕셔너리에 마스터 항목명 매핑 추가 (자기 자신으로)
    for master_cat in master_categories.keys():
        direct_cost_master[master_cat] = master_cat
    
    # 전체 직접비 합계 계산
    all_direct_cost_cols = [col for col in df.columns if col in direct_cost_master]
    df['직접비 합계'] = df[all_direct_cost_cols].sum(axis=1).astype(int)
    
    # 직접이익 계산 (매출총이익 - 직접비합계)
    if '매출총이익' in df.columns:
        df['직접이익'] = df['매출총이익'] - df['직접비 합계']
        df['직접이익'] = df['직접이익'].astype(int)
    else:
        df['직접이익'] = 0
    
    print(f"  [OK] 마스터 항목 집계 완료: {', '.join(master_categories.keys())}")
    
    return df


def aggregate_by_channel_item(df):
    """
    채널별/아이템별 집계 (매출총이익까지)
    
    행: 브랜드, 유통채널, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드
    값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익
    (직접비는 집계 후 별도 계산)
    """
    print("\n[채널별/아이템별 집계] 집계 중...")
    
    # 집계 기준 컬럼
    groupby_cols = ['브랜드', '유통채널', '채널명', '아이템_중분류', '아이템_소분류', '아이템코드']
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 (직접비 제외, 매출총이익까지만)
    value_cols = [
        '합계 : 판매금액(TAG가)', '합계 : 실판매액', '합계 : 실판매액(V-)',
        '합계 : 출고매출액(V-) Actual', '매출원가(평가감환입반영)', '매출총이익'
    ]
    
    # 직접비 관련 컬럼은 제외 (집계 후 별도 계산)
    
    # 실제 존재하는 컬럼만 사용
    available_value_cols = []
    processed_cols = set()  # 이미 처리된 컬럼 추적
    
    for col in value_cols:
        found = False
        # 1단계: 정확히 일치하는 컬럼 찾기
        if col in df.columns and col not in processed_cols:
            available_value_cols.append(col)
            processed_cols.add(col)
            found = True
        else:
            # 2단계: 유사한 컬럼명 찾기
            # 실판매액의 경우 두 컬럼을 모두 포함
            if col == '합계 : 실판매액':
                # '합계 : 실판매액'과 '합계 : 실판매액(V-)' 모두 찾기
                if '합계 : 실판매액' in df.columns and '합계 : 실판매액' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액')
                    processed_cols.add('합계 : 실판매액')
                    found = True
                if '합계 : 실판매액(V-)' in df.columns and '합계 : 실판매액(V-)' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액(V-)')
                    processed_cols.add('합계 : 실판매액(V-)')
                    found = True
            elif col == '합계 : 실판매액(V-)':
                # 이미 '합계 : 실판매액'에서 처리했을 수 있으므로 확인
                if '합계 : 실판매액(V-)' in df.columns and '합계 : 실판매액(V-)' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액(V-)')
                    processed_cols.add('합계 : 실판매액(V-)')
                    found = True
            else:
                # 다른 컬럼은 유사한 컬럼명 찾기
                best_match = None
                for df_col in df.columns:
                    if (col in str(df_col) or str(df_col) in col) and df_col not in processed_cols:
                        if best_match is None or len(str(df_col)) > len(str(best_match)):
                            best_match = df_col
                            found = True
                if found and best_match:
                    available_value_cols.append(best_match)
                    processed_cols.add(best_match)
        
        if not found:
            # 직접비 관련 컬럼은 제외 (집계 후 별도 계산)
            pass
    
    if not available_groupby or not available_value_cols:
        print("  [ERROR] 집계 기준 또는 값 컬럼을 찾을 수 없습니다.")
        return df
    
    # 집계 실행
    agg_dict = {col: 'sum' for col in available_value_cols}
    df_aggregated = df.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    print(f"  [OK] 집계 완료: {len(df_aggregated)}행")
    
    return df_aggregated


def calculate_analysis_month_from_update_date(update_date_str: str) -> str:
    """
    업데이트일자로부터 주차 시작일의 월을 계산하여 분석월 반환
    
    Args:
        update_date_str: YYYYMMDD 형식의 업데이트일자 (예: "20251201")
    
    Returns:
        YYYYMM 형식의 분석월 (주차 시작일의 월, 예: "202511")
    
    예시:
        업데이트일자: 2025-12-01 (월요일)
        주차 시작일: 2025-11-24 (전주 월요일)
        분석월: 2025-11
    """
    # YYYYMMDD -> Date 객체
    year = int(update_date_str[:4])
    month = int(update_date_str[4:6])
    day = int(update_date_str[6:8])
    update_date = datetime(year, month, day)
    
    # 업데이트일자의 요일 (0=월요일, 6=일요일)
    day_of_week = update_date.weekday()  # 0=월요일, 6=일요일
    
    # 전주 월요일까지의 일수 계산
    if day_of_week == 0:  # 월요일
        days_to_monday = 7
    elif day_of_week == 6:  # 일요일
        days_to_monday = 6
    else:  # 화~토요일
        days_to_monday = day_of_week + 7
    
    # 주차 시작일 계산 (전주 월요일)
    week_start_date = update_date - timedelta(days=days_to_monday)
    
    # 주차 시작일의 월을 분석월로 사용
    analysis_year = week_start_date.year
    analysis_month = week_start_date.month
    analysis_month_str = f"{analysis_year}{analysis_month:02d}"  # YYYYMM 형식
    
    return analysis_month_str


def extract_evaluation_setting(plan_dir: str, channel_master: Dict[str, int]) -> pd.DataFrame:
    """
    계획 파일에서 재고평가감_설정금액 추출 (Unassigned 컬럼)
    
    Args:
        plan_dir: 계획 파일 디렉토리
        channel_master: 채널 마스터 매핑
    
    Returns:
        pd.DataFrame: 브랜드별 평가감설정 데이터프레임 (브랜드, 유통채널, 채널명, 매출원가(평가감환입반영), 매출총이익, 직접이익)
    """
    print("\n[평가감설정 추출] 계획 파일에서 재고평가감_설정금액 추출 중...")
    
    if not os.path.exists(plan_dir):
        print(f"  [WARNING] 계획 데이터 폴더가 없습니다: {plan_dir}")
        return pd.DataFrame()
    
    # 계획 파일 찾기 (RF 파일 제외)
    plan_files = []
    for filename in os.listdir(plan_dir):
        if filename.endswith(".csv") and "RF" not in filename.upper():
            filepath = os.path.join(plan_dir, filename)
            plan_files.append(filepath)
    
    if not plan_files:
        print(f"  [WARNING] 계획 파일을 찾을 수 없습니다: {plan_dir}")
        return pd.DataFrame()
    
    evaluation_settings = []
    
    for filepath in plan_files:
        filename = os.path.basename(filepath)
        
        try:
            df, channels, brand_code = extract_direct.read_plan_file(filepath)
            
            # Unassigned 컬럼 찾기
            unassigned_col = None
            for channel in channels:
                if channel and ("Unassigned" in str(channel) or str(channel).strip() == "Unassigned"):
                    unassigned_col = channel
                    break
            
            if not unassigned_col or unassigned_col not in df.columns:
                print(f"  [WARNING] {filename}: Unassigned 컬럼을 찾을 수 없습니다.")
                continue
            
            # 재고평가감_설정 또는 재고자산평가_설정 행 찾기
            evaluation_row_idx = None
            evaluation_row_name = None
            for idx, row in df.iterrows():
                row_str = str(row["구분"]).strip()
                if "재고평가감_설정" in row_str or "재고평가감 설정" in row_str:
                    evaluation_row_idx = idx
                    evaluation_row_name = "재고평가감_설정"
                    break
                elif "재고자산평가_설정" in row_str or "재고자산평가 설정" in row_str:
                    evaluation_row_idx = idx
                    evaluation_row_name = "재고자산평가_설정"
                    break
            
            if evaluation_row_idx is None:
                print(f"  [WARNING] {filename}: 재고평가감_설정 또는 재고자산평가_설정 행을 찾을 수 없습니다.")
                continue
            
            # 평가감설정금액 값 추출
            evaluation_val = df.at[evaluation_row_idx, unassigned_col]
            if pd.isna(evaluation_val) or evaluation_val == "":
                print(f"  [WARNING] {filename}: {evaluation_row_name} 값이 없습니다.")
                continue
            
            try:
                evaluation_amount = float(evaluation_val) * 1000  # 계획 파일은 *1000
            except (ValueError, TypeError):
                print(f"  [WARNING] {filename}: {evaluation_row_name} 값을 숫자로 변환할 수 없습니다.")
                continue
            
            # 유통채널 0 (미지정)으로 설정
            channel_num = 0
            channel_name = "미지정"
            
            # 평가감설정은 음수로 저장 (설정이므로)
            evaluation_settings.append({
                '브랜드': brand_code,
                '유통채널': channel_num,
                '채널명': channel_name,
                '매출원가(평가감환입반영)': evaluation_amount,
                '매출총이익': -evaluation_amount,  # 평가감설정은 손익에 반영
                '직접이익': -evaluation_amount  # 평가감설정은 직접이익에 반영
            })
            
        except Exception as e:
            print(f"  [ERROR] 파일 처리 실패: {filename} - {e}")
            continue
    
    if not evaluation_settings:
        print("  [WARNING] 추출된 평가감설정이 없습니다.")
        return pd.DataFrame()
    
    evaluation_df = pd.DataFrame(evaluation_settings)
    print(f"  [OK] 평가감설정 추출 완료: {len(evaluation_df)}건")
    
    return evaluation_df


def aggregate_by_channel(df, plan_dir: str = None, channel_master: Dict[str, int] = None):
    """
    채널별 집계 (매출총이익까지)
    
    행: 브랜드, 유통채널, 채널명
    값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익
    (직접비는 집계 후 별도 계산)
    
    Args:
        df: 집계할 데이터프레임
        plan_dir: 계획 파일 디렉토리 (평가감설정 추출용)
        channel_master: 채널 마스터 매핑 (평가감설정 추출용)
    """
    print("\n[채널별 집계] 집계 중...")
    
    # 집계 기준 컬럼
    groupby_cols = ['브랜드', '유통채널', '채널명']
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 (직접비 제외, 매출총이익까지만)
    value_cols = [
        '합계 : 판매금액(TAG가)', '합계 : 실판매액', '합계 : 실판매액(V-)',
        '합계 : 출고매출액(V-) Actual', '매출원가(평가감환입반영)', '매출총이익'
    ]
    
    # 직접비 관련 컬럼은 제외 (집계 후 별도 계산)
    
    # 실제 존재하는 컬럼만 사용
    available_value_cols = []
    processed_cols = set()  # 이미 처리된 컬럼 추적
    
    for col in value_cols:
        found = False
        # 1단계: 정확히 일치하는 컬럼 찾기
        if col in df.columns and col not in processed_cols:
            available_value_cols.append(col)
            processed_cols.add(col)
            found = True
        else:
            # 2단계: 유사한 컬럼명 찾기
            # 실판매액의 경우 두 컬럼을 모두 포함
            if col == '합계 : 실판매액':
                # '합계 : 실판매액'과 '합계 : 실판매액(V-)' 모두 찾기
                if '합계 : 실판매액' in df.columns and '합계 : 실판매액' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액')
                    processed_cols.add('합계 : 실판매액')
                    found = True
                if '합계 : 실판매액(V-)' in df.columns and '합계 : 실판매액(V-)' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액(V-)')
                    processed_cols.add('합계 : 실판매액(V-)')
                    found = True
            elif col == '합계 : 실판매액(V-)':
                # 이미 '합계 : 실판매액'에서 처리했을 수 있으므로 확인
                if '합계 : 실판매액(V-)' in df.columns and '합계 : 실판매액(V-)' not in processed_cols:
                    available_value_cols.append('합계 : 실판매액(V-)')
                    processed_cols.add('합계 : 실판매액(V-)')
                    found = True
            else:
                # 다른 컬럼은 유사한 컬럼명 찾기
                best_match = None
                for df_col in df.columns:
                    if (col in str(df_col) or str(df_col) in col) and df_col not in processed_cols:
                        if best_match is None or len(str(df_col)) > len(str(best_match)):
                            best_match = df_col
                            found = True
                if found and best_match:
                    available_value_cols.append(best_match)
                    processed_cols.add(best_match)
        
        if not found:
            # 직접비 관련 컬럼은 제외 (집계 후 별도 계산)
            pass
    
    if not available_groupby or not available_value_cols:
        print("  [ERROR] 집계 기준 또는 값 컬럼을 찾을 수 없습니다.")
        return df
    
    # 집계 실행
    agg_dict = {col: 'sum' for col in available_value_cols}
    df_aggregated = df.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    print(f"  [OK] 집계 완료: {len(df_aggregated)}행")
    
    # 평가감설정 추가
    if plan_dir and channel_master:
        print("\n[평가감설정 추가] 계획 파일에서 평가감설정 추출 중...")
        evaluation_df = extract_evaluation_setting(plan_dir, channel_master)
        
        if not evaluation_df.empty:
            # 평가감설정 행을 위한 빈 컬럼 생성 (나머지 컬럼은 공란/NaN)
            for col in available_value_cols:
                if col not in evaluation_df.columns:
                    evaluation_df[col] = None  # 공란으로 설정
            
            # 집계 결과와 평가감설정 합치기
            # 컬럼 순서 맞추기 (중복 제거)
            all_cols = list(dict.fromkeys(available_groupby + available_value_cols))
            
            # evaluation_df에 없는 컬럼 추가
            for col in all_cols:
                if col not in evaluation_df.columns:
                    evaluation_df[col] = None
            
            # df_aggregated에 없는 컬럼 추가
            for col in all_cols:
                if col not in df_aggregated.columns:
                    df_aggregated[col] = None
            
            # 컬럼 순서 맞추기
            evaluation_df = evaluation_df[all_cols]
            df_aggregated = df_aggregated[all_cols]
            
            # 합치기
            df_aggregated = pd.concat([df_aggregated, evaluation_df], ignore_index=True)
            print(f"  [OK] 평가감설정 추가 완료: {len(evaluation_df)}건")
        else:
            print("  [WARNING] 평가감설정을 추출할 수 없습니다.")
    
    return df_aggregated


def main():
    """
    메인 실행 함수
    """
    print("=" * 80)
    print("KE30 전체 전처리 파이프라인 시작")
    print("=" * 80)
    
    # ==========================================
    # Step 1: 마스터 파일 로드
    # ==========================================
    print("\n[1단계] 마스터 파일 로드 중...")
    channel_master = process_ke30.load_channel_master()
    item_master = process_ke30.load_item_master()
    jeonganbi_master = process_ke30.load_jeonganbi_rate_master()
    evaluation_master = process_ke30.load_evaluation_rate_master()
    direct_cost_master = aggregate_direct.load_direct_cost_master()
    
    # 직접비 관련 마스터
    channel_master_for_direct_cost = extract_direct.load_channel_master()
    royalty_master = extract_direct.load_royalty_rate_master()
    
    # ==========================================
    # Step 2: KE30 파일 찾기 및 읽기
    # ==========================================
    print("\n[2단계] KE30 파일 찾기 및 읽기...")
    excel_path, base_filename = process_ke30.find_latest_ke30_file()
    
    # 파일명에서 날짜 추출 (예: ke30_20251117_202511 -> 20251117, 202511)
    match = re.match(r'ke30_(\d{8})_(\d{6})', base_filename)
    if not match:
        raise ValueError(f"[ERROR] 파일명 형식이 올바르지 않습니다: {base_filename}")
    
    date_str = match.group(1)  # 20251117 (업데이트일자)
    file_analysis_month = match.group(2)  # 202511 (파일명의 분석월)
    
    # 업데이트일자로부터 주차 시작일의 월 계산
    calculated_analysis_month = calculate_analysis_month_from_update_date(date_str)
    
    # 파일명의 분석월과 계산된 분석월 비교
    if file_analysis_month != calculated_analysis_month:
        print(f"\n[WARNING] 파일명의 분석월({file_analysis_month})과 계산된 분석월({calculated_analysis_month})이 다릅니다.")
        print(f"   업데이트일자: {date_str}")
        print(f"   파일명 분석월: {file_analysis_month}")
        print(f"   계산된 분석월(주차 시작일 기준): {calculated_analysis_month}")
        print(f"   → 파일명의 분석월({file_analysis_month})을 사용합니다.")
    else:
        print(f"\n[INFO] 분석월 확인: {file_analysis_month} (파일명과 계산 결과 일치)")
    
    # 파일명의 분석월을 사용 (사용자 요청에 따라)
    analysis_month = file_analysis_month
    analysis_month_formatted = f"{analysis_month[:4]}-{analysis_month[4:6]}"  # 2025-11
    
    print(f"\n[INFO] 분석월 설정:")
    print(f"   업데이트일자: {date_str}")
    print(f"   사용할 분석월: {analysis_month} (파일명에서 추출)")
    print(f"   계산된 분석월: {calculated_analysis_month} (주차 시작일 기준, 참고용)")
    
    # 출력 디렉토리 생성
    date_output_dir = CSV_OUTPUT_DIR / analysis_month / "current_year" / date_str
    date_output_dir.mkdir(parents=True, exist_ok=True)
    
    # CSV 변환
    csv_output_path = os.path.join(date_output_dir, f"{base_filename}.csv")
    df_raw = process_ke30.convert_excel_to_csv(excel_path, csv_output_path)
    
    # ==========================================
    # Step 3: [전체 전처리] 데이터 전처리
    # ==========================================
    print("\n[3단계] [전체 전처리] 데이터 전처리...")
    df_processed = process_ke30.preprocess_ke30_data(df_raw, channel_master, item_master)
    
    # 원가 계산 필드 추가 (파일명에서 추출한 분석월 사용)
    df_with_cost = process_ke30.add_cost_calculation_fields(df_processed, jeonganbi_master, evaluation_master, analysis_month)
    
    # 전처리 완료 파일 저장
    preprocessed_output_path = date_output_dir / f"{base_filename}_전처리완료.csv"
    df_with_cost.to_csv(preprocessed_output_path, index=False, encoding='utf-8-sig')
    print(f"\n[OK] [전체 전처리] 파일 저장: {preprocessed_output_path}")
    print(f"   데이터: {len(df_with_cost)}행 × {len(df_with_cost.columns)}열")
    
    # ==========================================
    # Step 4: [채널별/아이템별 전처리] 집계 (매출총이익까지)
    # ==========================================
    print("\n[4단계] [채널별/아이템별 전처리] 집계 (매출총이익까지)...")
    df_shop_item = aggregate_by_channel_item(df_with_cost)
    
    shop_item_output_path = date_output_dir / f"{base_filename}_Shop_item.csv"
    df_shop_item.to_csv(shop_item_output_path, index=False, encoding='utf-8-sig')
    print(f"\n[OK] [채널별/아이템별 전처리] 파일 저장: {shop_item_output_path}")
    print(f"   데이터: {len(df_shop_item)}행 × {len(df_shop_item.columns)}열")
    
    # ==========================================
    # Step 5: [채널별 전처리] 집계 (매출총이익까지)
    # ==========================================
    print("\n[5단계] [채널별 전처리] 집계 (매출총이익까지)...")
    df_shop = aggregate_by_channel(df_with_cost, str(PLAN_DIR), channel_master_for_direct_cost)
    
    shop_output_path = date_output_dir / f"{base_filename}_Shop.csv"
    df_shop.to_csv(shop_output_path, index=False, encoding='utf-8-sig')
    print(f"\n[OK] [채널별 전처리] 파일 저장: {shop_output_path}")
    print(f"   데이터: {len(df_shop)}행 × {len(df_shop.columns)}열")
    
    # ==========================================
    # Step 6: [직접비 계산] 집계된 데이터에 직접비 계산 및 직접이익 계산
    # ==========================================
    print("\n[6단계] [직접비 계산] 집계된 데이터에 직접비 계산 및 직접이익 계산...")
    
    # 직접비율 파일 경로 (분석월 포함)
    rates_output_path = PLAN_DIR / f"{analysis_month}R_직접비율_추출결과.csv"
    
    # 직접비율 및 계획 금액 추출
    if rates_output_path.exists():
        print(f"  [INFO] 기존 직접비율 파일 발견: {rates_output_path}")
        print(f"  [INFO] 기존 파일 재사용 중...")
        rates_pivoted_df = pd.read_csv(rates_output_path, encoding='utf-8-sig')
        print(f"  [OK] 기존 직접비율 파일 로드 완료")
        
        # rates_df와 plan_amounts_df는 직접비 계산에 필요하므로 계획 파일에서 추출
        plan_amounts_df = extract_direct.extract_plan_amounts(str(PLAN_DIR), channel_master_for_direct_cost)
        rates_df = extract_direct.extract_direct_cost_rates(str(PLAN_DIR), channel_master_for_direct_cost)
    else:
        # 기존 파일이 없으면 추출 및 저장
        print(f"  [INFO] 직접비율 파일이 없어 새로 추출합니다...")
        plan_amounts_df = extract_direct.extract_plan_amounts(str(PLAN_DIR), channel_master_for_direct_cost)
        rates_df = extract_direct.extract_direct_cost_rates(str(PLAN_DIR), channel_master_for_direct_cost)
        rates_pivoted_df = extract_direct.pivot_and_format_rates(rates_df, channel_master_for_direct_cost)
        
        # 직접비율 파일 저장
        rates_output_path.parent.mkdir(parents=True, exist_ok=True)
        rates_pivoted_df.to_csv(rates_output_path, index=False, encoding='utf-8-sig')
        print(f"  [OK] 직접비율 파일 저장: {rates_output_path}")
    
    # 채널/아이템별 집계 데이터는 직접비 계산 제외 (매출총이익까지만)
    print("\n  [채널/아이템별 집계 데이터] 직접비 계산 제외 (매출총이익까지만 유지)")
    
    # 채널별 집계 데이터에 직접비 계산 적용
    print("\n  [채널별 집계 데이터에 직접비 계산 적용]")
    df_shop_with_costs = extract_direct.apply_direct_costs_to_ke30(
        str(shop_output_path),
        rates_df,
        plan_amounts_df,
        royalty_master
    )
    
    # 직접비 합계 및 직접이익 계산 (채널별)
    direct_cost_columns_shop = [col for col in df_shop_with_costs.columns if col in DIRECT_COST_ITEMS]
    if direct_cost_columns_shop:
        df_shop_with_costs['직접비 합계'] = df_shop_with_costs[direct_cost_columns_shop].sum(axis=1).astype(int)
        if '매출총이익' in df_shop_with_costs.columns:
            df_shop_with_costs['직접이익'] = (df_shop_with_costs['매출총이익'] - df_shop_with_costs['직접비 합계']).astype(int)
        else:
            df_shop_with_costs['직접이익'] = 0
    else:
        df_shop_with_costs['직접비 합계'] = 0
        df_shop_with_costs['직접이익'] = 0
    
    # 채널별 파일 저장
    df_shop_with_costs.to_csv(shop_output_path, index=False, encoding='utf-8-sig')
    print(f"  [OK] 채널별 직접비 계산 완료: {len(df_shop_with_costs)}행")
    
    print(f"\n[OK] [직접비 계산] 완료")
    
    # ==========================================
    # Step 7: 메타데이터 저장
    # ==========================================
    print("\n[7단계] 메타데이터 저장...")
    
    metadata = {
        "update_date": date_str,  # 20251201
        "update_date_formatted": f"{date_str[:4]}.{date_str[4:6]}.{date_str[6:8]}",  # 2025.12.01
        "analysis_month": analysis_month,  # 202511 (파일명에서 추출)
        "analysis_month_formatted": analysis_month_formatted,  # 2025-11
        "calculated_analysis_month": calculated_analysis_month,  # 주차 시작일 기준 계산값 (참고용)
        "processed_at": datetime.now().isoformat(),
        "files": {
            "preprocessed": f"{base_filename}_전처리완료.csv",
            "shop_item": f"{base_filename}_Shop_item.csv",
            "shop": f"{base_filename}_Shop.csv"
        }
    }
    
    metadata_path = date_output_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"[OK] 메타데이터 저장: {metadata_path}")
    
    print("\n" + "=" * 80)
    print("[COMPLETE] KE30 전체 전처리 파이프라인 완료!")
    print("=" * 80)
    print(f"\n생성된 파일:")
    print(f"  1. {preprocessed_output_path}")
    print(f"  2. {shop_item_output_path}")
    print(f"  3. {shop_output_path}")
    print(f"  4. {rates_output_path}")
    print(f"  5. {metadata_path}")


if __name__ == "__main__":
    main()

