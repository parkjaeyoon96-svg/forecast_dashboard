"""
브랜드별 손익계산서(PL) 데이터 생성 스크립트
========================================

데이터 소스:
1. 당년 데이터 (forecast): forecast_YYYYMMDD_YYYYMM_Shop.csv
2. 전년 데이터: previous_rawdata_YYYYMM_Shop.csv (update_brand_kpi.py에서 계산된 값 사용)
3. 계획 데이터: plan_YYYYMM_전처리완료.csv

계산 로직:
- 당년 영업이익(forecast) = 직접이익(forecast) - 계획 영업비
- 전년 영업이익: update_brand_kpi.py에서 계산된 값 사용
- 계획 영업이익: 계획 데이터에서 가져오기

작성일: 2025-11
"""

import os
import sys
import json
import pandas as pd
from typing import Dict, Optional
from pathlib import Path
from path_utils import get_current_year_dir, get_current_year_file_path, get_plan_file_path, get_previous_year_file_path, extract_year_month_from_date

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "raw")
PUBLIC_DIR = os.path.join(ROOT, "public")
MASTER_DIR = os.path.join(ROOT, "Master")
DIRECT_COST_MASTER_PATH = os.path.join(MASTER_DIR, "직접비마스터.csv")

# 브랜드 코드 → 대시보드 브랜드명 매핑
BRAND_NAME_MAP = {
    'M': 'MLB',
    'I': 'MLB_KIDS',
    'X': 'DISCOVERY',
    'V': 'DUVETICA',
    'ST': 'SERGIO',
    'W': 'SUPRA'
}

def load_direct_cost_master() -> Dict[str, str]:
    """
    직접비 마스터 파일 로드: 계정명 -> 계정전환 매핑
    
    Returns:
        Dict[str, str]: 계정명 -> 계정전환 매핑 딕셔너리
    """
    if not os.path.exists(DIRECT_COST_MASTER_PATH):
        print(f"[WARNING] 직접비 마스터 파일이 없습니다: {DIRECT_COST_MASTER_PATH}")
        return {}
    
    df = pd.read_csv(DIRECT_COST_MASTER_PATH, encoding="utf-8-sig")
    
    # 컬럼 찾기
    account_col = None  # 계정명 컬럼
    conversion_col = None  # 계정전환 컬럼
    
    for col in df.columns:
        col_str = str(col).strip()
        if account_col is None and ("계정명" in col_str or "세부" in col_str):
            account_col = col
        if conversion_col is None and ("계정전환" in col_str or "대분류" in col_str):
            conversion_col = col
    
    if account_col is None or conversion_col is None:
        # 기본값: 두 번째와 세 번째 컬럼
        if len(df.columns) >= 3:
            account_col, conversion_col = df.columns[1], df.columns[2]
        elif len(df.columns) >= 2:
            account_col, conversion_col = df.columns[0], df.columns[1]
        else:
            print(f"[WARNING] 직접비 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
            return {}
    
    mapping = {}
    for _, row in df[[account_col, conversion_col]].dropna().iterrows():
        account = str(row[account_col]).strip()
        conversion = str(row[conversion_col]).strip()
        if account and conversion:
            mapping[account] = conversion
    
    print(f"[OK] 직접비 마스터 로드: {len(mapping)}개 매핑")
    return mapping


def aggregate_direct_cost_details(df: pd.DataFrame, brand_code: str, direct_cost_master: Dict[str, str]) -> Dict[str, float]:
    """
    직접비 세부 항목을 마스터 기준으로 집계
    
    Args:
        df: 데이터프레임
        brand_code: 브랜드 코드
        direct_cost_master: 직접비 마스터 매핑 딕셔너리
    
    Returns:
        Dict[str, float]: 마스터 항목별 합계 (원 단위)
    """
    result = {
        '인건비': 0.0,
        '임차관리비': 0.0,
        '물류운송비': 0.0,
        '로열티': 0.0,
        '감가상각비': 0.0,
        '기타': 0.0
    }
    
    # 직접비 마스터에 있는 컬럼 찾기
    for col in df.columns:
        if col in direct_cost_master:
            master_category = direct_cost_master[col]
            if master_category in result:
                # 해당 컬럼의 값 합산
                for val in df[col]:
                    result[master_category] += extract_numeric(val)
    
    return result


def load_direct_cost_master() -> Dict[str, str]:
    """
    직접비 마스터 파일 로드: 계정명 -> 계정전환 매핑
    
    Returns:
        Dict[str, str]: 계정명 -> 계정전환 매핑 딕셔너리
    """
    if not os.path.exists(DIRECT_COST_MASTER_PATH):
        print(f"[WARNING] 직접비 마스터 파일이 없습니다: {DIRECT_COST_MASTER_PATH}")
        return {}
    
    df = pd.read_csv(DIRECT_COST_MASTER_PATH, encoding="utf-8-sig")
    
    # 컬럼 찾기
    account_col = None  # 계정명 컬럼
    conversion_col = None  # 계정전환 컬럼
    
    for col in df.columns:
        col_str = str(col).strip()
        if account_col is None and ("계정명" in col_str or "세부" in col_str):
            account_col = col
        if conversion_col is None and ("계정전환" in col_str or "대분류" in col_str):
            conversion_col = col
    
    if account_col is None or conversion_col is None:
        # 기본값: 두 번째와 세 번째 컬럼
        if len(df.columns) >= 3:
            account_col, conversion_col = df.columns[1], df.columns[2]
        elif len(df.columns) >= 2:
            account_col, conversion_col = df.columns[0], df.columns[1]
        else:
            print(f"[WARNING] 직접비 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
            return {}
    
    mapping = {}
    for _, row in df[[account_col, conversion_col]].dropna().iterrows():
        account = str(row[account_col]).strip()
        conversion = str(row[conversion_col]).strip()
        if account and conversion:
            mapping[account] = conversion
    
    print(f"[OK] 직접비 마스터 로드: {len(mapping)}개 매핑")
    return mapping


def aggregate_direct_cost_details(df: pd.DataFrame, brand_code: str, direct_cost_master: Dict[str, str]) -> Dict[str, float]:
    """
    직접비 세부 항목을 마스터 기준으로 집계
    
    Args:
        df: 데이터프레임 (브랜드 필터링된 데이터)
        brand_code: 브랜드 코드
        direct_cost_master: 직접비 마스터 매핑 딕셔너리
    
    Returns:
        Dict[str, float]: 마스터 항목별 합계 (원 단위)
    """
    result = {
        '인건비': 0.0,
        '임차관리비': 0.0,
        '물류운송비': 0.0,
        '로열티': 0.0,
        '감가상각비': 0.0,
        '기타': 0.0
    }
    
    # 직접비 마스터에 있는 컬럼 찾기 (부분 매칭 지원)
    # 예: "지급임차료_매장(고정) 등" -> "지급임차료_매장(고정)" 매핑
    for col in df.columns:
        matched = False
        # 정확한 매칭 먼저 시도
        if col in direct_cost_master:
            master_category = direct_cost_master[col]
            if master_category in result:
                # 해당 컬럼의 값 합산
                for val in df[col]:
                    result[master_category] += extract_numeric(val)
                matched = True
        else:
            # 부분 매칭 시도 (예: "지급임차료_매장(고정) 등" -> "지급임차료_매장(고정)")
            for master_col, master_category in direct_cost_master.items():
                # 마스터 컬럼명이 데이터 컬럼명에 포함되어 있는지 확인
                if master_col in col:
                    if master_category in result:
                        # 해당 컬럼의 값 합산
                        for val in df[col]:
                            result[master_category] += extract_numeric(val)
                        matched = True
                        break  # 매칭되면 다음 컬럼으로
    
    return result


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

def load_forecast_data(date_str: str) -> Optional[pd.DataFrame]:
    """당년 forecast 데이터 로드"""
    year_month = extract_year_month_from_date(date_str)
    current_year_dir = get_current_year_dir(date_str)
    
    # forecast 파일 찾기: forecast_YYYYMMDD_YYYYMM_Shop.csv
    forecast_filename = f"forecast_{date_str}_{year_month}_Shop.csv"
    forecast_path = os.path.join(current_year_dir, forecast_filename)
    
    # 파일이 없으면 전처리완료 파일 찾기
    if not os.path.exists(forecast_path):
        # 전처리완료 파일 패턴: forecast_YYYYMMDD_YYYYMM_전처리완료.csv 또는 ke30 파일
        forecast_processed = f"forecast_{date_str}_{year_month}_전처리완료.csv"
        forecast_path = os.path.join(current_year_dir, forecast_processed)
        
        if not os.path.exists(forecast_path):
            # ke30 파일 사용
            ke30_filename = f"ke30_{date_str}_{year_month}_Shop.csv"
            forecast_path = os.path.join(current_year_dir, ke30_filename)
            
            if not os.path.exists(forecast_path):
                ke30_processed = f"ke30_{date_str}_{year_month}_전처리완료.csv"
                forecast_path = os.path.join(current_year_dir, ke30_processed)
    
    if not os.path.exists(forecast_path):
        print(f"  [WARNING] Forecast 파일을 찾을 수 없습니다: {current_year_dir}")
        return None
    
    print(f"  [LOAD] Forecast 데이터: {os.path.basename(forecast_path)}")
    df = pd.read_csv(forecast_path, encoding='utf-8-sig')
    return df

def load_plan_data(year_month: str) -> Optional[pd.DataFrame]:
    """계획 데이터 로드"""
    plan_path = get_plan_file_path(year_month)  # 기본값: plan_YYYYMM_전처리완료.csv
    
    if not os.path.exists(plan_path):
        print(f"  [WARNING] 계획 데이터 파일을 찾을 수 없습니다: {plan_path}")
        return None
    
    print(f"  [LOAD] 계획 데이터: {os.path.basename(plan_path)}")
    df = pd.read_csv(plan_path, encoding='utf-8-sig')
    return df

def load_previous_year_kpi(year_month: str) -> Optional[pd.DataFrame]:
    """전년도 데이터 로드"""
    # 전년도 파일: previous_rawdata_YYYYMM_Shop.csv
    previous_filename = f"previous_rawdata_{year_month}_Shop.csv"
    previous_path = get_previous_year_file_path(year_month, previous_filename)
    
    if not os.path.exists(previous_path):
        print(f"  [WARNING] 전년도 데이터 파일을 찾을 수 없습니다: {previous_path}")
        return None
    
    print(f"  [LOAD] 전년도 데이터: {os.path.basename(previous_path)}")
    df = pd.read_csv(previous_path, encoding='utf-8-sig')
    return df

def get_plan_operating_expense(df_plan: pd.DataFrame, brand_code: str) -> float:
    """계획 데이터에서 브랜드별 영업비 추출 (pivot 후 구조: 채널='내수합계' 행의 '영업비' 컬럼)"""
    if df_plan is None or df_plan.empty:
        return 0.0
    
    # pivot 후 구조: 브랜드, Version, 채널 컬럼이 있고, 지표들이 컬럼으로 변환됨
    # 채널='내수합계'인 행에서 '영업비' 컬럼 값 추출
    
    # 브랜드 필터링
    if '브랜드' not in df_plan.columns:
        return 0.0
    
    brand_df = df_plan[df_plan['브랜드'].astype(str).str.strip() == brand_code]
    
    if brand_df.empty:
        return 0.0
    
    # 채널='내수합계'인 행 찾기
    if '채널' in brand_df.columns:
        내수합계_df = brand_df[brand_df['채널'].astype(str).str.strip() == '내수합계']
    elif '채널명' in brand_df.columns:
        내수합계_df = brand_df[brand_df['채널명'].astype(str).str.strip() == '내수합계']
    else:
        return 0.0
    
    if 내수합계_df.empty:
        return 0.0
    
    # '영업비' 컬럼 찾기
    영업비 = 0.0
    for col in 내수합계_df.columns:
        if col == '영업비':
            값 = extract_numeric(내수합계_df[col].iloc[0] if len(내수합계_df) > 0 else 0)
            영업비 = 값
            break
    
    return 영업비

def create_brand_pl_data(date_str: str) -> Dict:
    """
    브랜드별 손익계산서 데이터 생성
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열
        
    Returns:
        brandPLData 딕셔너리
    """
    print("\n[brandPLData 생성] 브랜드별 손익계산서 데이터 생성 중...")
    
    year_month = extract_year_month_from_date(date_str)
    brand_pl_data = {}
    
    # 직접비 마스터 로드
    direct_cost_master = load_direct_cost_master()
    
    # 1. 데이터 로드
    df_forecast = load_forecast_data(date_str)
    df_plan = load_plan_data(year_month)
    df_previous = load_previous_year_kpi(year_month)
    
    # 브랜드 목록 (M, I, X, V, ST, W)
    brand_codes = ['M', 'I', 'X', 'V', 'ST', 'W']
    
    for brand_code in brand_codes:
        brand_name = BRAND_NAME_MAP.get(brand_code, brand_code)
        print(f"\n  [브랜드] {brand_name} ({brand_code})")
        
        # 초기값
        pl_data = {
            'tagRevenue': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'revenue': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'discountRate': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'cog': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'grossProfit': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'directCost': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'directCostDetail': {
                '인건비': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
                '임차관리비': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
                '물류운송비': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
                '로열티': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
                '감가상각비': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
                '기타': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0}
            },
            'directProfit': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'operatingExpense': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
            'opProfit': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0}
        }
        
        # 2. 당년 데이터 (forecast) 집계
        if df_forecast is not None and not df_forecast.empty:
            brand_col = None
            tag_col = None
            sales_col = None
            cogs_col = None
            direct_cost_col = None
            direct_profit_col = None
            
            # 컬럼 찾기
            출고매출_col = None  # 매출총이익 계산용
            gross_profit_col = None  # 매출총이익 직접 컬럼
            for col in df_forecast.columns:
                col_str = str(col)
                if '브랜드' in col_str:
                    brand_col = col
                elif 'TAG가' in col_str or 'TAG매출' in col_str or ('판매금액' in col_str and 'TAG' in col_str):
                    if '합계' in col_str:
                        tag_col = col
                elif '실판매액' in col_str and ('합계' in col_str or col_str.strip() == '실판매액'):
                    # V+ 우선
                    if '(V+)' in col_str:
                        sales_col = col
                    elif not sales_col:
                        sales_col = col
                elif '출고매출' in col_str or ('출고' in col_str and '매출' in col_str):
                    # 출고매출액 컬럼 찾기 (매출총이익 계산용)
                    if 'Actual' in col_str or '(V-)' in col_str:
                        출고매출_col = col
                    elif not 출고매출_col:
                        출고매출_col = col
                elif '매출원가' in col_str:
                    # 우선순위: 평가감환입반영 > Actual > 기타
                    if '평가감환입반영' in col_str or '평가감환입' in col_str:
                        cogs_col = col
                    elif 'Actual' in col_str and not cogs_col:
                        cogs_col = col
                    elif not cogs_col:
                        cogs_col = col
                elif '매출총이익' in col_str:
                    gross_profit_col = col  # 매출총이익 직접 컬럼
                elif '직접비' in col_str and '합계' in col_str:
                    direct_cost_col = col
                elif '직접이익' in col_str:
                    direct_profit_col = col
            
            if brand_col:
                brand_df = df_forecast[df_forecast[brand_col].astype(str).str.strip() == brand_code]
                
                if not brand_df.empty:
                    # forecast 값 집계 (원 단위) - 미지정 채널 포함
                    tag_revenue_sum = 0.0
                    revenue_sum = 0.0
                    출고매출_sum = 0.0  # 출고매출(V-) 합산 (매출총이익 계산용)
                    cogs_sum = 0.0
                    gross_profit_sum = 0.0  # 매출총이익 합산
                    direct_cost_sum = 0.0
                    direct_profit_sum = 0.0
                    인건비_sum = 0.0
                    임차관리비_sum = 0.0
                    물류운송비_sum = 0.0
                    로열티_sum = 0.0
                    감가상각비_sum = 0.0
                    기타_sum = 0.0
                    
                    if tag_col:
                        for val in brand_df[tag_col]:
                            tag_revenue_sum += extract_numeric(val)
                    
                    if sales_col:
                        for val in brand_df[sales_col]:
                            revenue_sum += extract_numeric(val)
                    
                    # 출고매출(V-) 합산 (매출총이익 계산용)
                    if 출고매출_col:
                        for val in brand_df[출고매출_col]:
                            출고매출_sum += extract_numeric(val)
                    
                    if cogs_col:
                        print(f"    [매출원가-forecast] 컬럼: {cogs_col}")
                        for val in brand_df[cogs_col]:
                            cogs_sum += extract_numeric(val)
                        print(f"    [매출원가-forecast] 합계: {cogs_sum:,.0f}원 ({cogs_sum/100000000:.2f}억원)")
                    else:
                        print(f"    [WARNING] 매출원가 컬럼을 찾을 수 없습니다.")
                    
                    # 매출총이익 컬럼에서 직접 가져오기
                    if gross_profit_col:
                        print(f"    [매출총이익-forecast] 컬럼: {gross_profit_col}")
                        for val in brand_df[gross_profit_col]:
                            gross_profit_sum += extract_numeric(val)
                        print(f"    [매출총이익-forecast] 합계: {gross_profit_sum:,.0f}원 ({gross_profit_sum/100000000:.2f}억원)")
                    
                    if direct_cost_col:
                        for val in brand_df[direct_cost_col]:
                            direct_cost_sum += extract_numeric(val)
                    
                    if direct_profit_col:
                        for val in brand_df[direct_profit_col]:
                            direct_profit_sum += extract_numeric(val)
                    
                    # 직접비 세부 항목 집계 (직접비마스터 사용)
                    direct_cost_details = aggregate_direct_cost_details(brand_df, brand_code, direct_cost_master)
                    인건비_sum = direct_cost_details['인건비']
                    임차관리비_sum = direct_cost_details['임차관리비']
                    물류운송비_sum = direct_cost_details['물류운송비']
                    로열티_sum = direct_cost_details['로열티']
                    감가상각비_sum = direct_cost_details['감가상각비']
                    기타_sum = direct_cost_details['기타']
                    
                    # 억원 단위로 변환하여 저장
                    pl_data['tagRevenue']['forecast'] = round(tag_revenue_sum / 100000000, 2)
                    pl_data['revenue']['forecast'] = round(revenue_sum / 100000000, 2)
                    pl_data['cog']['forecast'] = round(cogs_sum / 100000000, 2)
                    
                    # 매출총이익: 컬럼에서 직접 가져오기 (없으면 출고매출(V-) - 매출원가로 계산)
                    if gross_profit_col and gross_profit_sum > 0:
                        pl_data['grossProfit']['forecast'] = round(gross_profit_sum / 100000000, 2)
                    elif 출고매출_col and 출고매출_sum > 0:
                        # 폴백: 출고매출(V-) - 매출원가
                        출고매출_억원 = round(출고매출_sum / 100000000, 2)
                        pl_data['grossProfit']['forecast'] = round(출고매출_억원 - pl_data['cog']['forecast'], 2)
                        print(f"    [매출총이익-forecast] 컬럼이 없어 계산값 사용 (출고매출 - 매출원가): {pl_data['grossProfit']['forecast']:.2f}억원")
                    else:
                        # 최종 폴백: 실판매출 - 매출원가
                        pl_data['grossProfit']['forecast'] = round(
                            pl_data['revenue']['forecast'] - pl_data['cog']['forecast'], 2
                        )
                        print(f"    [WARNING] 매출총이익 계산에 필요한 컬럼이 없어 실판매출-매출원가 계산값 사용: {pl_data['grossProfit']['forecast']:.2f}억원")
                    
                    pl_data['directCost']['forecast'] = round(direct_cost_sum / 100000000, 2)
                    # 직접이익 = 매출총이익 - 직접비 (직접 계산)
                    pl_data['directProfit']['forecast'] = round(
                        pl_data['grossProfit']['forecast'] - pl_data['directCost']['forecast'], 2
                    )
                    print(f"    [직접이익-forecast] 계산값 사용 (매출총이익 - 직접비): {pl_data['directProfit']['forecast']:.2f}억원")
                    
                    # 직접비 세부 항목 (억원 단위)
                    pl_data['directCostDetail']['인건비']['forecast'] = round(인건비_sum / 100000000, 2)
                    pl_data['directCostDetail']['임차관리비']['forecast'] = round(임차관리비_sum / 100000000, 2)
                    pl_data['directCostDetail']['물류운송비']['forecast'] = round(물류운송비_sum / 100000000, 2)
                    pl_data['directCostDetail']['로열티']['forecast'] = round(로열티_sum / 100000000, 2)
                    pl_data['directCostDetail']['감가상각비']['forecast'] = round(감가상각비_sum / 100000000, 2)
                    pl_data['directCostDetail']['기타']['forecast'] = round(기타_sum / 100000000, 2)
                    
                    # 할인율 계산: 1 - 실판매출 / TAG매출
                    if tag_revenue_sum > 0:
                        pl_data['discountRate']['forecast'] = round((1 - (revenue_sum / tag_revenue_sum)) * 100, 1)
                    else:
                        pl_data['discountRate']['forecast'] = 0.0
        
        # 3. 계획 데이터 집계
        if df_plan is not None and not df_plan.empty:
            # 계획 영업비 추출
            plan_op_expense = get_plan_operating_expense(df_plan, brand_code)
            plan_op_expense_억원 = round(plan_op_expense / 100000000, 2)
            
            # 영업비 저장 (목표 = 월말예상 = 계획 영업비)
            pl_data['operatingExpense']['target'] = plan_op_expense_억원
            pl_data['operatingExpense']['forecast'] = plan_op_expense_억원
            
            # 계획 데이터에서 다른 값들 추출 (pivot 후 구조: 채널='내수합계' 행에서 지표 컬럼 값 추출)
            if '브랜드' in df_plan.columns:
                plan_brand_df = df_plan[df_plan['브랜드'].astype(str).str.strip() == brand_code]
                
                # 채널='내수합계'인 행 찾기
                채널_col = '채널' if '채널' in plan_brand_df.columns else '채널명'
                if 채널_col in plan_brand_df.columns:
                    내수합계_df = plan_brand_df[plan_brand_df[채널_col].astype(str).str.strip() == '내수합계']
                    
                    if not 내수합계_df.empty:
                        # 각 지표 컬럼에서 값 추출
                        row = 내수합계_df.iloc[0]
                        
                        # TAG매출 (TAG가 [v+] 또는 TAG가)
                        tag_revenue_target = 0.0
                        for col in 내수합계_df.columns:
                            col_str = str(col)
                            if ('TAG가 [v+]' in col_str or 'TAG가' == col_str.strip() or 
                                ('TAG' in col_str and '매출' in col_str)):
                                tag_revenue_target = extract_numeric(row[col])
                                pl_data['tagRevenue']['target'] = round(tag_revenue_target / 100000000, 2)
                                break
                        
                        # 실판매액 [v+]
                        revenue_target = 0.0
                        for col in 내수합계_df.columns:
                            if '실판매액 [v+]' in str(col) or (col == '실판매액' and '[v+]' not in str(row)):
                                revenue_target = extract_numeric(row[col])
                                pl_data['revenue']['target'] = round(revenue_target / 100000000, 2)
                                break
                        
                        # 할인율 계산: 1 - 실판매출 / TAG매출
                        if tag_revenue_target > 0:
                            pl_data['discountRate']['target'] = round((1 - (revenue_target / tag_revenue_target)) * 100, 1)
                        else:
                            pl_data['discountRate']['target'] = 0.0
                        
                        # 매출원가 (매출원가(환입후) 필드 사용)
                        for col in 내수합계_df.columns:
                            col_str = str(col).strip()
                            if col_str == '매출원가(환입후)' or ('매출원가(환입후)' in col_str):
                                값 = extract_numeric(row[col])
                                pl_data['cog']['target'] = round(값 / 100000000, 2)
                                print(f"    [매출원가-target] 컬럼: {col}, 값: {값:,.0f}원 ({pl_data['cog']['target']:.2f}억원)")
                                break
                            # 폴백: 기존 '매출원가' 컬럼도 허용
                            elif col_str == '매출원가' or ('매출원가' in col_str and '원가' in col_str and '환입후' not in col_str):
                                값 = extract_numeric(row[col])
                                pl_data['cog']['target'] = round(값 / 100000000, 2)
                                print(f"    [매출원가-target] 컬럼: {col}, 값: {값:,.0f}원 ({pl_data['cog']['target']:.2f}억원)")
                                break
                        
                        # 매출총이익
                        for col in 내수합계_df.columns:
                            if col == '매출총이익':
                                값 = extract_numeric(row[col])
                                pl_data['grossProfit']['target'] = round(값 / 100000000, 2)
                                break
                        
                        # 직접비
                        for col in 내수합계_df.columns:
                            if col == '직접비' or (col == '직접비 합계'):
                                값 = extract_numeric(row[col])
                                pl_data['directCost']['target'] = round(값 / 100000000, 2)
                                break
                        
                        # 직접비 세부 항목 (직접비 마스터 사용)
                        # 내수합계_df를 DataFrame으로 변환하여 aggregate_direct_cost_details 함수 사용
                        direct_cost_details = aggregate_direct_cost_details(내수합계_df, brand_code, direct_cost_master)
                        인건비_sum = direct_cost_details['인건비']
                        임차관리비_sum = direct_cost_details['임차관리비']
                        물류운송비_sum = direct_cost_details['물류운송비']
                        로열티_sum = direct_cost_details['로열티']
                        감가상각비_sum = direct_cost_details['감가상각비']
                        기타_sum = direct_cost_details['기타']
                        
                        # 억원 단위로 변환하여 저장
                        pl_data['directCostDetail']['인건비']['target'] = round(인건비_sum / 100000000, 2)
                        pl_data['directCostDetail']['임차관리비']['target'] = round(임차관리비_sum / 100000000, 2)
                        pl_data['directCostDetail']['물류운송비']['target'] = round(물류운송비_sum / 100000000, 2)
                        pl_data['directCostDetail']['로열티']['target'] = round(로열티_sum / 100000000, 2)
                        pl_data['directCostDetail']['감가상각비']['target'] = round(감가상각비_sum / 100000000, 2)
                        pl_data['directCostDetail']['기타']['target'] = round(기타_sum / 100000000, 2)
                        
                        # 직접이익 = 매출총이익 - 직접비 (직접 계산)
                        if pl_data['grossProfit']['target'] > 0 and pl_data['directCost']['target'] > 0:
                            pl_data['directProfit']['target'] = round(
                                pl_data['grossProfit']['target'] - pl_data['directCost']['target'], 2
                            )
                            print(f"    [직접이익-target] 계산값 사용 (매출총이익 - 직접비): {pl_data['directProfit']['target']:.2f}억원")
                        else:
                            # 폴백: 계획 데이터에서 직접이익 컬럼 찾기
                            for col in 내수합계_df.columns:
                                if col == '직접이익':
                                    값 = extract_numeric(row[col])
                                    pl_data['directProfit']['target'] = round(값 / 100000000, 2)
                                    print(f"    [직접이익-target] 컬럼에서 가져옴: {pl_data['directProfit']['target']:.2f}억원")
                                    break
                        
                        # 영업이익
                        for col in 내수합계_df.columns:
                            if col == '영업이익':
                                값 = extract_numeric(row[col])
                                pl_data['opProfit']['target'] = round(값 / 100000000, 2)
                                break
            
            # 당년 영업이익(forecast) = 직접이익(forecast) - 계획 영업비
            pl_data['opProfit']['forecast'] = round(
                pl_data['directProfit']['forecast'] - plan_op_expense_억원, 2
            )
            print(f"    당년 영업이익: {pl_data['directProfit']['forecast']:.2f} - {plan_op_expense_억원:.2f} = {pl_data['opProfit']['forecast']:.2f}억원")
        
        # 4. 전년 데이터 집계
        if df_previous is not None and not df_previous.empty:
            brand_col = None
            tag_col = None
            sales_col = None
            cogs_col = None
            direct_cost_col = None
            direct_profit_col = None
            op_expense_col = None
            
            # 컬럼 찾기
            출고매출_col_prev = None  # 전년 출고매출(V-) 컬럼 (매출총이익 계산용)
            gross_profit_col_prev = None  # 전년 매출총이익 컬럼
            for col in df_previous.columns:
                col_str = str(col)
                if '브랜드코드' in col_str or ('브랜드' in col_str and '코드' in col_str):
                    brand_col = col
                elif 'TAG매출' in col_str or 'TAG매출액' in col_str or ('TAG' in col_str and '매출' in col_str):
                    tag_col = col
                elif '실매출액' in col_str or ('실판매액' in col_str and '전년' not in col_str):
                    # ★★★ 실매출액이 부가세 포함(V+)이므로 우선 사용 ★★★
                    if sales_col is None:
                        sales_col = col
                    # '부가세제외'가 없는 실매출액이 부가세 포함이므로 우선 사용
                    if '부가세제외' not in col_str and '실매출액' in col_str:
                        sales_col = col
                elif '출고매출' in col_str or ('출고' in col_str and '매출' in col_str) or '부가세제외 실판매액' in col_str:
                    # 출고매출(V-) 또는 부가세제외 실판매액 (전년 데이터)
                    출고매출_col_prev = col
                elif '매출원가' in col_str:
                    # 우선순위: 환입후매출원가+평가감 > 기타
                    if '환입후매출원가' in col_str or '평가감' in col_str:
                        cogs_col = col
                    elif not cogs_col:
                        cogs_col = col
                elif '매출총이익' in col_str:
                    gross_profit_col_prev = col
                elif '직접비' in col_str and '합계' in col_str:
                    direct_cost_col = col
                elif '직접이익' in col_str:
                    direct_profit_col = col
                elif '영업비' in col_str:
                    op_expense_col = col
            
            if brand_col:
                brand_df = df_previous[df_previous[brand_col].astype(str).str.strip() == brand_code].copy()
                
                if not brand_df.empty:
                    # 공통 채널 분리 (영업비만 공통 채널에서 추출, 매출원가는 공통 포함하여 전체 합산)
                    채널_col = None
                    for col in brand_df.columns:
                        if '채널' in str(col):
                            채널_col = col
                            break
                    
                    if 채널_col:
                        prev_brand_df = brand_df[brand_df[채널_col].astype(str).str.strip() != '공통']
                        op_expense_df = brand_df[brand_df[채널_col].astype(str).str.strip() == '공통']
                    else:
                        prev_brand_df = brand_df
                        op_expense_df = pd.DataFrame()
                    
                    # 전년 값 집계 (원 단위)
                    # 매출원가는 공통 채널 포함 (전체 brand_df 사용)
                    tag_revenue_sum = 0.0
                    revenue_sum = 0.0
                    출고매출_sum_prev = 0.0  # 전년 출고매출(V-) 합산 (매출총이익 계산용)
                    cogs_sum = 0.0
                    gross_profit_sum_prev = 0.0  # 전년 매출총이익 합산
                    direct_cost_sum = 0.0
                    direct_profit_sum = 0.0
                    
                    if tag_col:
                        for val in prev_brand_df[tag_col]:
                            tag_revenue_sum += extract_numeric(val)
                    
                    # ★★★ 전년 실판금액(V+): 실매출액 컬럼 사용, 공통 채널 포함하여 전체 합산 ★★★
                    if sales_col:
                        # 공통 채널 포함하여 전체 합산 (실판 V+ = 부가세 포함 실매출액)
                        # brand_df는 공통 채널 포함한 전체 데이터
                        revenue_sum = 0.0
                        for val in brand_df[sales_col]:
                            revenue_sum += extract_numeric(val)
                        print(f"    [실판매출(V+)-prev] 컬럼: {sales_col}")
                        print(f"    [실판매출(V+)-prev] 브랜드 데이터 행 수: {len(brand_df)}")
                        print(f"    [실판매출(V+)-prev] 합계 (공통 포함): {revenue_sum:,.0f}원 ({revenue_sum/100000000:.2f}억원)")
                    else:
                        print(f"    [WARNING] 전년 실매출액 컬럼을 찾을 수 없습니다.")
                    
                    # 출고매출(V-) 또는 부가세제외 실판매액 합산 (공통 채널 포함하여 전체 합산)
                    if 출고매출_col_prev:
                        for val in brand_df[출고매출_col_prev]:
                            출고매출_sum_prev += extract_numeric(val)
                    
                    if cogs_col:
                        print(f"    [매출원가-prev] 컬럼: {cogs_col}")
                        # 매출원가는 공통 채널 포함하여 전체 합산
                        for val in brand_df[cogs_col]:
                            cogs_sum += extract_numeric(val)
                        print(f"    [매출원가-prev] 합계 (공통 포함): {cogs_sum:,.0f}원 ({cogs_sum/100000000:.2f}억원)")
                    else:
                        print(f"    [WARNING] 전년 매출원가 컬럼을 찾을 수 없습니다.")
                    
                    # 매출총이익 컬럼에서 직접 가져오기 (공통 채널 포함하여 전체 합산)
                    if gross_profit_col_prev:
                        print(f"    [매출총이익-prev] 컬럼: {gross_profit_col_prev}")
                        for val in brand_df[gross_profit_col_prev]:
                            gross_profit_sum_prev += extract_numeric(val)
                        print(f"    [매출총이익-prev] 합계 (공통 포함): {gross_profit_sum_prev:,.0f}원 ({gross_profit_sum_prev/100000000:.2f}억원)")
                    
                    if direct_cost_col:
                        for val in prev_brand_df[direct_cost_col]:
                            direct_cost_sum += extract_numeric(val)
                    
                    if direct_profit_col:
                        for val in prev_brand_df[direct_profit_col]:
                            direct_profit_sum += extract_numeric(val)
                    
                    # 직접비 세부 항목 집계 (직접비마스터 사용)
                    direct_cost_details = aggregate_direct_cost_details(prev_brand_df, brand_code, direct_cost_master)
                    인건비_sum = direct_cost_details['인건비']
                    임차관리비_sum = direct_cost_details['임차관리비']
                    물류운송비_sum = direct_cost_details['물류운송비']
                    로열티_sum = direct_cost_details['로열티']
                    감가상각비_sum = direct_cost_details['감가상각비']
                    기타_sum = direct_cost_details['기타']
                    
                    # 억원 단위로 변환하여 저장
                    pl_data['tagRevenue']['prev'] = round(tag_revenue_sum / 100000000, 2)
                    pl_data['revenue']['prev'] = round(revenue_sum / 100000000, 2)
                    pl_data['cog']['prev'] = round(cogs_sum / 100000000, 2)
                    
                    # 매출총이익: 컬럼에서 직접 가져오기 (없으면 출고매출(V-) - 매출원가로 계산)
                    if gross_profit_col_prev and gross_profit_sum_prev > 0:
                        pl_data['grossProfit']['prev'] = round(gross_profit_sum_prev / 100000000, 2)
                    elif 출고매출_col_prev and 출고매출_sum_prev > 0:
                        # 폴백: 출고매출(V-) - 매출원가
                        출고매출_억원_prev = round(출고매출_sum_prev / 100000000, 2)
                        pl_data['grossProfit']['prev'] = round(출고매출_억원_prev - pl_data['cog']['prev'], 2)
                        print(f"    [매출총이익-prev] 컬럼이 없어 계산값 사용 (출고매출 - 매출원가): {pl_data['grossProfit']['prev']:.2f}억원")
                    else:
                        # 최종 폴백: 실판매출 - 매출원가
                        pl_data['grossProfit']['prev'] = round(
                            pl_data['revenue']['prev'] - pl_data['cog']['prev'], 2
                        )
                        print(f"    [WARNING] 전년 매출총이익 계산에 필요한 컬럼이 없어 실판매출-매출원가 계산값 사용: {pl_data['grossProfit']['prev']:.2f}억원")
                    
                    pl_data['directCost']['prev'] = round(direct_cost_sum / 100000000, 2)
                    # 직접이익 = 매출총이익 - 직접비 (직접 계산)
                    pl_data['directProfit']['prev'] = round(
                        pl_data['grossProfit']['prev'] - pl_data['directCost']['prev'], 2
                    )
                    print(f"    [직접이익-prev] 계산값 사용 (매출총이익 - 직접비): {pl_data['directProfit']['prev']:.2f}억원")
                    
                    # 직접비 세부 항목 (억원 단위)
                    pl_data['directCostDetail']['인건비']['prev'] = round(인건비_sum / 100000000, 2)
                    pl_data['directCostDetail']['임차관리비']['prev'] = round(임차관리비_sum / 100000000, 2)
                    pl_data['directCostDetail']['물류운송비']['prev'] = round(물류운송비_sum / 100000000, 2)
                    pl_data['directCostDetail']['로열티']['prev'] = round(로열티_sum / 100000000, 2)
                    pl_data['directCostDetail']['감가상각비']['prev'] = round(감가상각비_sum / 100000000, 2)
                    pl_data['directCostDetail']['기타']['prev'] = round(기타_sum / 100000000, 2)
                    
                    # 할인율 계산: 1 - 실판매출 / TAG매출
                    if tag_revenue_sum > 0:
                        pl_data['discountRate']['prev'] = round((1 - (revenue_sum / tag_revenue_sum)) * 100, 1)
                    else:
                        pl_data['discountRate']['prev'] = 0.0
                    
                    # 전년 영업비 (공통 채널)
                    prev_op_expense = 0.0
                    
                    # 영업비 컬럼 재확인 (brand_df에서 다시 검색)
                    if not op_expense_col:
                        for col in brand_df.columns:
                            col_str = str(col).strip()
                            if '영업비' in col_str:
                                op_expense_col = col
                                print(f"    [영업비-prev] 컬럼 재검색 성공: {op_expense_col}")
                                break
                    
                    # 공통 채널 데이터 재확인
                    if 채널_col and op_expense_col:
                        op_expense_df = brand_df[brand_df[채널_col].astype(str).str.strip() == '공통']
                    
                    if op_expense_col and not op_expense_df.empty:
                        prev_op_expense = op_expense_df[op_expense_col].sum()
                        print(f"    [영업비-prev] 컬럼: {op_expense_col}")
                        print(f"    [영업비-prev] 공통 채널 데이터 행 수: {len(op_expense_df)}")
                        print(f"    [영업비-prev] 합계: {prev_op_expense:,.0f}원 ({prev_op_expense/100000000:.2f}억원)")
                    else:
                        if not op_expense_col:
                            print(f"    [WARNING] 전년 영업비 컬럼을 찾을 수 없습니다.")
                            print(f"    [DEBUG] 사용 가능한 컬럼: {list(brand_df.columns)}")
                        if op_expense_df.empty:
                            print(f"    [WARNING] 전년 영업비 공통 채널 데이터가 없습니다.")
                            # 공통 채널이 없으면 전체 브랜드 데이터에서 영업비 합산 시도
                            if op_expense_col and not brand_df.empty:
                                prev_op_expense = brand_df[op_expense_col].sum()
                                if prev_op_expense > 0:
                                    print(f"    [영업비-prev] 공통 채널 없음, 전체 브랜드 데이터에서 합산: {prev_op_expense:,.0f}원 ({prev_op_expense/100000000:.2f}억원)")
                    
                    # 전년 영업비 저장 (억원 단위)
                    pl_data['operatingExpense']['prev'] = round(prev_op_expense / 100000000, 2)
                    
                    # 전년 영업이익 = 전년 직접이익 - 전년 영업비
                    pl_data['opProfit']['prev'] = round(
                        pl_data['directProfit']['prev'] - pl_data['operatingExpense']['prev'], 2
                    )
        
        # 5. YOY 및 Achievement 계산
        # YOY: (forecast / prev) * 100
        if pl_data['tagRevenue']['prev'] > 0:
            pl_data['tagRevenue']['yoy'] = round((pl_data['tagRevenue']['forecast'] / pl_data['tagRevenue']['prev']) * 100)
        if pl_data['revenue']['prev'] > 0:
            pl_data['revenue']['yoy'] = round((pl_data['revenue']['forecast'] / pl_data['revenue']['prev']) * 100)
        if pl_data['cog']['prev'] > 0:
            pl_data['cog']['yoy'] = round((pl_data['cog']['forecast'] / pl_data['cog']['prev']) * 100)
        if pl_data['grossProfit']['prev'] > 0:
            pl_data['grossProfit']['yoy'] = round((pl_data['grossProfit']['forecast'] / pl_data['grossProfit']['prev']) * 100)
        if pl_data['directCost']['prev'] > 0:
            pl_data['directCost']['yoy'] = round((pl_data['directCost']['forecast'] / pl_data['directCost']['prev']) * 100)
        if pl_data['directProfit']['prev'] > 0:
            pl_data['directProfit']['yoy'] = round((pl_data['directProfit']['forecast'] / pl_data['directProfit']['prev']) * 100)
        if pl_data['operatingExpense']['prev'] > 0:
            pl_data['operatingExpense']['yoy'] = round((pl_data['operatingExpense']['forecast'] / pl_data['operatingExpense']['prev']) * 100)
        if pl_data['opProfit']['prev'] > 0:
            pl_data['opProfit']['yoy'] = round((pl_data['opProfit']['forecast'] / pl_data['opProfit']['prev']) * 100)
        
        # 직접비 세부 항목 YOY
        for detail_name in pl_data['directCostDetail'].keys():
            if pl_data['directCostDetail'][detail_name]['prev'] > 0:
                pl_data['directCostDetail'][detail_name]['yoy'] = round(
                    (pl_data['directCostDetail'][detail_name]['forecast'] / pl_data['directCostDetail'][detail_name]['prev']) * 100
                )
        
        # Achievement: (forecast / target) * 100
        if pl_data['tagRevenue']['target'] > 0:
            pl_data['tagRevenue']['achievement'] = round((pl_data['tagRevenue']['forecast'] / pl_data['tagRevenue']['target']) * 100)
        if pl_data['revenue']['target'] > 0:
            pl_data['revenue']['achievement'] = round((pl_data['revenue']['forecast'] / pl_data['revenue']['target']) * 100)
        if pl_data['cog']['target'] > 0:
            pl_data['cog']['achievement'] = round((pl_data['cog']['forecast'] / pl_data['cog']['target']) * 100)
        if pl_data['grossProfit']['target'] > 0:
            pl_data['grossProfit']['achievement'] = round((pl_data['grossProfit']['forecast'] / pl_data['grossProfit']['target']) * 100)
        if pl_data['directCost']['target'] > 0:
            pl_data['directCost']['achievement'] = round((pl_data['directCost']['forecast'] / pl_data['directCost']['target']) * 100)
        if pl_data['directProfit']['target'] > 0:
            pl_data['directProfit']['achievement'] = round((pl_data['directProfit']['forecast'] / pl_data['directProfit']['target']) * 100)
        if pl_data['operatingExpense']['target'] > 0:
            pl_data['operatingExpense']['achievement'] = round((pl_data['operatingExpense']['forecast'] / pl_data['operatingExpense']['target']) * 100)
        if pl_data['opProfit']['target'] > 0:
            pl_data['opProfit']['achievement'] = round((pl_data['opProfit']['forecast'] / pl_data['opProfit']['target']) * 100)
        
        # 직접비 세부 항목 Achievement
        for detail_name in pl_data['directCostDetail'].keys():
            if pl_data['directCostDetail'][detail_name]['target'] > 0:
                pl_data['directCostDetail'][detail_name]['achievement'] = round(
                    (pl_data['directCostDetail'][detail_name]['forecast'] / pl_data['directCostDetail'][detail_name]['target']) * 100
                )
        
        brand_pl_data[brand_name] = pl_data
        print(f"    [OK] {brand_name} PL 데이터 생성 완료")
    
    print(f"\n[OK] brandPLData 생성 완료: {len(brand_pl_data)}개 브랜드")
    return brand_pl_data


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='브랜드별 손익계산서 데이터 생성')
    parser.add_argument('date', help='YYYYMMDD 형식의 날짜 (예: 20251124)')
    
    args = parser.parse_args()
    date_str = args.date
    
    brand_pl_data = create_brand_pl_data(date_str)
    
    # JSON 파일 저장 (날짜별 폴더에만 저장)
    json_dir = os.path.join(PUBLIC_DIR, "data", date_str)
    os.makedirs(json_dir, exist_ok=True)
    json_path = os.path.join(json_dir, "brand_pl.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(brand_pl_data, f, ensure_ascii=False, indent=2)
    print(f"  [OK] JSON 저장: {json_path}")
    
    return brand_pl_data


if __name__ == "__main__":
    main()

