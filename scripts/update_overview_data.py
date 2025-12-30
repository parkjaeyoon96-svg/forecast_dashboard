"""
전체현황(Overview) 데이터 업데이트 스크립트
==========================================

데이터 소스 (채널별 전처리 파일):
1. 계획: plan_YYYYMM_전처리완료.csv
2. 당년 (월말예상): forecast_YYYYMMDD_YYYYMM_Shop.csv
3. KPI 현시점: ke30_YYYYMMDD_YYYYMM_Shop.csv
4. 전년: previous_rawdata_YYYYMM_Shop.csv

생성 데이터:
1. D.by_brand: 브랜드별 매출/영업이익 (6개 브랜드, SUPRA 포함)
2. D.overviewPL: 전체 손익계산서 데이터
3. D.waterfallData: Waterfall 차트 데이터
4. D.cumulativeTrendData: 월중누적매출추이 데이터

조건: W(SUPRA)는 브랜드별 탭에는 없으나 전체 집계에는 포함

작성일: 2025-11
"""

import os
import sys
import json
import re
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from path_utils import (
    get_current_year_file_path, 
    get_plan_file_path, 
    get_previous_year_file_path,
    extract_year_month_from_date,
    get_previous_year_month
)

ROOT = os.path.dirname(os.path.dirname(__file__))
PUBLIC_DIR = os.path.join(ROOT, "public")
RAW_DIR = os.path.join(ROOT, "raw")
MASTER_DIR = os.path.join(ROOT, "Master")
DIRECT_COST_MASTER_PATH = os.path.join(MASTER_DIR, "직접비마스터.csv")

# 브랜드 코드 → 표시명 매핑
BRAND_CODE_MAP = {
    'M': 'MLB',
    'I': 'MLB KIDS',
    'X': 'DISCOVERY',
    'V': 'DUVETICA',
    'ST': 'SERGIO',
    'W': 'SUPRA'
}

# 브랜드 코드 목록 (전체 6개)
ALL_BRAND_CODES = ['M', 'I', 'X', 'V', 'ST', 'W']


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


def load_ke30_shop_data(date_str: str) -> Optional[pd.DataFrame]:
    """ke30 Shop 파일 로드 (KPI 현시점)"""
    year_month = extract_year_month_from_date(date_str)
    filename = f"ke30_{date_str}_{year_month}_Shop.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        print(f"  [WARNING] ke30 Shop 파일을 찾을 수 없습니다: {filepath}")
        return None
    
    print(f"  [LOAD] KE30 현시점: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    return df


def load_forecast_shop_data(date_str: str) -> Optional[pd.DataFrame]:
    """forecast Shop 파일 로드 (당년 월말예상)"""
    year_month = extract_year_month_from_date(date_str)
    filename = f"forecast_{date_str}_{year_month}_Shop.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        print(f"  [WARNING] forecast Shop 파일을 찾을 수 없습니다: {filepath}")
        return None
    
    print(f"  [LOAD] Forecast 월말예상: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    return df


def load_plan_data(year_month: str) -> Optional[pd.DataFrame]:
    """계획 전처리 완료 파일 로드"""
    filepath = get_plan_file_path(year_month)
    
    if not os.path.exists(filepath):
        print(f"  [WARNING] 계획 파일을 찾을 수 없습니다: {filepath}")
        return None
    
    print(f"  [LOAD] 계획 데이터: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    return df


def load_previous_year_data(year_month: str) -> Optional[pd.DataFrame]:
    """전년 데이터 로드"""
    filename = f"previous_rawdata_{year_month}_Shop.csv"
    filepath = get_previous_year_file_path(year_month, filename)
    
    if not os.path.exists(filepath):
        print(f"  [WARNING] 전년 파일을 찾을 수 없습니다: {filepath}")
        return None
    
    print(f"  [LOAD] 전년 데이터: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    return df


def find_column(df: pd.DataFrame, keywords: List[str], exclude: List[str] = None) -> Optional[str]:
    """키워드를 포함하는 컬럼 찾기"""
    exclude = exclude or []
    for col in df.columns:
        col_str = str(col)
        if all(kw in col_str for kw in keywords):
            if not any(ex in col_str for ex in exclude):
                return col
    return None


def aggregate_brand_data(df: pd.DataFrame, brand_col: str, brand_code: str, 
                         sales_col: str, tag_col: str = None, 
                         profit_col: str = None, cogs_col: str = None,
                         gross_profit_col: str = None, direct_cost_col: str = None,
                         op_expense_col: str = None) -> Dict[str, float]:
    """브랜드별 데이터 집계"""
    brand_df = df[df[brand_col].astype(str).str.strip() == brand_code]
    
    result = {
        'sales': 0.0,
        'tag': 0.0,
        'profit': 0.0,
        'cogs': 0.0,
        'gross_profit': 0.0,
        'direct_cost': 0.0,
        'op_expense': 0.0
    }
    
    if brand_df.empty:
        return result
    
    if sales_col:
        result['sales'] = brand_df[sales_col].apply(extract_numeric).sum()
    if tag_col:
        result['tag'] = brand_df[tag_col].apply(extract_numeric).sum()
    if profit_col:
        result['profit'] = brand_df[profit_col].apply(extract_numeric).sum()
    if cogs_col:
        result['cogs'] = brand_df[cogs_col].apply(extract_numeric).sum()
    if gross_profit_col:
        result['gross_profit'] = brand_df[gross_profit_col].apply(extract_numeric).sum()
    if direct_cost_col:
        result['direct_cost'] = brand_df[direct_cost_col].apply(extract_numeric).sum()
    if op_expense_col:
        # 영업비는 채널='공통'인 행에서만 추출
        channel_col = find_column(brand_df, ['채널'])
        if channel_col:
            common_df = brand_df[brand_df[channel_col].astype(str).str.strip() == '공통']
            if not common_df.empty and op_expense_col in common_df.columns:
                result['op_expense'] = common_df[op_expense_col].apply(extract_numeric).sum()
    
    return result


def create_by_brand_data(date_str: str) -> List[Dict[str, Any]]:
    """
    브랜드별 매출/영업이익 데이터 생성 (6개 브랜드)
    forecast(월말예상) 데이터 사용
    
    Returns:
        List[Dict]: [{BRAND, SALES, DIRECT_PROFIT, OPERATING_PROFIT, ACHIEVEMENT, YOY_SALES}, ...]
    """
    print("\n[1/5] by_brand 데이터 생성 중... (forecast 기준)")
    
    year_month = extract_year_month_from_date(date_str)
    
    # 데이터 로드
    df_forecast = load_forecast_shop_data(date_str)
    df_plan = load_plan_data(year_month)
    df_prev = load_previous_year_data(year_month)
    
    by_brand = []
    
    for brand_code in ALL_BRAND_CODES:
        brand_name = BRAND_CODE_MAP.get(brand_code, brand_code)
        
        brand_data = {
            'BRAND': brand_name,
            'SALES': 0.0,
            'DIRECT_PROFIT': 0.0,
            'OPERATING_PROFIT': 0.0,
            'ACHIEVEMENT': 0,
            'YOY_SALES': 100
        }
        
        # 1. 월말예상 매출/직접이익 (forecast)
        forecast_sales = 0.0
        forecast_profit = 0.0
        if df_forecast is not None:
            forecast_brand_col = find_column(df_forecast, ['브랜드'])
            forecast_sales_col = find_column(df_forecast, ['실판매액', '합계'], exclude=['(V-)'])
            forecast_profit_col = find_column(df_forecast, ['직접이익'])
            
            if forecast_brand_col and forecast_sales_col:
                forecast_agg = aggregate_brand_data(df_forecast, forecast_brand_col, brand_code, 
                                                 forecast_sales_col, profit_col=forecast_profit_col)
                forecast_sales = forecast_agg['sales']
                forecast_profit = forecast_agg['profit']
                # 억원 단위로 변환
                brand_data['SALES'] = round(forecast_sales / 100000000, 1)
                brand_data['DIRECT_PROFIT'] = round(forecast_profit / 100000000, 1)
        
        # 2. 영업이익 계산 (forecast 직접이익 - 계획 영업비)
        # forecast 직접비 가져오기 (직접비가 있으면 사용, 없으면 직접이익 사용)
        forecast_direct_cost = 0.0
        if df_forecast is not None:
            forecast_brand_col = find_column(df_forecast, ['브랜드'])
            forecast_direct_cost_col = find_column(df_forecast, ['직접비', '합계'])
            
            if forecast_brand_col and forecast_direct_cost_col:
                forecast_brand_df = df_forecast[df_forecast[forecast_brand_col].astype(str).str.strip() == brand_code]
                if not forecast_brand_df.empty:
                    forecast_direct_cost = forecast_brand_df[forecast_direct_cost_col].apply(extract_numeric).sum()
        
        # 계획 영업비 가져오기
        op_expense_plan = 0.0
        if df_plan is not None:
            plan_brand_col = find_column(df_plan, ['브랜드'])
            plan_channel_col = find_column(df_plan, ['채널'])
            
            if plan_brand_col and plan_channel_col:
                plan_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                plan_domestic = plan_brand_df[plan_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not plan_domestic.empty:
                    row = plan_domestic.iloc[0]
                    for col in plan_domestic.columns:
                        col_str = str(col)
                        if col_str == '영업비':
                            op_expense_plan = extract_numeric(row[col])
                            break
        
        # 영업이익 = forecast 직접이익 - 계획 영업비
        # (직접비가 있으면 직접이익에서 직접비를 빼고 영업비를 빼는 것이지만, 
        #  직접이익 = 매출총이익 - 직접비 이므로, 영업이익 = 직접이익 - 영업비가 맞음)
        operating_profit = forecast_profit - op_expense_plan
        brand_data['OPERATING_PROFIT'] = round(operating_profit / 100000000, 1)
        
        # 3. 목표대비 달성률 (forecast 실판매액 / plan 실판매액)
        if df_plan is not None:
            plan_brand_col = find_column(df_plan, ['브랜드'])
            plan_channel_col = find_column(df_plan, ['채널'])
            plan_sales_col = find_column(df_plan, ['실판매액'])
            
            if plan_brand_col and plan_channel_col and plan_sales_col:
                plan_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                plan_domestic = plan_brand_df[plan_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not plan_domestic.empty:
                    plan_sales = plan_domestic[plan_sales_col].apply(extract_numeric).sum()
                    if plan_sales > 0:
                        brand_data['ACHIEVEMENT'] = round((forecast_sales / plan_sales) * 100)
        
        # 4. 전년대비 (forecast 실판매액 / 전년 실판매액)
        if df_prev is not None:
            # 전년 매출
            prev_brand_col = find_column(df_prev, ['브랜드'])
            prev_sales_col = find_column(df_prev, ['실매출액']) or find_column(df_prev, ['실판매액'])
            
            prev_sales = 0.0
            if prev_brand_col and prev_sales_col:
                prev_agg = aggregate_brand_data(df_prev, prev_brand_col, brand_code, 
                                                prev_sales_col)
                prev_sales = prev_agg['sales']
            
            if prev_sales > 0:
                brand_data['YOY_SALES'] = round((forecast_sales / prev_sales) * 100)
        
        by_brand.append(brand_data)
        print(f"    {brand_name}: 매출 {brand_data['SALES']}억원, 직접이익 {brand_data['DIRECT_PROFIT']}억원, 영업이익 {brand_data['OPERATING_PROFIT']}억원, 달성률 {brand_data['ACHIEVEMENT']}%, 전년비 {brand_data['YOY_SALES']}%")
    
    print(f"  [OK] {len(by_brand)}개 브랜드 데이터 생성 완료 (forecast 기준)")
    return by_brand


def create_overview_kpi(date_str: str, by_brand: List[Dict], overview_pl: Dict[str, Any]) -> Dict[str, Any]:
    """
    전체 현황 KPI 생성 (6개 브랜드 합산, brand_kpi와 동일한 구조)
    update_brand_kpi.py의 계산 로직을 참고하여 동일하게 계산
    
    Args:
        date_str: 날짜 문자열
        by_brand: 브랜드별 데이터
        overview_pl: 전체 손익계산서 데이터
    
    Returns:
        Dict: {"OVERVIEW": {...}} 형태의 KPI 데이터
    """
    print("\n[1.5/5] 전체 현황 KPI 생성 중...")
    
    year_month = extract_year_month_from_date(date_str)
    
    # 데이터 로드
    df_ke30 = load_ke30_shop_data(date_str)
    df_forecast = load_forecast_shop_data(date_str)
    df_plan = load_plan_data(year_month)
    df_prev = load_previous_year_data(year_month)
    
    # 초기화 (원 단위)
    kpi_data = {
        'revenue': 0.0,  # 현시점 실판매출
        'directProfit': 0.0,  # 현시점 직접이익
        'directProfitRate': 0.0,  # 직접이익율
        'discountRate': 0.0,  # 할인율
        'progressRate': 0.0,  # 진척율 (직접이익 기준)
        'progressRateForecast': 0.0,  # 월말예상 진척율 (직접이익 기준)
        'revenueForecast': 0.0,  # 월말예상 실판매출
        'directProfitForecast': 0.0,  # 월말예상 직접이익
        'directProfitRateForecast': 0.0,  # 월말예상 직접이익율
        'discountRateForecast': 0.0,  # 월말예상 할인율
        'operatingProfit': 0.0,  # 현시점 영업이익
        'operatingProfitForecast': 0.0,  # 월말예상 영업이익
        'operatingProfitRate': 0.0,  # 현시점 영업이익율
        'operatingProfitRateForecast': 0.0,  # 월말예상 영업이익율
        'revenuePrevious': 0.0,  # 전년 실판매출
        'directProfitPrevious': 0.0,  # 전년 직접이익
        'operatingProfitPrevious': 0.0,  # 전년 영업이익
        'revenuePlan': 0.0,  # 계획 실판매출
        'directProfitPlan': 0.0,  # 계획 직접이익
        'directProfitRatePlan': 0.0,  # 계획 직접이익율
        'revenueVsPlan': 0.0,  # 목표대비 매출 (%)
        'revenueVsPrevious': 0.0,  # 전년대비 매출 (%)
        'profitVsPlan': 0.0,  # 목표대비 직접이익 (%)
        'profitVsPrevious': 0.0  # 전년대비 직접이익 (%)
    }
    
    # 현시점 영업비 (ke30에서 '공통' 채널의 영업비 합계)
    op_expense_current = 0.0
    
    # 1. 현시점 데이터 (ke30)
    if df_ke30 is not None:
        brand_col = find_column(df_ke30, ['브랜드'])
        sales_col = find_column(df_ke30, ['실판매액', '합계'], exclude=['(V-)'])
        tag_col = find_column(df_ke30, ['TAG가', '합계']) or find_column(df_ke30, ['TAG매출'])
        profit_col = find_column(df_ke30, ['직접이익'])
        op_expense_col = find_column(df_ke30, ['영업비'])
        channel_col = find_column(df_ke30, ['채널'])
        
        if brand_col and sales_col:
            for brand_code in ALL_BRAND_CODES:
                agg = aggregate_brand_data(df_ke30, brand_col, brand_code, 
                                         sales_col, tag_col, profit_col=profit_col,
                                         op_expense_col=op_expense_col)
                kpi_data['revenue'] += agg['sales']
                kpi_data['directProfit'] += agg['profit']
                op_expense_current += agg['op_expense']
        
        if op_expense_col:
            print(f"    [DEBUG] 현시점 영업비 합계: {op_expense_current/100000000:.2f}억원")
        else:
            print(f"    [WARNING] 현시점 영업비 컬럼을 찾을 수 없습니다")
    
    # 2. 월말예상 데이터 (forecast)
    if df_forecast is not None:
        brand_col = find_column(df_forecast, ['브랜드'])
        sales_col = find_column(df_forecast, ['실판매액', '합계'], exclude=['(V-)'])
        tag_col = find_column(df_forecast, ['TAG가', '합계']) or find_column(df_forecast, ['TAG매출'])
        profit_col = find_column(df_forecast, ['직접이익'])
        
        if brand_col and sales_col:
            for brand_code in ALL_BRAND_CODES:
                agg = aggregate_brand_data(df_forecast, brand_col, brand_code, 
                                         sales_col, tag_col, profit_col=profit_col)
                kpi_data['revenueForecast'] += agg['sales']
                kpi_data['directProfitForecast'] += agg['profit']
    
    # 3. 전년 데이터
    if df_prev is not None:
        brand_col = find_column(df_prev, ['브랜드'])
        sales_col = find_column(df_prev, ['실매출액']) or find_column(df_prev, ['실판매액'])
        tag_col = find_column(df_prev, ['TAG매출']) or find_column(df_prev, ['TAG가'])
        profit_col = find_column(df_prev, ['직접이익'])
        op_expense_col = find_column(df_prev, ['영업비'])
        
        if brand_col:
            for brand_code in ALL_BRAND_CODES:
                agg = aggregate_brand_data(df_prev, brand_col, brand_code, 
                                         sales_col, tag_col, profit_col=profit_col,
                                         op_expense_col=op_expense_col)
                kpi_data['revenuePrevious'] += agg['sales']
                kpi_data['directProfitPrevious'] += agg['profit']
                # 전년 영업이익 = 직접이익 - 영업비
                prev_op_expense = agg['op_expense']
                kpi_data['operatingProfitPrevious'] += (agg['profit'] - prev_op_expense)
    
    # 4. 계획 데이터
    op_expense_plan = 0.0  # 계획 영업비 (월말예상 영업비로 사용)
    if df_plan is not None:
        plan_brand_col = find_column(df_plan, ['브랜드'])
        plan_channel_col = find_column(df_plan, ['채널'])
        
        if plan_brand_col and plan_channel_col:
            # 실판매액[v+] 컬럼을 먼저 찾기 (모든 브랜드 확인)
            plan_revenue_col = None
            for brand_code in ALL_BRAND_CODES:
                sample_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                sample_domestic = sample_brand_df[sample_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not sample_domestic.empty:
                    for col in sample_domestic.columns:
                        col_str = str(col).strip()
                        # 실판매액[v+] 우선순위: [v+] 포함 > 기타 실판매액
                        if '실판매액' in col_str:
                            if '[v+]' in col_str.lower() and plan_revenue_col is None:
                                plan_revenue_col = col
                                break
                    if plan_revenue_col:
                        break
            
            # [v+]가 없으면 일반 실판매액 찾기
            if not plan_revenue_col:
                for brand_code in ALL_BRAND_CODES:
                    sample_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                    sample_domestic = sample_brand_df[sample_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                    
                    if not sample_domestic.empty:
                        for col in sample_domestic.columns:
                            col_str = str(col).strip()
                            if '실판매액' in col_str:
                                plan_revenue_col = col
                                break
                        if plan_revenue_col:
                            break
            
            if plan_revenue_col:
                print(f"  [DEBUG] KPI 계획 실판매액 컬럼: {plan_revenue_col}")
            
            for brand_code in ALL_BRAND_CODES:
                plan_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                plan_domestic = plan_brand_df[plan_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not plan_domestic.empty:
                    row = plan_domestic.iloc[0]
                    for col in plan_domestic.columns:
                        col_str = str(col).strip()
                        val = extract_numeric(row[col])
                        
                        # 실판매액[v+] 우선, 없으면 일반 실판매액
                        if col == plan_revenue_col:
                            kpi_data['revenuePlan'] += val
                        elif col_str == '직접이익':
                            kpi_data['directProfitPlan'] += val
                        elif col_str == '영업비':
                            op_expense_plan += val
    
    # 5. 계산 필드
    # 직접이익율 (직접이익/실판매출*1.1*100) - update_brand_kpi.py와 동일
    if kpi_data['revenue'] > 0:
        kpi_data['directProfitRate'] = round((kpi_data['directProfit'] / kpi_data['revenue']) * 1.1 * 100, 2)
    if kpi_data['revenueForecast'] > 0:
        kpi_data['directProfitRateForecast'] = round((kpi_data['directProfitForecast'] / kpi_data['revenueForecast']) * 1.1 * 100, 2)
    if kpi_data['revenuePlan'] > 0:
        kpi_data['directProfitRatePlan'] = round((kpi_data['directProfitPlan'] / kpi_data['revenuePlan']) * 1.1 * 100, 2)
    
    # 할인율 (1 - 실판매출/TAG매출)*100
    # 현시점 할인율 계산용 TAG매출 (ke30에서 가져온 값 사용)
    if df_ke30 is not None:
        brand_col = find_column(df_ke30, ['브랜드'])
        tag_col = find_column(df_ke30, ['TAG가', '합계']) or find_column(df_ke30, ['TAG매출'])
        sales_col = find_column(df_ke30, ['실판매액', '합계'], exclude=['(V-)'])
        
        if brand_col and tag_col and sales_col:
            tag_total = 0.0
            sales_total = 0.0
            for brand_code in ALL_BRAND_CODES:
                tag_agg = aggregate_brand_data(df_ke30, brand_col, brand_code, sales_col, tag_col)
                tag_total += tag_agg['tag']
                sales_total += tag_agg['sales']
            
            if tag_total > 0:
                kpi_data['discountRate'] = round((1 - (sales_total / tag_total)) * 100, 2)
    
    # 월말예상 할인율
    if df_forecast is not None:
        brand_col = find_column(df_forecast, ['브랜드'])
        tag_col = find_column(df_forecast, ['TAG가', '합계']) or find_column(df_forecast, ['TAG매출'])
        sales_col = find_column(df_forecast, ['실판매액', '합계'], exclude=['(V-)'])
        
        if brand_col and tag_col and sales_col:
            tag_total = 0.0
            sales_total = 0.0
            for brand_code in ALL_BRAND_CODES:
                tag_agg = aggregate_brand_data(df_forecast, brand_col, brand_code, sales_col, tag_col)
                tag_total += tag_agg['tag']
                sales_total += tag_agg['sales']
            
            if tag_total > 0:
                kpi_data['discountRateForecast'] = round((1 - (sales_total / tag_total)) * 100, 2)
    
    # 진척율 (직접이익 기준) - update_brand_kpi.py와 동일
    # 현시점 진척율 = 현시점 직접이익 / 계획 직접이익 × 100
    if kpi_data['directProfitPlan'] > 0:
        kpi_data['progressRate'] = round((kpi_data['directProfit'] / kpi_data['directProfitPlan']) * 100, 1)
    # 월말예상 진척율 = 월말예상 직접이익 / 계획 직접이익 × 100
    if kpi_data['directProfitPlan'] > 0 and kpi_data['directProfitForecast'] > 0:
        kpi_data['progressRateForecast'] = round((kpi_data['directProfitForecast'] / kpi_data['directProfitPlan']) * 100, 1)
    
    # 영업이익 계산
    # 현시점 영업이익 = 직접이익(현시점) - 영업비(계획)
    kpi_data['operatingProfit'] = round(kpi_data['directProfit'] - op_expense_plan, 0)
    # 월말예상 영업이익 = 직접이익(월말예상) - 영업비(계획)
    kpi_data['operatingProfitForecast'] = round(kpi_data['directProfitForecast'] - op_expense_plan, 0)
    
    # 영업이익율 계산 (영업이익 / 실판매출 × 1.1 × 100)
    if kpi_data['revenue'] > 0:
        kpi_data['operatingProfitRate'] = round((kpi_data['operatingProfit'] / kpi_data['revenue']) * 1.1 * 100, 2)
    if kpi_data['revenueForecast'] > 0:
        kpi_data['operatingProfitRateForecast'] = round((kpi_data['operatingProfitForecast'] / kpi_data['revenueForecast']) * 1.1 * 100, 2)
    
    # 목표대비 계산 (월말예상 vs 목표) - update_brand_kpi.py와 동일
    # 목표대비 = (월말예상 - 목표) / 목표 × 100
    if kpi_data['revenuePlan'] > 0:
        kpi_data['revenueVsPlan'] = round(((kpi_data['revenueForecast'] - kpi_data['revenuePlan']) / kpi_data['revenuePlan']) * 100, 1)
    if kpi_data['directProfitPlan'] > 0:
        kpi_data['profitVsPlan'] = round(((kpi_data['directProfitForecast'] - kpi_data['directProfitPlan']) / kpi_data['directProfitPlan']) * 100, 1)
    
    # 전년대비 계산 (월말예상 vs 전년) - update_brand_kpi.py와 동일
    # 전년대비 = (월말예상 - 전년) / 전년 × 100
    if kpi_data['revenuePrevious'] > 0:
        kpi_data['revenueVsPrevious'] = round(((kpi_data['revenueForecast'] - kpi_data['revenuePrevious']) / kpi_data['revenuePrevious']) * 100, 1)
    if kpi_data['directProfitPrevious'] > 0:
        kpi_data['profitVsPrevious'] = round(((kpi_data['directProfitForecast'] - kpi_data['directProfitPrevious']) / kpi_data['directProfitPrevious']) * 100, 1)
    
    print(f"  [OK] 전체 현황 KPI 생성 완료")
    print(f"    현시점 매출: {kpi_data['revenue']/100000000:.1f}억원, 직접이익: {kpi_data['directProfit']/100000000:.1f}억원")
    print(f"    월말예상 매출: {kpi_data['revenueForecast']/100000000:.1f}억원, 직접이익: {kpi_data['directProfitForecast']/100000000:.1f}억원")
    print(f"    진척율: {kpi_data['progressRate']:.1f}% (현시점), {kpi_data['progressRateForecast']:.1f}% (월말예상)")
    print(f"    목표대비: 매출 {kpi_data['revenueVsPlan']:+.1f}%, 직접이익 {kpi_data['profitVsPlan']:+.1f}%")
    print(f"    전년대비: 매출 {kpi_data['revenueVsPrevious']:+.1f}%, 직접이익 {kpi_data['profitVsPrevious']:+.1f}%")
    
    return {"OVERVIEW": kpi_data}


def load_direct_cost_master() -> Dict[str, str]:
    """
    직접비 마스터 파일 로드: 계정명 -> 계정전환 매핑
    브랜드별 손익계산서 로직과 동일
    
    Returns:
        Dict[str, str]: 계정명 -> 계정전환 매핑 딕셔너리
    """
    if not os.path.exists(DIRECT_COST_MASTER_PATH):
        print(f"  [WARNING] 직접비 마스터 파일이 없습니다: {DIRECT_COST_MASTER_PATH}")
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
            print(f"  [WARNING] 직접비 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
            return {}
    
    mapping = {}
    for _, row in df[[account_col, conversion_col]].dropna().iterrows():
        account = str(row[account_col]).strip()
        conversion = str(row[conversion_col]).strip()
        if account and conversion:
            mapping[account] = conversion
    
    print(f"  [OK] 직접비 마스터 로드: {len(mapping)}개 매핑")
    return mapping


def aggregate_direct_cost_details_overview(df: pd.DataFrame, direct_cost_master: Dict[str, str]) -> Dict[str, float]:
    """
    직접비 세부 항목을 마스터 기준으로 집계 (전체 브랜드 합산)
    브랜드별 손익계산서 로직과 동일
    
    Args:
        df: 데이터프레임 (전체 브랜드 데이터)
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
            # 부분 매칭 시도
            for master_col, master_category in direct_cost_master.items():
                if master_col in col:
                    if master_category in result:
                        # 해당 컬럼의 값 합산
                        for val in df[col]:
                            result[master_category] += extract_numeric(val)
                        matched = True
                        break
    
    return result


def create_overview_pl_data(date_str: str) -> Dict[str, Any]:
    """
    전체 손익계산서 데이터 생성 (6개 브랜드 합산)
    브랜드별 손익계산서 로직과 동일한 방식으로 계산
    
    Returns:
        Dict: {tagRevenue, revenue, cog, grossProfit, directCost, directCostDetail, 
               directProfit, operatingExpense, opProfit} - 각각 {prev, target, forecast, yoy, achievement}
    """
    print("\n[2/5] 전체 손익계산서 데이터 생성 중... (브랜드별 로직 참고)")
    
    year_month = extract_year_month_from_date(date_str)
    
    # 직접비 마스터 로드
    direct_cost_master = load_direct_cost_master()
    
    # 데이터 로드
    df_forecast = load_forecast_shop_data(date_str)
    df_plan = load_plan_data(year_month)
    df_prev = load_previous_year_data(year_month)
    
    # 초기화 (억원 단위로 저장)
    overview_pl = {
        'tagRevenue': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
        'revenue': {'prev': 0.0, 'target': 0.0, 'forecast': 0.0, 'yoy': 0, 'achievement': 0},
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
    
    # 원 단위로 집계할 변수들
    forecast_sums = {
        'tag_revenue': 0.0, 'revenue': 0.0, '출고매출': 0.0, 'cog': 0.0, 'gross_profit': 0.0,
        'direct_cost': 0.0, 'direct_profit': 0.0
    }
    plan_sums = {
        'tag_revenue': 0.0, 'revenue': 0.0, 'cog': 0.0, 'gross_profit': 0.0,
        'direct_cost': 0.0, 'direct_profit': 0.0, 'op_expense': 0.0, 'op_profit': 0.0
    }
    prev_sums = {
        'tag_revenue': 0.0, 'revenue': 0.0, '출고매출': 0.0, 'cog': 0.0, 'gross_profit': 0.0,
        'direct_cost': 0.0, 'direct_profit': 0.0, 'op_expense': 0.0
    }
    
    # 직접비 세부 항목 (원 단위)
    forecast_direct_cost_details = {
        '인건비': 0.0, '임차관리비': 0.0, '물류운송비': 0.0,
        '로열티': 0.0, '감가상각비': 0.0, '기타': 0.0
    }
    plan_direct_cost_details = {
        '인건비': 0.0, '임차관리비': 0.0, '물류운송비': 0.0,
        '로열티': 0.0, '감가상각비': 0.0, '기타': 0.0
    }
    prev_direct_cost_details = {
        '인건비': 0.0, '임차관리비': 0.0, '물류운송비': 0.0,
        '로열티': 0.0, '감가상각비': 0.0, '기타': 0.0
    }
    
    # 1. Forecast 데이터 (월말예상) - 브랜드별 로직과 동일
    if df_forecast is not None:
        brand_col = find_column(df_forecast, ['브랜드'])
        tag_col = find_column(df_forecast, ['TAG가', '합계']) or find_column(df_forecast, ['TAG매출'])
        sales_col = find_column(df_forecast, ['실판매액', '합계'], exclude=['(V-)'])
        출고매출_col = None
        cogs_col = None
        gross_profit_col = find_column(df_forecast, ['매출총이익'])
        direct_cost_col = find_column(df_forecast, ['직접비', '합계'])
        direct_profit_col = find_column(df_forecast, ['직접이익'])
        
        # 출고매출(V-) 컬럼 찾기 (매출총이익 계산용)
        for col in df_forecast.columns:
            col_str = str(col)
            if '출고매출' in col_str or ('출고' in col_str and '매출' in col_str):
                if '(V-)' in col_str or 'Actual' in col_str:
                    출고매출_col = col
                    break
                elif not 출고매출_col:
                    출고매출_col = col
        
        # 매출원가 컬럼 찾기 (우선순위: 평가감환입반영 > Actual > 기타)
        for col in df_forecast.columns:
            col_str = str(col)
            if '매출원가' in col_str:
                if '평가감환입반영' in col_str or '평가감환입' in col_str:
                    cogs_col = col
                    break
                elif 'Actual' in col_str and not cogs_col:
                    cogs_col = col
                elif not cogs_col:
                    cogs_col = col
        
        if brand_col:
            for brand_code in ALL_BRAND_CODES:
                brand_df = df_forecast[df_forecast[brand_col].astype(str).str.strip() == brand_code]
                
                if not brand_df.empty:
                    # TAG매출
                    if tag_col:
                        for val in brand_df[tag_col]:
                            forecast_sums['tag_revenue'] += extract_numeric(val)
                    
                    # 실판매출
                    if sales_col:
                        for val in brand_df[sales_col]:
                            forecast_sums['revenue'] += extract_numeric(val)
                    
                    # 출고매출(V-) (매출총이익 계산용)
                    if 출고매출_col:
                        for val in brand_df[출고매출_col]:
                            forecast_sums['출고매출'] += extract_numeric(val)
                    
                    # 매출원가
                    if cogs_col:
                        for val in brand_df[cogs_col]:
                            forecast_sums['cog'] += extract_numeric(val)
                    
                    # 매출총이익 (컬럼에서 직접 가져오기)
                    if gross_profit_col:
                        for val in brand_df[gross_profit_col]:
                            forecast_sums['gross_profit'] += extract_numeric(val)
                    
                    # 직접비
                    if direct_cost_col:
                        for val in brand_df[direct_cost_col]:
                            forecast_sums['direct_cost'] += extract_numeric(val)
                    
                    # 직접비 세부 항목 집계 (직접비 마스터 사용)
                    direct_cost_details = aggregate_direct_cost_details_overview(brand_df, direct_cost_master)
                    for key in forecast_direct_cost_details:
                        forecast_direct_cost_details[key] += direct_cost_details[key]
        
        # 매출총이익 계산 (컬럼이 없으면 출고매출(V-) - 매출원가)
        if forecast_sums['gross_profit'] == 0 and forecast_sums['출고매출'] > 0:
            forecast_sums['gross_profit'] = forecast_sums['출고매출'] - forecast_sums['cog']
        elif forecast_sums['gross_profit'] == 0:
            # 최종 폴백: 실판매출 - 매출원가
            forecast_sums['gross_profit'] = forecast_sums['revenue'] - forecast_sums['cog']
        
        # 직접이익 = 매출총이익 - 직접비 (직접 계산)
        forecast_sums['direct_profit'] = forecast_sums['gross_profit'] - forecast_sums['direct_cost']
        
        # 억원 단위로 변환하여 저장
        overview_pl['tagRevenue']['forecast'] = round(forecast_sums['tag_revenue'] / 100000000, 2)
        overview_pl['revenue']['forecast'] = round(forecast_sums['revenue'] / 100000000, 2)
        overview_pl['cog']['forecast'] = round(forecast_sums['cog'] / 100000000, 2)
        overview_pl['grossProfit']['forecast'] = round(forecast_sums['gross_profit'] / 100000000, 2)
        overview_pl['directCost']['forecast'] = round(forecast_sums['direct_cost'] / 100000000, 2)
        overview_pl['directProfit']['forecast'] = round(forecast_sums['direct_profit'] / 100000000, 2)
        
        # 직접비 세부 항목 (억원 단위)
        for key in forecast_direct_cost_details:
            overview_pl['directCostDetail'][key]['forecast'] = round(forecast_direct_cost_details[key] / 100000000, 2)
    
    # 2. 계획 데이터 (목표) - 브랜드별 로직과 동일
    if df_plan is not None:
        plan_brand_col = find_column(df_plan, ['브랜드'])
        plan_channel_col = find_column(df_plan, ['채널'])
        
        if plan_brand_col and plan_channel_col:
            # 실판매액[v+]와 매출원가 컬럼을 먼저 찾기
            plan_revenue_col = None
            plan_cog_col = None
            
            # 내수합계 행 확인하여 컬럼 찾기 (모든 브랜드 확인)
            for brand_code in ALL_BRAND_CODES:
                sample_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                sample_domestic = sample_brand_df[sample_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not sample_domestic.empty:
                    for col in sample_domestic.columns:
                        col_str = str(col).strip()
                        # 실판매액[v+] 우선순위: [v+] 포함 > 기타 실판매액
                        if '실판매액' in col_str:
                            if '[v+]' in col_str.lower() and plan_revenue_col is None:
                                plan_revenue_col = col
                            elif plan_revenue_col is None:
                                plan_revenue_col = col
                        # 매출원가 컬럼 찾기
                        if '매출원가' in col_str and plan_cog_col is None:
                            plan_cog_col = col
                    
                    if plan_revenue_col and plan_cog_col:
                        break
            
            if plan_revenue_col:
                print(f"  [DEBUG] 손익계산서 계획 실판매액 컬럼: {plan_revenue_col}")
            if plan_cog_col:
                print(f"  [DEBUG] 손익계산서 계획 매출원가 컬럼: {plan_cog_col}")
            
            for brand_code in ALL_BRAND_CODES:
                plan_brand_df = df_plan[df_plan[plan_brand_col].astype(str).str.strip() == brand_code]
                plan_domestic = plan_brand_df[plan_brand_df[plan_channel_col].astype(str).str.strip() == '내수합계']
                
                if not plan_domestic.empty:
                    row = plan_domestic.iloc[0]
                    
                    # 각 컬럼 찾기 및 합산
                    for col in plan_domestic.columns:
                        col_str = str(col).strip()
                        val = extract_numeric(row[col])
                        
                        if 'TAG가' in col_str or ('TAG' in col_str and '매출' in col_str):
                            plan_sums['tag_revenue'] += val
                        # 실판매액[v+] 우선, 없으면 일반 실판매액
                        elif col == plan_revenue_col:
                            plan_sums['revenue'] += val
                        # 매출원가
                        elif col == plan_cog_col:
                            plan_sums['cog'] += val
                        elif '매출총이익' in col_str:
                            plan_sums['gross_profit'] += val
                        elif col_str == '직접비' or '직접비 합계' in col_str:
                            plan_sums['direct_cost'] += val
                        elif col_str == '직접이익':
                            plan_sums['direct_profit'] += val
                        elif col_str == '영업비':
                            plan_sums['op_expense'] += val
                        elif col_str == '영업이익':
                            plan_sums['op_profit'] += val
                    
                    # 직접비 세부 항목 집계 (직접비 마스터 사용)
                    direct_cost_details = aggregate_direct_cost_details_overview(plan_domestic, direct_cost_master)
                    for key in plan_direct_cost_details:
                        plan_direct_cost_details[key] += direct_cost_details[key]
            
            # 직접이익 = 매출총이익 - 직접비 (직접 계산)
            if plan_sums['gross_profit'] > 0 and plan_sums['direct_cost'] > 0:
                plan_sums['direct_profit'] = plan_sums['gross_profit'] - plan_sums['direct_cost']
            
            # 억원 단위로 변환하여 저장
            overview_pl['tagRevenue']['target'] = round(plan_sums['tag_revenue'] / 100000000, 2)
            overview_pl['revenue']['target'] = round(plan_sums['revenue'] / 100000000, 2)
            overview_pl['cog']['target'] = round(plan_sums['cog'] / 100000000, 2)
            overview_pl['grossProfit']['target'] = round(plan_sums['gross_profit'] / 100000000, 2)
            overview_pl['directCost']['target'] = round(plan_sums['direct_cost'] / 100000000, 2)
            overview_pl['directProfit']['target'] = round(plan_sums['direct_profit'] / 100000000, 2)
            overview_pl['operatingExpense']['target'] = round(plan_sums['op_expense'] / 100000000, 2)
            overview_pl['operatingExpense']['forecast'] = round(plan_sums['op_expense'] / 100000000, 2)  # forecast 영업비 = 계획 영업비
            overview_pl['opProfit']['target'] = round(plan_sums['op_profit'] / 100000000, 2)
            
            # 직접비 세부 항목 (억원 단위)
            for key in plan_direct_cost_details:
                overview_pl['directCostDetail'][key]['target'] = round(plan_direct_cost_details[key] / 100000000, 2)
            
            # 당년 영업이익(forecast) = 직접이익(forecast) - 계획 영업비
            overview_pl['opProfit']['forecast'] = round(
                overview_pl['directProfit']['forecast'] - overview_pl['operatingExpense']['forecast'], 2
            )
    
    # 3. 전년 데이터 - 브랜드별 로직과 동일
    if df_prev is not None:
        prev_brand_col = find_column(df_prev, ['브랜드'])
        prev_tag_col = find_column(df_prev, ['TAG매출']) or find_column(df_prev, ['TAG가'])
        prev_sales_col = find_column(df_prev, ['실매출액']) or find_column(df_prev, ['실판매액'])
        prev_cogs_col = None
        prev_gross_profit_col = find_column(df_prev, ['매출총이익'])
        prev_direct_cost_col = find_column(df_prev, ['직접비', '합계'])
        prev_profit_col = find_column(df_prev, ['직접이익'])
        prev_op_expense_col = find_column(df_prev, ['영업비'])
        prev_channel_col = find_column(df_prev, ['채널'])
        prev_출고매출_col = None
        
        # 전년 매출원가 컬럼 찾기 (우선순위: 환입후매출원가+평가감 > 기타)
        for col in df_prev.columns:
            col_str = str(col)
            if '매출원가' in col_str:
                if '환입후매출원가' in col_str or '평가감' in col_str:
                    prev_cogs_col = col
                    break
                elif not prev_cogs_col:
                    prev_cogs_col = col
        
        # 전년 출고매출(V-) 컬럼 찾기
        for col in df_prev.columns:
            col_str = str(col)
            if '출고매출' in col_str or ('출고' in col_str and '매출' in col_str) or '부가세제외 실판매액' in col_str:
                prev_출고매출_col = col
                break
        
        if prev_brand_col:
            for brand_code in ALL_BRAND_CODES:
                brand_df = df_prev[df_prev[prev_brand_col].astype(str).str.strip() == brand_code]
                
                if not brand_df.empty:
                    # 공통 채널 분리 (영업비만 공통 채널에서 추출, 매출원가는 공통 포함하여 전체 합산)
                    if prev_channel_col:
                        prev_brand_df = brand_df[brand_df[prev_channel_col].astype(str).str.strip() != '공통']
                        op_expense_df = brand_df[brand_df[prev_channel_col].astype(str).str.strip() == '공통']
                    else:
                        prev_brand_df = brand_df
                        op_expense_df = pd.DataFrame()
                    
                    # TAG매출 (공통 제외)
                    if prev_tag_col:
                        for val in prev_brand_df[prev_tag_col]:
                            prev_sums['tag_revenue'] += extract_numeric(val)
                    
                    # 실판매출(V+) (공통 포함하여 전체 합산)
                    if prev_sales_col:
                        for val in brand_df[prev_sales_col]:
                            prev_sums['revenue'] += extract_numeric(val)
                    
                    # 출고매출(V-) 또는 부가세제외 실판매액 (공통 포함하여 전체 합산)
                    if prev_출고매출_col:
                        for val in brand_df[prev_출고매출_col]:
                            prev_sums['출고매출'] += extract_numeric(val)
                    
                    # 매출원가 (공통 포함하여 전체 합산)
                    if prev_cogs_col:
                        for val in brand_df[prev_cogs_col]:
                            prev_sums['cog'] += extract_numeric(val)
                    
                    # 매출총이익 (컬럼에서 직접 가져오기, 공통 포함하여 전체 합산)
                    if prev_gross_profit_col:
                        for val in brand_df[prev_gross_profit_col]:
                            prev_sums['gross_profit'] += extract_numeric(val)
                    
                    # 직접비 (공통 제외)
                    if prev_direct_cost_col:
                        for val in prev_brand_df[prev_direct_cost_col]:
                            prev_sums['direct_cost'] += extract_numeric(val)
                    
                    # 직접비 세부 항목 집계 (직접비 마스터 사용, 공통 제외)
                    direct_cost_details = aggregate_direct_cost_details_overview(prev_brand_df, direct_cost_master)
                    for key in prev_direct_cost_details:
                        prev_direct_cost_details[key] += direct_cost_details[key]
                    
                    # 전년 영업비 (공통 채널)
                    if prev_op_expense_col and not op_expense_df.empty:
                        for val in op_expense_df[prev_op_expense_col]:
                            prev_sums['op_expense'] += extract_numeric(val)
        
        # 매출총이익 계산 (컬럼이 없으면 출고매출(V-) - 매출원가)
        if prev_sums['gross_profit'] == 0 and prev_sums['출고매출'] > 0:
            prev_sums['gross_profit'] = prev_sums['출고매출'] - prev_sums['cog']
        elif prev_sums['gross_profit'] == 0:
            # 최종 폴백: 실판매출 - 매출원가
            prev_sums['gross_profit'] = prev_sums['revenue'] - prev_sums['cog']
        
        # 직접이익 = 매출총이익 - 직접비 (직접 계산)
        prev_sums['direct_profit'] = prev_sums['gross_profit'] - prev_sums['direct_cost']
        
        # 억원 단위로 변환하여 저장
        overview_pl['tagRevenue']['prev'] = round(prev_sums['tag_revenue'] / 100000000, 2)
        overview_pl['revenue']['prev'] = round(prev_sums['revenue'] / 100000000, 2)
        overview_pl['cog']['prev'] = round(prev_sums['cog'] / 100000000, 2)
        overview_pl['grossProfit']['prev'] = round(prev_sums['gross_profit'] / 100000000, 2)
        overview_pl['directCost']['prev'] = round(prev_sums['direct_cost'] / 100000000, 2)
        overview_pl['directProfit']['prev'] = round(prev_sums['direct_profit'] / 100000000, 2)
        overview_pl['operatingExpense']['prev'] = round(prev_sums['op_expense'] / 100000000, 2)
        
        # 직접비 세부 항목 (억원 단위)
        for key in prev_direct_cost_details:
            overview_pl['directCostDetail'][key]['prev'] = round(prev_direct_cost_details[key] / 100000000, 2)
        
        # 전년 영업이익 = 전년 직접이익 - 전년 영업비
        overview_pl['opProfit']['prev'] = round(
            overview_pl['directProfit']['prev'] - overview_pl['operatingExpense']['prev'], 2
        )
    
    # 4. YOY 및 Achievement 계산 - 브랜드별 로직과 동일
    # YOY: (forecast / prev) * 100
    if overview_pl['tagRevenue']['prev'] > 0:
        overview_pl['tagRevenue']['yoy'] = round((overview_pl['tagRevenue']['forecast'] / overview_pl['tagRevenue']['prev']) * 100)
    if overview_pl['revenue']['prev'] > 0:
        overview_pl['revenue']['yoy'] = round((overview_pl['revenue']['forecast'] / overview_pl['revenue']['prev']) * 100)
    if overview_pl['cog']['prev'] > 0:
        overview_pl['cog']['yoy'] = round((overview_pl['cog']['forecast'] / overview_pl['cog']['prev']) * 100)
    if overview_pl['grossProfit']['prev'] > 0:
        overview_pl['grossProfit']['yoy'] = round((overview_pl['grossProfit']['forecast'] / overview_pl['grossProfit']['prev']) * 100)
    if overview_pl['directCost']['prev'] > 0:
        overview_pl['directCost']['yoy'] = round((overview_pl['directCost']['forecast'] / overview_pl['directCost']['prev']) * 100)
    if overview_pl['directProfit']['prev'] > 0:
        overview_pl['directProfit']['yoy'] = round((overview_pl['directProfit']['forecast'] / overview_pl['directProfit']['prev']) * 100)
    if overview_pl['operatingExpense']['prev'] > 0:
        overview_pl['operatingExpense']['yoy'] = round((overview_pl['operatingExpense']['forecast'] / overview_pl['operatingExpense']['prev']) * 100)
    if overview_pl['opProfit']['prev'] > 0:
        overview_pl['opProfit']['yoy'] = round((overview_pl['opProfit']['forecast'] / overview_pl['opProfit']['prev']) * 100)
    
    # 직접비 세부 항목 YOY
    for detail_name in overview_pl['directCostDetail'].keys():
        if overview_pl['directCostDetail'][detail_name]['prev'] > 0:
            overview_pl['directCostDetail'][detail_name]['yoy'] = round(
                (overview_pl['directCostDetail'][detail_name]['forecast'] / overview_pl['directCostDetail'][detail_name]['prev']) * 100
            )
    
    # Achievement: (forecast / target) * 100
    if overview_pl['tagRevenue']['target'] > 0:
        overview_pl['tagRevenue']['achievement'] = round((overview_pl['tagRevenue']['forecast'] / overview_pl['tagRevenue']['target']) * 100)
    if overview_pl['revenue']['target'] > 0:
        overview_pl['revenue']['achievement'] = round((overview_pl['revenue']['forecast'] / overview_pl['revenue']['target']) * 100)
    if overview_pl['cog']['target'] > 0:
        overview_pl['cog']['achievement'] = round((overview_pl['cog']['forecast'] / overview_pl['cog']['target']) * 100)
    if overview_pl['grossProfit']['target'] > 0:
        overview_pl['grossProfit']['achievement'] = round((overview_pl['grossProfit']['forecast'] / overview_pl['grossProfit']['target']) * 100)
    if overview_pl['directCost']['target'] > 0:
        overview_pl['directCost']['achievement'] = round((overview_pl['directCost']['forecast'] / overview_pl['directCost']['target']) * 100)
    if overview_pl['directProfit']['target'] > 0:
        overview_pl['directProfit']['achievement'] = round((overview_pl['directProfit']['forecast'] / overview_pl['directProfit']['target']) * 100)
    if overview_pl['operatingExpense']['target'] > 0:
        overview_pl['operatingExpense']['achievement'] = round((overview_pl['operatingExpense']['forecast'] / overview_pl['operatingExpense']['target']) * 100)
    if overview_pl['opProfit']['target'] > 0:
        overview_pl['opProfit']['achievement'] = round((overview_pl['opProfit']['forecast'] / overview_pl['opProfit']['target']) * 100)
    
    # 직접비 세부 항목 Achievement
    for detail_name in overview_pl['directCostDetail'].keys():
        if overview_pl['directCostDetail'][detail_name]['target'] > 0:
            overview_pl['directCostDetail'][detail_name]['achievement'] = round(
                (overview_pl['directCostDetail'][detail_name]['forecast'] / overview_pl['directCostDetail'][detail_name]['target']) * 100
            )
    
    print(f"  [OK] 전체 손익계산서 데이터 생성 완료")
    print(f"    실판매출: 전년 {overview_pl['revenue']['prev']}억, 목표 {overview_pl['revenue']['target']}억, 월말예상 {overview_pl['revenue']['forecast']}억")
    print(f"    영업이익: 전년 {overview_pl['opProfit']['prev']}억, 목표 {overview_pl['opProfit']['target']}억, 월말예상 {overview_pl['opProfit']['forecast']}억")
    
    return overview_pl


def create_waterfall_data(overview_pl: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    손익구조 Waterfall 데이터 생성
    
    Args:
        overview_pl: 전체 손익계산서 데이터
    
    Returns:
        List[Dict]: Waterfall 차트 데이터
    """
    print("\n[3/5] Waterfall 차트 데이터 생성 중...")
    
    # 월말예상(forecast) 기준
    revenue = overview_pl['revenue']['forecast']
    cogs = overview_pl['cog']['forecast']
    gross_profit = overview_pl['grossProfit']['forecast']
    direct_cost = overview_pl['directCost']['forecast']
    direct_profit = overview_pl['directProfit']['forecast']
    op_expense = overview_pl['operatingExpense']['forecast']
    op_profit = overview_pl['opProfit']['forecast']
    
    # 매출총이익 계산 (없는 경우)
    if gross_profit == 0:
        gross_profit = round(revenue - cogs, 1)
    
    # 직접이익 계산 (없는 경우)
    if direct_profit == 0:
        direct_profit = round(gross_profit - direct_cost, 1)
    
    waterfall_data = [
        {'label': '실판매출', 'value': revenue, 'type': 'total'},
        {'label': '매출원가(-)', 'value': cogs, 'type': 'decrease'},
        {'label': '매출총이익', 'value': gross_profit, 'type': 'subtotal'},
        {'label': '직접비(-)', 'value': direct_cost, 'type': 'decrease'},
        {'label': '직접이익', 'value': direct_profit, 'type': 'subtotal'},
        {'label': '영업비(-)', 'value': op_expense, 'type': 'decrease'},
        {'label': '영업이익', 'value': op_profit, 'type': 'result'}
    ]
    
    print(f"  [OK] Waterfall 데이터 생성 완료")
    print(f"    실판매출 {revenue}억 → 매출원가 -{cogs}억 → 매출총이익 {gross_profit}억")
    print(f"    → 직접비 -{direct_cost}억 → 직접이익 {direct_profit}억 → 영업비 -{op_expense}억 → 영업이익 {op_profit}억")
    
    return waterfall_data


def safe_float(value) -> Optional[float]:
    """안전하게 float 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value) -> Optional[int]:
    """안전하게 int 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_percentage(value) -> Optional[str]:
    """퍼센트 문자열을 처리 (예: "169%" -> "169%", 빈값 -> None)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    value_str = str(value).strip()
    if value_str == '':
        return None
    # 숫자만 있는 경우 퍼센트로 변환
    try:
        num = float(value_str.replace('%', ''))
        return f"{int(num)}%"
    except (ValueError, TypeError):
        return value_str


def process_overview_clothing_csv(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    당시즌의류 브랜드별 현황 CSV 처리 (전체현황용 - 모든 브랜드 통합)
    
    Args:
        file_path: CSV 파일 경로
    
    Returns:
        Dict: {브랜드코드: [아이템 데이터 리스트]}
    """
    print(f"\n[당시즌의류] 파일 읽는 중: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"  [WARNING] 파일을 찾을 수 없습니다: {file_path}")
        return {}
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"  총 {len(df)}개 행 로드됨")
    
    # 브랜드 코드 매핑 (브랜드명 -> 브랜드 코드)
    brand_name_to_code = {
        'MLB': 'M',
        'MLB KIDS': 'I',
        'DISCOVERY': 'X',
        'DUVETICA': 'V',
        'SERGIO': 'ST',
        'SUPRA': 'W'
    }
    
    result = {}
    
    for _, row in df.iterrows():
        brand_name = str(row['브랜드']).strip()
        brand_code = brand_name_to_code.get(brand_name, brand_name)
        
        if brand_code not in result:
            result[brand_code] = []
        
        item_data = {
            "category": str(row['대분류']).strip() if pd.notna(row['대분류']) else "",
            "subCategory": str(row['중분류']).strip() if pd.notna(row['중분류']) else "",
            "itemCode": str(row['아이템코드']).strip() if pd.notna(row['아이템코드']) else "",
            "itemName": str(row['아이템명(한글)']).strip() if pd.notna(row['아이템명(한글)']) else "",
            "orderTag": safe_float(row['발주(TAG)']),
            "orderYoY": safe_float(row['전년비(발주)']),
            "weeklySalesTag": safe_float(row['주간판매매출(TAG)']),
            "weeklyYoY": safe_float(row['전년비(주간)']),
            "cumSalesTag": safe_float(row['누적판매매출(TAG)']),
            "cumYoY": safe_float(row['전년비(누적)']),
            "cumSalesRate": safe_float(row['누적판매율당년']),
            "cumSalesRateDiff": safe_float(row['누적판매율차이']),
            "pyClosingSalesRate": safe_float(row['전년마감판매율']),
            # 판매율 계산용 원본 TAG가 데이터 추가 (clothingSummary 집계용)
            "storageTagAmt": safe_float(row.get('누적입고TAG가', 0)),  # 당년 누적입고TAG가
            "cumSalesTagPy": safe_float(row.get('전년누적판매TAG가', 0)),  # 전년 누적판매TAG가
            "storageTagAmtPy": safe_float(row.get('전년누적입고TAG가', 0)),  # 전년 누적입고TAG가
            "cumSalesTagPyEnd": safe_float(row.get('전년마감누적판매TAG가', 0)),  # 전년마감 누적판매TAG가
            "storageTagAmtPyEnd": safe_float(row.get('전년마감누적입고TAG가', 0))  # 전년마감 누적입고TAG가
        }
        
        result[brand_code].append(item_data)
    
    print(f"  브랜드 수: {len(result)}")
    for brand_code, items in result.items():
        brand_name = BRAND_CODE_MAP.get(brand_code, brand_code)
        print(f"    {brand_code} ({brand_name}): {len(items)}개 아이템")
    
    return result


def process_overview_acc_csv(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    ACC 재고주수 분석 CSV 처리 (전체현황용 - 모든 브랜드 통합)
    
    Args:
        file_path: CSV 파일 경로
    
    Returns:
        Dict: {브랜드코드: [아이템 데이터 리스트]}
    """
    print(f"\n[ACC 재고주수] 파일 읽는 중: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"  [WARNING] 파일을 찾을 수 없습니다: {file_path}")
        return {}
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"  총 {len(df)}개 행 로드됨")
    
    result = {}
    
    for _, row in df.iterrows():
        brand_code = str(row['브랜드코드']).strip()
        
        if brand_code not in result:
            result[brand_code] = []
        
        item_data = {
            "category": str(row['카테고리']).strip() if pd.notna(row['카테고리']) else "",
            "itemCode": str(row['아이템']).strip() if pd.notna(row['아이템']) else "",
            "itemName": str(row['아이템명']).strip() if pd.notna(row['아이템명']) else "",
            "saleQty": safe_int(row['판매수량']),
            "saleAmt": safe_int(row['판매매출']),
            "yoyRate": parse_percentage(row['전년비']),
            "shareRate": str(row['비중']).strip() if pd.notna(row['비중']) else "0%",
            "avg4wSaleQty": safe_float(row['4주평균판매량']),
            "stockQty": safe_int(row['재고']),
            "stockWeeks": safe_float(row['재고주수']),
            "pyStockWeeks": safe_float(row['전년재고주수']),
            "stockWeeksDiff": safe_float(row['재고주수차이(당년-전년)'])
        }
        
        result[brand_code].append(item_data)
    
    print(f"  브랜드 수: {len(result)}")
    for brand_code, items in result.items():
        brand_name = BRAND_CODE_MAP.get(brand_code, brand_code)
        print(f"    {brand_code} ({brand_name}): {len(items)}개 아이템")
    
    return result


def create_brand_stock_metadata(date_str: str) -> Dict[str, Any]:
    """
    브랜드 재고 분석 메타데이터 생성 (generate_brand_stock_analysis.py와 동일한 로직)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜
    
    Returns:
        Dict: brandStockMetadata
    """
    from datetime import timedelta
    
    now = datetime.now()
    update_dt = datetime.strptime(date_str, '%Y%m%d')
    
    # 주간 날짜 계산 (월요일 ~ 일요일)
    weekday = update_dt.weekday()
    monday = update_dt - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    
    # 당년 주간 (최근 월~일)
    cy_week_start = monday.strftime('%Y-%m-%d')
    cy_week_end = sunday.strftime('%Y-%m-%d')
    
    # 전년 동주차 (1년 전)
    py_monday = monday.replace(year=monday.year - 1)
    py_sunday = sunday.replace(year=sunday.year - 1)
    py_week_start = py_monday.strftime('%Y-%m-%d')
    py_week_end = py_sunday.strftime('%Y-%m-%d')
    
    # 현재 시즌 판단 (월 기준)
    current_month = update_dt.month
    if current_month >= 8 or current_month <= 2:
        cy_season = f"{update_dt.year % 100}F" if current_month >= 8 else f"{(update_dt.year - 1) % 100}F"
        py_season = f"{(int(cy_season[:2]) - 1)}F"
    else:
        cy_season = f"{update_dt.year % 100}S"
        py_season = f"{(int(cy_season[:2]) - 1)}S"
    
    # 전년 시즌 마감일 (F/W는 다음해 2월 말)
    if 'F' in cy_season:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year + 1}-02-28"
    else:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year}-07-31"
    
    metadata = {
        "updateDate": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
        "cyWeekStart": cy_week_start,
        "cyWeekEnd": cy_week_end,
        "pyWeekStart": py_week_start,
        "pyWeekEnd": py_week_end,
        "cySeason": cy_season,
        "pySeason": py_season,
        "pySeasonEnd": py_season_end,
        "generatedAt": now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    return metadata


def create_overview_stock_analysis_data(date_str: str) -> Dict[str, Any]:
    """
    전체현황 당시즌의류/ACC 판매율 분석 데이터 생성
    
    Args:
        date_str: YYYYMMDD 형식의 날짜
    
    Returns:
        Dict: {brandStockMetadata: {...}, clothingBrandStatus: {...}, accStockAnalysis: {...}, 
               clothingSummary: {...}, accSummary: {...}}
    """
    print(f"\n[5/5] 전체현황 당시즌의류/ACC 판매율 분석 데이터 생성 중...")
    
    # ETC 파일은 업데이트 날짜의 연월 폴더에 있음 (YYYYMM)
    update_year_month = date_str[:6]  # 20251201 -> 202512
    etc_dir = os.path.join(RAW_DIR, update_year_month, "ETC")
    
    # 파일 경로
    clothing_csv = os.path.join(etc_dir, f"당시즌의류_브랜드별현황_{date_str}.csv")
    acc_csv = os.path.join(etc_dir, f"ACC_재고주수분석_{date_str}.csv")
    
    # 없으면 분석월 폴더에서도 찾기 (하위 호환성)
    if not os.path.exists(clothing_csv) or not os.path.exists(acc_csv):
        year_month = extract_year_month_from_date(date_str)
        etc_dir = os.path.join(RAW_DIR, year_month, "ETC")
        clothing_csv = os.path.join(etc_dir, f"당시즌의류_브랜드별현황_{date_str}.csv")
        acc_csv = os.path.join(etc_dir, f"ACC_재고주수분석_{date_str}.csv")
    
    # 데이터 처리
    clothing_data = {}
    acc_data = {}
    
    if os.path.exists(clothing_csv):
        clothing_data = process_overview_clothing_csv(clothing_csv)
    else:
        print(f"  [WARNING] 당시즌의류 파일을 찾을 수 없습니다: {clothing_csv}")
    
    if os.path.exists(acc_csv):
        acc_data = process_overview_acc_csv(acc_csv)
    else:
        print(f"  [WARNING] ACC 재고주수 파일을 찾을 수 없습니다: {acc_csv}")
    
    # 메타데이터 생성
    brand_stock_metadata = create_brand_stock_metadata(date_str)
    
    # 브랜드별 요약 통계 (당시즌의류) + 아이템별 집계
    clothing_summary = {}
    overall_item_totals = {}  # 전체 아이템별 집계 (모든 브랜드 합산)
    
    for brand_code, items in clothing_data.items():
        if not isinstance(items, list):
            continue
        
        # 기본 집계
        total_order_tag = sum(item.get("orderTag", 0) or 0 for item in items)  # 발주TAG (참고용)
        total_weekly_sales = sum(item.get("weeklySalesTag", 0) or 0 for item in items)
        total_cum_sales = sum(item.get("cumSalesTag", 0) or 0 for item in items)
        
        # 판매율 계산용 원본 데이터 집계
        total_storage_tag_amt = sum(item.get("storageTagAmt", 0) or 0 for item in items)
        total_cum_sales_tag = sum(item.get("cumSalesTag", 0) or 0 for item in items)
        total_storage_tag_amt_py = sum(item.get("storageTagAmtPy", 0) or 0 for item in items)
        total_cum_sales_tag_py = sum(item.get("cumSalesTagPy", 0) or 0 for item in items)
        total_storage_tag_amt_py_end = sum(item.get("storageTagAmtPyEnd", 0) or 0 for item in items)
        total_cum_sales_tag_py_end = sum(item.get("cumSalesTagPyEnd", 0) or 0 for item in items)
        
        # 판매율 계산
        cum_sales_rate = None
        cum_sales_rate_py = None
        py_closing_sales_rate = None
        cum_sales_rate_diff = None
        
        if total_storage_tag_amt > 0:
            cum_sales_rate = total_cum_sales_tag / total_storage_tag_amt
        if total_storage_tag_amt_py > 0:
            cum_sales_rate_py = total_cum_sales_tag_py / total_storage_tag_amt_py
        if total_storage_tag_amt_py_end > 0:
            py_closing_sales_rate = total_cum_sales_tag_py_end / total_storage_tag_amt_py_end
        if cum_sales_rate is not None and cum_sales_rate_py is not None:
            cum_sales_rate_diff = round(cum_sales_rate - cum_sales_rate_py, 4)
        
        clothing_summary[brand_code] = {
            "itemCount": len(items),
            "totalOrderTag": total_order_tag,
            "totalWeeklySales": total_weekly_sales,
            "totalCumSales": total_cum_sales,
            # 판매율 데이터 추가
            "cumSalesRate": cum_sales_rate,
            "cumSalesRatePy": cum_sales_rate_py,
            "cumSalesRateDiff": cum_sales_rate_diff,
            "pyClosingSalesRate": py_closing_sales_rate
        }
        
        # 아이템별 집계 (모든 브랜드의 동일 아이템 합산)
        for item in items:
            item_code = str(item.get("itemCode", "")).strip()
            if not item_code:
                continue
            
            if item_code not in overall_item_totals:
                overall_item_totals[item_code] = {
                    'cumSales': 0, 'stor': 0, 
                    'cumSalesPy': 0, 'storPy': 0, 
                    'tagPyEnd': 0, 'storPyEnd': 0
                }
            
            overall_item_totals[item_code]['cumSales'] += item.get("cumSalesTag", 0) or 0
            overall_item_totals[item_code]['stor'] += item.get("storageTagAmt", 0) or 0
            overall_item_totals[item_code]['cumSalesPy'] += item.get("cumSalesTagPy", 0) or 0
            overall_item_totals[item_code]['storPy'] += item.get("storageTagAmtPy", 0) or 0
            overall_item_totals[item_code]['tagPyEnd'] += item.get("cumSalesTagPyEnd", 0) or 0
            overall_item_totals[item_code]['storPyEnd'] += item.get("storageTagAmtPyEnd", 0) or 0
    
    # 아이템별 판매율 계산 (브랜드별 세분화)
    item_totals_overall_rates = {}
    
    # 브랜드별로 아이템 데이터 구조화
    for brand_code, items in clothing_data.items():
        if not isinstance(items, list):
            continue
        
        for item in items:
            item_code = str(item.get("itemCode", "")).strip()
            item_name = str(item.get("itemName", "")).strip()
            parent_category = str(item.get("parent", "")).strip()
            
            if not item_code:
                continue
            
            # 아이템별 구조 초기화
            if item_code not in item_totals_overall_rates:
                item_totals_overall_rates[item_code] = {
                    "name": item_name,
                    "brands": {}
                }
            
            # 브랜드별 판매율 데이터 추가
            cum_sales_tag = item.get("cumSalesTag", 0) or 0
            storage_tag_amt = item.get("storageTagAmt", 0) or 0
            cum_sales_tag_py = item.get("cumSalesTagPy", 0) or 0
            storage_tag_amt_py = item.get("storageTagAmtPy", 0) or 0
            cum_sales_tag_py_end = item.get("cumSalesTagPyEnd", 0) or 0
            storage_tag_amt_py_end = item.get("storageTagAmtPyEnd", 0) or 0
            weekly_sales_tag = item.get("weeklySalesTag", 0) or 0
            
            cum_sales_rate = cum_sales_tag / storage_tag_amt if storage_tag_amt > 0 else None
            cum_sales_rate_py = cum_sales_tag_py / storage_tag_amt_py if storage_tag_amt_py > 0 else None
            py_closing_sales_rate = cum_sales_tag_py_end / storage_tag_amt_py_end if storage_tag_amt_py_end > 0 else None
            
            item_totals_overall_rates[item_code]["brands"][brand_code] = {
                "cumSalesRate": cum_sales_rate,
                "cumSalesRatePy": cum_sales_rate_py,
                "pyClosingSalesRate": py_closing_sales_rate,
                "cumSalesTag": cum_sales_tag,
                "weeklySalesTag": weekly_sales_tag,
                "parent": parent_category
            }
    
    # ★ 각 아이템별 전체 브랜드 합산 판매율 계산 (overall_item_totals 사용) ★
    for item_code, totals in overall_item_totals.items():
        if item_code not in item_totals_overall_rates:
            item_totals_overall_rates[item_code] = {
                "name": "",
                "brands": {}
            }
        
        # 아이템별 전체 판매율 계산 (모든 브랜드 합산)
        item_cum_rate = totals['cumSales'] / totals['stor'] if totals['stor'] > 0 else None
        item_cum_rate_py = totals['cumSalesPy'] / totals['storPy'] if totals['storPy'] > 0 else None
        item_py_closing_rate = totals['tagPyEnd'] / totals['storPyEnd'] if totals['storPyEnd'] > 0 else None
        item_rate_diff = round(item_cum_rate - item_cum_rate_py, 4) if (item_cum_rate and item_cum_rate_py) else None
        
        # 아이템별 전체 판매율 추가 (대시보드에서 직접 참조)
        item_totals_overall_rates[item_code]["cumSalesRate"] = item_cum_rate
        item_totals_overall_rates[item_code]["cumSalesRatePy"] = item_cum_rate_py
        item_totals_overall_rates[item_code]["cumSalesRateDiff"] = item_rate_diff
        item_totals_overall_rates[item_code]["pyClosingSalesRate"] = item_py_closing_rate
        item_totals_overall_rates[item_code]["totalCumSales"] = totals['cumSales']
        item_totals_overall_rates[item_code]["totalStorage"] = totals['stor']
    
    # 전체 판매율 계산 (모든 브랜드 합산)
    total_storage_all = sum(vals['stor'] for vals in overall_item_totals.values())
    total_cum_sales_all = sum(vals['cumSales'] for vals in overall_item_totals.values())
    total_storage_py_all = sum(vals['storPy'] for vals in overall_item_totals.values())
    total_cum_sales_py_all = sum(vals['cumSalesPy'] for vals in overall_item_totals.values())
    total_storage_py_end_all = sum(vals['storPyEnd'] for vals in overall_item_totals.values())
    total_cum_sales_py_end_all = sum(vals['tagPyEnd'] for vals in overall_item_totals.values())
    
    overall_cum_rate = total_cum_sales_all / total_storage_all if total_storage_all > 0 else None
    overall_cum_rate_py = total_cum_sales_py_all / total_storage_py_all if total_storage_py_all > 0 else None
    overall_py_closing_rate = total_cum_sales_py_end_all / total_storage_py_end_all if total_storage_py_end_all > 0 else None
    overall_rate_diff = round(overall_cum_rate - overall_cum_rate_py, 4) if (overall_cum_rate and overall_cum_rate_py) else None
    
    # clothingSummary에 전체 판매율 추가
    clothing_summary['overall'] = {
        "itemCount": sum(s.get('itemCount', 0) for s in clothing_summary.values()),
        "totalOrderTag": sum(s.get('totalOrderTag', 0) for s in clothing_summary.values()),
        "totalWeeklySales": sum(s.get('totalWeeklySales', 0) for s in clothing_summary.values()),
        "totalCumSales": total_cum_sales_all,
        "cumSalesRate": overall_cum_rate,
        "cumSalesRatePy": overall_cum_rate_py,
        "cumSalesRateDiff": overall_rate_diff,
        "pyClosingSalesRate": overall_py_closing_rate
    }
    
    # ★ clothingItemRatesOverall에도 전체 평균(overall) 추가 ★
    item_totals_overall_rates['overall'] = {
        "cumSalesRate": overall_cum_rate,
        "cumSalesRatePy": overall_cum_rate_py,
        "cumSalesRateDiff": overall_rate_diff,
        "pyClosingSalesRate": overall_py_closing_rate,
        "totalCumSales": total_cum_sales_all,
        "totalStorage": total_storage_all,
        "totalCumSalesPy": total_cum_sales_py_all,
        "totalStoragePy": total_storage_py_all,
        "totalCumSalesPyEnd": total_cum_sales_py_end_all,
        "totalStoragePyEnd": total_storage_py_end_all
    }
    
    print(f"  [집계] 전체 아이템별 판매율: {len(item_totals_overall_rates)}개 아이템")
    print(f"  [집계] 전체 판매율: {overall_cum_rate * 100:.1f}%" if overall_cum_rate else "  [집계] 전체 판매율: N/A")
    
    # 브랜드별 요약 통계 (ACC)
    acc_summary = {}
    for brand_code, items in acc_data.items():
        if not isinstance(items, list):
            continue
        acc_summary[brand_code] = {
            "itemCount": len(items),
            "totalSaleQty": sum(item.get("saleQty", 0) or 0 for item in items),
            "totalSaleAmt": sum(item.get("saleAmt", 0) or 0 for item in items),
            "totalStockQty": sum(item.get("stockQty", 0) or 0 for item in items)
        }
    
    result = {
        "brandStockMetadata": brand_stock_metadata,
        "clothingBrandStatus": clothing_data,
        "accStockAnalysis": acc_data,
        "clothingSummary": clothing_summary,
        "accSummary": acc_summary,
        "clothingItemRatesOverall": item_totals_overall_rates
    }
    
    print(f"  [OK] 전체현황 판매율 분석 데이터 생성 완료")
    print(f"    당시즌의류: {len(clothing_data)}개 브랜드")
    print(f"    ACC 재고주수: {len(acc_data)}개 브랜드")
    print(f"    아이템별 판매율: {len(item_totals_overall_rates)}개 아이템")
    
    return result


def extract_brand_total_plan_data(df_plan: pd.DataFrame) -> Dict[str, float]:
    """
    계획 파일에서 브랜드별 전체 계획 매출 추출 (내수합계)
    
    Args:
        df_plan: 계획 데이터프레임
    
    Returns:
        Dict: {브랜드코드: 전체계획매출액(원 단위)}
    """
    print("\n[계산] 브랜드별 전체 계획 매출 추출 중...")
    
    # 컬럼 찾기
    brand_col = find_column(df_plan, ['브랜드'])
    channel_col = find_column(df_plan, ['채널'])
    sales_col = None
    
    for col in df_plan.columns:
        col_str = str(col).strip()
        if '실판매액' in col_str and '[v+]' in col_str.lower() and sales_col is None:
            sales_col = col
            break
    
    if not brand_col or not channel_col or not sales_col:
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        print(f"    브랜드 컬럼: {brand_col}")
        print(f"    채널 컬럼: {channel_col}")
        print(f"    매출 컬럼: {sales_col}")
        return {}
    
    print(f"  브랜드 컬럼: {brand_col}")
    print(f"  채널 컬럼: {channel_col}")
    print(f"  매출 컬럼: {sales_col}")
    
    result = {}
    
    for brand_code in ALL_BRAND_CODES:
        brand_df = df_plan[df_plan[brand_col].astype(str).str.strip() == brand_code]
        plan_domestic = brand_df[brand_df[channel_col].astype(str).str.strip() == '내수합계']
        
        if not plan_domestic.empty:
            row = plan_domestic.iloc[0]
            total_sales = extract_numeric(row[sales_col])
            result[brand_code] = total_sales
            print(f"  {brand_code} ({BRAND_CODE_MAP.get(brand_code, brand_code)}): {total_sales:,.0f}원")
    
    return result




def create_cumulative_trend_data(date_str: str, recent_weeks: int = 4) -> Optional[Dict[str, Any]]:
    """
    월중누적매출추이 데이터 생성 (전체 브랜드 합산)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜
        recent_weeks: 표시할 최근 주차 수 (기본값: 4)
    
    Returns:
        Dict: {weeks: [...], current: [...], prev: [...], target: float}
    """
    print(f"\n[4/5] 월중누적매출추이 데이터 생성 중 (최근 {recent_weeks}주)...")
    
    # ETC 파일은 업데이트 날짜의 연월 폴더에 있음 (YYYYMM)
    update_year_month = date_str[:6]  # 20251201 -> 202512
    
    # weekly_sales_trend 파일 찾기 (업데이트 날짜의 연월 폴더에서)
    etc_dir = os.path.join(RAW_DIR, update_year_month, "ETC")
    trend_file = os.path.join(etc_dir, f"weekly_sales_trend_{date_str}.csv")
    
    # 없으면 분석월 폴더에서도 찾기 (하위 호환성)
    if not os.path.exists(trend_file):
        year_month = extract_year_month_from_date(date_str)
        etc_dir = os.path.join(RAW_DIR, year_month, "ETC")
        trend_file = os.path.join(etc_dir, f"weekly_sales_trend_{date_str}.csv")
    
    if not os.path.exists(trend_file):
        print(f"  [WARNING] 주차별 매출추세 파일을 찾을 수 없습니다: {trend_file}")
        return None
    
    print(f"  [LOAD] 주차별 매출추세: {os.path.basename(trend_file)}")
    df = pd.read_csv(trend_file, encoding="utf-8-sig")
    
    # 컬럼 찾기
    brand_col = find_column(df, ['브랜드'])
    type_col = find_column(df, ['구분'])
    end_date_col = find_column(df, ['종료일'])
    sales_col = find_column(df, ['실판매출'])
    
    if not all([brand_col, type_col, end_date_col, sales_col]):
        print(f"  [WARNING] 필요한 컬럼을 찾을 수 없습니다.")
        return None
    
    # 주차 목록 추출 및 정렬
    all_weeks = sorted(df[end_date_col].unique())
    
    # 최근 N주만 선택
    weeks = all_weeks[-recent_weeks:] if len(all_weeks) > recent_weeks else all_weeks
    print(f"  전체 {len(all_weeks)}주 중 최근 {len(weeks)}주 선택")
    
    # 전체 브랜드 주차별 합산
    current_data = []
    prev_data = []
    
    for week in weeks:
        week_df = df[df[end_date_col] == week]
        
        # 당년 합산
        current_sum = 0.0
        prev_sum = 0.0
        
        for brand_code in ALL_BRAND_CODES:
            brand_df = week_df[week_df[brand_col].astype(str).str.strip() == brand_code]
            
            current_row = brand_df[brand_df[type_col].astype(str).str.strip() == '당년']
            if not current_row.empty:
                current_sum += current_row[sales_col].apply(extract_numeric).sum()
            
            prev_row = brand_df[brand_df[type_col].astype(str).str.strip() == '전년']
            if not prev_row.empty:
                prev_sum += prev_row[sales_col].apply(extract_numeric).sum()
        
        # 백만원 단위로 변환
        current_data.append(round(current_sum / 1000000, 1))
        prev_data.append(round(prev_sum / 1000000, 1))
    
    # 누적 계산 (선택된 주차 내에서의 누적)
    cumulative_current = []
    cumulative_prev = []
    cum_c, cum_p = 0.0, 0.0
    
    for i in range(len(current_data)):
        cum_c += current_data[i]
        cum_p += prev_data[i]
        cumulative_current.append(round(cum_c, 1))
        cumulative_prev.append(round(cum_p, 1))
    
    # 주차 레이블 생성 (날짜에서 MM/DD 형식)
    week_labels = []
    for week in weeks:
        try:
            # 날짜 형식 파싱 (예: 2025-11-02)
            parts = str(week).split('-')
            if len(parts) >= 3:
                month = int(parts[1])
                day = int(parts[2])
                week_labels.append(f"{month}/{day}")
            else:
                week_labels.append(str(week))
        except:
            week_labels.append(str(week))
    
    cumulative_trend = {
        'weeks': week_labels,
        'weekly_current': current_data,
        'weekly_prev': prev_data,
        'cumulative_current': cumulative_current,
        'cumulative_prev': cumulative_prev
    }
    
    print(f"  [OK] 월중누적매출추이 데이터 생성 완료 (최근 {len(weeks)}주)")
    print(f"    주차: {week_labels}")
    print(f"    누적 당년: {cumulative_current}")
    print(f"    누적 전년: {cumulative_prev}")
    
    return cumulative_trend


def update_data_js(date_str: str, by_brand: List, overview_pl: Dict, 
                   waterfall_data: List, cumulative_trend: Dict = None,
                   overview_kpi: Dict = None):
    """
    data_YYYYMMDD.js 파일에 overview 데이터 추가
    """
    print("\n[5/5] data.js 파일 업데이트 중...")
    
    data_js_path = os.path.join(PUBLIC_DIR, f"data_{date_str}.js")
    
    # 기존 파일 읽기
    if os.path.exists(data_js_path):
        with open(data_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"  [LOAD] 기존 파일: {os.path.basename(data_js_path)}")
    else:
        # 새 파일 생성
        content = f"""// 트리맵 + 메트릭 데이터 (window 전역 할당 전용)
// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

(function(){{
  if (typeof window !== 'undefined') {{
    if (typeof window.D === 'undefined') {{
      window.D = {{}};
    }}
  }}
  console.log('[Data.js] window variables updated');
}})();
"""
        print(f"  [NEW] 새 파일 생성: {os.path.basename(data_js_path)}")
    
    # 기존 overview 데이터 및 관련 주석 제거 (있는 경우)
    patterns_to_remove = [
        r"\s*// Overview 데이터 \(자동 생성:.*?\)\s*",
        r"\s*// window\.D 초기화 확인\s*",
        r"\s*if \(typeof window\.D === 'undefined'\) \{\s*window\.D = \{\};\s*\}\s*",
        r"window\.D\.by_brand\s*=\s*\[[\s\S]*?\];",
        r"window\.D\.overviewPL\s*=\s*\{[\s\S]*?\};",
        r"window\.overviewPL\s*=\s*\{[\s\S]*?\};",
        r"window\.D\.waterfallData\s*=\s*\[[\s\S]*?\];",
        r"window\.D\.cumulativeTrendData\s*=\s*\{[\s\S]*?\};",
        r"window\.D\.overviewKPI\s*=\s*\{[\s\S]*?\};"
    ]
    
    for pattern in patterns_to_remove:
        content = re.sub(pattern, '', content)
    
    # 연속된 빈 줄 제거 (3개 이상 → 1개로)
    content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
    
    # console.log 앞에 데이터 삽입 (window.D 초기화 포함)
    insert_code = f"""
  // Overview 데이터 (자동 생성: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
  // window.D 초기화 확인
  if (typeof window.D === 'undefined') {{
    window.D = {{}};
  }}
  window.D.by_brand = {json.dumps(by_brand, ensure_ascii=False, indent=2)};
  window.D.overviewPL = {json.dumps(overview_pl, ensure_ascii=False, indent=2)};
  window.overviewPL = {json.dumps(overview_pl, ensure_ascii=False, indent=2)};
  window.D.waterfallData = {json.dumps(waterfall_data, ensure_ascii=False, indent=2)};
"""
    
    if cumulative_trend:
        insert_code += f"""  window.D.cumulativeTrendData = {json.dumps(cumulative_trend, ensure_ascii=False, indent=2)};
"""
    
    if overview_kpi:
        insert_code += f"""  window.D.overviewKPI = {json.dumps(overview_kpi, ensure_ascii=False, indent=2)};
"""
    
    # console.log 줄 앞에 삽입
    if "console.log('[Data.js]" in content:
        content = content.replace(
            "console.log('[Data.js]",
            insert_code + "\n  console.log('[Data.js]"
        )
    else:
        # console.log가 없으면 })(); 앞에 삽입
        content = content.replace("})();", insert_code + "\n})();")
    
    # 파일 저장
    with open(data_js_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    file_size = os.path.getsize(data_js_path) / 1024
    print(f"  [OK] 파일 저장 완료: {data_js_path}")
    print(f"    파일 크기: {file_size:.2f} KB")


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="전체현황 데이터 업데이트")
    parser.add_argument("date", help="YYYYMMDD 형식의 날짜 (예: 20251124)")
    
    args = parser.parse_args()
    date_str = args.date
    
    # 날짜 형식 검증
    if len(date_str) != 8 or not date_str.isdigit():
        print("[ERROR] 날짜 형식이 올바르지 않습니다. YYYYMMDD 형식이어야 합니다.")
        sys.exit(1)
    
    print("=" * 60)
    print("전체현황 데이터 업데이트")
    print("=" * 60)
    print(f"날짜: {date_str}")
    print(f"연월: {extract_year_month_from_date(date_str)}")
    
    try:
        # 1. by_brand 데이터 생성
        by_brand = create_by_brand_data(date_str)
        
        # 1.5. 전체 손익계산서 데이터 생성 (KPI 계산에 필요)
        overview_pl = create_overview_pl_data(date_str)
        
        # 1.6. 전체 현황 KPI 생성
        overview_kpi = create_overview_kpi(date_str, by_brand, overview_pl)
        
        # 1.7. 브랜드별 전체 계획 데이터 추출
        year_month = extract_year_month_from_date(date_str)
        df_plan = load_plan_data(year_month)
        brand_total_plan = {}
        if df_plan is not None:
            brand_total_plan = extract_brand_total_plan_data(df_plan)
        
        # 2. 전체 손익계산서 데이터 생성 (이미 위에서 생성됨, 재사용)
        # overview_pl = create_overview_pl_data(date_str)  # 제거 (중복)
        
        # 3. Waterfall 데이터 생성
        waterfall_data = create_waterfall_data(overview_pl)
        
        # 4. 월중누적매출추이 데이터 생성
        cumulative_trend = create_cumulative_trend_data(date_str)
        
        # 5. 전체현황 당시즌의류/ACC 판매율 분석 데이터 생성
        stock_analysis = create_overview_stock_analysis_data(date_str)
        
        # 6. data.js 파일 업데이트
        update_data_js(date_str, by_brand, overview_pl, waterfall_data, cumulative_trend, overview_kpi)
        
        print("\n" + "=" * 60)
        print("[OK] 전체현황 데이터 업데이트 완료")
        print("=" * 60)
        
        # ★★★ JSON 파일로 저장 (개별 파일 + 통합 파일) ★★★
        json_dir = os.path.join(PUBLIC_DIR, "data", date_str)
        os.makedirs(json_dir, exist_ok=True)
        
        # 1. 개별 JSON 파일로 저장 (대시보드 영역별)
        overview_kpi_path = os.path.join(json_dir, "overview_kpi.json")
        with open(overview_kpi_path, "w", encoding="utf-8") as f:
            json.dump(overview_kpi, f, ensure_ascii=False, indent=2)
        print(f"  [OK] JSON 저장: overview_kpi.json")
        
        # by_brand 데이터도 별도로 저장 (하위 호환성)
        overview_by_brand_path = os.path.join(json_dir, "overview_by_brand.json")
        with open(overview_by_brand_path, "w", encoding="utf-8") as f:
            json.dump(by_brand, f, ensure_ascii=False, indent=2)
        print(f"  [OK] JSON 저장: overview_by_brand.json ({len(by_brand)}개 브랜드)")
        
        # 브랜드별 계획 데이터 저장 (전체 브랜드 레이더 차트용)
        if brand_total_plan:
            # 브랜드 코드를 브랜드명으로 변환하여 저장
            brand_plan_data = {}
            for brand_code, plan_value in brand_total_plan.items():
                brand_name = BRAND_CODE_MAP.get(brand_code, brand_code)
                # 브랜드명 키로 변환 (MLB, MLB_KIDS 등)
                brand_key = brand_name.replace(' ', '_') if ' ' in brand_name else brand_name
                brand_plan_data[brand_key] = plan_value / 100000000  # 억원 단위로 변환
            
            brand_plan_path = os.path.join(json_dir, "brand_plan.json")
            with open(brand_plan_path, "w", encoding="utf-8") as f:
                json.dump(brand_plan_data, f, ensure_ascii=False, indent=2)
            print(f"  [OK] JSON 저장: brand_plan.json (전체 브랜드 레이더 차트용)")
        
        overview_pl_path = os.path.join(json_dir, "overview_pl.json")
        with open(overview_pl_path, "w", encoding="utf-8") as f:
            json.dump(overview_pl, f, ensure_ascii=False, indent=2)
        print(f"  [OK] JSON 저장: overview_pl.json")
        
        if waterfall_data:
            overview_waterfall_path = os.path.join(json_dir, "overview_waterfall.json")
            with open(overview_waterfall_path, "w", encoding="utf-8") as f:
                json.dump(waterfall_data, f, ensure_ascii=False, indent=2)
            print(f"  [OK] JSON 저장: overview_waterfall.json ({len(waterfall_data)}개 항목)")
        
        if cumulative_trend:
            overview_trend_path = os.path.join(json_dir, "overview_trend.json")
            with open(overview_trend_path, "w", encoding="utf-8") as f:
                json.dump(cumulative_trend, f, ensure_ascii=False, indent=2)
            print(f"  [OK] JSON 저장: overview_trend.json")
        
        # 전체현황 당시즌의류/ACC 판매율 분석 데이터 저장
        if stock_analysis:
            # overview_stock_analysis.json 저장 (overview.json에 포함용)
            overview_stock_path = os.path.join(json_dir, "overview_stock_analysis.json")
            with open(overview_stock_path, "w", encoding="utf-8") as f:
                json.dump(stock_analysis, f, ensure_ascii=False, indent=2)
            print(f"  [OK] JSON 저장: overview_stock_analysis.json")
            print(f"    당시즌의류: {len(stock_analysis.get('clothingBrandStatus', {}))}개 브랜드")
            print(f"    ACC 재고주수: {len(stock_analysis.get('accStockAnalysis', {}))}개 브랜드")
            
            # stock_analysis.json도 저장 (대시보드가 이 파일을 로드함)
            stock_analysis_path = os.path.join(json_dir, "stock_analysis.json")
            with open(stock_analysis_path, "w", encoding="utf-8") as f:
                json.dump(stock_analysis, f, ensure_ascii=False, indent=2)
            print(f"  [OK] JSON 저장: stock_analysis.json (대시보드용)")
        
        # 2. 통합 overview.json도 저장 (하위 호환성 유지)
        overview_json = {
            "by_brand": by_brand,
            "overviewPL": overview_pl,
            "overviewKPI": overview_kpi,
            "waterfallData": waterfall_data,
            "cumulativeTrendData": cumulative_trend,
            "stockAnalysis": stock_analysis
        }
        overview_json_path = os.path.join(json_dir, "overview.json")
        with open(overview_json_path, "w", encoding="utf-8") as f:
            json.dump(overview_json, f, ensure_ascii=False, indent=2)
        print(f"  [OK] JSON 저장: overview.json (통합 파일, 하위 호환성)")
        
        # 요약 출력
        print("\n[요약]")
        total_sales = sum(b['SALES'] for b in by_brand)
        total_profit = sum(b['DIRECT_PROFIT'] for b in by_brand)
        print(f"  전체 매출 (6개 브랜드): {total_sales:.1f}억원")
        print(f"  전체 직접이익: {total_profit:.1f}억원")
        print(f"  전체 영업이익 (월말예상): {overview_pl['opProfit']['forecast']:.1f}억원")
        
    except Exception as e:
        print(f"\n[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

