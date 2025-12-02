"""
브랜드별 분석 KPI 업데이트 스크립트
===================================

계산 항목:
1. 실판매출(현시점): ke30 Shop 파일에서 브랜드별 실판매액 합계
2. 직접이익(현시점): ke30 Shop 파일에서 브랜드별 직접이익 합계
3. 직접이익율(현시점): 직접이익/실판매출*1.1*100
4. 할인율: (1 - 실판매액 / TAG매출) * 100
5. 목표대비 진척율: plan 파일 내수합계의 실판매액[v+] 대비 ke30 실판매액

출력:
- data_YYYYMMDD.js 파일의 brandKPI 객체를 업데이트/병합
- 기존 필드(tag, cogs, grossProfit 등)는 유지하고 새 필드 추가

작성일: 2025-01
수정일: 2025-01 (data.js 통합 방식으로 변경)
"""

import os
import sys
import json
import re
import pandas as pd
from typing import Dict, Optional
from datetime import datetime
from path_utils import get_current_year_file_path, get_plan_file_path, extract_year_month_from_date, get_previous_year_file_path, get_previous_year_month

ROOT = os.path.dirname(os.path.dirname(__file__))
PUBLIC_DIR = os.path.join(ROOT, "public")

def extract_numeric(value) -> float:
    """숫자 문자열(콤마 포함)을 float로 변환"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # 문자열인 경우 콤마 제거
    if isinstance(value, str):
        value = value.replace(",", "").replace(" ", "").strip()
        if value == "" or value == "-":
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def load_ke30_shop_data(date_str: str) -> pd.DataFrame:
    """
    ke30 Shop 파일 로드
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251124")
    
    Returns:
        pd.DataFrame: Shop 데이터
    """
    year_month = extract_year_month_from_date(date_str)
    filename = f"ke30_{date_str}_{year_month}_Shop.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] ke30 Shop 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_plan_data(year_month: str) -> pd.DataFrame:
    """
    계획 전처리 완료 파일 로드
    
    Args:
        year_month: YYYYMM 형식의 연월 (예: "202511")
    
    Returns:
        pd.DataFrame: 계획 데이터
    """
    filepath = get_plan_file_path(year_month)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_forecast_shop_data(date_str: str) -> pd.DataFrame:
    """
    forecast Shop 파일 로드 (월말예상 데이터)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251124")
    
    Returns:
        pd.DataFrame: forecast Shop 데이터
    """
    year_month = extract_year_month_from_date(date_str)
    filename = f"forecast_{date_str}_{year_month}_Shop.csv"
    filepath = get_current_year_file_path(date_str, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] forecast Shop 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def load_previous_year_shop_data(year_month: str) -> pd.DataFrame:
    """
    전년 Shop 파일 로드
    
    Args:
        year_month: YYYYMM 형식의 연월 (예: "202511")
    
    Returns:
        pd.DataFrame: 전년 Shop 데이터
    """
    filename = f"previous_rawdata_{year_month}_Shop.csv"
    filepath = get_previous_year_file_path(year_month, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[ERROR] 전년 Shop 파일을 찾을 수 없습니다: {filepath}")
    
    print(f"[읽기] {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
    
    return df

def calculate_brand_kpi(date_str: str, year_month: Optional[str] = None) -> pd.DataFrame:
    """
    브랜드별 KPI 계산
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251124")
        year_month: YYYYMM 형식의 연월 (없으면 date_str에서 추출)
    
    Returns:
        pd.DataFrame: 브랜드별 KPI 데이터프레임
    """
    if year_month is None:
        year_month = extract_year_month_from_date(date_str)
    
    print("=" * 60)
    print("브랜드별 KPI 계산")
    print("=" * 60)
    print(f"날짜: {date_str}")
    print(f"연월: {year_month}")
    
    # 1. ke30 Shop 데이터 로드 (현시점 데이터)
    df_shop = load_ke30_shop_data(date_str)
    
    # 1-1. forecast Shop 데이터 로드 (월말예상 데이터)
    df_forecast = None
    try:
        df_forecast = load_forecast_shop_data(date_str)
    except FileNotFoundError as e:
        print(f"  [WARNING] forecast 파일을 찾을 수 없어 월말예상 계산을 건너뜁니다: {e}")
    
    # 컬럼명 확인
    sales_col = None
    tag_col = None
    profit_col = None
    brand_col = None
    channel_col = None
    operating_expense_col = None
    
    # 실판매액 컬럼 찾기: 부가세 포함 실판매액 우선 (V+ > 합계 : 실판매액 > V-)
    for col in df_shop.columns:
        col_str = str(col)
        if '실판매액' in col_str and '합계' in col_str:
            if sales_col is None:
                sales_col = col
            # 부가세 포함 (V+) 우선
            if '(V+)' in col_str or 'v+' in col_str.lower():
                sales_col = col
                break
            # "합계 : 실판매액" (부가세 포함일 가능성, V-가 아닌 경우)
            if '(V-)' not in col_str:
                sales_col = col
        if 'TAG가' in col_str and '합계' in col_str:
            tag_col = col
        if '직접이익' in str(col):
            profit_col = col
        if '브랜드' in str(col):
            brand_col = col
        if '채널' in str(col) and '명' in str(col):
            channel_col = col
        if '영업비' in str(col):
            operating_expense_col = col
    
    if not sales_col:
        raise ValueError(f"[ERROR] 실판매액 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df_shop.columns)}")
    if not tag_col:
        print(f"  [WARNING] TAG가 컬럼을 찾을 수 없어 할인율 계산을 건너뜁니다. 사용 가능한 컬럼: {list(df_shop.columns)}")
    if not profit_col:
        raise ValueError(f"[ERROR] 직접이익 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df_shop.columns)}")
    if not brand_col:
        raise ValueError(f"[ERROR] 브랜드 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df_shop.columns)}")
    
    print(f"  실판매액 컬럼: {sales_col}")
    if tag_col:
        print(f"  TAG가 컬럼: {tag_col}")
    print(f"  직접이익 컬럼: {profit_col}")
    print(f"  브랜드 컬럼: {brand_col}")
    if channel_col:
        print(f"  채널 컬럼: {channel_col}")
    if operating_expense_col:
        print(f"  영업비 컬럼: {operating_expense_col}")
    
    # 2. 브랜드별 집계 (ke30 데이터)
    print(f"\n[계산] 브랜드별 집계 (ke30)...")
    brand_agg = {}
    
    for brand in df_shop[brand_col].unique():
        if pd.isna(brand):
            continue
        
        brand_str = str(brand).strip()
        brand_df = df_shop[df_shop[brand_col] == brand]
        
        # 실판매액 합계
        sales_sum = 0.0
        for val in brand_df[sales_col]:
            sales_sum += extract_numeric(val)
        
        # TAG가 합계 (할인율 계산용)
        tag_sum = 0.0
        if tag_col:
            for val in brand_df[tag_col]:
                tag_sum += extract_numeric(val)
        
        # 직접이익 합계
        profit_sum = 0.0
        for val in brand_df[profit_col]:
            profit_sum += extract_numeric(val)
        
        # 영업비 합계 (채널명='공통'인 경우만)
        operating_expense_sum = 0.0
        if operating_expense_col and channel_col:
            common_df = brand_df[brand_df[channel_col].astype(str).str.strip() == '공통']
            if not common_df.empty:
                for val in common_df[operating_expense_col]:
                    operating_expense_sum += extract_numeric(val)
        
        brand_agg[brand_str] = {
            '실판매출(현시점)': sales_sum,
            '직접이익(현시점)': profit_sum,
            '영업비(현시점)': operating_expense_sum
        }
        
        # 직접이익율 계산 (직접이익/실판매출*1.1*100)
        if sales_sum > 0:
            profit_rate = (profit_sum / sales_sum) * 1.1 * 100
        else:
            profit_rate = 0.0
        
        brand_agg[brand_str]['직접이익율(현시점)'] = profit_rate
        
        # 할인율 계산: (1 - 실판매액 / TAG매출) * 100
        if tag_col and tag_sum > 0:
            discount_rate = (1 - (sales_sum / tag_sum)) * 100
        else:
            discount_rate = 0.0
        
        brand_agg[brand_str]['할인율'] = discount_rate
        
        print(f"  {brand_str}: 직접이익 {profit_sum:,.0f}원, 영업비 {operating_expense_sum:,.0f}원")
    
    # 2-1. forecast 데이터로부터 월말예상 값 계산
    if df_forecast is not None:
        print(f"\n[계산] 브랜드별 월말예상 집계 (forecast)...")
        
        # forecast 파일 컬럼 찾기
        forecast_sales_col = None
        forecast_profit_col = None
        forecast_brand_col = None
        forecast_tag_col = None
        forecast_channel_col = None
        forecast_op_expense_col = None  # ★ 영업비 컬럼 ★
        
        for col in df_forecast.columns:
            col_str = str(col)
            if '실판매액' in col_str and '합계' in col_str:
                if forecast_sales_col is None:
                    forecast_sales_col = col
                # 부가세 포함 (V+) 우선
                if '(V+)' in col_str or 'v+' in col_str.lower():
                    forecast_sales_col = col
                    break
                # "합계 : 실판매액" (부가세 포함일 가능성, V-가 아닌 경우)
                if '(V-)' not in col_str:
                    forecast_sales_col = col
            if 'TAG가' in col_str and '합계' in col_str:
                forecast_tag_col = col
            if '직접이익' in str(col):
                forecast_profit_col = col
            if '브랜드' in str(col):
                forecast_brand_col = col
            if '채널' in str(col):
                forecast_channel_col = col
            # ★ 영업비 컬럼 찾기 ★
            if '영업비' in col_str and '합계' not in col_str.lower():
                forecast_op_expense_col = col
        
        if forecast_sales_col and forecast_profit_col and forecast_brand_col:
            print(f"  forecast 실판매액 컬럼: {forecast_sales_col}")
            if forecast_tag_col:
                print(f"  forecast TAG가 컬럼: {forecast_tag_col}")
            print(f"  forecast 직접이익 컬럼: {forecast_profit_col}")
            print(f"  forecast 브랜드 컬럼: {forecast_brand_col}")
            
            for brand in df_forecast[forecast_brand_col].unique():
                if pd.isna(brand):
                    continue
                
                brand_str = str(brand).strip()
                brand_df = df_forecast[df_forecast[forecast_brand_col] == brand]
                
                # 월말예상 실판매액 합계
                forecast_sales_sum = 0.0
                for val in brand_df[forecast_sales_col]:
                    forecast_sales_sum += extract_numeric(val)
                
                # 월말예상 직접이익 합계
                forecast_profit_sum = 0.0
                for val in brand_df[forecast_profit_col]:
                    forecast_profit_sum += extract_numeric(val)
                
                # 월말예상 TAG가 합계 (할인율 계산용)
                forecast_tag_sum = 0.0
                if forecast_tag_col:
                    for val in brand_df[forecast_tag_col]:
                        forecast_tag_sum += extract_numeric(val)
                
                # 브랜드별 집계에 추가
                if brand_str not in brand_agg:
                    brand_agg[brand_str] = {
                        '실판매출(현시점)': 0.0,
                        '직접이익(현시점)': 0.0,
                        '직접이익율(현시점)': 0.0,
                        '할인율': 0.0
                    }
                
                brand_agg[brand_str]['실판매출(월말예상)'] = forecast_sales_sum
                brand_agg[brand_str]['직접이익(월말예상)'] = forecast_profit_sum
                
                # 월말예상 직접이익율 계산 (직접이익/실판매출*1.1*100)
                if forecast_sales_sum > 0:
                    forecast_profit_rate = (forecast_profit_sum / forecast_sales_sum) * 1.1 * 100
                else:
                    forecast_profit_rate = 0.0
                
                brand_agg[brand_str]['직접이익율(월말예상)'] = forecast_profit_rate
                
                # 월말예상 할인율 계산
                if forecast_tag_col and forecast_tag_sum > 0:
                    forecast_discount_rate = (1 - (forecast_sales_sum / forecast_tag_sum)) * 100
                else:
                    forecast_discount_rate = 0.0
                
                brand_agg[brand_str]['할인율(월말예상)'] = forecast_discount_rate
                
                # ★ 월말예상 영업비 집계 ('공통' 채널에서) ★
                forecast_op_expense_sum = 0.0
                if forecast_op_expense_col and forecast_channel_col:
                    common_df = brand_df[brand_df[forecast_channel_col].astype(str).str.contains('공통', na=False)]
                    for val in common_df[forecast_op_expense_col]:
                        forecast_op_expense_sum += extract_numeric(val)
                brand_agg[brand_str]['영업비(월말예상)'] = forecast_op_expense_sum
                
                print(f"  {brand_str}: 월말예상 실판매출 {forecast_sales_sum:,.0f}원, 직접이익 {forecast_profit_sum:,.0f}원, 영업비 {forecast_op_expense_sum:,.0f}원")
        else:
            print(f"  [WARNING] forecast 파일에서 필요한 컬럼을 찾을 수 없습니다.")
            print(f"    sales_col: {forecast_sales_col}, profit_col: {forecast_profit_col}, brand_col: {forecast_brand_col}")
    else:
        # forecast 파일이 없으면 모든 브랜드에 대해 0으로 설정
        for brand_str in brand_agg.keys():
            brand_agg[brand_str]['실판매출(월말예상)'] = 0.0
            brand_agg[brand_str]['직접이익(월말예상)'] = 0.0
            brand_agg[brand_str]['직접이익율(월말예상)'] = 0.0
            brand_agg[brand_str]['할인율(월말예상)'] = 0.0
            brand_agg[brand_str]['영업비(월말예상)'] = 0.0
    
    # 3. 전년 데이터 로드 및 브랜드별 집계
    print(f"\n[계산] 브랜드별 전년 데이터 집계...")
    previous_year_month = get_previous_year_month(date_str)
    df_previous = None
    try:
        df_previous = load_previous_year_shop_data(year_month)  # 같은 연월 폴더에서 전년 파일 찾기
        
        # 전년 데이터 컬럼 찾기
        prev_tag_col = None
        prev_sales_col = None
        prev_profit_col = None
        prev_brand_col = None
        prev_operating_expense_col = None
        prev_channel_col = None
        
        for col in df_previous.columns:
            col_str = str(col)
            if 'TAG매출액' in col_str or 'TAG가' in col_str:
                prev_tag_col = col
            # ★ 부가세 포함 실매출액 우선 사용 ★
            if col_str == '실매출액':
                prev_sales_col = col
            elif ('실매출액' in col_str or '실판매액' in col_str) and prev_sales_col is None:
                # 부가세제외 컬럼은 fallback으로만 사용
                if '부가세제외' not in col_str:
                    prev_sales_col = col
            if '직접이익' in col_str:
                prev_profit_col = col
            if '브랜드' in col_str:
                prev_brand_col = col
            if '영업비' in col_str:
                prev_operating_expense_col = col
            if '채널' in col_str and '명' in col_str:
                prev_channel_col = col
        
        if prev_tag_col and prev_sales_col and prev_profit_col and prev_brand_col:
            print(f"  전년 TAG매출액 컬럼: {prev_tag_col}")
            print(f"  전년 실매출액 컬럼: {prev_sales_col}")
            print(f"  전년 직접이익 컬럼: {prev_profit_col}")
            print(f"  전년 브랜드 컬럼: {prev_brand_col}")
            
            for brand in df_previous[prev_brand_col].unique():
                if pd.isna(brand):
                    continue
                
                brand_str = str(brand).strip()
                brand_df = df_previous[df_previous[prev_brand_col] == brand]
                
                # 전년 TAG매출액 합계
                prev_tag_sum = 0.0
                for val in brand_df[prev_tag_col]:
                    prev_tag_sum += extract_numeric(val)
                
                # 전년 실매출액 합계
                prev_sales_sum = 0.0
                for val in brand_df[prev_sales_col]:
                    prev_sales_sum += extract_numeric(val)
                
                # 전년 직접이익 합계
                prev_profit_sum = 0.0
                for val in brand_df[prev_profit_col]:
                    prev_profit_sum += extract_numeric(val)
                
                # 전년 영업비 합계 (채널명='공통'인 경우만)
                prev_operating_expense_sum = 0.0
                if prev_operating_expense_col and prev_channel_col:
                    # 채널명='공통'인 행만 필터링
                    common_df = brand_df[brand_df[prev_channel_col].astype(str).str.strip() == '공통']
                    if not common_df.empty and prev_operating_expense_col in common_df.columns:
                        for val in common_df[prev_operating_expense_col]:
                            prev_operating_expense_sum += extract_numeric(val)
                
                # 브랜드별 집계에 추가
                if brand_str not in brand_agg:
                    brand_agg[brand_str] = {
                        '실판매출(현시점)': 0.0,
                        '직접이익(현시점)': 0.0,
                        '직접이익율(현시점)': 0.0,
                        '할인율': 0.0
                    }
                
                brand_agg[brand_str]['TAG매출액(전년)'] = prev_tag_sum
                brand_agg[brand_str]['실매출액(전년)'] = prev_sales_sum
                brand_agg[brand_str]['직접이익(전년)'] = prev_profit_sum
                brand_agg[brand_str]['영업비(전년)'] = prev_operating_expense_sum
                
                # 전년 영업이익 계산 (직접이익 - 영업비)
                prev_operating_profit = prev_profit_sum - prev_operating_expense_sum
                brand_agg[brand_str]['영업이익(전년)'] = prev_operating_profit
                
                # 전년 할인율 계산
                if prev_tag_sum > 0:
                    prev_discount_rate = (1 - (prev_sales_sum / prev_tag_sum)) * 100
                else:
                    prev_discount_rate = 0.0
                brand_agg[brand_str]['할인율(전년)'] = prev_discount_rate
                
                # 전년 직접이익율 계산
                if prev_sales_sum > 0:
                    prev_profit_rate = (prev_profit_sum / prev_sales_sum) * 1.1 * 100
                else:
                    prev_profit_rate = 0.0
                brand_agg[brand_str]['직접이익율(전년)'] = prev_profit_rate
                
                # 전년 영업이익율 계산 (영업이익 / 매출 × 1.1 × 100)
                if prev_sales_sum > 0:
                    prev_operating_profit_rate = (prev_operating_profit / prev_sales_sum) * 1.1 * 100
                else:
                    prev_operating_profit_rate = 0.0
                brand_agg[brand_str]['영업이익율(전년)'] = prev_operating_profit_rate
                
                print(f"  {brand_str}: 전년 실매출액 {prev_sales_sum:,.0f}원, 직접이익 {prev_profit_sum:,.0f}원, 영업비 {prev_operating_expense_sum:,.0f}원, 영업이익 {prev_operating_profit:,.0f}원")
        else:
            print(f"  [WARNING] 전년 파일에서 필요한 컬럼을 찾을 수 없습니다.")
            print(f"    tag_col: {prev_tag_col}, sales_col: {prev_sales_col}, profit_col: {prev_profit_col}, brand_col: {prev_brand_col}")
            for brand_str in brand_agg.keys():
                brand_agg[brand_str]['TAG매출액(전년)'] = 0.0
                brand_agg[brand_str]['실매출액(전년)'] = 0.0
                brand_agg[brand_str]['직접이익(전년)'] = 0.0
                brand_agg[brand_str]['영업비(전년)'] = 0.0
                brand_agg[brand_str]['영업이익(전년)'] = 0.0
                brand_agg[brand_str]['할인율(전년)'] = 0.0
                brand_agg[brand_str]['직접이익율(전년)'] = 0.0
                brand_agg[brand_str]['영업이익율(전년)'] = 0.0
    except FileNotFoundError as e:
        print(f"  [WARNING] 전년 파일을 찾을 수 없어 전년 데이터 계산을 건너뜁니다: {e}")
        for brand_str in brand_agg.keys():
            brand_agg[brand_str]['TAG매출액(전년)'] = 0.0
            brand_agg[brand_str]['실매출액(전년)'] = 0.0
            brand_agg[brand_str]['직접이익(전년)'] = 0.0
            brand_agg[brand_str]['영업비(전년)'] = 0.0
            brand_agg[brand_str]['영업이익(전년)'] = 0.0
            brand_agg[brand_str]['할인율(전년)'] = 0.0
            brand_agg[brand_str]['직접이익율(전년)'] = 0.0
            brand_agg[brand_str]['영업이익율(전년)'] = 0.0
    
    # 4. 계획 데이터 로드 및 목표대비 계산
    print(f"\n[계산] 브랜드별 목표대비 계산...")
    try:
        df_plan = load_plan_data(year_month)
        
        # 계획 데이터 구조 확인: "구분" 컬럼과 "내수합계" 컬럼이 있는지 확인
        plan_구분_col = None
        plan_내수합계_col = None
        plan_brand_col = None
        
        for col in df_plan.columns:
            col_str = str(col).strip()
            if col_str == '구분':
                plan_구분_col = col
            if col_str == '내수합계':
                plan_내수합계_col = col
            if '브랜드' in col_str:
                plan_brand_col = col
        
        if plan_구분_col and plan_내수합계_col:
            print(f"  계획 구분 컬럼: {plan_구분_col}")
            print(f"  계획 내수합계 컬럼: {plan_내수합계_col}")
            
            # 계획 데이터를 브랜드별로 그룹화 (브랜드 컬럼이 있는 경우)
            if plan_brand_col:
                plan_brands = df_plan[plan_brand_col].unique()
            else:
                # 브랜드 컬럼이 없으면 모든 행을 하나의 그룹으로 처리
                plan_brands = [None]
            
            for plan_brand in plan_brands:
                if plan_brand is not None:
                    plan_df = df_plan[df_plan[plan_brand_col] == plan_brand]
                    brand_str = str(plan_brand).strip()
                else:
                    plan_df = df_plan
                    brand_str = None
                
                # 각 구분별로 값 추출
                plan_tag_vp = 0.0
                plan_sales_vp = 0.0
                plan_discount_rate = 0.0
                plan_profit = 0.0
                plan_profit_rate = 0.0
                plan_operating_profit = 0.0
                plan_operating_profit_rate = 0.0
                
                for _, row in plan_df.iterrows():
                    구분 = str(row[plan_구분_col]).strip()
                    내수합계값 = extract_numeric(row[plan_내수합계_col])
                    
                    if 'TAG가' in 구분 and '[v+]' in 구분:
                        plan_tag_vp = 내수합계값
                    elif '실판매액' in 구분 and '[v+]' in 구분:
                        plan_sales_vp = 내수합계값
                    elif '할인율' in 구분:
                        plan_discount_rate = 내수합계값
                    elif 구분 == '직접이익':
                        plan_profit = 내수합계값
                    elif '직접이익율' in 구분:
                        plan_profit_rate = 내수합계값
                    elif 구분 == '영업이익':
                        plan_operating_profit = 내수합계값
                    elif '영업이익율' in 구분:
                        plan_operating_profit_rate = 내수합계값
                
                # 브랜드별로 계획 데이터 저장
                if brand_str and brand_str in brand_agg:
                    brand_agg[brand_str]['TAG가(목표)'] = plan_tag_vp
                    brand_agg[brand_str]['실판매액(목표)'] = plan_sales_vp
                    brand_agg[brand_str]['할인율(목표)'] = plan_discount_rate
                    brand_agg[brand_str]['직접이익(목표)'] = plan_profit
                    brand_agg[brand_str]['직접이익율(목표)'] = plan_profit_rate
                    brand_agg[brand_str]['영업이익(목표)'] = plan_operating_profit
                    brand_agg[brand_str]['영업이익율(목표)'] = plan_operating_profit_rate
                    
                    # 목표대비 진척율 계산 (기존 로직)
                    if plan_sales_vp > 0:
                        ke30_sales = brand_agg[brand_str]['실판매출(현시점)']
                        progress_rate = (ke30_sales / plan_sales_vp) * 100
                    else:
                        progress_rate = 0.0
                    brand_agg[brand_str]['목표대비 진척율'] = progress_rate
                    
                    # 목표대비 할인율 차이 계산
                    current_discount = brand_agg[brand_str].get('할인율', 0.0)
                    brand_agg[brand_str]['목표대비 할인율'] = current_discount - plan_discount_rate
                    
                    print(f"  {brand_str}: 목표 실판매액 {plan_sales_vp:,.0f}원, 목표 할인율 {plan_discount_rate:.2f}%")
                elif brand_str is None:
                    # 브랜드 컬럼이 없는 경우, 모든 브랜드에 동일한 계획 데이터 적용
                    for brand_key in brand_agg.keys():
                        brand_agg[brand_key]['TAG가(목표)'] = plan_tag_vp
                        brand_agg[brand_key]['실판매액(목표)'] = plan_sales_vp
                        brand_agg[brand_key]['할인율(목표)'] = plan_discount_rate
                        brand_agg[brand_key]['직접이익(목표)'] = plan_profit
                        brand_agg[brand_key]['직접이익율(목표)'] = plan_profit_rate
                        brand_agg[brand_key]['영업이익(목표)'] = plan_operating_profit
                        brand_agg[brand_key]['영업이익율(목표)'] = plan_operating_profit_rate
                        
                        if plan_sales_vp > 0:
                            ke30_sales = brand_agg[brand_key]['실판매출(현시점)']
                            progress_rate = (ke30_sales / plan_sales_vp) * 100
                        else:
                            progress_rate = 0.0
                        brand_agg[brand_key]['목표대비 진척율'] = progress_rate
                        
                        current_discount = brand_agg[brand_key].get('할인율', 0.0)
                        brand_agg[brand_key]['목표대비 할인율'] = current_discount - plan_discount_rate
        else:
            # 기존 로직 (컬럼 기반)
            plan_sales_col = None
            plan_brand_col = None
            plan_channel_col = None
            plan_operating_profit_col = None
            plan_operating_profit_rate_col = None
            plan_direct_profit_col = None
            plan_operating_expense_col = None
            
            for col in df_plan.columns:
                col_str = str(col)
                if '실판매액' in col_str and '[v+]' in col_str:
                    plan_sales_col = col
                if '브랜드' in col_str:
                    plan_brand_col = col
                if '채널' in col_str:
                    plan_channel_col = col
                # 영업이익 컬럼 찾기 (영업이익율이 아닌 영업이익)
                if col_str == '영업이익' or (col_str.startswith('영업이익') and '율' not in col_str and '%' not in col_str):
                    plan_operating_profit_col = col
                # 직접이익 컬럼 찾기
                if col_str == '직접이익' or (col_str.startswith('직접이익') and '율' not in col_str and '%' not in col_str):
                    plan_direct_profit_col = col
                if '영업이익율' in col_str or '영업이익(%)' in col_str:
                    plan_operating_profit_rate_col = col
                if col_str == '직접이익' or col_str.startswith('직접이익') and '율' not in col_str:
                    plan_direct_profit_col = col
                if col_str == '영업비' or col_str.startswith('영업비'):
                    plan_operating_expense_col = col
            
            if plan_sales_col and plan_brand_col and plan_channel_col:
                print(f"  계획 실판매액 컬럼: {plan_sales_col}")
                print(f"  계획 브랜드 컬럼: {plan_brand_col}")
                print(f"  계획 채널 컬럼: {plan_channel_col}")
                if plan_operating_profit_col:
                    print(f"  계획 영업이익 컬럼: {plan_operating_profit_col}")
                if plan_operating_expense_col:
                    print(f"  계획 영업비 컬럼: {plan_operating_expense_col}")
                
                # 내수합계 행만 필터링
                if '내수합계' in df_plan[plan_channel_col].values:
                    plan_domestic = df_plan[df_plan[plan_channel_col] == '내수합계']
                    
                    for _, row in plan_domestic.iterrows():
                        brand = str(row[plan_brand_col]).strip()
                        plan_sales = extract_numeric(row[plan_sales_col])
                        
                        # 영업이익 가져오기
                        plan_op_profit = 0.0
                        if plan_operating_profit_col:
                            plan_op_profit = extract_numeric(row[plan_operating_profit_col])
                        
                        # 영업이익율 가져오기
                        plan_op_profit_rate = 0.0
                        if plan_operating_profit_rate_col:
                            plan_op_profit_rate = extract_numeric(row[plan_operating_profit_rate_col])
                        
                        # 영업비 가져오기
                        plan_op_expense = 0.0
                        if plan_operating_expense_col:
                            plan_op_expense = extract_numeric(row[plan_operating_expense_col])
                        
                        if brand in brand_agg:
                            ke30_sales = brand_agg[brand]['실판매출(현시점)']
                            ke30_profit = brand_agg[brand]['직접이익(현시점)']
                            forecast_profit = brand_agg[brand].get('직접이익(월말예상)', 0)
                            
                            # 직접이익 계획 가져오기
                            plan_direct_profit_val = 0.0
                            if plan_direct_profit_col:
                                plan_direct_profit_val = extract_numeric(row[plan_direct_profit_col])
                            
                            # ★ 진척율 계산: 직접이익 기준 ★
                            # 현시점 진척율 = 현시점 직접이익 / 계획 직접이익
                            if plan_direct_profit_val > 0:
                                progress_rate_current = (ke30_profit / plan_direct_profit_val) * 100
                            else:
                                progress_rate_current = 0.0
                            
                            # 월말예상 진척율 = 월말예상 직접이익 / 계획 직접이익
                            if plan_direct_profit_val > 0 and forecast_profit > 0:
                                progress_rate_forecast = (forecast_profit / plan_direct_profit_val) * 100
                            else:
                                progress_rate_forecast = 0.0
                            
                            brand_agg[brand]['목표대비 진척율'] = progress_rate_current
                            brand_agg[brand]['목표대비 진척율(월말예상)'] = progress_rate_forecast
                            brand_agg[brand]['실판매액(목표)'] = plan_sales
                            brand_agg[brand]['직접이익(목표)'] = plan_direct_profit_val
                            brand_agg[brand]['영업이익(목표)'] = plan_op_profit
                            brand_agg[brand]['영업이익율(목표)'] = plan_op_profit_rate
                            brand_agg[brand]['영업비(목표)'] = plan_op_expense
                            # 영어 키로도 저장 (인코딩 문제 방지)
                            brand_agg[brand]['op_profit_plan'] = plan_op_profit
                            brand_agg[brand]['op_profit_rate_plan'] = plan_op_profit_rate
                            brand_agg[brand]['op_expense_plan'] = plan_op_expense
                            brand_agg[brand]['direct_profit_plan'] = plan_direct_profit_val
                            print(f"  {brand}: 계획 실판매액 {plan_sales:,.0f}원, 계획 직접이익 {plan_direct_profit_val:,.0f}원, 영업이익 {plan_op_profit:,.0f}원")
                else:
                    print(f"  [WARNING] '내수합계' 채널을 찾을 수 없습니다.")
                    for brand in brand_agg.keys():
                        brand_agg[brand]['목표대비 진척율'] = 0.0
            else:
                print(f"  [WARNING] 계획 파일에서 필요한 컬럼을 찾을 수 없습니다.")
                for brand in brand_agg.keys():
                    brand_agg[brand]['목표대비 진척율'] = 0.0
        
        # 모든 브랜드에 기본값 설정 (누락된 필드)
        for brand_str in brand_agg.keys():
            if 'TAG가(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['TAG가(목표)'] = 0.0
            if '실판매액(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['실판매액(목표)'] = 0.0
            if '할인율(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['할인율(목표)'] = 0.0
            if '직접이익(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['직접이익(목표)'] = 0.0
            if '직접이익율(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['직접이익율(목표)'] = 0.0
            if '영업이익(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['영업이익(목표)'] = 0.0
            if '영업이익율(목표)' not in brand_agg[brand_str]:
                brand_agg[brand_str]['영업이익율(목표)'] = 0.0
            if '목표대비 진척율' not in brand_agg[brand_str]:
                brand_agg[brand_str]['목표대비 진척율'] = 0.0
            if '목표대비 할인율' not in brand_agg[brand_str]:
                brand_agg[brand_str]['목표대비 할인율'] = 0.0
                
    except FileNotFoundError as e:
        print(f"  [WARNING] 계획 파일을 로드할 수 없어 목표대비 계산을 건너뜁니다: {e}")
        for brand in brand_agg.keys():
            brand_agg[brand]['목표대비 진척율'] = 0.0
            brand_agg[brand]['TAG가(목표)'] = 0.0
            brand_agg[brand]['실판매액(목표)'] = 0.0
            brand_agg[brand]['할인율(목표)'] = 0.0
            brand_agg[brand]['직접이익(목표)'] = 0.0
            brand_agg[brand]['직접이익율(목표)'] = 0.0
            brand_agg[brand]['영업이익(목표)'] = 0.0
            brand_agg[brand]['영업이익율(목표)'] = 0.0
            brand_agg[brand]['목표대비 할인율'] = 0.0
    
    # 5. 비교 계산 (전년대비, 목표대비) - 월말예상 데이터 사용
    print(f"\n[계산] 브랜드별 전년대비/목표대비 계산 (월말예상 기준)...")
    for brand_str in brand_agg.keys():
        # 월말예상 데이터 가져오기
        forecast_sales = brand_agg[brand_str].get('실판매출(월말예상)', 0.0)
        forecast_discount = brand_agg[brand_str].get('할인율(월말예상)', 0.0)
        
        # 전년 데이터 가져오기
        prev_sales = brand_agg[brand_str].get('실매출액(전년)', 0.0)
        prev_discount = brand_agg[brand_str].get('할인율(전년)', 0.0)
        
        # 목표 데이터 가져오기
        plan_sales = brand_agg[brand_str].get('실판매액(목표)', 0.0)
        plan_discount = brand_agg[brand_str].get('할인율(목표)', 0.0)
        
        # 전년대비 매출 계산: 월말예상(당년) - 전년
        brand_agg[brand_str]['전년대비 매출'] = forecast_sales - prev_sales
        
        # 전년대비 할인율 계산: 월말예상(당년) - 전년
        brand_agg[brand_str]['전년대비 할인율'] = forecast_discount - prev_discount
        
        # 목표대비 매출 계산: 월말예상(당년) - 목표
        brand_agg[brand_str]['목표대비 매출'] = forecast_sales - plan_sales
        
        # 목표대비 할인율 계산: 월말예상(당년) - 목표
        brand_agg[brand_str]['목표대비 할인율(차이)'] = forecast_discount - plan_discount
        
        # 전년대비 영업이익율 계산 (당년 영업이익은 brandPLData에서 가져오므로 여기서는 비율만 계산)
        # 전년 영업이익은 이미 계산됨
        prev_operating_profit = brand_agg[brand_str].get('영업이익(전년)', 0.0)
        prev_sales_for_yoy = brand_agg[brand_str].get('실매출액(전년)', 0.0)
        
        # 전년 영업이익 대비 비율을 계산할 때는 당년 영업이익이 필요하지만,
        # 당년 영업이익은 brandPLData에서 별도로 계산되므로 여기서는 전년 영업이익 정보만 저장
        # 전년대비 영업이익율은 brandPLData의 yoy 값에서 계산됨
        
        print(f"  {brand_str}: 전년대비 매출 {brand_agg[brand_str]['전년대비 매출']:,.0f}원, 전년대비 할인율 {brand_agg[brand_str]['전년대비 할인율']:.2f}%p, 전년 영업이익 {prev_operating_profit:,.0f}원")
    
    # 6. 결과 데이터프레임 생성
    result_rows = []
    for brand, kpi in sorted(brand_agg.items()):
        row = {
            '브랜드': brand,
            '실판매출(현시점)': int(kpi['실판매출(현시점)']),
            '직접이익(현시점)': int(kpi['직접이익(현시점)']),
            '직접이익율(현시점)': round(kpi['직접이익율(현시점)'], 2),
            '할인율': round(kpi.get('할인율', 0.0), 2),
            '목표대비 진척율': round(kpi.get('목표대비 진척율', 0.0), 1),
            '영업비(현시점)': int(kpi.get('영업비(현시점)', 0))
        }
        
        # 월말예상 값 추가
        if '실판매출(월말예상)' in kpi:
            row['실판매출(월말예상)'] = int(kpi['실판매출(월말예상)'])
            row['직접이익(월말예상)'] = int(kpi['직접이익(월말예상)'])
            row['직접이익율(월말예상)'] = round(kpi['직접이익율(월말예상)'], 2)
            row['할인율(월말예상)'] = round(kpi.get('할인율(월말예상)', 0.0), 2)
        else:
            row['실판매출(월말예상)'] = 0
            row['직접이익(월말예상)'] = 0
            row['직접이익율(월말예상)'] = 0.0
            row['할인율(월말예상)'] = 0.0
        
        # 전년 데이터 추가
        row['TAG매출액(전년)'] = int(kpi.get('TAG매출액(전년)', 0))
        row['실매출액(전년)'] = int(kpi.get('실매출액(전년)', 0))
        row['직접이익(전년)'] = int(kpi.get('직접이익(전년)', 0))
        row['영업비(전년)'] = int(kpi.get('영업비(전년)', 0))
        row['영업이익(전년)'] = int(kpi.get('영업이익(전년)', 0))
        row['할인율(전년)'] = round(kpi.get('할인율(전년)', 0.0), 2)
        row['직접이익율(전년)'] = round(kpi.get('직접이익율(전년)', 0.0), 2)
        row['영업이익율(전년)'] = round(kpi.get('영업이익율(전년)', 0.0), 2)
        
        # 계획 데이터 추가
        row['TAG가(목표)'] = int(kpi.get('TAG가(목표)', 0))
        row['실판매액(목표)'] = int(kpi.get('실판매액(목표)', 0))
        row['할인율(목표)'] = round(kpi.get('할인율(목표)', 0.0), 2)
        row['직접이익(목표)'] = int(kpi.get('직접이익(목표)', 0))
        row['직접이익율(목표)'] = round(kpi.get('직접이익율(목표)', 0.0), 2)
        row['영업이익(목표)'] = int(kpi.get('영업이익(목표)', 0))
        row['영업이익율(목표)'] = round(kpi.get('영업이익율(목표)', 0.0), 2)
        
        # 비교 계산 추가
        row['목표대비 할인율'] = round(kpi.get('목표대비 할인율', 0.0), 2)
        row['전년대비 매출'] = int(kpi.get('전년대비 매출', 0))
        row['전년대비 할인율'] = round(kpi.get('전년대비 할인율', 0.0), 2)
        
        result_rows.append(row)
    
    result_df = pd.DataFrame(result_rows)
    
    # 7. 결과 출력
    print("\n" + "=" * 60)
    print("브랜드별 KPI 결과")
    print("=" * 60)
    print(result_df.to_string(index=False))
    
    return result_df, brand_agg

def convert_to_dict_format(result_df: pd.DataFrame, brand_agg: Dict) -> Dict:
    """
    데이터프레임을 대시보드에서 사용할 딕셔너리 형식으로 변환
    brand_agg를 직접 사용하여 인코딩 문제를 방지
    
    Args:
        result_df: KPI 데이터프레임
        brand_agg: 브랜드별 집계 딕셔너리
    
    Returns:
        dict: 브랜드별 KPI 딕셔너리
    """
    kpi_dict = {}
    
    for brand, kpi in sorted(brand_agg.items()):
        # 현시점 데이터
        revenue_current = int(kpi.get('실판매출(현시점)', 0))
        direct_profit_current = int(kpi.get('직접이익(현시점)', 0))
        
        # 월말예상 데이터
        revenue_forecast = int(kpi.get('실판매출(월말예상)', 0))
        direct_profit_forecast = int(kpi.get('직접이익(월말예상)', 0))
        
        # ★ 현시점/월말예상 영업비 = 계획파일의 영업비 (동일하게 사용) ★
        op_expense_plan = int(kpi.get('영업비(목표)', 0))
        
        # ★ 현시점 영업이익 = 직접이익(현시점) - 영업비(계획) ★
        op_profit_current = direct_profit_current - op_expense_plan
        
        # ★ 월말예상 영업이익 = 직접이익(월말예상) - 영업비(계획) ★
        op_profit_forecast = direct_profit_forecast - op_expense_plan
        
        # 계획 직접이익
        direct_profit_plan = int(kpi.get('direct_profit_plan', 0) or kpi.get('직접이익(목표)', 0))
        
        # ★ 진척율 계산 (직접이익 기준) ★
        # 현시점 진척율 = 현시점 직접이익 / 계획 직접이익 × 100
        progress_rate_current = round(kpi.get('목표대비 진척율', 0.0), 1)
        # 월말예상 진척율 = 월말예상 직접이익 / 계획 직접이익 × 100
        progress_rate_forecast = round(kpi.get('목표대비 진척율(월말예상)', 0.0), 1)
        
        # 영업이익율 계산 (영업이익 / 실판매출 × 1.1 × 100)
        op_profit_rate_current = (op_profit_current / revenue_current * 1.1 * 100) if revenue_current > 0 else 0
        op_profit_rate_forecast = (op_profit_forecast / revenue_forecast * 1.1 * 100) if revenue_forecast > 0 else 0
        
        # ★ 전년/계획 데이터 ★
        revenue_previous = int(kpi.get('실매출액(전년)', 0))
        revenue_plan = int(kpi.get('실판매액(목표)', 0))
        direct_profit_previous = int(kpi.get('직접이익(전년)', 0))
        direct_profit_plan = int(kpi.get('직접이익(목표)', 0))
        
        # ★ 목표대비 계산 (월말예상 vs 목표) ★
        # 목표대비 = (월말예상 - 목표) / 목표 × 100
        revenue_vs_plan = round(((revenue_forecast - revenue_plan) / revenue_plan * 100), 1) if revenue_plan > 0 else 0
        profit_vs_plan = round(((direct_profit_forecast - direct_profit_plan) / direct_profit_plan * 100), 1) if direct_profit_plan > 0 else 0
        
        # ★ 전년대비 계산 (월말예상 vs 전년) ★
        # 전년대비 = (월말예상 - 전년) / 전년 × 100
        revenue_vs_previous = round(((revenue_forecast - revenue_previous) / revenue_previous * 100), 1) if revenue_previous > 0 else 0
        profit_vs_previous = round(((direct_profit_forecast - direct_profit_previous) / direct_profit_previous * 100), 1) if direct_profit_previous > 0 else 0
        
        # ★ 필요한 필드만 JSON에 저장 ★
        kpi_dict[brand] = {
            # 현시점 데이터
            'revenue': revenue_current,
            'directProfit': direct_profit_current,
            'directProfitRate': round(kpi.get('직접이익율(현시점)', 0.0), 2),
            'discountRate': round(kpi.get('할인율', 0.0), 2),
            'progressRate': progress_rate_current,
            'progressRateForecast': progress_rate_forecast,
            # 월말예상 데이터
            'revenueForecast': revenue_forecast,
            'directProfitForecast': direct_profit_forecast,
            'directProfitRateForecast': round(kpi.get('직접이익율(월말예상)', 0.0), 2),
            'discountRateForecast': round(kpi.get('할인율(월말예상)', 0.0), 2),
            # 영업이익 데이터
            'operatingProfit': int(op_profit_current),
            'operatingProfitForecast': int(op_profit_forecast),
            'operatingProfitRate': round(op_profit_rate_current, 2),
            'operatingProfitRateForecast': round(op_profit_rate_forecast, 2),
            # 전년 데이터
            'revenuePrevious': revenue_previous,
            'directProfitPrevious': direct_profit_previous,
            'operatingProfitPrevious': int(kpi.get('영업이익(전년)', 0)),
            # 계획 데이터
            'revenuePlan': revenue_plan,
            'directProfitPlan': direct_profit_plan,
            'directProfitRatePlan': round(kpi.get('직접이익율(목표)', 0.0), 2),
            # ★ 목표대비/전년대비 (%) ★
            'revenueVsPlan': revenue_vs_plan,         # 매출 목표대비
            'revenueVsPrevious': revenue_vs_previous, # 매출 전년대비
            'profitVsPlan': profit_vs_plan,           # 직접이익 목표대비
            'profitVsPrevious': profit_vs_previous    # 직접이익 전년대비
        }
        
        print(f"  {brand}: 영업이익(현시점) {op_profit_current/100000000:.2f}억 = 직접이익 {direct_profit_current/100000000:.2f}억 - 영업비(계획) {op_expense_plan/100000000:.2f}억")
        print(f"         영업이익(월말예상) {op_profit_forecast/100000000:.2f}억 = 직접이익 {direct_profit_forecast/100000000:.2f}억 - 영업비(계획) {op_expense_plan/100000000:.2f}억")
        print(f"         목표대비 매출 {revenue_vs_plan:+.1f}%, 전년대비 매출 {revenue_vs_previous:+.1f}%")
    
    return kpi_dict

def load_existing_data_js(data_js_path: str) -> tuple:
    """
    기존 data.js 파일을 읽어서 brandKPI를 추출하고 전체 내용 반환
    
    Args:
        data_js_path: data.js 파일 경로
    
    Returns:
        tuple: (전체 파일 내용, 기존 brandKPI 딕셔너리 또는 None)
    """
    if not os.path.exists(data_js_path):
        return None, None
    
    try:
        with open(data_js_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # brandKPI 부분 추출 (정규표현식 사용)
        # var brandKPI = {...}; 패턴 찾기
        pattern = r'var brandKPI = (\{.*?\});'
        match = re.search(pattern, content, re.DOTALL)
        
        if match:
            brand_kpi_str = match.group(1)
            # JSON 파싱
            existing_brand_kpi = json.loads(brand_kpi_str)
            return content, existing_brand_kpi
        else:
            print(f"[WARNING] data.js에서 brandKPI를 찾을 수 없습니다: {data_js_path}")
            return content, None
    except Exception as e:
        print(f"[WARNING] data.js 읽기 실패: {e}")
        return None, None

def merge_brand_kpi(existing_kpi: Dict, new_kpi: Dict) -> Dict:
    """
    기존 brandKPI와 새로운 brandKPI를 병합
    
    Args:
        existing_kpi: 기존 brandKPI 딕셔너리
        new_kpi: 새로운 brandKPI 딕셔너리
    
    Returns:
        dict: 병합된 brandKPI 딕셔너리
    """
    merged = {}
    
    # 모든 브랜드에 대해 병합
    all_brands = set(existing_kpi.keys()) | set(new_kpi.keys())
    
    for brand in all_brands:
        merged[brand] = {}
        
        # 기존 데이터 먼저 복사
        if brand in existing_kpi:
            merged[brand].update(existing_kpi[brand])
        
        # 새로운 데이터로 덮어쓰기 또는 추가
        if brand in new_kpi:
            merged[brand].update(new_kpi[brand])
    
    return merged

def update_data_js_with_brand_kpi(data_js_path: str, new_kpi_dict: Dict):
    """
    data.js 파일의 brandKPI 부분을 업데이트
    
    Args:
        data_js_path: data.js 파일 경로
        new_kpi_dict: 새로운 brandKPI 딕셔너리
    """
    # 기존 파일 읽기
    content, existing_kpi = load_existing_data_js(data_js_path)
    
    if content is None:
        raise FileNotFoundError(f"[ERROR] data.js 파일을 찾을 수 없습니다: {data_js_path}")
    
    # brandKPI 병합
    if existing_kpi:
        merged_kpi = merge_brand_kpi(existing_kpi, new_kpi_dict)
        print(f"[병합] 기존 brandKPI 필드 유지 + 새 필드 추가")
    else:
        merged_kpi = new_kpi_dict
        print(f"[신규] brandKPI 생성")
    
    # brandKPI 부분 교체
    import re
    pattern = r'var brandKPI = (\{.*?\});'
    
    # 새로운 brandKPI JSON 문자열 생성
    new_brand_kpi_str = json.dumps(merged_kpi, ensure_ascii=False, indent=2)
    
    # 교체
    new_content = re.sub(
        pattern,
        f'var brandKPI = {new_brand_kpi_str};',
        content,
        flags=re.DOTALL
    )
    
    # 파일 저장
    os.makedirs(os.path.dirname(data_js_path), exist_ok=True)
    with open(data_js_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    
    file_size = os.path.getsize(data_js_path) / 1024  # KB
    print(f"\n[저장] {data_js_path}")
    print(f"  데이터: {len(merged_kpi)}개 브랜드")
    print(f"  파일 크기: {file_size:.2f} KB")


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="브랜드별 KPI 계산")
    parser.add_argument("date", help="YYYYMMDD 형식의 날짜 (예: 20251124)")
    parser.add_argument("--year-month", help="YYYYMM 형식의 연월 (선택사항, 없으면 date에서 추출)")
    parser.add_argument("--output-js", help="JS 출력 파일 경로 (선택사항)")
    
    args = parser.parse_args()
    
    date_str = args.date
    year_month = args.year_month
    
    # 날짜 형식 검증
    if len(date_str) != 8 or not date_str.isdigit():
        print("[ERROR] 날짜 형식이 올바르지 않습니다. YYYYMMDD 형식이어야 합니다.")
        sys.exit(1)
    
    try:
        # KPI 계산
        result_df, brand_agg = calculate_brand_kpi(date_str, year_month)
        
        # 딕셔너리 형식으로 변환
        kpi_dict = convert_to_dict_format(result_df, brand_agg)
        
        # data.js 파일 경로
        data_js_path = os.path.join(PUBLIC_DIR, f"data_{date_str}.js")
        
        # data.js가 없으면 경고
        if not os.path.exists(data_js_path):
            print(f"[WARNING] data.js 파일이 없습니다: {data_js_path}")
            print(f"[WARNING] 먼저 create_treemap_data.py를 실행하여 data.js를 생성하세요.")
            print(f"[INFO] 임시로 brand_kpi.js 파일을 생성합니다.")
            # 임시로 brand_kpi.js 생성 (하위 호환성)
            brand_kpi_js_path = os.path.join(PUBLIC_DIR, f"brand_kpi_{date_str}.js")
            with open(brand_kpi_js_path, "w", encoding="utf-8") as f:
                f.write("// 브랜드별 KPI 데이터\n")
                f.write(f"// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("(function(){\n")
                f.write("  var brandKPI = ")
                f.write(json.dumps(kpi_dict, ensure_ascii=False, indent=2))
                f.write(";\n")
                f.write("  if (typeof window !== 'undefined') {\n")
                f.write("    window.brandKPI = brandKPI;\n")
                f.write("  }\n")
                f.write("  console.log('[Brand KPI] 브랜드별 KPI 데이터 로드 완료');\n")
                f.write("})();\n")
            print(f"  임시 파일: {brand_kpi_js_path}")
        else:
            # data.js 업데이트
            update_data_js_with_brand_kpi(data_js_path, kpi_dict)
        
        print("\n[OK] 브랜드별 KPI 계산 및 저장 완료")
        print(f"  업데이트된 파일: {data_js_path}")
        
        # ★★★ JSON 파일로도 저장 ★★★
        json_dir = os.path.join(PUBLIC_DIR, "data", date_str)
        os.makedirs(json_dir, exist_ok=True)
        json_path = os.path.join(json_dir, "brand_kpi.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(kpi_dict, f, ensure_ascii=False, indent=2)
        print(f"  [OK] JSON 저장: {json_path}")
        
    except Exception as e:
        print(f"[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()


