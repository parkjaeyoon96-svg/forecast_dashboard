# -*- coding: utf-8 -*-
"""
당시즌의류/ACC 재고주수 분석 데이터 처리 및 JSON 파일 생성
- 당시즌의류: raw/202511/ETC/당시즌의류_브랜드별현황_YYYYMMDD.csv
- ACC판매율 분석: raw/202511/ETC/ACC_재고주수분석_YYYYMMDD.csv

출력: public/brand_stock_analysis_YYYYMMDD.js
"""

import pandas as pd
import json
import os
import re
from datetime import datetime, timedelta


def safe_float(value):
    """안전하게 float 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value):
    """안전하게 int 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_percentage(value):
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


def calculate_sales_rate(cum_sales_tag, storage_tag_amt):
    """누적판매율 계산: 누적판매매출 / 입고금액"""
    if storage_tag_amt is None or storage_tag_amt == 0 or cum_sales_tag is None:
        return None
    try:
        return round(cum_sales_tag / storage_tag_amt, 4)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def process_clothing_csv(file_path):
    """
    당시즌의류 브랜드별 현황 CSV 처리
    
    CSV 컬럼:
    - 브랜드, 대분류, 중분류, 아이템코드, 아이템명(한글), 발주(TAG), 전년비(발주), 
      주간판매매출(TAG), 전년비(주간), 누적판매매출(TAG), 전년비(누적), 
      누적판매매출_원본, 입고금액_당년, 누적판매매출_전년, 입고금액_전년,
      전년마감판매매출, 전년마감입고금액
    """
    print(f"[당시즌의류] 파일 읽는 중: {file_path}")
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"[당시즌의류] 총 {len(df)}개 행 로드됨")
    print(f"[당시즌의류] 컬럼: {list(df.columns)}")
    
    # 브랜드별로 데이터 그룹화
    result = {}
    
    for _, row in df.iterrows():
        # 브랜드 컬럼명 하위 호환성
        brand = str(row.get('브랜드') or row.get('BRD_CD') or '').strip()
        if not brand:
            continue
        
        if brand not in result:
            result[brand] = []
        
        # 판매율 계산용 원본 데이터 가져오기 (CSV에서 받은 원본 데이터만 사용)
        # download_brand_stock_analysis.py에서 저장한 컬럼명 사용
        cum_sales_tag = safe_float(row.get('누적판매TAG가') or row.get('누적판매매출_원본') or row.get('누적판매매출(TAG)'))
        storage_tag_amt_cy = safe_float(row.get('누적입고TAG가') or row.get('입고금액_당년') or row.get('AC_STOR_TAG_AMT_KOR'))
        cum_sales_tag_py = safe_float(row.get('전년누적판매TAG가') or row.get('누적판매매출_전년') or row.get('AC_TAG_AMG_PY'))
        storage_tag_amt_py = safe_float(row.get('전년누적입고TAG가') or row.get('입고금액_전년') or row.get('AC_STOR_TAG_AMT_KOR_PY'))
        tag_amt_py_end = safe_float(row.get('전년마감누적판매TAG가') or row.get('전년마감판매매출') or row.get('TAG_AMT_PY_END'))
        storage_tag_amt_py_end = safe_float(row.get('전년마감누적입고TAG가') or row.get('전년마감입고금액') or row.get('AC_STOR_TAG_AMT_KOR_PY_END'))
        
        # 판매율 계산 (파이썬에서)
        cum_sales_rate = calculate_sales_rate(cum_sales_tag, storage_tag_amt_cy)
        cum_sales_rate_py = calculate_sales_rate(cum_sales_tag_py, storage_tag_amt_py)
        py_closing_sales_rate = calculate_sales_rate(tag_amt_py_end, storage_tag_amt_py_end)
        
        # 판매율 차이 계산
        cum_sales_rate_diff = None
        if cum_sales_rate is not None and cum_sales_rate_py is not None:
            cum_sales_rate_diff = round(cum_sales_rate - cum_sales_rate_py, 4)
        
        # 컬럼명 매핑 (하위 호환성) - CSV에서 받은 컬럼명 우선 사용
        brand_col = row.get('브랜드') or row.get('BRD_CD')
        category_col = row.get('대분류') or row.get('PARENT_PRDT_KIND_NM')
        subcategory_col = row.get('중분류') or row.get('PRDT_KIND_NM')
        item_code_col = row.get('아이템코드') or row.get('ITEM')
        item_name_col = row.get('아이템명(한글)') or row.get('아이템명') or row.get('ITEM_NM')
        # 주간/누적 판매 데이터 (CSV에서 받은 컬럼명 사용)
        weekly_sales_col = row.get('주간판매TAG가') or row.get('주간판매매출(TAG)') or row.get('W_TAG_AMG')
        weekly_yoy_col = row.get('주간전년비') or row.get('전년비(주간)')
        cum_sales_col = row.get('누적판매TAG가') or row.get('누적판매매출(TAG)') or row.get('AC_TAG_AMG')
        cum_yoy_col = row.get('누적전년비') or row.get('전년비(누적)')
        # 발주 데이터 (없을 수 있음)
        order_tag_col = row.get('발주(TAG)') or row.get('누적발주TAG가') or row.get('AC_ORD_TAG_AMT')
        order_yoy_col = row.get('전년비(발주)')
        
        item_data = {
            "category": str(category_col).strip() if pd.notna(category_col) else "",
            "subCategory": str(subcategory_col).strip() if pd.notna(subcategory_col) else "",
            "itemCode": str(item_code_col).strip() if pd.notna(item_code_col) else "",
            "itemName": str(item_name_col).strip() if pd.notna(item_name_col) else "",
            "orderTag": safe_float(order_tag_col),
            "orderYoY": safe_float(order_yoy_col),
            "weeklySalesTag": safe_float(weekly_sales_col),
            "weeklyYoY": safe_float(weekly_yoy_col),
            "cumSalesTag": safe_float(cum_sales_col),
            "cumYoY": safe_float(cum_yoy_col),
            # 프론트엔드 재계산을 위한 입고금액 데이터 추가
            "storageTagAmt": storage_tag_amt_cy,  # 당년 누적입고TAG가
            "storageTagAmtPy": storage_tag_amt_py,  # 전년 누적입고TAG가
            "cumSalesTagPy": cum_sales_tag_py,  # 전년 누적판매TAG가
            "storageTagAmtPyEnd": storage_tag_amt_py_end,  # 전년마감 누적입고TAG가
            "cumSalesTagPyEnd": tag_amt_py_end,  # 전년마감 누적판매TAG가
            # 파이썬에서 계산한 판매율 (참고용)
            "cumSalesRate": cum_sales_rate,
            "cumSalesRatePy": cum_sales_rate_py,
            "cumSalesRateDiff": cum_sales_rate_diff,
            "pyClosingSalesRate": py_closing_sales_rate
        }
        
        result[brand].append(item_data)
    
    # 전체판매율 계산 (브랜드별, 전체) - DataFrame에서 직접 집계 및 재계산
    print(f"\n[당시즌의류] 판매율 계산 중...")
    
    # 컬럼명 찾기 (하위 호환성)
    brand_col_name = None
    for col in ['브랜드', 'BRD_CD']:
        if col in df.columns:
            brand_col_name = col
            break
    # 중분류 컬럼명 (카테고리 집계용)
    subcategory_col_name = None
    for col in ['중분류', 'PRDT_KIND_NM']:
        if col in df.columns:
            subcategory_col_name = col
            break
    
    # 판매율 계산용 원본 데이터 컬럼 찾기
    cum_sales_col = None  # 당년 누적판매TAG가
    storage_amt_col = None  # 당년 누적입고TAG가
    cum_sales_py_col = None  # 전년 누적판매TAG가
    storage_amt_py_col = None  # 전년 누적입고TAG가
    tag_amt_py_end_col = None  # 전년마감 누적판매TAG가
    storage_tag_amt_py_end_col = None  # 전년마감 누적입고TAG가
    
    for col in df.columns:
        if '누적판매매출_원본' in col or '누적판매TAG가' in col:
            if '전년' not in col and '마감' not in col:
                cum_sales_col = col
        if '입고금액_당년' in col or '누적입고TAG가' in col:
            if '전년' not in col and '마감' not in col:
                storage_amt_col = col
        if '누적판매매출_전년' in col or '전년누적판매TAG가' in col:
            if '마감' not in col:
                cum_sales_py_col = col
        if '입고금액_전년' in col or '전년누적입고TAG가' in col:
            if '마감' not in col:
                storage_amt_py_col = col
        if '전년마감누적판매TAG가' in col or '전년마감판매매출' in col:
            tag_amt_py_end_col = col
        if '전년마감누적입고TAG가' in col or '전년마감입고금액' in col:
            storage_tag_amt_py_end_col = col
    
    # 아이템 코드 컬럼명 찾기
    item_code_col_name = None
    for col in ['아이템코드', 'ITEM']:
        if col in df.columns:
            item_code_col_name = col
            break
    
    if not item_code_col_name:
        print(f"[경고] 아이템 코드 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df.columns)}")
    else:
        print(f"[당시즌의류] 아이템 코드 컬럼: {item_code_col_name}")
    
    # 브랜드별/카테고리별/아이템별 집계 및 판매율 재계산
    brand_totals = {}
    category_totals = {}  # brand -> subcategory -> rates
    brand_item_totals = {}  # brand -> item_code -> rates (브랜드별 아이템별)
    overall_category_totals = {}  # subcategory -> rates (전체 브랜드 합산)
    overall_item_totals = {}  # item_code -> rates (전체 아이템별)
    if cum_sales_col and storage_amt_col and brand_col_name:
        for brand in result.keys():
            brand_df = df[df[brand_col_name].astype(str).str.strip() == brand]
            
            # 당년 누적판매율 재계산
            brand_cum_sales = brand_df[cum_sales_col].apply(lambda x: safe_float(x) or 0).sum()
            brand_storage_amt = brand_df[storage_amt_col].apply(lambda x: safe_float(x) or 0).sum()
            
            # 전년 누적판매율 재계산
            brand_cum_sales_py = brand_df[cum_sales_py_col].apply(lambda x: safe_float(x) or 0).sum() if cum_sales_py_col and cum_sales_py_col in brand_df.columns else 0
            brand_storage_amt_py = brand_df[storage_amt_py_col].apply(lambda x: safe_float(x) or 0).sum() if storage_amt_py_col and storage_amt_py_col in brand_df.columns else 0
            
            # 전년마감판매율 재계산 (예: MLB 의류의 경우 56% = 전년누적판매TAG가/전년누적입고TAG가)
            brand_tag_amt_py_end = brand_df[tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if tag_amt_py_end_col and tag_amt_py_end_col in brand_df.columns else 0
            brand_storage_tag_amt_py_end = brand_df[storage_tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if storage_tag_amt_py_end_col and storage_tag_amt_py_end_col in brand_df.columns else 0
            
            # 판매율 계산
            cum_sales_rate = calculate_sales_rate(brand_cum_sales, brand_storage_amt)
            cum_sales_rate_py = calculate_sales_rate(brand_cum_sales_py, brand_storage_amt_py)
            py_closing_sales_rate = calculate_sales_rate(brand_tag_amt_py_end, brand_storage_tag_amt_py_end)
            
            # 차이 계산 (당년 - 전년)
            cum_sales_rate_diff = None
            if cum_sales_rate is not None and cum_sales_rate_py is not None:
                cum_sales_rate_diff = round(cum_sales_rate - cum_sales_rate_py, 4)
            
            brand_totals[brand] = {
                # 집계 금액 데이터 (Python 네이티브 타입으로 변환)
                'totalOrderTagPy': float(brand_storage_amt_py) if brand_storage_amt_py else 0,  # 전년누적입고TAG가
                'totalCumSalesPy': float(brand_cum_sales_py) if brand_cum_sales_py else 0,  # 전년누적판매TAG가
                'totalOrderTag': float(brand_storage_amt) if brand_storage_amt else 0,  # 누적입고TAG가
                'totalCumSales': float(brand_cum_sales) if brand_cum_sales else 0,  # 누적판매TAG가
                'totalOrderTagPyEnd': float(brand_storage_tag_amt_py_end) if brand_storage_tag_amt_py_end else 0,  # 전년마감누적입고TAG가
                'totalCumSalesPyEnd': float(brand_tag_amt_py_end) if brand_tag_amt_py_end else 0,  # 전년마감누적판매TAG가
                # 판매율 데이터
                'cumSalesRate': cum_sales_rate,
                'cumSalesRatePy': cum_sales_rate_py,
                'cumSalesRateDiff': cum_sales_rate_diff,
                'pyClosingSalesRate': py_closing_sales_rate
            }
            
            # 카테고리별 판매율 및 집계 금액 (브랜드별)
            if subcategory_col_name:
                cat_group = brand_df.groupby(subcategory_col_name)
                category_totals[brand] = {}
                for subcat, sub_df in cat_group:
                    sub_cum = sub_df[cum_sales_col].apply(lambda x: safe_float(x) or 0).sum()
                    sub_stor = sub_df[storage_amt_col].apply(lambda x: safe_float(x) or 0).sum()
                    sub_cum_py = sub_df[cum_sales_py_col].apply(lambda x: safe_float(x) or 0).sum() if cum_sales_py_col and cum_sales_py_col in sub_df.columns else 0
                    sub_stor_py = sub_df[storage_amt_py_col].apply(lambda x: safe_float(x) or 0).sum() if storage_amt_py_col and storage_amt_py_col in sub_df.columns else 0
                    sub_tag_py_end = sub_df[tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if tag_amt_py_end_col and tag_amt_py_end_col in sub_df.columns else 0
                    sub_stor_py_end = sub_df[storage_tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if storage_tag_amt_py_end_col and storage_tag_amt_py_end_col in sub_df.columns else 0
                    
                    # 판매율 계산
                    cat_cum_sales_rate = calculate_sales_rate(sub_cum, sub_stor)
                    cat_cum_sales_rate_py = calculate_sales_rate(sub_cum_py, sub_stor_py)
                    cat_py_closing_sales_rate = calculate_sales_rate(sub_tag_py_end, sub_stor_py_end)
                    
                    # 판매율 차이 계산
                    cat_cum_sales_rate_diff = None
                    if cat_cum_sales_rate is not None and cat_cum_sales_rate_py is not None:
                        cat_cum_sales_rate_diff = round(cat_cum_sales_rate - cat_cum_sales_rate_py, 4)
                    
                    category_totals[brand][str(subcat)] = {
                        # 집계 금액 데이터 (Python 네이티브 타입으로 변환)
                        'totalOrderTag': float(sub_stor) if sub_stor else 0,  # 누적입고TAG가 (당년)
                        'totalCumSales': float(sub_cum) if sub_cum else 0,  # 누적판매TAG가 (당년)
                        'totalOrderTagPy': float(sub_stor_py) if sub_stor_py else 0,  # 전년누적입고TAG가
                        'totalCumSalesPy': float(sub_cum_py) if sub_cum_py else 0,  # 전년누적판매TAG가
                        'totalOrderTagPyEnd': float(sub_stor_py_end) if sub_stor_py_end else 0,  # 전년마감누적입고TAG가
                        'totalCumSalesPyEnd': float(sub_tag_py_end) if sub_tag_py_end else 0,  # 전년마감누적판매TAG가
                        # 판매율 데이터
                        'cumSalesRate': cat_cum_sales_rate,
                        'cumSalesRatePy': cat_cum_sales_rate_py,
                        'cumSalesRateDiff': cat_cum_sales_rate_diff,
                        'pyClosingSalesRate': cat_py_closing_sales_rate
                    }
                    
                    # 전체 카테고리 합산
                    if str(subcat) not in overall_category_totals:
                        overall_category_totals[str(subcat)] = {'cumSales': 0, 'stor': 0, 'cumSalesPy': 0, 'storPy': 0, 'tagPyEnd': 0, 'storPyEnd': 0}
                    overall_category_totals[str(subcat)]['cumSales'] += sub_cum
                    overall_category_totals[str(subcat)]['stor'] += sub_stor
                    overall_category_totals[str(subcat)]['cumSalesPy'] += sub_cum_py
                    overall_category_totals[str(subcat)]['storPy'] += sub_stor_py
                    overall_category_totals[str(subcat)]['tagPyEnd'] += sub_tag_py_end
                    overall_category_totals[str(subcat)]['storPyEnd'] += sub_stor_py_end
            
            # 브랜드별 아이템별 판매율 재계산 (CSV 대신 result 딕셔너리 사용)
            if item_code_col_name:
                item_group = brand_df.groupby(item_code_col_name)
                brand_item_totals[brand] = {}
                for item_code, item_df in item_group:
                    item_cum = item_df[cum_sales_col].apply(lambda x: safe_float(x) or 0).sum()
                    item_stor = item_df[storage_amt_col].apply(lambda x: safe_float(x) or 0).sum()
                    item_cum_py = item_df[cum_sales_py_col].apply(lambda x: safe_float(x) or 0).sum() if cum_sales_py_col and cum_sales_py_col in item_df.columns else 0
                    item_stor_py = item_df[storage_amt_py_col].apply(lambda x: safe_float(x) or 0).sum() if storage_amt_py_col and storage_amt_py_col in item_df.columns else 0
                    item_tag_py_end = item_df[tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if tag_amt_py_end_col and tag_amt_py_end_col in item_df.columns else 0
                    item_stor_py_end = item_df[storage_tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if storage_tag_amt_py_end_col and storage_tag_amt_py_end_col in item_df.columns else 0
                    
                    brand_item_totals[brand][str(item_code)] = {
                        'cumSalesRate': calculate_sales_rate(item_cum, item_stor),
                        'cumSalesRatePy': calculate_sales_rate(item_cum_py, item_stor_py),
                        'pyClosingSalesRate': calculate_sales_rate(item_tag_py_end, item_stor_py_end)
                    }
                    
                    # 전체 아이템 합산 (모든 브랜드의 동일 아이템 합계)
                    if str(item_code) not in overall_item_totals:
                        overall_item_totals[str(item_code)] = {'cumSales': 0, 'stor': 0, 'cumSalesPy': 0, 'storPy': 0, 'tagPyEnd': 0, 'storPyEnd': 0}
                    overall_item_totals[str(item_code)]['cumSales'] += item_cum
                    overall_item_totals[str(item_code)]['stor'] += item_stor
                    overall_item_totals[str(item_code)]['cumSalesPy'] += item_cum_py
                    overall_item_totals[str(item_code)]['storPy'] += item_stor_py
                    overall_item_totals[str(item_code)]['tagPyEnd'] += item_tag_py_end
                    overall_item_totals[str(item_code)]['storPyEnd'] += item_stor_py_end
    
    # ★ CSV 컬럼명에 의존하지 않고 result 딕셔너리에서 직접 아이템별 평균 판매율 계산 ★
    # 위의 CSV 기반 로직이 실패해도 result에서 재계산
    print(f"\n[당시즌의류] result 딕셔너리에서 아이템별 평균 판매율 재계산 중...")
    result_based_item_totals = {}
    result_based_brand_item_totals = {}
    
    for brand, items in result.items():
        result_based_brand_item_totals[brand] = {}
        for item in items:
            item_code = str(item.get('itemCode', '')).strip()
            if not item_code:
                continue
            
            # 브랜드별 아이템별 판매율
            result_based_brand_item_totals[brand][item_code] = {
                'cumSalesRate': item.get('cumSalesRate'),
                'cumSalesRatePy': item.get('cumSalesRatePy'),
                'pyClosingSalesRate': item.get('pyClosingSalesRate')
            }
            
            # 전체 아이템별 합산
            if item_code not in result_based_item_totals:
                result_based_item_totals[item_code] = {
                    'cumSales': 0, 'stor': 0,
                    'cumSalesPy': 0, 'storPy': 0,
                    'tagPyEnd': 0, 'storPyEnd': 0
                }
            
            result_based_item_totals[item_code]['cumSales'] += safe_float(item.get('cumSalesTag')) or 0
            result_based_item_totals[item_code]['stor'] += safe_float(item.get('storageTagAmt')) or 0
            result_based_item_totals[item_code]['cumSalesPy'] += safe_float(item.get('cumSalesTagPy')) or 0
            result_based_item_totals[item_code]['storPy'] += safe_float(item.get('storageTagAmtPy')) or 0
            result_based_item_totals[item_code]['tagPyEnd'] += safe_float(item.get('cumSalesTagPyEnd')) or 0
            result_based_item_totals[item_code]['storPyEnd'] += safe_float(item.get('storageTagAmtPyEnd')) or 0
    
    # CSV 기반 계산이 실패했으면 result 기반으로 교체
    if not overall_item_totals and result_based_item_totals:
        print(f"[당시즌의류] CSV 기반 계산 실패 → result 기반 데이터 사용")
        overall_item_totals = result_based_item_totals
        if not brand_item_totals:
            brand_item_totals = result_based_brand_item_totals
    elif result_based_item_totals:
        # 둘 다 있으면 result 기반이 더 신뢰할 수 있으므로 교체
        print(f"[당시즌의류] result 기반 데이터로 교체 (더 안정적)")
        overall_item_totals = result_based_item_totals
        brand_item_totals = result_based_brand_item_totals
    
    print(f"[당시즌의류] 아이템별 집계 완료: {len(overall_item_totals)}개 아이템")
    
    # 전체 집계 및 재계산 (CSV 기반 - 가능한 경우에만)
    if cum_sales_col and storage_amt_col:
        total_cum_sales = df[cum_sales_col].apply(lambda x: safe_float(x) or 0).sum()
        total_storage_amt = df[storage_amt_col].apply(lambda x: safe_float(x) or 0).sum()
        total_cum_sales_py = df[cum_sales_py_col].apply(lambda x: safe_float(x) or 0).sum() if cum_sales_py_col and cum_sales_py_col in df.columns else 0
        total_storage_amt_py = df[storage_amt_py_col].apply(lambda x: safe_float(x) or 0).sum() if storage_amt_py_col and storage_amt_py_col in df.columns else 0
        total_tag_amt_py_end = df[tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if tag_amt_py_end_col and tag_amt_py_end_col in df.columns else 0
        total_storage_tag_amt_py_end = df[storage_tag_amt_py_end_col].apply(lambda x: safe_float(x) or 0).sum() if storage_tag_amt_py_end_col and storage_tag_amt_py_end_col in df.columns else 0
        
        overall_sales_rate = calculate_sales_rate(total_cum_sales, total_storage_amt)
        overall_sales_rate_py = calculate_sales_rate(total_cum_sales_py, total_storage_amt_py)
        overall_py_closing_sales_rate = calculate_sales_rate(total_tag_amt_py_end, total_storage_tag_amt_py_end)
        
        # ★ 전체 평균(overall)을 brand_totals에 추가 ★
        overall_rate_diff = None
        if overall_sales_rate is not None and overall_sales_rate_py is not None:
            overall_rate_diff = round(overall_sales_rate - overall_sales_rate_py, 4)
        
        brand_totals['overall'] = {
            'totalOrderTag': float(total_storage_amt) if total_storage_amt else 0,
            'totalOrderTagPy': float(total_storage_amt_py) if total_storage_amt_py else 0,
            'totalCumSales': float(total_cum_sales) if total_cum_sales else 0,
            'totalCumSalesPy': float(total_cum_sales_py) if total_cum_sales_py else 0,
            'totalOrderTagPyEnd': float(total_storage_tag_amt_py_end) if total_storage_tag_amt_py_end else 0,
            'totalCumSalesPyEnd': float(total_tag_amt_py_end) if total_tag_amt_py_end else 0,
            'cumSalesRate': overall_sales_rate,
            'cumSalesRatePy': overall_sales_rate_py,
            'cumSalesRateDiff': overall_rate_diff,
            'pyClosingSalesRate': overall_py_closing_sales_rate
        }
        
        overall_rate_str = f"{overall_sales_rate:.4f}" if overall_sales_rate else 'N/A'
        overall_rate_py_str = f"{overall_sales_rate_py:.4f}" if overall_sales_rate_py else 'N/A'
        overall_py_closing_str = f"{overall_py_closing_sales_rate:.4f}" if overall_py_closing_sales_rate else 'N/A'
        print(f"[당시즌의류] 전체 판매율: {overall_rate_str}")
        print(f"[당시즌의류] 전년 전체 판매율: {overall_rate_py_str}")
        print(f"[당시즌의류] 전년마감 전체 판매율: {overall_py_closing_str}")
        for brand, totals in brand_totals.items():
            if brand == 'overall':  # overall은 출력하지 않음
                continue
            cum_rate_str = f"{totals['cumSalesRate']:.4f}" if totals['cumSalesRate'] else 'N/A'
            cum_rate_py_str = f"{totals['cumSalesRatePy']:.4f}" if totals['cumSalesRatePy'] else 'N/A'
            py_closing_str = f"{totals['pyClosingSalesRate']:.4f}" if totals['pyClosingSalesRate'] else 'N/A'
            print(f"  - {brand}: 당년 {cum_rate_str}, 전년 {cum_rate_py_str}, 전년마감 {py_closing_str}")
    
    print(f"[당시즌의류] 브랜드 수: {len(result)}")
    for brand, items in result.items():
        print(f"  - {brand}: {len(items)}개 아이템")
    
    # 카테고리 전체 합산을 최종 비율로 변환
    category_totals_overall_rates = {}
    for subcat, vals in overall_category_totals.items():
        # 판매율 계산
        overall_cat_cum_rate = calculate_sales_rate(vals['cumSales'], vals['stor'])
        overall_cat_cum_rate_py = calculate_sales_rate(vals['cumSalesPy'], vals['storPy'])
        overall_cat_py_closing_rate = calculate_sales_rate(vals['tagPyEnd'], vals['storPyEnd'])
        
        # 판매율 차이 계산
        overall_cat_rate_diff = None
        if overall_cat_cum_rate is not None and overall_cat_cum_rate_py is not None:
            overall_cat_rate_diff = round(overall_cat_cum_rate - overall_cat_cum_rate_py, 4)
        
        category_totals_overall_rates[subcat] = {
            # 집계 금액 데이터 (Python 네이티브 타입으로 변환)
            'totalOrderTag': float(vals['stor']) if vals['stor'] else 0,  # 누적입고TAG가 (당년)
            'totalCumSales': float(vals['cumSales']) if vals['cumSales'] else 0,  # 누적판매TAG가 (당년)
            'totalOrderTagPy': float(vals['storPy']) if vals['storPy'] else 0,  # 전년누적입고TAG가
            'totalCumSalesPy': float(vals['cumSalesPy']) if vals['cumSalesPy'] else 0,  # 전년누적판매TAG가
            'totalOrderTagPyEnd': float(vals['storPyEnd']) if vals['storPyEnd'] else 0,  # 전년마감누적입고TAG가
            'totalCumSalesPyEnd': float(vals['tagPyEnd']) if vals['tagPyEnd'] else 0,  # 전년마감누적판매TAG가
            # 판매율 데이터
            'cumSalesRate': overall_cat_cum_rate,
            'cumSalesRatePy': overall_cat_cum_rate_py,
            'cumSalesRateDiff': overall_cat_rate_diff,
            'pyClosingSalesRate': overall_cat_py_closing_rate
        }
    
    # 아이템 전체 합산을 최종 비율로 변환 (전체현황 아이템별 판매율)
    item_totals_overall_rates = {}
    for item_code, vals in overall_item_totals.items():
        cum_rate = calculate_sales_rate(vals['cumSales'], vals['stor'])
        item_totals_overall_rates[item_code] = {
            'cumSalesRate': cum_rate,
            'cumSalesRatePy': calculate_sales_rate(vals['cumSalesPy'], vals['storPy']),
            'pyClosingSalesRate': calculate_sales_rate(vals['tagPyEnd'], vals['storPyEnd'])
        }
        # 디버깅: 각 아이템별 판매율 출력
        if cum_rate is not None:
            print(f"  [아이템별 평균] {item_code}: {cum_rate:.4f} (판매: {vals['cumSales']:.0f}, 입고: {vals['stor']:.0f})")
        else:
            print(f"  [아이템별 평균] {item_code}: None (판매: {vals['cumSales']:.0f}, 입고: {vals['stor']:.0f})")
    
    # ★ 아이템별 판매율에도 전체 평균(overall) 추가 ★
    # brand_totals['overall']이 있으면 동일한 값을 item_totals_overall_rates['overall']에도 추가
    if 'overall' in brand_totals:
        item_totals_overall_rates['overall'] = {
            'cumSalesRate': brand_totals['overall'].get('cumSalesRate'),
            'cumSalesRatePy': brand_totals['overall'].get('cumSalesRatePy'),
            'cumSalesRateDiff': brand_totals['overall'].get('cumSalesRateDiff'),
            'pyClosingSalesRate': brand_totals['overall'].get('pyClosingSalesRate'),
            'totalCumSales': brand_totals['overall'].get('totalCumSales', 0),
            'totalStorage': brand_totals['overall'].get('totalOrderTag', 0),
            'totalCumSalesPy': brand_totals['overall'].get('totalCumSalesPy', 0),
            'totalStoragePy': brand_totals['overall'].get('totalOrderTagPy', 0),
            'totalCumSalesPyEnd': brand_totals['overall'].get('totalCumSalesPyEnd', 0),
            'totalStoragePyEnd': brand_totals['overall'].get('totalOrderTagPyEnd', 0)
        }
        print(f"  [전체 평균] overall: {brand_totals['overall'].get('cumSalesRate'):.4f} (전체 브랜드 합산)")
    
    print(f"[당시즌의류] 브랜드별 아이템별 판매율 집계 완료: {len(brand_item_totals)}개 브랜드")
    print(f"[당시즌의류] 전체 아이템별 판매율 집계 완료: {len(item_totals_overall_rates)}개 아이템")
    print(f"[당시즌의류] 전체 아이템별 판매율 아이템 코드 목록: {list(item_totals_overall_rates.keys())}")
    
    # 브랜드별 집계 결과도 함께 반환 (전체현황/브랜드별 분석에서 사용)
    # brand_totals가 없으면 빈 딕셔너리 반환
    if not brand_totals:
        brand_totals = {}
    if not category_totals:
        category_totals = {}
    if not brand_item_totals:
        brand_item_totals = {}
    if not item_totals_overall_rates:
        item_totals_overall_rates = {}
    
    return result, brand_totals, category_totals, category_totals_overall_rates, brand_item_totals, item_totals_overall_rates


def process_acc_csv(file_path):
    """
    ACC 재고주수 분석 CSV 처리
    
    CSV 컬럼:
    - 브랜드코드, 카테고리, 아이템, 아이템명, 판매수량, 판매매출, 전년비, 비중, 
      4주평균판매량, 재고, 재고주수, 전년재고주수, 재고주수차이(당년-전년)
    """
    print(f"[ACC 재고주수] 파일 읽는 중: {file_path}")
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"[ACC 재고주수] 총 {len(df)}개 행 로드됨")
    print(f"[ACC 재고주수] 컬럼: {list(df.columns)}")
    
    # 브랜드별로 데이터 그룹화
    result = {}
    
    for _, row in df.iterrows():
        brand = str(row['브랜드코드']).strip()
        
        if brand not in result:
            result[brand] = []
        
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
        
        result[brand].append(item_data)
    
    print(f"[ACC 재고주수] 브랜드 수: {len(result)}")
    for brand, items in result.items():
        print(f"  - {brand}: {len(items)}개 아이템")
    
    return result


def generate_js_file(clothing_data, acc_data, update_date, output_path=None, project_root=None, brand_totals=None, category_totals=None, category_totals_overall=None, brand_item_totals=None, item_totals_overall=None, write_js=False):
    """
    대시보드 데이터 파일 생성 (JSON 중심, JS는 선택)
    
    Args:
        clothing_data: 브랜드별 아이템 데이터
        acc_data: ACC 재고주수 데이터
        update_date: 업데이트 일자 (YYYYMMDD)
        output_path: (선택) JS 출력 파일 경로
        project_root: 프로젝트 루트 디렉토리
        brand_totals: 브랜드별 집계 판매율 (전체현황/브랜드별 분석용)
        category_totals: 브랜드·카테고리별 집계 판매율
        category_totals_overall: 전체 카테고리별 집계 판매율
        brand_item_totals: 브랜드별 아이템별 판매율
        item_totals_overall: 전체 아이템별 판매율
        write_js: JS 파일 생성 여부 (기본 False)
    """
    # project_root가 없으면 output_path에서 추출
    if project_root is None:
        if output_path and 'public' in output_path:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(output_path)))
        else:
            # 스크립트 위치에서 추출
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
    
    now = datetime.now()
    
    # 주간 날짜 계산 (월요일 ~ 일요일)
    # update_date 기준으로 전주(업데이트일자 전날까지)의 월요일과 일요일 계산
    # 업데이트일자가 월요일이면, 전주 일요일이 마지막 분석 주의 종료일
    update_dt = datetime.strptime(update_date, '%Y%m%d')
    
    # 당년 주간: 업데이트일 전 주 (월~일)
    cy_week_end = update_dt - timedelta(days=1)  # 업데이트 전날 (일요일)
    cy_week_start = cy_week_end - timedelta(days=6)  # 월요일
    
    # 전년 동주차: 364일 전 (52주 = 364일, 같은 요일)
    py_week_end = cy_week_end - timedelta(days=364)
    py_week_start = cy_week_start - timedelta(days=364)
    
    # 날짜 포맷팅
    cy_week_start = cy_week_start.strftime('%Y-%m-%d')
    cy_week_end = cy_week_end.strftime('%Y-%m-%d')
    py_week_start = py_week_start.strftime('%Y-%m-%d')
    py_week_end = py_week_end.strftime('%Y-%m-%d')
    
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
        "updateDate": f"{update_date[:4]}-{update_date[4:6]}-{update_date[6:8]}",
        "cyWeekStart": cy_week_start,
        "cyWeekEnd": cy_week_end,
        "pyWeekStart": py_week_start,
        "pyWeekEnd": py_week_end,
        "cySeason": cy_season,
        "pySeason": py_season,
        "pySeasonEnd": py_season_end,
        "generatedAt": now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # 브랜드별 요약 통계 계산 (당시즌의류) - JavaScript 파일 생성 전에 계산
    clothing_summary = {}
    for brand, items in clothing_data.items():
        summary = {
            "itemCount": len(items),
            "totalWeeklySales": sum(item.get("weeklySalesTag", 0) or 0 for item in items),
            "totalCumSales": sum(item.get("cumSalesTag", 0) or 0 for item in items)
        }
        # 브랜드별 집계 판매율 및 집계 금액 추가 (재계산된 값)
        if brand_totals and brand in brand_totals:
            summary.update({
                # 집계 금액 데이터 (재계산된 값 사용)
                "totalOrderTag": brand_totals[brand].get('totalOrderTag', 0),  # 누적입고TAG가 (당년)
                "totalOrderTagPy": brand_totals[brand].get('totalOrderTagPy', 0),  # 전년누적입고TAG가
                "totalCumSalesPy": brand_totals[brand].get('totalCumSalesPy', 0),  # 전년누적판매TAG가
                "totalOrderTagPyEnd": brand_totals[brand].get('totalOrderTagPyEnd', 0),  # 전년마감누적입고TAG가
                "totalCumSalesPyEnd": brand_totals[brand].get('totalCumSalesPyEnd', 0),  # 전년마감누적판매TAG가
                # 판매율 데이터
                "cumSalesRate": brand_totals[brand].get('cumSalesRate'),
                "cumSalesRatePy": brand_totals[brand].get('cumSalesRatePy'),
                "cumSalesRateDiff": brand_totals[brand].get('cumSalesRateDiff'),
                "pyClosingSalesRate": brand_totals[brand].get('pyClosingSalesRate')
            })
        else:
            # brand_totals가 없으면 아이템별 합계 사용 (폴백)
            summary["totalOrderTag"] = sum(item.get("orderTag", 0) or 0 for item in items)
        clothing_summary[brand] = summary
    
    # ★ 전체 평균(overall) 추가 ★
    if brand_totals and 'overall' in brand_totals:
        clothing_summary['overall'] = {
            "totalOrderTag": brand_totals['overall'].get('totalOrderTag', 0),
            "totalOrderTagPy": brand_totals['overall'].get('totalOrderTagPy', 0),
            "totalCumSales": brand_totals['overall'].get('totalCumSales', 0),
            "totalCumSalesPy": brand_totals['overall'].get('totalCumSalesPy', 0),
            "totalOrderTagPyEnd": brand_totals['overall'].get('totalOrderTagPyEnd', 0),
            "totalCumSalesPyEnd": brand_totals['overall'].get('totalCumSalesPyEnd', 0),
            "cumSalesRate": brand_totals['overall'].get('cumSalesRate'),
            "cumSalesRatePy": brand_totals['overall'].get('cumSalesRatePy'),
            "cumSalesRateDiff": brand_totals['overall'].get('cumSalesRateDiff'),
            "pyClosingSalesRate": brand_totals['overall'].get('pyClosingSalesRate')
        }
    
    # JavaScript 파일 내용 생성
    js_content = f'''// 브랜드별 현황 - 당시즌의류/ACC 재고주수 분석 데이터
// 자동 생성 일시: {now.strftime('%Y-%m-%d %H:%M:%S')}
// 업데이트 일자: {update_date[:4]}-{update_date[4:6]}-{update_date[6:8]}
// 당년 주간: {cy_week_start} ~ {cy_week_end}
// 전년 동주차: {py_week_start} ~ {py_week_end}

(function() {{
  // 메타데이터
  var brandStockMetadata = {json.dumps(metadata, ensure_ascii=False, indent=2)};
  
  // 당시즌의류 브랜드별 현황 (ACC 제외)
  var clothingBrandStatus = {json.dumps(clothing_data, ensure_ascii=False, indent=2)};
  
  // ACC 재고주수 분석
  var accStockAnalysis = {json.dumps(acc_data, ensure_ascii=False, indent=2)};
  
  // 브랜드별 요약 통계 (당시즌의류)
  var clothingSummary = {json.dumps(clothing_summary, ensure_ascii=False, indent=2)};

  // 브랜드별 집계 판매율 (재계산된 값)
  var clothingBrandRates = {json.dumps(brand_totals if brand_totals else {}, ensure_ascii=False, indent=2)};
  
  // 브랜드별 카테고리별 판매율 (재계산된 값)
  var clothingCategoryRates = {json.dumps(category_totals if category_totals else {}, ensure_ascii=False, indent=2)};
  
  // 전체 카테고리별 판매율 (재계산된 값)
  var clothingCategoryRatesOverall = {json.dumps(category_totals_overall if category_totals_overall else {}, ensure_ascii=False, indent=2)};
  
  // 브랜드별 아이템별 판매율 (재계산된 값)
  var clothingBrandItemRates = {json.dumps(brand_item_totals if brand_item_totals else {}, ensure_ascii=False, indent=2)};
  
  // 전체 아이템별 판매율 (재계산된 값)
  var clothingItemRatesOverall = {json.dumps(item_totals_overall if item_totals_overall else {}, ensure_ascii=False, indent=2)};
  
  // 브랜드별 요약 통계 (ACC)
  var accSummary = {{}};
  for (var brand in accStockAnalysis) {{
    var items = accStockAnalysis[brand];
    accSummary[brand] = {{
      itemCount: items.length,
      totalSaleQty: items.reduce(function(sum, item) {{ return sum + (item.saleQty || 0); }}, 0),
      totalSaleAmt: items.reduce(function(sum, item) {{ return sum + (item.saleAmt || 0); }}, 0),
      totalStockQty: items.reduce(function(sum, item) {{ return sum + (item.stockQty || 0); }}, 0)
    }};
  }}
  
  // 전역 객체에 할당
  if (typeof window !== 'undefined') {{
    window.brandStockMetadata = brandStockMetadata;
    window.clothingBrandStatus = clothingBrandStatus;
    window.accStockAnalysis = accStockAnalysis;
    window.clothingSummary = clothingSummary;
    window.accSummary = accSummary;
    window.clothingBrandRates = clothingBrandRates;
    window.clothingCategoryRates = clothingCategoryRates;
    window.clothingCategoryRatesOverall = clothingCategoryRatesOverall;
    window.clothingBrandItemRates = clothingBrandItemRates;
    window.clothingItemRatesOverall = clothingItemRatesOverall;
  }}
}})();
'''
    
    # 파일 저장
    print(f"\n[출력] JavaScript 파일 저장 중: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    print(f"[출력] 파일 저장 완료!")
    
    # JSON 파일도 별도로 저장 (다른 데이터와 동일한 경로: /public/data/{dateStr}/stock_analysis.json)
    json_dir = os.path.join(project_root, 'public', 'data', update_date)
    os.makedirs(json_dir, exist_ok=True)
    json_output_path = os.path.join(json_dir, 'stock_analysis.json')
    
    # 브랜드별 요약 통계 계산 (당시즌의류)
    clothing_summary = {}
    for brand, items in clothing_data.items():
        summary = {
            "itemCount": len(items),
            "totalWeeklySales": sum(item.get("weeklySalesTag", 0) or 0 for item in items),
            "totalCumSales": sum(item.get("cumSalesTag", 0) or 0 for item in items)
        }
        # 브랜드별 집계 판매율 및 집계 금액 추가 (재계산된 값)
        if brand_totals and brand in brand_totals:
            summary.update({
                # 집계 금액 데이터 (재계산된 값 사용)
                "totalOrderTag": brand_totals[brand].get('totalOrderTag', 0),  # 누적입고TAG가 (당년)
                "totalOrderTagPy": brand_totals[brand].get('totalOrderTagPy', 0),  # 전년누적입고TAG가
                "totalCumSalesPy": brand_totals[brand].get('totalCumSalesPy', 0),  # 전년누적판매TAG가
                "totalOrderTagPyEnd": brand_totals[brand].get('totalOrderTagPyEnd', 0),  # 전년마감누적입고TAG가
                "totalCumSalesPyEnd": brand_totals[brand].get('totalCumSalesPyEnd', 0),  # 전년마감누적판매TAG가
                # 판매율 데이터
                "cumSalesRate": brand_totals[brand].get('cumSalesRate'),
                "cumSalesRatePy": brand_totals[brand].get('cumSalesRatePy'),
                "cumSalesRateDiff": brand_totals[brand].get('cumSalesRateDiff'),
                "pyClosingSalesRate": brand_totals[brand].get('pyClosingSalesRate')
            })
        else:
            # brand_totals가 없으면 아이템별 합계 사용 (폴백)
            summary["totalOrderTag"] = sum(item.get("orderTag", 0) or 0 for item in items)
        clothing_summary[brand] = summary
    
    # ★ 전체 평균(overall) 추가 ★
    if brand_totals and 'overall' in brand_totals:
        clothing_summary['overall'] = {
            "totalOrderTag": brand_totals['overall'].get('totalOrderTag', 0),
            "totalOrderTagPy": brand_totals['overall'].get('totalOrderTagPy', 0),
            "totalCumSales": brand_totals['overall'].get('totalCumSales', 0),
            "totalCumSalesPy": brand_totals['overall'].get('totalCumSalesPy', 0),
            "totalOrderTagPyEnd": brand_totals['overall'].get('totalOrderTagPyEnd', 0),
            "totalCumSalesPyEnd": brand_totals['overall'].get('totalCumSalesPyEnd', 0),
            "cumSalesRate": brand_totals['overall'].get('cumSalesRate'),
            "cumSalesRatePy": brand_totals['overall'].get('cumSalesRatePy'),
            "cumSalesRateDiff": brand_totals['overall'].get('cumSalesRateDiff'),
            "pyClosingSalesRate": brand_totals['overall'].get('pyClosingSalesRate')
        }
    
    # 브랜드별 요약 통계 계산 (ACC)
    acc_summary = {}
    for brand, items in acc_data.items():
        acc_summary[brand] = {
            "itemCount": len(items),
            "totalSaleQty": sum(item.get("saleQty", 0) or 0 for item in items),
            "totalSaleAmt": sum(item.get("saleAmt", 0) or 0 for item in items),
            "totalStockQty": sum(item.get("stockQty", 0) or 0 for item in items)
        }
    
    # 브랜드·카테고리별 판매율 집계 추가
    category_totals = category_totals or {}
    category_totals_overall = category_totals_overall or {}
    brand_item_totals = brand_item_totals or {}
    item_totals_overall = item_totals_overall or {}
    
    # Dashboard.html에서 기대하는 구조: brandStockMetadata, clothingBrandStatus, accStockAnalysis, clothingSummary, accSummary
    json_data = {
        "brandStockMetadata": metadata,
        "clothingBrandStatus": clothing_data,
        "accStockAnalysis": acc_data,
        "clothingSummary": clothing_summary,
        "accSummary": acc_summary,
        # 추가: 브랜드별·카테고리별 판매율 집계
        "clothingBrandRates": brand_totals,
        "clothingCategoryRates": category_totals,
        "clothingCategoryRatesOverall": category_totals_overall,
        # 추가: 브랜드별 아이템별 판매율 집계
        "clothingBrandItemRates": brand_item_totals,
        "clothingItemRatesOverall": item_totals_overall
    }
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    print(f"[출력] JSON 파일 저장됨: {json_output_path}")
    
    return metadata


def generate_json_file(clothing_data, acc_data, update_date, project_root=None, brand_totals=None, category_totals=None, category_totals_overall=None, brand_item_totals=None, item_totals_overall=None):
    """
    JS 없이 JSON만 생성하는 경량 출력 함수
    """
    if project_root is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)

    now = datetime.now()
    update_dt = datetime.strptime(update_date, '%Y%m%d')

    cy_week_end_dt = update_dt - timedelta(days=1)
    cy_week_start_dt = cy_week_end_dt - timedelta(days=6)
    py_week_end_dt = cy_week_end_dt - timedelta(days=364)
    py_week_start_dt = cy_week_start_dt - timedelta(days=364)

    # 시즌 계산 로직 (JS 생성 함수와 동일)
    current_month = update_dt.month
    if current_month >= 8 or current_month <= 2:
        cy_season = f"{update_dt.year % 100}F" if current_month >= 8 else f"{(update_dt.year - 1) % 100}F"
        py_season = f"{(int(cy_season[:2]) - 1)}F"
    else:
        cy_season = f"{update_dt.year % 100}S"
        py_season = f"{(int(cy_season[:2]) - 1)}S"

    if 'F' in cy_season:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year + 1}-02-28"
    else:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year}-07-31"

    metadata = {
        "updateDate": f"{update_date[:4]}-{update_date[4:6]}-{update_date[6:8]}",
        "cyWeekStart": cy_week_start_dt.strftime('%Y-%m-%d'),
        "cyWeekEnd": cy_week_end_dt.strftime('%Y-%m-%d'),
        "pyWeekStart": py_week_start_dt.strftime('%Y-%m-%d'),
        "pyWeekEnd": py_week_end_dt.strftime('%Y-%m-%d'),
        "cySeason": cy_season,
        "pySeason": py_season,
        "pySeasonEnd": py_season_end,
        "generatedAt": now.strftime('%Y-%m-%d %H:%M:%S')
    }

    clothing_summary = {}
    for brand, items in clothing_data.items():
        summary = {
            "itemCount": len(items),
            "totalWeeklySales": sum(item.get("weeklySalesTag", 0) or 0 for item in items),
            "totalCumSales": sum(item.get("cumSalesTag", 0) or 0 for item in items)
        }
        if brand_totals and brand in brand_totals:
            summary.update({
                # 집계 금액 데이터 (재계산된 값 사용)
                "totalOrderTag": brand_totals[brand].get('totalOrderTag', 0),  # 누적입고TAG가 (당년)
                "totalOrderTagPy": brand_totals[brand].get('totalOrderTagPy', 0),  # 전년누적입고TAG가
                "totalCumSalesPy": brand_totals[brand].get('totalCumSalesPy', 0),  # 전년누적판매TAG가
                "totalOrderTagPyEnd": brand_totals[brand].get('totalOrderTagPyEnd', 0),  # 전년마감누적입고TAG가
                "totalCumSalesPyEnd": brand_totals[brand].get('totalCumSalesPyEnd', 0),  # 전년마감누적판매TAG가
                # 판매율 데이터
                "cumSalesRate": brand_totals[brand].get('cumSalesRate'),
                "cumSalesRatePy": brand_totals[brand].get('cumSalesRatePy'),
                "cumSalesRateDiff": brand_totals[brand].get('cumSalesRateDiff'),
                "pyClosingSalesRate": brand_totals[brand].get('pyClosingSalesRate')
            })
        else:
            # brand_totals가 없으면 아이템별 합계 사용 (폴백)
            summary["totalOrderTag"] = sum(item.get("orderTag", 0) or 0 for item in items)
        clothing_summary[brand] = summary
    
    # ★ 전체 평균(overall) 추가 ★
    if brand_totals and 'overall' in brand_totals:
        clothing_summary['overall'] = {
            "totalOrderTag": brand_totals['overall'].get('totalOrderTag', 0),
            "totalOrderTagPy": brand_totals['overall'].get('totalOrderTagPy', 0),
            "totalCumSales": brand_totals['overall'].get('totalCumSales', 0),
            "totalCumSalesPy": brand_totals['overall'].get('totalCumSalesPy', 0),
            "totalOrderTagPyEnd": brand_totals['overall'].get('totalOrderTagPyEnd', 0),
            "totalCumSalesPyEnd": brand_totals['overall'].get('totalCumSalesPyEnd', 0),
            "cumSalesRate": brand_totals['overall'].get('cumSalesRate'),
            "cumSalesRatePy": brand_totals['overall'].get('cumSalesRatePy'),
            "cumSalesRateDiff": brand_totals['overall'].get('cumSalesRateDiff'),
            "pyClosingSalesRate": brand_totals['overall'].get('pyClosingSalesRate')
        }

    acc_summary = {}
    for brand, items in acc_data.items():
        acc_summary[brand] = {
            "itemCount": len(items),
            "totalSaleQty": sum(item.get("saleQty", 0) or 0 for item in items),
            "totalSaleAmt": sum(item.get("saleAmt", 0) or 0 for item in items),
            "totalStockQty": sum(item.get("stockQty", 0) or 0 for item in items)
        }

    category_totals = category_totals or {}
    category_totals_overall = category_totals_overall or {}
    brand_item_totals = brand_item_totals or {}
    item_totals_overall = item_totals_overall or {}

    json_dir = os.path.join(project_root, 'public', 'data', update_date)
    os.makedirs(json_dir, exist_ok=True)
    json_output_path = os.path.join(json_dir, 'stock_analysis.json')

    json_data = {
        "brandStockMetadata": metadata,
        "clothingBrandStatus": clothing_data,
        "accStockAnalysis": acc_data,
        "clothingSummary": clothing_summary,
        "accSummary": acc_summary,
        "clothingBrandRates": brand_totals,
        "clothingCategoryRates": category_totals,
        "clothingCategoryRatesOverall": category_totals_overall,
        "clothingBrandItemRates": brand_item_totals,
        "clothingItemRatesOverall": item_totals_overall
    }

    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"[출력] JSON 파일 저장됨: {json_output_path}")
    return metadata, json_output_path


def extract_date_from_filename(filename):
    """
    파일명에서 날짜 추출 (예: 당시즌의류_브랜드별현황_20251124.csv -> 20251124)
    """
    match = re.search(r'(\d{8})', filename)
    if match:
        return match.group(1)
    return None


def main():
    """
    메인 실행 함수
    """
    import sys
    
    try:
        print("=" * 60)
        print("당시즌의류/ACC 재고주수 분석 데이터 처리")
        print("=" * 60)
        
        # 프로젝트 루트 디렉토리
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        
        # 명령줄 인자로 날짜 받기 (선택적)
        update_date = None
        if len(sys.argv) > 1:
            date_arg = sys.argv[1]
            # YYYYMMDD 형식인지 확인
            if re.match(r'^\d{8}$', date_arg):
                update_date = date_arg
                print(f"[인자] 날짜 파라미터: {update_date}")
            else:
                print(f"[경고] 잘못된 날짜 형식: {date_arg} (YYYYMMDD 형식이어야 함)")
        
        # 기본 날짜 설정 (오늘 또는 가장 최근 파일의 날짜)
        default_date = datetime.now().strftime('%Y%m%d')
        
        # raw 폴더에서 날짜 추출
        raw_base = os.path.join(project_root, 'raw')
        
        # 분석월 폴더에서 CSV 파일 찾기 (metadata.json에서 analysis_month 읽기)
        analysis_month = None
        if update_date:
            # metadata.json에서 analysis_month 읽기 시도
            # 여러 가능한 경로 시도
            possible_paths = [
                os.path.join(raw_base, update_date[:6], 'current_year', update_date, 'metadata.json'),
                os.path.join(raw_base, '202511', 'current_year', update_date, 'metadata.json'),  # 20251201은 202511 폴더에 있을 수 있음
            ]
            
            # 모든 year_month 폴더에서 찾기
            if os.path.exists(raw_base):
                for folder in os.listdir(raw_base):
                    if re.match(r'\d{6}', folder):
                        possible_paths.append(os.path.join(raw_base, folder, 'current_year', update_date, 'metadata.json'))
            
            for metadata_path in possible_paths:
                if os.path.exists(metadata_path):
                    try:
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                            analysis_month = metadata.get('analysis_month')
                            if analysis_month:
                                break
                    except:
                        pass
            
            # metadata.json에서 못 찾으면 날짜에서 추출 (YYYYMMDD -> YYYYMM)
            if not analysis_month:
                analysis_month = update_date[:6]
        else:
            # 날짜가 없으면 최신 폴더 찾기
            year_month_folders = []
            if os.path.exists(raw_base):
                for folder in os.listdir(raw_base):
                    if re.match(r'\d{6}', folder):
                        year_month_folders.append(folder)
            if year_month_folders:
                analysis_month = sorted(year_month_folders)[-1]
            else:
                analysis_month = '202511'
        
        etc_folder = os.path.join(raw_base, analysis_month, 'ETC')
        print(f"[설정] 분석월: {analysis_month}")
        print(f"[설정] ETC 폴더 경로: {etc_folder}")
        
        # CSV 파일 찾기
        clothing_csv = None
        acc_csv = None
        found_date = None
        
        if os.path.exists(etc_folder):
            for file in os.listdir(etc_folder):
                if '당시즌의류_브랜드별현황' in file and file.endswith('.csv'):
                    file_date = extract_date_from_filename(file)
                    # 날짜가 지정된 경우 해당 날짜의 파일만 찾기
                    if not update_date or (file_date and file_date == update_date):
                        clothing_csv = os.path.join(etc_folder, file)
                        if file_date:
                            found_date = file_date
                elif 'ACC_재고주수분석' in file and file.endswith('.csv'):
                    file_date = extract_date_from_filename(file)
                    # 날짜가 지정된 경우 해당 날짜의 파일만 찾기
                    if not update_date or (file_date and file_date == update_date):
                        acc_csv = os.path.join(etc_folder, file)
                        if not found_date and file_date:
                            found_date = file_date
        
        if not clothing_csv or not acc_csv:
            print("[오류] CSV 파일을 찾을 수 없습니다.")
            print(f"  - 당시즌의류 CSV: {clothing_csv}")
            print(f"  - ACC 재고주수 CSV: {acc_csv}")
            print(f"  - 검색 경로: {etc_folder}")
            return 1
        
        # 날짜 결정: 인자로 받은 날짜 > CSV 파일에서 추출한 날짜 > 기본값
        if not update_date:
            update_date = found_date if found_date else default_date
        
        print(f"[설정] 업데이트 일자: {update_date}")
        print(f"[설정] 당시즌의류 CSV: {clothing_csv}")
        print(f"[설정] ACC 재고주수 CSV: {acc_csv}")
        print()
        
        # CSV 파일 처리
        clothing_data, brand_totals, category_totals, category_totals_overall, brand_item_totals, item_totals_overall = process_clothing_csv(clothing_csv)
        print()
        acc_data = process_acc_csv(acc_csv)
        print()
        
        # JSON 파일 생성 (JS 미사용)
        metadata, json_output_path = generate_json_file(
            clothing_data,
            acc_data,
            update_date,
            project_root,
            brand_totals,
            category_totals,
            category_totals_overall,
            brand_item_totals,
            item_totals_overall
        )
        
        print()
        print("=" * 60)
        print("처리 완료!")
        print("=" * 60)
        print(f"[요약] 업데이트 일자: {metadata['updateDate']}")
        print(f"[요약] 당년 주간: {metadata['cyWeekStart']} ~ {metadata['cyWeekEnd']}")
        print(f"[요약] 전년 동주차: {metadata['pyWeekStart']} ~ {metadata['pyWeekEnd']}")
        print(f"[요약] 시즌: 당년 {metadata['cySeason']}, 전년 {metadata['pySeason']}")
        print(f"[요약] 당시즌의류 브랜드 수: {len(clothing_data)}")
        print(f"[요약] ACC 재고주수 브랜드 수: {len(acc_data)}")
        print(f"[출력 파일] {json_output_path}")
        
        return 0
    except Exception as e:
        import traceback
        print(f"\n[오류] 예외 발생: {e}")
        print(f"[오류] 상세 정보:")
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    exit_code = main()
    sys.exit(exit_code if exit_code is not None else 0)





