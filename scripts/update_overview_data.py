#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
전체현황 데이터 업데이트 스크립트
================================

브랜드별 KPI를 합산하여 overview_kpi.json을 생성합니다.

사용법:
    python scripts/update_overview_data.py 20260112
"""

import os
import sys
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
PUBLIC_DIR = ROOT / "public"


def update_overview_data(date_str: str):
    """브랜드별 KPI를 합산하여 overview_kpi.json 생성"""
    
    # 브랜드별 KPI 파일 로드
    brand_kpi_path = PUBLIC_DIR / "data" / date_str / "brand_kpi.json"
    
    if not brand_kpi_path.exists():
        print(f"[ERROR] 브랜드별 KPI 파일을 찾을 수 없습니다: {brand_kpi_path}")
        print(f"[INFO] 먼저 update_brand_kpi.py를 실행하여 brand_kpi.json을 생성하세요.")
        return False
    
    with open(brand_kpi_path, 'r', encoding='utf-8') as f:
        brand_kpi = json.load(f)
    
    print(f"[읽기] {brand_kpi_path}")
    print(f"  브랜드 수: {len(brand_kpi)}")
    
    # 브랜드별 합계 계산
    overview = {
        # 현시점 데이터
        'revenue': sum(b.get('revenue', 0) for b in brand_kpi.values()),
        'directProfit': sum(b.get('directProfit', 0) for b in brand_kpi.values()),
        
        # 월말예상 데이터
        'revenueForecast': sum(b.get('revenueForecast', 0) for b in brand_kpi.values()),
        'directProfitForecast': sum(b.get('directProfitForecast', 0) for b in brand_kpi.values()),
        
        # 전년 데이터
        'revenuePrevious': sum(b.get('revenuePrevious', 0) for b in brand_kpi.values()),
        'directProfitPrevious': sum(b.get('directProfitPrevious', 0) for b in brand_kpi.values()),
        'operatingProfitPrevious': sum(b.get('operatingProfitPrevious', 0) for b in brand_kpi.values()),
        
        # 계획 데이터
        'revenuePlan': sum(b.get('revenuePlan', 0) for b in brand_kpi.values()),
        'directProfitPlan': sum(b.get('directProfitPlan', 0) for b in brand_kpi.values()),
    }
    
    # 직접이익율 계산 (현시점)
    if overview['revenue'] > 0:
        overview['directProfitRate'] = round(
            (overview['directProfit'] / overview['revenue']) * 1.1 * 100, 2
        )
    else:
        overview['directProfitRate'] = 0.0
    
    # 직접이익율 계산 (월말예상)
    if overview['revenueForecast'] > 0:
        overview['directProfitRateForecast'] = round(
            (overview['directProfitForecast'] / overview['revenueForecast']) * 1.1 * 100, 2
        )
    else:
        overview['directProfitRateForecast'] = 0.0
    
    # 직접이익율 계산 (계획)
    if overview['revenuePlan'] > 0:
        overview['directProfitRatePlan'] = round(
            (overview['directProfitPlan'] / overview['revenuePlan']) * 1.1 * 100, 2
        )
    else:
        overview['directProfitRatePlan'] = 0.0
    
    # 할인율 계산 (현시점) - 브랜드별 가중평균
    total_tag_revenue = 0
    total_discount_amount = 0
    for brand_data in brand_kpi.values():
        revenue = brand_data.get('revenue', 0)
        discount_rate = brand_data.get('discountRate', 0)
        if revenue > 0 and discount_rate > 0:
            # 할인율로부터 TAG매출 역산: revenue / (1 - discount_rate/100)
            tag_revenue = revenue / (1 - discount_rate / 100) if discount_rate < 100 else revenue
            total_tag_revenue += tag_revenue
            total_discount_amount += tag_revenue * (discount_rate / 100)
    
    if total_tag_revenue > 0:
        overview['discountRate'] = round((total_discount_amount / total_tag_revenue) * 100, 2)
    else:
        overview['discountRate'] = 0.0
    
    # 할인율 계산 (월말예상)
    total_tag_revenue_forecast = 0
    total_discount_amount_forecast = 0
    for brand_data in brand_kpi.values():
        revenue_forecast = brand_data.get('revenueForecast', 0)
        discount_rate_forecast = brand_data.get('discountRateForecast', 0)
        if revenue_forecast > 0 and discount_rate_forecast > 0:
            tag_revenue_forecast = revenue_forecast / (1 - discount_rate_forecast / 100) if discount_rate_forecast < 100 else revenue_forecast
            total_tag_revenue_forecast += tag_revenue_forecast
            total_discount_amount_forecast += tag_revenue_forecast * (discount_rate_forecast / 100)
    
    if total_tag_revenue_forecast > 0:
        overview['discountRateForecast'] = round((total_discount_amount_forecast / total_tag_revenue_forecast) * 100, 2)
    else:
        overview['discountRateForecast'] = 0.0
    
    # 진척율 계산 (현시점)
    if overview['directProfitPlan'] > 0:
        overview['progressRate'] = round(
            (overview['directProfit'] / overview['directProfitPlan']) * 100, 1
        )
    else:
        overview['progressRate'] = 0.0
    
    # 진척율 계산 (월말예상)
    if overview['directProfitPlan'] > 0:
        overview['progressRateForecast'] = round(
            (overview['directProfitForecast'] / overview['directProfitPlan']) * 100, 1
        )
    else:
        overview['progressRateForecast'] = 0.0
    
    # 영업이익 계산 (현시점/월말예상) - 브랜드별 합산
    overview['operatingProfit'] = sum(b.get('operatingProfit', 0) for b in brand_kpi.values())
    overview['operatingProfitForecast'] = sum(b.get('operatingProfitForecast', 0) for b in brand_kpi.values())
    
    # 영업이익율 계산
    if overview['revenue'] > 0:
        overview['operatingProfitRate'] = round(
            (overview['operatingProfit'] / overview['revenue']) * 1.1 * 100, 2
        )
    else:
        overview['operatingProfitRate'] = 0.0
    
    if overview['revenueForecast'] > 0:
        overview['operatingProfitRateForecast'] = round(
            (overview['operatingProfitForecast'] / overview['revenueForecast']) * 1.1 * 100, 2
        )
    else:
        overview['operatingProfitRateForecast'] = 0.0
    
    # 목표대비/전년대비 계산
    if overview['revenuePlan'] > 0:
        overview['revenueVsPlan'] = round(
            ((overview['revenueForecast'] - overview['revenuePlan']) / overview['revenuePlan']) * 100, 1
        )
    else:
        overview['revenueVsPlan'] = 0.0
    
    if overview['revenuePrevious'] > 0:
        overview['revenueVsPrevious'] = round(
            ((overview['revenueForecast'] - overview['revenuePrevious']) / overview['revenuePrevious']) * 100, 1
        )
    else:
        overview['revenueVsPrevious'] = 0.0
    
    if overview['directProfitPlan'] > 0:
        overview['profitVsPlan'] = round(
            ((overview['directProfitForecast'] - overview['directProfitPlan']) / overview['directProfitPlan']) * 100, 1
        )
    else:
        overview['profitVsPlan'] = 0.0
    
    if overview['directProfitPrevious'] > 0:
        overview['profitVsPrevious'] = round(
            ((overview['directProfitForecast'] - overview['directProfitPrevious']) / overview['directProfitPrevious']) * 100, 1
        )
    else:
        overview['profitVsPrevious'] = 0.0
    
    # 결과 저장
    output_dir = PUBLIC_DIR / "data" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "overview_kpi.json"
    
    result = {
        "OVERVIEW": overview
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n[저장] {output_path}")
    print(f"\n[계산 결과]")
    print(f"  현시점 실판매출: {overview['revenue']:,.0f}원")
    print(f"  월말예상 실판매출: {overview['revenueForecast']:,.0f}원")
    print(f"  현시점 직접이익: {overview['directProfit']:,.0f}원")
    print(f"  월말예상 직접이익: {overview['directProfitForecast']:,.0f}원")
    print(f"  목표대비 매출: {overview['revenueVsPlan']:+.1f}%")
    print(f"  전년대비 매출: {overview['revenueVsPrevious']:+.1f}%")
    
    # 2. overview_pl.json 생성 (브랜드별 PL 데이터 합산)
    brand_pl_path = PUBLIC_DIR / "data" / date_str / "brand_pl.json"
    if brand_pl_path.exists():
        print(f"\n[생성] overview_pl.json")
        with open(brand_pl_path, 'r', encoding='utf-8') as f:
            brand_pl = json.load(f)
        
        # 브랜드별 PL 데이터 합산
        overview_pl = {
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
        
        # 브랜드별 합산 (억원 단위)
        for brand_name, pl_data in brand_pl.items():
            for key in ['tagRevenue', 'revenue', 'cog', 'grossProfit', 'directCost', 'directProfit', 'operatingExpense', 'opProfit']:
                if key in pl_data:
                    for period in ['prev', 'target', 'forecast']:
                        if period in pl_data[key]:
                            overview_pl[key][period] += pl_data[key][period]
            
            # 직접비 세부 항목 합산
            if 'directCostDetail' in pl_data:
                for detail_key in overview_pl['directCostDetail'].keys():
                    if detail_key in pl_data['directCostDetail']:
                        for period in ['prev', 'target', 'forecast']:
                            if period in pl_data['directCostDetail'][detail_key]:
                                overview_pl['directCostDetail'][detail_key][period] += pl_data['directCostDetail'][detail_key][period]
        
        # YOY 및 Achievement 계산
        for key in ['tagRevenue', 'revenue', 'cog', 'grossProfit', 'directCost', 'directProfit', 'operatingExpense', 'opProfit']:
            if overview_pl[key]['prev'] > 0:
                overview_pl[key]['yoy'] = round((overview_pl[key]['forecast'] / overview_pl[key]['prev']) * 100)
            if overview_pl[key]['target'] > 0:
                overview_pl[key]['achievement'] = round((overview_pl[key]['forecast'] / overview_pl[key]['target']) * 100)
        
        # 직접비 세부 항목 YOY 및 Achievement
        for detail_key in overview_pl['directCostDetail'].keys():
            if overview_pl['directCostDetail'][detail_key]['prev'] > 0:
                overview_pl['directCostDetail'][detail_key]['yoy'] = round(
                    (overview_pl['directCostDetail'][detail_key]['forecast'] / overview_pl['directCostDetail'][detail_key]['prev']) * 100
                )
            if overview_pl['directCostDetail'][detail_key]['target'] > 0:
                overview_pl['directCostDetail'][detail_key]['achievement'] = round(
                    (overview_pl['directCostDetail'][detail_key]['forecast'] / overview_pl['directCostDetail'][detail_key]['target']) * 100
                )
        
        # 할인율 계산
        if overview_pl['tagRevenue']['forecast'] > 0:
            overview_pl['discountRate']['forecast'] = round(
                ((overview_pl['tagRevenue']['forecast'] - overview_pl['revenue']['forecast']) / overview_pl['tagRevenue']['forecast']) * 100, 1
            )
        if overview_pl['tagRevenue']['prev'] > 0:
            overview_pl['discountRate']['prev'] = round(
                ((overview_pl['tagRevenue']['prev'] - overview_pl['revenue']['prev']) / overview_pl['tagRevenue']['prev']) * 100, 1
            )
        if overview_pl['tagRevenue']['target'] > 0:
            overview_pl['discountRate']['target'] = round(
                ((overview_pl['tagRevenue']['target'] - overview_pl['revenue']['target']) / overview_pl['tagRevenue']['target']) * 100, 1
            )
        
        # YOY 및 Achievement 계산
        if overview_pl['discountRate']['prev'] > 0:
            overview_pl['discountRate']['yoy'] = round(overview_pl['discountRate']['forecast'] - overview_pl['discountRate']['prev'], 1)
        if overview_pl['discountRate']['target'] > 0:
            overview_pl['discountRate']['achievement'] = round(overview_pl['discountRate']['forecast'] - overview_pl['discountRate']['target'], 1)
        
        # overview_pl.json 저장
        overview_pl_path = output_dir / "overview_pl.json"
        with open(overview_pl_path, 'w', encoding='utf-8') as f:
            json.dump(overview_pl, f, ensure_ascii=False, indent=2)
        print(f"  [저장] {overview_pl_path}")
    
    # 3. overview_by_brand.json 생성 (브랜드별 매출/이익 데이터)
    if brand_pl_path.exists() and brand_kpi_path.exists():
        print(f"\n[생성] overview_by_brand.json")
        with open(brand_pl_path, 'r', encoding='utf-8') as f:
            brand_pl = json.load(f)
        with open(brand_kpi_path, 'r', encoding='utf-8') as f:
            brand_kpi = json.load(f)
        
        # 브랜드명 매핑
        brand_name_map = {
            'M': 'MLB',
            'I': 'MLB KIDS',
            'X': 'DISCOVERY',
            'V': 'DUVETICA',
            'ST': 'SERGIO',
            'W': 'SUPRA'
        }
        
        overview_by_brand = []
        for brand_code, brand_name in brand_name_map.items():
            if brand_code in brand_kpi and brand_name in brand_pl:
                kpi = brand_kpi[brand_code]
                pl = brand_pl[brand_name]
                
                # 월말예상 데이터 사용
                sales = (kpi.get('revenueForecast', 0) or kpi.get('revenue', 0)) / 100000000  # 억원 단위
                direct_profit = (kpi.get('directProfitForecast', 0) or kpi.get('directProfit', 0)) / 100000000
                operating_profit = (kpi.get('operatingProfitForecast', 0) or kpi.get('operatingProfit', 0)) / 100000000
                
                # 목표대비 달성율
                achievement = 0
                if kpi.get('revenuePlan', 0) > 0:
                    achievement = round((kpi.get('revenueForecast', 0) or kpi.get('revenue', 0)) / kpi.get('revenuePlan', 1) * 100)
                
                # 전년대비 매출
                yoy_sales = 0
                if kpi.get('revenuePrevious', 0) > 0:
                    yoy_sales = round(((kpi.get('revenueForecast', 0) or kpi.get('revenue', 0)) / kpi.get('revenuePrevious', 1) - 1) * 100)
                
                overview_by_brand.append({
                    'BRAND': brand_name,
                    'SALES': round(sales, 1),
                    'DIRECT_PROFIT': round(direct_profit, 1),
                    'OPERATING_PROFIT': round(operating_profit, 1),
                    'ACHIEVEMENT': achievement,
                    'YOY_SALES': yoy_sales
                })
        
        # overview_by_brand.json 저장
        overview_by_brand_path = output_dir / "overview_by_brand.json"
        with open(overview_by_brand_path, 'w', encoding='utf-8') as f:
            json.dump(overview_by_brand, f, ensure_ascii=False, indent=2)
        print(f"  [저장] {overview_by_brand_path}")
    
    # 4. overview_waterfall.json 생성 (손익 구조 7단계)
    if brand_pl_path.exists():
        print(f"\n[생성] overview_waterfall.json")
        with open(brand_pl_path, 'r', encoding='utf-8') as f:
            brand_pl = json.load(f)
        
        # 브랜드별 합산 (월말예상 데이터 사용)
        total_revenue = 0.0
        total_cog = 0.0
        total_gross_profit = 0.0
        total_direct_cost = 0.0
        total_direct_profit = 0.0
        total_operating_expense = 0.0
        total_op_profit = 0.0
        
        for brand_name, pl_data in brand_pl.items():
            total_revenue += pl_data.get('revenue', {}).get('forecast', 0.0)
            total_cog += pl_data.get('cog', {}).get('forecast', 0.0)
            total_gross_profit += pl_data.get('grossProfit', {}).get('forecast', 0.0)
            total_direct_cost += pl_data.get('directCost', {}).get('forecast', 0.0)
            total_direct_profit += pl_data.get('directProfit', {}).get('forecast', 0.0)
            total_operating_expense += pl_data.get('operatingExpense', {}).get('forecast', 0.0)
            total_op_profit += pl_data.get('opProfit', {}).get('forecast', 0.0)
        
        # Waterfall 차트 데이터 생성 (억원 단위)
        overview_waterfall = [
            {
                'label': '실판매출',
                'value': round(total_revenue, 2),
                'type': 'total'
            },
            {
                'label': '매출원가(-)',
                'value': round(total_cog, 2),
                'type': 'decrease'
            },
            {
                'label': '매출총이익',
                'value': round(total_gross_profit, 2),
                'type': 'subtotal'
            },
            {
                'label': '직접비(-)',
                'value': round(total_direct_cost, 2),
                'type': 'decrease'
            },
            {
                'label': '직접이익',
                'value': round(total_direct_profit, 2),
                'type': 'subtotal'
            },
            {
                'label': '영업비(-)',
                'value': round(total_operating_expense, 2),
                'type': 'decrease'
            },
            {
                'label': '영업이익',
                'value': round(total_op_profit, 2),
                'type': 'result'
            }
        ]
        
        # overview_waterfall.json 저장
        overview_waterfall_path = output_dir / "overview_waterfall.json"
        with open(overview_waterfall_path, 'w', encoding='utf-8') as f:
            json.dump(overview_waterfall, f, ensure_ascii=False, indent=2)
        print(f"  [저장] {overview_waterfall_path}")
    
    # 5. overview_trend.json 생성 (월중 누적 매출 추이 - 최근 4주)
    print(f"\n[생성] overview_trend.json (최근 4주)")
    weekly_trend_path = output_dir / "weekly_trend.json"
    if weekly_trend_path.exists():
        with open(weekly_trend_path, 'r', encoding='utf-8') as f:
            weekly_trend = json.load(f)
        
        # weekly_trend.json에서 주차별 데이터 추출
        summary = weekly_trend.get('summary', {})
        total_weekly = summary.get('total', {}).get('weekly', {})
        
        # 주차별 데이터 수집 (weeks 순서대로)
        weeks_list = weekly_trend.get('weeks', [])
        weekly_current = []  # 당년 (백만원 단위)
        weekly_prev = []     # 전년 (백만원 단위)
        
        for week_label in weeks_list:
            week_data = total_weekly.get(week_label, {})
            current = week_data.get('당년', 0) / 1_000_000  # 원 -> 백만원
            prev = week_data.get('전년', 0) / 1_000_000      # 원 -> 백만원
            weekly_current.append(round(current, 1))
            weekly_prev.append(round(prev, 1))
        
        # ★★★ 최근 4주만 선택 ★★★
        weeks_list = weeks_list[-4:] if len(weeks_list) > 4 else weeks_list
        weekly_current = weekly_current[-4:] if len(weekly_current) > 4 else weekly_current
        weekly_prev = weekly_prev[-4:] if len(weekly_prev) > 4 else weekly_prev
        
        # 누적 계산 (최근 4주 기준)
        cumulative_current = []
        cumulative_prev = []
        cum_current = 0
        cum_prev = 0
        
        for i in range(len(weekly_current)):
            cum_current += weekly_current[i]
            cum_prev += weekly_prev[i]
            cumulative_current.append(round(cum_current, 1))
            cumulative_prev.append(round(cum_prev, 1))
        
        # overview_trend.json 생성
        overview_trend_data = {
            'weeks': weeks_list,
            'weekly_current': weekly_current,
            'weekly_prev': weekly_prev,
            'cumulative_current': cumulative_current,
            'cumulative_prev': cumulative_prev
        }
        
        overview_trend_path = output_dir / "overview_trend.json"
        with open(overview_trend_path, 'w', encoding='utf-8') as f:
            json.dump(overview_trend_data, f, ensure_ascii=False, indent=2)
        print(f"  [저장] {overview_trend_path}")
        print(f"  주차 수: {len(weeks_list)}주 (최근 4주)")
    else:
        print(f"  ⚠ weekly_trend.json 파일이 없습니다. overview_trend.json을 생성할 수 없습니다.")
        print(f"     [정보] download_weekly_sales_trend.py가 먼저 실행되어야 합니다.")
    
    # 6. overview.json 생성 (모든 overview 데이터 통합)
    print(f"\n[생성] overview.json")
    overview_data = {}
    
    # overview_by_brand.json 로드
    overview_by_brand_path = output_dir / "overview_by_brand.json"
    if overview_by_brand_path.exists():
        with open(overview_by_brand_path, 'r', encoding='utf-8') as f:
            overview_data['by_brand'] = json.load(f)
        print(f"  ✓ by_brand")
    
    # overview_pl.json 로드
    overview_pl_path = output_dir / "overview_pl.json"
    if overview_pl_path.exists():
        with open(overview_pl_path, 'r', encoding='utf-8') as f:
            overview_data['overviewPL'] = json.load(f)
        print(f"  ✓ overviewPL")
    
    # overview_kpi.json 로드
    overview_kpi_path = output_dir / "overview_kpi.json"
    if overview_kpi_path.exists():
        with open(overview_kpi_path, 'r', encoding='utf-8') as f:
            overview_data['overviewKPI'] = json.load(f)
        print(f"  ✓ overviewKPI")
    
    # overview_waterfall.json 로드
    overview_waterfall_path = output_dir / "overview_waterfall.json"
    if overview_waterfall_path.exists():
        with open(overview_waterfall_path, 'r', encoding='utf-8') as f:
            overview_data['waterfallData'] = json.load(f)
        print(f"  ✓ waterfallData")
    
    # overview_trend.json 로드 (cumulativeTrendData)
    overview_trend_path = output_dir / "overview_trend.json"
    if overview_trend_path.exists():
        with open(overview_trend_path, 'r', encoding='utf-8') as f:
            overview_data['cumulativeTrendData'] = json.load(f)
        print(f"  ✓ cumulativeTrendData")
    
    # stock_analysis.json에서 clothingSummary, accSummary, clothingItemRatesOverall 로드
    stock_analysis_path = output_dir / "stock_analysis.json"
    if stock_analysis_path.exists():
        with open(stock_analysis_path, 'r', encoding='utf-8') as f:
            stock_analysis = json.load(f)
        
        # clothingSummary 추가
        if 'clothingSummary' in stock_analysis:
            overview_data['clothingSummary'] = stock_analysis['clothingSummary']
            print(f"  ✓ clothingSummary")
        
        # accSummary 추가
        if 'accSummary' in stock_analysis:
            overview_data['accSummary'] = stock_analysis['accSummary']
            print(f"  ✓ accSummary")
        
        # clothingItemRatesOverall 추가 (평균판매율 데이터)
        if 'clothingItemRatesOverall' in stock_analysis:
            overview_data['clothingItemRatesOverall'] = stock_analysis['clothingItemRatesOverall']
            print(f"  ✓ clothingItemRatesOverall (평균판매율 데이터 통합)")
        else:
            print(f"  ⚠ clothingItemRatesOverall가 stock_analysis.json에 없습니다.")
    else:
        print(f"  ⚠ stock_analysis.json 파일이 없습니다. clothingItemRatesOverall를 포함할 수 없습니다.")
        print(f"     [정보] generate_brand_stock_analysis.py가 먼저 실행되어야 합니다.")
    
    # overview.json 저장
    if overview_data:
        overview_json_path = output_dir / "overview.json"
        with open(overview_json_path, 'w', encoding='utf-8') as f:
            json.dump(overview_data, f, ensure_ascii=False, indent=2)
        print(f"  [저장] {overview_json_path}")
        print(f"  [완료] overview.json 생성 완료 (총 {len(overview_data)}개 섹션)")
    else:
        print(f"  [경고] overview.json에 포함할 데이터가 없습니다.")
    
    return True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python scripts/update_overview_data.py <date>")
        print("예시: python scripts/update_overview_data.py 20260112")
        sys.exit(1)
    
    date_str = sys.argv[1]
    success = update_overview_data(date_str)
    sys.exit(0 if success else 1)
