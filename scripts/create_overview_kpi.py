#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
overview_kpi.json 생성 스크립트
==============================

브랜드별 KPI를 합산하여 전체현황 KPI를 생성합니다.

사용법:
    python scripts/create_overview_kpi.py 20260112
"""

import os
import sys
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
PUBLIC_DIR = ROOT / "public"


def create_overview_kpi(date_str: str):
    """브랜드별 KPI를 합산하여 overview_kpi.json 생성"""
    
    # 브랜드별 KPI 파일 로드
    brand_kpi_path = PUBLIC_DIR / "data" / date_str / "brand_kpi.json"
    
    if not brand_kpi_path.exists():
        print(f"[ERROR] 브랜드별 KPI 파일을 찾을 수 없습니다: {brand_kpi_path}")
        sys.exit(1)
    
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
    
    # 영업이익 계산 (현시점) - 직접이익 - 영업비
    # 영업비는 브랜드별로 계산되어 있지 않으므로, 직접이익에서 영업이익을 빼는 방식으로 계산
    # 단, operatingProfit이 있으면 사용
    operating_profit_sum = sum(b.get('operatingProfit', 0) for b in brand_kpi.values())
    if operating_profit_sum != 0:
        overview['operatingProfit'] = operating_profit_sum
    else:
        # 영업비 합계 계산 (계획 영업비 사용)
        operating_expense_plan_sum = 0
        for brand_data in brand_kpi.values():
            # 영업비는 계획 데이터에서 가져오거나, 직접이익 - 영업이익으로 역산
            # 여기서는 간단히 0으로 설정 (실제로는 brandPLData에서 가져와야 함)
            pass
        overview['operatingProfit'] = overview['directProfit'] - operating_expense_plan_sum
    
    # 영업이익 계산 (월말예상)
    operating_profit_forecast_sum = sum(b.get('operatingProfitForecast', 0) for b in brand_kpi.values())
    if operating_profit_forecast_sum != 0:
        overview['operatingProfitForecast'] = operating_profit_forecast_sum
    else:
        overview['operatingProfitForecast'] = overview['directProfitForecast'] - operating_expense_plan_sum
    
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
    
    return result


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python scripts/create_overview_kpi.py <date>")
        print("예시: python scripts/create_overview_kpi.py 20260112")
        sys.exit(1)
    
    date_str = sys.argv[1]
    create_overview_kpi(date_str)








