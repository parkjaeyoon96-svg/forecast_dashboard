#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""브랜드별 합계와 overview_kpi.json 비교"""

import json
from pathlib import Path

date_str = "20260112"
brand_kpi_path = Path(f"public/data/{date_str}/brand_kpi.json")
overview_kpi_path = Path(f"public/data/{date_str}/overview_kpi.json")

with open(brand_kpi_path, 'r', encoding='utf-8') as f:
    brand_kpi = json.load(f)

with open(overview_kpi_path, 'r', encoding='utf-8') as f:
    overview_kpi = json.load(f)

# 브랜드별 합계 계산
revenue_sum = sum(b['revenue'] for b in brand_kpi.values())
direct_profit_sum = sum(b['directProfit'] for b in brand_kpi.values())
revenue_forecast_sum = sum(b.get('revenueForecast', 0) for b in brand_kpi.values())
direct_profit_forecast_sum = sum(b.get('directProfitForecast', 0) for b in brand_kpi.values())
revenue_previous_sum = sum(b.get('revenuePrevious', 0) for b in brand_kpi.values())
direct_profit_previous_sum = sum(b.get('directProfitPrevious', 0) for b in brand_kpi.values())
revenue_plan_sum = sum(b.get('revenuePlan', 0) for b in brand_kpi.values())
direct_profit_plan_sum = sum(b.get('directProfitPlan', 0) for b in brand_kpi.values())

overview = overview_kpi['OVERVIEW']

print("=" * 60)
print("브랜드별 합계 vs overview_kpi.json 비교")
print("=" * 60)
print()

print(f"[현시점 실판매출]")
print(f"  브랜드별 합계: {revenue_sum:,.0f}원")
print(f"  overview_kpi.json: {overview['revenue']:,.0f}원")
print(f"  차이: {abs(revenue_sum - overview['revenue']):,.0f}원")
print()

print(f"[현시점 직접이익]")
print(f"  브랜드별 합계: {direct_profit_sum:,.0f}원")
print(f"  overview_kpi.json: {overview['directProfit']:,.0f}원")
print(f"  차이: {abs(direct_profit_sum - overview['directProfit']):,.0f}원")
print()

print(f"[월말예상 실판매출]")
print(f"  브랜드별 합계: {revenue_forecast_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('revenueForecast', 0):,.0f}원")
print(f"  차이: {abs(revenue_forecast_sum - overview.get('revenueForecast', 0)):,.0f}원")
print()

print(f"[월말예상 직접이익]")
print(f"  브랜드별 합계: {direct_profit_forecast_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('directProfitForecast', 0):,.0f}원")
print(f"  차이: {abs(direct_profit_forecast_sum - overview.get('directProfitForecast', 0)):,.0f}원")
print()

print(f"[전년 실판매출]")
print(f"  브랜드별 합계: {revenue_previous_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('revenuePrevious', 0):,.0f}원")
print(f"  차이: {abs(revenue_previous_sum - overview.get('revenuePrevious', 0)):,.0f}원")
print()

print(f"[전년 직접이익]")
print(f"  브랜드별 합계: {direct_profit_previous_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('directProfitPrevious', 0):,.0f}원")
print(f"  차이: {abs(direct_profit_previous_sum - overview.get('directProfitPrevious', 0)):,.0f}원")
print()

print(f"[계획 실판매출]")
print(f"  브랜드별 합계: {revenue_plan_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('revenuePlan', 0):,.0f}원")
print(f"  차이: {abs(revenue_plan_sum - overview.get('revenuePlan', 0)):,.0f}원")
print()

print(f"[계획 직접이익]")
print(f"  브랜드별 합계: {direct_profit_plan_sum:,.0f}원")
print(f"  overview_kpi.json: {overview.get('directProfitPlan', 0):,.0f}원")
print(f"  차이: {abs(direct_profit_plan_sum - overview.get('directProfitPlan', 0)):,.0f}원")
print()

print("=" * 60)
print("브랜드별 상세 내역")
print("=" * 60)
for brand, data in brand_kpi.items():
    print(f"{brand}: revenue={data['revenue']:,.0f}, directProfit={data['directProfit']:,.0f}")








