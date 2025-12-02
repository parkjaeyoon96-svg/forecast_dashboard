#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.js 및 기타 JS 파일에서 JSON 추출 스크립트 v3 (선택적)
===========================================================

⚠️ 주의: 이 스크립트는 선택적입니다.
각 Python 스크립트가 이미 직접 JSON 파일을 생성하므로,
이 스크립트는 JS 파일이 남아있는 경우에만 사용됩니다.

IIFE 구조에서도 데이터 추출 가능
"""

import re
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent
PUBLIC_DIR = ROOT / "public"


def find_var_in_iife(content: str, var_name: str) -> str:
    """IIFE 내부의 var 선언에서 JSON 추출"""
    # var varName = { 또는 var varName = [ 패턴
    pattern = rf'\bvar\s+{re.escape(var_name)}\s*=\s*'
    match = re.search(pattern, content)
    
    if not match:
        return None
    
    start = match.end()
    
    # 시작 문자 찾기
    while start < len(content) and content[start] not in '{[':
        start += 1
    
    if start >= len(content):
        return None
    
    # 중괄호/대괄호 매칭
    brace_count = 0
    in_string = False
    escape_next = False
    end = start
    
    for i in range(start, len(content)):
        char = content[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
        
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if not in_string:
            if char in '{[':
                brace_count += 1
            elif char in '}]':
                brace_count -= 1
                if brace_count == 0:
                    end = i + 1
                    break
    
    return content[start:end]


def find_window_d_property(content: str, prop_name: str) -> str:
    """window.D.property = 패턴에서 JSON 추출"""
    pattern = rf'window\.D\.{re.escape(prop_name)}\s*=\s*'
    match = re.search(pattern, content)
    
    if not match:
        return None
    
    start = match.end()
    
    # 변수 참조인 경우 스킵 (예: window.D.brandPLData = brandPLData;)
    rest = content[start:start+50].strip()
    if rest and not rest.startswith('{') and not rest.startswith('['):
        return None
    
    while start < len(content) and content[start] not in '{[':
        start += 1
    
    if start >= len(content):
        return None
    
    brace_count = 0
    in_string = False
    escape_next = False
    end = start
    
    for i in range(start, len(content)):
        char = content[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
        
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if not in_string:
            if char in '{[':
                brace_count += 1
            elif char in '}]':
                brace_count -= 1
                if brace_count == 0:
                    end = i + 1
                    break
    
    return content[start:end]


def calculate_analysis_month_from_update_date(update_date_str: str) -> str:
    """
    업데이트일자로부터 주차 시작일의 월을 계산하여 분석월 반환
    (당년 전처리 스크립트와 동일한 로직)
    
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


def find_latest_date_from_raw_folder() -> str:
    """
    raw 폴더에서 최신 날짜 폴더 찾기 (당년 전처리와 동일한 로직)
    
    Returns:
        YYYYMMDD 형식의 날짜 문자열 (예: "20251124")
    """
    raw_dir = ROOT / "raw"
    
    if not raw_dir.exists():
        return None
    
    # YYYYMM 형식의 폴더 찾기 (내림차순)
    for year_month_dir in sorted(raw_dir.iterdir(), reverse=True):
        if year_month_dir.is_dir() and year_month_dir.name.isdigit() and len(year_month_dir.name) == 6:
            current_year_dir = year_month_dir / "current_year"
            if current_year_dir.exists():
                # YYYYMMDD 형식의 날짜 폴더 찾기 (내림차순)
                for date_dir in sorted(current_year_dir.iterdir(), reverse=True):
                    if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                        # 전처리 완료 파일이 있는지 확인
                        preprocessed_files = list(date_dir.glob("*_전처리완료.csv"))
                        if preprocessed_files:
                            return date_dir.name
    
    return None


def parse_json_safe(json_str: str, var_name: str) -> dict:
    """JSON 안전하게 파싱"""
    if not json_str:
        return None
    
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # 후행 쉼표 제거
        fixed = re.sub(r',(\s*[}\]])', r'\1', json_str)
        try:
            return json.loads(fixed)
        except json.JSONDecodeError as e:
            print(f"  [ERROR] {var_name} JSON 파싱 실패: {str(e)[:50]}")
            return None


def export_to_json(date_str: str):
    """모든 데이터를 JSON 파일로 내보내기"""
    
    output_dir = PUBLIC_DIR / "data" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[시작] {date_str} 데이터 JSON 추출")
    print(f"[출력] {output_dir}")
    print()
    
    # 1. data.js에서 추출 (선택적, JS 파일이 있으면 변환)
    data_js_path = PUBLIC_DIR / f"data_{date_str}.js"
    if data_js_path.exists():
        print(f"[읽기] {data_js_path.name} (선택적 변환)")
        with open(data_js_path, 'r', encoding='utf-8') as f:
            data_content = f.read()
        
        # Overview 데이터 (window.D.xxx) - 이미 개별 JSON 파일이 있으면 스킵
        overview_files_exist = all([
            (output_dir / "overview_by_brand.json").exists(),
            (output_dir / "overview_pl.json").exists(),
            (output_dir / "overview_waterfall.json").exists(),
            (output_dir / "overview_trend.json").exists()
        ])
        
        if overview_files_exist:
            print(f"  [스킵] overview 개별 JSON 파일 이미 존재, data.js에서 추출 생략")
        else:
            overview_data = {}
            for prop in ['by_brand', 'overviewPL', 'waterfallData', 'cumulativeTrendData']:
                json_str = find_window_d_property(data_content, prop)
                data = parse_json_safe(json_str, f"D.{prop}")
                if data:
                    overview_data[prop] = data
                    print(f"  ✓ D.{prop}")
                else:
                    print(f"  ✗ D.{prop}")
            
            if overview_data:
                with open(output_dir / "overview.json", 'w', encoding='utf-8') as f:
                    json.dump(overview_data, f, ensure_ascii=False, indent=2)
                print(f"  → overview.json ({len(json.dumps(overview_data))//1024}KB)")
        
        # var 변수들 추출 (이미 JSON 파일이 있으면 스킵)
        var_mappings = {
            'brandKPI': 'brand_kpi.json',
            'brandPLData': 'brand_pl.json',
            'channelPL': 'channel_pl.json',
            'channelProfitLossData': 'channel_profit_loss.json',
        }
        
        for var_name, filename in var_mappings.items():
            if (output_dir / filename).exists():
                print(f"  [스킵] {filename} 이미 존재, {var_name} 추출 생략")
            else:
                json_str = find_var_in_iife(data_content, var_name)
                data = parse_json_safe(json_str, var_name)
                if data:
                    with open(output_dir / filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"  ✓ {var_name} → {filename}")
        
        # 지표 데이터 통합 (이미 JSON 파일이 있으면 스킵)
        if (output_dir / "metrics.json").exists():
            print(f"  [스킵] metrics.json 이미 존재, data.js에서 추출 생략")
        else:
            metrics_data = {}
            for var_name in ['brandNames', 'channelItemSalesData', 'channelMetrics', 
                             'channelItemMetrics', 'itemMetrics', 'itemChannelMetrics']:
                json_str = find_var_in_iife(data_content, var_name)
                data = parse_json_safe(json_str, var_name)
                if data:
                    metrics_data[var_name] = data
                    print(f"  ✓ {var_name}")
            
            if metrics_data:
                with open(output_dir / "metrics.json", 'w', encoding='utf-8') as f:
                    json.dump(metrics_data, f, ensure_ascii=False, indent=2)
                print(f"  → metrics.json")
    
    print()
    
    # 2. weekly_sales_trend.js (선택적, JS 파일이 있으면 변환)
    weekly_js_path = PUBLIC_DIR / f"weekly_sales_trend_{date_str}.js"
    if weekly_js_path.exists():
        # 이미 JSON 파일이 있으면 스킵
        if (output_dir / "weekly_trend.json").exists():
            print(f"[스킵] weekly_trend.json 이미 존재, {weekly_js_path.name} 변환 생략")
        else:
            print(f"[읽기] {weekly_js_path.name} (선택적 변환)")
        with open(weekly_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        json_str = find_var_in_iife(content, 'weeklySalesTrend')
        if not json_str:
            # const 패턴 시도
            pattern = r'\bconst\s+weeklySalesTrend\s*=\s*'
            match = re.search(pattern, content)
            if match:
                start = match.end()
                while start < len(content) and content[start] not in '{[':
                    start += 1
                brace_count = 0
                in_string = False
                end = start
                for i in range(start, len(content)):
                    char = content[i]
                    if char == '"':
                        in_string = not in_string
                    if not in_string:
                        if char in '{[':
                            brace_count += 1
                        elif char in '}]':
                            brace_count -= 1
                            if brace_count == 0:
                                end = i + 1
                                break
                json_str = content[start:end]
        
        data = parse_json_safe(json_str, 'weeklySalesTrend')
        if data:
            with open(output_dir / "weekly_trend.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"  ✓ weeklySalesTrend → weekly_trend.json")
    
    # 3. brand_stock_analysis.js (선택적, JS 파일이 있으면 변환)
    stock_js_path = PUBLIC_DIR / f"brand_stock_analysis_{date_str}.js"
    if stock_js_path.exists():
        # 이미 JSON 파일이 있으면 스킵
        if (output_dir / "stock_analysis.json").exists():
            print(f"[스킵] stock_analysis.json 이미 존재, {stock_js_path.name} 변환 생략")
        else:
            print(f"[읽기] {stock_js_path.name} (선택적 변환)")
        with open(stock_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        stock_data = {}
        for var_name in ['brandStockMetadata', 'clothingBrandStatus', 'accStockAnalysis', 
                         'clothingSummary', 'accSummary']:
            json_str = find_var_in_iife(content, var_name)
            data = parse_json_safe(json_str, var_name)
            if data:
                stock_data[var_name] = data
                print(f"  ✓ {var_name}")
        
        if stock_data:
            with open(output_dir / "stock_analysis.json", 'w', encoding='utf-8') as f:
                json.dump(stock_data, f, ensure_ascii=False, indent=2)
            print(f"  → stock_analysis.json")
    
    # 4. treemap_data_v2.js (선택적, JS 파일이 있으면 변환)
    treemap_js_path = PUBLIC_DIR / f"treemap_data_v2_{date_str}.js"
    if treemap_js_path.exists():
        # 이미 JSON 파일이 있으면 스킵
        if (output_dir / "treemap.json").exists():
            print(f"[스킵] treemap.json 이미 존재, {treemap_js_path.name} 변환 생략")
        else:
            print(f"[읽기] {treemap_js_path.name} (선택적 변환)")
        with open(treemap_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        json_str = find_var_in_iife(content, 'channelTreemapData')
        data = parse_json_safe(json_str, 'channelTreemapData')
        if data:
            with open(output_dir / "treemap.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"  ✓ channelTreemapData → treemap.json")
    
    print()
    print("=" * 50)
    print(f"[완료] JSON 파일 변환 완료 (선택적)")
    print(f"[참고] 각 Python 스크립트가 이미 직접 JSON 파일을 생성합니다.")
    print(f"[참고] 이 스크립트는 JS 파일이 남아있는 경우에만 사용됩니다.")
    
    # 생성된 파일 목록
    print()
    print("생성된 파일:")
    total_size = 0
    for f in sorted(output_dir.glob("*.json")):
        size_kb = f.stat().st_size / 1024
        total_size += size_kb
        print(f"  ✓ {f.name} ({size_kb:.1f} KB)")
    print(f"\n총 크기: {total_size:.1f} KB")


if __name__ == '__main__':
    # 날짜 인자가 없으면 raw 폴더에서 최신 날짜 찾기
    if len(sys.argv) < 2:
        print("[정보] 날짜 인자가 없습니다. raw 폴더에서 최신 날짜를 찾습니다...")
        date_str = find_latest_date_from_raw_folder()
        
        if not date_str:
            print("[오류] raw 폴더에서 날짜를 찾을 수 없습니다.")
            print("사용법: python export_to_json.py <date>")
            print("예시: python export_to_json.py 20251124")
            sys.exit(1)
        
        print(f"[자동] 최신 날짜 사용: {date_str}")
        print()
    else:
        date_str = sys.argv[1]
        
        # 날짜 형식 검증 (YYYYMMDD)
        if not re.match(r'^\d{8}$', date_str):
            print(f"[오류] 날짜 형식이 올바르지 않습니다: {date_str}")
            print("올바른 형식: YYYYMMDD (예: 20251124)")
            sys.exit(1)
    
    export_to_json(date_str)
