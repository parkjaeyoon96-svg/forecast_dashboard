"""
전년 트리맵 데이터 다운로드 및 전처리
======================================

전년 동주차 데이터를 다운로드하여 YOY 계산에 사용

작성일: 2025-01
"""

import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

from download_treemap_rawdata import download_treemap_data
from preprocess_treemap_data import preprocess_treemap_data
from path_utils import extract_year_month_from_date

def download_previous_year_treemap(current_date_str: str):
    """
    전년 동주차 트리맵 데이터 다운로드 및 전처리
    
    Args:
        current_date_str: 당년 날짜 (YYYYMMDD)
    """
    print("=" * 80)
    print("전년 트리맵 데이터 다운로드")
    print("=" * 80)
    
    # 당년 날짜 파싱
    current_date = datetime.strptime(current_date_str, '%Y%m%d')
    
    # 전년 동일 날짜
    prev_date = current_date.replace(year=current_date.year - 1)
    
    # 시작일: 해당 주차의 월요일
    weekday = prev_date.weekday()
    if weekday == 6:  # 일요일
        start_date = prev_date + timedelta(days=1)
    else:
        start_date = prev_date - timedelta(days=weekday)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = prev_date.strftime('%Y-%m-%d')
    prev_date_str = prev_date.strftime('%Y%m%d')
    
    print(f"당년: {current_date_str}")
    print(f"전년: {prev_date_str}")
    print(f"기간: {start_date_str} ~ {end_date_str}\n")
    
    # 저장 경로
    year_month = extract_year_month_from_date(current_date_str)
    prev_dir = os.path.join(ROOT, "raw", year_month, "previous_year")
    os.makedirs(prev_dir, exist_ok=True)
    
    try:
        # Step 1: 원본 데이터 다운로드
        print("\n" + "=" * 80)
        print("Step 1: 전년 원본 데이터 다운로드")
        print("=" * 80)
        
        raw_filepath = os.path.join(prev_dir, f"treemap_raw_prev_{current_date_str}.csv")
        download_treemap_data(start_date_str, end_date_str, raw_filepath)
        
        # Step 2: 전처리
        print("\n" + "=" * 80)
        print("Step 2: 전년 데이터 전처리")
        print("=" * 80)
        
        preprocessed_filepath = os.path.join(prev_dir, f"treemap_preprocessed_prev_{current_date_str}.csv")
        preprocess_treemap_data(raw_filepath, preprocessed_filepath, prev_date)
        
        print("\n" + "=" * 80)
        print("✅ 전년 트리맵 데이터 준비 완료!")
        print("=" * 80)
        print(f"저장 위치: {preprocessed_filepath}")
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] 전년 데이터 다운로드 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="전년 트리맵 데이터 다운로드")
    parser.add_argument("date", help="당년 날짜 (YYYYMMDD)")
    
    args = parser.parse_args()
    
    return download_previous_year_treemap(args.date)

if __name__ == "__main__":
    sys.exit(main())




