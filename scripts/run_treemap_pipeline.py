"""
트리맵 데이터 전체 파이프라인
==============================

1. 스노우플레이크에서 원본 데이터 다운로드
2. 데이터 전처리 (채널/아이템 매핑, 시즌 분류)
3. 트리맵 JSON 생성 (YOY 포함)

작성일: 2025-01
"""

import os
import sys
from datetime import datetime, timedelta

# 스크립트 경로 추가
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

from download_treemap_rawdata import download_treemap_data
from preprocess_treemap_data import preprocess_treemap_data
from create_treemap_data_v2 import main as create_treemap_json
from path_utils import get_current_year_file_path, extract_year_month_from_date

def run_treemap_pipeline(end_date_str: str):
    """
    트리맵 데이터 파이프라인 실행
    
    Args:
        end_date_str: 종료일 (YYYYMMDD)
    """
    print("=" * 80)
    print("트리맵 데이터 파이프라인")
    print("=" * 80)
    print(f"종료일: {end_date_str}\n")
    
    # 날짜 계산
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    # 시작일: 해당 주차의 월요일 (동주차 로직)
    # 일요일(6)이면 다음 월요일부터, 아니면 이번 주 월요일부터
    weekday = end_date.weekday()
    if weekday == 6:  # 일요일
        start_date = end_date + timedelta(days=1)
    else:
        start_date = end_date - timedelta(days=weekday)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str_formatted = end_date.strftime('%Y-%m-%d')
    
    print(f"기간: {start_date_str} ~ {end_date_str_formatted}")
    print(f"  (동주차: {start_date.strftime('%W')}주차)\n")
    
    year_month = extract_year_month_from_date(end_date_str)
    
    try:
        # Step 1: 스노우플레이크에서 원본 데이터 다운로드
        print("\n" + "=" * 80)
        print("Step 1: 원본 데이터 다운로드")
        print("=" * 80)
        
        raw_filename = f"treemap_raw_{end_date_str}.csv"
        raw_filepath = get_current_year_file_path(end_date_str, raw_filename)
        
        download_treemap_data(start_date_str, end_date_str_formatted, raw_filepath)
        
        # Step 2: 데이터 전처리
        print("\n" + "=" * 80)
        print("Step 2: 데이터 전처리")
        print("=" * 80)
        
        preprocessed_filename = f"treemap_preprocessed_{end_date_str}.csv"
        preprocessed_filepath = get_current_year_file_path(end_date_str, preprocessed_filename)
        
        preprocess_treemap_data(raw_filepath, preprocessed_filepath, end_date)
        
        # Step 3: 트리맵 JSON 생성
        print("\n" + "=" * 80)
        print("Step 3: 트리맵 JSON 생성")
        print("=" * 80)
        
        # create_treemap_data_v2의 main 함수 호출을 위해 sys.argv 설정
        old_argv = sys.argv
        sys.argv = ['create_treemap_data_v2.py', end_date_str]
        
        try:
            result = create_treemap_json()
            sys.argv = old_argv
            
            if result != 0:
                raise Exception("트리맵 JSON 생성 실패")
        except:
            sys.argv = old_argv
            raise
        
        print("\n" + "=" * 80)
        print("✅ 트리맵 파이프라인 완료!")
        print("=" * 80)
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] 파이프라인 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="트리맵 데이터 전체 파이프라인")
    parser.add_argument("date", help="종료일 (YYYYMMDD)")
    
    args = parser.parse_args()
    
    return run_treemap_pipeline(args.date)

if __name__ == "__main__":
    sys.exit(main())





