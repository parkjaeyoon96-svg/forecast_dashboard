#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
특정 날짜의 metadata.json에서 analysis_month를 읽어오는 스크립트
"""
import json
import os
import glob
import sys


def get_analysis_month_from_date(date_str: str) -> str:
    """
    특정 날짜의 metadata.json에서 analysis_month를 읽어온다.
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 (예: "20251201")
    
    Returns:
        analysis_month (YYYYMM 형식) 또는 빈 문자열
    """
    # raw/*/current_year/YYYYMMDD/metadata.json 패턴으로 찾기
    patterns = glob.glob(os.path.join("raw", "*", "current_year", date_str, "metadata.json"))
    
    if not patterns:
        # 패턴을 찾지 못한 경우 빈 문자열 반환
        return ""
    
    # 첫 번째 매칭되는 파일 읽기
    metadata_path = patterns[0]
    
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        analysis_month = data.get("analysis_month", "")
        
        # analysis_month가 없으면 날짜의 앞 6자리 사용
        if not analysis_month and len(date_str) >= 6:
            analysis_month = date_str[:6]
        
        return analysis_month
    except Exception as e:
        # 오류 발생 시 빈 문자열 반환
        return ""


def main():
    if len(sys.argv) < 2:
        print("")
        sys.exit(1)
    
    date_str = sys.argv[1]
    analysis_month = get_analysis_month_from_date(date_str)
    print(analysis_month)
    
    sys.exit(0 if analysis_month else 1)


if __name__ == "__main__":
    main()



























