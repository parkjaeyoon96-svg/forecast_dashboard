#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
KE30 Excel 파일에서 날짜 추출 (당년 전처리와 동일한 로직)
"""
import os
import re
import glob
import sys

KE30_INPUT_DIR = r"C:\ke30"

def get_latest_ke30_date():
    """
    C:\ke30 폴더에서 최신 KE30 Excel 파일 찾아서 날짜 추출
    
    Returns:
        tuple: (date_str, year_month) 또는 (None, None)
    """
    if not os.path.exists(KE30_INPUT_DIR):
        return None, None
    
    # ke30_로 시작하는 모든 Excel 파일 찾기
    files = []
    for pattern in ['ke30_*.xlsx', 'ke30_*.xls']:
        files.extend(glob.glob(os.path.join(KE30_INPUT_DIR, pattern)))
    
    if not files:
        return None, None
    
    # 최신 파일 선택 (수정 시간 기준)
    latest_file = max(files, key=os.path.getmtime)
    base_filename = os.path.basename(latest_file)
    
    # 파일명에서 날짜 추출 (예: ke30_20251201_202511.xlsx -> 20251201, 202511)
    match = re.match(r'ke30_(\d{8})_(\d{6})', base_filename)
    if not match:
        return None, None
    
    date_str = match.group(1)  # 20251201 (업데이트일자)
    year_month = match.group(2)  # 202511 (분석월)
    
    return date_str, year_month

if __name__ == "__main__":
    date_str, year_month = get_latest_ke30_date()
    
    if len(sys.argv) > 1 and sys.argv[1] == "year_month":
        print(year_month if year_month else "")
    else:
        print(date_str if date_str else "")


