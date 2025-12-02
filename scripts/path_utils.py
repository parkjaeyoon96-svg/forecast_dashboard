"""
데이터 경로 유틸리티 함수
새로운 폴더 구조에 맞춘 경로 생성 함수들
"""
import os
import json
import glob

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "raw")

from datetime import datetime, timedelta


def _calculate_analysis_month_from_update_date(date_str: str) -> str:
    """
    업데이트일자(YYYYMMDD)로부터 '분석월'(주차 시작일의 월)을 계산
    - KE30 전체 파이프라인(convert_ke30_to_forecast 등)에서 사용하는 로직과 동일하게 맞춤
    
    예:
        date_str = "20251201" (월요일)
        → 전주 월요일 = 2025-11-24
        → 분석월 = "202511"
    """
    if not (isinstance(date_str, str) and len(date_str) == 8 and date_str.isdigit()):
        # 형식이 이상하면 안전하게 YYYYMM 그대로 반환
        return date_str[:6]

    year = int(date_str[:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    update_date = datetime(year, month, day)

    # 요일: 0=월요일, 6=일요일
    day_of_week = update_date.weekday()

    # 전주 월요일까지의 일수 (process_ke30_full_pipeline / convert_ke30_to_forecast와 동일 로직)
    if day_of_week == 0:  # 월요일
        days_to_monday = 7
    elif day_of_week == 6:  # 일요일
        days_to_monday = 6
    else:  # 화~토요일
        days_to_monday = day_of_week + 7

    week_start_date = update_date - timedelta(days=days_to_monday)
    return f"{week_start_date.year}{week_start_date.month:02d}"


def get_analysis_month_from_metadata(date_str: str) -> str:
    """
    metadata.json에서 analysis_month 읽기 (당년 전처리와 동일한 로직)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열 (예: "20251201")
    
    Returns:
        str: YYYYMM 형식의 분석월 (예: "202511")
        metadata.json이 없으면 date_str에서 추출한 값 반환
    """
    # raw 폴더 전체에서 해당 날짜의 metadata.json 찾기
    pattern = os.path.join(RAW_DIR, "*", "current_year", date_str, "metadata.json")
    metadata_files = glob.glob(pattern)
    
    if metadata_files:
        try:
            with open(metadata_files[0], 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                analysis_month = metadata.get('analysis_month')
                if analysis_month:
                    return analysis_month
        except (json.JSONDecodeError, KeyError, IOError):
            pass
    
    # metadata.json이 없으면 "업데이트월"이 아니라
    # 업데이트일자로부터 계산한 "분석월"을 사용하도록 변경
    # (예: 20251201 → 202511)
    return _calculate_analysis_month_from_update_date(date_str)

def get_current_year_dir(date_str: str) -> str:
    """
    당년 데이터 디렉토리 경로 반환
    구조: raw/YYYYMM/current_year/YYYYMMDD/
    metadata.json에서 analysis_month를 읽어서 올바른 분석월 폴더 사용
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열 (예: "20251201")
    
    Returns:
        str: 디렉토리 경로
    """
    year_month = get_analysis_month_from_metadata(date_str)  # metadata.json에서 읽기
    return os.path.join(RAW_DIR, year_month, "current_year", date_str)

def get_current_year_file_path(date_str: str, filename: str) -> str:
    """
    당년 데이터 파일 경로 반환
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열
        filename: 파일명 (예: "ke30_20251117_202511_전처리완료.csv")
    
    Returns:
        str: 파일 전체 경로
    """
    return os.path.join(get_current_year_dir(date_str), filename)

def get_previous_year_dir(year_month: str) -> str:
    """
    전년 데이터 디렉토리 경로 반환
    구조: raw/YYYYMM/previous_year/
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202411")
    
    Returns:
        str: 디렉토리 경로
    """
    return os.path.join(RAW_DIR, year_month, "previous_year")

def get_previous_year_file_path(year_month: str, filename: str = None) -> str:
    """
    전년 데이터 파일 경로 반환
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202411")
        filename: 파일명 (없으면 기본 파일명 생성)
    
    Returns:
        str: 파일 전체 경로
    """
    if filename is None:
        filename = f"ke30_prev_{year_month}_전처리완료.csv"
    return os.path.join(get_previous_year_dir(year_month), filename)

def get_plan_dir(year_month: str) -> str:
    """
    계획 데이터 디렉토리 경로 반환
    구조: raw/YYYYMM/plan/
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202511")
    
    Returns:
        str: 디렉토리 경로
    """
    return os.path.join(RAW_DIR, year_month, "plan")

def get_plan_file_path(year_month: str, filename: str = None) -> str:
    """
    계획 데이터 파일 경로 반환
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202511")
        filename: 파일명 (없으면 기본 파일명 생성)
    
    Returns:
        str: 파일 전체 경로
    """
    if filename is None:
        filename = f"plan_{year_month}_전처리완료.csv"
    return os.path.join(get_plan_dir(year_month), filename)

def extract_year_month_from_date(date_str: str) -> str:
    """
    YYYYMMDD 형식에서 YYYYMM 추출
    metadata.json에서 analysis_month를 읽어서 올바른 분석월 반환
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열
    
    Returns:
        str: YYYYMM 형식의 분석월 문자열 (metadata.json에서 읽거나 date_str에서 추출)
    """
    return get_analysis_month_from_metadata(date_str)

def get_previous_year_month(current_date_str: str) -> str:
    """
    당년 날짜에서 전년 동월 계산
    예: 20251117 -> 202411
    
    Args:
        current_date_str: YYYYMMDD 형식의 당년 날짜
    
    Returns:
        str: YYYYMM 형식의 전년 연월
    """
    year = int(current_date_str[:4])
    month = current_date_str[4:6]
    prev_year = year - 1
    return f"{prev_year}{month}"

def get_forecast_dir(year_month: str) -> str:
    """
    예상 데이터 디렉토리 경로 반환
    구조: raw/YYYYMM/forecast/
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202511")
    
    Returns:
        str: 디렉토리 경로
    """
    return os.path.join(RAW_DIR, year_month, "forecast")

def get_forecast_file_path(year_month: str, filename: str = None) -> str:
    """
    예상 데이터 파일 경로 반환
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202511")
        filename: 파일명 (없으면 기본 파일명 생성)
    
    Returns:
        str: 파일 전체 경로
    """
    if filename is None:
        filename = f"forecast_{year_month}_전처리완료.csv"
    return os.path.join(get_forecast_dir(year_month), filename)

