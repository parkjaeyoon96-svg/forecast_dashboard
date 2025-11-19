"""
데이터 경로 유틸리티 함수
새로운 폴더 구조에 맞춘 경로 생성 함수들
"""
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "raw")

def get_current_year_dir(date_str: str) -> str:
    """
    당년 데이터 디렉토리 경로 반환
    구조: raw/YYYYMM/present/YYYYMMDD/
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열 (예: "20251117")
    
    Returns:
        str: 디렉토리 경로
    """
    year_month = date_str[:6]  # "202511"
    return os.path.join(RAW_DIR, year_month, "present", date_str)

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
    구조: raw/YYYYMM/previous/
    
    Args:
        year_month: YYYYMM 형식의 연월 문자열 (예: "202411")
    
    Returns:
        str: 디렉토리 경로
    """
    return os.path.join(RAW_DIR, year_month, "previous")

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
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열
    
    Returns:
        str: YYYYMM 형식의 연월 문자열
    """
    return date_str[:6]

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

