"""
전년 동월 누적 주차별 매출 데이터로부터 진척율과 진척일자 계산

사용법:
    python scripts/calculate_progress_days.py [분석월] [입력파일]
    
예시:
    python scripts/calculate_progress_days.py 202511
    python scripts/calculate_progress_days.py 2025-11 raw/202511/previous_year/cumulative_sales_202411.csv
    
설명:
    1. 전년 동월 누적 주차별 매출 데이터를 읽어서
    2. 각 주차의 진척율 계산 (누적매출 / 월말누적매출)
    3. 진척일자 환산 (진척율 × 월 일수)
    4. CSV 파일로 저장
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from calendar import monthrange
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def parse_analysis_month(analysis_month: str) -> tuple:
    """
    분석월 파싱
    
    Args:
        analysis_month: 분석월 (YYYY-MM 또는 YYYYMM 형식)
    
    Returns:
        tuple: (년도, 월, YYYYMM 형식 문자열)
    """
    # YYYY-MM 형식인 경우
    if '-' in analysis_month:
        year, month = analysis_month.split('-')
        year = int(year)
        month = int(month)
    # YYYYMM 형식인 경우
    elif len(analysis_month) == 6 and analysis_month.isdigit():
        year = int(analysis_month[:4])
        month = int(analysis_month[4:6])
    else:
        raise ValueError(f"분석월 형식이 올바르지 않습니다: {analysis_month} (YYYY-MM 또는 YYYYMM 형식 필요)")
    
    analysis_month_str = f"{year}{month:02d}"
    return (year, month, analysis_month_str)

def get_days_in_month(year: int, month: int) -> int:
    """
    해당 월의 일수 반환
    
    Args:
        year: 연도
        month: 월
    
    Returns:
        int: 월 일수
    """
    _, days = monthrange(year, month)
    return days

def calculate_progress_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    진척율 계산
    - 각 주차 누적매출 / 월말 누적매출 (마지막 주차)
    
    Args:
        df: 누적 주차별 매출 DataFrame (브랜드 컬럼 + 날짜별 컬럼)
    
    Returns:
        pd.DataFrame: 진척율이 추가된 DataFrame
    """
    print("\n[진척율 계산] 시작...")
    
    # 브랜드 컬럼 제외한 날짜 컬럼들
    date_columns = [col for col in df.columns if col != '브랜드']
    
    # 결과를 저장할 DataFrame
    result_df = df[['브랜드']].copy()
    
    # 각 브랜드별로 진척율 계산
    for idx, row in df.iterrows():
        brand = row['브랜드']
        
        # 각 주차의 누적매출
        cumulative_sales = []
        for col in date_columns:
            value = row[col]
            if pd.isna(value) or value == 0:
                cumulative_sales.append(0)
            else:
                cumulative_sales.append(float(value))
        
        # 월말 누적매출 (마지막 주차)
        month_end_sales = cumulative_sales[-1] if cumulative_sales else 0
        
        # 진척율 계산 (백분율)
        progress_rates = []
        for cum_sale in cumulative_sales:
            if month_end_sales > 0:
                progress_rate = (cum_sale / month_end_sales) * 100
            else:
                progress_rate = 0
            progress_rates.append(progress_rate)
        
        # 결과에 추가
        for i, col in enumerate(date_columns):
            result_df.at[idx, col] = progress_rates[i]
    
    print(f"✅ 진척율 계산 완료: {len(result_df)}개 브랜드")
    return result_df

def calculate_progress_days(df_progress_rate: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    진척일자 환산
    - 진척율 × 월 일수
    
    Args:
        df_progress_rate: 진척율 DataFrame
        year: 연도
        month: 월
    
    Returns:
        pd.DataFrame: 진척일자가 추가된 DataFrame
    """
    print("\n[진척일자 환산] 시작...")
    
    days_in_month = get_days_in_month(year, month)
    print(f"   월 일수: {days_in_month}일")
    
    # 결과를 저장할 DataFrame
    result_df = df_progress_rate[['브랜드']].copy()
    
    # 날짜 컬럼들
    date_columns = [col for col in df_progress_rate.columns if col != '브랜드']
    
    # 각 주차별로 진척일자 계산
    for idx, row in df_progress_rate.iterrows():
        for col in date_columns:
            progress_rate = row[col]  # 백분율
            progress_days = (progress_rate / 100) * days_in_month
            result_df.at[idx, col] = round(progress_days, 2)  # 소수점 둘째 자리(0.00)까지 표시
    
    print(f"✅ 진척일자 환산 완료: {len(result_df)}개 브랜드")
    return result_df

def save_to_csv(df: pd.DataFrame, output_path: Path):
    """
    DataFrame을 CSV 파일로 저장
    
    Args:
        df: 저장할 DataFrame
        output_path: 저장할 파일 경로
    """
    try:
        # 디렉토리가 없으면 생성
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # CSV로 저장 (UTF-8 with BOM for Excel compatibility)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ CSV 파일 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        raise

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='전년 동월 누적 주차별 매출 데이터로부터 진척율과 진척일자 계산',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/calculate_progress_days.py 202511
  python scripts/calculate_progress_days.py 2025-11 raw/202511/previous_year/cumulative_sales_202411.csv
  
설명:
  1. 전년 동월 누적 주차별 매출 데이터를 읽어서
  2. 각 주차의 진척율 계산 (누적매출 / 월말누적매출)
  3. 진척일자 환산 (진척율 × 월 일수)
  4. CSV 파일로 저장
        """
    )
    
    parser.add_argument(
        'analysis_month',
        type=str,
        help='분석월 (예: 2025-11 또는 202511)'
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        nargs='?',
        default=None,
        help='입력 CSV 파일 경로 (지정하지 않으면 자동 찾기)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='출력 파일 경로 (지정하지 않으면 자동 생성)'
    )
    
    args = parser.parse_args()
    
    # 분석월 파싱
    try:
        year, month, analysis_month_str = parse_analysis_month(args.analysis_month)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    # 전년 년월 계산
    prev_year = year - 1
    prev_year_month_str = f"{prev_year}{month:02d}"
    
    print("=" * 60)
    print("진척율 및 진척일자 계산")
    print("=" * 60)
    print(f"📅 분석월: {args.analysis_month}")
    print(f"📅 전년 년월: {prev_year}-{month:02d} ({prev_year_month_str})")
    print()
    
    try:
        # 입력 파일 경로 결정
        if args.input_file:
            input_path = Path(args.input_file)
        else:
            # 자동 경로: raw/{분석년월}/previous_year/cumulative_sales_{전년년월}.csv
            input_path = project_root / "raw" / analysis_month_str / "previous_year" / f"cumulative_sales_{prev_year_month_str}.csv"
        
        if not input_path.exists():
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_path}")
        
        print(f"📂 입력 파일: {input_path}")
        
        # CSV 파일 읽기
        print("\n[읽기] CSV 파일 읽는 중...")
        df = pd.read_csv(input_path, encoding='utf-8-sig')
        print(f"✅ 데이터 로드 완료: {len(df)}개 브랜드, {len(df.columns)}개 컬럼")
        
        # 진척율 계산
        df_progress_rate = calculate_progress_rate(df)
        
        # 진척일자 환산
        df_progress_days = calculate_progress_days(df_progress_rate, year, month)
        
        # 출력 경로 결정
        if args.output:
            output_path = Path(args.output)
        else:
            # 자동 경로: raw/{분석년월}/previous_year/progress_days_{분석년월}.csv
            output_path = project_root / "raw" / analysis_month_str / "previous_year" / f"progress_days_{analysis_month_str}.csv"
        
        # CSV 저장
        save_to_csv(df_progress_days, output_path)
        
        # 데이터 요약 정보 출력
        print()
        print("=" * 60)
        print("📊 계산 결과 요약")
        print("=" * 60)
        print(f"총 브랜드 수: {len(df_progress_days):,}개")
        print(f"주차 수: {len(df_progress_days.columns) - 1}개")
        print()
        print("브랜드별 진척일자 (일):")
        for idx, row in df_progress_days.iterrows():
            brand = row['브랜드']
            days_values = [str(row[col]) for col in df_progress_days.columns if col != '브랜드']
            print(f"  {brand}: {', '.join(days_values)}")
        
        print()
        print("=" * 60)
        print("✅ 계산 완료!")
        print("=" * 60)
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ 오류 발생: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

