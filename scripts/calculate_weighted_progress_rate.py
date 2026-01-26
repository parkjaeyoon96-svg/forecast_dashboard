"""
분석월의 요일계수와 명절계수를 이용한 가중치 진척율 계산

사용법:
    python scripts/calculate_weighted_progress_rate.py [분석월]
    
예시:
    python scripts/calculate_weighted_progress_rate.py 202601
    python scripts/calculate_weighted_progress_rate.py 2026-01
    
설명:
    1. Master/명절계수.csv와 Master/요일계수.csv 읽기
    2. 분석월의 1일부터 말일까지 각 일자별로:
       - 명절계수 우선 적용 (적용일자 매칭)
       - 명절이 아닌 날짜는 요일계수 적용
    3. 계산:
       - 월말계수 = 전체 일자 계수의 합
       - 진척계수 = 해당 일자까지의 누적 계수
       - 진척율 = 진척계수 / 월말계수
    4. CSV 파일로 저장
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
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

def load_holiday_coefficients(master_dir: Path) -> pd.DataFrame:
    """
    명절계수 마스터 파일 읽기
    
    Args:
        master_dir: Master 디렉토리 경로
    
    Returns:
        pd.DataFrame: 명절계수 데이터
    """
    holiday_file = master_dir / "명절계수.csv"
    
    if not holiday_file.exists():
        raise FileNotFoundError(f"명절계수 파일을 찾을 수 없습니다: {holiday_file}")
    
    df = pd.read_csv(holiday_file, encoding='utf-8-sig')
    
    # 필수 컬럼 확인
    required_cols = ['구분', 'D_index', '명절계수', '적용일자']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"명절계수 파일에 필수 컬럼이 없습니다: {missing_cols}")
    
    # 적용일자를 datetime으로 변환
    df['적용일자'] = pd.to_datetime(df['적용일자'])
    
    print(f"✅ 명절계수 파일 로드 완료: {len(df)}개 데이터")
    return df

def load_weekday_coefficients(master_dir: Path) -> dict:
    """
    요일계수 마스터 파일 읽기
    
    Args:
        master_dir: Master 디렉토리 경로
    
    Returns:
        dict: 요일별 계수 딕셔너리 {'Mon': 0.871143, ...}
    """
    weekday_file = master_dir / "요일계수.csv"
    
    if not weekday_file.exists():
        raise FileNotFoundError(f"요일계수 파일을 찾을 수 없습니다: {weekday_file}")
    
    df = pd.read_csv(weekday_file, encoding='utf-8-sig')
    
    # 필수 컬럼 확인
    if '요일' not in df.columns or '계수' not in df.columns:
        raise ValueError("요일계수 파일에 '요일', '계수' 컬럼이 필요합니다")
    
    # 딕셔너리로 변환
    weekday_dict = {}
    for _, row in df.iterrows():
        weekday = row['요일']
        coef = row['계수']
        weekday_dict[weekday] = coef
    
    print(f"✅ 요일계수 파일 로드 완료: {len(weekday_dict)}개 요일")
    return weekday_dict

def get_daily_coefficient(date: datetime, holiday_df: pd.DataFrame, weekday_dict: dict) -> tuple:
    """
    특정 날짜의 계수 가져오기 (명절계수 우선, 없으면 요일계수)
    
    Args:
        date: 날짜
        holiday_df: 명절계수 DataFrame
        weekday_dict: 요일계수 딕셔너리
    
    Returns:
        tuple: (계수, 구분) - 구분은 '명절' 또는 '요일'
    """
    # 1. 명절계수 확인 (적용일자 매칭)
    holiday_row = holiday_df[holiday_df['적용일자'] == date]
    
    if not holiday_row.empty:
        coefficient = holiday_row['명절계수'].values[0]
        return (coefficient, '명절')
    
    # 2. 요일계수 적용
    weekday = date.strftime('%a')  # Mon, Tue, Wed, Thu, Fri, Sat, Sun
    
    if weekday not in weekday_dict:
        raise ValueError(f"요일계수를 찾을 수 없습니다: {weekday} ({date.strftime('%Y-%m-%d')})")
    
    coefficient = weekday_dict[weekday]
    return (coefficient, '요일')

def calculate_weighted_progress_rate(year: int, month: int, holiday_df: pd.DataFrame, weekday_dict: dict) -> pd.DataFrame:
    """
    분석월의 일자별 가중치 진척율 계산
    
    Args:
        year: 연도
        month: 월
        holiday_df: 명절계수 DataFrame
        weekday_dict: 요일계수 딕셔너리
    
    Returns:
        pd.DataFrame: 일자별 진척율 데이터
    """
    print(f"\n[진척율 계산] {year}년 {month}월")
    
    # 월의 일수
    _, days_in_month = monthrange(year, month)
    print(f"   월 일수: {days_in_month}일")
    
    # 결과 저장용 리스트
    results = []
    
    # 각 날짜별 계수 계산
    for day in range(1, days_in_month + 1):
        date = datetime(year, month, day)
        weekday = date.strftime('%a')
        
        # 계수 가져오기
        coefficient, coef_type = get_daily_coefficient(date, holiday_df, weekday_dict)
        
        results.append({
            '월': month,
            '일': day,
            '요일': weekday,
            '계수구분': coef_type,
            '계수': coefficient
        })
    
    # DataFrame 생성
    df = pd.DataFrame(results)
    
    # 월말계수 계산 (전체 일자 계수의 합)
    total_coefficient = df['계수'].sum()
    df['월말계수'] = total_coefficient
    
    # 진척계수 계산 (누적 합)
    df['진척계수'] = df['계수'].cumsum()
    
    # 진척율 계산 (진척계수 / 월말계수)
    df['진척율'] = df['진척계수'] / df['월말계수']
    
    print(f"✅ 진척율 계산 완료")
    print(f"   월말계수 합계: {total_coefficient:.6f}")
    print(f"   명절 적용 일수: {len(df[df['계수구분'] == '명절'])}일")
    print(f"   요일 적용 일수: {len(df[df['계수구분'] == '요일'])}일")
    
    return df

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
        
        # 기존 파일이 있으면 삭제
        if output_path.exists():
            try:
                output_path.unlink()
                print(f"[INFO] 기존 파일 삭제: {output_path.name}")
            except PermissionError:
                print(f"[WARNING] 기존 파일을 삭제할 수 없습니다 (다른 프로그램에서 열려있을 수 있음): {output_path.name}")
                raise
        
        # CSV로 저장 (UTF-8 with BOM for Excel compatibility)
        df.to_csv(output_path, index=False, encoding='utf-8-sig', float_format='%.6f')
        print(f"\n✅ CSV 파일 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        raise

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='분석월의 요일계수와 명절계수를 이용한 가중치 진척율 계산',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/calculate_weighted_progress_rate.py 202601
  python scripts/calculate_weighted_progress_rate.py 2026-01
  
설명:
  1. Master/명절계수.csv와 Master/요일계수.csv 읽기
  2. 분석월의 1일부터 말일까지 각 일자별로 계수 적용
  3. 진척율 계산 및 CSV 저장
        """
    )
    
    parser.add_argument(
        'analysis_month',
        type=str,
        help='분석월 (예: 2026-01 또는 202601)'
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
    
    print("=" * 60)
    print("가중치 진척율 계산")
    print("=" * 60)
    print(f"📅 분석월: {year}년 {month}월 ({analysis_month_str})")
    print()
    
    try:
        # Master 디렉토리
        master_dir = project_root / "Master"
        
        # 명절계수 로드
        print("[1단계] 명절계수 파일 읽기...")
        holiday_df = load_holiday_coefficients(master_dir)
        print()
        
        # 요일계수 로드
        print("[2단계] 요일계수 파일 읽기...")
        weekday_dict = load_weekday_coefficients(master_dir)
        print()
        
        # 진척율 계산
        print("[3단계] 진척율 계산...")
        result_df = calculate_weighted_progress_rate(year, month, holiday_df, weekday_dict)
        print()
        
        # 출력 경로 결정
        if args.output:
            output_path = Path(args.output)
        else:
            # 자동 경로: raw/{분석년월}/progress_rate/weighted_progress_rate_{분석년월}.csv
            output_path = project_root / "raw" / analysis_month_str / "progress_rate" / f"weighted_progress_rate_{analysis_month_str}.csv"
        
        # CSV 저장
        print("[4단계] CSV 저장...")
        save_to_csv(result_df, output_path)
        
        # 데이터 요약 정보 출력
        print()
        print("=" * 60)
        print("📊 계산 결과 요약")
        print("=" * 60)
        print(f"총 일수: {len(result_df)}일")
        print(f"월말계수: {result_df['월말계수'].iloc[0]:.6f}")
        print()
        print("첫 5일 진척율:")
        for idx, row in result_df.head(5).iterrows():
            print(f"  {int(row['월'])}/{int(row['일']):2d} ({row['요일']}): {row['진척율']*100:5.2f}% [{row['계수구분']}]")
        print("  ...")
        print("마지막 5일 진척율:")
        for idx, row in result_df.tail(5).iterrows():
            print(f"  {int(row['월'])}/{int(row['일']):2d} ({row['요일']}): {row['진척율']*100:5.2f}% [{row['계수구분']}]")
        
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
















