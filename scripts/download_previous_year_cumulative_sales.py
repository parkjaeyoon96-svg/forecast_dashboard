"""
전년 동월의 브랜드별 누적 주차별 매출을 Snowflake에서 조회하여 CSV로 다운로드하는 스크립트

사용법:
    python scripts/download_previous_year_cumulative_sales.py [분석월]
    
예시:
    python scripts/download_previous_year_cumulative_sales.py 202511
    python scripts/download_previous_year_cumulative_sales.py 2025-11
    
설명:
    분석월을 입력하면 자동으로 전년 년월을 계산하여 쿼리에 사용합니다.
    예: 분석월이 2025-11이면 전년 2024-11의 주차별 누적매출을 조회합니다.
    
환경 변수:
    SNOWFLAKE_ACCOUNT: Snowflake 계정명
    SNOWFLAKE_USERNAME: 사용자명
    SNOWFLAKE_PASSWORD: 비밀번호
    SNOWFLAKE_WAREHOUSE: 웨어하우스명
    SNOWFLAKE_DATABASE: 데이터베이스명
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    print("⚠️ .env 파일을 찾을 수 없습니다. 환경 변수에서 직접 읽습니다.")

def get_snowflake_connection():
    """
    Snowflake 데이터베이스 연결 생성
    
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake 연결 객체
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("✅ Snowflake 연결 성공!")
        return conn
    except Exception as e:
        print(f"❌ Snowflake 연결 실패: {e}")
        raise

def calculate_previous_year_month(analysis_month: str) -> tuple:
    """
    분석월에서 전년 년월 계산 및 날짜 범위 계산
    
    Args:
        analysis_month: 분석월 (YYYY-MM 또는 YYYYMM 형식)
    
    Returns:
        tuple: (전년년도, 전년월, 전년월_YYYYMM, 분석년도, 분석월, 분석월_YYYYMM)
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
    
    prev_year = year - 1
    prev_year_month_str = f"{prev_year}{month:02d}"
    analysis_month_str = f"{year}{month:02d}"
    
    return (prev_year, month, prev_year_month_str, year, month, analysis_month_str)

def get_week_start_dates(year: int, month: int) -> list:
    """
    해당 월의 주차 시작일(월요일) 목록 계산
    
    Args:
        year: 연도
        month: 월
    
    Returns:
        list: 주차 시작일 목록 (datetime 객체)
    """
    # 월의 첫 날
    first_day = datetime(year, month, 1)
    
    # 첫 번째 월요일 찾기
    # weekday(): 0=월요일, 6=일요일
    days_until_monday = (7 - first_day.weekday()) % 7
    if days_until_monday == 0:
        # 첫 날이 월요일이면 그대로 사용
        first_monday = first_day
    else:
        # 첫 날이 월요일이 아니면 다음 월요일 찾기
        first_monday = first_day + timedelta(days=days_until_monday)
    
    # 첫 번째 월요일이 다음 달이면, 이전 주 월요일 사용
    if first_monday.month != month:
        first_monday = first_monday - timedelta(days=7)
    
    # 월의 마지막 날
    _, last_day = monthrange(year, month)
    last_date = datetime(year, month, last_day)
    
    # 주차 시작일 목록 생성
    week_starts = []
    current_monday = first_monday
    
    while current_monday <= last_date:
        # 해당 월 내의 월요일만 포함
        if current_monday.month == month:
            week_starts.append(current_monday)
        current_monday += timedelta(days=7)
    
    return week_starts

def get_cumulative_sales_query(prev_year: int, prev_month: int) -> str:
    """
    전년 동월의 브랜드별 누적 주차별 매출 조회 쿼리 생성
    
    Args:
        prev_year: 전년도
        prev_month: 전년월
    
    Returns:
        str: SQL 쿼리
    """
    # 월의 첫 날과 마지막 날
    first_day = datetime(prev_year, prev_month, 1)
    _, last_day = monthrange(prev_year, prev_month)
    last_date = datetime(prev_year, prev_month, last_day)
    
    # 주차 시작일 목록
    week_starts = get_week_start_dates(prev_year, prev_month)
    
    # ★ 수정: 마지막 주차는 해당 월의 말일까지만 가져오기 ★
    # WHERE 조건 끝 날짜: 해당 월의 마지막 날 + 1일 (쿼리에서 < 연산자 사용)
    if prev_month == 12:
        query_end_date = datetime(prev_year + 1, 1, 1)
    else:
        query_end_date = datetime(prev_year, prev_month + 1, 1)
    
    # 주차 시작일 문자열 리스트 생성 (쿼리용)
    week_start_strs = [ws.strftime('%Y-%m-%d') for ws in week_starts]
    week_start_case = ',\n        '.join([
        f"SUM(CASE WHEN week_start_dt = '{ws}' THEN cum_sale ELSE 0 END) AS \"{ws}\""
        for ws in week_start_strs
    ])
    
    query = f"""
WITH base AS (
    SELECT
        CASE
            WHEN BRD_CD = 'M' AND CHNL_CD = '2'  THEN 'M(면세)'
            WHEN BRD_CD = 'M' AND CHNL_CD <> '2' THEN 'M(면세제외)'
            ELSE BRD_CD
        END AS brand_grp,
        DATE_TRUNC('WEEK', PST_DT)::DATE AS week_start_dt,
        SUM(ACT_SALE_AMT) AS sale_amt
    FROM SAP_FNF.DW_COPA_D
    WHERE PST_DT >= '{first_day.strftime('%Y-%m-%d')}'::DATE          
      AND PST_DT <  '{query_end_date.strftime('%Y-%m-%d')}'::DATE          
      AND BRD_CD <> 'A'
      AND CHNL_CD <> '9'
    GROUP BY brand_grp, week_start_dt
),
weekly AS (
    SELECT
        brand_grp,
        week_start_dt,
        SUM(sale_amt) AS week_sale
    FROM base
    GROUP BY brand_grp, week_start_dt
),
cum AS (
    SELECT
        brand_grp,
        week_start_dt,
        SUM(week_sale) OVER (
            PARTITION BY brand_grp
            ORDER BY week_start_dt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_sale
    FROM weekly
)
SELECT
    brand_grp AS "브랜드",
    {week_start_case}
FROM cum
WHERE week_start_dt IN ({','.join([f"'{ws}'" for ws in week_start_strs])})
GROUP BY brand_grp
ORDER BY brand_grp
"""
    
    return query

def execute_query_to_dataframe(conn, query: str):
    """
    Snowflake 쿼리 실행 및 결과를 pandas DataFrame으로 반환
    
    Args:
        conn: Snowflake 연결 객체
        query: 실행할 SQL 쿼리
        
    Returns:
        pd.DataFrame: 쿼리 결과
    """
    try:
        print("📊 쿼리 실행 중...")
        cursor = conn.cursor()
        cursor.execute(query)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 데이터 가져오기
        print("📥 데이터 가져오는 중...")
        data = cursor.fetchall()
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        print(f"✅ {len(df):,}건의 데이터를 조회했습니다.")
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        raise

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
        print(f"✅ CSV 파일 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        raise

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='전년 동월의 브랜드별 누적 주차별 매출을 Snowflake에서 조회하여 CSV로 다운로드',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/download_previous_year_cumulative_sales.py 202511
  python scripts/download_previous_year_cumulative_sales.py 2025-11
  
설명:
  분석월을 입력하면 자동으로 전년 년월을 계산하여 쿼리에 사용합니다.
  예: 분석월이 2025-11이면 전년 2024-11의 주차별 누적매출을 조회합니다.
        """
    )
    
    parser.add_argument(
        'analysis_month',
        type=str,
        help='분석월 (예: 2025-11 또는 202511)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='출력 파일 경로 (지정하지 않으면 자동 생성)'
    )
    
    args = parser.parse_args()
    
    # 분석월에서 전년 년월 계산
    try:
        prev_year, prev_month, prev_year_month_str, year, month, analysis_month_str = calculate_previous_year_month(args.analysis_month)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    print("=" * 60)
    print("전년 동월 누적 주차별 매출 다운로드 시작")
    print("=" * 60)
    print(f"📅 분석월: {args.analysis_month}")
    print(f"📅 전년 년월: {prev_year}-{prev_month:02d} ({prev_year_month_str})")
    print()
    
    conn = None
    try:
        # Snowflake 연결
        conn = get_snowflake_connection()
        
        # 쿼리 생성
        query = get_cumulative_sales_query(prev_year, prev_month)
        
        # 쿼리 실행
        df = execute_query_to_dataframe(conn, query)
        
        # 출력 경로 결정
        if args.output:
            output_path = Path(args.output)
        else:
            # 자동 경로 생성: raw/{분석년월}/previous_year/cumulative_sales_{전년년월}.csv
            output_path = project_root / "raw" / analysis_month_str / "previous_year" / f"cumulative_sales_{prev_year_month_str}.csv"
        
        # CSV 저장
        save_to_csv(df, output_path)
        
        # 데이터 요약 정보 출력
        print()
        print("=" * 60)
        print("📊 데이터 요약")
        print("=" * 60)
        print(f"총 브랜드 수: {len(df):,}개")
        print(f"총 컬럼 수: {len(df.columns)}개")
        print()
        print("브랜드 목록:")
        for i, brand in enumerate(df['브랜드'], 1):
            print(f"  {i:2d}. {brand}")
        
        print()
        print("=" * 60)
        print("✅ 다운로드 완료!")
        print("=" * 60)
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ 오류 발생: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\n🔌 Snowflake 연결 종료")

if __name__ == "__main__":
    main()

