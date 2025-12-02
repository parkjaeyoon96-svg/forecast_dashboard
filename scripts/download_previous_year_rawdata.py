"""
전년 로데이터를 Snowflake에서 조회하여 CSV로 다운로드하는 스크립트

사용법:
    python scripts/download_previous_year_rawdata.py [분석월] [브랜드코드]
    
예시:
    python scripts/download_previous_year_rawdata.py 2025-11 X
    python scripts/download_previous_year_rawdata.py 202511 ST
    python scripts/download_previous_year_rawdata.py 2025-11
    
설명:
    분석월을 입력하면 자동으로 전년 년월을 계산하여 쿼리에 사용합니다.
    예: 분석월이 2025-11이면 전년 PST_YYYYMM은 202411이 됩니다.
    
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
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# 출력 인코딩 설정 (Windows 환경에서 이모지 출력 오류 방지)
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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

def calculate_previous_year_month(analysis_month: str) -> str:
    """
    분석월에서 전년 년월 계산
    예: 2025-11 -> 202411, 202511 -> 202411
    
    Args:
        analysis_month: 분석월 (YYYY-MM 또는 YYYYMM 형식)
    
    Returns:
        str: 전년 년월 (YYYYMM 형식)
    """
    # YYYY-MM 형식인 경우
    if '-' in analysis_month:
        year, month = analysis_month.split('-')
        prev_year = int(year) - 1
        return f"{prev_year}{month}"
    # YYYYMM 형식인 경우
    elif len(analysis_month) == 6 and analysis_month.isdigit():
        year = int(analysis_month[:4])
        month = analysis_month[4:6]
        prev_year = year - 1
        return f"{prev_year}{month}"
    else:
        raise ValueError(f"분석월 형식이 올바르지 않습니다: {analysis_month} (YYYY-MM 또는 YYYYMM 형식 필요)")

def get_previous_year_query(previous_year_month: str, brand_code: str = None):
    """
    전년 로데이터 조회 쿼리 생성
    
    Args:
        previous_year_month: 조회할 전년 년월 (예: '202411')
        brand_code: 브랜드 코드 (예: 'X', 'ST', None이면 모든 브랜드)
    
    Returns:
        str: SQL 쿼리
    """
    query = """
SELECT
    /* 기본 식별 필드 */
    d.PST_YYYYMM              AS "전기년월",
    d.BRD_CD                  AS "브랜드코드",
    d.BRD_NM                  AS "브랜드명",
    d.CHNL_CD                 AS "채널코드",
    d.SHOP_CD                 AS "매장코드 (SAP기준)",
    d.SHOP_NM                 AS "매장명",
    /* 시즌 */
    CASE 
        WHEN d.BRD_CD = 'ST' THEN SUBSTR(d.PRDT_CD, 3, 2)
        ELSE SUBSTR(d.PRDT_CD, 2, 3)
    END                       AS "시즌",
    /* 제품 정보 */
    d.PRDT_CD                 AS "제품코드",
    d.PRDT_NM                 AS "제품명",
    /* 아이템코드 */
    CASE 
        WHEN d.BRD_CD = 'ST' THEN SUBSTR(d.PRDT_CD, 8, 2)
        ELSE SUBSTR(d.PRDT_CD, 7, 2)
    END                       AS "아이템코드",
    /* 제품계층 */
    m.PRDT_HRRC1_NM           AS "제품계층1(대분류)",
    m.PRDT_HRRC2_NM           AS "제품계층2(중분류)",
    m.PRDT_HRRC3_NM           AS "제품계층3(소분류)",
    /* 매출 / 비용 필드 */
    d.TAG_SALE_AMT            AS "TAG매출액",
    d.ACT_SALE_AMT            AS "실매출액",
    d.VAT_EXC_ACT_SALE_AMT    AS "부가세제외 실판매액",
    d.DSTRB_CMS               AS "유통 수수료",
    d.COGS                    AS "매출원가 ( 환입후매출원가+평가감(추가) )",
    /* 매출총이익 = 부가세제외 실판매액 - 유통수수료 - 매출원가 */
    ( d.VAT_EXC_ACT_SALE_AMT 
      - d.DSTRB_CMS
      - d.COGS
    )                         AS "매출총이익",
    d.RYT                     AS "지급수수료_로열티",
    d.LGT_CST                 AS "지급수수료_물류용역비", --물류용역비 + 물류운송
    d.CARD_CMS                AS "지급수수료_카드수수료",
    d.SHOP_RNT                AS "지급임차료_매장(고정)", --매장(고정)+매장(변동)+관리비
    d.SHOP_DEPRC_CST          AS "감가상각비_임차시설물",
    d.SM_CMS                  AS "지급수수료_중간관리수수료",
    d.DF_SALE_STFF_CMS        AS "지급수수료_판매사원도급비(면세)",
    d.DMGMT_SALE_STFF_CMS     AS "지급수수료_판매사원도급비(직영)",
    d.ALNC_ONLN_CMS           AS "지급수수료_온라인위탁판매수수료",
    d.STRG_CST                AS "지급수수료_이천보관료",
    /* 직접비 합계 */
    ( d.RYT 
      + d.LGT_CST
      + d.CARD_CMS
      + d.SHOP_RNT
      + d.SHOP_DEPRC_CST
      + d.SM_CMS
      + d.DF_SALE_STFF_CMS
      + d.DMGMT_SALE_STFF_CMS
      + d.ALNC_ONLN_CMS
      + d.STRG_CST 
    ) AS "직접비 합계",
    /* 직접이익 = 매출총이익 - 직접비합계 */
    (
        ( d.VAT_EXC_ACT_SALE_AMT 
          - d.DSTRB_CMS
          - d.COGS
        )
        -
        ( d.RYT 
          + d.LGT_CST
          + d.CARD_CMS
          + d.SHOP_RNT
          + d.SHOP_DEPRC_CST
          + d.SM_CMS
          + d.DF_SALE_STFF_CMS
          + d.DMGMT_SALE_STFF_CMS
          + d.ALNC_ONLN_CMS
          + d.STRG_CST
        )
    ) AS "직접이익"
FROM FNF.SAP_FNF.DM_PL_SHOP_PRDT_M d
LEFT JOIN FNF.SAP_FNF.MST_PRDT m
       ON d.PRDT_CD = m.PRDT_CD
      AND d.BRD_CD  = m.BRD_CD
WHERE d.PST_YYYYMM = '{previous_year_month}'
  AND d.CHNL_CD <> '9'
"""
    
    return query.format(previous_year_month=previous_year_month)

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
        description='전년 로데이터를 Snowflake에서 조회하여 CSV로 다운로드',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/download_previous_year_rawdata.py 2025-11 X
  python scripts/download_previous_year_rawdata.py 202511 ST
  python scripts/download_previous_year_rawdata.py 2025-11 --output raw/previous/202411/rawdata_X.csv
  
설명:
  분석월을 입력하면 자동으로 전년 년월을 계산하여 쿼리에 사용합니다.
  예: 분석월이 2025-11이면 전년 PST_YYYYMM은 202411이 됩니다.
        """
    )
    
    parser.add_argument(
        'analysis_month',
        type=str,
        help='분석월 (예: 2025-11 또는 202511)'
    )
    
    parser.add_argument(
        'brand_code',
        type=str,
        nargs='?',
        default=None,
        help='브랜드 코드 (예: X, ST, V, W, I, M). 지정하지 않으면 모든 브랜드'
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
        previous_year_month = calculate_previous_year_month(args.analysis_month)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    # 분석월에서 년월 추출 (YYYYMM 형식)
    if '-' in args.analysis_month:
        analysis_year_month = args.analysis_month.replace('-', '')
    else:
        analysis_year_month = args.analysis_month
    
    print("=" * 60)
    print("전년 로데이터 다운로드 시작")
    print("=" * 60)
    print(f"📅 분석월: {args.analysis_month}")
    print(f"📅 전년 년월 (PST_YYYYMM): {previous_year_month}")
    if args.brand_code:
        print(f"🏷️  브랜드: {args.brand_code}")
    else:
        print(f"🏷️  브랜드: 전체")
    print()
    
    conn = None
    try:
        # Snowflake 연결
        conn = get_snowflake_connection()
        
        # 쿼리 생성
        query = get_previous_year_query(previous_year_month, args.brand_code)
        
        # 쿼리 실행
        df = execute_query_to_dataframe(conn, query)
        
        # 출력 경로 결정
        if args.output:
            output_path = Path(args.output)
        else:
            # 자동 경로 생성: raw/{분석년월}/previous_year/rawdata_{분석년월}_{브랜드코드}.csv
            brand_suffix = f"_{args.brand_code}" if args.brand_code else "_ALL"
            output_path = project_root / "raw" / analysis_year_month / "previous_year" / f"rawdata_{analysis_year_month}{brand_suffix}.csv"
        
        # CSV 저장
        save_to_csv(df, output_path)
        
        # 데이터 요약 정보 출력
        print()
        print("=" * 60)
        print("📊 데이터 요약")
        print("=" * 60)
        print(f"총 행 수: {len(df):,}건")
        print(f"총 컬럼 수: {len(df.columns)}개")
        print()
        print("컬럼 목록:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
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

