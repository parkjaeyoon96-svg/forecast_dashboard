"""
당시즌 의류 판매율 분석 Snowflake 쿼리 실행 스크립트

사용법:
    python scripts/query_sales_rate.py
    
출력: JSON 형식으로 CUR, PY, PY_END 데이터 반환
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# Windows 콘솔 인코딩 설정
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


def get_snowflake_connection():
    """Snowflake 데이터베이스 연결 생성"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        return conn
    except Exception as e:
        print(f"[오류] Snowflake 연결 실패: {e}", file=sys.stderr)
        raise


def get_sales_rate_query():
    """판매율 분석 쿼리 생성"""
    
    query = """
WITH PARAM AS (
  SELECT
      DATEADD(DAY, -1, CURRENT_DATE())                                      AS ASOF_DT
    , DATEADD(YEAR, -1, DATEADD(DAY, -1, CURRENT_DATE()))                   AS ASOF_DT_PY
    /* 당년: 25F (FW 시즌이라고 가정) */
    , CONCAT(RIGHT(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 1, 2), 'F')     AS CUR_SESN
    /* 전년: 24F */
    , CONCAT(RIGHT(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 2, 2), 'F')     AS PY_SESN
    /* 전년 마감(24F 마감): (ASOF_DT의 전년도) 2/28  -> 예: 2025-02-28 */
    , DATE_FROM_PARTS(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 1, 2, 28)    AS PY_END_DT
),

BASE AS (
  /* 1) 당년(25F) 어제까지 스냅샷 */
  SELECT
      pa.ASOF_DT                  AS ASOF_DT
    , 'CUR'                       AS PERIOD_GB
    , a.BRD_CD
    , a.SESN                      AS SESN
    , a.PRDT_CD
    , b.PRDT_KIND_NM
    , b.ITEM                      AS ITEM_CD
    , b.ITEM_NM
    , b.PRDT_NM
    , a.AC_ORD_QTY_KOR
    , a.AC_ORD_TAG_AMT_KOR
    , a.AC_STOR_QTY_KOR
    , a.AC_STOR_TAG_AMT_KOR
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)         AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG
    , a.STOCK_QTY
    , a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b
    ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.CUR_SESN
    AND a.BRD_CD <> 'A'
    AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.ASOF_DT BETWEEN a.START_DT AND a.END_DT

  UNION ALL

  /* 2) 전년(24F) 전년-어제까지 스냅샷 */
  SELECT
      pa.ASOF_DT_PY               AS ASOF_DT
    , 'PY'                        AS PERIOD_GB
    , a.BRD_CD
    , a.SESN                      AS SESN
    , a.PRDT_CD
    , b.PRDT_KIND_NM
    , b.ITEM                      AS ITEM_CD
    , b.ITEM_NM
    , b.PRDT_NM
    , a.AC_ORD_QTY_KOR
    , a.AC_ORD_TAG_AMT_KOR
    , a.AC_STOR_QTY_KOR
    , a.AC_STOR_TAG_AMT_KOR
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)         AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG
    , a.STOCK_QTY
    , a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b
    ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.PY_SESN
    AND a.BRD_CD <> 'A'
    AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.ASOF_DT_PY BETWEEN a.START_DT AND a.END_DT

  UNION ALL

  /* 3) 전년마감(24F) 2/28 스냅샷 */
  SELECT
      pa.PY_END_DT                AS ASOF_DT
    , 'PY_END'                    AS PERIOD_GB
    , a.BRD_CD
    , a.SESN                      AS SESN
    , a.PRDT_CD
    , b.PRDT_KIND_NM
    , b.ITEM                      AS ITEM_CD
    , b.ITEM_NM
    , b.PRDT_NM
    , a.AC_ORD_QTY_KOR
    , a.AC_ORD_TAG_AMT_KOR
    , a.AC_STOR_QTY_KOR
    , a.AC_STOR_TAG_AMT_KOR
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)         AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG
    , a.STOCK_QTY
    , a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b
    ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.PY_SESN
    AND a.BRD_CD <> 'A'
    AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.PY_END_DT BETWEEN a.START_DT AND a.END_DT
)

SELECT
    ASOF_DT
  , PERIOD_GB
  , BRD_CD
  , MAX(SESN)         AS SESN
  , PRDT_CD
  , MAX(PRDT_KIND_NM) AS PRDT_KIND_NM
  , MAX(ITEM_CD)      AS ITEM_CD
  , MAX(ITEM_NM)      AS ITEM_NM
  , MAX(PRDT_NM)      AS PRDT_NM
  , SUM(AC_ORD_QTY_KOR)      AS AC_ORD_QTY_KOR
  , SUM(AC_ORD_TAG_AMT_KOR)  AS AC_ORD_TAG_AMT_KOR
  , SUM(AC_STOR_QTY_KOR)     AS AC_STOR_QTY_KOR
  , SUM(AC_STOR_TAG_AMT_KOR) AS AC_STOR_TAG_AMT_KOR
  , SUM(SALE_QTY)            AS SALE_QTY
  , SUM(SALE_TAG)            AS SALE_TAG
  , SUM(STOCK_QTY)           AS STOCK_QTY
  , SUM(STOCK_TAG_AMT)       AS STOCK_TAG_AMT
FROM BASE
GROUP BY
    ASOF_DT, PERIOD_GB, BRD_CD, PRDT_CD
/* 발주/입고/판매/재고 TAG 전부 0이면 제외 */
HAVING
    COALESCE(SUM(AC_ORD_TAG_AMT_KOR), 0)
  + COALESCE(SUM(AC_STOR_TAG_AMT_KOR), 0)
  + COALESCE(SUM(SALE_TAG), 0)
  + COALESCE(SUM(STOCK_TAG_AMT), 0) <> 0
ORDER BY
    BRD_CD, PRDT_CD, PERIOD_GB, ASOF_DT
"""
    
    return query


def execute_query(conn, query: str):
    """쿼리 실행 및 DataFrame 반환"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()
        return df
    except Exception as e:
        print(f"[오류] 쿼리 실행 실패: {e}", file=sys.stderr)
        raise


def main():
    """메인 실행 함수"""
    try:
        # Snowflake 연결
        conn = get_snowflake_connection()
        
        # 쿼리 실행
        query = get_sales_rate_query()
        df = execute_query(conn, query)
        
        conn.close()
        
        # 데이터를 기간별로 분리
        cur_data = df[df['PERIOD_GB'] == 'CUR'].to_dict('records')
        py_data = df[df['PERIOD_GB'] == 'PY'].to_dict('records')
        py_end_data = df[df['PERIOD_GB'] == 'PY_END'].to_dict('records')
        
        # 기간 정보 추출 (각 기간의 ASOF_DT)
        cur_date = df[df['PERIOD_GB'] == 'CUR']['ASOF_DT'].iloc[0] if len(cur_data) > 0 else None
        py_date = df[df['PERIOD_GB'] == 'PY']['ASOF_DT'].iloc[0] if len(py_data) > 0 else None
        py_end_date = df[df['PERIOD_GB'] == 'PY_END']['ASOF_DT'].iloc[0] if len(py_end_data) > 0 else None
        
        # JSON 형식으로 변환
        result = {
            'success': True,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'periodInfo': {
                'curDate': str(cur_date) if cur_date else '',
                'pyDate': str(py_date) if py_date else '',
                'pyEndDate': str(py_end_date) if py_end_date else ''
            },
            'data': {
                'CUR': cur_data,
                'PY': py_data,
                'PY_END': py_end_data
            },
            'rowCount': {
                'CUR': len(cur_data),
                'PY': len(py_data),
                'PY_END': len(py_end_data)
            }
        }
        
        # JSON 출력
        print(json.dumps(result, ensure_ascii=False, default=str))
        
    except Exception as e:
        # 에러 발생 시 에러 정보를 JSON으로 반환
        error_result = {
            'success': False,
            'error': str(e),
            'date': datetime.now().strftime('%Y-%m-%d')
        }
        print(json.dumps(error_result, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

