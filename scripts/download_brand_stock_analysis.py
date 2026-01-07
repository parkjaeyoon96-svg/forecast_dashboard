"""
브랜드별 현황 - 당시즌의류/ACC 재고주수 분석 다운로드 스크립트

사용법:
    python scripts/download_brand_stock_analysis.py --update-date 2025-11-24
    python scripts/download_brand_stock_analysis.py  # 기본값: 오늘 날짜
    python scripts/download_brand_stock_analysis.py --no-js  # JS 파일 생성 안 함
    
출력:
    - raw/202511/ETC/당시즌의류_브랜드별현황_YYYYMMDD.csv
    - raw/202511/ETC/ACC_재고주수분석_YYYYMMDD.csv
    - public/brand_stock_analysis_YYYYMMDD.js (대시보드용)
"""

import os
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal
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
else:
    print("⚠️ .env 파일을 찾을 수 없습니다. 환경 변수에서 직접 읽습니다.")


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
        print("[성공] Snowflake 연결 성공!")
        return conn
    except Exception as e:
        print(f"[오류] Snowflake 연결 실패: {e}")
        raise


def execute_query(conn, query: str) -> pd.DataFrame:
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
        print(f"[오류] 쿼리 실행 실패: {e}")
        raise


def calculate_dates(update_date: datetime) -> dict:
    """
    업데이트 일자 기준으로 매출 조회 날짜 계산
    
    Args:
        update_date: 업데이트 일자 (예: 2025-11-24)
        
    Returns:
        dict: 계산된 날짜 정보
            - cy_week_end: 당년 주간 종료일 (업데이트 전날)
            - cy_week_start: 당년 주간 시작일 (종료일 - 6일)
            - py_week_end: 전년 동주차 종료일 (364일 전)
            - py_week_start: 전년 동주차 시작일
            - cy_season: 당년 시즌 코드
            - py_season: 전년 시즌 코드
            - py_season_end: 전년 시즌 마감일
    """
    # 당년 주간: 업데이트일 전 주 (월~일)
    cy_week_end = update_date - timedelta(days=1)  # 업데이트 전날 (일요일)
    cy_week_start = cy_week_end - timedelta(days=6)  # 월요일
    
    # 전년 동주차: 364일 전 (52주 = 364일, 같은 요일)
    py_week_end = cy_week_end - timedelta(days=364)
    py_week_start = cy_week_start - timedelta(days=364)
    
    # 시즌 코드 결정 (F: 가을/겨울, S: 봄/여름)
    # 8월~2월: F시즌, 3월~7월: S시즌
    cy_year = update_date.year
    cy_month = update_date.month
    
    if cy_month >= 8 or cy_month <= 2:
        # F시즌 (가을/겨울)
        if cy_month <= 2:
            cy_season = f"{(cy_year - 1) % 100:02d}F"  # 1,2월은 전년도 F시즌
            py_season = f"{(cy_year - 2) % 100:02d}F"
        else:
            cy_season = f"{cy_year % 100:02d}F"
            py_season = f"{(cy_year - 1) % 100:02d}F"
    else:
        # S시즌 (봄/여름)
        cy_season = f"{cy_year % 100:02d}S"
        py_season = f"{(cy_year - 1) % 100:02d}S"
    
    # 전년 시즌 마감일 (F시즌: 2월말, S시즌: 7월말)
    if 'F' in cy_season:
        py_season_end_year = int('20' + py_season[:2]) + 1
        py_season_end = datetime(py_season_end_year, 2, 28)
    else:
        py_season_end_year = int('20' + py_season[:2])
        py_season_end = datetime(py_season_end_year, 7, 31)
    
    # 4주 평균 계산을 위한 날짜
    cy_4w_start = cy_week_end - timedelta(days=21)  # 4주 시작
    py_4w_start = py_week_end - timedelta(days=21)
    
    return {
        'update_date': update_date,
        'cy_week_start': cy_week_start,
        'cy_week_end': cy_week_end,
        'py_week_start': py_week_start,
        'py_week_end': py_week_end,
        'cy_season': cy_season,
        'py_season': py_season,
        'py_season_end': py_season_end,
        'cy_4w_start': cy_4w_start,
        'py_4w_start': py_4w_start
    }


def build_clothing_query(dates: dict) -> str:
    """
    당시즌의류 분석 쿼리 생성 (ACC 제외)
    """
    cy_week_end = dates['cy_week_end'].strftime('%Y-%m-%d')
    py_week_end = dates['py_week_end'].strftime('%Y-%m-%d')
    py_season_end = dates['py_season_end'].strftime('%Y-%m-%d')
    cy_season = dates['cy_season']
    py_season = dates['py_season']
    
    query = f"""
WITH RDS AS (
  SELECT
      U.BRD_CD,
      B.ITEM AS MASTER,
      B.PARENT_PRDT_KIND_NM,
      B.PRDT_KIND_NM,
      B.ITEM,
      MAX(B.ITEM_NM) AS ITEM_NM,
      SUM(W_QTY_PY)                   AS W_QTY_PY,
      SUM(W_TAG_AMG_PY)               AS W_TAG_AMG_PY,
      SUM(W_SALE_AMT_PY)              AS W_SALE_AMT_PY,
      SUM(AC_ORD_QTY_PY)              AS AC_ORD_QTY_PY,
      SUM(AC_ORD_TAG_AMT_PY)          AS AC_ORD_TAG_AMT_PY,
      SUM(AC_STOR_QTY_KOR_PY)         AS AC_STOR_QTY_KOR_PY,
      SUM(AC_STOR_TAG_AMT_KOR_PY)     AS AC_STOR_TAG_AMT_KOR_PY,
      SUM(AC_QTY_PY)                  AS AC_QTY_PY,
      SUM(AC_TAG_AMG_PY)              AS AC_TAG_AMG_PY,
      SUM(AC_SALE_AMT_PY)             AS AC_SALE_AMT_PY,
      SUM(STOCK_QTY_PY)               AS STOCK_QTY_PY,
      SUM(STOCK_TAG_AMT_PY)           AS STOCK_TAG_AMT_PY,
      SUM(AC_ORD_QTY_PY_END)          AS AC_ORD_QTY_PY_END,
      SUM(AC_ORD_TAG_AMT_PY_END)      AS AC_ORD_TAG_AMT_PY_END,
      SUM(AC_STOR_QTY_KOR_PY_END)     AS AC_STOR_QTY_KOR_PY_END,
      SUM(AC_STOR_TAG_AMT_KOR_PY_END) AS AC_STOR_TAG_AMT_KOR_PY_END,
      SUM(QTY_PY_END)                 AS QTY_PY_END,
      SUM(TAG_AMT_PY_END)             AS TAG_AMT_PY_END,
      SUM(SALE_AMT_PY_END)            AS SALE_AMT_PY_END,
      SUM(W_QTY)                      AS W_QTY,
      SUM(W_TAG_AMG)                  AS W_TAG_AMG,
      SUM(W_SALE_AMT)                 AS W_SALE_AMT,
      SUM(AC_ORD_QTY)                 AS AC_ORD_QTY,
      SUM(AC_ORD_TAG_AMT)             AS AC_ORD_TAG_AMT,
      SUM(AC_STOR_QTY_KOR)            AS AC_STOR_QTY_KOR,
      SUM(AC_STOR_TAG_AMT_KOR)        AS AC_STOR_TAG_AMT_KOR,
      SUM(AC_QTY)                     AS AC_QTY,
      SUM(AC_TAG_AMG)                 AS AC_TAG_AMG,
      SUM(AC_SALE_AMT)                AS AC_SALE_AMT,
      SUM(STOCK_QTY)                  AS STOCK_QTY,
      SUM(STOCK_TAG_AMT)              AS STOCK_TAG_AMT
  FROM (
      /* 1) 전년 주간 */
      SELECT
          D.PRDT_CD, D.BRD_CD,
          (D.SALE_NML_TAG_AMT_CNS + D.SALE_RET_TAG_AMT_CNS) AS W_TAG_AMG_PY,
          (D.SALE_NML_QTY_CNS      + D.SALE_RET_QTY_CNS)    AS W_QTY_PY,
          (D.SALE_NML_SALE_AMT_CNS + D.SALE_RET_SALE_AMT_CNS) AS W_SALE_AMT_PY,
          0::NUMBER AS AC_ORD_QTY_PY, 0::NUMBER AS AC_ORD_TAG_AMT_PY,
          0::NUMBER AS AC_STOR_QTY_KOR_PY, 0::NUMBER AS AC_STOR_TAG_AMT_KOR_PY,
          0::NUMBER AS AC_TAG_AMG_PY,  0::NUMBER AS AC_QTY_PY, 0::NUMBER AS AC_SALE_AMT_PY,
          0::NUMBER AS STOCK_QTY_PY,   0::NUMBER AS STOCK_TAG_AMT_PY,
          0::NUMBER AS AC_ORD_QTY_PY_END, 0::NUMBER AS AC_ORD_TAG_AMT_PY_END,
          0::NUMBER AS AC_STOR_QTY_KOR_PY_END, 0::NUMBER AS AC_STOR_TAG_AMT_KOR_PY_END,
          0::NUMBER AS QTY_PY_END, 0::NUMBER AS TAG_AMT_PY_END, 0::NUMBER AS SALE_AMT_PY_END,
          0::NUMBER AS W_TAG_AMG, 0::NUMBER AS W_QTY, 0::NUMBER AS W_SALE_AMT,
          0::NUMBER AS AC_ORD_QTY, 0::NUMBER AS AC_ORD_TAG_AMT,
          0::NUMBER AS AC_STOR_QTY_KOR, 0::NUMBER AS AC_STOR_TAG_AMT_KOR,
          0::NUMBER AS AC_TAG_AMG,  0::NUMBER AS AC_QTY, 0::NUMBER AS AC_SALE_AMT,
          0::NUMBER AS STOCK_QTY,   0::NUMBER AS STOCK_TAG_AMT
      FROM PRCS.DW_SCS_D D
      WHERE D.SESN IN ('{py_season}')
        AND D.DT BETWEEN DATE '{py_week_end}' - 6 AND DATE '{py_week_end}'
      UNION ALL
      /* 2) 전년 누적 스냅샷 */
      SELECT
          A.PRDT_CD, A.BRD_CD,
          0,0,0,
          A.AC_ORD_QTY, A.AC_ORD_TAG_AMT, A.AC_STOR_QTY_KOR, A.AC_STOR_TAG_AMT_KOR,
          (A.AC_SALE_NML_TAG_AMT_CNS + A.AC_SALE_RET_TAG_AMT_CNS),
          (A.AC_SALE_NML_QTY_CNS     + A.AC_SALE_RET_QTY_CNS),
          (A.AC_SALE_NML_SALE_AMT_CNS+ A.AC_SALE_RET_SALE_AMT_CNS),
          A.STOCK_QTY, A.STOCK_TAG_AMT,
          0,0,0,0, 0,0,0,
          0,0,0,
          0,0,0,0, 0,0,0, 0,0
      FROM PRCS.DW_SCS_DACUM A
      WHERE A.SESN IN ('{py_season}')
        AND DATE '{py_week_end}' BETWEEN A.START_DT AND A.END_DT
      UNION ALL
      /* 3) 전년 시즌 마감 스냅샷 */
      SELECT
          A.PRDT_CD, A.BRD_CD,
          0,0,0,
          0,0,0,0, 0,0,0, 0,0,
          A.AC_ORD_QTY, A.AC_ORD_TAG_AMT, A.AC_STOR_QTY_KOR, A.AC_STOR_TAG_AMT_KOR,
          (A.AC_SALE_NML_QTY_CNS     + A.AC_SALE_RET_QTY_CNS)      AS QTY_PY_END,
          (A.AC_SALE_NML_TAG_AMT_CNS + A.AC_SALE_RET_TAG_AMT_CNS)  AS TAG_AMT_PY_END,
          (A.AC_SALE_NML_SALE_AMT_CNS+ A.AC_SALE_RET_SALE_AMT_CNS) AS SALE_AMT_PY_END,
          0,0,0,
          0,0,0,0, 0,0,0, 0,0
      FROM PRCS.DW_SCS_DACUM A
      WHERE A.SESN IN ('{py_season}')
        AND DATE '{py_season_end}' BETWEEN A.START_DT AND A.END_DT
      UNION ALL
      /* 4) 당해 주간 */
      SELECT
          D.PRDT_CD, D.BRD_CD,
          0,0,0,
          0,0,0,0, 0,0,0, 0,0,
          0,0,0,0, 0,0,0,
          (D.SALE_NML_TAG_AMT_CNS + D.SALE_RET_TAG_AMT_CNS),
          (D.SALE_NML_QTY_CNS     + D.SALE_RET_QTY_CNS),
          (D.SALE_NML_SALE_AMT_CNS+ D.SALE_RET_SALE_AMT_CNS),
          0,0,0,0, 0,0,0, 0,0
      FROM PRCS.DW_SCS_D D
      WHERE D.SESN IN ('{cy_season}')
        AND D.DT BETWEEN DATE '{cy_week_end}' - 6 AND DATE '{cy_week_end}'
      UNION ALL
      /* 5) 당해 누적 스냅샷 */
      SELECT
          A.PRDT_CD, A.BRD_CD,
          0,0,0,
          0,0,0,0, 0,0,0, 0,0,
          0,0,0,0, 0,0,0,
          0,0,0,
          A.AC_ORD_QTY, A.AC_ORD_TAG_AMT, A.AC_STOR_QTY_KOR, A.AC_STOR_TAG_AMT_KOR,
          (A.AC_SALE_NML_TAG_AMT_CNS + A.AC_SALE_RET_TAG_AMT_CNS),
          (A.AC_SALE_NML_QTY_CNS     + A.AC_SALE_RET_QTY_CNS),
          (A.AC_SALE_NML_SALE_AMT_CNS+ A.AC_SALE_RET_SALE_AMT_CNS),
          A.STOCK_QTY, A.STOCK_TAG_AMT
      FROM PRCS.DW_SCS_DACUM A
      WHERE A.SESN IN ('{cy_season}')
        AND DATE '{cy_week_end}' BETWEEN A.START_DT AND A.END_DT
  ) U
  JOIN PRCS.DB_PRDT B ON U.PRDT_CD = B.PRDT_CD
  GROUP BY
      U.BRD_CD, B.PARENT_PRDT_KIND_NM, B.PRDT_KIND_NM, B.ITEM
)
SELECT
    BRD_CD                                         AS "브랜드",
    PARENT_PRDT_KIND_NM                            AS "대분류",
    PRDT_KIND_NM                                   AS "중분류",
    ITEM                                           AS "아이템코드",
    ITEM_NM                                        AS "아이템명(한글)",
    AC_ORD_TAG_AMT                                 AS "발주(TAG)",
    ROUND( AC_ORD_TAG_AMT / NULLIF(AC_ORD_TAG_AMT_PY, 0), 4 ) AS "전년비(발주)",
    W_TAG_AMG                                      AS "주간판매매출(TAG)",
    ROUND( W_TAG_AMG / NULLIF(W_TAG_AMG_PY, 0), 4 )           AS "전년비(주간)",
    AC_TAG_AMG                                     AS "누적판매매출(TAG)",
    ROUND( AC_TAG_AMG / NULLIF(AC_TAG_AMG_PY, 0), 4 )         AS "전년비(누적)",
    ROUND( AC_TAG_AMG / NULLIF(AC_STOR_TAG_AMT_KOR, 0), 4 )   AS "누적판매율당년",
    ROUND(
      (AC_TAG_AMG   / NULLIF(AC_STOR_TAG_AMT_KOR,    0))
      - (AC_TAG_AMG_PY / NULLIF(AC_STOR_TAG_AMT_KOR_PY, 0)),
      4
    )                                               AS "누적판매율차이",
    ROUND( TAG_AMT_PY_END / NULLIF(AC_STOR_TAG_AMT_KOR_PY_END, 0), 4 ) AS "전년마감판매율",
    -- 판매율 재계산을 위한 원본 데이터
    AC_TAG_AMG                                     AS "누적판매TAG가",
    AC_STOR_TAG_AMT_KOR                            AS "누적입고TAG가",
    AC_TAG_AMG_PY                                  AS "전년누적판매TAG가",
    AC_STOR_TAG_AMT_KOR_PY                         AS "전년누적입고TAG가",
    TAG_AMT_PY_END                                 AS "전년마감누적판매TAG가",
    AC_STOR_TAG_AMT_KOR_PY_END                     AS "전년마감누적입고TAG가"
FROM RDS
WHERE PARENT_PRDT_KIND_NM <> 'ACC'
ORDER BY BRD_CD, PARENT_PRDT_KIND_NM, PRDT_KIND_NM, ITEM
"""
    return query


def build_acc_stock_query(dates: dict) -> str:
    """
    ACC 재고주수 분석 쿼리 생성
    """
    cy_week_end = dates['cy_week_end'].strftime('%Y-%m-%d')
    cy_week_start = dates['cy_week_start'].strftime('%Y-%m-%d')
    py_week_end = dates['py_week_end'].strftime('%Y-%m-%d')
    py_week_start = dates['py_week_start'].strftime('%Y-%m-%d')
    cy_4w_start = dates['cy_4w_start'].strftime('%Y-%m-%d')
    py_4w_start = dates['py_4w_start'].strftime('%Y-%m-%d')
    
    query = f"""
WITH
base AS (
  SELECT 
    a.end_dt,
    a.brd_cd,
    b.item,
    b.item_nm,
    b.prdt_kind_nm,
    b.parent_prdt_kind_nm,
    (a.sale_nml_sale_amt_cns + a.sale_ret_sale_amt_cns) AS sale_amt,
    (a.sale_nml_qty_cns  + a.sale_ret_qty_cns)          AS sale_qty,
    a.stock_qty,
    a.stock_tag_amt AS stock_tag_amt
  FROM prcs.db_scs_w a
  JOIN prcs.db_prdt  b
    ON a.prdt_cd = b.prdt_cd
   AND a.brd_cd  = b.brd_cd
  WHERE b.parent_prdt_kind_nm = 'ACC'
),

-- CY: 당년 주간
cy AS (
  SELECT 
    brd_cd, item,
    MAX(item_nm)       AS item_nm,
    MAX(prdt_kind_nm)  AS prdt_kind_nm,
    SUM(sale_amt)      AS sale_amt_cy,
    SUM(sale_qty)      AS sale_qty_cy
  FROM base
  WHERE end_dt BETWEEN '{cy_week_start}' AND '{cy_week_end}'
  GROUP BY brd_cd, item
),

-- PY: 전년 동주차 (364일 전)
py AS (
  SELECT 
    brd_cd, item,
    SUM(sale_amt) AS sale_amt_py,
    SUM(sale_qty) AS sale_qty_py
  FROM base
  WHERE end_dt BETWEEN '{py_week_start}' AND '{py_week_end}'
  GROUP BY brd_cd, item
),

-- CY 4주 평균
avg4w AS (
  SELECT 
    brd_cd, item,
    SUM(sale_qty)::numeric / 4.0 AS sale_qty_4w_avg
  FROM base
  WHERE end_dt BETWEEN '{cy_4w_start}' AND '{cy_week_end}'
  GROUP BY brd_cd, item
),

-- PY 4주 평균 (364일 전)
avg4w_py AS (
  SELECT 
    brd_cd, item,
    SUM(sale_qty)::numeric / 4.0 AS sale_qty_4w_avg_py
  FROM base
  WHERE end_dt BETWEEN '{py_4w_start}' AND '{py_week_end}'
  GROUP BY brd_cd, item
),

-- CY 재고
stk AS (
  SELECT 
    brd_cd, item,
    SUM(stock_qty) AS stock_qty_asof,
    SUM(stock_tag_amt) AS stock_tag_amt_asof
  FROM base
  WHERE end_dt = '{cy_week_end}'
  GROUP BY brd_cd, item
),

-- PY 재고 (364일 전)
stk_py AS (
  SELECT 
    brd_cd, item,
    SUM(stock_qty) AS stock_qty_asof_py,
    SUM(stock_tag_amt) AS stock_tag_amt_asof_py
  FROM base
  WHERE end_dt = '{py_week_end}'
  GROUP BY brd_cd, item
),

merge AS (
  SELECT
    COALESCE(c.brd_cd, p.brd_cd, a.brd_cd, ap.brd_cd, s.brd_cd, sp.brd_cd) AS brd_cd,
    COALESCE(c.item,   p.item,   a.item,   ap.item,   s.item,   sp.item)   AS item,
    COALESCE(c.item_nm, c2.item_nm)             AS item_nm,
    COALESCE(c.prdt_kind_nm, c2.prdt_kind_nm)  AS prdt_kind_nm,
    COALESCE(c.sale_amt_cy, 0)                  AS sale_amt_cy,
    COALESCE(p.sale_amt_py, 0)                  AS sale_amt_py,
    COALESCE(c.sale_qty_cy, 0)                  AS sale_qty_cy,
    COALESCE(a.sale_qty_4w_avg, 0)::numeric     AS sale_qty_4w_avg,
    COALESCE(ap.sale_qty_4w_avg_py, 0)::numeric AS sale_qty_4w_avg_py,
    COALESCE(s.stock_qty_asof, 0)               AS stock_qty_asof,
    COALESCE(sp.stock_qty_asof_py, 0)           AS stock_qty_asof_py,
    COALESCE(s.stock_tag_amt_asof, 0)           AS stock_tag_amt_asof,
    COALESCE(sp.stock_tag_amt_asof_py, 0)      AS stock_tag_amt_asof_py
  FROM cy c
  LEFT JOIN (
    SELECT brd_cd, item, MAX(item_nm) AS item_nm, MAX(prdt_kind_nm) AS prdt_kind_nm
    FROM base GROUP BY brd_cd, item
  ) c2 ON c2.brd_cd = c.brd_cd AND c2.item = c.item

  FULL OUTER JOIN py       p  ON c.brd_cd = p.brd_cd  AND c.item = p.item
  FULL OUTER JOIN avg4w    a  ON COALESCE(c.brd_cd,p.brd_cd) = a.brd_cd
                              AND COALESCE(c.item,  p.item)  = a.item
  FULL OUTER JOIN avg4w_py ap ON COALESCE(c.brd_cd,p.brd_cd,a.brd_cd) = ap.brd_cd
                              AND COALESCE(c.item,  p.item,  a.item)  = ap.item
  FULL OUTER JOIN stk      s  ON COALESCE(c.brd_cd,p.brd_cd,a.brd_cd,ap.brd_cd) = s.brd_cd
                              AND COALESCE(c.item,  p.item,  a.item,  ap.item)  = s.item
  FULL OUTER JOIN stk_py   sp ON COALESCE(c.brd_cd,p.brd_cd,a.brd_cd,ap.brd_cd,s.brd_cd) = sp.brd_cd
                              AND COALESCE(c.item,  p.item,  a.item,  ap.item,  s.item)  = sp.item
),

tot AS (
  SELECT brd_cd, SUM(sale_amt_cy) AS total_sale_amt_cy
  FROM merge
  GROUP BY brd_cd
)

SELECT
  m.brd_cd                               AS "브랜드코드",
  m.prdt_kind_nm                         AS "카테고리",
  m.item                                 AS "아이템",
  m.item_nm                              AS "아이템명",
  m.sale_qty_cy::bigint                  AS "판매수량",
  m.sale_amt_cy::bigint                  AS "판매매출",

  CASE 
    WHEN m.sale_amt_py = 0 THEN NULL
    ELSE ROUND((m.sale_amt_cy::numeric / NULLIF(m.sale_amt_py,0)) * 100)::int || '%'
  END                                    AS "전년비",

  CASE 
    WHEN t.total_sale_amt_cy = 0 THEN '0%'
    ELSE ROUND((m.sale_amt_cy::numeric / t.total_sale_amt_cy) * 100)::int || '%'
  END                                    AS "비중",

  ROUND(m.sale_qty_4w_avg, 2)            AS "4주평균판매량",
  m.stock_qty_asof::bigint               AS "재고",
  m.stock_tag_amt_asof::bigint          AS "재고금액",

  ROUND(
    CASE WHEN m.sale_qty_4w_avg > 0 
         THEN m.stock_qty_asof::numeric / m.sale_qty_4w_avg
         ELSE NULL END
  , 1)                                   AS "재고주수",

  ROUND(
    CASE WHEN m.sale_qty_4w_avg_py > 0 
         THEN m.stock_qty_asof_py::numeric / m.sale_qty_4w_avg_py
         ELSE NULL END
  , 1)                                   AS "전년재고주수",

  ROUND(
    (
      CASE WHEN m.sale_qty_4w_avg > 0 
           THEN m.stock_qty_asof::numeric / m.sale_qty_4w_avg
           ELSE NULL END
      -
      CASE WHEN m.sale_qty_4w_avg_py > 0 
           THEN m.stock_qty_asof_py::numeric / m.sale_qty_4w_avg_py
           ELSE NULL END
    )
  , 1)                                   AS "재고주수차이(당년-전년)"

FROM merge m
JOIN tot  t ON m.brd_cd = t.brd_cd
WHERE m.sale_amt_cy    <> 0
   OR m.sale_qty_cy    <> 0
   OR m.stock_qty_asof <> 0
   OR m.stock_tag_amt_asof <> 0
ORDER BY m.brd_cd, m.sale_amt_cy DESC
"""
    return query


def save_to_js(clothing_df: pd.DataFrame, acc_df: pd.DataFrame, 
               dates: dict, output_path: Path) -> None:
    """
    데이터를 JavaScript 파일로 저장
    
    Args:
        clothing_df: 당시즌의류 DataFrame
        acc_df: ACC 재고주수 DataFrame
        dates: 날짜 정보 딕셔너리
        output_path: 저장 경로
    """
    # 디렉토리 생성
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 당시즌의류 데이터 변환
    clothing_data = {}
    for _, row in clothing_df.iterrows():
        brand = row['브랜드']
        if brand not in clothing_data:
            clothing_data[brand] = []
        
        item_data = {
            'category': row['대분류'],
            'subCategory': row['중분류'],
            'itemCode': row['아이템코드'],
            'itemName': row['아이템명(한글)'],
            'orderTag': float(row['발주(TAG)']) if pd.notna(row['발주(TAG)']) else 0,
            'orderYoY': float(row['전년비(발주)']) if pd.notna(row['전년비(발주)']) else None,
            'weeklySalesTag': float(row['주간판매매출(TAG)']) if pd.notna(row['주간판매매출(TAG)']) else 0,
            'weeklyYoY': float(row['전년비(주간)']) if pd.notna(row['전년비(주간)']) else None,
            'cumSalesTag': float(row['누적판매매출(TAG)']) if pd.notna(row['누적판매매출(TAG)']) else 0,
            'cumYoY': float(row['전년비(누적)']) if pd.notna(row['전년비(누적)']) else None,
            'cumSalesRate': float(row['누적판매율당년']) if pd.notna(row['누적판매율당년']) else None,
            'cumSalesRateDiff': float(row['누적판매율차이']) if pd.notna(row['누적판매율차이']) else None,
            'pyClosingSalesRate': float(row['전년마감판매율']) if pd.notna(row['전년마감판매율']) else None
        }
        clothing_data[brand].append(item_data)
    
    # ACC 재고주수 데이터 변환
    acc_data = {}
    for _, row in acc_df.iterrows():
        brand = row['브랜드코드']
        if brand not in acc_data:
            acc_data[brand] = []
        
        item_data = {
            'category': row['카테고리'],
            'itemCode': row['아이템'],
            'itemName': row['아이템명'],
            'saleQty': int(row['판매수량']) if pd.notna(row['판매수량']) else 0,
            'saleAmt': int(row['판매매출']) if pd.notna(row['판매매출']) else 0,
            'yoyRate': row['전년비'] if pd.notna(row['전년비']) else None,
            'shareRate': row['비중'] if pd.notna(row['비중']) else '0%',
            'avg4wSaleQty': float(row['4주평균판매량']) if pd.notna(row['4주평균판매량']) else 0,
            'stockQty': int(row['재고']) if pd.notna(row['재고']) else 0,
            'stockAmt': int(float(row['재고금액'])) if pd.notna(row.get('재고금액', 0)) and row.get('재고금액', 0) != 0 else 0,
            'stockWeeks': float(row['재고주수']) if pd.notna(row['재고주수']) else None,
            'pyStockWeeks': float(row['전년재고주수']) if pd.notna(row['전년재고주수']) else None,
            'stockWeeksDiff': float(row['재고주수차이(당년-전년)']) if pd.notna(row['재고주수차이(당년-전년)']) else None
        }
        acc_data[brand].append(item_data)
    
    # 메타데이터
    metadata = {
        'updateDate': dates['update_date'].strftime('%Y-%m-%d'),
        'cyWeekStart': dates['cy_week_start'].strftime('%Y-%m-%d'),
        'cyWeekEnd': dates['cy_week_end'].strftime('%Y-%m-%d'),
        'pyWeekStart': dates['py_week_start'].strftime('%Y-%m-%d'),
        'pyWeekEnd': dates['py_week_end'].strftime('%Y-%m-%d'),
        'cySeason': dates['cy_season'],
        'pySeason': dates['py_season'],
        'pySeasonEnd': dates['py_season_end'].strftime('%Y-%m-%d'),
        'generatedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # JavaScript 파일 생성
    js_content = f"""// 브랜드별 현황 - 당시즌의류/ACC 재고주수 분석 데이터
// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 업데이트 일자: {dates['update_date'].strftime('%Y-%m-%d')}
// 당년 주간: {dates['cy_week_start'].strftime('%Y-%m-%d')} ~ {dates['cy_week_end'].strftime('%Y-%m-%d')}
// 전년 동주차: {dates['py_week_start'].strftime('%Y-%m-%d')} ~ {dates['py_week_end'].strftime('%Y-%m-%d')}

(function() {{
  // 메타데이터
  var brandStockMetadata = {json.dumps(metadata, ensure_ascii=False, indent=2)};
  
  // 당시즌의류 브랜드별 현황 (ACC 제외)
  var clothingBrandStatus = {json.dumps(clothing_data, ensure_ascii=False, indent=2)};
  
  // ACC 재고주수 분석
  var accStockAnalysis = {json.dumps(acc_data, ensure_ascii=False, indent=2)};
  
  // 브랜드별 요약 통계 (당시즌의류)
  var clothingSummary = {{}};
  for (var brand in clothingBrandStatus) {{
    var items = clothingBrandStatus[brand];
    clothingSummary[brand] = {{
      itemCount: items.length,
      totalOrderTag: items.reduce(function(sum, item) {{ return sum + (item.orderTag || 0); }}, 0),
      totalWeeklySales: items.reduce(function(sum, item) {{ return sum + (item.weeklySalesTag || 0); }}, 0),
      totalCumSales: items.reduce(function(sum, item) {{ return sum + (item.cumSalesTag || 0); }}, 0)
    }};
  }}
  
  // 브랜드별 요약 통계 (ACC)
  var accSummary = {{}};
  for (var brand in accStockAnalysis) {{
    var items = accStockAnalysis[brand];
    accSummary[brand] = {{
      itemCount: items.length,
      totalSaleQty: items.reduce(function(sum, item) {{ return sum + (item.saleQty || 0); }}, 0),
      totalSaleAmt: items.reduce(function(sum, item) {{ return sum + (item.saleAmt || 0); }}, 0),
      totalStockQty: items.reduce(function(sum, item) {{ return sum + (item.stockQty || 0); }}, 0)
    }};
  }}
  
  // 전역 객체에 할당
  if (typeof window !== 'undefined') {{
    window.brandStockMetadata = brandStockMetadata;
    window.clothingBrandStatus = clothingBrandStatus;
    window.accStockAnalysis = accStockAnalysis;
    window.clothingSummary = clothingSummary;
    window.accSummary = accSummary;
  }}
}})();
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    file_size = output_path.stat().st_size / 1024
    print(f"   [완료] JS 파일 저장 완료: {output_path}")
    print(f"   [파일크기] 파일 크기: {file_size:.2f} KB")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='브랜드별 현황 - 당시즌의류/ACC 재고주수 분석 다운로드')
    parser.add_argument('--update-date', '-d', type=str, 
                        help='업데이트 일자 (YYYY-MM-DD 형식, 기본값: 오늘)')
    parser.add_argument('--output-dir', '-o', type=str,
                        help='출력 디렉토리 (기본값: raw/YYYYMM/ETC)')
    parser.add_argument('--no-js', action='store_true',
                        help='JS 파일 생성 안 함')
    args = parser.parse_args()
    
    # 업데이트 일자 설정
    if args.update_date:
        try:
            update_date = datetime.strptime(args.update_date, '%Y-%m-%d')
        except ValueError:
            print(f"[오류] 날짜 형식 오류: {args.update_date}")
            print("   올바른 형식: YYYY-MM-DD (예: 2025-11-24)")
            sys.exit(1)
    else:
        update_date = datetime.now()
    
    # 날짜 계산
    dates = calculate_dates(update_date)
    
    print("=" * 60)
    print("[브랜드별 현황] 당시즌의류/ACC 재고주수 분석")
    print("=" * 60)
    print(f"\n[날짜 설정]")
    print(f"   업데이트 일자: {dates['update_date'].strftime('%Y-%m-%d')}")
    print(f"   당년 주간: {dates['cy_week_start'].strftime('%Y-%m-%d')} ~ {dates['cy_week_end'].strftime('%Y-%m-%d')}")
    print(f"   전년 동주차: {dates['py_week_start'].strftime('%Y-%m-%d')} ~ {dates['py_week_end'].strftime('%Y-%m-%d')}")
    print(f"   당년 시즌: {dates['cy_season']}")
    print(f"   전년 시즌: {dates['py_season']}")
    print(f"   전년 시즌 마감: {dates['py_season_end'].strftime('%Y-%m-%d')}")
    
    # 출력 디렉토리 설정 (평가월 사용)
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # 업데이트 날짜를 YYYYMMDD 형식으로 변환
        date_str = update_date.strftime('%Y%m%d')
        # 평가월(analysis_month) 추출 (metadata.json에서 읽거나 계산)
        from scripts.path_utils import extract_year_month_from_date
        year_month = extract_year_month_from_date(date_str)
        output_dir = project_root / 'raw' / year_month / 'ETC'
    
    # 디렉토리 생성
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n📁 출력 디렉토리: {output_dir}")
    
    # 파일명 설정
    date_suffix = update_date.strftime('%Y%m%d')
    clothing_file = output_dir / f"당시즌의류_브랜드별현황_{date_suffix}.csv"
    acc_file = output_dir / f"ACC_재고주수분석_{date_suffix}.csv"
    
    conn = None
    try:
        # Snowflake 연결
        conn = get_snowflake_connection()
        
        # 1. 당시즌의류 분석
        print(f"\n[진행] 당시즌의류 분석 쿼리 실행 중...")
        clothing_query = build_clothing_query(dates)
        clothing_df = execute_query(conn, clothing_query)
        clothing_df.to_csv(clothing_file, index=False, encoding='utf-8-sig')
        print(f"   [완료] 저장 완료: {clothing_file}")
        print(f"   [데이터] 데이터 건수: {len(clothing_df):,}건")
        
        # 2. ACC 재고주수 분석
        print(f"\n[진행] ACC 재고주수 분석 쿼리 실행 중...")
        acc_query = build_acc_stock_query(dates)
        acc_df = execute_query(conn, acc_query)
        acc_df.to_csv(acc_file, index=False, encoding='utf-8-sig')
        print(f"   [완료] 저장 완료: {acc_file}")
        print(f"   [데이터] 데이터 건수: {len(acc_df):,}건")
        
        # 3. JS 파일 생성 (옵션)
        if not args.no_js:
            print(f"\n[진행] JS 파일 생성 중...")
            js_output_path = project_root / 'public' / f'brand_stock_analysis_{date_suffix}.js'
            save_to_js(clothing_df, acc_df, dates, js_output_path)
        
        print("\n" + "=" * 60)
        print("[완료] 모든 작업이 완료되었습니다!")
        print("=" * 60)
        
        print(f"\n[생성된 파일]")
        print(f"   - {clothing_file}")
        print(f"   - {acc_file}")
        if not args.no_js:
            print(f"   - {js_output_path}")
        
        # ★★★ JSON 파일로도 저장 ★★★
        json_dir = project_root / 'public' / 'data' / date_suffix
        json_dir.mkdir(parents=True, exist_ok=True)
        
        # dates 딕셔너리의 datetime 객체를 문자열로 변환
        dates_for_json = {
            'update_date': dates['update_date'].strftime('%Y-%m-%d') if isinstance(dates.get('update_date'), datetime) else dates.get('update_date'),
            'cy_week_start': dates['cy_week_start'].strftime('%Y-%m-%d') if isinstance(dates.get('cy_week_start'), datetime) else dates.get('cy_week_start'),
            'cy_week_end': dates['cy_week_end'].strftime('%Y-%m-%d') if isinstance(dates.get('cy_week_end'), datetime) else dates.get('cy_week_end'),
            'py_week_start': dates['py_week_start'].strftime('%Y-%m-%d') if isinstance(dates.get('py_week_start'), datetime) else dates.get('py_week_start'),
            'py_week_end': dates['py_week_end'].strftime('%Y-%m-%d') if isinstance(dates.get('py_week_end'), datetime) else dates.get('py_week_end'),
            'cy_season': dates.get('cy_season'),
            'py_season': dates.get('py_season'),
            'py_season_end': dates['py_season_end'].strftime('%Y-%m-%d') if isinstance(dates.get('py_season_end'), datetime) else dates.get('py_season_end'),
            'cy_4w_start': dates['cy_4w_start'].strftime('%Y-%m-%d') if isinstance(dates.get('cy_4w_start'), datetime) else dates.get('cy_4w_start'),
            'py_4w_start': dates['py_4w_start'].strftime('%Y-%m-%d') if isinstance(dates.get('py_4w_start'), datetime) else dates.get('py_4w_start'),
        }
        
        # DataFrame을 딕셔너리로 변환 (Decimal 타입 처리)
        def convert_decimal_to_float(obj):
            """Decimal 타입을 float로 변환하는 재귀 함수"""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimal_to_float(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimal_to_float(item) for item in obj]
            elif isinstance(obj, (int, float, str, bool, type(None))):
                return obj
            else:
                # 다른 타입도 시도
                try:
                    return float(obj)
                except (ValueError, TypeError):
                    return str(obj)
        
        # 의류 및 ACC 데이터를 브랜드별로 그룹화하여 JSON으로 변환 (generate_brand_stock_analysis.py와 동일한 구조)
        clothing_by_brand = {}
        if not clothing_df.empty:
            print(f"\n[의류 데이터 변환] 총 {len(clothing_df)}건")
            print(f"[의류 데이터 변환] 컬럼: {list(clothing_df.columns)}")
            
            # 브랜드 컬럼명 확인 (한글 또는 영문)
            brand_col = None
            for col in ['브랜드', 'brand', 'BRAND', '브랜드코드']:
                if col in clothing_df.columns:
                    brand_col = col
                    print(f"[의류 데이터 변환] 브랜드 컬럼 발견: {brand_col}")
                    break
            
            if brand_col:
                unique_brands = clothing_df[brand_col].unique()
                print(f"[의류 데이터 변환] 브랜드 목록: {list(unique_brands)}")
                
                for brand_code in unique_brands:
                    brand_code_str = str(brand_code).strip()
                    brand_df = clothing_df[clothing_df[brand_col] == brand_code]
                    
                    # generate_brand_stock_analysis.py와 동일한 필드명 구조로 변환
                    brand_items = []
                    for _, row in brand_df.iterrows():
                        item_data = {
                            "category": str(row.get('대분류', '')).strip() if pd.notna(row.get('대분류')) else "",
                            "subCategory": str(row.get('중분류', '')).strip() if pd.notna(row.get('중분류')) else "",
                            "itemCode": str(row.get('아이템코드', '')).strip() if pd.notna(row.get('아이템코드')) else "",
                            "itemName": str(row.get('아이템명(한글)', '')).strip() if pd.notna(row.get('아이템명(한글)')) else "",
                            "orderTag": convert_decimal_to_float(row.get('발주(TAG)', 0)) if pd.notna(row.get('발주(TAG)', 0)) else None,
                            "orderYoY": convert_decimal_to_float(row.get('전년비(발주)', None)) if pd.notna(row.get('전년비(발주)', None)) else None,
                            "weeklySalesTag": convert_decimal_to_float(row.get('주간판매매출(TAG)', 0)) if pd.notna(row.get('주간판매매출(TAG)', 0)) else None,
                            "weeklyYoY": convert_decimal_to_float(row.get('전년비(주간)', None)) if pd.notna(row.get('전년비(주간)', None)) else None,
                            "cumSalesTag": convert_decimal_to_float(row.get('누적판매매출(TAG)', 0)) if pd.notna(row.get('누적판매매출(TAG)', 0)) else None,
                            "cumYoY": convert_decimal_to_float(row.get('전년비(누적)', None)) if pd.notna(row.get('전년비(누적)', None)) else None,
                            "cumSalesRate": convert_decimal_to_float(row.get('누적판매율당년', None)) if pd.notna(row.get('누적판매율당년', None)) else None,
                            "cumSalesRateDiff": convert_decimal_to_float(row.get('누적판매율차이', None)) if pd.notna(row.get('누적판매율차이', None)) else None,
                            "pyClosingSalesRate": convert_decimal_to_float(row.get('전년마감판매율', None)) if pd.notna(row.get('전년마감판매율', None)) else None
                        }
                        brand_items.append(item_data)
                    
                    clothing_by_brand[brand_code_str] = brand_items
                    print(f"[의류 데이터 변환] 브랜드 {brand_code_str}: {len(brand_items)}건")
            else:
                print(f"[의류 데이터 변환] ⚠️ 브랜드 컬럼을 찾을 수 없습니다!")
                print(f"[의류 데이터 변환] 사용 가능한 컬럼: {list(clothing_df.columns)}")
        
        acc_by_brand = {}
        if not acc_df.empty:
            print(f"\n[ACC 데이터 변환] 총 {len(acc_df)}건")
            print(f"[ACC 데이터 변환] 컬럼: {list(acc_df.columns)}")
            
            # 브랜드 컬럼명 확인
            brand_col = None
            for col in ['브랜드코드', 'brand', 'BRAND', '브랜드']:
                if col in acc_df.columns:
                    brand_col = col
                    print(f"[ACC 데이터 변환] 브랜드 컬럼 발견: {brand_col}")
                    break
            
            if brand_col:
                unique_brands = acc_df[brand_col].unique()
                print(f"[ACC 데이터 변환] 브랜드 목록: {list(unique_brands)}")
                
                for brand_code in unique_brands:
                    brand_code_str = str(brand_code).strip()
                    brand_df = acc_df[acc_df[brand_col] == brand_code]
                    
                    # generate_brand_stock_analysis.py와 동일한 필드명 구조로 변환
                    brand_items = []
                    for _, row in brand_df.iterrows():
                        # 전년비와 비중은 퍼센트 문자열로 유지
                        yoy_rate = row.get('전년비', None)
                        if pd.notna(yoy_rate) and yoy_rate != '':
                            yoy_rate_str = str(yoy_rate).strip()
                            if not yoy_rate_str.endswith('%'):
                                try:
                                    yoy_rate_str = f"{int(float(yoy_rate_str))}%"
                                except:
                                    pass
                        else:
                            yoy_rate_str = None
                        
                        share_rate = row.get('비중', '0%')
                        if pd.notna(share_rate) and share_rate != '':
                            share_rate_str = str(share_rate).strip()
                        else:
                            share_rate_str = "0%"
                        
                        item_data = {
                            "category": str(row.get('카테고리', '')).strip() if pd.notna(row.get('카테고리')) else "",
                            "itemCode": str(row.get('아이템', '')).strip() if pd.notna(row.get('아이템')) else "",
                            "itemName": str(row.get('아이템명', '')).strip() if pd.notna(row.get('아이템명')) else "",
                            "saleQty": int(convert_decimal_to_float(row.get('판매수량', 0))) if pd.notna(row.get('판매수량', 0)) else None,
                            "saleAmt": int(convert_decimal_to_float(row.get('판매매출', 0))) if pd.notna(row.get('판매매출', 0)) else None,
                            "yoyRate": yoy_rate_str,
                            "shareRate": share_rate_str,
                            "avg4wSaleQty": convert_decimal_to_float(row.get('4주평균판매량', None)) if pd.notna(row.get('4주평균판매량', None)) else None,
                            "stockQty": int(convert_decimal_to_float(row.get('재고', 0))) if pd.notna(row.get('재고', 0)) else None,
                            "stockAmt": int(convert_decimal_to_float(row.get('재고금액', 0))) if pd.notna(row.get('재고금액', 0)) else 0,
                            "stockWeeks": convert_decimal_to_float(row.get('재고주수', None)) if pd.notna(row.get('재고주수', None)) else None,
                            "pyStockWeeks": convert_decimal_to_float(row.get('전년재고주수', None)) if pd.notna(row.get('전년재고주수', None)) else None,
                            "stockWeeksDiff": convert_decimal_to_float(row.get('재고주수차이(당년-전년)', None)) if pd.notna(row.get('재고주수차이(당년-전년)', None)) else None
                        }
                        brand_items.append(item_data)
                    
                    acc_by_brand[brand_code_str] = brand_items
                    print(f"[ACC 데이터 변환] 브랜드 {brand_code_str}: {len(brand_items)}건")
            else:
                print(f"[ACC 데이터 변환] ⚠️ 브랜드 컬럼을 찾을 수 없습니다!")
                print(f"[ACC 데이터 변환] 사용 가능한 컬럼: {list(acc_df.columns)}")
        
        # 브랜드별 요약 통계 계산 (당시즌의류)
        clothing_summary = {}
        for brand, items in clothing_by_brand.items():
            clothing_summary[brand] = {
                "itemCount": len(items),
                "totalOrderTag": sum(item.get("orderTag", 0) or 0 for item in items),
                "totalWeeklySales": sum(item.get("weeklySalesTag", 0) or 0 for item in items),
                "totalCumSales": sum(item.get("cumSalesTag", 0) or 0 for item in items)
            }
        
        # 브랜드별 요약 통계 계산 (ACC)
        acc_summary = {}
        for brand, items in acc_by_brand.items():
            acc_summary[brand] = {
                "itemCount": len(items),
                "totalSaleQty": sum(item.get("saleQty", 0) or 0 for item in items),
                "totalSaleAmt": sum(item.get("saleAmt", 0) or 0 for item in items),
                "totalStockQty": sum(item.get("stockQty", 0) or 0 for item in items),
                "totalStockAmt": sum(item.get("stockAmt", 0) or 0 for item in items)
            }
        
        stock_data = {
            'brandStockMetadata': dates_for_json,
            'clothingBrandStatus': clothing_by_brand,
            'accStockAnalysis': acc_by_brand,
            'clothingSummary': clothing_summary,
            'accSummary': acc_summary
        }
        
        json_path = json_dir / "stock_analysis.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(stock_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n  [완료] JSON 저장: {json_path}")
        print(f"  [데이터] 의류 브랜드 수: {len(clothing_by_brand)}")
        print(f"  [데이터] ACC 브랜드 수: {len(acc_by_brand)}")
        if clothing_by_brand:
            total_clothing_items = sum(len(items) for items in clothing_by_brand.values())
            print(f"  [데이터] 의류 총 아이템 수: {total_clothing_items}")
        if acc_by_brand:
            total_acc_items = sum(len(items) for items in acc_by_brand.values())
            print(f"  [데이터] ACC 총 아이템 수: {total_acc_items}")
        
    except Exception as e:
        print(f"\n[오류] 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\n[연결종료] Snowflake 연결 종료")


if __name__ == "__main__":
    main()

