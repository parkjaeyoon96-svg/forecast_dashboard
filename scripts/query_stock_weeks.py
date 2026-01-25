"""
ACC 재고주수 분석 Snowflake 쿼리 실행 스크립트

사용법:
    python scripts/query_stock_weeks.py
    
출력: JSON 형식으로 CY(당년), PY(전년) 데이터 반환
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


def get_stock_weeks_query():
    """ACC 재고주수 분석 쿼리 생성"""
    
    query = """
/* ============================================================
   ✅ 재고 기준 재고주수 (판매 0이어도 재고 있으면 노출)
   ✅ DACUM은 '기준일 BETWEEN START_DT AND END_DT'로 구간 매칭
   ✅ 날짜를 "구간"으로 넣어서 여러 일자를 한번에 조회 가능
      - params에서 start_asof_dt ~ end_asof_dt 설정
      - 기본: 어제 하루만
   ============================================================ */
WITH params AS (
    SELECT
        /* 🔧 여기만 바꾸면 됨 */
        (CURRENT_DATE - 1)::DATE AS start_asof_dt,
        (CURRENT_DATE - 1)::DATE AS end_asof_dt
),
/* 날짜 리스트 생성 (ROWCOUNT는 상수) */
base_date AS (
    SELECT
        DATEADD(day, seq4(), p.start_asof_dt) AS asof_dt,
        DATEADD(year, -1, DATEADD(day, seq4(), p.start_asof_dt)) AS asof_dt_py
    FROM params p,
         TABLE(GENERATOR(ROWCOUNT => 4000))
    WHERE DATEADD(day, seq4(), p.start_asof_dt) <= p.end_asof_dt
),
/* ✅ 상품 마스터 : ACC만 (prdt_cd 단위 1행 보장) */
prdt AS (
    SELECT
        c.brd_cd,
        c.prdt_cd,
        MAX(c.prdt_kind_nm) AS prdt_kind_nm,
        MAX(c.item)         AS item,
        MAX(c.item_nm)      AS item_nm,
        MAX(c.prdt_nm)      AS prdt_nm
    FROM fnf.prcs.db_prdt c
    WHERE c.parent_prdt_kind_nm = 'ACC'
    GROUP BY 1,2
),
/* ✅ 재고 베이스 (당년/전년) */
stock_base AS (
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'CY' AS yy,
        SUM(a.stock_qty)     AS stock_qty,
        SUM(a.stock_tag_amt) AS stock_tag_amt
    FROM base_date d
    JOIN fnf.prcs.dw_scs_dacum a
      ON d.asof_dt BETWEEN a.start_dt AND a.end_dt
    JOIN prdt p
      ON a.brd_cd = p.brd_cd
     AND a.prdt_cd = p.prdt_cd
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'PY' AS yy,
        SUM(a.stock_qty)     AS stock_qty,
        SUM(a.stock_tag_amt) AS stock_tag_amt
    FROM base_date d
    JOIN fnf.prcs.dw_scs_dacum a
      ON d.asof_dt_py BETWEEN a.start_dt AND a.end_dt
    JOIN prdt p
      ON a.brd_cd = p.brd_cd
     AND a.prdt_cd = p.prdt_cd
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
),
/* 최근 28일 판매수량 (당년/전년) */
sale_28d AS (
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'CY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_28d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a
      ON a.dt BETWEEN DATEADD(day, -27, d.asof_dt) AND d.asof_dt
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'PY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_28d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a
      ON a.dt BETWEEN DATEADD(day, -27, d.asof_dt_py) AND d.asof_dt_py
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
),
/* 최근 7일 판매(주간) */
sale_7d AS (
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'CY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_7d,
        SUM(a.SALE_NML_TAG_AMT_CNS + a.SALE_RET_TAG_AMT_CNS) AS sale_tag_7d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a
      ON a.dt BETWEEN DATEADD(day, -6, d.asof_dt) AND d.asof_dt
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt,
        a.brd_cd,
        a.prdt_cd,
        'PY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_7d,
        SUM(a.SALE_NML_TAG_AMT_CNS + a.SALE_RET_TAG_AMT_CNS) AS sale_tag_7d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a
      ON a.dt BETWEEN DATEADD(day, -6, d.asof_dt_py) AND d.asof_dt_py
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
)
SELECT
    st.asof_dt                                         AS ASOF_DT,
    st.brd_cd                                          AS BRD_CD,
    st.yy                                              AS YY,
    p.prdt_kind_nm                                     AS PRDT_KIND_NM,
    p.item                                             AS ITEM_CD,
    p.item_nm                                          AS ITEM_NM,
    st.prdt_cd                                         AS PRDT_CD,
    p.prdt_nm                                          AS PRDT_NM,
    COALESCE(s7.sale_qty_7d, 0)                        AS SALE_QTY_7D,
    COALESCE(s7.sale_tag_7d, 0)                        AS SALE_TAG_7D,
    COALESCE(s28.sale_qty_28d, 0)                      AS SALE_QTY_28D,
    st.stock_qty                                       AS STOCK_QTY,
    st.stock_tag_amt                                   AS STOCK_TAG_AMT
FROM stock_base st
JOIN prdt p
  ON st.brd_cd = p.brd_cd
 AND st.prdt_cd = p.prdt_cd
LEFT JOIN sale_28d s28
  ON st.asof_dt  = s28.asof_dt
 AND st.brd_cd   = s28.brd_cd
 AND st.prdt_cd  = s28.prdt_cd
 AND st.yy       = s28.yy
LEFT JOIN sale_7d s7
  ON st.asof_dt  = s7.asof_dt
 AND st.brd_cd   = s7.brd_cd
 AND st.prdt_cd  = s7.prdt_cd
 AND st.yy       = s7.yy
WHERE st.stock_qty > 0
ORDER BY
    1, 2, 3, 13 DESC NULLS LAST
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
        query = get_stock_weeks_query()
        df = execute_query(conn, query)
        
        conn.close()
        
        # 데이터를 당년/전년으로 분리
        cy_data = df[df['YY'] == 'CY'].to_dict('records')
        py_data = df[df['YY'] == 'PY'].to_dict('records')
        
        # 기준일 추출
        asof_dt = df['ASOF_DT'].iloc[0] if len(df) > 0 else None
        
        # JSON 형식으로 변환
        result = {
            'success': True,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'asof_dt': str(asof_dt) if asof_dt else '',
            'data': {
                'CY': cy_data,
                'PY': py_data
            },
            'rowCount': {
                'CY': len(cy_data),
                'PY': len(py_data)
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
