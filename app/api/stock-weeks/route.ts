import { NextResponse } from 'next/server';
import { Redis } from '@upstash/redis';

/**
 * 재고주수 분석 데이터 조회 API
 * 
 * Snowflake SQL API를 사용하여 직접 쿼리 실행
 * Redis 캐시로 성능 최적화 (24시간)
 */

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 60; // 최대 60초

// Redis 클라이언트
const redis = process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN
  ? new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    })
  : null;

/**
 * Snowflake SQL API로 쿼리 실행
 */
async function querySnowflake(sqlText: string): Promise<any> {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const username = process.env.SNOWFLAKE_USERNAME;
  const password = process.env.SNOWFLAKE_PASSWORD;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const database = process.env.SNOWFLAKE_DATABASE;
  
  if (!account || !username || !password) {
    throw new Error('Snowflake 환경 변수가 설정되지 않았습니다.');
  }

  const authString = Buffer.from(`${username}:${password}`).toString('base64');
  const apiUrl = `https://${account}.snowflakecomputing.com/api/v2/statements`;
  
  console.log('[Snowflake API] 재고주수 쿼리 실행 시작');
  
  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Basic ${authString}`,
      'Accept': 'application/json',
      'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT'
    },
    body: JSON.stringify({
      statement: sqlText,
      timeout: 60,
      warehouse: warehouse,
      database: database,
      role: 'ACCOUNTADMIN'
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Snowflake API 오류: ${response.status} - ${errorText}`);
  }

  const result = await response.json();
  console.log('[Snowflake API] 재고주수 쿼리 실행 완료');
  
  return result;
}

/**
 * Snowflake 결과를 JSON으로 변환
 */
function parseSnowflakeResult(result: any): any[] {
  if (!result.data || !Array.isArray(result.data)) {
    return [];
  }

  const columns = result.resultSetMetaData?.rowType?.map((col: any) => col.name) || [];
  
  return result.data.map((row: any[]) => {
    const obj: any = {};
    columns.forEach((col: string, idx: number) => {
      obj[col] = row[idx];
    });
    return obj;
  });
}

/**
 * 재고주수 분석 쿼리
 */
function getStockWeeksQuery(): string {
  return `
WITH params AS (
    SELECT
        (CURRENT_DATE - 1)::DATE AS start_asof_dt,
        (CURRENT_DATE - 1)::DATE AS end_asof_dt
),
base_date AS (
    SELECT
        DATEADD(day, seq4(), p.start_asof_dt) AS asof_dt,
        DATEADD(year, -1, DATEADD(day, seq4(), p.start_asof_dt)) AS asof_dt_py
    FROM params p,
         TABLE(GENERATOR(ROWCOUNT => 4000))
    WHERE DATEADD(day, seq4(), p.start_asof_dt) <= p.end_asof_dt
),
prdt AS (
    SELECT
        c.brd_cd, c.prdt_cd,
        MAX(c.prdt_kind_nm) AS prdt_kind_nm,
        MAX(c.item) AS item,
        MAX(c.item_nm) AS item_nm,
        MAX(c.prdt_nm) AS prdt_nm
    FROM fnf.prcs.db_prdt c
    WHERE c.parent_prdt_kind_nm = 'ACC'
    GROUP BY 1,2
),
stock_base AS (
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'CY' AS yy,
        SUM(a.stock_qty) AS stock_qty,
        SUM(a.stock_tag_amt) AS stock_tag_amt
    FROM base_date d
    JOIN fnf.prcs.dw_scs_dacum a ON d.asof_dt BETWEEN a.start_dt AND a.end_dt
    JOIN prdt p ON a.brd_cd = p.brd_cd AND a.prdt_cd = p.prdt_cd
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'PY' AS yy,
        SUM(a.stock_qty) AS stock_qty,
        SUM(a.stock_tag_amt) AS stock_tag_amt
    FROM base_date d
    JOIN fnf.prcs.dw_scs_dacum a ON d.asof_dt_py BETWEEN a.start_dt AND a.end_dt
    JOIN prdt p ON a.brd_cd = p.brd_cd AND a.prdt_cd = p.prdt_cd
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
),
sale_28d AS (
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'CY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_28d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a ON a.dt BETWEEN DATEADD(day, -27, d.asof_dt) AND d.asof_dt
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'PY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_28d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a ON a.dt BETWEEN DATEADD(day, -27, d.asof_dt_py) AND d.asof_dt_py
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
),
sale_7d AS (
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'CY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_7d,
        SUM(a.SALE_NML_TAG_AMT_CNS + a.SALE_RET_TAG_AMT_CNS) AS sale_tag_7d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a ON a.dt BETWEEN DATEADD(day, -6, d.asof_dt) AND d.asof_dt
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        d.asof_dt, a.brd_cd, a.prdt_cd, 'PY' AS yy,
        SUM(a.SALE_NML_QTY_CNS + a.SALE_RET_QTY_CNS) AS sale_qty_7d,
        SUM(a.SALE_NML_TAG_AMT_CNS + a.SALE_RET_TAG_AMT_CNS) AS sale_tag_7d
    FROM base_date d
    JOIN fnf.prcs.dw_scs_d a ON a.dt BETWEEN DATEADD(day, -6, d.asof_dt_py) AND d.asof_dt_py
    WHERE a.brd_cd <> 'A'
    GROUP BY 1,2,3,4
)
SELECT
    st.asof_dt AS ASOF_DT, st.brd_cd AS BRD_CD, st.yy AS YY,
    p.prdt_kind_nm AS PRDT_KIND_NM, p.item AS ITEM_CD, p.item_nm AS ITEM_NM,
    st.prdt_cd AS PRDT_CD, p.prdt_nm AS PRDT_NM,
    COALESCE(s7.sale_qty_7d, 0) AS SALE_QTY_7D,
    COALESCE(s7.sale_tag_7d, 0) AS SALE_TAG_7D,
    COALESCE(s28.sale_qty_28d, 0) AS SALE_QTY_28D,
    st.stock_qty AS STOCK_QTY, st.stock_tag_amt AS STOCK_TAG_AMT
FROM stock_base st
JOIN prdt p ON st.brd_cd = p.brd_cd AND st.prdt_cd = p.prdt_cd
LEFT JOIN sale_28d s28 ON st.asof_dt = s28.asof_dt AND st.brd_cd = s28.brd_cd 
    AND st.prdt_cd = s28.prdt_cd AND st.yy = s28.yy
LEFT JOIN sale_7d s7 ON st.asof_dt = s7.asof_dt AND st.brd_cd = s7.brd_cd 
    AND st.prdt_cd = s7.prdt_cd AND st.yy = s7.yy
WHERE st.stock_qty > 0
ORDER BY 1, 2, 3, 13 DESC NULLS LAST
`;
}

export async function GET(request: Request) {
  try {
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `stock-weeks-${today}`;
    
    // 1. Redis 캐시 확인
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) {
        console.log(`[재고주수 API] Redis 캐시 적중: ${cacheKey}`);
        return NextResponse.json(cached, {
          headers: {
            'Cache-Control': 'public, max-age=3600'
          }
        });
      }
    }
    
    // 2. Snowflake 쿼리 실행
    console.log(`[재고주수 API] 캐시 미스 - Snowflake 조회 시작`);
    const sqlResult = await querySnowflake(getStockWeeksQuery());
    const allData = parseSnowflakeResult(sqlResult);
    
    // 3. 데이터 가공
    const cyData = allData.filter(row => row.YY === 'CY');
    const pyData = allData.filter(row => row.YY === 'PY');
    
    const asofDt = allData.length > 0 ? allData[0].ASOF_DT : null;
    
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      asof_dt: asofDt ? String(asofDt) : '',
      data: {
        CY: cyData,
        PY: pyData
      },
      rowCount: {
        CY: cyData.length,
        PY: pyData.length
      }
    };
    
    console.log(`[재고주수 API] 데이터 조회 완료: CY ${cyData.length}, PY ${pyData.length}`);
    
    // 4. Redis 캐시 저장 (24시간)
    if (redis) {
      await redis.setex(cacheKey, 86400, JSON.stringify(result));
      console.log(`[재고주수 API] Redis 캐시 저장 완료: ${cacheKey}`);
    }
    
    return NextResponse.json(result, {
      headers: {
        'Cache-Control': 'public, max-age=3600'
      }
    });
    
  } catch (error: any) {
    console.error('[재고주수 API 에러]', error);
    return NextResponse.json(
      { 
        success: false, 
        error: error.message,
        details: error.stack 
      },
      { status: 500 }
    );
  }
}

