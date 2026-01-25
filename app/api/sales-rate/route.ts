import { NextResponse } from 'next/server';
import { Redis } from '@upstash/redis';

/**
 * 판매율 분석 데이터 조회 API
 * 
 * Snowflake SQL API를 사용하여 직접 쿼리 실행
 * Redis 캐시로 성능 최적화 (24시간)
 */

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 60; // 최대 60초 (Vercel Pro 필요)

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

  // 1. JWT 토큰 생성 (Basic Auth 사용)
  const authString = Buffer.from(`${username}:${password}`).toString('base64');
  
  // Snowflake SQL API endpoint
  const apiUrl = `https://${account}.snowflakecomputing.com/api/v2/statements`;
  
  console.log('[Snowflake API] 쿼리 실행 시작');
  
  // 2. SQL 쿼리 실행 요청
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
      role: 'ACCOUNTADMIN' // 필요시 수정
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Snowflake API 오류: ${response.status} - ${errorText}`);
  }

  const result = await response.json();
  console.log('[Snowflake API] 쿼리 실행 완료');
  
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
 * 판매율 분석 쿼리
 */
function getSalesRateQuery(): string {
  return `
WITH PARAM AS (
  SELECT
      DATEADD(DAY, -1, CURRENT_DATE())                                      AS ASOF_DT
    , DATEADD(YEAR, -1, DATEADD(DAY, -1, CURRENT_DATE()))                   AS ASOF_DT_PY
    , CONCAT(RIGHT(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 1, 2), 'F')     AS CUR_SESN
    , CONCAT(RIGHT(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 2, 2), 'F')     AS PY_SESN
    , DATE_FROM_PARTS(YEAR(DATEADD(DAY, -1, CURRENT_DATE())) - 1, 2, 28)    AS PY_END_DT
),

BASE AS (
  SELECT
      pa.ASOF_DT AS ASOF_DT, 'CUR' AS PERIOD_GB, a.BRD_CD, a.SESN AS SESN, a.PRDT_CD,
      b.PRDT_KIND_NM, b.ITEM AS ITEM_CD, b.ITEM_NM, b.PRDT_NM,
      a.AC_ORD_QTY_KOR, a.AC_ORD_TAG_AMT_KOR, a.AC_STOR_QTY_KOR, a.AC_STOR_TAG_AMT_KOR,
      (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS) AS SALE_QTY,
      (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG,
      a.STOCK_QTY, a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.CUR_SESN AND a.BRD_CD <> 'A' AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.ASOF_DT BETWEEN a.START_DT AND a.END_DT

  UNION ALL

  SELECT
      pa.ASOF_DT_PY AS ASOF_DT, 'PY' AS PERIOD_GB, a.BRD_CD, a.SESN AS SESN, a.PRDT_CD,
      b.PRDT_KIND_NM, b.ITEM AS ITEM_CD, b.ITEM_NM, b.PRDT_NM,
      a.AC_ORD_QTY_KOR, a.AC_ORD_TAG_AMT_KOR, a.AC_STOR_QTY_KOR, a.AC_STOR_TAG_AMT_KOR,
      (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS) AS SALE_QTY,
      (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG,
      a.STOCK_QTY, a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.PY_SESN AND a.BRD_CD <> 'A' AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.ASOF_DT_PY BETWEEN a.START_DT AND a.END_DT

  UNION ALL

  SELECT
      pa.PY_END_DT AS ASOF_DT, 'PY_END' AS PERIOD_GB, a.BRD_CD, a.SESN AS SESN, a.PRDT_CD,
      b.PRDT_KIND_NM, b.ITEM AS ITEM_CD, b.ITEM_NM, b.PRDT_NM,
      a.AC_ORD_QTY_KOR, a.AC_ORD_TAG_AMT_KOR, a.AC_STOR_QTY_KOR, a.AC_STOR_TAG_AMT_KOR,
      (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS) AS SALE_QTY,
      (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG,
      a.STOCK_QTY, a.STOCK_TAG_AMT
  FROM FNF.PRCS.DW_SCS_DACUM a
  JOIN PRCS.DB_PRDT b ON a.PRDT_CD = b.PRDT_CD
  JOIN PARAM pa ON 1=1
  WHERE a.SESN = pa.PY_SESN AND a.BRD_CD <> 'A' AND b.PARENT_PRDT_KIND_NM = '의류'
    AND pa.PY_END_DT BETWEEN a.START_DT AND a.END_DT
)

SELECT
    ASOF_DT, PERIOD_GB, BRD_CD, MAX(SESN) AS SESN, PRDT_CD,
    MAX(PRDT_KIND_NM) AS PRDT_KIND_NM, MAX(ITEM_CD) AS ITEM_CD,
    MAX(ITEM_NM) AS ITEM_NM, MAX(PRDT_NM) AS PRDT_NM,
    SUM(AC_ORD_QTY_KOR) AS AC_ORD_QTY_KOR,
    SUM(AC_ORD_TAG_AMT_KOR) AS AC_ORD_TAG_AMT_KOR,
    SUM(AC_STOR_QTY_KOR) AS AC_STOR_QTY_KOR,
    SUM(AC_STOR_TAG_AMT_KOR) AS AC_STOR_TAG_AMT_KOR,
    SUM(SALE_QTY) AS SALE_QTY, SUM(SALE_TAG) AS SALE_TAG,
    SUM(STOCK_QTY) AS STOCK_QTY, SUM(STOCK_TAG_AMT) AS STOCK_TAG_AMT
FROM BASE
GROUP BY ASOF_DT, PERIOD_GB, BRD_CD, PRDT_CD
HAVING COALESCE(SUM(AC_ORD_TAG_AMT_KOR), 0) + COALESCE(SUM(AC_STOR_TAG_AMT_KOR), 0)
     + COALESCE(SUM(SALE_TAG), 0) + COALESCE(SUM(STOCK_TAG_AMT), 0) <> 0
ORDER BY BRD_CD, PRDT_CD, PERIOD_GB, ASOF_DT
`;
}

export async function GET(request: Request) {
  try {
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `sales-rate-${today}`;
    
    // 1. Redis 캐시 확인
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) {
        console.log(`[판매율 API] Redis 캐시 적중: ${cacheKey}`);
        return NextResponse.json(cached, {
          headers: {
            'Cache-Control': 'public, max-age=3600'
          }
        });
      }
    }
    
    // 2. Snowflake 쿼리 실행
    console.log(`[판매율 API] 캐시 미스 - Snowflake 조회 시작`);
    const sqlResult = await querySnowflake(getSalesRateQuery());
    const allData = parseSnowflakeResult(sqlResult);
    
    // 3. 데이터 가공
    const curData = allData.filter(row => row.PERIOD_GB === 'CUR');
    const pyData = allData.filter(row => row.PERIOD_GB === 'PY');
    const pyEndData = allData.filter(row => row.PERIOD_GB === 'PY_END');
    
    const curDate = curData.length > 0 ? curData[0].ASOF_DT : null;
    const pyDate = pyData.length > 0 ? pyData[0].ASOF_DT : null;
    const pyEndDate = pyEndData.length > 0 ? pyEndData[0].ASOF_DT : null;
    
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      periodInfo: {
        curDate: curDate ? String(curDate) : '',
        pyDate: pyDate ? String(pyDate) : '',
        pyEndDate: pyEndDate ? String(pyEndDate) : ''
      },
      data: {
        CUR: curData,
        PY: pyData,
        PY_END: pyEndData
      },
      rowCount: {
        CUR: curData.length,
        PY: pyData.length,
        PY_END: pyEndData.length
      }
    };
    
    console.log(`[판매율 API] 데이터 조회 완료: CUR ${curData.length}, PY ${pyData.length}, PY_END ${pyEndData.length}`);
    
    // 4. Redis 캐시 저장 (24시간)
    if (redis) {
      await redis.setex(cacheKey, 86400, JSON.stringify(result));
      console.log(`[판매율 API] Redis 캐시 저장 완료: ${cacheKey}`);
    }
    
    return NextResponse.json(result, {
      headers: {
        'Cache-Control': 'public, max-age=3600'
      }
    });
    
  } catch (error: any) {
    console.error('[판매율 API 에러]', error);
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
