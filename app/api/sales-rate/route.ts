import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';

/**
 * 당시즌 의류 판매율 분석 데이터 조회 API
 * 
 * GET /api/sales-rate
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - periodInfo: { curDate, pyDate, pyEndDate }
 * - data: { CUR, PY, PY_END } (각 기간별 판매율 데이터)
 * - rowCount: { CUR, PY, PY_END } (각 기간별 데이터 개수)
 * 
 * 캐싱 전략:
 * - Redis 캐시 (24시간 TTL)
 * - 키: sales-rate-YYYYMMDD (날짜별)
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 forceUpdate 확인
    const { searchParams } = new URL(request.url);
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    
    // 오늘 날짜로 캐시 키 생성 (재고주수와 동일)
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `sales-rate-${today}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[판매율 API] 캐시 히트: ${cacheKey}`);
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[판매율 API] 강제 업데이트: ${cacheKey}`);
    }
    
    console.log(`[판매율 API] 캐시 미스: ${cacheKey} - Snowflake 조회 시작`);
    
    // 2. Snowflake 쿼리 실행
    const query = getSalesRateQuery();
    const rows = await executeSnowflakeQuery(query);
    
    // 3. 데이터를 기간별로 분리
    const curData = rows.filter(row => row.PERIOD_GB === 'CUR');
    const pyData = rows.filter(row => row.PERIOD_GB === 'PY');
    const pyEndData = rows.filter(row => row.PERIOD_GB === 'PY_END');
    
    // 4. 기간 정보 추출
    const curDate = curData.length > 0 ? curData[0].ASOF_DT : '';
    const pyDate = pyData.length > 0 ? pyData[0].ASOF_DT : '';
    const pyEndDate = pyEndData.length > 0 ? pyEndData[0].ASOF_DT : '';
    
    // 5. 결과 구성
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      periodInfo: {
        curDate: formatDate(curDate),
        pyDate: formatDate(pyDate),
        pyEndDate: formatDate(pyEndDate)
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
      },
      cached: false
    };
    
    // 6. Redis 캐시에 저장 (24시간)
    await setCache(cacheKey, result, 86400);
    console.log(`[판매율 API] 캐시 저장 완료: ${cacheKey}`);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[판매율 API] 에러 발생:', error);
    
    return NextResponse.json(
      { 
        success: false, 
        error: error.message || '데이터 조회 실패',
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      },
      { status: 500 }
    );
  }
}

/**
 * 판매율 분석 Snowflake 쿼리 생성
 */
function getSalesRateQuery(): string {
  return `
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
  JOIN FNF.PRCS.DB_PRDT b
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
  JOIN FNF.PRCS.DB_PRDT b
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
  JOIN FNF.PRCS.DB_PRDT b
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
`;
}

/**
 * 날짜 포맷 변환 (Date 객체 또는 문자열 -> YYYY-MM-DD)
 */
function formatDate(date: any): string {
  if (!date) return '';
  if (typeof date === 'string') return date;
  if (date instanceof Date) return date.toISOString().split('T')[0];
  return String(date);
}
