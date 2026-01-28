import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';

/**
 * ACC 재고주수 분석 데이터 조회 API
 * 
 * GET /api/stock-weeks
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - asof_dt: string (기준일)
 * - data: { CY, PY } (당년/전년 재고주수 데이터)
 * - rowCount: { CY, PY } (각 기간별 데이터 개수)
 * 
 * 캐싱 전략:
 * - Redis 캐시 (24시간 TTL)
 * - 키: stock-weeks-YYYYMMDD (날짜별)
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 forceUpdate 확인
    const { searchParams } = new URL(request.url);
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    
    // 오늘 날짜로 캐시 키 생성
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `stock-weeks-${today}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[재고주수 API] 캐시 히트: ${cacheKey}`);
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[재고주수 API] 강제 업데이트: ${cacheKey}`);
    }
    
    console.log(`[재고주수 API] 캐시 미스: ${cacheKey} - Snowflake 조회 시작`);
    
    // 2. Snowflake 쿼리 실행
    const query = getStockWeeksQuery();
    const rows = await executeSnowflakeQuery(query);
    
    // 3. 데이터를 당년/전년으로 분리
    const cyData = rows.filter(row => row.YY === 'CY');
    const pyData = rows.filter(row => row.YY === 'PY');
    
    // 4. 기준일 추출
    const asofDt = rows.length > 0 ? rows[0].ASOF_DT : '';
    
    // 5. 결과 구성
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      asof_dt: formatDate(asofDt),
      data: {
        CY: cyData,
        PY: pyData
      },
      rowCount: {
        CY: cyData.length,
        PY: pyData.length
      },
      cached: false
    };
    
    // 6. Redis 캐시에 저장 (24시간)
    await setCache(cacheKey, result, 86400);
    console.log(`[재고주수 API] 캐시 저장 완료: ${cacheKey}`);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[재고주수 API] 에러 발생:', error);
    
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
 * ACC 재고주수 분석 Snowflake 쿼리 생성
 */
function getStockWeeksQuery(): string {
  return `
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
