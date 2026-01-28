import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';

/**
 * 할인내역 데이터 조회 API
 * 
 * GET /api/discount-detail
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - data: 할인내역 원본 데이터
 * - rowCount: 전체 데이터 개수
 * 
 * 캐싱 전략:
 * - Redis 캐시 (24시간 TTL)
 * - 키: discount-detail-{브랜드코드}-YYYYMM
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 brand, month, forceUpdate 확인
    const { searchParams } = new URL(request.url);
    const brandCode = searchParams.get('brand');
    const analysisMonth = searchParams.get('month'); // YYYY-MM 형식
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    
    // 브랜드 코드 필수 확인
    if (!brandCode) {
      return NextResponse.json(
        { success: false, error: '브랜드 코드가 필요합니다.' },
        { status: 400 }
      );
    }
    
    // 분석월 기준으로 캐시 키 생성
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const monthKey = analysisMonth ? analysisMonth.replace('-', '') : today.slice(0, 6);
    const cacheKey = `discount-detail-${brandCode}-${monthKey}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[할인내역 API] 캐시 히트: ${cacheKey}`);
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[할인내역 API] 강제 업데이트: ${cacheKey}`);
    }
    
    console.log(`[할인내역 API] 캐시 미스: ${cacheKey} - Snowflake 조회 시작`);
    
    // 2. Snowflake 쿼리 실행
    const query = getDiscountQuery(brandCode);
    const rows = await executeSnowflakeQuery(query);
    
    console.log(`[할인내역 API] Snowflake 조회 완료: ${rows.length}행`);
    
    // 3. 결과 구성
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      brandCode,
      analysisMonth: analysisMonth || today.slice(0, 6),
      data: rows,
      rowCount: rows.length,
      cached: false
    };
    
    // 4. Redis 캐시에 저장 (24시간)
    await setCache(cacheKey, result, 86400);
    console.log(`[할인내역 API] 캐시 저장 완료: ${cacheKey}`);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[할인내역 API] 에러 발생:', error);
    
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
 * 할인내역 Snowflake 쿼리 생성
 * @param brandCode 브랜드 코드 (M, I, X, V, ST, W)
 */
function getDiscountQuery(brandCode: string): string {
  return `
WITH base AS (
    SELECT
        /* CY / PY 구분 */
        CASE
            WHEN a.SALE_DT >= DATE_TRUNC('month', CURRENT_DATE)
             AND a.SALE_DT <  CURRENT_DATE
                THEN 'CY'
            ELSE 'PY'
        END AS gubun,

        a.BRD_CD,
        b.CD_NM,
        e.PRDT_KIND_NM,
        e.ITEM,
        e.ITEM_NM,
        a.PRDT_CD,
        e.PRDT_NM,

        /* 🔹 채널코드 (M 브랜드 특정 매장 RF 치환) */
        CASE
            WHEN a.BRD_CD = 'M'
             AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                THEN 'RF'
            ELSE s.DIST_TYPE_SAP
        END AS channel_cd,

        a.TAG_AMT,
        a.SALE_AMT

    FROM PRCS.DW_SALE a
    JOIN PRCS.DB_SHOP s
      ON a.SHOP_ID = s.SHOP_ID
     AND a.BRD_CD  = s.BRD_CD
     AND s.MNG_TYPE = 'A'
     AND s.ANAL_CNTRY = 'KO'
    JOIN FNF.PRCS.DB_PRDT e
      ON a.PRDT_CD = e.PRDT_CD
    LEFT JOIN PRCS.DW_COMN_CD b
      ON a.DIST_CLS = b.CD
     AND b.PARENT_CD = 'C034'
    LEFT JOIN PRCS.DW_COMN_CD c
      ON a.MARGIN_TYPE_CD = c.CD
     AND c.PARENT_CD = 'S079'
    WHERE
        a.BRD_CD = '${brandCode}'
        AND (
            (
                /* CY */
                a.SALE_DT BETWEEN DATE_TRUNC('month', CURRENT_DATE)
                              AND CURRENT_DATE - INTERVAL '1 day'
            )
            OR
            (
                /* PY */
                a.SALE_DT BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 year')
                              AND (CURRENT_DATE - INTERVAL '1 day') - INTERVAL '1 year'
            )
        )
)

SELECT
    gubun              AS "구분",
    BRD_CD             AS "브랜드",
    CD_NM              AS "할인유형명",
    channel_cd         AS "채널코드",

    /* 🔹 채널명 */
    CASE channel_cd
        WHEN '01'  THEN '백화점'
        WHEN '02'  THEN '면세점'
        WHEN '03'  THEN '직영점(가두)'
        WHEN '04'  THEN '자사몰'
        WHEN '05'  THEN '제휴몰'
        WHEN '06'  THEN '대리점'
        WHEN '07'  THEN '아울렛'
        WHEN '11' THEN '직영몰'
        WHEN '12' THEN '직영점(가두2)'
        WHEN 'RF' THEN 'RF'
        ELSE '기타'
    END               AS "채널명",

    PRDT_KIND_NM       AS "카테고리",
    ITEM               AS "아이템코드",
    ITEM_NM            AS "아이템명",
    PRDT_CD            AS "품번",
    PRDT_NM            AS "품명",
    SUM(TAG_AMT)       AS "TAG매출",
    SUM(SALE_AMT)      AS "실판매출"

FROM base
GROUP BY
    gubun,
    BRD_CD,
    CD_NM,
    channel_cd,
    PRDT_KIND_NM,
    ITEM,
    ITEM_NM,
    PRDT_CD,
    PRDT_NM
HAVING SUM(SALE_AMT) <> 0
ORDER BY gubun, CD_NM
`;
}

