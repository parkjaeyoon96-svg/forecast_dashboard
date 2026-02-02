import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';
import { getTodayCompact, getToday, calculateAsofDate } from '@/lib/dateUtils';

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
 * - 키: discount-detail-{브랜드코드}-{분석월YYYYMM}-{업데이트일자YYYYMMDD}
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 brand, month, date, forceUpdate 확인
    const { searchParams } = new URL(request.url);
    const brandCode = searchParams.get('brand');
    const analysisMonth = searchParams.get('month'); // YYYY-MM 형식
    const updateDateParam = searchParams.get('date'); // YYYYMMDD 형식 (선택)
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    
    // 브랜드 코드 필수 확인
    if (!brandCode) {
      return NextResponse.json(
        { success: false, error: '브랜드 코드가 필요합니다.' },
        { status: 400 }
      );
    }
    
    // 날짜별 캐시 키 생성 (한국 시간 기준)
    // 분석월과 업데이트 일자를 캐시 키에 포함하여 다른 기간 데이터가 섞이지 않도록 함
    const today = getTodayCompact();
    // 분석월과 업데이트 일자가 있으면 캐시 키에 포함, 없으면 오늘 날짜만 사용
    const monthKey = analysisMonth ? analysisMonth.replace('-', '') : today.slice(0, 6);
    const dateKey = updateDateParam && updateDateParam.length === 8 ? updateDateParam : today;
    const cacheKey = `discount-detail-${brandCode}-${monthKey}-${dateKey}`;
    
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
    
    // 2. 업데이트 일자 변환 (YYYYMMDD -> YYYY-MM-DD)
    let updateDate: string | undefined;
    if (updateDateParam && updateDateParam.length === 8) {
      updateDate = `${updateDateParam.slice(0, 4)}-${updateDateParam.slice(4, 6)}-${updateDateParam.slice(6, 8)}`;
    }
    
    // 3. 기준일 계산 (분석월과 업데이트 일자 모두 고려)
    const asof_dt = analysisMonth ? calculateAsofDate(analysisMonth, updateDate) : null;
    console.log(`[할인내역 API] 기준일:`, { analysisMonth, updateDate, updateDateParam, asof_dt });
    
    // 3. Snowflake 쿼리 실행
    const query = getDiscountQuery(brandCode, asof_dt);
    const rows = await executeSnowflakeQuery(query);
    
    console.log(`[할인내역 API] Snowflake 조회 완료: ${rows.length}행`);
    if (rows.length > 0) {
      console.log(`[할인내역 API] 첫 번째 행 샘플:`, rows[0]);
      console.log(`[할인내역 API] 첫 번째 행 키:`, Object.keys(rows[0]));
    }
    
    // 4. 결과 구성
    const result = {
      success: true,
      date: getToday(), // 한국 시간 기준
      asof_dt: asof_dt || 'CURRENT_DATE-1', // Snowflake에서 계산
      brandCode,
      analysisMonth: analysisMonth || today.slice(0, 6),
      data: rows,
      rowCount: rows.length,
      cached: false
    };
    
    // 5. Redis 캐시에 저장 (24시간)
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
 * @param asofDate 기준일 (YYYY-MM-DD) - null이면 Snowflake CURRENT_DATE 사용
 */
function getDiscountQuery(brandCode: string, asofDate: string | null): string {
  // 분석월이 지정되지 않으면 Snowflake의 CURRENT_DATE 사용
  const dateLogic = asofDate 
    ? `'${asofDate}'::DATE AS asof_dt   -- 파라미터로 받은 기준일`
    : `DATEADD(DAY, -1, CURRENT_DATE())::DATE AS asof_dt   -- Snowflake 어제 날짜`;
  
  return `
WITH params AS (
    SELECT
        ${dateLogic}
),
/* ✅ 날짜 로직
   - CY: 이번달 1일 ~ asof_dt (분석월 말일 또는 어제)
   - PY: 전년 동일월 1일 ~ 전년 동일일(= DATEADD(YEAR,-1,asof_dt))
*/
periods AS (
    SELECT
        'CY' AS gubun,
        DATE_TRUNC('MONTH', asof_dt)::DATE AS dt_from,
        asof_dt AS dt_to
    FROM params
    UNION ALL
    SELECT
        'PY' AS gubun,
        DATEADD(YEAR, -1, DATE_TRUNC('MONTH', asof_dt))::DATE AS dt_from,
        DATEADD(YEAR, -1, asof_dt)::DATE AS dt_to
    FROM params
),
base AS (
    SELECT
        p.gubun,
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

    FROM periods p
    JOIN PRCS.DW_SALE a
      ON a.SALE_DT BETWEEN p.dt_from AND p.dt_to
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
    WHERE a.BRD_CD = '${brandCode}'
)

SELECT
    gubun              AS "구분",
    BRD_CD             AS "브랜드",
    COALESCE(CD_NM, '기타')  AS "할인유형명",
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
