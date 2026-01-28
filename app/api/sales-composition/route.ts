import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';

/**
 * 매출구성 데이터 조회 API (채널별/아이템별 트리맵용)
 * 
 * GET /api/sales-composition
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - asof_dt: string (기준일 - 어제)
 * - data: { CY, PY } (당년/전년 매출구성 데이터)
 * - rowCount: { CY, PY } (각 기간별 데이터 개수)
 * 
 * 캐싱 전략:
 * - Redis 캐시 (24시간 TTL)
 * - 키: sales-composition-YYYYMMDD (날짜별)
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 forceUpdate, month 확인
    const { searchParams } = new URL(request.url);
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    const analysisMonth = searchParams.get('month'); // YYYY-MM 형식
    
    // 분석월 기준으로 캐시 키 생성
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const monthKey = analysisMonth ? analysisMonth.replace('-', '') : today.slice(0, 6);
    const cacheKey = `sales-composition-${monthKey}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[매출구성 API] 캐시 히트: ${cacheKey}`);
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[매출구성 API] 강제 업데이트: ${cacheKey}`);
    }
    
    console.log(`[매출구성 API] 캐시 미스: ${cacheKey} - Snowflake 조회 시작`);
    console.log(`[매출구성 API] Snowflake 환경변수 확인:`, {
      account: process.env.SNOWFLAKE_ACCOUNT ? '✓' : '✗',
      username: process.env.SNOWFLAKE_USERNAME ? '✓' : '✗',
      password: process.env.SNOWFLAKE_PASSWORD ? '✓' : '✗',
      warehouse: process.env.SNOWFLAKE_WAREHOUSE ? '✓' : '✗',
      database: process.env.SNOWFLAKE_DATABASE ? '✓' : '✗'
    });
    
    // 2. 기준일 계산 (분석월 기준 또는 어제)
    const asof_dt = analysisMonth ? calculateAsofDate(analysisMonth) : getYesterday();
    console.log(`[매출구성 API] 기준일:`, { analysisMonth, asof_dt });
    
    // 3. Snowflake 쿼리 실행
    const query = getSalesCompositionQuery(asof_dt);
    console.log(`[매출구성 API] 쿼리 실행 시작...`);
    const startTime = Date.now();
    const rows = await executeSnowflakeQuery(query);
    const elapsed = Date.now() - startTime;
    console.log(`[매출구성 API] 쿼리 완료 (${elapsed}ms, ${rows.length}행)`);
    
    // 4. 데이터를 당년/전년으로 분리
    const cyData = rows.filter(row => row.구분 === 'CY');
    const pyData = rows.filter(row => row.구분 === 'PY');
    
    // 5. 결과 구성
    const result = {
      success: true,
      date: new Date().toISOString().split('T')[0],
      asof_dt,
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
    console.log(`[매출구성 API] 캐시 저장 완료: ${cacheKey}`);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[매출구성 API] 에러 발생:', {
      message: error.message,
      code: error.code,
      sqlState: error.sqlState,
      stack: error.stack
    });
    
    return NextResponse.json(
      { 
        success: false, 
        error: error.message || '데이터 조회 실패',
        errorCode: error.code,
        errorDetails: error.sqlState,
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      },
      { status: 500 }
    );
  }
}

/**
 * 매출구성 Snowflake 쿼리 생성
 * @param asofDate 기준일 (YYYY-MM-DD)
 */
function getSalesCompositionQuery(asofDate: string): string {
  return `
WITH params AS (
    SELECT
        '${asofDate}'::DATE AS asof_dt   -- 파라미터로 받은 기준일
),
/* ✅ 날짜 로직
   - CY: 이번달 1일 ~ 어제(asof_dt)
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
/* shop 필터 (KO, 09 제외) */
shop_flt AS (
    SELECT
        BRD_CD,
        SHOP_ID,
        DIST_TYPE_SAP,
        SALE_TYPE_SAP
    FROM FNF.PRCS.DB_SHOP
    WHERE ANAL_CNTRY = 'KO'
      AND DIST_TYPE_SAP <> '09'
),
/* ✅ DW 선집계: SHOP_ID는 RF 판단에만 사용, 집계는 (구분/브랜드/상품/시즌/채널코드) 기준 */
dw_agg AS (
    SELECT
        p.gubun,
        a.BRD_CD,
        a.PRDT_CD,
        a.SESN,
        /* RF 강제 반영된 유통채널코드 */
        CASE
            WHEN a.BRD_CD = 'M'
             AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                THEN 'RF'
            ELSE sh.DIST_TYPE_SAP
        END AS CHNL_CD,
        /* TAG 매출 */
        SUM(
            CASE
                WHEN
                    (CASE
                        WHEN a.BRD_CD = 'M'
                         AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                            THEN 'RF'
                        ELSE sh.DIST_TYPE_SAP
                     END) IN ('08','99')
                    THEN (a.DELV_NML_TAG_AMT + a.DELV_RET_TAG_AMT)
                ELSE (a.SALE_NML_TAG_AMT + a.SALE_RET_TAG_AMT)
            END
        ) AS TAG_SALES,
        /* 실판매출 (08/99에만 SALE_TYPE 제한 적용) */
        SUM(
            CASE
                WHEN
                    (CASE
                        WHEN a.BRD_CD = 'M'
                         AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                            THEN 'RF'
                        ELSE sh.DIST_TYPE_SAP
                     END) IN ('08','99')
                 AND sh.SALE_TYPE_SAP IN ('Z001','Z003')
                    THEN (a.DELV_NML_SUPP_AMT + a.DELV_RET_SUPP_AMT) * 1.1
                WHEN
                    (CASE
                        WHEN a.BRD_CD = 'M'
                         AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                            THEN 'RF'
                        ELSE sh.DIST_TYPE_SAP
                     END) IN ('08','99')
                    THEN 0
                ELSE (a.SALE_NML_SALE_AMT + a.SALE_RET_SALE_AMT)
            END
        ) AS REAL_SALES
    FROM periods p
    JOIN FNF.PRCS.DW_SH_SCS_D a
      ON a.DT BETWEEN p.dt_from AND p.dt_to
    JOIN shop_flt sh
      ON a.BRD_CD  = sh.BRD_CD
     AND a.SHOP_ID = sh.SHOP_ID
    WHERE a.BRD_CD <> 'A'
    GROUP BY
        p.gubun,
        a.BRD_CD,
        a.PRDT_CD,
        a.SESN,
        CASE
            WHEN a.BRD_CD = 'M'
             AND a.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                THEN 'RF'
            ELSE sh.DIST_TYPE_SAP
        END
),
/* ✅ 현재시즌(YY + 코드)을 dt_to(=어제) 기준으로 계산 */
season_ref AS (
    SELECT
        gubun,
        TO_CHAR(
            CASE
                WHEN MONTH(dt_to) BETWEEN 3 AND 8 THEN dt_to
                WHEN MONTH(dt_to) BETWEEN 9 AND 12 THEN dt_to
                ELSE DATEADD(YEAR, -1, dt_to)   -- 1~2월은 직전년도 FW가 현재시즌
            END
        , 'YY') AS cur_yy,
        CASE
            WHEN MONTH(dt_to) BETWEEN 3 AND 8 THEN 'S'
            ELSE 'F'
        END AS cur_code
    FROM periods
)
SELECT
    /* ✅ 출력 순서: 구분, 브랜드, 채널, 카테고리, 아이템, TAG매출, 실판매출 */
    d.gubun AS "구분",
    d.BRD_CD AS "브랜드",
    /* 채널 */
    CASE d.CHNL_CD
        WHEN 'RF' THEN 'RF'
        WHEN '01' THEN '백화점'
        WHEN '02' THEN '면세점'
        WHEN '03' THEN '직영가두'
        WHEN '04' THEN '자사몰'
        WHEN '05' THEN '제휴몰'
        WHEN '06' THEN '대리점'
        WHEN '07' THEN '아울렛'
        WHEN '08' THEN '사입'
        WHEN '11' THEN '직영몰'
        WHEN '12' THEN '직영2'
        WHEN '99' THEN '기타'
        ELSE '기타'
    END AS "채널",
    /* 카테고리 (기존 아이템대분류 로직) */
    CASE
        /* 1) ACC면 PRDT_KIND_NM 반환 후 한글 매핑 */
        WHEN b.PARENT_PRDT_KIND_NM = 'ACC' THEN
            CASE b.PRDT_KIND_NM
                WHEN 'Bag'      THEN '가방'
                WHEN 'Shoes'    THEN '신발'
                WHEN 'Headwear' THEN '모자'
                WHEN 'Acc_etc'  THEN '기타ACC'
                ELSE b.PRDT_KIND_NM
            END
        /* 2) 그 외: SESN + 현재시즌으로 당시즌/차시즌/과시즌 */
        ELSE
            CASE
                /* left(SESN,3)에 N 포함 → 연도(YY)만 카운팅 */
                WHEN SUBSTR(d.SESN, 1, 3) LIKE '%N%' THEN
                    CASE
                        WHEN SUBSTR(d.SESN, 1, 2) = sr.cur_yy THEN '당시즌의류'
                        ELSE '과시즌의류'
                    END
                /* 일반 시즌 */
                ELSE
                    CASE
                        WHEN SUBSTR(d.SESN, 1, 2) = sr.cur_yy
                         AND RIGHT(d.SESN, 1) = sr.cur_code
                            THEN '당시즌의류'
                        WHEN SUBSTR(d.SESN, 1, 2) = sr.cur_yy
                            THEN '차시즌의류'
                        ELSE '과시즌의류'
                    END
            END
    END AS "카테고리",
    /* 아이템 (기존 ITEM_NM) */
    b.ITEM_NM AS "아이템",
    /* 매출 */
    SUM(d.TAG_SALES)  AS "TAG매출",
    SUM(d.REAL_SALES) AS "실판매출"
FROM dw_agg d
JOIN season_ref sr
  ON d.gubun = sr.gubun
JOIN FNF.PRCS.DB_PRDT b
  ON d.BRD_CD  = b.BRD_CD
 AND d.PRDT_CD = b.PRDT_CD
GROUP BY
    d.gubun,
    d.BRD_CD,
    d.CHNL_CD,
    b.ITEM_NM,
    b.PARENT_PRDT_KIND_NM,
    b.PRDT_KIND_NM,
    d.SESN,
    sr.cur_yy,
    sr.cur_code
HAVING (SUM(d.TAG_SALES) + SUM(d.REAL_SALES)) <> 0
`;
}

/**
 * 어제 날짜 반환 (YYYY-MM-DD)
 */
function getYesterday(): string {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  return yesterday.toISOString().split('T')[0];
}

/**
 * 분석월 기준 asof_dt 계산
 * - 분석월이 과거월: 해당 월의 말일
 * - 분석월이 현재월: 어제
 * 
 * @param analysisMonth YYYY-MM 형식의 분석월
 * @returns YYYY-MM-DD 형식의 기준일
 */
function calculateAsofDate(analysisMonth: string): string {
  const [year, month] = analysisMonth.split('-').map(Number);
  const targetMonthStart = new Date(year, month - 1, 1);
  const today = new Date();
  
  // 현재월인 경우: 어제까지
  if (year === today.getFullYear() && month === today.getMonth() + 1) {
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  }
  
  // 과거월인 경우: 해당 월의 말일
  const lastDay = new Date(year, month, 0); // month가 다음달이므로 0일 = 말일
  return lastDay.toISOString().split('T')[0];
}

