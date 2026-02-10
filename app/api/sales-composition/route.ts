import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';
import { getTodayCompact, getToday, getYesterday, calculateAsofDate } from '@/lib/dateUtils';

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
 * - 키: sales-composition-{분석월YYYYMM}-{업데이트일자YYYYMMDD} (할인내역과 동일하게 기간별 분리)
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 forceUpdate, month, date, brand 확인 (할인내역 API와 동일하게 month·date 사용)
    const { searchParams } = new URL(request.url);
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    const analysisMonth = searchParams.get('month'); // YYYY-MM 형식
    const updateDateParam = searchParams.get('date'); // YYYYMMDD 형식 (업데이트 일자, 기준일 계산용)
    const brandCode = searchParams.get('brand'); // 브랜드 코드 (M, I, X, V, ST, W)
    
    // 캐시 키: 할인내역과 동일하게 분석월·업데이트 일자 포함 (다른 기간 데이터가 섞이지 않도록)
    const today = getTodayCompact();
    const monthKey = analysisMonth ? analysisMonth.replace('-', '') : today.slice(0, 6);
    const dateKey = updateDateParam && updateDateParam.length === 8 ? updateDateParam : today;
    const cacheKey = `sales-composition-${monthKey}-${dateKey}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[매출구성 API] 캐시 히트: ${cacheKey}`, brandCode ? `브랜드=${brandCode}` : '전체');
        // 브랜드 요청인 경우 캐시된 전체 데이터에서 해당 브랜드만 필터링 후 인사이트 재계산 (전 브랜드 동일 인사이트 방지)
        if (brandCode && cachedData.data?.CY) {
          const filteredCY = (cachedData.data.CY as any[]).filter((row: any) => row.브랜드 === brandCode);
          const filteredPY = (cachedData.data.PY as any[])?.filter((row: any) => row.브랜드 === brandCode) ?? [];
          const insights = generateTreemapInsights(filteredCY);
          return NextResponse.json({
            ...cachedData,
            asof_dt: cachedData.asof_dt ?? getYesterday(),
            cached: true,
            cacheKey,
            data: { CY: filteredCY, PY: filteredPY },
            rowCount: { CY: filteredCY.length, PY: filteredPY.length },
            insights
          });
        }
        return NextResponse.json({
          ...cachedData,
          asof_dt: cachedData.asof_dt ?? getYesterday(),
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
    
    // 2. 업데이트 일자 변환 (YYYYMMDD -> YYYY-MM-DD) 및 기준일 계산 (할인내역과 동일 로직)
    let updateDate: string | undefined;
    if (updateDateParam && updateDateParam.length === 8) {
      updateDate = `${updateDateParam.slice(0, 4)}-${updateDateParam.slice(4, 6)}-${updateDateParam.slice(6, 8)}`;
    }
    const asof_dt = analysisMonth ? calculateAsofDate(analysisMonth, updateDate) : getYesterday();
    console.log(`[매출구성 API] 기준일:`, { analysisMonth, updateDate, updateDateParam, asof_dt });
    
    // 3. Snowflake 쿼리 실행
    const query = getSalesCompositionQuery(asof_dt);
    console.log(`[매출구성 API] 쿼리 실행 시작...`);
    const startTime = Date.now();
    const rows = await executeSnowflakeQuery(query);
    const elapsed = Date.now() - startTime;
    console.log(`[매출구성 API] 쿼리 완료 (${elapsed}ms, ${rows.length}행)`);
    
    // 4. 데이터를 당년/전년으로 분리 (캐시에는 항상 전체 데이터 저장 → 브랜드별 인사이트 정확도 보장)
    const cyDataFull = rows.filter(row => row.구분 === 'CY');
    const pyDataFull = rows.filter(row => row.구분 === 'PY');
    
    // 5. 캐시 저장용: 전체 데이터 기준 인사이트로 캐시 (브랜드 요청 시 캐시 히트에서 필터링 후 재계산)
    const insightsFull = generateTreemapInsights(cyDataFull);
    const resultForCache = {
      success: true,
      date: getToday(),
      asof_dt: asof_dt ?? getYesterday(),
      data: { CY: cyDataFull, PY: pyDataFull },
      rowCount: { CY: cyDataFull.length, PY: pyDataFull.length },
      insights: insightsFull,
      cached: false
    };
    await setCache(cacheKey, resultForCache, 86400);
    console.log(`[매출구성 API] 캐시 저장 완료: ${cacheKey} (전체 데이터)`);
    
    // 6. 응답: 브랜드 지정 시 해당 브랜드만 필터링 후 인사이트 재계산
    if (brandCode) {
      const cyData = cyDataFull.filter(row => row.브랜드 === brandCode);
      const pyData = pyDataFull.filter(row => row.브랜드 === brandCode);
      console.log(`[매출구성 API] 브랜드 필터링: ${brandCode}, 필터링 후 행 수: ${cyData.length}`);
      const insights = generateTreemapInsights(cyData);
      return NextResponse.json({
        ...resultForCache,
        data: { CY: cyData, PY: pyData },
        rowCount: { CY: cyData.length, PY: pyData.length },
        insights
      });
    }
    
    return NextResponse.json(resultForCache);
    
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
 * @param asofDate 기준일 (YYYY-MM-DD) - null이면 Snowflake CURRENT_DATE 사용
 */
function getSalesCompositionQuery(asofDate: string | null): string {
  // 분석월이 지정되지 않으면 Snowflake의 어제 날짜 사용
  const dateLogic = asofDate 
    ? `'${asofDate}'::DATE AS asof_dt   -- 파라미터로 받은 기준일`
    : `DATEADD(DAY, -1, CURRENT_DATE())::DATE AS asof_dt   -- Snowflake 어제 날짜`;
  
  return `
WITH params AS (
    SELECT
        ${dateLogic}
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
/* ✅ 현재시즌 + 차시즌(Next Season) 계산 */
season_ref AS (
    SELECT
        gubun,
        /* 현재 시즌 연도(YY): 1~2월은 직전년도 FW를 현재시즌으로 */
        TO_CHAR(
            CASE
                WHEN MONTH(dt_to) BETWEEN 3 AND 12 THEN dt_to
                ELSE DATEADD(YEAR, -1, dt_to)
            END
        , 'YY') AS cur_yy,
        /* 현재 시즌 코드: 3~8월=S, 그 외=F */
        CASE
            WHEN MONTH(dt_to) BETWEEN 3 AND 8 THEN 'S'
            ELSE 'F'
        END AS cur_code,
        /* ✅ 차시즌(Next) 연도(YY)
           - 현재가 F면 next는 다음해 S
           - 현재가 S면 next는 같은해 F
        */
        CASE
            WHEN (CASE WHEN MONTH(dt_to) BETWEEN 3 AND 8 THEN 'S' ELSE 'F' END) = 'F'
                THEN LPAD(
                        TO_VARCHAR(
                            MOD(
                                TO_NUMBER(
                                    TO_CHAR(
                                        CASE
                                            WHEN MONTH(dt_to) BETWEEN 3 AND 12 THEN dt_to
                                            ELSE DATEADD(YEAR, -1, dt_to)
                                        END
                                    , 'YY')
                                ) + 1
                            , 100)
                        )
                     , 2, '0')
            ELSE TO_CHAR(
                    CASE
                        WHEN MONTH(dt_to) BETWEEN 3 AND 12 THEN dt_to
                        ELSE DATEADD(YEAR, -1, dt_to)
                    END
                , 'YY')
        END AS next_yy,
        /* ✅ 차시즌(Next) 코드 */
        CASE
            WHEN (CASE WHEN MONTH(dt_to) BETWEEN 3 AND 8 THEN 'S' ELSE 'F' END) = 'F'
                THEN 'S'
            ELSE 'F'
        END AS next_code
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
    /* ✅ 카테고리 */
    CASE
        /* 1) ACC는 시즌 분류 대상 아님 */
        WHEN b.PARENT_PRDT_KIND_NM = 'ACC' THEN
            CASE b.PRDT_KIND_NM
                WHEN 'Bag'      THEN '가방'
                WHEN 'Shoes'    THEN '신발'
                WHEN 'Headwear' THEN '모자'
                WHEN 'Acc_etc'  THEN '기타ACC'
                ELSE b.PRDT_KIND_NM
            END
        /* 2) 의류: 당시즌 / 차시즌 / 과시즌 */
        ELSE
            CASE
                /* ✅ N 시즌: 현재연도면 당시즌, 미래연도면 차시즌, 과거면 과시즌 */
                WHEN SUBSTR(d.SESN, 1, 3) LIKE '%N%' THEN
                    CASE
                        WHEN TO_NUMBER(SUBSTR(d.SESN, 1, 2)) = TO_NUMBER(sr.cur_yy) THEN '당시즌의류'
                        WHEN TO_NUMBER(SUBSTR(d.SESN, 1, 2)) > TO_NUMBER(sr.cur_yy) THEN '차시즌의류'
                        ELSE '과시즌의류'
                    END
                /* ✅ 일반 시즌: (YY + S/F) */
                ELSE
                    CASE
                        /* 당시즌: cur_yy + cur_code */
                        WHEN SUBSTR(d.SESN, 1, 2) = sr.cur_yy
                         AND RIGHT(d.SESN, 1) = sr.cur_code
                            THEN '당시즌의류'
                        /* ✅ 차시즌: (next_yy + next_code) OR (미래연도 yy > cur_yy) */
                        WHEN (
                                SUBSTR(d.SESN, 1, 2) = sr.next_yy
                            AND RIGHT(d.SESN, 1) = sr.next_code
                             )
                             OR (
                                TO_NUMBER(SUBSTR(d.SESN, 1, 2)) > TO_NUMBER(sr.cur_yy)
                             )
                            THEN '차시즌의류'
                        /* 그 외: 과시즌 */
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
    sr.cur_code,
    sr.next_yy,
    sr.next_code
HAVING (SUM(d.TAG_SALES) + SUM(d.REAL_SALES)) <> 0
`;
}

/**
 * 트리맵 인사이트 생성 함수
 * 판매비중이 가장 높은 채널과 아이템에 대한 핵심인사이트 생성
 */
function generateTreemapInsights(cyData: any[]): {
  topChannel: { name: string; sales: number; share: number; insight: string } | null;
  topItem: { name: string; sales: number; share: number; insight: string } | null;
} {
  if (!cyData || cyData.length === 0) {
    return { topChannel: null, topItem: null };
  }

  // 채널별 매출 집계
  const channelSales: { [key: string]: number } = {};
  let totalSales = 0;

  cyData.forEach(row => {
    const channel = row.채널 || '';
    const sales = parseFloat(row.실판매출 || 0);
    if (channel && sales > 0) {
      channelSales[channel] = (channelSales[channel] || 0) + sales;
      totalSales += sales;
    }
  });

  // 가장 높은 채널 찾기
  let topChannel: { name: string; sales: number; share: number; insight: string } | null = null;
  if (Object.keys(channelSales).length > 0 && totalSales > 0) {
    const sortedChannels = Object.entries(channelSales)
      .map(([name, sales]) => ({ name, sales: sales as number }))
      .sort((a, b) => b.sales - a.sales);
    
    if (sortedChannels.length > 0) {
      const top = sortedChannels[0];
      const share = (top.sales / totalSales) * 100;
      const salesInBillions = top.sales / 100000000; // 억원 단위
      
      topChannel = {
        name: top.name,
        sales: top.sales,
        share: share,
        insight: `<strong>${top.name}</strong>이(가) 전체 매출의 <strong>${share.toFixed(1)}%</strong>를 차지하며 가장 큰 비중을 보이고 있습니다. (${salesInBillions.toFixed(1)}억원)`
      };
    }
  }

  // 아이템별 매출 집계
  const itemSales: { [key: string]: number } = {};
  let totalItemSales = 0;

  cyData.forEach(row => {
    const item = row.아이템 || '';
    const sales = parseFloat(row.실판매출 || 0);
    if (item && sales > 0) {
      itemSales[item] = (itemSales[item] || 0) + sales;
      totalItemSales += sales;
    }
  });

  // 가장 높은 아이템 찾기
  let topItem: { name: string; sales: number; share: number; insight: string } | null = null;
  if (Object.keys(itemSales).length > 0 && totalItemSales > 0) {
    const sortedItems = Object.entries(itemSales)
      .map(([name, sales]) => ({ name, sales: sales as number }))
      .sort((a, b) => b.sales - a.sales);
    
    if (sortedItems.length > 0) {
      const top = sortedItems[0];
      const share = (top.sales / totalItemSales) * 100;
      const salesInBillions = top.sales / 100000000; // 억원 단위
      
      topItem = {
        name: top.name,
        sales: top.sales,
        share: share,
        insight: `<strong>${top.name}</strong>이(가) 전체 매출의 <strong>${share.toFixed(1)}%</strong>를 차지하며 가장 큰 비중을 보이고 있습니다. (${salesInBillions.toFixed(1)}억원)`
      };
    }
  }

  return { topChannel, topItem };
}
