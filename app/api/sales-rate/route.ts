import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';
import { getTodayCompact, getToday, getYesterday, formatDate } from '@/lib/dateUtils';

/**
 * 시즌 정보 인터페이스
 */
interface SeasonInfo {
  curSeason: string;
  pySeason: string;
  curAsofDt: string;    // 시즌 마감일 고려한 기준일
  pyAsofDt: string;
  pyEndDt: string;      // SQL 형식
}

/**
 * 현재 날짜 기준으로 시즌 자동 감지
 * 규칙:
 * - 3월~8월: 해당 연도 S 시즌 (26년 3월 → 26S)
 * - 9월~12월: 해당 연도 F 시즌 (25년 10월 → 25F)
 * - 1월~2월: 전년도 F 시즌 (26년 1월 → 25F)
 */
function getCurrentSeason(): string {
  const now = new Date();
  const month = now.getMonth() + 1; // 1-12
  const year = now.getFullYear();
  
  if (month >= 3 && month <= 8) {
    // S 시즌 (3월~8월): 해당 연도
    const seasonYear = year % 100; // 2026 -> 26
    return `${seasonYear.toString().padStart(2, '0')}S`;
  } else if (month >= 9) {
    // F 시즌 (9월~12월): 해당 연도
    const seasonYear = year % 100; // 2025 -> 25
    return `${seasonYear.toString().padStart(2, '0')}F`;
  } else {
    // F 시즌 (1월~2월): 전년도
    const seasonYear = (year - 1) % 100; // 2026 -> 25
    return `${seasonYear.toString().padStart(2, '0')}F`;
  }
}

/**
 * 25F부터 현재 시즌까지 시즌 리스트 생성
 * 예: 2026-03-05 → ['25F', '26S']
 * 예: 2026-09-01 → ['25F', '26S', '26F']
 */
function generateSeasonList(): string[] {
  const seasons: string[] = [];
  const currentSeason = getCurrentSeason();
  
  // 시작 시즌: 25F
  let year = 25;
  let type: 'F' | 'S' = 'F';
  
  while (true) {
    const season = `${year.toString().padStart(2, '0')}${type}`;
    seasons.push(season);
    
    // 현재 시즌에 도달하면 종료
    if (season === currentSeason) {
      break;
    }
    
    // 다음 시즌으로 이동
    if (type === 'F') {
      type = 'S';
      year++; // F -> S로 넘어가면 연도 증가
    } else {
      type = 'F';
    }
  }
  
  return seasons;
}

/**
 * 시즌 정보 계산
 */
function getSeasonInfo(selectedSeason: string): SeasonInfo {
  const year = parseInt(selectedSeason.substring(0, 2));
  const type = selectedSeason.substring(2); // 'F' or 'S'
  const fullYear = 2000 + year;
  
  // 전년 시즌
  const pySeason = `${(year - 1).toString().padStart(2, '0')}${type}`;
  
  // 시즌별 마감일
  let seasonEndMonth: number, seasonEndDay: number;
  if (type === 'F') {
    // F 시즌: 다음 해 2월 28일
    seasonEndMonth = 2;
    seasonEndDay = 28;
  } else if (type === 'S') {
    // S 시즌: 당해 8월 31일
    seasonEndMonth = 8;
    seasonEndDay = 31;
  } else {
    throw new Error(`Unknown season type: ${type}`);
  }
  
  // 당년 기준일: MIN(한국시간 어제, 시즌마감일) — 트리맵·재고주수와 동일하게 KST 기준
  const yesterdayStr = getYesterday(); // YYYY-MM-DD (KST)
  const yesterday = new Date(yesterdayStr + 'T12:00:00.000Z'); // UTC 정오로 서버 타임존 영향 제거

  // F 시즌은 다음 해 2월이 마감이므로
  const seasonEndYear = type === 'F' ? fullYear + 1 : fullYear;
  const seasonEndDate = new Date(seasonEndYear, seasonEndMonth - 1, seasonEndDay);
  const useYesterday = yesterday <= seasonEndDate;
  const curAsofDt = useYesterday ? yesterdayStr : formatDateForSql(seasonEndDate);

  // 전년 기준일: 당년 기준일 - 1년 (문자열에서 파싱해 타임존 일관)
  const [cyY, cyM, cyD] = curAsofDt.split('-').map(Number);
  const pyAsofDt = `${cyY - 1}-${String(cyM).padStart(2, '0')}-${String(cyD).padStart(2, '0')}`;

  // 전년 마감일
  const pyEndYear = type === 'F' ? fullYear : fullYear - 1;
  const pyEndDate = new Date(pyEndYear, seasonEndMonth - 1, seasonEndDay);

  return {
    curSeason: selectedSeason,
    pySeason: pySeason,
    curAsofDt,
    pyAsofDt,
    pyEndDt: `DATE_FROM_PARTS(${pyEndDate.getFullYear()}, ${pyEndDate.getMonth() + 1}, ${pyEndDate.getDate()})`
  };
}

/**
 * SQL용 날짜 포맷
 */
function formatDateForSql(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * 당시즌 의류 판매율 분석 데이터 조회 API
 * 
 * GET /api/sales-rate
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - seasons: string[] (시즌 리스트)
 * - currentSeason: string (현재 시즌)
 * - dataBySeasons: { [season]: { periodInfo, data, rowCount } }
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
    
    // 오늘 날짜로 캐시 키 생성 (한국 시간 기준)
    const today = getTodayCompact();
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
    // 2. 모든 시즌 데이터를 한 번에 조회
    const query = getSalesRateQueryAllSeasons();
    const rows = await executeSnowflakeQuery(query);
    
    // 3. 시즌별로 데이터 그룹화
    const seasonList = generateSeasonList();
    const dataBySeasons: any = {};
    
    seasonList.forEach(season => {
      // 해당 시즌의 데이터만 필터링
      const seasonRows = rows.filter(row => row.SEASON_ID === season);
      
      // 기간별로 분리
      const curData = seasonRows.filter(row => row.PERIOD_GB === 'CUR');
      const pyData = seasonRows.filter(row => row.PERIOD_GB === 'PY');
      const pyEndData = seasonRows.filter(row => row.PERIOD_GB === 'PY_END');
      
      // 기간 정보 추출
      const curDate = curData.length > 0 ? curData[0].ASOF_DT : '';
      const pyDate = pyData.length > 0 ? pyData[0].ASOF_DT : '';
      const pyEndDate = pyEndData.length > 0 ? pyEndData[0].ASOF_DT : '';
      
      dataBySeasons[season] = {
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
        }
      };
    });
    
    // 4. 결과 구성
    const result = {
      success: true,
      date: getToday(), // 한국 시간 기준
      seasons: seasonList,
      currentSeason: getCurrentSeason(),
      dataBySeasons: dataBySeasons,
      cached: false
    };
    
    // 5. Redis 캐시에 저장 (24시간)
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
 * 판매율 분석 Snowflake 쿼리 생성 (모든 시즌 동시 조회)
 */
function getSalesRateQueryAllSeasons(): string {
  const seasons = generateSeasonList();
  console.log('[판매율 API] 조회할 시즌:', seasons);
  
  // 각 시즌별 쿼리를 생성하고 UNION ALL로 연결
  const seasonQueries = seasons.map(season => {
    const seasonInfo = getSeasonInfo(season);
    
    return `
-- ========== ${season} 시즌 데이터 ==========
SELECT
    '${season}' AS SEASON_ID,
    ASOF_DT,
    PERIOD_GB,
    BRD_CD,
    SESN,
    PRDT_CD,
    PRDT_KIND_NM,
    ITEM_CD,
    ITEM_NM,
    PRDT_NM,
    AC_ORD_QTY_KOR,
    AC_ORD_TAG_AMT_KOR,
    AC_STOR_QTY_KOR,
    AC_STOR_TAG_AMT_KOR,
    SALE_QTY,
    SALE_TAG,
    SALE_AMT,
    STOCK_QTY,
    STOCK_TAG_AMT
FROM (
  WITH PARAM AS (
    SELECT
        TO_DATE('${seasonInfo.curAsofDt}')        AS ASOF_DT
      , TO_DATE('${seasonInfo.pyAsofDt}')         AS ASOF_DT_PY
      , '${seasonInfo.curSeason}'                 AS CUR_SESN
      , '${seasonInfo.pySeason}'                  AS PY_SESN
      , ${seasonInfo.pyEndDt}                     AS PY_END_DT
  ),
  
  BASE AS (
    /* 1) 당년 시즌 스냅샷 */
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
      , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
      , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
      , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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

    /* 2) 전년 시즌 스냅샷 */
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
      , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
      , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
      , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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

    /* 3) 전년마감 시즌 스냅샷 */
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
      , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
      , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
      , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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
    , SUM(SALE_AMT)            AS SALE_AMT
    , SUM(STOCK_QTY)           AS STOCK_QTY
    , SUM(STOCK_TAG_AMT)       AS STOCK_TAG_AMT
  FROM BASE
  GROUP BY
      ASOF_DT, PERIOD_GB, BRD_CD, PRDT_CD
  HAVING
      COALESCE(SUM(AC_ORD_TAG_AMT_KOR), 0)
    + COALESCE(SUM(AC_STOR_TAG_AMT_KOR), 0)
    + COALESCE(SUM(SALE_TAG), 0)
    + COALESCE(SUM(STOCK_TAG_AMT), 0) <> 0
  ORDER BY
      BRD_CD, PRDT_CD, PERIOD_GB, ASOF_DT
)`;
  });
  
  // UNION ALL로 모든 시즌 연결
  return seasonQueries.join('\nUNION ALL\n');
}

/**
 * 판매율 분석 Snowflake 쿼리 생성 (단일 시즌 - 하위 호환성)
 */
function getSalesRateQuery(selectedSeason?: string | null): string {
  let paramBlock: string;
  
  if (selectedSeason) {
    // 시즌이 지정된 경우: 해당 시즌을 당년으로 설정
    const seasonInfo = getSeasonInfo(selectedSeason);
    
    paramBlock = `
WITH PARAM AS (
  SELECT
      TO_DATE('${seasonInfo.curAsofDt}')        AS ASOF_DT
    , TO_DATE('${seasonInfo.pyAsofDt}')         AS ASOF_DT_PY
    , '${seasonInfo.curSeason}'                 AS CUR_SESN
    , '${seasonInfo.pySeason}'                  AS PY_SESN
    , ${seasonInfo.pyEndDt}                     AS PY_END_DT
),`;
  } else {
    // 자동 계산 (현재 시즌)
    const currentSeason = getCurrentSeason();
    const seasonInfo = getSeasonInfo(currentSeason);
    
    paramBlock = `
WITH PARAM AS (
  SELECT
      TO_DATE('${seasonInfo.curAsofDt}')        AS ASOF_DT
    , TO_DATE('${seasonInfo.pyAsofDt}')         AS ASOF_DT_PY
    , '${seasonInfo.curSeason}'                 AS CUR_SESN
    , '${seasonInfo.pySeason}'                  AS PY_SESN
    , ${seasonInfo.pyEndDt}                     AS PY_END_DT
),`;
  }
  
  // 나머지 쿼리는 동일
  return paramBlock + `
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
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
    , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
    , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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
    , (a.AC_SALE_NML_QTY_CNS + a.AC_SALE_RET_QTY_CNS)               AS SALE_QTY
    , (a.AC_SALE_NML_TAG_AMT_CNS + a.AC_SALE_RET_TAG_AMT_CNS)       AS SALE_TAG
    , (a.AC_SALE_NML_SALE_AMT_CNS + a.AC_SALE_RET_SALE_AMT_CNS)     AS SALE_AMT
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
  , SUM(SALE_AMT)            AS SALE_AMT
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

// formatDate 함수는 @/lib/dateUtils에서 import
