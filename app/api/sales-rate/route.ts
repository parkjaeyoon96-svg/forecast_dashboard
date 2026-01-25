import { NextResponse } from 'next/server';
import { Redis } from '@upstash/redis';
import { promises as fs } from 'fs';
import path from 'path';
import snowflake from 'snowflake-sdk';

/**
 * 판매율 분석 데이터 조회 API
 * 
 * 캐시 전략 (우선순위):
 * 1. Redis (Upstash) - 프로덕션용
 * 2. 로컬 파일 캐시 - 개발/폴백용
 * 3. Snowflake 쿼리 - 캐시 미스 시
 */

// Redis 클라이언트 (환경 변수가 있을 때만)
const redis = process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN
  ? new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    })
  : null;

// 로컬 캐시 디렉토리
const CACHE_DIR = path.join(process.cwd(), '.cache');

/**
 * Redis에서 캐시 조회
 */
async function getFromRedis(key: string): Promise<any | null> {
  if (!redis) return null;
  
  try {
    const cached = await redis.get(key);
    if (cached) {
      console.log(`[Redis 캐시 적중] ${key}`);
      return cached;
    }
  } catch (error) {
    console.error('[Redis 조회 실패]', error);
  }
  
  return null;
}

/**
 * Redis에 캐시 저장 (24시간 TTL)
 */
async function setToRedis(key: string, data: any): Promise<void> {
  if (!redis) return;
  
  try {
    // 24시간 TTL (86400초)
    await redis.setex(key, 86400, JSON.stringify(data));
    console.log(`[Redis 캐시 저장 완료] ${key}`);
  } catch (error) {
    console.error('[Redis 저장 실패]', error);
  }
}

/**
 * 로컬 파일에서 캐시 조회
 */
async function getFromFileCache(key: string): Promise<any | null> {
  try {
    const filePath = path.join(CACHE_DIR, `${key}.json`);
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const cached = JSON.parse(fileContent);
    
    // 캐시 만료 확인 (24시간)
    const cacheTime = new Date(cached.cachedAt).getTime();
    const now = Date.now();
    const hoursDiff = (now - cacheTime) / (1000 * 60 * 60);
      
    if (hoursDiff < 24) {
      console.log(`[파일 캐시 적중] ${key} (${hoursDiff.toFixed(1)}시간 전)`);
      return cached.data;
    } else {
      console.log(`[파일 캐시 만료] ${key}`);
      // 만료된 캐시 삭제
      await fs.unlink(filePath).catch(() => {});
      }
    } catch (error) {
    // 파일이 없거나 읽기 실패
  }
  
  return null;
}

/**
 * 로컬 파일에 캐시 저장
 */
async function setToFileCache(key: string, data: any): Promise<void> {
  try {
    // 캐시 디렉토리 생성
    await fs.mkdir(CACHE_DIR, { recursive: true });
    
    const filePath = path.join(CACHE_DIR, `${key}.json`);
    const cacheData = {
      cachedAt: new Date().toISOString(),
      data: data
    };
    
    await fs.writeFile(filePath, JSON.stringify(cacheData, null, 2), 'utf-8');
    console.log(`[파일 캐시 저장 완료] ${key}`);
  } catch (error) {
    console.error('[파일 캐시 저장 실패]', error);
  }
}

/**
 * Snowflake 쿼리 생성
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
  JOIN PRCS.DB_PRDT b
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
  JOIN PRCS.DB_PRDT b
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
  JOIN PRCS.DB_PRDT b
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
 * Snowflake 쿼리 실행 (Node.js SDK 사용)
 */
async function querySnowflake(): Promise<any> {
  console.log('[Snowflake 조회 시작]');
  
  return new Promise((resolve, reject) => {
    // Snowflake 연결 설정
    const connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USERNAME!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
      database: process.env.SNOWFLAKE_DATABASE!,
    });

    // 연결
    connection.connect((err, conn) => {
      if (err) {
        console.error('[Snowflake 연결 실패]', err);
        reject(new Error(`Snowflake 연결 실패: ${err.message}`));
        return;
      }

      console.log('[Snowflake 연결 성공]');

      // 쿼리 실행
      conn.execute({
        sqlText: getSalesRateQuery(),
        complete: (err, stmt, rows) => {
          // 연결 종료
          connection.destroy((err) => {
            if (err) {
              console.error('[Snowflake 연결 종료 실패]', err);
            }
          });

          if (err) {
            console.error('[Snowflake 쿼리 실패]', err);
            reject(new Error(`쿼리 실행 실패: ${err.message}`));
            return;
          }

          try {
            // 데이터를 기간별로 분리
            const allData = rows || [];
            const curData = allData.filter((row: any) => row.PERIOD_GB === 'CUR');
            const pyData = allData.filter((row: any) => row.PERIOD_GB === 'PY');
            const pyEndData = allData.filter((row: any) => row.PERIOD_GB === 'PY_END');

            // 기간 정보 추출
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

            console.log(`[Snowflake 조회 완료] CUR: ${curData.length}, PY: ${pyData.length}, PY_END: ${pyEndData.length}`);
            resolve(result);
          } catch (e: any) {
            reject(new Error(`결과 처리 실패: ${e.message}`));
          }
        }
      });
    });
  });
}

export async function GET(request: Request) {
  try {
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `sales-rate-${today}`;
    
    // 1. Redis 캐시 확인
    let cachedData = await getFromRedis(cacheKey);
    if (cachedData) {
      console.log(`[캐시 반환] 데이터 구조:`, {
        success: cachedData.success,
        hasData: !!cachedData.data,
        hasCUR: !!cachedData.data?.CUR,
        hasPY: !!cachedData.data?.PY,
        hasPY_END: !!cachedData.data?.PY_END,
        CURcount: cachedData.data?.CUR?.length || 0,
        sampleData: cachedData.data?.CUR?.[0]
      });
      return NextResponse.json(cachedData);
    }
    
    // 2. 로컬 파일 캐시 확인
    cachedData = await getFromFileCache(cacheKey);
    if (cachedData) {
      // Redis에도 저장 (프로덕션 배포 시 다른 인스턴스에서도 사용 가능)
      await setToRedis(cacheKey, cachedData);
      return NextResponse.json(cachedData);
    }
    
    // 3. 캐시 미스 - Snowflake 쿼리 실행
    console.log(`[캐시 미스] ${cacheKey} - 새로 조회합니다`);
    const result = await querySnowflake();
    
    // 4. 캐시 저장 (Redis와 파일 모두)
    await Promise.all([
      setToRedis(cacheKey, result),
      setToFileCache(cacheKey, result)
    ]);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[API 에러]', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
