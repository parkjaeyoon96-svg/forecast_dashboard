import { NextResponse } from 'next/server';
import { Redis } from '@upstash/redis';
import { promises as fs } from 'fs';
import path from 'path';
import snowflake from 'snowflake-sdk';

/**
 * ACC 재고주수 분석 데이터 조회 API
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
 * Snowflake 쿼리 실행 (Node.js SDK 사용)
 */
async function querySnowflake(): Promise<any> {
  console.log('[Snowflake 조회 시작 - 재고주수]');
  
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

      console.log('[Snowflake 연결 성공 - 재고주수]');

      // 쿼리 실행
      conn.execute({
        sqlText: getStockWeeksQuery(),
        complete: (err, stmt, rows) => {
          // 연결 종료
          connection.destroy((err) => {
            if (err) {
              console.error('[Snowflake 연결 종료 실패]', err);
            }
          });

          if (err) {
            console.error('[Snowflake 쿼리 실패 - 재고주수]', err);
            reject(new Error(`쿼리 실행 실패: ${err.message}`));
            return;
          }

          try {
            // 데이터를 당년/전년으로 분리
            const allData = rows || [];
            const cyData = allData.filter((row: any) => row.YY === 'CY');
            const pyData = allData.filter((row: any) => row.YY === 'PY');

            // 기준일 추출
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

            console.log(`[Snowflake 조회 완료 - 재고주수] CY: ${cyData.length}, PY: ${pyData.length}`);
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
    const cacheKey = `stock-weeks-${today}`;
    
    // 1. Redis 캐시 확인
    let cachedData = await getFromRedis(cacheKey);
    if (cachedData) {
      console.log(`[재고주수 캐시 반환] 데이터 구조:`, {
        success: cachedData.success,
        hasData: !!cachedData.data,
        hasCY: !!cachedData.data?.CY,
        hasPY: !!cachedData.data?.PY,
        CYcount: cachedData.data?.CY?.length || 0,
        PYcount: cachedData.data?.PY?.length || 0
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
    console.error('[재고주수 API 에러]', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}




