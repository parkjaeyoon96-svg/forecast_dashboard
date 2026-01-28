import { NextResponse } from 'next/server';
import { executeSnowflakeQuery } from '@/lib/snowflake';
import { getCache, setCache } from '@/lib/redis';
import { getTodayCompact, getToday, formatDate } from '@/lib/dateUtils';

/**
 * 프로모션(할인) 내용 데이터 조회 API
 * 
 * GET /api/promotion?brand=M
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - brandCode: string (브랜드 코드)
 * - data: 프로모션 원본 데이터
 * - rowCount: 전체 데이터 개수
 * 
 * 캐싱 전략:
 * - Redis 캐시 (24시간 TTL)
 * - 키: promotion-{브랜드코드}-YYYYMMDD
 * - 날짜가 바뀌면 자동으로 새로운 캐시 키 생성
 */
export async function GET(request: Request) {
  try {
    // URL 파라미터에서 brand, forceUpdate 확인
    const { searchParams } = new URL(request.url);
    const brandCode = searchParams.get('brand');
    const forceUpdate = searchParams.get('forceUpdate') === 'true';
    
    // 브랜드 코드 필수 확인
    if (!brandCode) {
      return NextResponse.json(
        { success: false, error: '브랜드 코드가 필요합니다.' },
        { status: 400 }
      );
    }
    
    // 날짜별 캐시 키 생성 (한국 시간 기준)
    // 오늘 날짜를 캐시 키에 포함하여 날짜가 바뀌면 자동 갱신
    const today = getTodayCompact();
    const cacheKey = `promotion-${brandCode}-${today}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        console.log(`[프로모션 API] 캐시 히트: ${cacheKey}`);
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[프로모션 API] 강제 업데이트: ${cacheKey}`);
    }
    
    console.log(`[프로모션 API] 캐시 미스: ${cacheKey} - Snowflake 조회 시작`);
    
    // 2. Snowflake 쿼리 실행
    const query = getPromotionQuery(brandCode);
    const rows = await executeSnowflakeQuery(query);
    
    console.log(`[프로모션 API] Snowflake 조회 완료: ${rows.length}행`);
    if (rows.length > 0) {
      console.log(`[프로모션 API] 첫 번째 행 샘플:`, rows[0]);
      console.log(`[프로모션 API] 첫 번째 행 키:`, Object.keys(rows[0]));
    }
    
    // 3. 결과 구성
    const result = {
      success: true,
      date: getToday(), // 한국 시간 기준
      brandCode,
      data: rows,
      rowCount: rows.length,
      cached: false
    };
    
    // 4. Redis 캐시에 저장 (24시간)
    await setCache(cacheKey, result, 86400);
    console.log(`[프로모션 API] 캐시 저장 완료: ${cacheKey}`);
    
    return NextResponse.json(result);
    
  } catch (error: any) {
    console.error('[프로모션 API] 에러 발생:', error);
    
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
 * 프로모션 Snowflake 쿼리 생성
 * @param brandCode 브랜드 코드 (M, I, X, V, ST, W)
 */
function getPromotionQuery(brandCode: string): string {
  return `
SELECT DISTINCT
    b.brd_cd AS "브랜드코드",
    a.sale_dt_fr AS "시작일자",
    a.rmk AS "사유",
    a.disc_clsby_nm AS "할인율",
    b.prdt_kind_nm AS "카테고리",
    b.item AS "아이템코드",
    b.item_nm AS "아이템명",
    a.prdt_cd AS "품번",
    b.prdt_nm AS "품명",
    a.flat_price AS "TAG가",
    a.sale_price AS "할인가"
FROM FNF.PRCS.DW_PRICE a
JOIN FNF.PRCS.DB_PRDT b
  ON a.prdt_cd = b.prdt_cd
WHERE a.sale_dt_fr BETWEEN
      DATEADD(MONTH, -2, DATE_TRUNC('MONTH', CURRENT_DATE))  -- 3개월 전 1일
  AND CURRENT_DATE
  AND b.brd_cd = '${brandCode}'
ORDER BY a.sale_dt_fr DESC, a.prdt_cd
`;
}
