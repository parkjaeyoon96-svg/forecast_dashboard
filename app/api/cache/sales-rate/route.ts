import { NextResponse } from 'next/server';
import { getCache, deleteCache } from '@/lib/redis';
import { getTodayCompact } from '@/lib/dateUtils';

/**
 * 판매율 캐시 관리 API
 * 
 * GET /api/cache/sales-rate - 현재 캐시 확인
 * DELETE /api/cache/sales-rate - 오늘 캐시 삭제
 */

export async function GET(request: Request) {
  try {
    const today = getTodayCompact();
    const cacheKey = `sales-rate-${today}`;
    
    const cachedData = await getCache<any>(cacheKey);
    
    if (cachedData) {
      return NextResponse.json({
        success: true,
        cacheKey,
        hasCache: true,
        cacheInfo: {
          success: cachedData.success,
          date: cachedData.date,
          seasons: cachedData.seasons,
          currentSeason: cachedData.currentSeason,
          dataBySeasons: cachedData.dataBySeasons ? Object.keys(cachedData.dataBySeasons) : [],
          cached: cachedData.cached
        }
      });
    } else {
      return NextResponse.json({
        success: true,
        cacheKey,
        hasCache: false,
        message: '캐시가 없습니다.'
      });
    }
  } catch (error: any) {
    console.error('[캐시 관리 API] 조회 에러:', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

export async function DELETE(request: Request) {
  try {
    const today = getTodayCompact();
    const cacheKey = `sales-rate-${today}`;
    
    const deleted = await deleteCache(cacheKey);
    
    return NextResponse.json({
      success: true,
      cacheKey,
      deleted,
      message: deleted ? '캐시가 삭제되었습니다.' : '캐시 삭제에 실패했습니다.'
    });
  } catch (error: any) {
    console.error('[캐시 관리 API] 삭제 에러:', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
