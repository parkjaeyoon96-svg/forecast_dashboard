import { NextResponse } from 'next/server';
import { readdir, stat } from 'fs/promises';
import { join } from 'path';

/**
 * public/data 폴더에서 JSON 파일이 있는 날짜 목록 반환
 */
export async function GET() {
  try {
    const dataDir = join(process.cwd(), 'public', 'data');
    
    // public/data 폴더가 없으면 빈 배열 반환
    try {
      const dataDirStats = await stat(dataDir);
      if (!dataDirStats.isDirectory()) {
        return NextResponse.json({
          success: true,
          dates: [],
          dateFileMap: {}
        });
      }
    } catch (e) {
      return NextResponse.json({
        success: true,
        dates: [],
        dateFileMap: {}
      });
    }
    
    const files = await readdir(dataDir);
    const dateMap = new Map<string, string>(); // 날짜 -> 폴더명 매핑
    
    // public/data 폴더 내의 날짜별 폴더 찾기 (YYYYMMDD 형식)
    for (const item of files) {
      const itemPath = join(dataDir, item);
      try {
        const stats = await stat(itemPath);
        if (stats.isDirectory() && /^\d{8}$/.test(item)) {
          // 날짜 폴더 (YYYYMMDD 형식)
          // overview.json 파일이 있는지 확인 (필수 파일)
          const dateFolderFiles = await readdir(itemPath);
          const hasOverviewJson = dateFolderFiles.some(file => file === 'overview.json');
          
          if (hasOverviewJson) {
            const dateStr = item; // 폴더명이 날짜 (YYYYMMDD)
            const formatted = `${dateStr.slice(0, 4)}.${dateStr.slice(4, 6)}.${dateStr.slice(6, 8)}`;
            dateMap.set(formatted, item);
          }
        }
      } catch (e) {
        // 파일이거나 접근 불가능한 경우 무시
      }
    }
    
    const dates = Array.from(dateMap.keys());
    const sortedDates = Array.from(dates).sort().reverse(); // 최신순
    
    return NextResponse.json({
      success: true,
      dates: sortedDates,
      dateFileMap: Object.fromEntries(
        sortedDates.map(date => [date, dateMap.get(date) || ''])
      )
    });
  } catch (error: any) {
    console.error('날짜 목록 조회 오류:', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

