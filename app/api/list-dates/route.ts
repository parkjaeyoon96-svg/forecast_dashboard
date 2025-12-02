import { NextResponse } from 'next/server';
import { readdir, stat } from 'fs/promises';
import { join } from 'path';

/**
 * raw 폴더에서 사용 가능한 날짜 목록 반환
 */
export async function GET() {
  try {
    const rawDir = join(process.cwd(), 'raw');
    const files = await readdir(rawDir);
    
    // 새 구조: raw/YYYYMM/current_year/YYYYMMDD/ke30_YYYYMMDD_YYYYMM_전처리완료.csv
    const dateMap = new Map<string, string>(); // 날짜 -> 파일명 매핑
    
    // 1. 새 구조에서 찾기: raw/YYYYMM/current_year/YYYYMMDD/
    for (const item of files) {
      const itemPath = join(rawDir, item);
      try {
        const stats = await stat(itemPath);
        if (stats.isDirectory() && /^\d{6}$/.test(item)) {
          // 월별 폴더 (YYYYMM 형식)
          const monthPath = itemPath;
          const currentYearPath = join(monthPath, 'current_year');
          
          try {
            const currentYearStats = await stat(currentYearPath);
            if (currentYearStats.isDirectory()) {
              // current_year 폴더 내의 날짜별 폴더 찾기
              const dateFolders = await readdir(currentYearPath);
              for (const dateFolder of dateFolders) {
                const dateFolderPath = join(currentYearPath, dateFolder);
                try {
                  const dateStats = await stat(dateFolderPath);
                  if (dateStats.isDirectory() && /^\d{8}$/.test(dateFolder)) {
                    // 날짜 폴더 (YYYYMMDD 형식)
                    const dateFiles = await readdir(dateFolderPath);
                    for (const file of dateFiles) {
                      if (file.endsWith('_전처리완료.csv')) {
                        const dateStr = dateFolder; // 폴더명이 날짜
                        const formatted = `${dateStr.slice(0, 4)}.${dateStr.slice(4, 6)}.${dateStr.slice(6, 8)}`;
                        dateMap.set(formatted, join(item, 'current_year', dateFolder, file));
                      }
                    }
                  }
                } catch (e) {
                  // 날짜 폴더 접근 불가능한 경우 무시
                }
              }
            }
          } catch (e) {
            // current_year 폴더가 없는 경우 무시
          }
        }
      } catch (e) {
        // 파일이거나 접근 불가능한 경우 무시
      }
    }
    
    // 2. 기존 구조 호환: raw/YYYYMMDD/ (날짜 폴더 직접)
    for (const item of files) {
      const itemPath = join(rawDir, item);
      try {
        const stats = await stat(itemPath);
        if (stats.isDirectory() && /^\d{8}$/.test(item)) {
          // 날짜 폴더 (YYYYMMDD 형식) - 기존 구조
          const dateFolderFiles = await readdir(itemPath);
          for (const file of dateFolderFiles) {
            if (file.endsWith('_전처리완료.csv')) {
              const dateStr = item; // 폴더명이 날짜
              const formatted = `${dateStr.slice(0, 4)}.${dateStr.slice(4, 6)}.${dateStr.slice(6, 8)}`;
              if (!dateMap.has(formatted)) {
                dateMap.set(formatted, join(item, file));
              }
            }
          }
        }
      } catch (e) {
        // 파일이거나 접근 불가능한 경우 무시
      }
    }
    
    // 3. 루트 폴더에서도 찾기 (기존 방식 호환)
    for (const file of files) {
      const datePattern = /ke30_(\d{8})_\d{6}_전처리완료\.csv/;
      const match = file.match(datePattern);
      if (match) {
        const dateStr = match[1]; // YYYYMMDD
        const formatted = `${dateStr.slice(0, 4)}.${dateStr.slice(4, 6)}.${dateStr.slice(6, 8)}`;
        if (!dateMap.has(formatted)) {
          dateMap.set(formatted, file);
        }
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

