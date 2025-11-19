import { NextResponse } from 'next/server';
import { readFile, readdir, stat } from 'fs/promises';
import { join } from 'path';
import { existsSync } from 'fs';

/**
 * 날짜별 데이터 파일 로드 및 트리맵 데이터 생성
 * GET /api/load-data-by-date?date=20251117
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const dateParam = searchParams.get('date'); // YYYYMMDD 형식
    
    if (!dateParam || dateParam.length !== 8) {
      return NextResponse.json(
        { success: false, error: '날짜 형식이 올바르지 않습니다. (YYYYMMDD 형식 필요)' },
        { status: 400 }
      );
    }
    
    const rawDir = join(process.cwd(), 'raw');
    const dateFolder = join(rawDir, dateParam);
    
    // 날짜 폴더 존재 확인
    if (!existsSync(dateFolder)) {
      return NextResponse.json(
        { 
          success: false, 
          error: `해당 날짜(${dateParam})의 데이터 폴더를 찾을 수 없습니다.`,
          suggestion: '먼저 전처리 스크립트를 실행하여 데이터를 생성해주세요.'
        },
        { status: 404 }
      );
    }
    
    // 날짜 폴더 내 파일 목록 확인
    const files = await readdir(dateFolder);
    const processedFile = files.find(f => f.endsWith('_전처리완료.csv'));
    
    if (!processedFile) {
      return NextResponse.json(
        { 
          success: false, 
          error: `해당 날짜 폴더에 전처리 완료 파일이 없습니다.`,
          folder: dateParam,
          files: files
        },
        { status: 404 }
      );
    }
    
    const filePath = join(dateFolder, processedFile);
    
    // 파일 정보
    const fileStats = await stat(filePath);
    
    return NextResponse.json({
      success: true,
      date: dateParam,
      folder: dateParam,
      filename: processedFile,
      filePath: filePath,
      fileSize: fileStats.size,
      message: '데이터 파일을 찾았습니다. 트리맵 데이터 생성을 실행해주세요.'
    });
    
  } catch (error: any) {
    console.error('날짜별 데이터 로드 오류:', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
