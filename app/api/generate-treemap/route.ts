import { NextResponse } from 'next/server';
import { exec } from 'child_process';
import { promisify } from 'util';
import { join } from 'path';
import { existsSync, statSync } from 'fs';

const execAsync = promisify(exec);

/**
 * 특정 날짜의 트리맵 데이터 생성
 * GET /api/generate-treemap?date=20251117
 * 파일이 이미 존재하고 최근에 생성된 경우 스킵하여 성능 최적화
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
    
    const publicDir = join(process.cwd(), 'public');
    const treemapFile = join(publicDir, `treemap_data_${dateParam}.js`);
    const dataFile = join(publicDir, `data_${dateParam}.js`);
    
    // 원본 전처리 파일 존재 여부 확인
    const rawDir = join(process.cwd(), 'raw');
    const yearMonth = dateParam.slice(0, 6); // YYYYMM
    const dateFolder = join(rawDir, yearMonth, 'current_year', dateParam);
    let sourceFileExists = false;
    
    try {
      if (existsSync(dateFolder)) {
        const { readdir } = await import('fs/promises');
        const files = await readdir(dateFolder);
        sourceFileExists = files.some(f => f.endsWith('_전처리완료.csv'));
      }
    } catch (e) {
      // 폴더 읽기 실패 시 무시
      console.warn(`폴더 읽기 실패: ${dateFolder}`, e);
    }
    
    // 파일이 이미 존재하는지 확인 (존재하면 스킵 - 이전 데이터 조회 시 재계산 방지)
    let shouldGenerate = true;
    if (existsSync(treemapFile) && existsSync(dataFile)) {
      // 원본 전처리 파일이 있고, 트리맵 데이터 파일도 있으면 스킵
      if (sourceFileExists) {
        shouldGenerate = false;
        console.log(`트리맵 데이터 파일이 이미 존재합니다 (${dateParam}). 이전 데이터를 사용합니다.`);
      } else {
        // 원본 파일이 없는데 트리맵 파일만 있으면 재생성 필요
        console.log(`원본 전처리 파일이 없습니다 (${dateParam}). 트리맵 데이터를 재생성합니다.`);
      }
    } else if (!sourceFileExists) {
      // 원본 파일도 없고 트리맵 파일도 없으면 에러
      return NextResponse.json(
        { 
          success: false, 
          error: `해당 날짜(${dateParam})의 원본 전처리 파일을 찾을 수 없습니다. 먼저 전처리 스크립트를 실행해주세요.` 
        },
        { status: 404 }
      );
    }
    
    if (shouldGenerate) {
      const scriptPath = join(process.cwd(), 'scripts', 'create_treemap_data.py');
      const pythonCommand = `python "${scriptPath}" ${dateParam}`;
      
      console.log(`트리맵 데이터 생성 실행: ${pythonCommand}`);
      
      const { stdout, stderr } = await execAsync(pythonCommand, {
        cwd: process.cwd(),
        maxBuffer: 10 * 1024 * 1024 // 10MB
      });
      
      if (stderr && !stderr.includes('[OK]') && !stderr.includes('[COMPLETE]')) {
        console.warn('경고:', stderr);
      }
      
      return NextResponse.json({
        success: true,
        date: dateParam,
        message: '트리맵 데이터 생성 완료',
        output: stdout,
        generated: true
      });
    } else {
      return NextResponse.json({
        success: true,
        date: dateParam,
        message: '트리맵 데이터 파일이 이미 존재합니다',
        generated: false
      });
    }
    
  } catch (error: any) {
    console.error('트리맵 데이터 생성 오류:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: error.message,
        stderr: error.stderr,
        stdout: error.stdout
      },
      { status: 500 }
    );
  }
}


