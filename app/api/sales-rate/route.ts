import { NextResponse } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';
import { getCache, setCache } from '@/lib/redis';

/**
 * 당시즌 의류 판매율 분석 데이터 조회 API
 * 
 * GET /api/sales-rate
 * 
 * 반환 데이터:
 * - success: boolean
 * - date: string (업데이트 일자)
 * - periodInfo: { curDate, pyDate, pyEndDate }
 * - data: { CUR, PY, PY_END } (각 기간별 판매율 데이터)
 * - rowCount: { CUR, PY, PY_END } (각 기간별 데이터 개수)
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
    
    // 오늘 날짜로 캐시 키 생성
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const cacheKey = `sales-rate-${today}`;
    
    // 1. Redis 캐시 확인 (강제 업데이트가 아닐 때만)
    if (!forceUpdate) {
      const cachedData = await getCache<any>(cacheKey);
      if (cachedData) {
        return NextResponse.json({
          ...cachedData,
          cached: true,
          cacheKey
        });
      }
    } else {
      console.log(`[강제 업데이트] ${cacheKey} - 캐시 무시하고 Snowflake 조회`);
    }
    
    console.log(`[캐시 미스] ${cacheKey} - Snowflake 조회 시작`);
    
    // 2. 캐시 미스 - Snowflake에서 조회
    const scriptPath = path.join(process.cwd(), 'scripts', 'query_sales_rate.py');
    
    // Python 가상환경 경로 확인
    const isWindows = process.platform === 'win32';
    const pythonPath = isWindows 
      ? path.join(process.cwd(), 'Forcast_venv', 'Scripts', 'python.exe')
      : path.join(process.cwd(), 'Forcast_venv', 'bin', 'python');
    
    // Python 스크립트 실행
    return new Promise<NextResponse>((resolve) => {
      const python = spawn(pythonPath, [scriptPath]);
      
      let output = '';
      let errorOutput = '';

      python.stdout.on('data', (data: Buffer) => {
        output += data.toString('utf8');
      });

      python.stderr.on('data', (data: Buffer) => {
        errorOutput += data.toString('utf8');
      });

      python.on('close', async (code: number) => {
        if (code === 0 && output) {
          try {
            const result = JSON.parse(output);
            
            if (result.success) {
              // 3. Redis 캐시에 저장 (24시간)
              await setCache(cacheKey, result, 86400);
              
              resolve(NextResponse.json({
                ...result,
                cached: false,
                cacheKey
              }));
            } else {
              console.error('[판매율 API] 쿼리 실패:', result.error);
              resolve(NextResponse.json(
                { 
                  success: false, 
                  error: result.error || '데이터 조회 실패',
                  details: errorOutput 
                },
                { status: 500 }
              ));
            }
          } catch (parseError: any) {
            console.error('[판매율 API] JSON 파싱 실패:', parseError);
            console.error('[판매율 API] 출력:', output);
            console.error('[판매율 API] 에러 출력:', errorOutput);
            
            resolve(NextResponse.json({
              success: false,
              error: 'JSON 파싱 실패',
              details: {
                parseError: parseError.message,
                output: output.substring(0, 500),
                errorOutput: errorOutput.substring(0, 500)
              }
            }, { status: 500 }));
          }
        } else {
          console.error('[판매율 API] Python 스크립트 실행 실패 (코드:', code, ')');
          console.error('[판매율 API] 에러 출력:', errorOutput);
          
          resolve(NextResponse.json({
            success: false,
            error: `Python 스크립트 실행 실패 (코드: ${code})`,
            details: errorOutput
          }, { status: 500 }));
        }
      });

      python.on('error', (error: Error) => {
        console.error('[판매율 API] Python 프로세스 에러:', error);
        
        resolve(NextResponse.json({
          success: false,
          error: 'Python 스크립트 실행 실패',
          details: error.message
        }, { status: 500 }));
      });
    });
  } catch (error: any) {
    console.error('[판매율 API] 예외 발생:', error);
    
    return NextResponse.json(
      { 
        success: false, 
        error: error.message,
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      },
      { status: 500 }
    );
  }
}

