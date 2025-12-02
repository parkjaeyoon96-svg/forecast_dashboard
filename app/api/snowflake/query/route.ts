import { NextResponse } from 'next/server';

/**
 * Snowflake 쿼리 실행 API
 * 
 * 사용법:
 * POST /api/snowflake/query
 * Body: { "query": "SELECT * FROM table_name LIMIT 10" }
 */
export async function POST(request: Request) {
  try {
    const { query } = await request.json();

    if (!query) {
      return NextResponse.json(
        { success: false, error: '쿼리가 필요합니다.' },
        { status: 400 }
      );
    }

    // Python 스크립트 실행
    const { spawn } = await import('child_process');
    const path = await import('path');
    
    const scriptPath = path.join(process.cwd(), 'scripts', 'snowflake_query.py');
    
    return new Promise((resolve, reject) => {
      const python = spawn('python', [scriptPath, query]);
      
      let output = '';
      let errorOutput = '';

      python.stdout.on('data', (data: Buffer) => {
        output += data.toString();
      });

      python.stderr.on('data', (data: Buffer) => {
        errorOutput += data.toString();
      });

      python.on('close', (code: number) => {
        if (code === 0) {
          try {
            const result = JSON.parse(output);
            resolve(NextResponse.json(result));
          } catch (e) {
            resolve(NextResponse.json({
              success: false,
              error: '결과 파싱 실패',
              output,
              errorOutput
            }, { status: 500 }));
          }
        } else {
          resolve(NextResponse.json({
            success: false,
            error: '쿼리 실행 실패',
            errorOutput
          }, { status: 500 }));
        }
      });
    });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

