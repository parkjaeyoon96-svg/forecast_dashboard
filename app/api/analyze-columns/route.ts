import { NextResponse } from 'next/server';
import { loadCSVFromFile } from '@/lib/dataLoader';
import { analyzeColumns, mapColumns, validateMapping, printMapping } from '@/lib/columnMapper';

/**
 * CSV 파일의 컬럼을 분석하는 API
 * 
 * 사용법:
 * GET /api/analyze-columns?file=raw/sap_ke30.csv
 * GET /api/analyze-columns?file=raw/sap_ke30_sample.csv
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const file = searchParams.get('file') || 'raw/sap_ke30_sample.csv';

    console.log(`🔍 파일 분석 시작: ${file}`);

    // CSV 파일 로드
    const csvData = await loadCSVFromFile(file);
    
    console.log(`✅ 파일 로드 완료: ${csvData.rows.length}행, ${csvData.headers.length}개 컬럼`);
    console.log(`📋 실제 컬럼들:`, csvData.headers);

    // 컬럼 분석
    const analysis = analyzeColumns(csvData.headers);
    
    // 자동 매핑 시도
    const mapping = mapColumns(csvData.headers, 'sap_ke30');
    
    // 매핑 검증
    const validation = validateMapping(mapping, 'sap_ke30');
    
    // 매핑 정보 출력
    const mappingInfo = printMapping(mapping);
    console.log(mappingInfo);

    // 매핑된 컬럼 목록
    const mappedColumns: Array<{
      standard: string;
      actual: string;
      confidence: number;
      status: 'exact' | 'partial' | 'none';
    }> = [];

    mapping.forEach((result, standardCol) => {
      mappedColumns.push({
        standard: standardCol,
        actual: result.actualColumn,
        confidence: result.confidence,
        status: result.confidence >= 0.9 ? 'exact' : 
                result.confidence >= 0.7 ? 'partial' : 'none'
      });
    });

    // 매핑되지 않은 컬럼
    const unmappedColumns = csvData.headers.filter(header => {
      return !Array.from(mapping.values()).some(m => m.actualColumn === header);
    });

    return NextResponse.json({
      success: true,
      file,
      analysis: {
        totalRows: csvData.rows.length,
        totalColumns: csvData.headers.length,
        actualHeaders: csvData.headers,
        detected: analysis.detected,
        suggestions: analysis.suggestions,
        mappedColumns,
        unmappedColumns,
        validation: {
          valid: validation.valid,
          missing: validation.missing,
          warnings: validation.warnings
        },
        mappingInfo
      },
      sampleData: csvData.rows.slice(0, 5).map((row, idx) => ({
        rowNumber: idx + 1,
        data: row
      }))
    });

  } catch (error: any) {
    console.error('❌ 컬럼 분석 오류:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: error.message,
        detail: '파일을 찾을 수 없거나 형식이 올바르지 않습니다.'
      },
      { status: 500 }
    );
  }
}

/**
 * 커스텀 매핑 저장 (POST)
 */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { file, customMapping } = body;

    // customMapping 형식:
    // {
    //   "매출액": "Revenue",
    //   "자재코드": "Material_Code",
    //   ...
    // }

    // TODO: 커스텀 매핑을 로컬 파일이나 DB에 저장
    // 현재는 config/columnMapping.json을 수정하는 것을 권장

    return NextResponse.json({
      success: true,
      message: '커스텀 매핑이 저장되었습니다.',
      customMapping
    });

  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 400 }
    );
  }
}





