import { NextResponse } from 'next/server';
import { 
  loadCSVFromFile, 
  loadExcelFromFile,
  aggregateSAPData,
  generateWeeklyData,
  generateFinancialReport,
  DashboardData,
  OverviewData
} from '@/lib/dataLoader';
import { 
  mapDataset, 
  validateMapping, 
  printMapping, 
  analyzeColumns,
  convertToStandardKE30,
  StandardKE30Data
} from '@/lib/columnMapper';

/**
 * 실제 CSV/Excel 파일에서 데이터를 로드하는 API (컬럼 자동 매핑)
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const preview = searchParams.get('preview') === 'true';

    const dashboardData: DashboardData = {
      overview: {} as OverviewData,
      brands: [],
      weeklyData: {
        labels: [],
        sales: [],
        prevYear: [],
        target: [],
        cumulative: [],
        cumulativePrev: [],
        cumulativeTarget: []
      },
      financialReport: { items: [] },
      rawData: {}
    };

    const loadingLog: string[] = [];

    // 1. SAP KE30 데이터 로드 및 컬럼 매핑
    try {
      const sapData = await loadCSVFromFile('raw/sap_ke30_sample.csv');
      loadingLog.push(`📂 SAP KE30 파일 로드: ${sapData.rows.length}행`);
      
      // 컬럼 미리보기 모드
      if (preview) {
        const analysis = analyzeColumns(sapData.headers);
        return NextResponse.json({
          success: true,
          preview: true,
          file: 'sap_ke30_sample.csv',
          headers: sapData.headers,
          detected: analysis.detected,
          suggestions: analysis.suggestions,
          sampleRows: sapData.rows.slice(0, 5)
        });
      }

      // 컬럼 자동 매핑
      const { mappedRows, mapping } = mapDataset(
        sapData.rows, 
        sapData.headers, 
        'sap_ke30'
      );

      loadingLog.push(`🔄 컬럼 매핑 완료: ${mapping.size}개 컬럼`);
      loadingLog.push(printMapping(mapping));

      // 매핑 검증
      const validation = validateMapping(mapping, 'sap_ke30');
      if (!validation.valid) {
        loadingLog.push(`⚠️ 필수 컬럼 누락: ${validation.missing.join(', ')}`);
        console.warn('필수 컬럼 누락:', validation.missing);
      }
      if (validation.warnings.length > 0) {
        loadingLog.push(`⚠️ 경고: ${validation.warnings.join(', ')}`);
      }

      // 표준 형식으로 변환
      const standardData: StandardKE30Data[] = mappedRows.map(convertToStandardKE30);
      
      dashboardData.rawData!.sap = standardData as any;
      
      // 기존 로직 사용 (브랜드별 집계 등)
      dashboardData.brands = aggregateSAPData(standardData as any);
      dashboardData.weeklyData = generateWeeklyData(standardData as any);
      dashboardData.financialReport = generateFinancialReport(dashboardData.brands);

      loadingLog.push(`✅ SAP 데이터 처리 완료: ${dashboardData.brands.length}개 브랜드`);

    } catch (error: any) {
      loadingLog.push(`⚠️ SAP KE30 파일 로드 실패: ${error.message}`);
      console.warn('SAP KE30 로드 실패:', error.message);
      
      // 샘플 데이터 사용
      dashboardData.brands = getSampleBrands();
      dashboardData.weeklyData = getSampleWeeklyData();
      dashboardData.financialReport = getSampleFinancialReport();
      loadingLog.push('📦 샘플 데이터 사용');
    }

    // 2. Snowflake 데이터 로드 (선택사항)
    try {
      const snowflakeData = await loadCSVFromFile('raw/snowflake_data.csv');
      
      const { mappedRows, mapping } = mapDataset(
        snowflakeData.rows,
        snowflakeData.headers,
        'snowflake'
      );

      dashboardData.rawData!.snowflake = mappedRows;
      loadingLog.push(`✅ Snowflake 데이터 로드: ${mappedRows.length}행`);
      loadingLog.push(printMapping(mapping));

    } catch (error: any) {
      loadingLog.push(`⚠️ Snowflake 파일 없음`);
    }

    // 3. 마스터 데이터 로드 (선택사항)
    try {
      const masterData = await loadExcelFromFile('raw/master_data.xlsx');
      dashboardData.rawData!.master = masterData as any;
      loadingLog.push(`✅ 마스터 데이터 로드: ${Object.keys(masterData).length}개 시트`);
      
      // 브랜드 정보가 있으면 병합
      if (masterData['브랜드'] || masterData['Brand']) {
        const brandSheet = masterData['브랜드'] || masterData['Brand'];
        loadingLog.push(`📋 브랜드 마스터: ${brandSheet.rows.length}개`);
        
        // TODO: 브랜드 정보 병합 로직
        // dashboardData.brands = mergeBrandMaster(dashboardData.brands, brandSheet.rows);
      }

    } catch (error: any) {
      loadingLog.push(`⚠️ 마스터 파일 없음`);
    }

    // 4. 계획 데이터 로드 (선택사항)
    try {
      const planData = await loadCSVFromFile('raw/plan_data.csv');
      dashboardData.rawData!.plan = planData.rows as any;
      loadingLog.push(`✅ 계획 데이터 로드: ${planData.rows.length}행`);

    } catch (error: any) {
      loadingLog.push(`⚠️ 계획 파일 없음`);
    }

    // 5. Overview 데이터 계산
    const totalRevenue = dashboardData.brands.reduce((sum, b) => sum + b.revenue, 0);
    const totalProfit = dashboardData.brands.reduce((sum, b) => sum + b.profit, 0);
    const totalTarget = dashboardData.brands.reduce((sum, b) => sum + b.target, 0);
    const avgAchievement = dashboardData.brands.length > 0
      ? dashboardData.brands.reduce((sum, b) => sum + b.achievement, 0) / dashboardData.brands.length
      : 0;

    dashboardData.overview = {
      currentRevenue: totalRevenue,
      currentProfit: totalProfit,
      currentProfitRate: totalRevenue > 0 ? (totalProfit / totalRevenue) * 100 : 0,
      currentProgress: avgAchievement,
      expectedRevenue: totalRevenue,
      expectedOperatingProfit: totalProfit * 0.6,
      expectedOperatingProfitRate: totalRevenue > 0 ? (totalProfit * 0.6 / totalRevenue) * 100 : 0,
      expectedAchievement: avgAchievement,
      insights: generateInsights(dashboardData.brands, totalRevenue, totalProfit, totalTarget)
    };

    return NextResponse.json({
      success: true,
      data: dashboardData,
      metadata: {
        loadedAt: new Date().toISOString(),
        sources: {
          sap: !!dashboardData.rawData?.sap,
          snowflake: !!dashboardData.rawData?.snowflake,
          master: !!dashboardData.rawData?.master,
          plan: !!dashboardData.rawData?.plan
        },
        loadingLog
      }
    });

  } catch (error: any) {
    console.error('❌ 데이터 로드 오류:', error);
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

/**
 * 컬럼 분석 API (POST)
 */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { file, customMapping } = body;

    // CSV 파일 로드
    const csvData = await loadCSVFromFile(file);
    
    // 컬럼 분석
    const analysis = analyzeColumns(csvData.headers);

    // 커스텀 매핑 적용 (사용자가 수동으로 지정한 경우)
    let finalMapping;
    if (customMapping) {
      // TODO: 커스텀 매핑 적용 로직
      finalMapping = customMapping;
    }

    return NextResponse.json({
      success: true,
      headers: csvData.headers,
      analysis,
      rowCount: csvData.rows.length,
      sampleRows: csvData.rows.slice(0, 10)
    });

  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

// ===== 헬퍼 함수들 =====

function generateInsights(brands: any[], totalRevenue: number, totalProfit: number, totalTarget: number): string {
  const variance = totalRevenue - totalTarget;
  const variancePct = totalTarget > 0 ? (variance / totalTarget) * 100 : 0;
  
  const worstBrand = brands.reduce((worst, brand) => 
    brand.achievement < worst.achievement ? brand : worst, 
    brands[0] || { name: '없음', achievement: 100 }
  );

  const bestBrand = brands.reduce((best, brand) => 
    brand.achievement > best.achievement ? brand : best,
    brands[0] || { name: '없음', achievement: 0 }
  );

  return `실판매출은 목표 대비 ${variancePct.toFixed(1)}%, 총 영업이익은 ${totalProfit.toLocaleString()}백만원입니다. ` +
    `${worstBrand.name}의 성과가 ${worstBrand.achievement.toFixed(1)}%로 저조하며, ` +
    `${bestBrand.name}는 ${bestBrand.achievement.toFixed(1)}%로 양호합니다.`;
}

function getSampleBrands() {
  return [
    { name: '브랜드A', revenue: 300, profit: 50, target: 320, prevYear: 310, profitRate: 16.7, achievement: 93.8 },
    { name: '브랜드B', revenue: 250, profit: 40, target: 270, prevYear: 260, profitRate: 16.0, achievement: 92.6 },
    { name: '브랜드C', revenue: 200, profit: 35, target: 210, prevYear: 220, profitRate: 17.5, achievement: 95.2 },
    { name: '브랜드D', revenue: 131.6, profit: 66.4, target: 150, prevYear: 140, profitRate: 50.5, achievement: 87.7 }
  ];
}

function getSampleWeeklyData() {
  return {
    labels: ['1주', '2주', '3주', '4주'],
    sales: [200, 220, 230, 231.6],
    prevYear: [210, 230, 240, 245],
    target: [220, 220, 220, 220],
    cumulative: [200, 420, 650, 881.6],
    cumulativePrev: [210, 440, 680, 925],
    cumulativeTarget: [220, 440, 660, 880]
  };
}

function getSampleFinancialReport() {
  return {
    items: [
      { name: '실판매출', current: 881.6, target: 928, prevYear: 918, variance: -46.4, variancePct: -5.0 },
      { name: '직접이익', current: 191.4, target: 208, prevYear: 201, variance: -16.6, variancePct: -8.0 },
      { name: '영업이익', current: 63.4, target: 80.2, prevYear: 86.8, variance: -16.8, variancePct: -21.0 }
    ]
  };
}
