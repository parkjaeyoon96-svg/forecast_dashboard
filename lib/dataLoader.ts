import * as XLSX from 'xlsx';

/**
 * CSV 데이터 파싱 타입
 */
export interface ParsedCSVData {
  headers: string[];
  rows: Record<string, any>[];
}

/**
 * SAP KE30 데이터 타입
 */
export interface SAPKe30Row {
  자재코드: string;
  자재명: string;
  수량: number;
  매출액: number;
  매출원가: number;
  판매관리비: number;
  영업이익: number;
  고정비?: number;
  변동비?: number;
  일자: string;
}

/**
 * Snowflake 데이터 타입
 */
export interface SnowflakeData {
  [key: string]: any;
}

/**
 * 마스터 데이터 타입
 */
export interface MasterData {
  brands: BrandInfo[];
  products: ProductInfo[];
  categories: CategoryInfo[];
}

export interface BrandInfo {
  code: string;
  name: string;
  target: number;
  prevYear: number;
}

export interface ProductInfo {
  code: string;
  name: string;
  brand: string;
  category: string;
}

export interface CategoryInfo {
  code: string;
  name: string;
  parentCode?: string;
}

/**
 * 계획 데이터 타입
 */
export interface PlanData {
  period: string;
  brand: string;
  target: number;
  revenue: number;
  profit: number;
}

/**
 * 통합 대시보드 데이터 타입
 */
export interface DashboardData {
  overview: OverviewData;
  brands: BrandData[];
  weeklyData: WeeklyData;
  financialReport: FinancialReportData;
  rawData?: {
    sap?: SAPKe30Row[];
    snowflake?: SnowflakeData[];
    master?: MasterData;
    plan?: PlanData[];
  };
}

export interface OverviewData {
  currentRevenue: number;
  currentProfit: number;
  currentProfitRate: number;
  currentProgress: number;
  expectedRevenue: number;
  expectedOperatingProfit: number;
  expectedOperatingProfitRate: number;
  expectedAchievement: number;
  insights?: string;
}

export interface BrandData {
  name: string;
  revenue: number;
  profit: number;
  target: number;
  prevYear: number;
  profitRate: number;
  achievement: number;
}

export interface WeeklyData {
  labels: string[];
  sales: number[];
  prevYear: number[];
  target: number[];
  cumulative: number[];
  cumulativePrev: number[];
  cumulativeTarget: number[];
}

export interface FinancialReportData {
  items: FinancialReportItem[];
}

export interface FinancialReportItem {
  name: string;
  current: number;
  target: number;
  prevYear: number;
  variance: number;
  variancePct: number;
}

/**
 * CSV 파일 로드 (서버사이드)
 */
export async function loadCSVFromFile(filePath: string): Promise<ParsedCSVData> {
  const fs = await import('fs');
  const path = await import('path');
  
  const fullPath = path.join(process.cwd(), filePath);
  
  if (!fs.existsSync(fullPath)) {
    throw new Error(`파일을 찾을 수 없습니다: ${filePath}`);
  }

  const fileContent = fs.readFileSync(fullPath, 'utf-8');
  const lines = fileContent.trim().split('\n');
  
  if (lines.length === 0) {
    throw new Error('파일이 비어있습니다');
  }

  const headers = lines[0].split(',').map(h => h.trim());
  const rows: Record<string, any>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    const row: Record<string, any> = {};

    headers.forEach((header, index) => {
      if (values[index] !== undefined) {
        row[header] = cleanValue(values[index]);
      }
    });

    rows.push(row);
  }

  return { headers, rows };
}

/**
 * CSV 라인 파싱 (따옴표 처리)
 */
function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  values.push(current.trim());
  return values;
}

/**
 * 값 정리 (쉼표 제거, 숫자 변환)
 */
function cleanValue(value: string): any {
  if (!value) return value;

  // 따옴표 제거
  value = value.replace(/^["']|["']$/g, '');

  // 쉼표 제거 후 숫자 변환 시도
  const numericValue = value.replace(/,/g, '');
  if (!isNaN(Number(numericValue)) && numericValue !== '') {
    return Number(numericValue);
  }

  return value;
}

/**
 * Excel 파일 로드
 */
export async function loadExcelFromFile(filePath: string): Promise<Record<string, ParsedCSVData>> {
  const fs = await import('fs');
  const path = await import('path');
  
  const fullPath = path.join(process.cwd(), filePath);
  
  if (!fs.existsSync(fullPath)) {
    throw new Error(`파일을 찾을 수 없습니다: ${filePath}`);
  }

  const fileBuffer = fs.readFileSync(fullPath);
  const workbook = XLSX.read(fileBuffer, { type: 'buffer' });

  const result: Record<string, ParsedCSVData> = {};

  workbook.SheetNames.forEach(sheetName => {
    const worksheet = workbook.Sheets[sheetName];
    const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 }) as any[][];

    if (jsonData.length === 0) return;

    const headers = jsonData[0].map((h: any) => String(h));
    const rows: Record<string, any>[] = [];

    for (let i = 1; i < jsonData.length; i++) {
      const row: Record<string, any> = {};
      headers.forEach((header, index) => {
        if (jsonData[i][index] !== undefined) {
          row[header] = jsonData[i][index];
        }
      });
      rows.push(row);
    }

    result[sheetName] = { headers, rows };
  });

  return result;
}

/**
 * SAP KE30 데이터 집계
 */
export function aggregateSAPData(rows: SAPKe30Row[]): BrandData[] {
  const brandMap = new Map<string, {
    revenue: number;
    profit: number;
    cost: number;
    quantity: number;
  }>();

  rows.forEach(row => {
    const brand = row.자재명 || row.자재코드;
    const existing = brandMap.get(brand) || { revenue: 0, profit: 0, cost: 0, quantity: 0 };

    brandMap.set(brand, {
      revenue: existing.revenue + (row.매출액 || 0),
      profit: existing.profit + (row.영업이익 || 0),
      cost: existing.cost + (row.매출원가 || 0),
      quantity: existing.quantity + (row.수량 || 0)
    });
  });

  const brands: BrandData[] = [];
  brandMap.forEach((data, name) => {
    brands.push({
      name,
      revenue: Math.round(data.revenue / 100), // 억원 단위
      profit: Math.round(data.profit / 100),
      target: Math.round(data.revenue / 100 * 1.1), // 임시: 현재 + 10%
      prevYear: Math.round(data.revenue / 100 * 1.05), // 임시: 현재 + 5%
      profitRate: data.revenue > 0 ? (data.profit / data.revenue) * 100 : 0,
      achievement: 95 // 임시
    });
  });

  return brands;
}

/**
 * 주차별 데이터 생성
 */
export function generateWeeklyData(rows: SAPKe30Row[]): WeeklyData {
  // 날짜별로 그룹화
  const dateMap = new Map<string, number>();
  
  rows.forEach(row => {
    const date = row.일자;
    const revenue = row.매출액 || 0;
    dateMap.set(date, (dateMap.get(date) || 0) + revenue);
  });

  // 주차별로 집계 (임시: 7일 단위)
  const sortedDates = Array.from(dateMap.keys()).sort();
  const weeklyRevenue: number[] = [];
  const labels: string[] = [];

  let weekNum = 1;
  let weekTotal = 0;
  let dayCount = 0;

  sortedDates.forEach((date, index) => {
    weekTotal += dateMap.get(date) || 0;
    dayCount++;

    if (dayCount === 7 || index === sortedDates.length - 1) {
      weeklyRevenue.push(Math.round(weekTotal / 100)); // 억원 단위
      labels.push(`${weekNum}주`);
      weekNum++;
      weekTotal = 0;
      dayCount = 0;
    }
  });

  // 누적 계산
  const cumulative = weeklyRevenue.map((_, i) => 
    weeklyRevenue.slice(0, i + 1).reduce((a, b) => a + b, 0)
  );

  return {
    labels,
    sales: weeklyRevenue,
    prevYear: weeklyRevenue.map(v => v * 1.05), // 임시: +5%
    target: weeklyRevenue.map(v => v * 1.1), // 임시: +10%
    cumulative,
    cumulativePrev: cumulative.map(v => v * 1.05),
    cumulativeTarget: cumulative.map(v => v * 1.1)
  };
}

/**
 * 재무 리포트 생성
 */
export function generateFinancialReport(brands: BrandData[]): FinancialReportData {
  const totalRevenue = brands.reduce((sum, b) => sum + b.revenue, 0);
  const totalProfit = brands.reduce((sum, b) => sum + b.profit, 0);
  const totalTarget = brands.reduce((sum, b) => sum + b.target, 0);
  const totalPrevYear = brands.reduce((sum, b) => sum + b.prevYear, 0);

  const items: FinancialReportItem[] = [
    {
      name: '실판매출',
      current: totalRevenue,
      target: totalTarget,
      prevYear: totalPrevYear,
      variance: totalRevenue - totalTarget,
      variancePct: totalTarget > 0 ? ((totalRevenue - totalTarget) / totalTarget) * 100 : 0
    },
    {
      name: '영업이익',
      current: totalProfit,
      target: totalTarget * 0.1, // 임시: 10%
      prevYear: totalPrevYear * 0.12, // 임시: 12%
      variance: totalProfit - (totalTarget * 0.1),
      variancePct: totalTarget > 0 ? ((totalProfit - (totalTarget * 0.1)) / (totalTarget * 0.1)) * 100 : 0
    }
  ];

  return { items };
}





