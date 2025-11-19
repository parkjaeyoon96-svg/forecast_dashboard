// Column mapping configuration
const columnMappingConfig: {
  [key: string]: {
    description: string;
    mappings: { [key: string]: string[] };
    required?: string[];
    optional?: string[];
  };
} = {
  "sap_ke30": {
    "description": "SAP KE30 컬럼 매핑 설정",
    "mappings": {
      "자재코드": ["자재코드", "Material", "자재", "MAT_CODE", "MATERIAL_CODE", "품번"],
      "자재명": ["자재명", "Material Name", "자재명칭", "MAT_NAME", "MATERIAL_NAME", "품명", "제품명", "상품명"],
      "브랜드": ["브랜드", "Brand", "BRAND", "브랜드명", "BRAND_NAME"],
      "수량": ["수량", "Quantity", "QTY", "판매수량", "SALES_QTY"],
      "매출액": ["매출액", "Revenue", "Sales", "REVENUE", "SALES_AMOUNT", "순매출액", "실판매출"],
      "매출원가": ["매출원가", "COGS", "Cost of Sales", "COST_OF_SALES", "원가"],
      "판매관리비": ["판매관리비", "SG&A", "Selling Expense", "SELLING_EXPENSE", "판관비"],
      "영업이익": ["영업이익", "Operating Profit", "EBIT", "OPERATING_INCOME", "OP"],
      "직접이익": ["직접이익", "Direct Profit", "DIRECT_PROFIT", "Gross Profit", "GP"],
      "고정비": ["고정비", "Fixed Cost", "FIXED_COST"],
      "변동비": ["변동비", "Variable Cost", "VARIABLE_COST"],
      "일자": ["일자", "Date", "DATE", "거래일자", "전표일자", "POSTING_DATE"],
      "카테고리": ["카테고리", "Category", "CATEGORY", "제품군", "상품군"],
      "채널": ["채널", "Channel", "CHANNEL", "판매채널", "유통채널"]
    },
    "required": ["자재코드", "자재명", "매출액"],
    "optional": ["수량", "매출원가", "판매관리비", "영업이익", "일자", "브랜드"]
  },
  "snowflake": {
    "description": "Snowflake 데이터 매핑",
    "mappings": {
      "일자": ["날짜", "Date", "DATE", "일자"],
      "브랜드": ["브랜드", "Brand", "BRAND"],
      "채널": ["채널", "Channel", "CHANNEL"],
      "매출액": ["매출액", "Revenue", "REVENUE", "Sales"],
      "방문자수": ["방문자수", "Visitors", "VISITORS"],
      "전환율": ["전환율", "Conversion Rate", "CONVERSION_RATE"]
    }
  }
};

/**
 * 컬럼 매핑 결과
 */
export interface ColumnMappingResult {
  standardColumn: string;  // 표준 컬럼명 (예: '매출액')
  actualColumn: string;    // 실제 CSV 컬럼명 (예: 'Revenue')
  confidence: number;      // 매칭 신뢰도 (0-1)
}

/**
 * 매핑된 데이터 행
 */
export interface MappedRow {
  [standardColumn: string]: any;
}

/**
 * CSV 컬럼을 표준 컬럼으로 매핑
 */
export function mapColumns(
  actualColumns: string[], 
  dataType: 'sap_ke30' | 'snowflake' = 'sap_ke30'
): Map<string, ColumnMappingResult> {
  const config = columnMappingConfig[dataType];
  const mappings = config.mappings;
  const result = new Map<string, ColumnMappingResult>();

  // 각 표준 컬럼에 대해 실제 컬럼 찾기
  Object.entries(mappings).forEach(([standardColumn, possibleNames]) => {
    const names = possibleNames as string[];
    
    // 실제 컬럼에서 매칭되는 것 찾기
    for (const actualColumn of actualColumns) {
      const normalized = normalizeColumnName(actualColumn);
      
      for (let i = 0; i < names.length; i++) {
        const possibleName = normalizeColumnName(names[i]);
        
        if (normalized === possibleName) {
          // 정확히 일치
          result.set(standardColumn, {
            standardColumn,
            actualColumn,
            confidence: 1.0 - (i * 0.1) // 첫 번째 매칭이 가장 높은 신뢰도
          });
          return; // 찾았으면 다음 표준 컬럼으로
        }
        
        // 부분 일치 (포함)
        if (normalized.includes(possibleName) || possibleName.includes(normalized)) {
          if (!result.has(standardColumn)) {
            result.set(standardColumn, {
              standardColumn,
              actualColumn,
              confidence: 0.7 - (i * 0.1)
            });
          }
        }
      }
    }
  });

  return result;
}

/**
 * 컬럼명 정규화 (비교용)
 */
function normalizeColumnName(name: string): string {
  return name
    .toLowerCase()
    .trim()
    .replace(/[_\-\s]/g, '') // 언더스코어, 하이픈, 공백 제거
    .replace(/[()]/g, '');    // 괄호 제거
}

/**
 * 실제 데이터를 표준 형식으로 변환
 */
export function mapRowData(
  row: Record<string, any>,
  columnMapping: Map<string, ColumnMappingResult>
): MappedRow {
  const mappedRow: MappedRow = {};

  columnMapping.forEach((mapping, standardColumn) => {
    const value = row[mapping.actualColumn];
    if (value !== undefined && value !== null) {
      mappedRow[standardColumn] = value;
    }
  });

  return mappedRow;
}

/**
 * 전체 데이터셋 변환
 */
export function mapDataset(
  rows: Record<string, any>[],
  actualColumns: string[],
  dataType: 'sap_ke30' | 'snowflake' = 'sap_ke30'
): { mappedRows: MappedRow[], mapping: Map<string, ColumnMappingResult> } {
  const mapping = mapColumns(actualColumns, dataType);
  const mappedRows = rows.map(row => mapRowData(row, mapping));

  return { mappedRows, mapping };
}

/**
 * 매핑 결과 검증
 */
export function validateMapping(
  mapping: Map<string, ColumnMappingResult>,
  dataType: 'sap_ke30' | 'snowflake' = 'sap_ke30'
): { valid: boolean, missing: string[], warnings: string[] } {
  const config = columnMappingConfig[dataType];
  const required = config.required || [];
  
  const missing: string[] = [];
  const warnings: string[] = [];

  // 필수 컬럼 확인
  required.forEach((col: string) => {
    if (!mapping.has(col)) {
      missing.push(col);
    } else {
      const result = mapping.get(col)!;
      if (result.confidence < 0.8) {
        warnings.push(`${col}: 낮은 신뢰도 (${(result.confidence * 100).toFixed(0)}%)`);
      }
    }
  });

  return {
    valid: missing.length === 0,
    missing,
    warnings
  };
}

/**
 * 매핑 정보를 사람이 읽을 수 있는 형식으로 출력
 */
export function printMapping(mapping: Map<string, ColumnMappingResult>): string {
  const lines: string[] = ['컬럼 매핑 결과:'];
  
  mapping.forEach((result, standardColumn) => {
    const confidence = (result.confidence * 100).toFixed(0);
    const emoji = result.confidence >= 0.9 ? '✅' : result.confidence >= 0.7 ? '⚠️' : '❓';
    lines.push(`  ${emoji} ${standardColumn} → ${result.actualColumn} (신뢰도: ${confidence}%)`);
  });

  return lines.join('\n');
}

/**
 * 수동 매핑 추가
 */
export function addCustomMapping(
  mapping: Map<string, ColumnMappingResult>,
  standardColumn: string,
  actualColumn: string
): Map<string, ColumnMappingResult> {
  mapping.set(standardColumn, {
    standardColumn,
    actualColumn,
    confidence: 1.0
  });
  return mapping;
}

/**
 * CSV 헤더 미리보기 및 제안
 */
export function analyzeColumns(actualColumns: string[]): {
  detected: string[];
  suggestions: { [actualColumn: string]: string[] };
} {
  const detected: string[] = [];
  const suggestions: { [actualColumn: string]: string[] } = {};

  const allMappings = columnMappingConfig.sap_ke30.mappings;

  actualColumns.forEach(actualCol => {
    const normalized = normalizeColumnName(actualCol);
    let found = false;

    Object.entries(allMappings).forEach(([standardCol, possibleNames]) => {
      const names = possibleNames as string[];
      
      for (const possibleName of names) {
        if (normalizeColumnName(possibleName) === normalized) {
          detected.push(`${actualCol} → ${standardCol}`);
          found = true;
          return;
        }
      }
    });

    if (!found) {
      // 제안 생성
      const candidateStandards: string[] = [];
      
      Object.entries(allMappings).forEach(([standardCol, possibleNames]) => {
        const names = possibleNames as string[];
        
        for (const possibleName of names) {
          const normPossible = normalizeColumnName(possibleName);
          if (normalized.includes(normPossible) || normPossible.includes(normalized)) {
            candidateStandards.push(standardCol);
            break;
          }
        }
      });

      if (candidateStandards.length > 0) {
        suggestions[actualCol] = candidateStandards;
      }
    }
  });

  return { detected, suggestions };
}

/**
 * 실제 KE30 데이터 타입 정의 (매핑 후)
 */
export interface StandardKE30Data {
  자재코드?: string;
  자재명?: string;
  브랜드?: string;
  수량?: number;
  매출액: number;  // 필수
  매출원가?: number;
  판매관리비?: number;
  영업이익?: number;
  직접이익?: number;
  고정비?: number;
  변동비?: number;
  일자?: string;
  카테고리?: string;
  채널?: string;
}

/**
 * 매핑된 데이터를 표준 타입으로 변환
 */
export function convertToStandardKE30(mappedRow: MappedRow): StandardKE30Data {
  return {
    자재코드: mappedRow['자재코드'],
    자재명: mappedRow['자재명'],
    브랜드: mappedRow['브랜드'],
    수량: mappedRow['수량'] ? Number(mappedRow['수량']) : undefined,
    매출액: Number(mappedRow['매출액'] || 0),
    매출원가: mappedRow['매출원가'] ? Number(mappedRow['매출원가']) : undefined,
    판매관리비: mappedRow['판매관리비'] ? Number(mappedRow['판매관리비']) : undefined,
    영업이익: mappedRow['영업이익'] ? Number(mappedRow['영업이익']) : undefined,
    직접이익: mappedRow['직접이익'] ? Number(mappedRow['직접이익']) : undefined,
    고정비: mappedRow['고정비'] ? Number(mappedRow['고정비']) : undefined,
    변동비: mappedRow['변동비'] ? Number(mappedRow['변동비']) : undefined,
    일자: mappedRow['일자'],
    카테고리: mappedRow['카테고리'],
    채널: mappedRow['채널']
  };
}

