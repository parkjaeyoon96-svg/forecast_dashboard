'use client';

import { useEffect, useState } from 'react';

// 전역 캐시 (모듈 레벨)
let globalCacheData: any = null;
let globalCacheTimestamp: number = 0;
const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24시간

// 카테고리 타입 정의
type CategoryType = '전체' | 'Outer' | 'Bottom' | 'Inner';

// 브랜드별 데이터 타입
interface BrandData {
  brandCode: string;
  brandName: string;
  category: string; // Outer, Bottom, Inner
  orderTag: number;
  storageTag: number;
  salesTag: number;
  salesRate: number;
}

// 기간별 집계 데이터
interface PeriodData {
  cur: BrandData[];  // 당년
  py: BrandData[];   // 전년
  pyEnd: BrandData[]; // 전년마감
}

// 합계 데이터
interface SummaryData {
  orderTag: number;
  storageTag: number;
  salesTag: number;
  salesRate: number;
}

interface PeriodSummary {
  cur: SummaryData;
  py: SummaryData;
  pyEnd: SummaryData;
}

interface SalesRateTableProps {
  className?: string;
}

export default function SalesRateTable({ className = '' }: SalesRateTableProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [updateDate, setUpdateDate] = useState('');
  const [periodInfo, setPeriodInfo] = useState({
    curDate: '',
    pyDate: '',
    pyEndDate: ''
  });
  
  const [rawData, setRawData] = useState<PeriodData>({
    cur: [],
    py: [],
    pyEnd: []
  });
  
  const [selectedCategory, setSelectedCategory] = useState<CategoryType>('전체');
  const [filteredData, setFilteredData] = useState<PeriodData>({
    cur: [],
    py: [],
    pyEnd: []
  });
  
  const [summary, setSummary] = useState<PeriodSummary>({
    cur: { orderTag: 0, storageTag: 0, salesTag: 0, salesRate: 0 },
    py: { orderTag: 0, storageTag: 0, salesTag: 0, salesRate: 0 },
    pyEnd: { orderTag: 0, storageTag: 0, salesTag: 0, salesRate: 0 }
  });

  // 브랜드 코드를 이름으로 변환
  const getBrandName = (brandCode: string): string => {
    const brandMap: Record<string, string> = {
      'C': 'KUHO',
      'D': 'CRES.E.DIM',
      'E': 'ETC',
      'F': 'SYSTEM',
      'G': 'GUGGY',
      'H': 'SJ',
      'I': 'MLB',
      'J': 'MLB KIDS',
      'K': 'VPLUS',
      'L': 'COLOMBO',
      'M': 'MAJE',
      'N': 'SANDRO',
      'O': 'CLAUDIE',
      'P': 'AP',
      'Q': 'THE KOOPLES',
      'R': 'ZADIG',
      'S': 'JWPEI',
      'T': 'TORY',
      'U': 'MCM',
      'V': 'METROCITY',
      'W': 'TED BAKER'
    };
    return brandMap[brandCode] || brandCode;
  };

  // 아이템 코드로 카테고리 분류
  const getCategory = (itemCode: string): string => {
    const outer = ['JK', 'CT', 'VT', 'CD', 'JP', 'PD'];
    const bottom = ['PT', 'SK', 'DP', 'LG', 'SP'];
    const inner = ['TS', 'KT', 'SH', 'BL', 'OP'];
    
    if (outer.includes(itemCode)) return 'Outer';
    if (bottom.includes(itemCode)) return 'Bottom';
    if (inner.includes(itemCode)) return 'Inner';
    return 'ETC';
  };

  // 데이터 로드
  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // 캐시 확인 (24시간 이내)
        const now = Date.now();
        const isCacheValid = globalCacheData && (now - globalCacheTimestamp < CACHE_DURATION);
        
        let result;
        
        if (isCacheValid) {
          console.log('[판매율 테이블] 캐시된 데이터 사용');
          result = globalCacheData;
        } else {
          console.log('[판매율 테이블] API 호출 중...');
          const response = await fetch('/api/sales-rate');
          result = await response.json();

          if (!result.success) {
            throw new Error(result.error || '데이터 로드 실패');
          }
          
          // 캐시 저장
          globalCacheData = result;
          globalCacheTimestamp = now;
          console.log('[판매율 테이블] 데이터 캐시 저장 완료');
        }

        // 업데이트 일자
        setUpdateDate(result.date);

        // 기간 정보 설정
        const curSample = result.data.CUR?.[0];
        const pySample = result.data.PY?.[0];
        const pyEndSample = result.data.PY_END?.[0];

        setPeriodInfo({
          curDate: curSample?.ASOF_DT || '',
          pyDate: pySample?.ASOF_DT || '',
          pyEndDate: pyEndSample?.ASOF_DT || ''
        });

        // 브랜드별 데이터 집계 (카테고리별로 구분하여 저장)
        const processData = (rawDataArray: any[]): BrandData[] => {
          // 1단계: 브랜드-카테고리별로 집계
          const brandCategoryMap = new Map<string, {
            brandCode: string;
            brandName: string;
            categories: Map<string, { orderTag: number; storageTag: number; salesTag: number }>;
          }>();

          rawDataArray.forEach((row) => {
            const brandCode = row.BRD_CD;
            const itemCode = row.ITEM_CD;
            const category = getCategory(itemCode);

            // 브랜드가 없으면 초기화
            if (!brandCategoryMap.has(brandCode)) {
              brandCategoryMap.set(brandCode, {
                brandCode,
                brandName: getBrandName(brandCode),
                categories: new Map()
              });
            }

            const brandData = brandCategoryMap.get(brandCode)!;
            
            // 카테고리가 없으면 초기화
            if (!brandData.categories.has(category)) {
              brandData.categories.set(category, {
                orderTag: 0,
                storageTag: 0,
                salesTag: 0
              });
            }

            // 카테고리별 데이터 누적
            const categoryData = brandData.categories.get(category)!;
            categoryData.orderTag += Number(row.AC_ORD_TAG_AMT_KOR || 0);
            categoryData.storageTag += Number(row.AC_STOR_TAG_AMT_KOR || 0);
            categoryData.salesTag += Number(row.SALE_TAG || 0);
          });

          // 2단계: 브랜드-카테고리 조합으로 BrandData 배열 생성
          const result: BrandData[] = [];
          
          brandCategoryMap.forEach((brandData) => {
            brandData.categories.forEach((categoryData, category) => {
              const salesRate = categoryData.storageTag > 0 
                ? (categoryData.salesTag / categoryData.storageTag) * 100 
                : 0;

              result.push({
                brandCode: brandData.brandCode,
                brandName: brandData.brandName,
                category,
                orderTag: categoryData.orderTag,
                storageTag: categoryData.storageTag,
                salesTag: categoryData.salesTag,
                salesRate
              });
            });
          });

          return result.sort((a, b) => {
            // 브랜드 코드 우선, 같으면 카테고리 순
            const brandCompare = a.brandCode.localeCompare(b.brandCode);
            if (brandCompare !== 0) return brandCompare;
            return a.category.localeCompare(b.category);
          });
        };

        const processedData = {
          cur: processData(result.data.CUR || []),
          py: processData(result.data.PY || []),
          pyEnd: processData(result.data.PY_END || [])
        };

        setRawData(processedData);
        setFilteredData(processedData); // 초기에는 전체 데이터 표시
        
        setLoading(false);
      } catch (err: any) {
        console.error('데이터 로드 에러:', err);
        setError(err.message);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // 카테고리 필터링 및 합계 계산
  useEffect(() => {
    const filterByCategory = (data: BrandData[]): BrandData[] => {
      if (selectedCategory === '전체') {
        // 전체: 브랜드별로 모든 카테고리 합산
        const brandMap = new Map<string, BrandData>();
        
        data.forEach(item => {
          if (!brandMap.has(item.brandCode)) {
            brandMap.set(item.brandCode, {
              brandCode: item.brandCode,
              brandName: item.brandName,
              category: '전체',
              orderTag: 0,
              storageTag: 0,
              salesTag: 0,
              salesRate: 0
            });
          }
          
          const brand = brandMap.get(item.brandCode)!;
          brand.orderTag += item.orderTag;
          brand.storageTag += item.storageTag;
          brand.salesTag += item.salesTag;
        });
        
        // 판매율 재계산
        brandMap.forEach(brand => {
          brand.salesRate = brand.storageTag > 0 
            ? (brand.salesTag / brand.storageTag) * 100 
            : 0;
        });
        
        return Array.from(brandMap.values()).sort((a, b) => 
          a.brandCode.localeCompare(b.brandCode)
        );
      } else {
        // 특정 카테고리: 해당 카테고리만 필터링
        return data.filter(item => item.category === selectedCategory);
      }
    };

    const calculateSummary = (data: BrandData[]): SummaryData => {
      const totals = data.reduce(
        (acc, item) => ({
          orderTag: acc.orderTag + item.orderTag,
          storageTag: acc.storageTag + item.storageTag,
          salesTag: acc.salesTag + item.salesTag
        }),
        { orderTag: 0, storageTag: 0, salesTag: 0 }
      );

      const salesRate = totals.storageTag > 0 
        ? (totals.salesTag / totals.storageTag) * 100 
        : 0;

      return { ...totals, salesRate };
    };

    const filtered = {
      cur: filterByCategory(rawData.cur),
      py: filterByCategory(rawData.py),
      pyEnd: filterByCategory(rawData.pyEnd)
    };

    setFilteredData(filtered);
    setSummary({
      cur: calculateSummary(filtered.cur),
      py: calculateSummary(filtered.py),
      pyEnd: calculateSummary(filtered.pyEnd)
    });
  }, [selectedCategory, rawData]);

  // 날짜 포맷팅
  const formatDate = (dateStr: string): string => {
    if (!dateStr) return '-';
    // YYYY-MM-DD 형식
    const match = dateStr.match(/^(\d{4})-?(\d{2})-?(\d{2})/);
    if (match) {
      return `${match[1]}-${match[2]}-${match[3]}`;
    }
    return dateStr;
  };

  // 금액 포맷팅 (억 단위)
  const formatAmount = (amount: number): string => {
    return (amount / 100000000).toFixed(1);
  };

  // 퍼센트 포맷팅
  const formatPercent = (value: number): string => {
    return value.toFixed(1) + '%';
  };

  if (loading) {
    return (
      <div className={`bg-white rounded-lg shadow-md p-6 ${className}`}>
        <div className="text-center text-gray-500">데이터 로딩 중...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`bg-white rounded-lg shadow-md p-6 ${className}`}>
        <div className="text-center text-red-500">에러: {error}</div>
      </div>
    );
  }

  return (
    <div className={`bg-white rounded-lg shadow-md ${className}`}>
      {/* 헤더 */}
      <div className="px-6 py-4 border-b border-gray-200">
        <h2 className="text-xl font-bold text-gray-800">당시즌 의류 판매율 분석</h2>
      </div>

      {/* 기본 정보 */}
      <div className="px-6 py-4 bg-gray-50 border-b border-gray-200">
        <div className="space-y-2 text-sm">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-gray-700">업데이트 일자:</span>
            <span className="text-gray-600">{formatDate(updateDate)}</span>
          </div>
          <div className="flex items-center gap-4">
            <span className="font-semibold text-gray-700">분석기간</span>
            <div className="flex gap-4 text-gray-600">
              <span>당년: {formatDate(periodInfo.curDate)} 누적</span>
              <span>/</span>
              <span>전년: {formatDate(periodInfo.pyDate)} 누적</span>
              <span>/</span>
              <span>마감: {formatDate(periodInfo.pyEndDate)} 누적</span>
            </div>
          </div>
        </div>
      </div>

      {/* 필터 버튼 */}
      <div className="px-6 py-4 border-b border-gray-200">
        <div className="flex gap-2">
          {(['전체', 'Outer', 'Bottom', 'Inner'] as CategoryType[]).map((category) => (
            <button
              key={category}
              onClick={() => setSelectedCategory(category)}
              className={`px-4 py-2 rounded-md font-medium transition-colors ${
                selectedCategory === category
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {category}
            </button>
          ))}
        </div>
      </div>

      {/* 테이블 */}
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-gray-100 border-b border-gray-200">
              <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700" rowSpan={2}>
                브랜드
              </th>
              <th className="px-4 py-3 text-center text-sm font-semibold text-gray-700 border-l border-gray-300" colSpan={4}>
                당년 ({formatDate(periodInfo.curDate)})
              </th>
              <th className="px-4 py-3 text-center text-sm font-semibold text-gray-700 border-l border-gray-300" colSpan={4}>
                전년 ({formatDate(periodInfo.pyDate)})
              </th>
              <th className="px-4 py-3 text-center text-sm font-semibold text-gray-700 border-l border-gray-300" colSpan={4}>
                전년마감 ({formatDate(periodInfo.pyEndDate)})
              </th>
            </tr>
            <tr className="bg-gray-50 border-b border-gray-200">
              {/* 당년 */}
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600 border-l border-gray-300">발주(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">입고(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매율</th>
              {/* 전년 */}
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600 border-l border-gray-300">발주(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">입고(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매율</th>
              {/* 전년마감 */}
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600 border-l border-gray-300">발주(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">입고(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매(억)</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-600">판매율</th>
            </tr>
          </thead>
          <tbody>
            {/* 합계 행 */}
            <tr className="bg-blue-50 border-b border-gray-300 font-semibold">
              <td className="px-4 py-3 text-sm text-gray-800">합계</td>
              {/* 당년 합계 */}
              <td className="px-4 py-3 text-sm text-right text-gray-800 border-l border-gray-300">
                {formatAmount(summary.cur.orderTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.cur.storageTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.cur.salesTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-blue-600">
                {formatPercent(summary.cur.salesRate)}
              </td>
              {/* 전년 합계 */}
              <td className="px-4 py-3 text-sm text-right text-gray-800 border-l border-gray-300">
                {formatAmount(summary.py.orderTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.py.storageTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.py.salesTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-blue-600">
                {formatPercent(summary.py.salesRate)}
              </td>
              {/* 전년마감 합계 */}
              <td className="px-4 py-3 text-sm text-right text-gray-800 border-l border-gray-300">
                {formatAmount(summary.pyEnd.orderTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.pyEnd.storageTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-gray-800">
                {formatAmount(summary.pyEnd.salesTag)}
              </td>
              <td className="px-4 py-3 text-sm text-right text-blue-600">
                {formatPercent(summary.pyEnd.salesRate)}
              </td>
            </tr>

            {/* 브랜드별 데이터 */}
            {filteredData.cur.map((curItem) => {
              const pyItem = filteredData.py.find(p => p.brandCode === curItem.brandCode) || {
                orderTag: 0, storageTag: 0, salesTag: 0, salesRate: 0
              };
              const pyEndItem = filteredData.pyEnd.find(p => p.brandCode === curItem.brandCode) || {
                orderTag: 0, storageTag: 0, salesTag: 0, salesRate: 0
              };

              return (
                <tr key={curItem.brandCode} className="border-b border-gray-200 hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm text-gray-800 font-medium">
                    {curItem.brandName}
                  </td>
                  {/* 당년 */}
                  <td className="px-4 py-3 text-sm text-right text-gray-700 border-l border-gray-300">
                    {formatAmount(curItem.orderTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(curItem.storageTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(curItem.salesTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-blue-600 font-medium">
                    {formatPercent(curItem.salesRate)}
                  </td>
                  {/* 전년 */}
                  <td className="px-4 py-3 text-sm text-right text-gray-700 border-l border-gray-300">
                    {formatAmount(pyItem.orderTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(pyItem.storageTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(pyItem.salesTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-blue-600 font-medium">
                    {formatPercent(pyItem.salesRate)}
                  </td>
                  {/* 전년마감 */}
                  <td className="px-4 py-3 text-sm text-right text-gray-700 border-l border-gray-300">
                    {formatAmount(pyEndItem.orderTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(pyEndItem.storageTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-gray-700">
                    {formatAmount(pyEndItem.salesTag)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right text-blue-600 font-medium">
                    {formatPercent(pyEndItem.salesRate)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* 데이터 개수 표시 */}
      <div className="px-6 py-3 bg-gray-50 border-t border-gray-200 text-xs text-gray-500">
        총 {filteredData.cur.length}개 브랜드 표시 중
      </div>
    </div>
  );
}

