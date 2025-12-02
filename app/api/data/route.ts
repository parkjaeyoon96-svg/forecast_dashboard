import { NextResponse } from 'next/server';

// 대시보드 데이터
export async function GET() {
  const data = {
    overview: {
      currentRevenue: 88160,
      currentProfit: 19140,
      currentProfitRate: 21.7,
      currentProgress: 95,
      expectedRevenue: 88160,
      expectedOperatingProfit: 6340,
      expectedOperatingProfitRate: 7.2,
      expectedAchievement: 95,
      insights: '실판매출은 목표 대비 -5%, 영업이익은 -21%로 개선이 필요합니다. 브랜드D의 성과가 저조하며, 직접이익율은 양호한 수준을 유지하고 있습니다.'
    },
    brands: [
      { 
        name: '브랜드A', 
        revenue: 30000, 
        profit: 5000, 
        target: 32000, 
        prevYear: 31000,
        profitRate: 16.7,
        achievement: 93.8
      },
      { 
        name: '브랜드B', 
        revenue: 25000, 
        profit: 4000, 
        target: 27000, 
        prevYear: 26000,
        profitRate: 16.0,
        achievement: 92.6
      },
      { 
        name: '브랜드C', 
        revenue: 20000, 
        profit: 3500, 
        target: 21000, 
        prevYear: 22000,
        profitRate: 17.5,
        achievement: 95.2
      },
      { 
        name: '브랜드D', 
        revenue: 13160, 
        profit: 6640, 
        target: 15000, 
        prevYear: 14000,
        profitRate: 50.5,
        achievement: 87.7
      }
    ],
    weeklyData: {
      labels: ['1주', '2주', '3주', '4주'],
      sales: [200, 220, 230, 231.6],
      prevYear: [210, 230, 240, 245],
      target: [220, 220, 220, 220],
      cumulative: [200, 420, 650, 881.6],
      cumulativePrev: [210, 440, 680, 925],
      cumulativeTarget: [220, 440, 660, 880]
    },
    financialReport: {
      items: [
        { name: '실판매출', current: 88160, target: 92800, prevYear: 91800, variance: -4640, variancePct: -5.0 },
        { name: '직접이익', current: 19140, target: 20800, prevYear: 20100, variance: -1660, variancePct: -8.0 },
        { name: '직접이익율', current: 21.7, target: 22.4, prevYear: 21.9, variance: -0.7, variancePct: -0.7 },
        { name: '영업이익', current: 6340, target: 8020, prevYear: 8680, variance: -1680, variancePct: -21.0 },
        { name: '영업이익율', current: 7.2, target: 8.6, prevYear: 9.5, variance: -1.4, variancePct: -1.4 }
      ]
    }
  };

  return NextResponse.json(data);
}






