import SalesRateTable from '../components/SalesRateTable';

export default function SalesRatePage() {
  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <div className="max-w-[1600px] mx-auto">
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900">당시즌 의류 판매율 분석</h1>
          <p className="text-gray-600 mt-2">브랜드별 발주, 입고, 판매 현황 및 판매율 분석</p>
        </div>
        
        <SalesRateTable />
      </div>
    </div>
  );
}








