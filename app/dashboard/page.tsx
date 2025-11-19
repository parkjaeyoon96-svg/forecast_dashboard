'use client';

import { useEffect, useState, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';

function DashboardContent() {
  const searchParams = useSearchParams();
  const [src, setSrc] = useState('/Dashboard.html');

  useEffect(() => {
    // 전체 화면 설정
    document.body.style.margin = '0';
    document.body.style.padding = '0';
    document.body.style.overflow = 'hidden';
    
    // URL에서 날짜 파라미터 가져오기
    const dateParam = searchParams.get('date');
    const timestamp = Date.now();
    
    // 날짜 파라미터가 있으면 전달
    if (dateParam) {
      setSrc(`/Dashboard.html?date=${dateParam}&t=${timestamp}`);
    } else {
      setSrc(`/Dashboard.html?t=${timestamp}`);
    }

    return () => {
      document.body.style.margin = '';
      document.body.style.padding = '';
      document.body.style.overflow = '';
    };
  }, [searchParams]);

  return (
    <iframe 
      src={src}
      style={{
        width: '100%',
        height: '100vh',
        border: 'none',
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0
      }}
      title="월중 손익예측 대시보드"
    />
  );
}

export default function DashboardPage() {
  return (
    <Suspense fallback={
      <div style={{
        width: '100%',
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '18px',
        color: '#666'
      }}>
        로딩 중...
      </div>
    }>
      <DashboardContent />
    </Suspense>
  );
}
