'use client';

import { useEffect, useMemo, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';

function DashboardContent() {
  const searchParams = useSearchParams();

  // 첫 렌더부터 iframe에 date/month 등 쿼리를 넣어서 로드 (서버/클라이언트 동일해야 hydration 오류 방지 — Date.now() 사용 안 함)
  const iframeSrc = useMemo(() => {
    const params = new URLSearchParams();
    searchParams.forEach((value, key) => {
      params.set(key, value);
    });
    const queryString = params.toString();
    return queryString ? `/Dashboard.html?${queryString}` : '/Dashboard.html';
  }, [searchParams]);

  useEffect(() => {
    document.body.style.margin = '0';
    document.body.style.padding = '0';
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.margin = '';
      document.body.style.padding = '';
      document.body.style.overflow = '';
    };
  }, []);

  return (
    <iframe 
      src={iframeSrc}
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
