'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

export default function Home() {
  const router = useRouter();
  const [selectedDate, setSelectedDate] = useState('');
  const [analysisMonth, setAnalysisMonth] = useState('2025-11');
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    // 사용 가능한 날짜 목록 로드
    fetch('/api/list-dates')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.dates.length > 0) {
          setAvailableDates(data.dates);
          // 기본값을 최신 날짜로 설정
          setSelectedDate(data.dates[0]);
        }
      })
      .catch(err => console.error('날짜 목록 로드 실패:', err));
  }, []);

  const handleDateChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSelectedDate(e.target.value);
  };

  const handleDashboardOpen = async () => {
    if (!selectedDate) {
      alert('날짜를 선택해주세요.');
      return;
    }
    if (!analysisMonth) {
      alert('분석월을 선택해주세요.');
      return;
    }
    setLoading(true);
    try {
      // YYYY.MM.DD -> YYYYMMDD 변환
      const dateParam = selectedDate.replace(/\./g, '');
      // 분석월을 URL 파라미터로 전달
      router.push(`/dashboard?date=${dateParam}&month=${analysisMonth}`);
    } catch (error) {
      console.error('대시보드 열기 오류:', error);
      alert('대시보드를 열 수 없습니다.');
    } finally {
      setLoading(false);
    }
  };

  // YYYY.MM.DD -> YYYY-MM-DD 변환 (input type="date" 형식)
  const formatDateForInput = (dateStr: string) => {
    if (!dateStr) return '';
    const parts = dateStr.split('.');
    if (parts.length === 3) {
      return `${parts[0]}-${parts[1]}-${parts[2]}`;
    }
    return '';
  };

  // YYYY-MM-DD -> YYYY.MM.DD 변환
  const formatDateFromInput = (dateStr: string) => {
    if (!dateStr) return '';
    const parts = dateStr.split('-');
    if (parts.length === 3) {
      return `${parts[0]}.${parts[1]}.${parts[2]}`;
    }
    return '';
  };

  const handleDateInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const inputValue = e.target.value;
    if (!inputValue) {
      setSelectedDate('');
      return;
    }
    
    // 선택한 날짜를 YYYY.MM.DD 형식으로 변환
    const formatted = formatDateFromInput(inputValue);
    
    // 사용 가능한 날짜 목록에 있는지 확인
    if (!availableDates.includes(formatted)) {
      // 사용 가능한 날짜가 아니면 알림 표시
      alert(`선택하신 날짜(${formatted})는 데이터가 없습니다.\n\n사용 가능한 날짜:\n${availableDates.join(', ')}`);
      
      // 가장 가까운 사용 가능한 날짜로 자동 선택
      if (availableDates.length > 0) {
        const selectedDateObj = new Date(inputValue);
        let closestDate = availableDates[0];
        let minDiff = Infinity;
        
        // 선택한 날짜와 가장 가까운 사용 가능한 날짜 찾기
        for (const dateStr of availableDates) {
          const dateParts = dateStr.split('.');
          const dateObj = new Date(parseInt(dateParts[0]), parseInt(dateParts[1]) - 1, parseInt(dateParts[2]));
          const diff = Math.abs(selectedDateObj.getTime() - dateObj.getTime());
          if (diff < minDiff) {
            minDiff = diff;
            closestDate = dateStr;
          }
        }
        
        setSelectedDate(closestDate);
        e.target.value = formatDateForInput(closestDate);
        alert(`가장 가까운 사용 가능한 날짜(${closestDate})로 자동 선택되었습니다.`);
      } else {
        setSelectedDate('');
        e.target.value = '';
      }
      return;
    }
    
    // 날짜를 Date 객체로 변환
    const selectedDateObj = new Date(inputValue);
    const dayOfWeek = selectedDateObj.getDay(); // 0=일요일, 1=월요일, ..., 6=토요일
    
    // 월요일인지 확인 (1 = 월요일)
    if (dayOfWeek !== 1) {
      // 월요일이 아니면 가장 가까운 다음 월요일로 조정
      const daysUntilMonday = dayOfWeek === 0 ? 1 : (8 - dayOfWeek);
      selectedDateObj.setDate(selectedDateObj.getDate() + daysUntilMonday);
      
      // 조정된 날짜를 포맷팅
      const adjustedFormatted = formatDateFromInput(selectedDateObj.toISOString().split('T')[0]);
      
      // 조정된 날짜가 사용 가능한 날짜 목록에 있는지 확인
      if (availableDates.includes(adjustedFormatted)) {
        setSelectedDate(adjustedFormatted);
        e.target.value = selectedDateObj.toISOString().split('T')[0];
        alert(`업데이트일자는 월요일만 선택 가능합니다.\n선택하신 날짜를 다음 월요일(${adjustedFormatted})로 조정했습니다.`);
      } else {
        // 조정된 날짜가 사용 가능하지 않으면 원래 날짜 유지 (월요일이 아니더라도)
        setSelectedDate(formatted);
        alert(`업데이트일자는 월요일만 선택 가능하지만, 조정된 날짜(${adjustedFormatted})에 데이터가 없습니다.\n현재 선택한 날짜(${formatted})를 유지합니다.`);
      }
    } else {
      // 월요일이면 정상 처리
      setSelectedDate(formatted);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-indigo-500 via-purple-500 to-pink-500">
      <main className="flex flex-col items-center gap-8 bg-white rounded-2xl p-12 shadow-2xl max-w-xl w-full">
        <div className="text-center">
          <h1 className="text-4xl font-bold text-slate-800 mb-4">
            월중 손익예측 시스템
          </h1>
          <p className="text-lg text-slate-600 mb-8">
            실시간 재무 데이터 분석 및 예측 대시보드
          </p>
        </div>
        
        {/* 분석월 및 업데이트 날짜 선택 섹션 */}
        <div className="w-full bg-slate-50 rounded-xl p-6 border-2 border-slate-200">
          <div className="mb-4">
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              분석월 <span className="text-red-500">*</span>
            </label>
            <input
              type="month"
              value={analysisMonth}
              onChange={(e) => setAnalysisMonth(e.target.value)}
              className="w-full px-4 py-3 border-2 border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent text-slate-800 font-medium"
            />
          </div>
          
          <div className="mb-4">
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              업데이트 일자 <span className="text-red-500">*</span>
            </label>
            <div className="flex items-center gap-3">
              <input
                type="date"
                value={formatDateForInput(selectedDate) || (availableDates.length > 0 ? formatDateForInput(availableDates[0]) : '')}
                onChange={handleDateInputChange}
                min={availableDates.length > 0 ? formatDateForInput(availableDates[availableDates.length - 1]) : undefined}
                max={availableDates.length > 0 ? formatDateForInput(availableDates[0]) : undefined}
                className="flex-1 px-4 py-3 border-2 border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent text-slate-800 font-medium"
                placeholder="날짜 선택"
              />
              <div className="text-slate-600 text-sm">
                📅
              </div>
            </div>
            <p className="mt-2 text-xs text-amber-600 font-medium">
              ⚠️ JSON 파일이 있는 날짜만 선택 가능합니다. 월요일만 선택 가능하며, 다른 요일을 선택하면 자동으로 다음 월요일로 조정됩니다.
            </p>
            {availableDates.length > 0 && (
              <p className="mt-1 text-xs text-slate-500">
                사용 가능한 날짜: {availableDates.join(', ')}
              </p>
            )}
          </div>
          
          <button
            onClick={handleDashboardOpen}
            disabled={!selectedDate || loading}
            className="w-full flex h-14 items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-indigo-600 to-purple-600 px-6 text-white text-lg font-semibold transition-all hover:shadow-xl hover:scale-105 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100"
          >
            {loading ? '로딩 중...' : '📊 대시보드 보기'}
          </button>
            </div>
        
        {/* 업데이트일자 설명 */}
        <div className="w-full mt-6 p-6 bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border border-blue-200 shadow-sm">
          <div className="flex items-start gap-4">
            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-md">
              <span className="text-2xl">📅</span>
            </div>
            <div className="flex-1">
              <h3 className="text-base font-bold text-slate-800 mb-3">업데이트일자 안내</h3>
              <p className="text-sm text-slate-700 leading-relaxed mb-3">
                업데이트일자는 <strong className="text-indigo-700">전 주 매출에 대한 분석</strong>을 제공합니다.
              </p>
              <p className="text-sm text-slate-600 mb-2">
                <strong className="text-indigo-700">예시:</strong> 업데이트일자에 <strong className="text-indigo-700">2025.11.17</strong>을 입력하면
              </p>
              <p className="text-sm text-slate-700 font-medium">
                → <span className="text-indigo-700 font-bold">2025-11-10 (월) ~ 2025-11-16 (일)</span> 주차별 분석 자료가 표시됩니다.
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
