'use client';

import { useState, useEffect, useRef } from 'react';
import { useRouter } from 'next/navigation';

export default function Home() {
  const router = useRouter();
  const [selectedDate, setSelectedDate] = useState('');
  const [analysisMonth, setAnalysisMonth] = useState('2025-11');
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [showCalendar, setShowCalendar] = useState(false);
  const [currentMonth, setCurrentMonth] = useState(new Date());
  const calendarRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // 사용 가능한 날짜 목록 로드
    fetch('/api/list-dates')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.dates.length > 0) {
          setAvailableDates(data.dates);
          // 기본값을 최신 날짜로 설정
          const latestDate = data.dates[0];
          setSelectedDate(latestDate);
          
          // 최신 날짜로부터 분석월 자동 계산
          const dateParam = latestDate.replace(/\./g, '');
          fetch(`/api/calculate-date-info?date=${dateParam}`)
            .then(res => res.json())
            .then(dateInfo => {
              if (dateInfo.success && dateInfo.analysisMonth) {
                setAnalysisMonth(dateInfo.analysisMonth);
              }
            })
            .catch(err => console.error('분석월 계산 실패:', err));
        }
      })
      .catch(err => console.error('날짜 목록 로드 실패:', err));
    
    // 달력에 가능한 날짜 표시를 위한 스타일 추가
    const style = document.createElement('style');
    style.textContent = `
      /* 가능한 날짜에 동그라미 표시를 위한 스타일 */
      input[type="date"] {
        position: relative;
      }
      /* Chrome/Safari에서 가능한 날짜 표시 */
      input[type="date"]::-webkit-calendar-picker-indicator {
        cursor: pointer;
        opacity: 1;
      }
    `;
    document.head.appendChild(style);
    
    return () => {
      document.head.removeChild(style);
    };
  }, []);

  // 달력 외부 클릭 시 닫기
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (calendarRef.current && !calendarRef.current.contains(event.target as Node)) {
        setShowCalendar(false);
      }
    };

    if (showCalendar) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showCalendar]);

  // 달력 날짜 생성 함수
  const generateCalendarDays = () => {
    const year = currentMonth.getFullYear();
    const month = currentMonth.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    const startDate = new Date(firstDay);
    startDate.setDate(startDate.getDate() - firstDay.getDay()); // 주의 시작일 (일요일)
    
    const days = [];
    const currentDate = new Date(startDate);
    
    for (let i = 0; i < 42; i++) {
      const dateStr = `${currentDate.getFullYear()}.${String(currentDate.getMonth() + 1).padStart(2, '0')}.${String(currentDate.getDate()).padStart(2, '0')}`;
      const isAvailable = availableDates.includes(dateStr);
      const isCurrentMonth = currentDate.getMonth() === month;
      const isSelected = selectedDate === dateStr;
      
      days.push({
        date: new Date(currentDate),
        dateStr,
        isAvailable,
        isCurrentMonth,
        isSelected
      });
      
      currentDate.setDate(currentDate.getDate() + 1);
    }
    
    return days;
  };

  const handleDateClick = (dateStr: string) => {
    if (!availableDates.includes(dateStr)) {
      return;
    }
    
    setSelectedDate(dateStr);
    setShowCalendar(false);
    
    // 날짜 선택 시 분석월 자동 계산
    const dateParam = dateStr.replace(/\./g, '');
    fetch(`/api/calculate-date-info?date=${dateParam}`)
      .then(res => res.json())
      .then(dateInfo => {
        if (dateInfo.success && dateInfo.analysisMonth) {
          setAnalysisMonth(dateInfo.analysisMonth);
        }
      })
      .catch(err => console.error('분석월 계산 실패:', err));
  };

  const handlePrevMonth = () => {
    setCurrentMonth(new Date(currentMonth.getFullYear(), currentMonth.getMonth() - 1, 1));
  };

  const handleNextMonth = () => {
    setCurrentMonth(new Date(currentMonth.getFullYear(), currentMonth.getMonth() + 1, 1));
  };

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
        
        // 조정된 날짜로 분석월 자동 계산
        const dateParam = adjustedFormatted.replace(/\./g, '');
        fetch(`/api/calculate-date-info?date=${dateParam}`)
          .then(res => res.json())
          .then(dateInfo => {
            if (dateInfo.success && dateInfo.analysisMonth) {
              setAnalysisMonth(dateInfo.analysisMonth);
            }
          })
          .catch(err => console.error('분석월 계산 실패:', err));
      } else {
        // 조정된 날짜가 사용 가능하지 않으면 원래 날짜 유지 (월요일이 아니더라도)
        setSelectedDate(formatted);
        alert(`업데이트일자는 월요일만 선택 가능하지만, 조정된 날짜(${adjustedFormatted})에 데이터가 없습니다.\n현재 선택한 날짜(${formatted})를 유지합니다.`);
        
        // 원래 날짜로 분석월 자동 계산
        const dateParam = formatted.replace(/\./g, '');
        fetch(`/api/calculate-date-info?date=${dateParam}`)
          .then(res => res.json())
          .then(dateInfo => {
            if (dateInfo.success && dateInfo.analysisMonth) {
              setAnalysisMonth(dateInfo.analysisMonth);
            }
          })
          .catch(err => console.error('분석월 계산 실패:', err));
      }
    } else {
      // 월요일이면 정상 처리
      setSelectedDate(formatted);
      
      // 업데이트 일자 선택 시 분석월 자동 계산
      const dateParam = formatted.replace(/\./g, '');
      fetch(`/api/calculate-date-info?date=${dateParam}`)
        .then(res => res.json())
        .then(dateInfo => {
          if (dateInfo.success && dateInfo.analysisMonth) {
            setAnalysisMonth(dateInfo.analysisMonth);
          }
        })
        .catch(err => console.error('분석월 계산 실패:', err));
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-indigo-500 via-purple-500 to-pink-500">
      <main className="flex flex-col items-center gap-8 bg-white rounded-2xl p-12 shadow-2xl max-w-xl w-full">
        <div className="text-center">
          <h1 className="text-4xl font-bold text-slate-800 mb-4">
            월손익 예상 대시보드
          </h1>
          <p className="text-lg text-slate-600 mb-8">
            브랜드별 월말 손익 예상 시스템
          </p>
        </div>
        
        {/* 분석월 및 업데이트 날짜 선택 섹션 */}
        <div className="w-full bg-slate-50 rounded-xl p-6 border-2 border-slate-200">
          <div className="mb-4">
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              📅 분석월 <span className="text-red-500">*</span>
            </label>
            <input
              type="month"
              value={analysisMonth}
              onChange={(e) => setAnalysisMonth(e.target.value)}
              className="w-full px-4 py-3 border-2 border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent text-slate-800 font-medium bg-white"
            />
            <p className="mt-1 text-xs text-slate-500">
              업데이트 일자를 선택하면 자동으로 계산됩니다. 필요시 수동으로 변경할 수 있습니다.
            </p>
          </div>
          
          <div className="mb-4">
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              📆 업데이트 일자 <span className="text-red-500">*</span>
            </label>
            <div className="flex items-center gap-3">
              <div className="flex-1 relative" ref={calendarRef}>
                <input
                  type="text"
                  readOnly
                  value={selectedDate || '날짜를 선택하세요'}
                  onClick={() => setShowCalendar(!showCalendar)}
                  className="w-full px-4 py-3 border-2 border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent text-slate-800 font-medium cursor-pointer bg-white"
                  placeholder="날짜 선택"
                />
                {showCalendar && (
                  <div className="absolute top-full left-0 mt-2 bg-white rounded-lg shadow-xl border-2 border-slate-200 p-4 z-50 min-w-[320px]">
                    {/* 달력 헤더 */}
                    <div className="flex items-center justify-between mb-4">
                      <button
                        onClick={handlePrevMonth}
                        className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
                      >
                        <span className="text-slate-600">‹</span>
                      </button>
                      <h3 className="text-lg font-semibold text-slate-800">
                        {currentMonth.getFullYear()}년 {currentMonth.getMonth() + 1}월
                      </h3>
                      <button
                        onClick={handleNextMonth}
                        className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
                      >
                        <span className="text-slate-600">›</span>
                      </button>
                    </div>
                    
                    {/* 요일 헤더 */}
                    <div className="grid grid-cols-7 gap-1 mb-2">
                      {['일', '월', '화', '수', '목', '금', '토'].map((day, index) => (
                        <div
                          key={index}
                          className="text-center text-xs font-semibold text-slate-600 py-2"
                        >
                          {day}
                        </div>
                      ))}
                    </div>
                    
                    {/* 날짜 그리드 */}
                    <div className="grid grid-cols-7 gap-1">
                      {generateCalendarDays().map((day, index) => (
                        <button
                          key={index}
                          onClick={() => handleDateClick(day.dateStr)}
                          disabled={!day.isAvailable}
                          className={`
                            relative p-2 rounded-lg text-sm transition-all
                            ${!day.isCurrentMonth ? 'text-slate-300' : 'text-slate-800'}
                            ${day.isAvailable 
                              ? 'hover:bg-indigo-100 cursor-pointer' 
                              : 'cursor-not-allowed opacity-50'
                            }
                            ${day.isSelected 
                              ? 'bg-indigo-600 text-white font-semibold' 
                              : ''
                            }
                          `}
                        >
                          {day.date.getDate()}
                          {day.isAvailable && !day.isSelected && (
                            <span className="absolute bottom-1 left-1/2 transform -translate-x-1/2 w-1.5 h-1.5 bg-purple-500 rounded-full"></span>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
              <div className="text-slate-600 text-sm">
                📅
              </div>
            </div>
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
              <h3 className="text-base font-bold text-slate-800 mb-3">📅 업데이트일자 안내</h3>
              <p className="text-sm text-slate-700 leading-relaxed mb-3">
                분석월의 1일 ~ 업데이트 일자 전 일까지의 분석내용을 제공합니다.
              </p>
              <p className="text-sm text-slate-600 mb-2">
                <strong className="text-indigo-700">예시:</strong> 업데이트일자에 <strong className="text-indigo-700">2025.11.24</strong>을 선택하면
              </p>
              <p className="text-sm text-slate-700 font-medium">
                → <span className="text-indigo-700 font-bold">2025-11-01 (월) ~ 2025-11-23 (일)</span> 의 누적 매출에 대한 분석 자료가 표시됩니다.
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
