import { NextResponse } from 'next/server';

/**
 * 업데이트일자로부터 주차 정보 계산
 * GET /api/calculate-date-info?date=20251117
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const dateParam = searchParams.get('date'); // YYYYMMDD 형식
    
    if (!dateParam || dateParam.length !== 8) {
      return NextResponse.json(
        { success: false, error: '날짜 형식이 올바르지 않습니다. (YYYYMMDD 형식 필요)' },
        { status: 400 }
      );
    }
    
    // YYYYMMDD -> Date 객체 변환
    const year = parseInt(dateParam.slice(0, 4));
    const month = parseInt(dateParam.slice(4, 6)) - 1; // 월은 0부터 시작
    const day = parseInt(dateParam.slice(6, 8));
    const updateDate = new Date(year, month, day);
    
    // 업데이트일자는 다음주 월요일이므로, 실제 주차는 전주 월요일~일요일
    // 업데이트일자의 이전 주 월요일 찾기
    const dayOfWeek = updateDate.getDay(); // 0=일요일, 1=월요일, ..., 6=토요일
    
    // 업데이트일자가 월요일이면, 전주 월요일은 7일 전
    // 업데이트일자가 다른 요일이면, 가장 가까운 전주 월요일 찾기
    let daysToMonday = 0;
    if (dayOfWeek === 1) {
      // 월요일이면 7일 전 (전주 월요일)
      daysToMonday = 7;
    } else if (dayOfWeek === 0) {
      // 일요일이면 6일 전 (전주 월요일)
      daysToMonday = 6;
    } else {
      // 화~토요일이면 (dayOfWeek - 1) + 7일 전
      // 예: 화요일(2)이면 (2-1) + 7 = 8일 전
      daysToMonday = (dayOfWeek - 1) + 7;
    }
    
    const weekStartDate = new Date(updateDate);
    weekStartDate.setDate(updateDate.getDate() - daysToMonday);
    
    const weekEndDate = new Date(weekStartDate);
    weekEndDate.setDate(weekStartDate.getDate() + 6); // 일요일
    
    // 주차 시작일이 월요일인지 확인 (디버깅용)
    const weekStartDayOfWeek = weekStartDate.getDay();
    if (weekStartDayOfWeek !== 1) {
      console.warn(`경고: 주차 시작일이 월요일이 아닙니다. (요일: ${weekStartDayOfWeek})`);
    }
    
    // 당월 (YYYY-MM 형식)
    const currentMonth = `${year}-${String(month + 1).padStart(2, '0')}`;
    
    // 전년동주차 (정확히 52주 전, 즉 364일 전)
    // 52주 = 364일 (7일 * 52)
    const prevYearWeekStart = new Date(weekStartDate);
    prevYearWeekStart.setDate(prevYearWeekStart.getDate() - 364);
    
    const prevYearWeekEnd = new Date(weekEndDate);
    prevYearWeekEnd.setDate(prevYearWeekEnd.getDate() - 364);
    
    // 날짜 포맷팅 함수
    const formatDate = (date: Date): string => {
      const y = date.getFullYear();
      const m = String(date.getMonth() + 1).padStart(2, '0');
      const d = String(date.getDate()).padStart(2, '0');
      return `${y}-${m}-${d}`;
    };
    
    const formatDateDot = (date: Date): string => {
      const y = date.getFullYear();
      const m = String(date.getMonth() + 1).padStart(2, '0');
      const d = String(date.getDate()).padStart(2, '0');
      return `${y}.${m}.${d}`;
    };
    
    return NextResponse.json({
      success: true,
      updateDate: formatDate(updateDate),
      updateDateFormatted: formatDateDot(updateDate),
      week: {
        start: formatDate(weekStartDate),
        end: formatDate(weekEndDate),
        startFormatted: formatDateDot(weekStartDate),
        endFormatted: formatDateDot(weekEndDate),
        display: `${formatDateDot(weekStartDate)} ~ ${formatDateDot(weekEndDate)}`
      },
      currentMonth: currentMonth,
      prevYearWeek: {
        start: formatDate(prevYearWeekStart),
        end: formatDate(prevYearWeekEnd),
        startFormatted: formatDateDot(prevYearWeekStart),
        endFormatted: formatDateDot(prevYearWeekEnd),
        display: `${formatDateDot(prevYearWeekStart)} ~ ${formatDateDot(prevYearWeekEnd)}`
      }
    });
    
  } catch (error: any) {
    console.error('날짜 정보 계산 오류:', error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

