/**
 * 날짜 유틸리티 함수 (한국 시간 기준)
 * 
 * 모든 날짜 계산은 한국 시간(KST, UTC+9) 기준으로 수행됩니다.
 */

/**
 * 한국 시간 기준 오늘 날짜 반환 (YYYY-MM-DD)
 * UTC 시간에 9시간을 더해서 한국 날짜 계산
 */
export function getToday(): string {
  const now = new Date();
  // UTC 시간을 한국 시간으로 변환 (UTC + 9시간)
  const kstTime = new Date(now.getTime() + (9 * 60 * 60 * 1000));
  
  // UTC 기준으로 년/월/일 추출 (이미 +9시간 된 시간이므로)
  const year = kstTime.getUTCFullYear();
  const month = String(kstTime.getUTCMonth() + 1).padStart(2, '0');
  const day = String(kstTime.getUTCDate()).padStart(2, '0');
  
  return `${year}-${month}-${day}`;
}

/**
 * 한국 시간 기준 현재 Date 객체 반환 (내부 사용)
 */
function getKSTNow(): Date {
  const now = new Date();
  return new Date(now.getTime() + (9 * 60 * 60 * 1000));
}

/**
 * 한국 시간 기준 오늘 날짜 반환 (YYYYMMDD)
 */
export function getTodayCompact(): string {
  return getToday().replace(/-/g, '');
}

/**
 * 한국 시간 기준 어제 날짜 반환 (YYYY-MM-DD)
 */
export function getYesterday(): string {
  const now = new Date();
  // UTC 시간을 한국 시간으로 변환 후 하루 빼기
  const kstTime = new Date(now.getTime() + (9 * 60 * 60 * 1000) - (24 * 60 * 60 * 1000));
  
  const year = kstTime.getUTCFullYear();
  const month = String(kstTime.getUTCMonth() + 1).padStart(2, '0');
  const day = String(kstTime.getUTCDate()).padStart(2, '0');
  
  return `${year}-${month}-${day}`;
}

/**
 * 한국 시간 기준 현재 월 반환 (YYYY-MM)
 */
export function getCurrentMonth(): string {
  const now = new Date();
  const kstTime = new Date(now.getTime() + (9 * 60 * 60 * 1000));
  
  const year = kstTime.getUTCFullYear();
  const month = String(kstTime.getUTCMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
}

/**
 * 분석월 기준 asof_dt 계산 (한국 시간 기준)
 * - 분석월이 업데이트 일자의 월과 같으면: 업데이트 일자의 전날까지
 * - 분석월이 업데이트 일자의 월보다 과거면: 해당 월의 말일까지
 * 
 * @param analysisMonth YYYY-MM 형식의 분석월
 * @param updateDate YYYY-MM-DD 형식의 업데이트 일자 (선택, 없으면 오늘 날짜 사용)
 * @returns YYYY-MM-DD 형식의 기준일
 */
export function calculateAsofDate(analysisMonth: string, updateDate?: string): string {
  const [analysisYear, analysisMonthNum] = analysisMonth.split('-').map(Number);
  
  // 업데이트 일자 파싱 (없으면 오늘 날짜 사용)
  let updateYear: number;
  let updateMonth: number;
  let updateDay: number;
  
  if (updateDate) {
    [updateYear, updateMonth, updateDay] = updateDate.split('-').map(Number);
  } else {
    // 한국 시간 기준 오늘 날짜
    const now = new Date();
    const kstTime = new Date(now.getTime() + (9 * 60 * 60 * 1000));
    updateYear = kstTime.getUTCFullYear();
    updateMonth = kstTime.getUTCMonth() + 1;
    updateDay = kstTime.getUTCDate();
  }
  
  // 분석월이 업데이트 일자의 월과 같은 경우: 업데이트 일자의 전날까지
  if (analysisYear === updateYear && analysisMonthNum === updateMonth) {
    // 업데이트 일자의 전날 계산
    const updateDateObj = new Date(Date.UTC(updateYear, updateMonth - 1, updateDay));
    const prevDay = new Date(updateDateObj.getTime() - (24 * 60 * 60 * 1000));
    const y = prevDay.getUTCFullYear();
    const m = String(prevDay.getUTCMonth() + 1).padStart(2, '0');
    const d = String(prevDay.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  
  // 분석월이 업데이트 일자의 월보다 과거인 경우: 해당 월의 말일
  const lastDay = new Date(Date.UTC(analysisYear, analysisMonthNum, 0));
  const y = lastDay.getUTCFullYear();
  const m = String(lastDay.getUTCMonth() + 1).padStart(2, '0');
  const d = String(lastDay.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/**
 * Date 객체를 YYYY-MM-DD 형식으로 변환
 */
export function formatDate(date: any): string {
  if (!date) return '';
  if (typeof date === 'string') return date;
  
  if (date instanceof Date) {
    // UTC 기준으로 날짜 추출
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
  
  return String(date);
}

/**
 * ISO 타임스탬프 반환 (한국 시간 기준)
 */
export function getKSTTimestamp(): string {
  const now = new Date();
  const kstTime = new Date(now.getTime() + (9 * 60 * 60 * 1000));
  return kstTime.toISOString();
}
