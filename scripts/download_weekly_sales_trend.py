"""
주차별 매출추세 데이터를 Snowflake에서 조회하여 CSV로 다운로드하는 스크립트

사용법:
    python scripts/download_weekly_sales_trend.py [업데이트일자]
    
예시:
    python scripts/download_weekly_sales_trend.py 2025-11-24
    python scripts/download_weekly_sales_trend.py  # 오늘 날짜 사용
    
설명:
    업데이트일자를 기준으로 이전 주차까지의 9주치 매출 데이터를 조회합니다.
    - 당년 매출과 전년 동주차 매출을 모두 조회
    - X축에는 주차종료 일요일 날짜 표시
    - YOY(전년 대비 성장률) 계산
    
환경 변수:
    SNOWFLAKE_ACCOUNT: Snowflake 계정명
    SNOWFLAKE_USERNAME: 사용자명
    SNOWFLAKE_PASSWORD: 비밀번호
    SNOWFLAKE_WAREHOUSE: 웨어하우스명
    SNOWFLAKE_DATABASE: 데이터베이스명
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# path_utils 임포트
from scripts.path_utils import get_plan_file_path, extract_year_month_from_date

# .env 파일 로드
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    print("⚠️ .env 파일을 찾을 수 없습니다. 환경 변수에서 직접 읽습니다.")


def get_snowflake_connection():
    """
    Snowflake 데이터베이스 연결 생성
    
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake 연결 객체
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            network_timeout=None,  # 타임아웃 없음
            login_timeout=60,      # 로그인 1분 타임아웃
            session_parameters={
                'QUERY_TAG': 'weekly_sales_trend',
                'STATEMENT_TIMEOUT_IN_SECONDS': 3600  # 쿼리 1시간 타임아웃
            }
        )
        print("✅ Snowflake 연결 성공!")
        return conn
    except Exception as e:
        print(f"❌ Snowflake 연결 실패: {e}")
        raise


def load_channel_master() -> dict:
    """
    채널마스터 파일을 로드하여 유통채널 코드 → 채널명 매핑 딕셔너리 반환
    
    Returns:
        dict: {유통채널코드: 채널명} 매핑 딕셔너리
    """
    master_path = project_root / "Master" / "채널마스터.csv"
    
    # 기본 매핑 (파일이 없을 경우 사용)
    default_mapping = {
        'RF': 'RF',
        '01': '백화점',
        '02': '면세점',
        '03': '직영점',
        '04': '자사몰',
        '05': '제휴몰',
        '06': '대리점',
        '07': '아울렛',
        '08': '사입',
        '09': '수출',
        '11': '직영몰',
        '12': '아울렛',
        '99': '기타'
    }
    
    if not master_path.exists():
        print(f"⚠️ 채널마스터 파일을 찾을 수 없습니다: {master_path}")
        print("   기본 매핑을 사용합니다.")
        return default_mapping
    
    try:
        master_df = pd.read_csv(master_path, encoding='utf-8-sig')
        
        # 채널번호 → 채널명 매핑 생성
        channel_mapping = {}
        for _, row in master_df.iterrows():
            channel_code = str(row['채널번호']).strip()
            channel_name = str(row['채널명']).strip()
            
            # 숫자인 경우 앞에 0 붙이기 (1 → 01)
            if channel_code.isdigit() and len(channel_code) == 1:
                channel_code = f"0{channel_code}"
            
            channel_mapping[channel_code] = channel_name
        
        print(f"✅ 채널마스터 로드 완료: {len(channel_mapping)}개 채널")
        return channel_mapping
    
    except Exception as e:
        print(f"⚠️ 채널마스터 로드 실패: {e}")
        print("   기본 매핑을 사용합니다.")
        return default_mapping


def map_channel_name(df: pd.DataFrame, channel_mapping: dict) -> pd.DataFrame:
    """
    유통채널 코드를 채널명으로 매핑
    
    Args:
        df: 데이터프레임 (유통채널 컬럼 포함)
        channel_mapping: 채널코드 → 채널명 매핑 딕셔너리
    
    Returns:
        pd.DataFrame: 채널명 컬럼이 추가된 데이터프레임
    """
    df = df.copy()
    df['채널명'] = df['유통채널'].map(channel_mapping).fillna('기타')
    return df


def calculate_week_end_dates(update_date: datetime, weeks: int = 9) -> tuple:
    """
    업데이트일자 기준으로 이전 9주차의 주차 종료일(일요일) 목록 계산
    
    업데이트일자가 월요일인 경우:
    - 당일이 속한 주의 직전 주(전주차)까지 분석
    - 예: 2025-11-24(월) → 11/23까지 분석 (11/17~11/23 주차 포함)
    
    Args:
        update_date: 업데이트일자 (예: 2025-11-24)
        weeks: 가져올 주차 수 (기본 9주)
    
    Returns:
        tuple: (주차종료일 리스트, 시작일, 종료일)
    """
    # 업데이트일자의 요일 확인 (0=월요일, 6=일요일)
    weekday = update_date.weekday()
    
    # 마지막 분석 대상 주차의 일요일 찾기
    # 업데이트일자가 월요일(0)이면, 전주 일요일이 마지막 분석 주의 종료일
    if weekday == 0:  # 월요일
        # 전주 일요일 = 업데이트일자 - 1일
        last_sunday = update_date - timedelta(days=1)
    else:
        # 그 외의 경우, 이전 완료된 주의 일요일 찾기
        days_since_sunday = (weekday + 1) % 7
        last_sunday = update_date - timedelta(days=days_since_sunday + 7)
    
    # 9주차의 종료일(일요일) 리스트 생성 (최신 순)
    week_end_dates = []
    for i in range(weeks):
        end_date = last_sunday - timedelta(days=7 * i)
        week_end_dates.append(end_date)
    
    # 오래된 순으로 정렬
    week_end_dates.sort()
    
    # 시작일 = 가장 오래된 주의 월요일 (일요일 - 6일)
    start_date = week_end_dates[0] - timedelta(days=6)
    
    # 종료일 = 가장 최신 주의 일요일
    end_date = week_end_dates[-1]
    
    return week_end_dates, start_date, end_date


def get_weekly_sales_query(start_date: datetime, end_date: datetime) -> str:
    """
    주차별 매출추세 조회 쿼리 생성
    
    Args:
        start_date: 조회 시작일 (첫 주 월요일)
        end_date: 조회 종료일 (마지막 주 일요일)
    
    Returns:
        str: SQL 쿼리
    """
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    query = f"""
WITH weeks AS (
    SELECT DISTINCT END_DT
    FROM FNF.PRCS.DB_SH_S_W
    WHERE END_DT BETWEEN '{start_str}'::DATE AND '{end_str}'::DATE
),

curr AS (  -- 당년
    SELECT
        s.BRD_CD AS "브랜드",
        w.END_DT AS "종료일",
        CASE
            WHEN s.BRD_CD = 'M'
             AND s.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                THEN 'RF'
            ELSE sh.DIST_TYPE_SAP
        END AS "유통채널",
        SUM(
            CASE
                WHEN sh.DIST_TYPE_SAP IN ('08','99')
                    THEN (s.DELV_NML_SUPP_AMT + s.DELV_RET_SUPP_AMT)
                ELSE (s.SALE_NML_SALE_AMT + s.SALE_RET_SALE_AMT)
            END
        ) AS "실판매출"
    FROM weeks w
    JOIN FNF.PRCS.DB_SH_S_W s
      ON s.END_DT = w.END_DT
    JOIN FNF.PRCS.DB_SHOP sh
      ON sh.BRD_CD = s.BRD_CD
     AND sh.SHOP_ID = s.SHOP_ID
     AND sh.ANAL_CNTRY = 'KO'
    WHERE s.BRD_CD != 'A'
    GROUP BY 1,2,3
    HAVING SUM(
            CASE
                WHEN sh.DIST_TYPE_SAP IN ('08','99')
                    THEN (s.DELV_NML_SUPP_AMT + s.DELV_RET_SUPP_AMT)
                ELSE (s.SALE_NML_SALE_AMT + s.SALE_RET_SALE_AMT)
            END
        ) <> 0
),

prev AS (  -- 전년 동주차
    SELECT
        s.BRD_CD AS "브랜드",
        w.END_DT AS "종료일",
        CASE
            WHEN s.BRD_CD = 'M'
             AND s.SHOP_ID IN ('649','155','524','526','82','744','6048','954')
                THEN 'RF'
            ELSE sh.DIST_TYPE_SAP
        END AS "유통채널",
        SUM(
            CASE
                WHEN sh.DIST_TYPE_SAP IN ('08','99')
                    THEN (s.DELV_NML_SUPP_AMT + s.DELV_RET_SUPP_AMT)
                ELSE (s.SALE_NML_SALE_AMT + s.SALE_RET_SALE_AMT)
            END
        ) AS "실판매출"
    FROM weeks w
    JOIN FNF.PRCS.DB_SH_S_W s
      ON s.END_DT = DATE_TRUNC('WEEK', DATEADD(YEAR, -1, w.END_DT)) + 6
    JOIN FNF.PRCS.DB_SHOP sh
      ON sh.BRD_CD = s.BRD_CD
     AND sh.SHOP_ID = s.SHOP_ID
     AND sh.ANAL_CNTRY = 'KO'
    WHERE s.BRD_CD != 'A'
    GROUP BY 1,2,3
    HAVING SUM(
            CASE
                WHEN sh.DIST_TYPE_SAP IN ('08','99')
                    THEN (s.DELV_NML_SUPP_AMT + s.DELV_RET_SUPP_AMT)
                ELSE (s.SALE_NML_SALE_AMT + s.SALE_RET_SALE_AMT)
            END
        ) <> 0
)

SELECT  "브랜드",
        '당년' AS "구분",
        "종료일",
        "유통채널",
        "실판매출"
FROM curr
UNION ALL
SELECT  "브랜드",
        '전년' AS "구분",
        "종료일",
        "유통채널",
        "실판매출"
FROM prev
ORDER BY "종료일", "브랜드", "유통채널", "구분"
"""
    
    return query


def execute_query_to_dataframe(conn, query: str):
    """
    Snowflake 쿼리 실행 및 결과를 pandas DataFrame으로 반환
    
    Args:
        conn: Snowflake 연결 객체
        query: 실행할 SQL 쿼리
        
    Returns:
        pd.DataFrame: 쿼리 결과
    """
    import time
    import sys
    try:
        print("📊 쿼리 실행 중...", flush=True)
        print("   (대용량 데이터 조회 시 수 분이 소요될 수 있습니다)", flush=True)
        
        cursor = conn.cursor()
        
        # 쿼리 실행 시작 시간
        start_time = time.time()
        print("   쿼리 전송 중...", flush=True)
        sys.stdout.flush()  # 강제로 출력 버퍼 비우기
        
        cursor.execute(query)
        
        exec_time = time.time() - start_time
        print(f"   쿼리 실행 완료 ({exec_time:.1f}초)", flush=True)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 데이터 가져오기
        print("📥 데이터 가져오는 중...", flush=True)
        sys.stdout.flush()
        fetch_start = time.time()
        
        # 배치로 데이터 가져오기 (메모리 효율성)
        batch_size = 10000
        all_data = []
        batch_count = 0
        
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            all_data.extend(batch)
            batch_count += 1
            if batch_count % 10 == 0:  # 10만 건마다 진행 상황 표시
                print(f"   진행 중... {len(all_data):,}건 조회됨", flush=True)
                sys.stdout.flush()
        
        fetch_time = time.time() - fetch_start
        print(f"   데이터 가져오기 완료 ({fetch_time:.1f}초)", flush=True)
        
        # DataFrame 생성
        df = pd.DataFrame(all_data, columns=columns)
        
        cursor.close()
        print(f"✅ 총 {len(df):,}건의 데이터를 조회했습니다.", flush=True)
        print(f"   전체 소요 시간: {time.time() - start_time:.1f}초", flush=True)
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        print(f"   오류 타입: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise


def process_weekly_sales_data(df: pd.DataFrame, channel_mapping: dict) -> pd.DataFrame:
    """
    주차별 매출 데이터 처리 - 채널명 매핑 추가
    
    Args:
        df: 원시 데이터 DataFrame
        channel_mapping: 채널코드 → 채널명 매핑 딕셔너리
    
    Returns:
        pd.DataFrame: 채널명이 추가된 데이터프레임
    """
    print("\n📈 데이터 처리 중...")
    
    # 채널명 매핑 추가
    df = map_channel_name(df, channel_mapping)
    
    # 컬럼 순서 정리: 브랜드, 구분, 종료일, 유통채널, 채널명, 실판매출
    df = df[['브랜드', '구분', '종료일', '유통채널', '채널명', '실판매출']]
    
    # 정렬: 종료일, 브랜드, 유통채널, 구분(당년 먼저)
    df['구분정렬'] = df['구분'].map({'당년': 0, '전년': 1})
    df = df.sort_values(['종료일', '브랜드', '유통채널', '구분정렬'])
    df = df.drop(columns=['구분정렬'])
    
    return df


def save_to_csv(df: pd.DataFrame, output_path: Path, description: str = ""):
    """
    DataFrame을 CSV 파일로 저장
    
    Args:
        df: 저장할 DataFrame
        output_path: 저장할 파일 경로
        description: 파일 설명
    """
    try:
        # 디렉토리가 없으면 생성
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # CSV로 저장 (UTF-8 with BOM for Excel compatibility)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✅ {description} 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        raise


def save_to_js(df: pd.DataFrame, output_path: Path, update_date: datetime, 
               week_end_dates: list, channel_mapping: dict, description: str = ""):
    """
    DataFrame을 JavaScript 파일로 저장
    
    Args:
        df: 저장할 DataFrame
        output_path: 저장할 파일 경로
        update_date: 업데이트 일자
        week_end_dates: 주차 종료일 리스트
        channel_mapping: 채널 매핑 딕셔너리
        description: 파일 설명
    """
    import json
    
    try:
        # 디렉토리가 없으면 생성
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 데이터 변환
        df_copy = df.copy()
        df_copy['종료일'] = df_copy['종료일'].astype(str)
        data_list = df_copy.to_dict('records')
        
        # 주차 표시 리스트
        weeks = [f"{d.month}/{d.day}" for d in week_end_dates]
        
        # 브랜드별, 채널명별 집계 데이터 생성
        brand_summary = {}
        for brand in df['브랜드'].unique():
            brand_df = df[df['브랜드'] == brand]
            brand_summary[brand] = {
                'weekly': {},
                'channels': {}
            }
            
            # 주차별 합계
            for end_date in week_end_dates:
                date_str = str(end_date.date())
                week_label = f"{end_date.month}/{end_date.day}"
                week_df = brand_df[brand_df['종료일'].astype(str) == date_str]
                
                curr = week_df[week_df['구분'] == '당년']['실판매출'].sum()
                prev = week_df[week_df['구분'] == '전년']['실판매출'].sum()
                yoy = round((curr - prev) / prev * 100, 2) if prev != 0 else 0
                
                brand_summary[brand]['weekly'][week_label] = {
                    '당년': int(curr),
                    '전년': int(prev),
                    'YOY': yoy
                }
            
            # 채널별 합계
            for channel_name in brand_df['채널명'].unique():
                ch_df = brand_df[brand_df['채널명'] == channel_name]
                curr = ch_df[ch_df['구분'] == '당년']['실판매출'].sum()
                prev = ch_df[ch_df['구분'] == '전년']['실판매출'].sum()
                yoy = round((curr - prev) / prev * 100, 2) if prev != 0 else 0
                
                brand_summary[brand]['channels'][channel_name] = {
                    '당년': int(curr),
                    '전년': int(prev),
                    'YOY': yoy
                }
        
        # 전체 집계
        total_summary = {'weekly': {}, 'channels': {}}
        for end_date in week_end_dates:
            date_str = str(end_date.date())
            week_label = f"{end_date.month}/{end_date.day}"
            week_df = df[df['종료일'].astype(str) == date_str]
            
            curr = week_df[week_df['구분'] == '당년']['실판매출'].sum()
            prev = week_df[week_df['구분'] == '전년']['실판매출'].sum()
            yoy = round((curr - prev) / prev * 100, 2) if prev != 0 else 0
            
            total_summary['weekly'][week_label] = {
                '당년': int(curr),
                '전년': int(prev),
                'YOY': yoy
            }
        
        for channel_name in df['채널명'].unique():
            ch_df = df[df['채널명'] == channel_name]
            curr = ch_df[ch_df['구분'] == '당년']['실판매출'].sum()
            prev = ch_df[ch_df['구분'] == '전년']['실판매출'].sum()
            yoy = round((curr - prev) / prev * 100, 2) if prev != 0 else 0
            
            total_summary['channels'][channel_name] = {
                '당년': int(curr),
                '전년': int(prev),
                'YOY': yoy
            }
        
        # JavaScript 객체 구조
        js_data = {
            'updateDate': update_date.strftime('%Y-%m-%d'),
            'period': {
                'start': str(week_end_dates[0].date() - timedelta(days=6)),
                'end': str(week_end_dates[-1].date())
            },
            'weeks': weeks,
            'channelMapping': channel_mapping,
            'brands': list(df['브랜드'].unique()),
            'channels': list(df['채널명'].unique()),
            'summary': {
                'total': total_summary,
                'byBrand': brand_summary
            },
            'rawData': data_list
        }
        
        # JavaScript 파일 내용 생성
        js_content = f"""// 주차별 매출추세 데이터
// 자동 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 업데이트일자: {update_date.strftime('%Y-%m-%d')}

const weeklySalesTrend = {json.dumps(js_data, ensure_ascii=False, indent=2)};

// Dashboard.html에서 사용
if (typeof window !== 'undefined') {{
    window.weeklySalesTrend = weeklySalesTrend;
}}

// Node.js 환경 지원
if (typeof module !== 'undefined' && module.exports) {{
    module.exports = weeklySalesTrend;
}}
"""
        
        # JavaScript 파일 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(js_content)
        
        print(f"✅ {description} 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024:.2f} KB")
        
        # JSON 파일도 함께 저장 (public/data/YYYYMMDD/weekly_trend.json)
        date_param = update_date.strftime('%Y%m%d')
        json_dir = Path(os.path.dirname(os.path.dirname(__file__))) / "public" / "data" / date_param
        json_dir.mkdir(parents=True, exist_ok=True)
        json_path = json_dir / "weekly_trend.json"
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(js_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON 저장 완료: {json_path}")
        print(f"   파일 크기: {json_path.stat().st_size / 1024:.2f} KB")
        
    except Exception as e:
        print(f"❌ JS 저장 실패: {e}")
        raise


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='주차별 매출추세 데이터를 Snowflake에서 조회하여 CSV로 다운로드',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/download_weekly_sales_trend.py 2025-11-24
  python scripts/download_weekly_sales_trend.py  # 오늘 날짜 사용
  
설명:
  업데이트일자를 기준으로 이전 주차까지의 9주치 매출 데이터를 조회합니다.
  - 당년 매출과 전년 동주차 매출을 모두 조회
  - X축에는 주차종료 일요일 날짜 표시
  - YOY(전년 대비 성장률) 계산
        """
    )
    
    parser.add_argument(
        'update_date',
        type=str,
        nargs='?',
        default=None,
        help='업데이트일자 (예: 2025-11-24, 기본값: 오늘)'
    )
    
    parser.add_argument(
        '--weeks',
        type=int,
        default=9,
        help='분석할 주차 수 (기본값: 9)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='출력 디렉토리 경로 (기본값: raw/YYYYMM/ETC)'
    )
    
    args = parser.parse_args()
    
    # 업데이트일자 파싱
    if args.update_date:
        try:
            update_date = datetime.strptime(args.update_date, '%Y-%m-%d')
        except ValueError:
            print(f"❌ 날짜 형식이 올바르지 않습니다: {args.update_date} (YYYY-MM-DD 형식 필요)")
            sys.exit(1)
    else:
        update_date = datetime.now()
    
    # 주차 종료일 계산
    week_end_dates, start_date, end_date = calculate_week_end_dates(update_date, args.weeks)
    
    print("=" * 70)
    print("📊 주차별 매출추세 데이터 다운로드")
    print("=" * 70)
    print(f"📅 업데이트일자: {update_date.strftime('%Y-%m-%d')} ({['월','화','수','목','금','토','일'][update_date.weekday()]})")
    print(f"📅 분석 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"📅 분석 주차 수: {args.weeks}주")
    print(f"📅 X축 표시 (주차 종료 일요일):")
    for i, d in enumerate(week_end_dates, 1):
        print(f"   {i}. {d.strftime('%Y-%m-%d')} ({d.month}/{d.day})")
    print()
    
    conn = None
    try:
        # 채널마스터 로드
        channel_mapping = load_channel_master()
        
        # Snowflake 연결
        conn = get_snowflake_connection()
        
        # 웨어하우스 상태 확인
        print("\n🏭 웨어하우스 상태 확인 중...")
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
            wh_info = cursor.fetchone()
            print(f"   웨어하우스: {wh_info[0]}")
            print(f"   데이터베이스: {wh_info[1]}")
            cursor.close()
        except Exception as e:
            print(f"   ⚠️ 상태 확인 실패: {e}")
        
        # 쿼리 생성 및 실행
        query = get_weekly_sales_query(start_date, end_date)
        print("\n📝 생성된 쿼리:")
        print("-" * 50)
        print(query[:500] + "..." if len(query) > 500 else query)
        print("-" * 50)
        
        df = execute_query_to_dataframe(conn, query)
        
        if df.empty:
            print("⚠️ 조회된 데이터가 없습니다.")
            sys.exit(0)
        
        # 데이터 처리 (채널 매핑 포함)
        result_df = process_weekly_sales_data(df, channel_mapping)
        
        # 출력 디렉토리 결정 (평가월 사용)
        if args.output_dir:
            output_dir = Path(args.output_dir)
        else:
            # 업데이트 날짜를 YYYYMMDD 형식으로 변환
            date_str = update_date.strftime('%Y%m%d')
            # 평가월(analysis_month) 추출 (metadata.json에서 읽거나 계산)
            analysis_month = extract_year_month_from_date(date_str)
            output_dir = project_root / "raw" / analysis_month / "ETC"
        
        # 파일명에 사용할 날짜
        date_suffix = update_date.strftime('%Y%m%d')
        
        # CSV 파일 저장
        print("\n" + "=" * 70)
        print("💾 파일 저장")
        print("=" * 70)
        
        # CSV 파일 저장
        save_to_csv(
            result_df,
            output_dir / f"weekly_sales_trend_{date_suffix}.csv",
            "주차별 매출추세 (CSV)"
        )
        
        # JS 파일 저장 (public 폴더에)
        js_output_path = project_root / "public" / f"weekly_sales_trend_{date_suffix}.js"
        save_to_js(
            result_df,
            js_output_path,
            update_date,
            week_end_dates,
            channel_mapping,
            "주차별 매출추세 (JS)"
        )
        
        # 결과 요약 출력
        print("\n" + "=" * 70)
        print("📊 데이터 요약")
        print("=" * 70)
        
        print(f"\n📋 총 데이터 건수: {len(result_df):,}건")
        
        print("\n🏷️ 브랜드 목록:")
        brands = result_df['브랜드'].unique()
        for brand in sorted(brands):
            print(f"   - {brand}")
        
        print("\n🏪 유통채널 → 채널명 매핑:")
        channel_df = result_df[['유통채널', '채널명']].drop_duplicates()
        for _, row in channel_df.sort_values('유통채널').iterrows():
            print(f"   - {row['유통채널']} → {row['채널명']}")
        
        # 주차별 전체 매출 계산
        print("\n📈 주차별 전체 매출 (백만원):")
        print(f"   주차: {' | '.join([f'{d.month}/{d.day}' for d in week_end_dates])}")
        
        # 당년
        curr_df = result_df[result_df['구분'] == '당년'].copy()
        # 종료일을 문자열로 변환하여 일관성 유지
        curr_df['종료일_str'] = curr_df['종료일'].astype(str)
        curr_by_week = curr_df.groupby('종료일_str')['실판매출'].sum() / 1_000_000
        curr_values = [str(round(curr_by_week.get(d.strftime('%Y-%m-%d'), 0), 1)) for d in week_end_dates]
        print(f"   당년: {' | '.join(curr_values)}")
        
        # 전년
        prev_df = result_df[result_df['구분'] == '전년'].copy()
        # 종료일을 문자열로 변환하여 일관성 유지
        prev_df['종료일_str'] = prev_df['종료일'].astype(str)
        prev_by_week = prev_df.groupby('종료일_str')['실판매출'].sum() / 1_000_000
        prev_values = [str(round(prev_by_week.get(d.strftime('%Y-%m-%d'), 0), 1)) for d in week_end_dates]
        print(f"   전년: {' | '.join(prev_values)}")
        
        print("\n" + "=" * 70)
        print("✅ 다운로드 완료!")
        print("=" * 70)
        
    except Exception as e:
        print()
        print("=" * 70)
        print(f"❌ 오류 발생: {e}")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\n🔌 Snowflake 연결 종료")


if __name__ == "__main__":
    main()

