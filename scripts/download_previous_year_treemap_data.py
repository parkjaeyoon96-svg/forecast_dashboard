"""
트리맵 전년비 데이터를 Snowflake에서 조회 및 전처리하여 CSV로 저장하는 스크립트

사용법:
    python scripts/download_previous_year_treemap_data.py 20251215
    
설명:
    업데이트일자(YYYYMMDD)를 입력하면:
    - 당년 기간: 2025-12-01 ~ 2025-12-14
    - 전년 기간: 2024-12-01 ~ 2024-12-15 (업데이트일자 포함)
    
로직:
    1. Snowflake에서 전년 동주차일까지 데이터 다운로드 (DW_COPA_D)
    2. 마스터 기반 전처리:
       - 채널명 매핑 (RF 처리 포함)
       - 아이템 중분류/소분류 매핑
    3. 브랜드별, 채널별, 아이템별 집계
    4. CSV 파일로 저장
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# 출력 인코딩 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    print("⚠️ .env 파일을 찾을 수 없습니다. 환경 변수에서 직접 읽습니다.")

def get_snowflake_connection():
    """Snowflake 데이터베이스 연결 생성"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("✅ Snowflake 연결 성공!")
        return conn
    except Exception as e:
        print(f"❌ Snowflake 연결 실패: {e}")
        raise

def calculate_previous_year_period(update_date_str: str):
    """
    업데이트일자로부터 전년 동기간 계산
    
    당년: 분석월의 1일 ~ 업데이트일의 D-1일 (단, 분석월 말일 초과 시 말일로 제한)
    전년: 당년과 동일 기간 (전년도 동일 월)
    
    예: 
    - 업데이트일: 20260105, 분석월: 202512
    - 당년: 2025-12-01 ~ 2025-12-31 (업데이트일 전날이 말일 초과하므로 말일로 제한)
    - 전년: 2024-12-01 ~ 2024-12-31 (당년과 동일 일수)
    
    Args:
        update_date_str: YYYYMMDD 형식 (예: 20251215)
    
    Returns:
        tuple: (전년_시작일, 전년_종료일) YYYY-MM-DD 형식
    """
    from calendar import monthrange
    
    # 업데이트일자 파싱
    update_date = datetime.strptime(update_date_str, '%Y%m%d')
    
    # ★ 분석월 계산: metadata.json에서 가져오기 ★
    analysis_month_str = update_date_str[:6]  # YYYYMM (기본값)
    
    # metadata.json에서 실제 분석월 확인
    try:
        from path_utils import get_current_year_file_path
        metadata_path = get_current_year_file_path(update_date_str, 'metadata.json')
        if os.path.exists(metadata_path):
            import json
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                if 'analysis_month' in metadata:
                    analysis_month_str = metadata['analysis_month']
                    print(f"📋 metadata.json에서 분석월 확인: {analysis_month_str}")
    except:
        pass
    
    # 분석월의 년월로 당년 기간 설정
    analysis_year = int(analysis_month_str[:4])
    analysis_month = int(analysis_month_str[4:6])
    
    # 당년 시작일: 분석월의 1일
    current_start = datetime(analysis_year, analysis_month, 1)
    
    # 당년 종료일: 업데이트일의 D-1일
    current_end_calc = update_date - timedelta(days=1)
    
    # 분석월의 말일
    last_day = monthrange(analysis_year, analysis_month)[1]
    current_month_end = datetime(analysis_year, analysis_month, last_day)
    
    # 업데이트일 전날이 분석월 말일을 초과하면 말일로 제한
    if current_end_calc > current_month_end:
        current_end = current_month_end
    else:
        current_end = current_end_calc
    
    # 당년의 일수 계산
    current_days = (current_end - current_start).days + 1
    
    # 전년 기간: 전년도 동일 월에서 동주차 계산
    prev_year = analysis_year - 1
    prev_month_start = datetime(prev_year, analysis_month, 1)
    
    # 당년 시작일과 전년 월초의 요일 차이 계산
    current_start_weekday = current_start.weekday()  # 0=월요일, 6=일요일
    prev_month_start_weekday = prev_month_start.weekday()
    
    # 전년 시작일: 전년도 해당 월에서 당년 시작일과 동일한 요일 찾기
    weekday_diff = current_start_weekday - prev_month_start_weekday
    if weekday_diff < 0:
        weekday_diff += 7
    prev_start = prev_month_start + timedelta(days=weekday_diff)
    
    # 전년 종료일: 전년 시작일로부터 당년과 동일한 일수
    prev_end = prev_start + timedelta(days=current_days - 1)
    
    prev_days = (prev_end - prev_start).days + 1
    
    prev_start_str = prev_start.strftime('%Y-%m-%d')
    prev_end_str = prev_end.strftime('%Y-%m-%d')
    
    # 요일 정보
    weekday_names = ['월', '화', '수', '목', '금', '토', '일']
    current_start_name = weekday_names[current_start.weekday()]
    current_end_name = weekday_names[current_end.weekday()]
    prev_start_name = weekday_names[prev_start.weekday()]
    prev_end_name = weekday_names[prev_end.weekday()]
    
    print(f"📅 당년 기간 ({analysis_month_str}월): {current_start.strftime('%Y-%m-%d')}({current_start_name}) ~ {current_end.strftime('%Y-%m-%d')}({current_end_name}) - {current_days}일")
    print(f"📅 전년 기간 ({prev_year}-{analysis_month:02d}월): {prev_start_str}({prev_start_name}) ~ {prev_end_str}({prev_end_name}) - {prev_days}일")
    
    return prev_start_str, prev_end_str

def get_treemap_previous_year_query(start_date: str, end_date: str):
    """
    트리맵 전년 데이터 조회 쿼리 생성
    DW_COPA_D 테이블에서 브랜드, 채널, 고객, 아이템 계층별로 세부 데이터 조회
    """
    query = f"""
SELECT
    BRD_CD AS "브랜드코드",
    CASE 
        WHEN BRD_CD = 'ST' THEN SUBSTR(PRDT_CD, 3, 3)
        ELSE SUBSTR(PRDT_CD, 2, 3)
    END AS "시즌",
    CHNL_CD AS "채널코드",
    CUST_CD AS "고객코드",
    PRDT_HRRC_CD1 AS "prdt_hrrc_cd1",
    PRDT_HRRC_CD2 AS "prdt_hrrc_cd2",
    PRDT_HRRC_CD3 AS "prdt_hrrc_cd3",
    CASE
        WHEN BRD_CD = 'ST' THEN SUBSTR(PRDT_CD, 8, 2)
        ELSE SUBSTR(PRDT_CD, 7, 2)
    END AS "아이템코드",
    SUM(TAG_SALE_AMT) AS "TAG매출",
    SUM(ACT_SALE_AMT) AS "실판매출"
FROM FNF.SAP_FNF.DW_COPA_D
WHERE PST_DT BETWEEN '{start_date}' AND '{end_date}'
  AND CORP_CD = '1000'
  AND BRD_CD <> 'A'
  AND CHNL_CD <> '9'
  AND PRDT_HRRC_CD1 <> 'E0100'
GROUP BY
    BRD_CD,
    CASE 
        WHEN BRD_CD = 'ST' THEN SUBSTR(PRDT_CD, 3, 3)
        ELSE SUBSTR(PRDT_CD, 2, 3)
    END,
    CHNL_CD,
    CUST_CD,
    PRDT_HRRC_CD1,
    PRDT_HRRC_CD2,
    PRDT_HRRC_CD3,
    CASE
        WHEN BRD_CD = 'ST' THEN SUBSTR(PRDT_CD, 8, 2)
        ELSE SUBSTR(PRDT_CD, 7, 2)
    END
ORDER BY BRD_CD, CHNL_CD
"""
    return query

def execute_query_to_dataframe(conn, query: str):
    """쿼리 실행 및 DataFrame 반환"""
    try:
        print("📊 쿼리 실행 중...")
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        print("📥 데이터 가져오는 중...")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        print(f"✅ {len(df):,}건의 데이터를 조회했습니다.")
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        raise

def load_channel_master():
    """채널 마스터 로드"""
    master_path = project_root / "Master" / "채널마스터.csv"
    if not master_path.exists():
        raise FileNotFoundError(f"채널 마스터를 찾을 수 없습니다: {master_path}")
    
    df = pd.read_csv(master_path, encoding='utf-8-sig')
    print(f"✅ 채널 마스터 로드: {len(df)}건")
    return df

def load_item_master():
    """아이템 마스터 로드"""
    master_path = project_root / "Master" / "아이템마스터.csv"
    if not master_path.exists():
        raise FileNotFoundError(f"아이템 마스터를 찾을 수 없습니다: {master_path}")
    
    df = pd.read_csv(master_path, encoding='utf-8-sig')
    print(f"✅ 아이템 마스터 로드: {len(df)}건")
    return df

def map_channel_name(row, channel_master):
    """
    채널명 매핑 로직
    CUST_CD가 채널마스터의 SAP_CD에 있으면 RF 반환
    없으면 채널코드에 해당하는 채널명 반환
    """
    cust_cd = str(row['고객코드']).strip()
    chnl_cd = str(row['채널코드']).strip()
    
    # RF 체크: CUST_CD가 SAP_CD에 존재하는지 확인
    # SAP_CD는 숫자일 수 있으므로 NULL이 아닌 값만 필터링하고 문자열로 변환
    rf_sap_codes = channel_master[channel_master['구분'] == 'RF']['SAP_CD'].dropna()
    rf_sap_codes_str = [str(int(float(code))).strip() for code in rf_sap_codes]
    
    if cust_cd in rf_sap_codes_str:
        return 'RF'
    
    # 채널코드로 채널명 찾기
    channel_row = channel_master[channel_master['채널번호'].astype(str) == chnl_cd]
    if not channel_row.empty:
        return str(channel_row.iloc[0]['채널명']).strip()
    
    return '기타'

def prepare_item_master_for_merge(item_master):
    """
    아이템 마스터를 merge용으로 준비
    PH01-3를 키로 사용하고 필요한 컬럼만 선택
    """
    # PH01-3를 문자열로 변환하고 중복 제거
    item_master_clean = item_master.copy()
    item_master_clean['PH01-3'] = item_master_clean['PH01-3'].astype(str).str.strip()
    
    # 필요한 컬럼만 선택하고 중복 제거 (첫 번째 값 유지)
    item_master_clean = item_master_clean[['PH01-3', 'PRDT_HRRC2_NM', 'PRDT_HRRC3_NM']].drop_duplicates(subset=['PH01-3'], keep='first')
    
    # NM 컬럼도 문자열로 정리
    item_master_clean['PRDT_HRRC2_NM'] = item_master_clean['PRDT_HRRC2_NM'].astype(str).str.strip()
    item_master_clean['PRDT_HRRC3_NM'] = item_master_clean['PRDT_HRRC3_NM'].astype(str).str.strip()
    
    return item_master_clean

def determine_season_category(row, current_date_str: str):
    """
    시즌 로직을 반영한 아이템_중분류 계산
    
    시즌 로직:
    - SS시즌: 3월~8월, FW시즌: 9월~익년 2월
    - 현재 시즌: 당시즌의류
    - 과거 시즌: 과시즌의류
    - 미래 시즌: 차시즌의류
    - ACC: PRDT_HRRC2_NM 그대로 반환
    
    Args:
        row: 데이터 행
        current_date_str: 현재 날짜 (YYYYMMDD)
    
    Returns:
        str: 아이템_중분류
    """
    prdt_hrrc_cd1 = str(row['prdt_hrrc_cd1']).strip().upper()
    season_code = str(row['시즌']).strip().upper()
    prdt_hrrc2_nm = str(row['PRDT_HRRC2_NM']).strip()
    
    # ACC인 경우 PRDT_HRRC2_NM 반환
    if prdt_hrrc_cd1 == 'ACC' or prdt_hrrc_cd1.startswith('E02'):
        return prdt_hrrc2_nm
    
    # 의류가 아닌 경우 PRDT_HRRC2_NM 반환
    if not (prdt_hrrc_cd1 == '의류' or prdt_hrrc_cd1.startswith('E01') or prdt_hrrc_cd1 == 'L0100'):
        return prdt_hrrc2_nm
    
    # 현재 날짜에서 년/월 추출
    current_year = int(current_date_str[:4])
    current_month = int(current_date_str[4:6])
    
    # 현재 시즌 결정 (SS: 3-8월, FW: 9-2월)
    if 3 <= current_month <= 8:
        current_season = 'S'
        current_season_year = current_year % 100  # 2025 -> 25
    else:
        current_season = 'F'
        # FW 시즌은 9월~익년 2월이므로
        # 1-2월이면 전년도 FW 시즌
        if current_month <= 2:
            current_season_year = (current_year - 1) % 100
        else:
            current_season_year = current_year % 100
    
    # 시즌 코드 파싱 (예: 25F, 25S, 25N)
    if not season_code or len(season_code) < 2:
        return prdt_hrrc2_nm
    
    try:
        # N을 포함하는 경우 (예: 25N) - 년도만 비교
        if 'N' in season_code:
            season_year = int(season_code.replace('N', ''))
            if season_year == current_season_year:
                return '당시즌의류'
            elif season_year < current_season_year:
                return '과시즌의류'
            else:
                return '차시즌의류'
        
        # 일반 시즌 코드 (예: 25F, 25S)
        season_year = int(season_code[:-1])
        season_type = season_code[-1]
        
        # 시즌 비교
        if season_year == current_season_year and season_type == current_season:
            return '당시즌의류'
        elif season_year < current_season_year:
            return '과시즌의류'
        elif season_year == current_season_year:
            # 같은 년도지만 시즌이 다른 경우
            if current_season == 'F' and season_type == 'S':
                return '과시즌의류'
            else:
                return '차시즌의류'
        else:
            return '차시즌의류'
            
    except (ValueError, IndexError):
        return prdt_hrrc2_nm

def preprocess_treemap_data(df: pd.DataFrame, current_date_str: str) -> pd.DataFrame:
    """
    전년 데이터 전처리 (집계 우선 순서로 성능 최적화)
    
    전처리 로직:
    1. 먼저 집계 (행 수 대폭 감소)
    2. 채널코드 -> 채널명 매핑 (RF 처리 포함)
    3. 아이템 소분류 -> PRDT_HRRC2_NM, PRDT_HRRC3_NM 매핑
    4. 아이템_중분류 필드 추가 (시즌 로직 반영)
    5. 최종 출력 형식 집계
    
    Args:
        df: 원본 데이터프레임
        current_date_str: 현재 날짜 (YYYYMMDD) - 시즌 판단용
    
    Returns:
        pd.DataFrame: 전처리 완료된 데이터
    """
    print("\n[전처리] 데이터 전처리 시작...")
    
    # 마스터 로드
    channel_master = load_channel_master()
    item_master = load_item_master()
    
    print(f"  원본 데이터: {len(df):,}건")
    
    # 1) 먼저 집계 (성능 최적화)
    print("  [1/5] 데이터 집계 중...")
    # 숫자 변환
    df['TAG매출'] = pd.to_numeric(df['TAG매출'], errors='coerce').fillna(0)
    df['실판매출'] = pd.to_numeric(df['실판매출'], errors='coerce').fillna(0)
    
    # 집계 키: 브랜드코드, 시즌, 채널코드, 고객코드, prdt_hrrc_cd1, prdt_hrrc_cd2, prdt_hrrc_cd3, 아이템코드
    group_cols = ['브랜드코드', '시즌', '채널코드', '고객코드', 'prdt_hrrc_cd1', 'prdt_hrrc_cd2', 'prdt_hrrc_cd3', '아이템코드']
    
    df_agg = df.groupby(group_cols, as_index=False).agg({
        'TAG매출': 'sum',
        '실판매출': 'sum'
    })
    
    print(f"    집계 후: {len(df):,}건 → {len(df_agg):,}건 (감소율: {(1 - len(df_agg) / len(df)) * 100:.1f}%)")
    
    # 2) 채널명 매핑
    print("  [2/5] 채널명 매핑 중...")
    df_agg['채널명'] = df_agg.apply(lambda row: map_channel_name(row, channel_master), axis=1)
    rf_count = (df_agg['채널명'] == 'RF').sum()
    print(f"    RF 매핑: {rf_count:,}건")
    
    # 3) 아이템 정보 매핑 (PRDT_HRRC2_NM, PRDT_HRRC3_NM) - merge 사용으로 성능 개선
    print("  [3/5] 아이템 정보 매핑 중...")
    # prdt_hrrc_cd3를 문자열로 변환
    df_agg['prdt_hrrc_cd3_str'] = df_agg['prdt_hrrc_cd3'].astype(str).str.strip()
    
    # 아이템 마스터 준비
    item_master_clean = prepare_item_master_for_merge(item_master)
    
    # merge로 매핑 (left join)
    df_agg = df_agg.merge(
        item_master_clean,
        left_on='prdt_hrrc_cd3_str',
        right_on='PH01-3',
        how='left'
    )
    
    # 매핑되지 않은 경우 '기타'로 채우기
    df_agg['PRDT_HRRC2_NM'] = df_agg['PRDT_HRRC2_NM'].fillna('기타')
    df_agg['PRDT_HRRC3_NM'] = df_agg['PRDT_HRRC3_NM'].fillna('기타')
    
    # 임시 컬럼 제거
    df_agg.drop(columns=['prdt_hrrc_cd3_str', 'PH01-3'], inplace=True, errors='ignore')
    
    # 4) 아이템_중분류 필드 추가 (시즌 로직)
    print("  [4/5] 아이템_중분류 계산 중 (시즌 로직 적용)...")
    df_agg['아이템_중분류'] = df_agg.apply(lambda row: determine_season_category(row, current_date_str), axis=1)
    
    # 당/과/차시즌 통계
    season_counts = df_agg['아이템_중분류'].value_counts()
    for season, count in season_counts.items():
        if '시즌' in season:
            print(f"    {season}: {count:,}건")
    
    # 5) 최종 출력 형식으로 정리
    print("  [5/5] 최종 데이터 정리 중...")
    
    # 브랜드 = 브랜드코드 그대로 사용 (변환하지 않음)
    df_agg['브랜드'] = df_agg['브랜드코드']
    
    # 유통채널 = 채널코드 (원본 값 유지)
    df_agg['유통채널'] = df_agg['채널코드']
    
    # 최종 컬럼 순서 정리
    output_columns = [
        '브랜드코드',
        '시즌',
        '채널코드',
        '고객코드',
        'prdt_hrrc_cd1',
        'prdt_hrrc_cd2',
        'PRDT_HRRC2_NM',
        'prdt_hrrc_cd3',
        'PRDT_HRRC3_NM',
        '아이템코드',
        'TAG매출',
        '실판매출',
        '브랜드',
        '유통채널',
        '채널명',
        '아이템_중분류'
    ]
    
    # 존재하는 컬럼만 선택
    available_columns = [col for col in output_columns if col in df_agg.columns]
    result_df = df_agg[available_columns].copy()
    
    print(f"  전처리 완료: {len(result_df):,}건")
    print(f"  브랜드 수: {result_df['브랜드코드'].nunique()}개")
    print(f"  채널 수: {result_df['채널명'].nunique()}개")
    print(f"  아이템_중분류 수: {result_df['아이템_중분류'].nunique()}개")
    
    return result_df

def save_to_csv(df: pd.DataFrame, output_path: Path):
    """DataFrame을 CSV 파일로 저장 (전처리 완료 버전)"""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 최종 출력 형식 (요청사항):
        # 행: 브랜드, 유통채널, 채널명, 아이템_중분류, 아이템소분류, 아이템코드
        # 값: 판매금액(TAG가), 실판매액
        
        # 그룹핑 집계
        group_columns = ['브랜드', '유통채널', '채널명', '아이템_중분류', 'PRDT_HRRC3_NM', '아이템코드']
        
        # 존재하는 컬럼만 사용
        available_group_cols = [col for col in group_columns if col in df.columns]
        
        df_aggregated = df.groupby(available_group_cols, as_index=False).agg({
            'TAG매출': 'sum',
            '실판매출': 'sum'
        })
        
        # 컬럼명 변경 (요청사항에 맞춤)
        df_aggregated = df_aggregated.rename(columns={
            'PRDT_HRRC3_NM': '아이템소분류',
            'TAG매출': '판매금액(TAG가)',
            '실판매출': '실판매액'
        })
        
        # 최종 출력 컬럼 (할인율 제외)
        final_columns = ['브랜드', '유통채널', '채널명', '아이템_중분류', '아이템소분류', '아이템코드', '판매금액(TAG가)', '실판매액']
        available_final_cols = [col for col in final_columns if col in df_aggregated.columns]
        
        df_output = df_aggregated[available_final_cols]
        df_output.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ CSV 파일 저장 완료: {output_path}")
        print(f"   파일 크기: {output_path.stat().st_size / 1024:.2f} KB")
        print(f"   데이터 행 수: {len(df_output):,}건")
        print(f"   출력 컬럼: {', '.join(available_final_cols)}")
        
        # 요약 통계
        total_tag = df_output['판매금액(TAG가)'].sum()
        total_sales = df_output['실판매액'].sum()
        print(f"   총 판매금액(TAG가): {total_tag / 100000000:.1f}억원")
        print(f"   총 실판매액: {total_sales / 100000000:.1f}억원")
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        raise

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='트리맵 전년 데이터 다운로드 및 전처리')
    parser.add_argument('update_date', help='업데이트일자 (YYYYMMDD 형식, 예: 20251215)')
    parser.add_argument('--output', help='출력 파일 경로 (선택사항)')
    
    args = parser.parse_args()
    update_date = args.update_date
    
    if len(update_date) != 8 or not update_date.isdigit():
        print("[ERROR] 업데이트일자 형식이 올바르지 않습니다. YYYYMMDD 형식이어야 합니다.")
        return 1
    
    conn = None
    
    try:
        print("=" * 70)
        print("트리맵 전년 데이터 다운로드 및 전처리")
        print("=" * 70)
        print(f"업데이트일자: {update_date}")
        print()
        
        # 전년 기간 계산
        prev_start, prev_end = calculate_previous_year_period(update_date)
        print()
        
        # Snowflake 연결
        conn = get_snowflake_connection()
        print()
        
        # 쿼리 생성 및 실행
        query = get_treemap_previous_year_query(prev_start, prev_end)
        df = execute_query_to_dataframe(conn, query)
        
        # 데이터 전처리 (마스터 매핑 + 시즌 로직 + 최종 형식)
        # 전년 데이터이므로 시즌 판단도 전년 종료일 기준으로
        # prev_end는 'YYYY-MM-DD' 형식이므로 YYYYMMDD로 변환
        prev_end_yyyymmdd = prev_end.replace('-', '')
        df_processed = preprocess_treemap_data(df, prev_end_yyyymmdd)
        
        # 출력 경로 결정
        if args.output:
            output_path = Path(args.output)
        else:
            year_month = update_date[:6]
            # 파일명: treemap_preprocessed_prev_YYYYMMDD.csv (전처리 완료 버전)
            output_path = project_root / "raw" / year_month / "previous_year" / f"treemap_preprocessed_prev_{update_date}.csv"
        
        # CSV 저장
        save_to_csv(df_processed, output_path)
        
        # 데이터 요약
        print()
        print("=" * 70)
        print("📊 데이터 요약")
        print("=" * 70)
        print(f"총 행 수: {len(df_processed):,}건")
        
        if '브랜드' in df_processed.columns:
            print(f"브랜드 수: {df_processed['브랜드'].nunique()}개")
        if '채널명' in df_processed.columns:
            print(f"채널 수: {df_processed['채널명'].nunique()}개")
        if '아이템_중분류' in df_processed.columns:
            print(f"아이템_중분류 수: {df_processed['아이템_중분류'].nunique()}개")
            
            # 아이템_중분류별 통계
            print("\n아이템_중분류별 통계:")
            item_cat_summary = df_processed.groupby('아이템_중분류')['실판매출'].sum().sort_values(ascending=False)
            for cat, sales in item_cat_summary.items():
                print(f"  {cat}: {sales / 100000000:.1f}억원")
        
        print(f"\n전년 TAG매출 합계: {df_processed['TAG매출'].sum() / 100000000:.1f}억원")
        print(f"전년 실판매출 합계: {df_processed['실판매출'].sum() / 100000000:.1f}억원")
        
        # 브랜드별 요약
        if '브랜드' in df_processed.columns:
            print("\n브랜드별 실판매출:")
            brand_summary = df_processed.groupby('브랜드')['실판매출'].sum().sort_values(ascending=False)
            for brand, sales in brand_summary.items():
                print(f"  {brand}: {sales / 100000000:.1f}억원")
        
        # 채널별 요약 (상위 5개)
        if '채널명' in df_processed.columns:
            print("\n채널별 실판매출 (상위 5개):")
            channel_summary = df_processed.groupby('채널명')['실판매출'].sum().sort_values(ascending=False).head(5)
            for channel, sales in channel_summary.items():
                print(f"  {channel}: {sales / 100000000:.1f}억원")
        
        print()
        print("=" * 70)
        print("✅ 다운로드 및 전처리 완료!")
        print("=" * 70)
        
        return 0
        
    except Exception as e:
        print()
        print("=" * 70)
        print(f"❌ 오류 발생: {e}")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if conn:
            conn.close()
            print("\n🔌 Snowflake 연결 종료")

if __name__ == "__main__":
    import sys
    sys.exit(main())

