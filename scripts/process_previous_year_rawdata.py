"""
전년 로데이터 전처리 스크립트
=================================

작업 흐름:
[전체 전처리] -> previous_rawdata_전처리완료.csv 저장
1. Snowflake 다운로드 (매출, 직접비, 영업비)
2. 전년 로데이터 CSV 파일 읽기 (raw/YYYYMM/previous_year/rawdata_ALL.csv)
3. 피벗 집계 전 전처리:
   - 매장코드(SAP기준) NaN 값을 '미지정'으로 대체
   - 제품계층1(대분류)이 '저장품'인 행 삭제
   - 브랜드코드가 'A'인 행 삭제
4. 피벗 집계 (브랜드코드, 시즌, 채널코드, 매장코드, 제품계층1-3, 아이템코드)
5. 직접비 마스터 매핑 후 컬럼명 변환 및 집계
6. 채널명 필드 추가 (채널코드 옆에 위치)
7. 아이템_중분류 필드 추가 (제품계층2(중분류) 옆에 위치)
8. 전체 전처리 완료 파일 저장

[채널/아이템별 전처리] -> previous_rawdata_Shop_Item.csv 저장
행: 브랜드코드, 채널명, 아이템_중분류, 제품계층3(소분류), 아이템코드
값: TAG매출액, 실매출액, 부가세제외 실판매액, 매출원가(환입후매출원가+평가감(추가)), 
    매출총이익, 인건비, 임차관리비, 물류운송비, 로열티, 감가상각비, 기타, 직접비합계, 직접이익

[브랜드/채널별 전처리] -> previous_rawdata_Shop.csv 저장
행: 브랜드코드, 채널명
값: TAG매출액, 실매출액, 부가세제외 실판매액, 매출원가(환입후매출원가+평가감(추가)), 
    매출총이익, 인건비, 임차관리비, 물류운송비, 로열티, 감가상각비, 기타, 직접비합계, 직접이익,
    영업비 (채널명='공통'인 경우만)

작성일: 2025-11
수정일: 2025-11-21
"""

import pandas as pd
import os
import sys
import re
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)

# 경로 설정
MASTER_DIR = project_root / "Master"
DIRECT_COST_MASTER_PATH = MASTER_DIR / "직접비마스터.csv"
CHANNEL_MASTER_PATH = MASTER_DIR / "채널마스터.csv"

# ================================
# Snowflake 연결 및 영업비 다운로드
# ================================

def get_snowflake_connection():
    """
    Snowflake 데이터베이스 연결 생성
    
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake 연결 객체
    """
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("[OK] Snowflake 연결 성공!")
        return conn
    except ImportError:
        print("[WARN] snowflake-connector-python이 설치되지 않았습니다. 영업비 다운로드를 건너뜁니다.")
        return None
    except Exception as e:
        print(f"[WARN] Snowflake 연결 실패: {e}. 영업비 다운로드를 건너뜁니다.")
        return None

def download_operating_expenses(previous_year_month: str) -> Optional[pd.DataFrame]:
    """
    영업비를 Snowflake에서 다운로드
    
    Args:
        previous_year_month: 전년 년월 (예: '202410')
    
    Returns:
        pd.DataFrame: 브랜드코드별 영업비 데이터 (브랜드코드, 영업비 컬럼)
    """
    print(f"\n[영업비 다운로드] Snowflake에서 영업비 조회 중... (전년월: {previous_year_month})")
    
    conn = get_snowflake_connection()
    if conn is None:
        print("  [WARN] Snowflake 연결 실패로 영업비 다운로드를 건너뜁니다.")
        return None
    
    try:
        query = """
SELECT
    a.brd_cd      AS "브랜드코드",
    SUM(a.idcst)  AS "영업비"
FROM sap_fnf.dm_pl_chnl_m a
WHERE 1 = 1
  AND a.chnl_type = '내수'
  AND a.pst_yyyymm = '{previous_year_month}'
GROUP BY a.brd_cd
ORDER BY a.brd_cd
        """.format(previous_year_month=previous_year_month)
        
        cursor = conn.cursor()
        cursor.execute(query)
        
        # 결과를 DataFrame으로 변환
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        conn.close()
        
        print(f"  [OK] 영업비 다운로드 완료: {len(df)}개 브랜드")
        for _, row in df.iterrows():
            print(f"     {row['브랜드코드']}: {row['영업비']:,.0f}원")
        
        return df
        
    except Exception as e:
        print(f"  [ERROR] 영업비 다운로드 실패: {e}")
        if conn:
            try:
                conn.close()
            except:
                pass
        return None

# ================================
# 마스터 파일 로드
# ================================

def load_direct_cost_master() -> Dict[str, str]:
    """
    직접비 마스터 파일 로드: 계정명 -> 계정전환 매핑
    
    Returns:
        Dict[str, str]: 계정명 -> 계정전환 매핑 딕셔너리
    """
    if not DIRECT_COST_MASTER_PATH.exists():
        raise FileNotFoundError(f"[ERROR] 직접비 마스터 파일이 없습니다: {DIRECT_COST_MASTER_PATH}")
    
    df = pd.read_csv(DIRECT_COST_MASTER_PATH, encoding="utf-8-sig")
    
    # 컬럼 찾기
    account_col = None  # 계정명 컬럼
    conversion_col = None  # 계정전환 컬럼 (C열)
    
    for col in df.columns:
        col_str = str(col).strip()
        if account_col is None and ("계정명" in col_str or "세부" in col_str):
            account_col = col
        if conversion_col is None and ("계정전환" in col_str or "대분류" in col_str):
            conversion_col = col
    
    if account_col is None or conversion_col is None:
        # 기본값: 첫 번째와 세 번째 컬럼 (C열)
        if len(df.columns) >= 3:
            account_col, conversion_col = df.columns[1], df.columns[2]  # B열, C열
        elif len(df.columns) >= 2:
            account_col, conversion_col = df.columns[0], df.columns[1]
        else:
            raise ValueError(f"[ERROR] 직접비 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
    
    mapping = {}
    for _, row in df[[account_col, conversion_col]].dropna().iterrows():
        account = str(row[account_col]).strip()
        conversion = str(row[conversion_col]).strip()
        if account and conversion:
            # 유사한 이름도 매칭하기 위해 부분 매칭 지원
            mapping[account] = conversion
    
    print(f"[OK] 직접비 마스터 로드: {len(mapping)}개 매핑")
    return mapping


def load_channel_master() -> pd.DataFrame:
    """
    채널 마스터 파일 로드
    
    Returns:
        pd.DataFrame: 채널 마스터 데이터
    """
    if not CHANNEL_MASTER_PATH.exists():
        raise FileNotFoundError(f"[ERROR] 채널 마스터 파일이 없습니다: {CHANNEL_MASTER_PATH}")
    
    df = pd.read_csv(CHANNEL_MASTER_PATH, encoding="utf-8-sig")
    print(f"[OK] 채널 마스터 로드: {len(df)}행")
    return df


# ================================
# 1단계: 피벗 집계
# ================================

def pivot_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    피벗 집계 수행
    
    집계 기준 (행):
    - 브랜드코드
    - 시즌
    - 채널코드
    - 매장코드(SAP기준)
    - 제품계층1(대분류)
    - 제품계층2(중분류)
    - 제품계층3(소분류)
    - 아이템코드
    
    집계 대상 (값): 모든 금액 필드 합산
    
    Args:
        df: 원본 데이터프레임
    
    Returns:
        pd.DataFrame: 집계된 데이터프레임
    """
    print("\n[1단계] 피벗 집계 시작...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        "브랜드코드",
        "시즌",
        "채널코드",
        "매장코드 (SAP기준)",
        "제품계층1(대분류)",
        "제품계층2(중분류)",
        "제품계층3(소분류)",
        "아이템코드"
    ]
    
    # 집계 대상 컬럼 (금액 필드)
    # 원본 데이터 컬럼명과 정확히 일치하도록 수정
    agg_cols = [
        "TAG매출액",
        "실매출액",
        "부가세제외 실판매액",
        "매출원가 ( 환입후매출원가+평가감(추가) )",
        "매출총이익",
        "지급수수료_로열티",
        "지급수수료_물류용역비",  # 원본 컬럼명: "지급수수료_물류용역비" (등 없음)
        "지급수수료_카드수수료",
        "지급임차료_매장(고정)",  # 원본 컬럼명: "지급임차료_매장(고정)" (등 없음)
        "감가상각비_임차시설물",
        "지급수수료_중간관리수수료",
        "지급수수료_판매사원도급비(면세)",
        "지급수수료_판매사원도급비(직영)",
        "지급수수료_온라인위탁판매수수료",
        "지급수수료_이천보관료",
        "직접비 합계",
        "직접이익"
    ]
    
    # 존재하는 컬럼만 선택
    available_groupby = [col for col in groupby_cols if col in df.columns]
    available_agg = [col for col in agg_cols if col in df.columns]
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    # 집계 전에 매장코드(SAP기준)의 NaN 값을 '미지정'으로 대체
    df_work = df.copy()
    if "매장코드 (SAP기준)" in df_work.columns:
        nan_count = df_work["매장코드 (SAP기준)"].isna().sum()
        if nan_count > 0:
            df_work["매장코드 (SAP기준)"] = df_work["매장코드 (SAP기준)"].fillna("미지정")
            print(f"  [INFO] 매장코드(SAP기준) NaN 값 {nan_count:,}개를 '미지정'으로 대체")
    
    # 집계 전에 제품계층1(대분류)이 '저장품'인 행 삭제
    if "제품계층1(대분류)" in df_work.columns:
        before_count = len(df_work)
        df_work = df_work[df_work["제품계층1(대분류)"].astype(str).str.strip() != "저장품"]
        after_count = len(df_work)
        removed_count = before_count - after_count
        if removed_count > 0:
            print(f"  [INFO] 제품계층1(대분류)='저장품'인 행 {removed_count:,}개 삭제: {before_count:,}행 -> {after_count:,}행")
    
    # 집계 전에 브랜드코드가 'A'인 행 삭제
    if "브랜드코드" in df_work.columns:
        before_count = len(df_work)
        df_work = df_work[df_work["브랜드코드"].astype(str).str.strip() != "A"]
        after_count = len(df_work)
        removed_count = before_count - after_count
        if removed_count > 0:
            print(f"  [INFO] 브랜드코드='A'인 행 {removed_count:,}개 삭제: {before_count:,}행 -> {after_count:,}행")
    
    # 집계 수행
    agg_dict = {col: 'sum' for col in available_agg}
    df_aggregated = df_work.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    print(f"  [OK] 집계 완료: {len(df)}행 -> {len(df_aggregated)}행")
    return df_aggregated


# ================================
# 2단계: 직접비 컬럼명 변환 및 집계
# ================================

def convert_and_aggregate_direct_costs(df: pd.DataFrame, dc_map: Dict[str, str]) -> pd.DataFrame:
    """
    직접비 컬럼명을 마스터 파일 기준으로 변환 후 집계
    
    변환 대상:
    - 지급수수료_로열티 → 로열티
    - 지급수수료_물류용역비 등 → 물류운송비
    - 지급수수료_카드수수료 → 기타
    - 지급임차료_매장(고정) 등 → 임차관리비
    - 감가상각비_임차시설물 → 감가상각비
    - 지급수수료_중간관리수수료 → 인건비
    - 지급수수료_판매사원도급비(면세) → 인건비
    - 지급수수료_판매사원도급비(직영) → 인건비
    - 지급수수료_온라인위탁판매수수료 → 기타
    - 지급수수료_이천보관료 → 물류운송비
    
    Args:
        df: 집계된 데이터프레임
        dc_map: 직접비 마스터 매핑 딕셔너리
    
    Returns:
        pd.DataFrame: 변환 및 집계된 데이터프레임
    """
    print("\n[2단계] 직접비 컬럼명 변환 및 집계 시작...")
    
    df_result = df.copy()
    
    # 변환 대상 컬럼 목록 (원본 컬럼명과 일치)
    direct_cost_cols = [
        "지급수수료_로열티",
        "지급수수료_물류용역비",  # 원본 컬럼명: "지급수수료_물류용역비" (등 없음)
        "지급수수료_카드수수료",
        "지급임차료_매장(고정)",  # 원본 컬럼명: "지급임차료_매장(고정)" (등 없음)
        "감가상각비_임차시설물",
        "지급수수료_중간관리수수료",
        "지급수수료_판매사원도급비(면세)",
        "지급수수료_판매사원도급비(직영)",
        "지급수수료_온라인위탁판매수수료",
        "지급수수료_이천보관료"
    ]
    
    # 존재하는 직접비 컬럼만 선택
    available_dc_cols = [col for col in direct_cost_cols if col in df_result.columns]
    
    if not available_dc_cols:
        print("  [WARN] 직접비 컬럼을 찾을 수 없습니다. 변환을 건너뜁니다.")
        return df_result
    
    # 컬럼명 -> 변환된 이름 매핑
    col_mapping = {}
    for col in available_dc_cols:
        # 직접비 마스터에서 매핑 찾기 (정확 매칭 또는 부분 매칭)
        converted_name = None
        for key, value in dc_map.items():
            if key in col or col in key:
                converted_name = value
                break
        
        # 매핑이 없으면 기본 변환 규칙 사용
        if converted_name is None:
            if "로열티" in col:
                converted_name = "로열티"
            elif "물류용역비" in col or "이천보관료" in col:
                converted_name = "물류운송비"
            elif "카드수수료" in col or "온라인위탁판매수수료" in col:
                converted_name = "기타"
            elif "임차료" in col or "매장(고정)" in col:
                converted_name = "임차관리비"
            elif "감가상각비" in col:
                converted_name = "감가상각비"
            elif "중간관리수수료" in col or "판매사원도급비" in col:
                converted_name = "인건비"
            else:
                converted_name = col  # 변환 불가능하면 원본 유지
        
        col_mapping[col] = converted_name
    
    # 집계 기준 컬럼 (직접비 컬럼 제외)
    exclude_cols = set(available_dc_cols) | {"직접비 합계", "직접이익"}
    groupby_cols = [col for col in df_result.columns if col not in exclude_cols]
    
    # 직접비 컬럼들을 변환된 이름으로 변경하고, 같은 이름끼리 합산
    # 먼저 직접비 컬럼들을 변환된 이름으로 변경
    for old_col, new_col in col_mapping.items():
        if old_col in df_result.columns:
            # 같은 변환된 이름을 가진 컬럼이 이미 있으면 합산
            if new_col in df_result.columns and new_col != old_col:
                df_result[new_col] = df_result[new_col] + df_result[old_col]
                df_result = df_result.drop(columns=[old_col])
            else:
                df_result = df_result.rename(columns={old_col: new_col})
    
    # 변환된 직접비 컬럼들
    converted_dc_cols = list(set(col_mapping.values()))
    
    # 집계 수행
    agg_dict = {}
    for col in df_result.columns:
        if col not in groupby_cols:
            agg_dict[col] = 'sum'
    
    df_aggregated = df_result.groupby(groupby_cols, as_index=False).agg(agg_dict)
    
    print(f"  [OK] 변환 및 집계 완료: {len(df_result)}행 -> {len(df_aggregated)}행")
    print(f"  변환된 직접비 항목: {converted_dc_cols}")
    
    return df_aggregated


# ================================
# 3단계: 채널명 필드 추가
# ================================

def add_channel_name(df: pd.DataFrame, channel_master: pd.DataFrame) -> pd.DataFrame:
    """
    채널명 필드 추가
    
    로직:
    - 로데이터의 매장코드(SAP기준)가 채널마스터의 E열(SAP_CD)에 해당하면 'RF' 반환
    - 그렇지 않으면 채널코드로 채널마스터 A열(채널번호)와 매칭하여 B열(채널명) 반환
    
    Args:
        df: 데이터프레임
        channel_master: 채널 마스터 데이터프레임
    
    Returns:
        pd.DataFrame: 채널명이 추가된 데이터프레임
    """
    print("\n[3단계] 채널명 필드 추가 시작...")
    
    df_result = df.copy()
    
    # 채널마스터에서 컬럼 찾기
    channel_num_col = None  # A열: 채널번호
    channel_name_col = None  # B열: 채널명
    sap_cd_col = None  # E열: SAP_CD
    
    for col in channel_master.columns:
        col_str = str(col).strip()
        if channel_num_col is None and ("채널번호" in col_str or col == channel_master.columns[0]):
            channel_num_col = col
        if channel_name_col is None and ("채널명" in col_str and "sap" not in col_str.lower()):
            channel_name_col = col
        if sap_cd_col is None and ("SAP_CD" in col_str or col == channel_master.columns[4] if len(channel_master.columns) > 4 else None):
            sap_cd_col = col
    
    if not channel_num_col or not channel_name_col:
        raise ValueError(f"[ERROR] 채널마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(channel_master.columns)}")
    
    # SAP_CD 리스트 (RF 고객 리스트)
    rf_customers = set()
    if sap_cd_col:
        rf_customers = set(channel_master[sap_cd_col].dropna().astype(str).str.strip())
    
    # 채널번호 -> 채널명 매핑
    channel_mapping = {}
    if channel_num_col and channel_name_col:
        for _, row in channel_master[[channel_num_col, channel_name_col]].dropna().iterrows():
            channel_num = str(row[channel_num_col]).strip()
            channel_name = str(row[channel_name_col]).strip()
            if channel_num and channel_name:
                channel_mapping[channel_num] = channel_name
    
    # 채널명 매핑 함수
    def map_channel_name(row):
        shop_cd = str(row.get("매장코드 (SAP기준)", "")).strip()
        channel_cd = str(row.get("채널코드", "")).strip()
        
        # ① 매장코드가 SAP_CD 리스트에 있으면 'RF' 반환
        if shop_cd in rf_customers:
            return "RF"
        
        # ② 채널코드로 채널명 매핑
        return channel_mapping.get(channel_cd, channel_cd)
    
    df_result["채널명"] = df_result.apply(map_channel_name, axis=1)
    
    # 채널명을 채널코드 옆에 위치시키기
    cols = list(df_result.columns)
    if "채널코드" in cols and "채널명" in cols:
        # 채널코드의 인덱스 찾기
        channel_code_idx = cols.index("채널코드")
        # 채널명 제거
        cols.remove("채널명")
        # 채널코드 다음에 채널명 삽입
        cols.insert(channel_code_idx + 1, "채널명")
        df_result = df_result[cols]
    
    print(f"  [OK] 채널명 추가 완료")
    return df_result


# ================================
# 4단계: 아이템_중분류 필드 추가
# ================================

def add_item_middle_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    아이템_중분류 필드 추가
    
    로직:
    - PRDT_HRRC1_NM이 '의류'인 경우:
      * 시즌이 '24F'면 '당시즌의류'
      * 시즌에 '25'가 포함되면 '차시즌의류'
      * 그 외 '과시즌의류'
    - PRDT_HRRC1_NM이 '의류'가 아닌 경우:
      * PRDT_HRRC2_NM 반환 (Headwear, Bag, Shoes 등)
    
    Args:
        df: 데이터프레임
    
    Returns:
        pd.DataFrame: 아이템_중분류가 추가된 데이터프레임
    """
    print("\n[4단계] 아이템_중분류 필드 추가 시작...")
    
    df_result = df.copy()
    
    # 컬럼명 확인
    hrrc1_col = "제품계층1(대분류)"
    hrrc2_col = "제품계층2(중분류)"
    season_col = "시즌"
    
    if hrrc1_col not in df_result.columns:
        print(f"  [WARN] {hrrc1_col} 컬럼을 찾을 수 없습니다.")
        df_result["아이템_중분류"] = ""
        return df_result
    
    def determine_item_middle_category(row):
        hrrc1 = str(row.get(hrrc1_col, "")).strip()
        hrrc2 = str(row.get(hrrc2_col, "")).strip() if hrrc2_col in df_result.columns else ""
        season = str(row.get(season_col, "")).strip() if season_col in df_result.columns else ""
        
        # PRDT_HRRC1_NM이 '의류'인 경우
        if "의류" in hrrc1:
            if season == "24F":
                return "당시즌의류"
            elif "25" in season:
                return "차시즌의류"
            else:
                return "과시즌의류"
        else:
            # PRDT_HRRC1_NM이 '의류'가 아닌 경우 PRDT_HRRC2_NM 반환
            return hrrc2 if hrrc2 else ""
    
    df_result["아이템_중분류"] = df_result.apply(determine_item_middle_category, axis=1)
    
    # 아이템_중분류를 제품계층2(중분류) 옆에 위치시키기
    cols = list(df_result.columns)
    if "제품계층2(중분류)" in cols and "아이템_중분류" in cols:
        # 제품계층2(중분류)의 인덱스 찾기
        hrrc2_idx = cols.index("제품계층2(중분류)")
        # 아이템_중분류 제거
        cols.remove("아이템_중분류")
        # 제품계층2(중분류) 다음에 아이템_중분류 삽입
        cols.insert(hrrc2_idx + 1, "아이템_중분류")
        df_result = df_result[cols]
    
    print(f"  [OK] 아이템_중분류 추가 완료")
    return df_result


# ================================
# 5단계: 2차 전처리 집계
# ================================

def aggregate_second_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    2차 전처리 집계 수행
    
    행: 브랜드코드, 채널명, 아이템_중분류, 제품계층3(소분류), 아이템코드
    값: TAG매출액, 실매출액, 부가세제외 실판매액, 매출원가(환입후매출원가+평가감(추가)), 매출총이익
    
    Args:
        df: 아이템_중분류가 추가된 데이터프레임
    
    Returns:
        pd.DataFrame: 2차 집계된 데이터프레임
    """
    print("\n[5단계] 2차 전처리 집계 시작...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        "브랜드코드",
        "채널명",
        "아이템_중분류",
        "제품계층3(소분류)",
        "아이템코드"
    ]
    
    # 집계 대상 컬럼 (값) - 다양한 컬럼명 패턴 지원
    value_col_patterns = {
        'TAG매출액': ['TAG매출액', 'TAG 매출액'],
        '실매출액': ['실매출액', '실 매출액'],
        '부가세제외 실판매액': ['부가세제외 실판매액', '부가세제외 실 판매액', '부가세제외 실판매액'],
        '매출원가(환입후매출원가+평가감(추가))': ['매출원가 ( 환입후매출원가+평가감(추가) )', '매출원가(환입후매출원가+평가감(추가))', '매출원가'],
        '매출총이익': ['매출총이익']
    }
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            # 유사한 컬럼명 찾기
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('-', '') == col.replace(' ', '').replace('-', '')]
            if similar:
                available_groupby.append(similar[0])
                print(f"  '{col}' → '{similar[0]}' 사용")
            else:
                print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 찾기
    col_mapping = {}
    used_columns = set()  # 이미 사용된 컬럼 추적
    
    available_agg = []
    for target_name, patterns in value_col_patterns.items():
        found = False
        for pattern in patterns:
            for col in df.columns:
                if col in used_columns:
                    continue  # 이미 사용된 컬럼은 건너뛰기
                    
                col_str = str(col).strip()
                
                # 정확한 매칭 우선
                if col_str == pattern:
                    available_agg.append(col)
                    col_mapping[target_name] = col
                    used_columns.add(col)
                    print(f"  '{target_name}' → '{col}' 사용")
                    found = True
                    break
                # 부분 매칭
                elif pattern in col_str:
                    available_agg.append(col)
                    col_mapping[target_name] = col
                    used_columns.add(col)
                    print(f"  '{target_name}' → '{col}' 사용")
                    found = True
                    break
            if found:
                break
        if not found:
            print(f"  [WARNING] '{target_name}' 컬럼 없음 (스킵)")
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    print(f"  집계 기준 (행): {available_groupby}")
    print(f"  집계 대상 (값): {len(available_agg)}개 컬럼")
    
    # 숫자 컬럼 변환
    for col in available_agg:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 집계 전 원본 행 수
    before_count = len(df)
    
    # GROUP BY 집계
    agg_dict = {col: 'sum' for col in available_agg}
    df_aggregated = df.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    # 컬럼명을 사용자가 요청한 이름으로 변경
    reverse_mapping = {v: k for k, v in col_mapping.items()}
    df_aggregated.rename(columns=reverse_mapping, inplace=True)
    
    # 컬럼 순서 정리 (행 컬럼 + 값 컬럼 순서대로)
    ordered_cols = []
    # 집계 기준 컬럼 먼저
    for col in groupby_cols:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    # 집계 대상 컬럼 (사용자가 요청한 순서대로)
    for target_name in ['TAG매출액', '실매출액', '부가세제외 실판매액', '매출원가(환입후매출원가+평가감(추가))', '매출총이익']:
        if target_name in df_aggregated.columns:
            ordered_cols.append(target_name)
    # 나머지 컬럼
    for col in df_aggregated.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df_aggregated = df_aggregated[ordered_cols]
    
    # 집계 후 행 수
    after_count = len(df_aggregated)
    
    print(f"  [OK] 2차 집계 완료: {before_count:,}행 → {after_count:,}행")
    
    # 집계 결과 요약
    if '브랜드코드' in df_aggregated.columns:
        print(f"\n  [INFO] 브랜드코드별 집계 건수:")
        brand_counts = df_aggregated.groupby('브랜드코드').size().sort_values(ascending=False)
        for brand, count in brand_counts.items():
            print(f"     {brand}: {count:,}건")
    
    if '채널명' in df_aggregated.columns:
        print(f"\n  [INFO] 채널명별 집계 건수:")
        channel_counts = df_aggregated.groupby('채널명').size().sort_values(ascending=False)
        for channel, count in channel_counts.items():
            print(f"     {channel}: {count:,}건")
    
    return df_aggregated


def aggregate_by_channel_item_with_direct_costs(df: pd.DataFrame, operating_expenses_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    채널/아이템별 집계 (직접비 합계, 직접이익만 포함)
    
    행: 브랜드코드, 채널명, 아이템_중분류, 제품계층3(소분류), 아이템코드
    값: TAG매출액, 실매출액, 부가세제외 실판매액, 매출원가, 매출총이익, 
        직접비합계, 직접이익
    
    Args:
        df: 아이템_중분류가 추가된 데이터프레임
        operating_expenses_df: 브랜드별 영업비 데이터프레임 (브랜드코드, 영업비 컬럼)
    
    Returns:
        pd.DataFrame: 집계된 데이터프레임
    """
    print("\n[채널/아이템별 집계] 집계 시작...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        "브랜드코드",
        "채널명",
        "아이템_중분류",
        "제품계층3(소분류)",
        "아이템코드"
    ]
    
    # 집계 대상 컬럼: 매출 관련 + 직접비 세부 항목(원본 컬럼명 그대로) + 직접비 합계, 직접이익
    # 직접비 세부 항목은 원본 컬럼명을 그대로 사용 (컬럼명 변환 없음)
    base_value_cols = [
        'TAG매출액',
        '실매출액',
        '부가세제외 실판매액',
        '매출원가 ( 환입후매출원가+평가감(추가) )',
        '매출총이익',
        '직접비 합계',
        '직접이익'
    ]
    
    # 직접비 세부 항목 컬럼 찾기 (지급수수료_, 지급임차료_, 감가상각비_로 시작하는 컬럼)
    direct_cost_cols = [col for col in df.columns 
                        if (col.startswith('지급수수료_') or col.startswith('지급임차료_') or col.startswith('감가상각비_'))
                        and col not in ['직접비 합계', '직접이익']]
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('-', '') == col.replace(' ', '').replace('-', '')]
            if similar:
                available_groupby.append(similar[0])
                print(f"  '{col}' → '{similar[0]}' 사용")
            else:
                print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 찾기 (매출 관련 + 직접비 세부 항목)
    available_agg = []
    for col in base_value_cols:
        if col in df.columns:
            available_agg.append(col)
            print(f"  '{col}' 사용")
        else:
            # 유사한 컬럼 찾기
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('(', '').replace(')', '') == col.replace(' ', '').replace('(', '').replace(')', '')]
            if similar:
                available_agg.append(similar[0])
                print(f"  '{col}' → '{similar[0]}' 사용")
            else:
                print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 직접비 세부 항목 컬럼 추가 (원본 컬럼명 그대로)
    for col in direct_cost_cols:
        if col not in available_agg:
            available_agg.append(col)
            print(f"  직접비 세부 항목: '{col}' 사용")
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    print(f"  집계 기준 (행): {available_groupby}")
    print(f"  집계 대상 (값): {len(available_agg)}개 컬럼")
    
    # 숫자 컬럼 변환
    for col in available_agg:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 집계 전 원본 행 수
    before_count = len(df)
    
    # GROUP BY 집계 (원본 컬럼명 그대로 사용)
    agg_dict = {col: 'sum' for col in available_agg}
    df_aggregated = df.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    # 컬럼 순서 정리
    ordered_cols = []
    for col in groupby_cols:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    # 값 컬럼 순서대로 (매출 관련 먼저)
    value_order = [
        'TAG매출액', '실매출액', '부가세제외 실판매액', 
        '매출원가 ( 환입후매출원가+평가감(추가) )', '매출총이익'
    ]
    for target_name in value_order:
        if target_name in df_aggregated.columns:
            ordered_cols.append(target_name)
    
    # 직접비 세부 항목 컬럼 추가 (계획 전처리 데이터 순서와 동일하게)
    # 계획 전처리 순서: 중간관리수수료, 중간관리수수료(직영), 판매사원도급비(직영), 판매사원도급비(면세),
    #                   물류용역비, 물류운송비, 이천보관료, 카드수수료, 온라인위탁판매수수료, 로열티,
    #                   임차료_매장(변동), 임차료_매장(고정), 임차료_관리비, 감가상각비_임차시설물
    direct_cost_order = [
        '지급수수료_중간관리수수료',
        '지급수수료_중간관리수수료(직영)',
        '지급수수료_판매사원도급비(직영)',
        '지급수수료_판매사원도급비(면세)',
        '지급수수료_물류용역비',
        '지급수수료_물류운송비',
        '지급수수료_이천보관료',
        '지급수수료_카드수수료',
        '지급수수료_온라인위탁판매수수료',
        '지급수수료_로열티',
        '지급임차료_매장(변동)',
        '지급임차료_매장(고정)',
        '지급임차료_관리비',
        '감가상각비_임차시설물'
    ]
    
    # 계획 전처리 순서대로 직접비 세부 항목 추가 (존재하는 컬럼만)
    for col in direct_cost_order:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    
    # 순서에 없는 직접비 세부 항목도 추가 (혹시 모를 경우 대비)
    direct_cost_cols_in_result = [col for col in df_aggregated.columns 
                                   if (col.startswith('지급수수료_') or col.startswith('지급임차료_') or col.startswith('감가상각비_'))
                                   and col not in ['직접비 합계', '직접이익']
                                   and col not in ordered_cols]
    ordered_cols.extend(direct_cost_cols_in_result)
    
    # 직접비 합계, 직접이익 추가
    if '직접비 합계' in df_aggregated.columns:
        ordered_cols.append('직접비 합계')
    if '직접이익' in df_aggregated.columns:
        ordered_cols.append('직접이익')
    
    # 나머지 컬럼 추가
    for col in df_aggregated.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df_aggregated = df_aggregated[ordered_cols]
    
    # 집계 후 행 수
    after_count = len(df_aggregated)
    
    print(f"  [OK] 집계 완료: {before_count:,}행 → {after_count:,}행")
    
    return df_aggregated


def aggregate_by_brand_channel_with_direct_costs(df: pd.DataFrame, operating_expenses_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    브랜드/채널별 집계 (직접비 합계, 직접이익만 포함)
    
    행: 브랜드코드, 채널명
    값: TAG매출액, 실매출액, 부가세제외 실판매액, 매출원가, 매출총이익,
        직접비합계, 직접이익
    
    Args:
        df: 아이템_중분류가 추가된 데이터프레임
    
    Returns:
        pd.DataFrame: 집계된 데이터프레임
    """
    print("\n[브랜드/채널별 집계] 집계 시작...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        "브랜드코드",
        "채널명"
    ]
    
    # 집계 대상 컬럼: 매출 관련 + 직접비 세부 항목(원본 컬럼명 그대로) + 직접비 합계, 직접이익
    # 직접비 세부 항목은 원본 컬럼명을 그대로 사용 (컬럼명 변환 없음)
    base_value_cols = [
        'TAG매출액',
        '실매출액',
        '부가세제외 실판매액',
        '매출원가 ( 환입후매출원가+평가감(추가) )',
        '매출총이익',
        '직접비 합계',
        '직접이익'
    ]
    
    # 직접비 세부 항목 컬럼 찾기 (지급수수료_, 지급임차료_, 감가상각비_로 시작하는 컬럼)
    direct_cost_cols = [col for col in df.columns 
                        if (col.startswith('지급수수료_') or col.startswith('지급임차료_') or col.startswith('감가상각비_'))
                        and col not in ['직접비 합계', '직접이익']]
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('-', '') == col.replace(' ', '').replace('-', '')]
            if similar:
                available_groupby.append(similar[0])
                print(f"  '{col}' → '{similar[0]}' 사용")
            else:
                print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 찾기 (매출 관련 + 직접비 세부 항목)
    available_agg = []
    for col in base_value_cols:
        if col in df.columns:
            available_agg.append(col)
            print(f"  '{col}' 사용")
        else:
            # 유사한 컬럼 찾기
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('(', '').replace(')', '') == col.replace(' ', '').replace('(', '').replace(')', '')]
            if similar:
                available_agg.append(similar[0])
                print(f"  '{col}' → '{similar[0]}' 사용")
            else:
                print(f"  [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 직접비 세부 항목 컬럼 추가 (원본 컬럼명 그대로)
    for col in direct_cost_cols:
        if col not in available_agg:
            available_agg.append(col)
            print(f"  직접비 세부 항목: '{col}' 사용")
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    print(f"  집계 기준 (행): {available_groupby}")
    print(f"  집계 대상 (값): {len(available_agg)}개 컬럼")
    
    # 숫자 컬럼 변환
    for col in available_agg:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 집계 전 원본 행 수
    before_count = len(df)
    
    # GROUP BY 집계 (원본 컬럼명 그대로 사용)
    agg_dict = {col: 'sum' for col in available_agg}
    df_aggregated = df.groupby(available_groupby, as_index=False).agg(agg_dict)
    
    # 컬럼 순서 정리
    ordered_cols = []
    for col in groupby_cols:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    # 값 컬럼 순서대로 (매출 관련 먼저, 그 다음 직접비 세부 항목, 마지막으로 직접비 합계, 직접이익)
    value_order = [
        'TAG매출액', '실매출액', '부가세제외 실판매액', 
        '매출원가 ( 환입후매출원가+평가감(추가) )', '매출총이익'
    ]
    for target_name in value_order:
        if target_name in df_aggregated.columns:
            ordered_cols.append(target_name)
    
    # 직접비 세부 항목 컬럼 추가 (계획 전처리 데이터 순서와 동일하게)
    # 계획 전처리 순서: 중간관리수수료, 중간관리수수료(직영), 판매사원도급비(직영), 판매사원도급비(면세),
    #                   물류용역비, 물류운송비, 이천보관료, 카드수수료, 온라인위탁판매수수료, 로열티,
    #                   임차료_매장(변동), 임차료_매장(고정), 임차료_관리비, 감가상각비_임차시설물
    direct_cost_order = [
        '지급수수료_중간관리수수료',
        '지급수수료_중간관리수수료(직영)',
        '지급수수료_판매사원도급비(직영)',
        '지급수수료_판매사원도급비(면세)',
        '지급수수료_물류용역비',
        '지급수수료_물류운송비',
        '지급수수료_이천보관료',
        '지급수수료_카드수수료',
        '지급수수료_온라인위탁판매수수료',
        '지급수수료_로열티',
        '지급임차료_매장(변동)',
        '지급임차료_매장(고정)',
        '지급임차료_관리비',
        '감가상각비_임차시설물'
    ]
    
    # 계획 전처리 순서대로 직접비 세부 항목 추가 (존재하는 컬럼만)
    for col in direct_cost_order:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    
    # 순서에 없는 직접비 세부 항목도 추가 (혹시 모를 경우 대비)
    direct_cost_cols_in_result = [col for col in df_aggregated.columns 
                                   if (col.startswith('지급수수료_') or col.startswith('지급임차료_') or col.startswith('감가상각비_'))
                                   and col not in ['직접비 합계', '직접이익']
                                   and col not in ordered_cols]
    ordered_cols.extend(direct_cost_cols_in_result)
    
    # 직접비 합계, 직접이익 추가
    if '직접비 합계' in df_aggregated.columns:
        ordered_cols.append('직접비 합계')
    if '직접이익' in df_aggregated.columns:
        ordered_cols.append('직접이익')
    
    # 나머지 컬럼 추가
    for col in df_aggregated.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df_aggregated = df_aggregated[ordered_cols]
    
    # 집계 후 행 수
    after_count = len(df_aggregated)
    
    print(f"  [OK] 집계 완료: {before_count:,}행 → {after_count:,}행")
    
    # 영업비 추가 (브랜드별로 merge, 채널명이 '공통'인 경우만)
    if operating_expenses_df is not None and '브랜드코드' in df_aggregated.columns:
        print("\n  [영업비 추가] 브랜드별 영업비 merge 중... (채널명='공통'인 경우만)")
        
        # 영업비 데이터 준비
        if '브랜드코드' in operating_expenses_df.columns and '영업비' in operating_expenses_df.columns:
            # 영업비 컬럼 초기화 (모두 0으로)
            df_aggregated['영업비'] = 0
            
            # 채널명이 '공통'인 행에만 영업비 추가
            if '채널명' in df_aggregated.columns:
                # 브랜드코드별 영업비를 merge
                df_aggregated = df_aggregated.merge(
                    operating_expenses_df[['브랜드코드', '영업비']],
                    on='브랜드코드',
                    how='left',
                    suffixes=('', '_merge')
                )
                
                # 공통 채널이 없는 브랜드에 대해 공통 채널 행 추가
                for brand_code in operating_expenses_df['브랜드코드'].unique():
                    brand_op_expense = operating_expenses_df[operating_expenses_df['브랜드코드'] == brand_code]['영업비'].iloc[0] if len(operating_expenses_df[operating_expenses_df['브랜드코드'] == brand_code]) > 0 else 0
                    
                    if brand_op_expense > 0:
                        # 해당 브랜드의 공통 채널이 있는지 확인
                        brand_rows = df_aggregated[df_aggregated['브랜드코드'] == brand_code]
                        has_common = len(brand_rows[brand_rows['채널명'].astype(str).str.strip() == '공통']) > 0
                        
                        if not has_common:
                            # 공통 채널 행 생성
                            if len(brand_rows) > 0:
                                common_row = brand_rows.iloc[0].copy()
                                # 모든 수치 컬럼을 0으로 설정
                                for col in df_aggregated.columns:
                                    if col not in ['브랜드코드', '채널명']:
                                        try:
                                            # 숫자형 컬럼인지 확인하고 0으로 설정
                                            pd.to_numeric(common_row[col], errors='raise')
                                            common_row[col] = 0
                                        except:
                                            pass  # 숫자가 아니면 그대로 유지
                                
                                # 채널명을 '공통'으로 설정
                                common_row['채널명'] = '공통'
                                
                                # 영업비만 설정
                                common_row['영업비'] = brand_op_expense
                                common_row['영업비_merge'] = brand_op_expense
                                
                                # 공통 채널 행을 df_aggregated에 추가
                                df_aggregated = pd.concat([df_aggregated, pd.DataFrame([common_row])], ignore_index=True)
                                print(f"    [영업비] {brand_code} 브랜드: 공통 채널 없음, 영업비 {brand_op_expense:,.0f}원으로 공통 채널 행 추가")
                
                # 채널명이 '공통'인 경우만 영업비_merge 값을 사용, 나머지는 0
                df_aggregated['영업비'] = df_aggregated.apply(
                    lambda row: row['영업비_merge'] if pd.notna(row.get('영업비_merge', 0)) and str(row.get('채널명', '')).strip() == '공통' else 0,
                    axis=1
                )
                
                # 임시 컬럼 제거
                if '영업비_merge' in df_aggregated.columns:
                    df_aggregated = df_aggregated.drop(columns=['영업비_merge'])
                
                # 공통 채널에 영업비가 추가된 행 수 확인
                common_count = len(df_aggregated[(df_aggregated['채널명'] == '공통') & (df_aggregated['영업비'] > 0)])
                print(f"  [OK] 영업비 추가 완료 (채널명='공통'인 {common_count}개 행에 영업비 추가)")
            else:
                print(f"  [WARN] '채널명' 컬럼이 없어 영업비를 추가할 수 없습니다.")
        else:
            print(f"  [WARN] 영업비 데이터에 필요한 컬럼이 없습니다. (브랜드코드, 영업비 필요)")
    else:
        if operating_expenses_df is None:
            print(f"  [INFO] 영업비 데이터가 제공되지 않아 영업비를 추가하지 않습니다.")
        else:
            print(f"  [WARN] '브랜드코드' 컬럼이 없어 영업비를 merge할 수 없습니다.")
    
    # 컬럼 순서 재정렬 (영업비를 직접이익 뒤에 배치)
    if '영업비' in df_aggregated.columns:
        ordered_cols = []
        for col in df_aggregated.columns:
            if col != '영업비':
                ordered_cols.append(col)
                # 직접이익 뒤에 영업비 추가
                if col == '직접이익':
                    ordered_cols.append('영업비')
        # 영업비가 아직 추가되지 않은 경우 맨 뒤에 추가
        if '영업비' not in ordered_cols:
            ordered_cols.append('영업비')
        df_aggregated = df_aggregated[ordered_cols]
    
    return df_aggregated


# ================================
# 메인 처리 함수
# ================================

def process_previous_year_rawdata(input_file: str, output_file: Optional[str] = None) -> pd.DataFrame:
    """
    전년 로데이터 전처리 메인 함수
    
    Args:
        input_file: 입력 CSV 파일 경로
        output_file: 출력 CSV 파일 경로 (None이면 자동 생성)
    
    Returns:
        pd.DataFrame: 전처리된 데이터프레임
    """
    print("=" * 60)
    print("전년 로데이터 전처리 시작")
    print("=" * 60)
    print(f"입력 파일: {input_file}")
    
    # 1. 마스터 파일 로드
    print("\n[준비] 마스터 파일 로드 중...")
    dc_map = load_direct_cost_master()
    channel_master = load_channel_master()
    
    # 2. 원본 데이터 로드
    print(f"\n[읽기] CSV 파일 읽는 중: {input_file}")
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    print(f"  원본 데이터: {len(df):,}행, {len(df.columns)}개 컬럼")
    
    # 3. 피벗 집계
    df = pivot_aggregate(df)
    
    # 4. 직접비 컬럼명 변환 및 집계 (제거: 세부 항목 그대로 유지)
    # df = convert_and_aggregate_direct_costs(df, dc_map)
    
    # 5. 채널명 필드 추가
    df = add_channel_name(df, channel_master)
    
    # 6. 아이템_중분류 필드 추가
    df = add_item_middle_category(df)
    
    # 7. [전체 전처리] 완료 파일 저장 (원본 데이터 그대로)
    input_path = Path(input_file)
    
    # 파일명에서 분석월 추출 (예: rawdata_202511_ALL.csv -> 202511)
    analysis_month = None
    filename_match = re.match(r'rawdata_(\d{6})_', input_path.name)
    if filename_match:
        analysis_month = filename_match.group(1)
        print(f"[INFO] 파일명에서 분석월 추출: {analysis_month}")
    else:
        # 폴더 경로에서 분석월 추출 (예: raw/202511/previous_year/ -> 202511)
        path_parts = input_path.parts
        for i, part in enumerate(path_parts):
            if part.isdigit() and len(part) == 6:
                analysis_month = part
                print(f"[INFO] 경로에서 분석월 추출: {analysis_month}")
                break
    
    if analysis_month is None:
        raise ValueError(f"[ERROR] 분석월을 추출할 수 없습니다. 파일명 형식: rawdata_YYYYMM_*.csv")
    
    # 파일명 변경: previous_rawdata_{분석월}_전처리완료.csv
    if output_file is None:
        output_file = input_path.parent / f"previous_rawdata_{analysis_month}_전처리완료.csv"
    else:
        output_file = Path(output_file)
    
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[저장] [전체 전처리] 파일 저장: {output_path}")
    print(f"  파일 크기: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"  데이터: {len(df):,}행 × {len(df.columns)}개 컬럼")
    
    # 7-1. 영업비 다운로드 (전년월 계산)
    # 분석월에서 전년월 계산 (예: 202511 -> 202410)
    if len(analysis_month) == 6 and analysis_month.isdigit():
        year = int(analysis_month[:4])
        month = analysis_month[4:6]
        prev_year = year - 1
        previous_year_month = f"{prev_year}{month}"
    else:
        previous_year_month = None
        print(f"  [WARN] 분석월 형식이 올바르지 않아 영업비 다운로드를 건너뜁니다: {analysis_month}")
    
    operating_expenses_df = None
    if previous_year_month:
        operating_expenses_df = download_operating_expenses(previous_year_month)
    
    # 8. [채널/아이템별 전처리] 집계 및 저장
    df_shop_item = aggregate_by_channel_item_with_direct_costs(df, operating_expenses_df)
    shop_item_output_path = input_path.parent / f"previous_rawdata_{analysis_month}_Shop_Item.csv"
    df_shop_item.to_csv(shop_item_output_path, index=False, encoding="utf-8-sig")
    print(f"\n[저장] [채널/아이템별 전처리] 파일 저장: {shop_item_output_path}")
    print(f"  파일 크기: {shop_item_output_path.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"  데이터: {len(df_shop_item):,}행 × {len(df_shop_item.columns)}개 컬럼")
    print(f"  집계 기준: 브랜드코드, 채널명, 아이템_중분류, 제품계층3(소분류), 아이템코드")
    
    # 9. [브랜드/채널별 전처리] 집계 및 저장
    df_shop = aggregate_by_brand_channel_with_direct_costs(df, operating_expenses_df)
    
    shop_output_path = input_path.parent / f"previous_rawdata_{analysis_month}_Shop.csv"
    
    # 채널명 순서 지정 (사용자 요청 순서)
    channel_order = [
        '공통',
        '백화점',
        '면세점',
        'RF',
        '직영점(가두)',
        '대리점',
        '제휴몰',
        '자사몰',
        '직영몰',
        '아울렛',
        '사입',
        '기타'
    ]
    
    # 채널명 순서대로 정렬
    if '채널명' in df_shop.columns:
        # 채널명에 순서 매기기 (리스트에 없는 채널명은 큰 값으로 처리)
        channel_order_dict = {channel: idx for idx, channel in enumerate(channel_order)}
        df_shop['_채널명_순서'] = df_shop['채널명'].map(channel_order_dict)
        # 리스트에 없는 채널명(예: 아울렛)은 기타 뒤에 오도록 큰 값 할당
        max_order = len(channel_order)
        df_shop['_채널명_순서'] = df_shop['_채널명_순서'].fillna(max_order + 1)
        # 브랜드코드, 채널명 순서로 정렬
        df_shop = df_shop.sort_values(['브랜드코드', '_채널명_순서'], ascending=[True, True])
        # 정렬용 컬럼 삭제
        df_shop = df_shop.drop(columns=['_채널명_순서'])
        # 인덱스 리셋
        df_shop = df_shop.reset_index(drop=True)
    
    df_shop.to_csv(shop_output_path, index=False, encoding="utf-8-sig")
    print(f"\n[저장] [브랜드/채널별 전처리] 파일 저장: {shop_output_path}")
    print(f"  파일 크기: {shop_output_path.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"  데이터: {len(df_shop):,}행 × {len(df_shop.columns)}개 컬럼")
    print(f"  집계 기준: 브랜드코드, 채널명")
    
    # 요약 정보 출력
    print("\n" + "=" * 60)
    print("[REPORT] 전처리 완료 요약")
    print("=" * 60)
    print(f"\n[전체 전처리 완료]")
    print(f"   - 파일: {output_path.name}")
    print(f"     * 행 수: {len(df):,}건, 컬럼 수: {len(df.columns)}개")
    print(f"\n[채널/아이템별 전처리 완료]")
    print(f"   - 파일: {shop_item_output_path.name}")
    print(f"     * 행 수: {len(df_shop_item):,}건, 컬럼 수: {len(df_shop_item.columns)}개")
    print(f"     * 집계 기준: 브랜드코드, 채널명, 아이템_중분류, 제품계층3(소분류), 아이템코드")
    print(f"\n[브랜드/채널별 전처리 완료]")
    print(f"   - 파일: {shop_output_path.name}")
    print(f"     * 행 수: {len(df_shop):,}건, 컬럼 수: {len(df_shop.columns)}개")
    print(f"     * 집계 기준: 브랜드코드, 채널명")
    
    # 브랜드별 집계 (Shop_item 기준)
    if '브랜드코드' in df_shop_item.columns:
        print(f"\n[INFO] Shop_Item 브랜드코드별 집계 건수:")
        brand_summary = df_shop_item.groupby('브랜드코드').size().sort_values(ascending=False)
        for brand, count in brand_summary.items():
            print(f"   {brand}: {count:,}건")
    
    # 채널명별 집계 (Shop 기준)
    if '채널명' in df_shop.columns:
        print(f"\n[INFO] Shop 채널명별 집계 건수:")
        channel_summary = df_shop.groupby('채널명').size().sort_values(ascending=False)
        for channel, count in channel_summary.items():
            print(f"   {channel}: {count:,}건")
    
    # 아이템_중분류별 집계 (Shop_item 기준)
    if '아이템_중분류' in df_shop_item.columns:
        print(f"\n[INFO] Shop_Item 아이템 중분류별 집계 건수:")
        item_summary = df_shop_item.groupby('아이템_중분류').size().sort_values(ascending=False)
        for item, count in item_summary.items():
            print(f"   {item}: {count:,}건")
    
    print("\n" + "=" * 60)
    print("[COMPLETE] 전처리 완료!")
    print("=" * 60)
    
    return df_shop_item


# ================================
# CLI 진입점
# ================================

def main():
    """CLI 진입점"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='전년 로데이터 전처리 스크립트',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/process_previous_year_rawdata.py raw/202511/previous_year/rawdata_ALL.csv
  python scripts/process_previous_year_rawdata.py raw/202511/previous_year/rawdata_ALL.csv --output raw/202511/previous_year/rawdata_전처리완료.csv
        """
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        help='입력 CSV 파일 경로 (예: raw/202511/previous_year/rawdata_ALL.csv)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='출력 CSV 파일 경로 (지정하지 않으면 자동 생성)'
    )
    
    args = parser.parse_args()
    
    try:
        process_previous_year_rawdata(args.input_file, args.output)
    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

