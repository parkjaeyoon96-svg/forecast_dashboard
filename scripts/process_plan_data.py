"""
계획 데이터 전처리 스크립트
=================================

작업 흐름:
1. SAC - 채널별 손익 - 브랜드별로 다운 후 템플릿에 붙이기 (수동 작업)
2. 계획 데이터 CSV 파일 읽기 (raw/YYYYMM/plan/ 폴더의 브랜드별 파일들)

[RF제외 전처리]
1. 데이터 전처리
   - RF 파일 제외
   - 내수합계 계산: Unassigned - 수출 = 내수합계 (각 지표별로)
   - Measure units 행 제거 (내수합계 계산 후)
2. 직접비 마스터 매핑 후 집계
3. 행열 전환 (pivot) - 채널명 변환 포함
4. 필터링: Unassigned, 수출, 실판금액이 없는 행 제거

[RF전처리]
1. 데이터 전처리
   - RF라고 들어간 파일 인식
2. 채널별 집계 (각 채널을 그대로 유지)
3. 직접비 마스터 매핑 후 집계
4. 행열 전환, 채널명 마스터파일 보고 채널 변경
5. RF 합계 넣기 (모든 채널의 합계를 RF 채널로 추가)

[M, M_RF 파일 집계]
1. M 파일의 채널 - M_RF 파일의 채널
2. M 파일에 RF 채널 추가

[브랜드별 파일 통합]
1. 채널 순서 정렬 (내수합계, 백화점, 면세점, RF, 직영점(가두), 대리점, 제휴몰, 자사몰, 직영몰, 아울렛, 사입, 기타)
2. 비율 필드 재계산 (할인율, 출고율 등)
3. 전처리 완료 파일 저장 (raw/YYYYMM/plan/plan_YYYYMM_전처리완료.csv)

작성일: 2025-11
수정일: 2025-11-21
"""

import os
import re
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from path_utils import get_plan_dir, get_plan_file_path

ROOT = os.path.dirname(os.path.dirname(__file__))
MASTER_DIR = os.path.join(ROOT, "Master")
DIRECT_COST_MASTER_PATH = os.path.join(MASTER_DIR, "직접비마스터.csv")
CHANNEL_MASTER_PATH = os.path.join(MASTER_DIR, "채널마스터.csv")

# 브랜드 코드 매핑
BRAND_CODE_MAP = {
    'M': 'M',
    'I': 'I',
    'ST': 'ST',
    'V': 'V',
    'W': 'W',
    'X': 'X'
}

def load_channel_master() -> Dict[str, str]:
    """채널마스터 파일 로드: 채널sap(C열) -> 채널명(B열) 매핑"""
    if not os.path.exists(CHANNEL_MASTER_PATH):
        raise FileNotFoundError(f"[ERROR] 채널마스터 파일이 없습니다: {CHANNEL_MASTER_PATH}")
    
    df = pd.read_csv(CHANNEL_MASTER_PATH, encoding="utf-8-sig")
    
    # 컬럼 찾기
    name_col = None  # B열: 채널명
    sap_col = None   # C열: 채널sap
    
    for col in df.columns:
        col_str = str(col).strip()
        if name_col is None and ("채널명" in col_str and "sap" not in col_str.lower()):
            name_col = col
        if sap_col is None and ("채널sap" in col_str or ("sap" in col_str.lower() and "채널" in col_str)):
            sap_col = col
    
    if name_col is None or sap_col is None:
        raise ValueError(f"[ERROR] 채널마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
    
    mapping = {}
    for _, row in df[[name_col, sap_col]].dropna().iterrows():
        sap = str(row[sap_col]).strip()
        name = str(row[name_col]).strip()
        if sap and name:
            mapping[sap] = name
    
    print(f"[OK] 채널마스터 로드: {len(mapping)}개 매핑")
    return mapping

def load_direct_cost_master() -> Dict[str, str]:
    """직접비 마스터 파일 로드: 계정명 -> 계정전환 매핑"""
    if not os.path.exists(DIRECT_COST_MASTER_PATH):
        raise FileNotFoundError(f"[ERROR] 직접비 마스터 파일이 없습니다: {DIRECT_COST_MASTER_PATH}")
    
    df = pd.read_csv(DIRECT_COST_MASTER_PATH, encoding="utf-8-sig")
    
    # 컬럼 찾기
    account_col = None
    conversion_col = None
    
    for col in df.columns:
        col_str = str(col).strip()
        if account_col is None and ("계정명" in col_str or "세부" in col_str):
            account_col = col
        if conversion_col is None and ("계정전환" in col_str or "대분류" in col_str):
            conversion_col = col
    
    if account_col is None or conversion_col is None:
        # 기본값: 첫 번째와 두 번째 컬럼
        if len(df.columns) >= 2:
            account_col, conversion_col = df.columns[0], df.columns[1]
        else:
            raise ValueError(f"[ERROR] 직접비 마스터 컬럼을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
    
    mapping = {}
    for _, row in df[[account_col, conversion_col]].dropna().iterrows():
        account = str(row[account_col]).strip()
        conversion = str(row[conversion_col]).strip()
        if account and conversion:
            mapping[account] = conversion
    
    print(f"[OK] 직접비 마스터 로드: {len(mapping)}개 매핑")
    return mapping

def detect_brand_from_filename(filename: str) -> Optional[str]:
    """파일명에서 브랜드 코드 추출 (예: 202511R_M.csv -> M)"""
    match = re.search(r'2025\d{2}R[_\-]?([A-Za-z]+)', filename)
    if match:
        code = match.group(1).upper()
        # 단일 알파벳 코드는 그대로 반환
        if len(code) == 1:
            return code
        # 복합 코드 처리
        if code.startswith("MLB"):
            return "M"
        if code.startswith("MLBKIDS") or code.startswith("MLBK"):
            return "I"
        if code.startswith("DISC"):
            return "X"
        if code.startswith("DUV"):
            return "V"
        if code.startswith("SER"):
            return "ST"
        if code.startswith("SUP"):
            return "W"
        return code
    return None

def is_rf_file(filename: str) -> bool:
    """파일명에 RF가 포함되어 있는지 확인"""
    return "RF" in filename.upper()

def read_plan_csv(filepath: str) -> tuple[pd.DataFrame, List[str], str, str]:
    """계획 CSV 파일 읽기 (와이드 포맷)"""
    df = pd.read_csv(filepath, encoding="utf-8-sig", header=None)
    
    # 첫 3행이 헤더 정보 (브랜드, Version, 채널)
    if len(df) < 3:
        raise ValueError(f"[ERROR] 파일 형식이 올바르지 않습니다: {filepath}")
    
    # 브랜드 코드 추출 (1행)
    brand_row = df.iloc[0]
    brand_code = None
    for val in brand_row.values[1:]:  # 첫 번째 컬럼 제외
        if pd.notna(val) and str(val).strip() in BRAND_CODE_MAP:
            brand_code = str(val).strip()
            break
    
    if not brand_code:
        brand_code = detect_brand_from_filename(os.path.basename(filepath)) or "UNKNOWN"
    
    # Version 추출 (2행)
    version_row = df.iloc[1]
    version = str(version_row.iloc[1]) if len(version_row) > 1 else "F11_2025_R"
    
    # 채널명 추출 (3행)
    channel_row = df.iloc[2]
    channels = [str(val).strip() if pd.notna(val) else "" for val in channel_row.values[1:]]
    
    # 데이터 부분 (4행부터)
    data_df = df.iloc[3:].copy()
    # 같은 이름의 컬럼이 여러 개 있을 수 있으므로, 인덱스 기반으로 접근할 수 있도록 처리
    # 컬럼명을 설정하되, 같은 이름이 여러 개 있어도 모두 유지
    data_df.columns = ["구분"] + channels
    
    # 구분 컬럼 정리
    data_df["구분"] = data_df["구분"].astype(str).str.strip()
    data_df = data_df[data_df["구분"].notna() & (data_df["구분"] != "")]
    data_df = data_df.reset_index(drop=True)
    
    # 브랜드와 Version 추가
    data_df["브랜드"] = brand_code
    data_df["Version"] = version
    
    return data_df, channels, brand_code, version

def add_domestic_total(df: pd.DataFrame, channels: List[str]) -> pd.DataFrame:
    """
    내수합계 계산: Unassigned - 수출 = 내수합계 (각 지표별로 계산하여 새 컬럼 추가)
    내수합계의 영업비, 영업이익, 영업이익율도 함께 계산
    
    계산 로직:
    - 내수합계 = Unassigned - 수출 (각 지표별)
    - 영업비(or 일반관리비) = Unassigned의 영업비(or 일반관리비) 금액
    - 영업이익 = 내수합계의 직접이익 - 영업비
    - 영업이익율 = 영업이익 / 실판매액[v+] * 1.1
    """
    # Unassigned와 수출 컬럼 찾기
    unassigned_col = None
    export_col = None
    
    for col in channels:
        col_clean = str(col).strip()
        if unassigned_col is None and ("Unassigned" in col_clean or col_clean == "Unassigned"):
            unassigned_col = col
        if export_col is None and ("수출" in col_clean):
            export_col = col
    
    if not unassigned_col:
        print(f"[WARNING] Unassigned 컬럼을 찾을 수 없습니다. 내수합계 계산을 건너뜁니다.")
        return df
    
    # Unassigned의 영업비(or 일반관리비) 값 추출 (원본 값, pivot_data에서 1000 곱함)
    unassigned_영업비 = 0
    for idx, row in df.iterrows():
        indicator = str(row["구분"]).strip()
        if indicator == "영업비" or indicator == "일반 관리비":
            try:
                value = row[unassigned_col]
                if pd.notna(value) and str(value).strip() != "":
                    unassigned_영업비 = float(value)  # 원본 값 (pivot_data에서 1000 곱함)
                    break
            except:
                pass
    
    # 내수합계 컬럼 추가
    df["내수합계"] = None
    
    # 각 구분별로 내수합계 계산
    for idx, row in df.iterrows():
        try:
            unassigned_val = float(row[unassigned_col]) if pd.notna(row[unassigned_col]) and str(row[unassigned_col]).strip() != "" else 0
        except:
            unassigned_val = 0
        
        if export_col:
            # 수출 컬럼이 있는 경우: 내수합계 = Unassigned - 수출
            try:
                export_val = float(row[export_col]) if pd.notna(row[export_col]) and str(row[export_col]).strip() != "" else 0
            except:
                export_val = 0
            domestic_val = unassigned_val - export_val
        else:
            # 수출 컬럼이 없는 경우: 내수합계 = Unassigned
            domestic_val = unassigned_val
        
        # 내수합계는 원본 값 그대로 저장 (pivot_data에서 1000 곱함)
        df.at[idx, "내수합계"] = domestic_val
    
    # 내수합계의 영업비, 영업이익, 영업이익율 계산
    # 직접이익 행 찾기
    직접이익_idx = None
    실판매액_idx = None
    for idx, row in df.iterrows():
        indicator = str(row["구분"]).strip()
        if indicator == "직접이익":
            직접이익_idx = idx
        if indicator == "실판매액 [v+]":
            실판매액_idx = idx
    
    if 직접이익_idx is not None:
        # 직접이익 값 가져오기 (이미 1000 곱해진 상태)
        직접이익 = df.at[직접이익_idx, "내수합계"]
        if pd.isna(직접이익):
            직접이익 = 0
        
        # 영업비 = Unassigned의 영업비 값
        영업비 = unassigned_영업비
        
        # 영업이익 = 내수합계의 직접이익 - 영업비
        영업이익 = 직접이익 - 영업비
        
        # 실판매액 [v+] 찾기
        실판매액 = 0
        if 실판매액_idx is not None:
            try:
                실판매액 = float(df.at[실판매액_idx, "내수합계"]) if pd.notna(df.at[실판매액_idx, "내수합계"]) else 0
            except:
                pass
        
        # 영업비 행 찾아서 값 업데이트
        영업비_idx = None
        영업이익_idx = None
        영업이익율_idx = None
        
        for idx, row in df.iterrows():
            indicator = str(row["구분"]).strip()
            if indicator == "영업비" or indicator == "일반 관리비":
                영업비_idx = idx
            elif indicator == "영업이익":
                영업이익_idx = idx
            elif indicator == "영업이익율(%)" or indicator == "영업이익율":
                영업이익율_idx = idx
        
        # 영업비 값 업데이트
        if 영업비_idx is not None:
            df.at[영업비_idx, "내수합계"] = 영업비
        
        # 영업이익 값 업데이트
        if 영업이익_idx is not None:
            df.at[영업이익_idx, "내수합계"] = 영업이익
        
        # 영업이익율 계산 및 업데이트
        if 영업이익율_idx is not None and 실판매액 > 0:
            영업이익율 = (영업이익 / 실판매액) * 1.1 * 100  # 퍼센트로 변환
            df.at[영업이익율_idx, "내수합계"] = 영업이익율
    
    return df

def aggregate_direct_costs(df: pd.DataFrame, dc_map: Dict[str, str]) -> pd.DataFrame:
    """직접비 마스터 매핑 후 집계"""
    # 직접비 항목들을 찾아서 매핑
    direct_cost_indices = []
    other_indices = []
    
    for idx, row in df.iterrows():
        account = str(row["구분"]).strip()
        if account in dc_map:
            direct_cost_indices.append(idx)
        else:
            other_indices.append(idx)
    
    if not direct_cost_indices:
        return df
    
    # 직접비 항목들을 계정전환 값으로 변경
    direct_cost_df = df.loc[direct_cost_indices].copy()
    direct_cost_df["구분"] = direct_cost_df["구분"].map(dc_map)
    
    # 숫자 컬럼만 선택 (구분, 브랜드, Version, 내수합계 제외)
    exclude_cols = ["구분", "브랜드", "Version", "내수합계"]
    numeric_cols = [col for col in direct_cost_df.columns if col not in exclude_cols]
    
    # 숫자 컬럼을 숫자 타입으로 변환 (문자열이 섞여 있을 수 있음)
    for col in numeric_cols:
        if col in direct_cost_df.columns:
            # 문자열을 숫자로 변환 (천단위 콤마 제거)
            # Series로 변환하여 처리
            col_series = direct_cost_df[col]
            if isinstance(col_series, pd.Series):
                direct_cost_df[col] = pd.to_numeric(
                    col_series.astype(str).str.replace(",", "").str.replace(" ", ""),
                    errors='coerce'
                ).fillna(0)
            else:
                # DataFrame인 경우 첫 번째 컬럼 사용
                direct_cost_df[col] = pd.to_numeric(
                    col_series.iloc[:, 0].astype(str).str.replace(",", "").str.replace(" ", ""),
                    errors='coerce'
                ).fillna(0)
    
    # 구분별로 집계
    # numeric_cols에 중복 제거 및 존재하는 컬럼만 선택
    numeric_cols_unique = []
    for col in numeric_cols:
        if col in direct_cost_df.columns and col not in numeric_cols_unique:
            numeric_cols_unique.append(col)
    
    if not numeric_cols_unique:
        # 숫자 컬럼이 없으면 구분만 집계
        aggregated = direct_cost_df.groupby("구분", as_index=False).size()
        aggregated = aggregated[["구분"]]
    else:
        # groupby로 집계
        aggregated = direct_cost_df.groupby("구분", as_index=False)[numeric_cols_unique].sum()
    
    aggregated["브랜드"] = direct_cost_df["브랜드"].iloc[0]
    aggregated["Version"] = direct_cost_df["Version"].iloc[0]
    
    # 내수합계도 집계
    if "내수합계" in direct_cost_df.columns:
        domestic_sum = direct_cost_df.groupby("구분", as_index=False)["내수합계"].sum()
        aggregated = aggregated.merge(domestic_sum, on="구분", how="left")
    
    # 다른 행들과 합치기
    other_df = df.loc[other_indices].copy()
    
    # 기존 직접비 행 제거하고 집계된 행 추가
    result_df = pd.concat([other_df, aggregated], ignore_index=True)
    
    return result_df

def pivot_data(df: pd.DataFrame, channels: List[str], channel_map: Dict[str, str]) -> tuple[pd.DataFrame, List[str]]:
    """행열 전환: 채널을 행으로, 지표를 컬럼으로 (채널명 변환 및 동일 채널명 집계 포함)
    
    Returns:
        tuple: (데이터프레임, 컬럼 순서 리스트) - 원본 "구분" 행 순서대로 컬럼 생성
    """
    # 채널 컬럼 목록 (내수합계 포함)
    all_channel_cols = channels + ["내수합계"] if "내수합계" in df.columns else channels
    
    # 원본 "구분" 행 순서 저장 (컬럼 순서 기준)
    indicator_order = []
    for idx, row in df.iterrows():
        indicator = str(row["구분"]).strip()
        if indicator not in ["브랜드", "Version", "채널"] and indicator not in indicator_order:
            if indicator == "일반 관리비":
                # "일반 관리비"는 "영업비"로 매핑되므로 "영업비"로 저장
                if "영업비" not in indicator_order:
                    indicator_order.append("영업비")
            else:
                indicator_order.append(indicator)
    
    pivoted_rows = []
    
    for channel in all_channel_cols:
        if channel not in df.columns:
            continue
        
        # 채널명 변환: 채널sap -> 채널명
        channel_name = channel_map.get(channel, channel)
        
        channel_data = {
            "브랜드": df["브랜드"].iloc[0],
            "Version": df["Version"].iloc[0],
            "채널": channel_name  # 변환된 채널명 사용
        }
        
        # 각 구분에 대한 값 추출
        for idx, row in df.iterrows():
            indicator = str(row["구분"]).strip()
            value = row[channel]
            
            # "브랜드", "Version", "채널"은 이미 channel_data에 있으므로 건너뛰기
            if indicator in ["브랜드", "Version", "채널"]:
                continue
            
            # "일반 관리비"를 "영업비"로 매핑
            if indicator == "일반 관리비":
                indicator = "영업비"
            
            # "원가"를 "매출원가(환입후)"로 변환
            if indicator == "원가":
                indicator = "매출원가(환입후)"
            
            # 숫자 변환 (천원 단위를 원 단위로 변환)
            try:
                if pd.isna(value) or str(value).strip() == "":
                    channel_data[indicator] = None
                else:
                    # 비율 컬럼은 1000 곱하지 않음
                    if indicator.endswith("(%)") or indicator.endswith("율(%)") or "율" in indicator:
                        channel_data[indicator] = float(value)
                    else:
                        channel_data[indicator] = float(value) * 1000  # 천원 단위를 원 단위로 변환
            except:
                channel_data[indicator] = None
        
        pivoted_rows.append(channel_data)
    
    result_df = pd.DataFrame(pivoted_rows)
    
    # 동일한 채널명으로 그룹화하여 집계
    if len(result_df) > 0:
            # 비율 컬럼 목록 (합계가 아닌 평균 또는 첫 번째 값 사용)
            ratio_columns = [col for col in result_df.columns if col.endswith("(%)") or col.endswith("율(%)")]
            
            # 그룹화할 키 컬럼
            group_cols = ["브랜드", "Version", "채널"]
            
            # 숫자 컬럼 목록 (집계 대상)
            numeric_cols = [col for col in result_df.columns if col not in group_cols]
            
            # 동일 채널명 집계
            aggregated_rows = []
            for (brand, version, channel_name), group_df in result_df.groupby(group_cols):
                agg_row = {
                    "브랜드": brand,
                    "Version": version,
                    "채널": channel_name
                }
                
                for col in numeric_cols:
                    if col in ratio_columns:
                        # 비율 컬럼은 첫 번째 값 사용 (또는 평균)
                        values = group_df[col].dropna()
                        if len(values) > 0:
                            agg_row[col] = values.iloc[0] if len(values) == 1 else values.mean()
                        else:
                            agg_row[col] = None
                    else:
                        # 일반 숫자 컬럼은 합계
                        # "영업비"의 경우 "일반 관리비"와 합산
                        if col == "영업비":
                            # 영업비 값과 일반 관리비 값 합산
                            영업비_values = group_df[col].dropna()
                            일반관리비_values = group_df.get("일반 관리비", pd.Series()).dropna() if "일반 관리비" in group_df.columns else pd.Series()
                            
                            total = 0
                            if len(영업비_values) > 0:
                                total += 영업비_values.sum()
                            if len(일반관리비_values) > 0:
                                total += 일반관리비_values.sum()
                            
                            agg_row[col] = total if total > 0 else None
                        else:
                            # 다른 컬럼은 기존 로직대로 합계
                            values = group_df[col].dropna()
                            if len(values) > 0:
                                agg_row[col] = values.sum()
                            else:
                                agg_row[col] = None
                
                aggregated_rows.append(agg_row)
            
            result_df = pd.DataFrame(aggregated_rows)
            
            # "일반 관리비" 컬럼 제거 (영업비에 합산되었으므로)
            if "일반 관리비" in result_df.columns:
                result_df = result_df.drop(columns=["일반 관리비"])
    
    # 원본 순서대로 컬럼 정렬
    base_cols = ["브랜드", "Version", "채널"]
    final_column_order = base_cols.copy()
    
    # indicator_order 순서대로 컬럼 추가
    for indicator in indicator_order:
        if indicator in result_df.columns:
            final_column_order.append(indicator)
    
    # indicator_order에 없지만 존재하는 컬럼 추가 (예: 내수합계에서 추가된 컬럼)
    for col in result_df.columns:
        if col not in final_column_order:
            final_column_order.append(col)
    
    # 존재하는 컬럼만 선택
    final_column_order = [col for col in final_column_order if col in result_df.columns]
    result_df = result_df[final_column_order]
    
    # 반환할 컬럼 순서: base_cols + indicator_order 순서
    column_order = base_cols + [col for col in indicator_order if col in result_df.columns]
    # 존재하는 모든 컬럼 포함 (indicator_order에 없는 것도)
    for col in result_df.columns:
        if col not in column_order:
            column_order.append(col)
    
    return result_df, column_order


def format_plan_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    계획 데이터 포맷팅
    
    1. 천단위 콤마 추가 (금액 필드)
    2. 소숫점 한자리까지 표시 (비율 필드)
    
    Args:
        df: 데이터프레임
    
    Returns:
        pd.DataFrame: 포맷팅된 데이터프레임
    """
    df_result = df.copy()
    
    # 천단위 콤마가 필요한 컬럼 목록
    comma_columns = [
        "TAG가 [v+]",
        "실판매액 [v+]",
        "실판매액 [v-]",
        "수수료차감매출 [v-]",
        "매출(출고)",
        "매출원가(환입후)",  # "매출원가"에서 변경
        "재고평가감_환입",
        "재고평가감_설정",
        "재고자산평가",
        "재고자산평가_설정",
        "재고자산평가_환입",
        "매출총이익",
        "인건비",
        "물류운송비",
        "로열티",
        "임차관리비",
        "감가상각비",
        "기타",
        "직접비",
        "직접이익",
        "영업비",
        "영업이익",
        # 직접비 세부 항목들
        "지급수수료_중간관리수수료",
        "지급수수료_중간관리수수료(직영)",
        "지급수수료_판매사원도급비(직영)",
        "지급수수료_판매사원도급비(면세)",
        "지급수수료_물류용역비",
        "지급수수료_물류운송비",
        "지급수수료_이천보관료",
        "지급수수료_카드수수료",
        "지급수수료_온라인위탁판매수수료",
        "지급수수료_로열티",
        "지급임차료_매장(변동)",
        "지급임차료_매장(고정)",
        "지급임차료_관리비",
        "감가상각비_임차시설물"
    ]
    
    # 소숫점 한자리까지 표시할 컬럼 목록
    decimal_columns = [
        "할인율(%)",
        "출고율(%)",
        "매출원가율(%)",
        "직접제조원가율(%)",
        "매출총이익율(%)",
        "직접이익율(%)",
        "영업이익율(%)"
    ]
    
    # 천단위 콤마 적용
    for col in comma_columns:
        if col in df_result.columns:
            def format_with_comma(x):
                if pd.isna(x):
                    return x
                try:
                    # 숫자형인 경우 천단위 콤마 추가하여 문자열로 변환
                    if isinstance(x, (int, float)):
                        return f"{int(round(x)):,}"
                    elif isinstance(x, str):
                        # 문자열인 경우 숫자로 변환 후 콤마 추가
                        num = float(x.replace(",", ""))
                        return f"{int(round(num)):,}"
                    return x
                except (ValueError, TypeError):
                    return x
            
            df_result[col] = df_result[col].apply(format_with_comma)
    
    # 소숫점 한자리까지 표시
    for col in decimal_columns:
        if col in df_result.columns:
            def format_decimal(x):
                if pd.isna(x):
                    return x
                try:
                    # 숫자형인 경우 소숫점 한자리로 반올림
                    if isinstance(x, (int, float)):
                        return f"{round(float(x), 1):.1f}"
                    elif isinstance(x, str):
                        # 문자열인 경우 숫자 추출 후 반올림
                        # % 기호 제거
                        num_str = x.replace("%", "").strip()
                        num = float(num_str)
                        return f"{round(num, 1):.1f}"
                    return x
                except (ValueError, TypeError):
                    return x
            
            df_result[col] = df_result[col].apply(format_decimal)
    
    return df_result

def recalculate_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """비율 필드 재계산"""
    df = df.copy()
    
    # 필요한 컬럼 확인
    tag_vp = "TAG가 [v+]"
    sales_vp = "실판매액 [v+]"
    sales_vm = "실판매액 [v-]"
    출고매출 = "매출(출고)"  # 출고매출액 [v-]
    원가 = "매출원가(환입후)"  # "원가"를 "매출원가(환입후)"로 변경
    매출총이익 = "매출총이익"
    직접이익 = "직접이익"
    영업이익 = "영업이익"
    
    # 숫자 타입 변환
    for col in [tag_vp, sales_vp, sales_vm, 출고매출, 원가, 매출총이익, 직접이익, 영업이익]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 1. 할인율(%) = (1 - 실판매액[v+]/TAG가[v+]) * 100 (퍼센트로 표시)
    if tag_vp in df.columns and sales_vp in df.columns:
        try:
            mask = df[tag_vp] != 0
            df.loc[mask, "할인율(%)"] = ((1 - df.loc[mask, sales_vp] / df.loc[mask, tag_vp]) * 100).round(2)
            df.loc[~mask, "할인율(%)"] = 0
        except:
            df["할인율(%)"] = 0
    
    # 2. 출고율(%) = 매출(출고)/실판매액[v-] * 100 (퍼센트로 표시)
    if 출고매출 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "출고율(%)"] = ((df.loc[mask, 출고매출] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "출고율(%)"] = 0
        except:
            df["출고율(%)"] = 0
    
    # 3. 매출원가율(%) = 매출원가(환입후)/실판매액[v-] * 100 (퍼센트로 표시)
    if 원가 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "매출원가율(%)"] = ((df.loc[mask, 원가] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "매출원가율(%)"] = 0
        except:
            df["매출원가율(%)"] = 0
    
    # 4. 직접제조원가율(%) = 매출원가(환입후)/TAG가[v+] * 1.1 * 100 (퍼센트로 표시)
    if 원가 in df.columns and tag_vp in df.columns:
        try:
            mask = df[tag_vp] != 0
            df.loc[mask, "직접제조원가율(%)"] = ((df.loc[mask, 원가] / df.loc[mask, tag_vp]) * 1.1 * 100).round(2)
            df.loc[~mask, "직접제조원가율(%)"] = 0
        except:
            df["직접제조원가율(%)"] = 0
    
    # 5. 매출총이익율(%) = 매출총이익/실판매액[v-] * 100 (퍼센트로 표시)
    if 매출총이익 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "매출총이익율(%)"] = ((df.loc[mask, 매출총이익] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "매출총이익율(%)"] = 0
        except:
            df["매출총이익율(%)"] = 0
    
    # 6. 직접이익율(%) = 직접이익/실판매액[v-] * 100 (퍼센트로 표시)
    if 직접이익 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "직접이익율(%)"] = ((df.loc[mask, 직접이익] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "직접이익율(%)"] = 0
        except:
            df["직접이익율(%)"] = 0
    
    # 7. 영업이익율(%) = 영업이익/실판매액[v-] * 100 (퍼센트로 표시)
    if 영업이익 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "영업이익율(%)"] = ((df.loc[mask, 영업이익] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "영업이익율(%)"] = 0
        except:
            df["영업이익율(%)"] = 0
    
    return df

def calculate_rf_total(df_pivoted: pd.DataFrame) -> pd.DataFrame:
    """
    RF 합계 계산 및 추가 (모든 채널의 합계를 RF 채널로 추가)
    
    Args:
        df_pivoted: 행열 전환된 데이터프레임
    
    Returns:
        pd.DataFrame: RF 합계가 추가된 데이터프레임
    """
    if len(df_pivoted) == 0:
        return df_pivoted
    
    # RF 합계 행 생성
    rf_total_row = {
        "브랜드": df_pivoted["브랜드"].iloc[0],
        "Version": df_pivoted["Version"].iloc[0],
        "채널": "RF"
    }
    
    # 숫자 컬럼만 선택 (브랜드, Version, 채널 제외)
    exclude_cols = ["브랜드", "Version", "채널"]
    numeric_cols = [col for col in df_pivoted.columns if col not in exclude_cols]
    
    # 각 숫자 컬럼에 대해 합계 계산
    for col in numeric_cols:
        if col in df_pivoted.columns:
            # 비율 컬럼은 평균 계산
            if col.endswith("(%)") or col.endswith("율(%)") or "율" in col:
                values = pd.to_numeric(df_pivoted[col], errors='coerce').dropna()
                if len(values) > 0:
                    rf_total_row[col] = values.mean()
                else:
                    rf_total_row[col] = None
            else:
                # 일반 숫자 컬럼은 합계
                values = pd.to_numeric(df_pivoted[col], errors='coerce').fillna(0)
                rf_total_row[col] = values.sum()
        else:
            rf_total_row[col] = None
    
    # RF 합계 행 추가
    rf_total_df = pd.DataFrame([rf_total_row])
    result_df = pd.concat([df_pivoted, rf_total_df], ignore_index=True)
    
    return result_df

def aggregate_channels_before_conversion(df: pd.DataFrame, channels: List[str]) -> pd.DataFrame:
    """
    채널별 집계 (채널명 변환 전)
    동일한 원본 채널명끼리 먼저 집계
    
    예: "(브랜드) 백화점" 컬럼이 여러 개 있으면 먼저 합산
    
    Args:
        df: 원본 데이터프레임
        channels: 채널 목록
    
    Returns:
        pd.DataFrame: 채널별 집계된 데이터프레임 (중복 채널명 제거)
    """
    # 동일한 채널명 그룹화 (예: "(브랜드) 백화점" 여러 개를 하나로)
    channel_groups = {}
    for i, channel in enumerate(channels):
        channel_str = str(channel).strip()
        if channel_str not in channel_groups:
            channel_groups[channel_str] = []
        # 컬럼 인덱스 저장 (pandas에서 같은 이름의 컬럼이 여러 개일 때 접근하기 위해)
        channel_groups[channel_str].append(i)
    
    # 각 구분별로 동일 채널명끼리 집계
    result_rows = []
    
    for idx, row in df.iterrows():
        indicator = str(row["구분"]).strip()
        
        agg_row = {
            "구분": indicator,
            "브랜드": row["브랜드"],
            "Version": row["Version"]
        }
        
        # 각 채널명 그룹별로 합산
        # df.columns는 ["구분"] + channels 순서이므로, 채널 컬럼은 인덱스 1부터 시작
        for channel_name, col_indices in channel_groups.items():
            total = 0
            for col_idx in col_indices:
                # 채널 컬럼 인덱스는 1부터 시작 (0은 "구분")
                actual_col_idx = col_idx + 1  # channels 리스트 인덱스를 df.columns 인덱스로 변환
                if actual_col_idx < len(df.columns):
                    try:
                        # iloc로 접근하여 정확한 컬럼 값 가져오기
                        value = df.iloc[idx, actual_col_idx]
                        if pd.notna(value) and str(value).strip() != "":
                            total += float(value)
                    except Exception as e:
                        pass
            
            # 집계된 값을 첫 번째 채널명으로 저장 (중복 제거)
            if col_indices:
                first_col_idx = col_indices[0]
                first_col_name = channels[first_col_idx]  # channels 리스트에서 직접 가져오기
                agg_row[first_col_name] = total
        
        result_rows.append(agg_row)
    
    result_df = pd.DataFrame(result_rows)
    
    # 중복된 채널 컬럼 제거 (첫 번째 것만 유지)
    final_columns = ["구분", "브랜드", "Version"]
    seen_channels = set()
    for channel in channels:
        channel_str = str(channel).strip()
        if channel_str not in seen_channels:
            # 첫 번째로 나타나는 채널명만 추가
            if channel in result_df.columns:
                final_columns.append(channel)
                seen_channels.add(channel_str)
    
    result_df = result_df[final_columns]
    
    return result_df

def process_rf_file(df: pd.DataFrame, channels: List[str], brand_code: str, version: str, 
                    dc_map: Dict[str, str], channel_map: Dict[str, str]) -> pd.DataFrame:
    """
    RF 파일 전처리 전체 프로세스
    
    Args:
        df: 원본 데이터프레임
        channels: 채널 목록
        brand_code: 브랜드 코드
        version: 버전
        dc_map: 직접비 마스터 매핑
        channel_map: 채널 마스터 매핑
    
    Returns:
        pd.DataFrame: RF 전처리 완료된 데이터프레임
    """
    print(f"  [RF 전처리] RF 파일 처리 시작...")
    
    # 1. Measure units 행 제거
    df_rf = df.copy()
    df_rf = df_rf[~df_rf["구분"].astype(str).str.contains("Measure units", case=False, na=False)]
    df_rf = df_rf[df_rf["구분"].astype(str).str.strip() != "Measure units"]
    
    # 2. 채널별 집계 (채널명 변환 전) - 동일한 원본 채널명끼리 먼저 집계
    print(f"    [2단계] 채널별 집계 (채널명 변환 전)...")
    print(f"      원본 채널: {channels}")
    df_rf = aggregate_channels_before_conversion(df_rf, channels)
    
    # 집계 후 남은 채널 목록 업데이트 (중복 제거된 채널만)
    channels_after_agg = [col for col in df_rf.columns if col not in ["구분", "브랜드", "Version"]]
    print(f"      원본 채널 수: {len(channels)}, 집계 후 채널 수: {len(channels_after_agg)}")
    print(f"      집계 후 채널: {channels_after_agg}")
    
    # 3. 직접비 마스터 매핑 후 집계 (제거: 세부 항목 그대로 유지)
    # print(f"    [3단계] 직접비 마스터 매핑 후 집계...")
    # df_rf = aggregate_direct_costs(df_rf, dc_map)
    
    # 4. 행열 전환 (pivot) - 채널명 마스터파일 보고 채널 변경
    print(f"    [4단계] 행열 전환 및 채널명 변환...")
    df_pivoted, _ = pivot_data(df_rf, channels_after_agg, channel_map)
    
    # 5. RF 합계 넣기 (모든 채널의 합계를 RF 채널로 추가)
    print(f"    [5단계] RF 합계 계산...")
    df_pivoted = calculate_rf_total(df_pivoted)
    
    # 6. 매장/ID/샵코드 컬럼 삭제
    cols_to_drop = ["매장", "ID", "샵코드", "매장코드", "매장코드(SAP기준)"]
    for col in cols_to_drop:
        if col in df_pivoted.columns:
            df_pivoted = df_pivoted.drop(columns=[col])
    
    print(f"  [OK] RF 전처리 완료: {len(df_pivoted)}행")
    return df_pivoted


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Unassigned, 수출, 실판금액이 없는 행 제거"""
    # 제거할 채널
    exclude_channels = ["Unassigned", "수출"]
    
    # 실판매액 [v+] 컬럼 확인
    sales_col = "실판매액 [v+]"
    
    filtered_df = df.copy()
    
    # 채널 필터링
    if "채널" in filtered_df.columns:
        # Unassigned, 수출 제거
        filtered_df = filtered_df[~filtered_df["채널"].isin(exclude_channels)]
        
        # 실판매액이 없는 행 제거
        if sales_col in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df[sales_col].notna() & 
                (filtered_df[sales_col] != 0) &
                (filtered_df[sales_col] != "")
            ]
    
    return filtered_df

def process_plan_files(year_month: str) -> pd.DataFrame:
    """계획 파일들 처리 및 통합"""
    plan_dir = get_plan_dir(year_month)
    
    if not os.path.exists(plan_dir):
        raise FileNotFoundError(f"[ERROR] 계획 데이터 폴더가 없습니다: {plan_dir}")
    
    # 마스터 파일 로드
    channel_map = load_channel_master()
    dc_map = load_direct_cost_master()
    
    # 브랜드별 파일 찾기 (형식: YYYYMMR_브랜드.csv, RF 제외)
    plan_files = []
    for filename in os.listdir(plan_dir):
        # 1. YYYYMMR_ 형식으로 시작하고 .csv로 끝나는 파일
        if not (filename.startswith(f"{year_month}R_") and filename.endswith(".csv")):
            continue
        
        # 2. RF가 포함된 파일은 제외
        if is_rf_file(filename):
            print(f"[INFO] RF 파일 제외: {filename}")
            continue
        
        # 3. 파일명 형식 검증: YYYYMMR_브랜드.csv (하이픈 없이)
        # 예: 202511R_M.csv, 202511R_ST.csv
        filename_without_ext = filename[:-4]  # .csv 제거
        pattern = f"^{year_month}R_[A-Za-z]+$"  # YYYYMMR_브랜드코드 형식
        if not re.match(pattern, filename_without_ext):
            print(f"[INFO] 형식 불일치 파일 제외: {filename} (형식: {year_month}R_브랜드.csv)")
            continue
        
        filepath = os.path.join(plan_dir, filename)
        plan_files.append(filepath)
    
    if not plan_files:
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {plan_dir} (형식: {year_month}R_브랜드.csv, RF 제외)")
    
    print(f"[INFO] 처리 대상 파일 수: {len(plan_files)}")
    
    # 브랜드별로 파일 분류 (RF 제외이므로 normal 파일만)
    brand_files = {}  # {brand_code: filepath}
    
    for filepath in plan_files:
        filename = os.path.basename(filepath)
        
        # 브랜드 코드 추출
        brand_code = detect_brand_from_filename(filename)
        if not brand_code:
            # 파일 읽어서 브랜드 코드 추출
            try:
                df_temp, _, brand_code, _ = read_plan_csv(filepath)
                brand_code = df_temp["브랜드"].iloc[0] if "브랜드" in df_temp.columns else "UNKNOWN"
            except:
                print(f"[WARNING] 브랜드 코드를 추출할 수 없어 제외: {filename}")
                continue
        
        if brand_code in brand_files:
            print(f"[WARNING] 브랜드 {brand_code}의 중복 파일 발견: {filename}, 기존: {os.path.basename(brand_files[brand_code])}")
            continue
        
        brand_files[brand_code] = filepath
    
    # 브랜드별로 처리 (RF 제외 파일만)
    all_results = []
    
    for brand_code, filepath in brand_files.items():
        print(f"\n[브랜드] {brand_code} 처리 시작...")
        
        try:
            print(f"  [파일] {os.path.basename(filepath)} 처리 중...")
            df, channels, brand_code_normal, version = read_plan_csv(filepath)
            
            # RF제외 전처리
            # 1. 내수합계 계산
            df = add_domestic_total(df, channels)
            
            # 2. Measure units 행 제거 (내수합계 계산 후)
            df = df[~df["구분"].astype(str).str.contains("Measure units", case=False, na=False)]
            df = df[df["구분"].astype(str).str.strip() != "Measure units"]
            
            # 3. 직접비 마스터 매핑 후 집계 (제거: 세부 항목 그대로 유지)
            # df = aggregate_direct_costs(df, dc_map)
            
            # 4. 행열 전환 (채널명 변환 포함)
            df_pivoted, column_order = pivot_data(df, channels, channel_map)
            
            # 5. 필터링: Unassigned, 수출, 실판금액이 없는 행 제거
            df_filtered = filter_rows(df_pivoted)
            
            all_results.append(df_filtered)
            print(f"  [OK] 브랜드 {brand_code} 처리 완료: {len(df_filtered)}행")
            
        except Exception as e:
            print(f"[ERROR] 브랜드 {brand_code} 처리 실패: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_results:
        raise RuntimeError("[ERROR] 처리된 파일이 없습니다.")
    
    # ★★★ M_RF 파일 처리: M 파일에서 RF 데이터 차감 ★★★
    rf_file = os.path.join(plan_dir, f"{year_month}R_M_RF.csv")
    if os.path.exists(rf_file):
        print(f"\n[RF 차감] M_RF 파일 처리 중...")
        try:
            # RF 파일 직접 읽기 (특수 형식: 매장별 컬럼)
            df_rf_raw = pd.read_csv(rf_file, encoding="utf-8-sig", header=None)
            
            # 행 정보 추출
            brand_row = df_rf_raw.iloc[0, 1:].tolist()  # 브랜드
            version_row = df_rf_raw.iloc[1, 1:].tolist()  # Version
            channel_row = df_rf_raw.iloc[2, 1:].tolist()  # 채널
            
            version_rf = version_row[0] if version_row else "F11_2025_R"
            
            # 지표별 데이터 추출 (6행부터)
            indicator_data = {}
            for idx in range(6, len(df_rf_raw)):
                indicator = str(df_rf_raw.iloc[idx, 0]).strip()
                values = df_rf_raw.iloc[idx, 1:].tolist()
                indicator_data[indicator] = values
            
            # 채널마스터 매핑 적용
            mapped_channels = []
            for ch in channel_row:
                ch_str = str(ch).strip()
                mapped_ch = channel_map.get(ch_str, ch_str)
                mapped_channels.append(mapped_ch)
            
            print(f"  [INFO] RF 파일 채널 (매핑 후): {list(set(mapped_channels))}")
            
            # 천원 단위 -> 원 단위 변환이 필요한 지표
            thousand_unit_indicators = [
                "TAG가 [v+]", "실판매액 [v+]", "실판매액 [v-]", "수수료차감매출 [v-]",
                "매출(출고)", "매출원가", "재고평가감_환입", "재고자산평가", "원가",
                "매출총이익", "직접비", "직접이익", "영업비", "영업이익",
                "지급수수료_중간관리수수료", "지급수수료_물류용역비", "지급수수료_물류운송비",
                "지급수수료_이천보관료", "지급수수료_카드수수료", "지급수수료_온라인위탁판매수수료",
                "지급수수료_로열티", "지급임차료_매장(변동)", "지급임차료_매장(고정)",
                "지급임차료_관리비", "감가상각비_임차시설물"
            ]
            
            # 채널별 합산
            channel_sums = {}
            for i, ch in enumerate(mapped_channels):
                if ch not in channel_sums:
                    channel_sums[ch] = {}
                
                for indicator, values in indicator_data.items():
                    if i < len(values):
                        val = values[i]
                        # 숫자 변환
                        try:
                            if pd.notna(val):
                                val = float(str(val).replace(",", ""))
                                # 천원 단위 -> 원 단위 변환
                                if indicator in thousand_unit_indicators:
                                    val = val * 1000
                            else:
                                val = 0.0
                        except:
                            val = 0.0
                        
                        if indicator not in channel_sums[ch]:
                            channel_sums[ch][indicator] = 0.0
                        channel_sums[ch][indicator] += val
            
            # 지표명 매핑 (원본 -> 표준)
            indicator_map = {
                "TAG가 [v+]": "TAG가 [v+]",
                "실판매액 [v+]": "실판매액 [v+]",
                "실판매액 [v-]": "실판매액 [v-]",
                "수수료차감매출 [v-]": "수수료차감매출 [v-]",
                "매출(출고)": "매출(출고)",
                "매출원가": "매출원가",
                "직접이익": "직접이익",
            }
            
            # 백화점 데이터 확인
            if "백화점" in channel_sums:
                백화점_실판매액 = channel_sums["백화점"].get("실판매액 [v+]", 0)
                print(f"  [DEBUG] 백화점 RF 실판매액: {백화점_실판매액/1e6:.1f}백만원 = {백화점_실판매액/1e8:.2f}억원")
            
            # RF 합계 계산 (모든 채널의 합계)
            rf_total = {}
            for ch, indicators in channel_sums.items():
                for indicator, val in indicators.items():
                    if indicator not in rf_total:
                        rf_total[indicator] = 0.0
                    rf_total[indicator] += val
            
            print(f"  [DEBUG] RF 합계 실판매액: {rf_total.get('실판매액 [v+]', 0)/1e8:.2f}억원")
            
            # M 브랜드 결과에서 RF 차감
            for i, result_df in enumerate(all_results):
                if result_df["브랜드"].iloc[0] == "M":
                    print(f"  [차감] M 브랜드에서 RF 데이터 차감 중...")
                    
                    # 채널별로 RF 데이터 차감
                    for rf_channel, rf_indicators in channel_sums.items():
                        # 내수합계는 차감하지 않음
                        if rf_channel == "내수합계":
                            continue
                        
                        # M 결과에서 해당 채널 찾기
                        mask = result_df["채널"] == rf_channel
                        if mask.any():
                            # 실판매액 [v+] 차감 예시 출력
                            if "실판매액 [v+]" in result_df.columns:
                                old_val = result_df.loc[mask, "실판매액 [v+]"].values[0]
                                rf_val = rf_indicators.get("실판매액 [v+]", 0)
                                new_val = old_val - rf_val
                                print(f"    - {rf_channel}: 실판매액 {old_val/1e8:.1f}억 - {rf_val/1e8:.1f}억 = {new_val/1e8:.1f}억")
                            
                            # 모든 지표 차감
                            for indicator, rf_val in rf_indicators.items():
                                # 컬럼명 매핑 (RF 파일의 지표명 -> M 파일의 컬럼명)
                                col = indicator
                                if col in result_df.columns:
                                    old_val = result_df.loc[mask, col].values[0]
                                    new_val = old_val - rf_val
                                    result_df.loc[mask, col] = new_val
                        else:
                            print(f"    - {rf_channel}: M 결과에 없음 (건너뜀)")
                    
                    # RF 채널 추가
                    rf_row_data = {
                        "브랜드": "M",
                        "Version": version_rf,
                        "채널": "RF"
                    }
                    for indicator, val in rf_total.items():
                        rf_row_data[indicator] = val
                    rf_sum = pd.DataFrame([rf_row_data])
                    
                    all_results[i] = pd.concat([result_df, rf_sum], ignore_index=True)
                    print(f"  [OK] RF 채널 추가 완료")
                    break
            
            print(f"[OK] RF 차감 완료")
        except Exception as e:
            print(f"[WARNING] RF 파일 처리 실패: {e}")
            import traceback
            traceback.print_exc()
    
    # 브랜드별 결과 통합
    final_df = pd.concat(all_results, ignore_index=True)
    
    # 채널 순서 정의 (사용자 요청 순서)
    channel_order = [
        "내수합계",
        "백화점",
        "면세점",
        "RF",
        "직영점(가두)",
        "대리점",
        "제휴몰",
        "자사몰",
        "직영몰",
        "아울렛",
        "사입",
        "기타"
    ]
    
    # 채널 순서대로 정렬 (채널이 없는 경우는 맨 뒤로)
    if "채널" in final_df.columns:
        # 채널 순서 매핑 생성
        channel_order_map = {ch: idx for idx, ch in enumerate(channel_order)}
        
        # 채널 순서 컬럼 추가 (정렬용)
        final_df["_channel_order"] = final_df["채널"].map(
            lambda x: channel_order_map.get(x, 999)  # 순서에 없는 채널은 999로 설정
        )
        
        # 브랜드, Version, 채널 순서로 정렬
        final_df = final_df.sort_values(
            by=["브랜드", "Version", "_channel_order", "채널"],
            ascending=[True, True, True, True]
        )
        
        # 정렬용 컬럼 제거
        final_df = final_df.drop(columns=["_channel_order"])
        
        print(f"[OK] 채널 순서 정렬 완료: {len(channel_order)}개 채널 순서 적용")
    
    # 중복 컬럼 제거
    # 1. .1로 끝나는 컬럼 제거 (pandas concat 시 자동 생성되는 중복 컬럼)
    duplicate_cols = [col for col in final_df.columns if col.endswith('.1')]
    if duplicate_cols:
        print(f"[INFO] 중복 컬럼 제거 (.1): {duplicate_cols}")
        final_df = final_df.drop(columns=duplicate_cols)
    
    # 2. "브랜드", "Version", "채널" 중복 제거 (원본 데이터의 "구분"에 포함된 경우)
    # 컬럼 목록에서 중복 확인
    seen_cols = set()
    cols_to_keep = []
    cols_to_drop = []
    
    for col in final_df.columns:
        if col in ["브랜드", "Version", "채널"]:
            if col not in seen_cols:
                seen_cols.add(col)
                cols_to_keep.append(col)
            else:
                cols_to_drop.append(col)
        else:
            cols_to_keep.append(col)
    
    if cols_to_drop:
        print(f"[INFO] 중복 컬럼 제거: {cols_to_drop}")
        final_df = final_df.drop(columns=cols_to_drop)
    
    # 재고평가감 필드 합산 처리
    # 재고평가감_환입 = 기존 재고평가감_환입 + 재고자산평가_설정
    # 재고평가감_설정 = 기존 재고평가감_설정 + 재고자산평가_환입
    if "재고평가감_환입" in final_df.columns or "재고자산평가_설정" in final_df.columns:
        # 숫자 타입 변환
        if "재고평가감_환입" in final_df.columns:
            final_df["재고평가감_환입"] = pd.to_numeric(final_df["재고평가감_환입"], errors='coerce').fillna(0)
        if "재고자산평가_설정" in final_df.columns:
            final_df["재고자산평가_설정"] = pd.to_numeric(final_df["재고자산평가_설정"], errors='coerce').fillna(0)
        
        # 재고평가감_환입 합산
        if "재고평가감_환입" in final_df.columns and "재고자산평가_설정" in final_df.columns:
            final_df["재고평가감_환입"] = final_df["재고평가감_환입"] + final_df["재고자산평가_설정"]
            print(f"[INFO] 재고평가감_환입 합산 완료 (재고평가감_환입 + 재고자산평가_설정)")
        elif "재고자산평가_설정" in final_df.columns:
            final_df["재고평가감_환입"] = final_df["재고자산평가_설정"]
            print(f"[INFO] 재고평가감_환입 생성 (재고자산평가_설정 값 사용)")
    
    if "재고평가감_설정" in final_df.columns or "재고자산평가_환입" in final_df.columns:
        # 숫자 타입 변환
        if "재고평가감_설정" in final_df.columns:
            final_df["재고평가감_설정"] = pd.to_numeric(final_df["재고평가감_설정"], errors='coerce').fillna(0)
        if "재고자산평가_환입" in final_df.columns:
            final_df["재고자산평가_환입"] = pd.to_numeric(final_df["재고자산평가_환입"], errors='coerce').fillna(0)
        
        # 재고평가감_설정 합산
        if "재고평가감_설정" in final_df.columns and "재고자산평가_환입" in final_df.columns:
            final_df["재고평가감_설정"] = final_df["재고평가감_설정"] + final_df["재고자산평가_환입"]
            print(f"[INFO] 재고평가감_설정 합산 완료 (재고평가감_설정 + 재고자산평가_환입)")
        elif "재고자산평가_환입" in final_df.columns:
            final_df["재고평가감_설정"] = final_df["재고자산평가_환입"]
            print(f"[INFO] 재고평가감_설정 생성 (재고자산평가_환입 값 사용)")
    
    # 재고자산평가_설정, 재고자산평가_환입 컬럼 제거
    cols_to_remove = []
    if "재고자산평가_설정" in final_df.columns:
        cols_to_remove.append("재고자산평가_설정")
    if "재고자산평가_환입" in final_df.columns:
        cols_to_remove.append("재고자산평가_환입")
    
    if cols_to_remove:
        final_df = final_df.drop(columns=cols_to_remove)
        print(f"[INFO] 재고자산평가 필드 제거: {cols_to_remove}")
    
    # 행열 전환 시 생성된 원본 순서대로 컬럼 유지
    # 첫 번째 브랜드 결과의 컬럼 순서를 기준으로 사용
    # (각 브랜드가 같은 원본 파일 형식을 사용한다고 가정)
    if len(all_results) > 0:
        # 첫 번째 결과의 컬럼 순서 저장 (base_cols 제외)
        base_cols = ["브랜드", "Version", "채널"]
        first_result = all_results[0]
        reference_column_order = list(first_result.columns)
        
        # base_cols를 앞에, 나머지는 원본 순서대로
        final_column_order = base_cols.copy()
        seen_in_order = set(base_cols)
        
        # reference_column_order 순서대로 추가 (base_cols 제외)
        for col in reference_column_order:
            if col not in seen_in_order:
                final_column_order.append(col)
                seen_in_order.add(col)
        
        # reference_column_order에 없지만 final_df에 있는 컬럼 추가 (알파벳 순)
        remaining_cols = sorted([col for col in final_df.columns if col not in seen_in_order])
        final_column_order.extend(remaining_cols)
        
        # 존재하는 컬럼만 선택
        final_column_order = [col for col in final_column_order if col in final_df.columns]
        
        # 매출원가(환입후)를 매출원가율(%) 앞으로 이동
        if "매출원가(환입후)" in final_column_order and "매출원가율(%)" in final_column_order:
            매출원가_idx = final_column_order.index("매출원가(환입후)")
            매출원가율_idx = final_column_order.index("매출원가율(%)")
            
            # 매출원가(환입후)를 제거하고 매출원가율(%) 앞에 삽입
            final_column_order.remove("매출원가(환입후)")
            if 매출원가_idx < 매출원가율_idx:
                # 매출원가가 매출원가율 앞에 있었으면, 매출원가율 앞에 삽입
                final_column_order.insert(매출원가율_idx - 1, "매출원가(환입후)")
            else:
                # 매출원가가 매출원가율 뒤에 있었으면, 매출원가율 앞에 삽입
                final_column_order.insert(매출원가율_idx, "매출원가(환입후)")
            
            print(f"[INFO] 매출원가(환입후)를 매출원가율(%) 앞으로 이동")
        
        final_df = final_df[final_column_order]
        
        print(f"[INFO] 행열 전환 순서대로 컬럼 유지: {len(final_column_order)}개 컬럼")
    else:
        # all_results가 비어있으면 기존 순서 유지
        print(f"[INFO] 컬럼 순서 유지 (변경 없음)")
    
    # 비율 필드 재계산 (마지막 단계)
    print(f"[INFO] 비율 필드 재계산 중...")
    final_df = recalculate_ratios(final_df)
    print(f"[OK] 비율 필드 재계산 완료")
    
    # 데이터 포맷팅 (천단위 콤마, 소숫점 한자리)
    print(f"[INFO] 데이터 포맷팅 중...")
    final_df = format_plan_data(final_df)
    print(f"[OK] 데이터 포맷팅 완료")
    
    return final_df

def extract_analysis_month_from_plan_files(plan_dir: str) -> str:
    """
    계획 파일명에서 분석월 추출
    
    Args:
        plan_dir: 계획 데이터 폴더 경로
    
    Returns:
        str: YYYYMM 형식의 분석월 (예: "202511")
    
    Raises:
        FileNotFoundError: 계획 파일을 찾을 수 없는 경우
    """
    if not os.path.exists(plan_dir):
        raise FileNotFoundError(f"[ERROR] 계획 데이터 폴더가 없습니다: {plan_dir}")
    
    # 계획 파일 찾기 (YYYYMMR_*.csv 형식)
    for filename in os.listdir(plan_dir):
        if filename.endswith(".csv") and "R_" in filename:
            # 파일명에서 분석월 추출 (예: 202511R_I.csv -> 202511)
            match = re.match(r'(\d{6})R_', filename)
            if match:
                year_month = match.group(1)
                print(f"[INFO] 계획 파일에서 분석월 추출: {filename} -> {year_month}")
                return year_month
    
    raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다. 형식: YYYYMMR_*.csv ({plan_dir})")


def main():
    """메인 함수"""
    import sys
    
    # 연월 추출 (명령줄 인자 또는 파일명에서 추출)
    if len(sys.argv) > 1:
        year_month = sys.argv[1]  # 예: "202511"
        print(f"[INFO] 명령줄 인자에서 분석월 사용: {year_month}")
    else:
        # 파일명에서 추출
        # 기본 경로: raw/ 폴더에서 최신 계획 파일 찾기
        raw_dir = os.path.join(ROOT, "raw")
        if os.path.exists(raw_dir):
            # 월별 폴더 찾기 (YYYYMM 형식)
            month_dirs = [d for d in os.listdir(raw_dir) 
                         if os.path.isdir(os.path.join(raw_dir, d)) and d.isdigit() and len(d) == 6]
            month_dirs.sort(reverse=True)  # 최신순
            
            for month_dir in month_dirs:
                plan_dir = os.path.join(raw_dir, month_dir, "plan")
                if os.path.exists(plan_dir):
                    try:
                        year_month = extract_analysis_month_from_plan_files(plan_dir)
                        print(f"[INFO] 파일명에서 분석월 추출: {year_month}")
                        break
                    except FileNotFoundError:
                        continue
            else:
                year_month = "202511"
                print(f"[WARNING] 계획 파일을 찾을 수 없어 기본값 사용: {year_month}")
        else:
            year_month = "202511"
            print(f"[WARNING] raw 폴더가 없어 기본값 사용: {year_month}")
    
    print("="*60)
    print("계획 데이터 전처리")
    print("="*60)
    print(f"분석월: {year_month}")
    
    try:
        # 파일 처리
        result_df = process_plan_files(year_month)
        
        # 저장
        output_path = get_plan_file_path(year_month)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        
        print(f"[OK] 저장 완료: {output_path}")
        print(f"[OK] 총 {len(result_df)}행, {len(result_df.columns)}컬럼")
        
    except Exception as e:
        print(f"[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
