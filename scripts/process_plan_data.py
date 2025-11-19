"""
계획 데이터 전처리 스크립트
- 입력: raw/YYYYMM/plan/ 폴더의 브랜드별 CSV 파일들 (예: 202511R_M.csv)
- 출력: raw/YYYYMM/plan/plan_YYYYMM_전처리완료.csv

처리 단계:
1. 내수합계 계산: Unassigned - 수출 = 내수합계 (각 지표별로)
2. 직접비 마스터 매핑 후 집계
3. 행열 전환 (pivot)
4. Unassigned, 수출, 실판금액이 없는 행 제거
5. 브랜드별 파일 통합
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
    
    # 구분별로 집계
    aggregated = direct_cost_df.groupby("구분")[numeric_cols].sum().reset_index()
    aggregated["브랜드"] = direct_cost_df["브랜드"].iloc[0]
    aggregated["Version"] = direct_cost_df["Version"].iloc[0]
    
    # 내수합계도 집계
    if "내수합계" in direct_cost_df.columns:
        domestic_sum = direct_cost_df.groupby("구분")["내수합계"].sum().reset_index()
        aggregated = aggregated.merge(domestic_sum, on="구분", how="left")
    
    # 다른 행들과 합치기
    other_df = df.loc[other_indices].copy()
    
    # 기존 직접비 행 제거하고 집계된 행 추가
    result_df = pd.concat([other_df, aggregated], ignore_index=True)
    
    return result_df

def pivot_data(df: pd.DataFrame, channels: List[str], channel_map: Dict[str, str]) -> pd.DataFrame:
    """행열 전환: 채널을 행으로, 지표를 컬럼으로 (채널명 변환 및 동일 채널명 집계 포함)"""
    # 채널 컬럼 목록 (내수합계 포함)
    all_channel_cols = channels + ["내수합계"] if "내수합계" in df.columns else channels
    
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
    
    return result_df


def recalculate_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """비율 필드 재계산"""
    df = df.copy()
    
    # 필요한 컬럼 확인
    tag_vp = "TAG가 [v+]"
    sales_vp = "실판매액 [v+]"
    sales_vm = "실판매액 [v-]"
    출고매출 = "매출(출고)"  # 출고매출액 [v-]
    원가 = "원가"
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
    
    # 3. 매출원가율(%) = 원가/실판매액[v-] * 100 (퍼센트로 표시)
    if 원가 in df.columns and sales_vm in df.columns:
        try:
            mask = df[sales_vm] != 0
            df.loc[mask, "매출원가율(%)"] = ((df.loc[mask, 원가] / df.loc[mask, sales_vm]) * 100).round(2)
            df.loc[~mask, "매출원가율(%)"] = 0
        except:
            df["매출원가율(%)"] = 0
    
    # 4. 직접제조원가율(%) = 원가/TAG가[v+] * 1.1 * 100 (퍼센트로 표시)
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
    
    # 브랜드별 파일 찾기
    plan_files = []
    for filename in os.listdir(plan_dir):
        if filename.startswith(f"{year_month}R_") and filename.endswith(".csv"):
            filepath = os.path.join(plan_dir, filename)
            plan_files.append(filepath)
    
    if not plan_files:
        raise FileNotFoundError(f"[ERROR] 계획 파일을 찾을 수 없습니다: {plan_dir}")
    
    print(f"[INFO] 처리 대상 파일 수: {len(plan_files)}")
    
    all_results = []
    
    for filepath in plan_files:
        filename = os.path.basename(filepath)
        print(f"[INFO] 파일 처리 중: {filename}")
        
        try:
            # 1. CSV 읽기
            df, channels, brand_code, version = read_plan_csv(filepath)
            
            # 2. 내수합계 계산
            df = add_domestic_total(df, channels)
            
            # 2-1. Measure units 행 제거 (내수합계 계산 후)
            df = df[~df["구분"].astype(str).str.contains("Measure units", case=False, na=False)]
            df = df[df["구분"].astype(str).str.strip() != "Measure units"]
            
            # 3. 직접비 마스터 매핑 후 집계
            df = aggregate_direct_costs(df, dc_map)
            
            # 4. 행열 전환 (채널명 변환 포함)
            df_pivoted = pivot_data(df, channels, channel_map)
            
            # 5. 필터링
            df_filtered = filter_rows(df_pivoted)
            
            all_results.append(df_filtered)
            
        except Exception as e:
            print(f"[ERROR] 파일 처리 실패 ({filename}): {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_results:
        raise RuntimeError("[ERROR] 처리된 파일이 없습니다.")
    
    # 브랜드별 결과 통합
    final_df = pd.concat(all_results, ignore_index=True)
    
    # 채널 순서 정의 (사용자 요청 순서)
    channel_order = [
        "내수합계",
        "백화점",
        "면세점",
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
    
    # 표준 컬럼 순서 정의 (사용자 요청 순서)
    standard_column_order = [
        "브랜드",
        "Version",
        "채널",
        "TAG가 [v+]",
        "실판매액 [v+]",
        "실판매액 [v-]",
        "수수료차감매출 [v-]",
        "할인율(%)",
        "출고율(%)",
        "매출(출고)",
        "매출원가",
        "재고평가감_환입",
        "재고평가감_설정",
        "재고자산평가",
        "원가",
        "매출원가율(%)",
        "직접제조원가율(%)",
        "매출총이익",
        "매출총이익율(%)",
        "인건비",
        "물류운송비",
        "로열티",
        "임차관리비",
        "감가상각비",
        "기타",
        "직접비",
        "직접이익",
        "직접이익율(%)",
        "영업비",
        "영업이익",
        "영업이익율(%)"
    ]
    
    # 표준 순서에 있는 컬럼과 없는 컬럼 분리
    base_cols = ["브랜드", "Version", "채널"]
    ordered_cols = []
    remaining_cols = []
    
    # 표준 순서대로 컬럼 추가
    for col in standard_column_order:
        if col in final_df.columns:
            ordered_cols.append(col)
    
    # 표준 순서에 없는 컬럼 찾기 (예: 내수합계, 일반 관리비 등)
    for col in final_df.columns:
        if col not in ordered_cols and col not in base_cols:
            remaining_cols.append(col)
    
    # 표준 순서에 없는 컬럼이 있으면 경고 출력
    if remaining_cols:
        print(f"[INFO] 표준 순서에 없는 컬럼 발견: {remaining_cols}")
        print(f"      이 컬럼들은 표준 순서 뒤에 알파벳 순으로 배치됩니다.")
    
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
    
    # 최종 컬럼 순서: base_cols + 표준 순서 + 나머지 (알파벳 순)
    final_column_order = base_cols + [col for col in standard_column_order if col in final_df.columns] + sorted(remaining_cols)
    
    # 존재하는 컬럼만 선택 (중복 제거)
    final_column_order = []
    seen_in_order = set()
    for col in base_cols + [col for col in standard_column_order if col in final_df.columns] + sorted(remaining_cols):
        if col in final_df.columns and col not in seen_in_order:
            final_column_order.append(col)
            seen_in_order.add(col)
    
    final_df = final_df[final_column_order]
    
    # 비율 필드 재계산 (마지막 단계)
    print(f"[INFO] 비율 필드 재계산 중...")
    final_df = recalculate_ratios(final_df)
    print(f"[OK] 비율 필드 재계산 완료")
    
    return final_df

def main():
    """메인 함수"""
    import sys
    
    # 연월 추출 (명령줄 인자 또는 기본값)
    if len(sys.argv) > 1:
        year_month = sys.argv[1]  # 예: "202511"
    else:
        # 기본값: 현재 폴더 구조에서 추출
        year_month = "202511"
        print(f"[WARNING] 연월이 지정되지 않아 기본값 사용: {year_month}")
    
    print("="*60)
    print("계획 데이터 전처리")
    print("="*60)
    print(f"연월: {year_month}")
    
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
