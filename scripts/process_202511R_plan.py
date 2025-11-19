r"""
202511R 계획(Plan) CSV 전처리 스크립트
 - 입력: C:\Users\AD0283\Desktop\AIproject\Project_Forcast\raw 폴더의 '202511R*.csv' 파일들
 - 채널명 정규화: 채널마스터(B열: 채널명) 기준으로 '(브랜드)백화점' → '백화점' 등 표준화
 - 브랜드별로 통합하여 한 개의 집계 파일 출력
 - 직접비: 직접비 매핑 규칙에 따라 여러 세부 항목을 '인건비/물류운송비/로열티/임차관리비/감가상각비/기타/영업비' 등으로 통합
 - 결과 컬럼: ['브랜드','구분','백화점','면세점','직영점(가두)','자사몰','제휴몰','대리점','아울렛','사입','수출','직영몰','전체']

예시 행(MLB):
브랜드,구분,백화점,면세점,직영점(가두),자사몰,제휴몰,대리점,아울렛,사입,수출,직영몰,전체
M,TAG가 [v+],9348281,10918718,2964483,1450138,2571476,6534429,,8547585,171423536,662640,42997751
M,실판매액 [v+],... (이하 생략)
M,매출원가,...
M,매출총이익 = 실판매액[v+] - 매출원가
M,직접비 = 인건비+물류운송비+로열티+임차관리비+감가상각비+기타
M,인건비,...
M,물류운송비,...
M,로열티,...
M,임차관리비,...
M,감가상각비,...
M,기타,...
M,직접이익 = 매출총이익 - 직접비
M,영업비,...
M,영업이익 = 직접이익 - 영업비
"""

from __future__ import annotations
import os
import re
import json
from typing import Dict, List, Tuple
import pandas as pd
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "raw")
MASTER_DIR = os.path.join(ROOT, "Master")
# OUTPUT_PATH는 process() 함수에서 동적으로 생성 (새 구조: raw/YYYYMM/plan/plan_YYYYMM_전처리완료.csv)
CHANNEL_MASTER_PATH = os.path.join(MASTER_DIR, "채널마스터.csv")
# 기본적으로 Master 폴더의 "직접비 마스터" 파일(예: '직접비마스터.csv' / '직접비_마스터.csv' 등)을 우선 사용하고,
# 없을 경우 config/direct_cost_mapping.json → 마지막으로 DEFAULT_DIRECT_COST_MAP 순으로 폴백
DIRECT_COST_MAP_PATH_JSON = os.path.join(ROOT, "config", "direct_cost_mapping.json")

# 표준 채널 컬럼 순서
STD_CHANNELS = ["백화점","면세점","직영점(가두)","자사몰","제휴몰","대리점","아울렛","사입","수출","직영몰","기타"]

# 기본 직접비 매핑 (config가 없을 때 사용)
DEFAULT_DIRECT_COST_MAP = {
    # 인건비 계정 예시 (요청 항목)
    "지급수수료_중간관리수수료": "인건비",
    "지급수수료_중간관리수수료(직영)": "인건비",
    "지급수수료_판매사원도급비(직영)": "인건비",
    "지급수수료_판매사원도급비(면세)": "인건비",
    # 추가 예시 키워드 기반 (필요 시 확장)
    "인건비": "인건비",
    "급여": "인건비",
    "복리후생": "인건비",
    "수수료": "인건비",
    "물류운송": "물류운송비",
    "운송": "물류운송비",
    "로열티": "로열티",
    "임차": "임차관리비",
    "임대": "임차관리비",
    "감가상각": "감가상각비",
    "상각": "감가상각비",
    "광고": "영업비",
    "판매관리비": "영업비",
    "기타": "기타"
}

def load_channel_master() -> tuple[list[str], dict]:
    """채널마스터에서 표준 채널 목록과 sap→표준명 매핑을 읽어온다."""
    if not os.path.exists(CHANNEL_MASTER_PATH):
        raise FileNotFoundError(f"[ERROR] 채널마스터가 없습니다: {CHANNEL_MASTER_PATH}")
    df = pd.read_csv(CHANNEL_MASTER_PATH, encoding="utf-8-sig")
    cols = list(df.columns)
    name_col = None
    sap_col = None
    for c in cols:
        sc = str(c)
        if name_col is None and ("채널명" in sc):
            name_col = c
        if sap_col is None and ("sap" in sc.lower()):
            sap_col = c
    if name_col is None or sap_col is None:
        raise ValueError(f"[ERROR] 채널마스터에 '채널명' / '채널sap' 컬럼이 필요합니다. 현재컬럼: {cols}")
    df = df[[name_col, sap_col]].dropna()
    df[name_col] = df[name_col].astype(str).str.strip()
    df[sap_col] = df[sap_col].astype(str).str.strip()
    sap_to_name: dict = {}
    std_list: list[str] = []
    for _, r in df.iterrows():
        nm = r[name_col]
        sp = r[sap_col]
        sap_to_name[sp] = nm
        if nm not in std_list:
            std_list.append(nm)
    # 표준 목록은 사전 정의 순서를 우선 반영
    final_std: list[str] = []
    for ch in STD_CHANNELS:
        if ch in std_list and ch not in final_std:
            final_std.append(ch)
    for ch in std_list:
        if ch not in final_std:
            final_std.append(ch)
    return final_std, sap_to_name

def _find_direct_cost_master_file() -> str|None:
    """Master 폴더에서 '직접비' 및 '마스터' 키워드가 포함된 csv/xlsx 파일을 탐색."""
    if not os.path.isdir(MASTER_DIR):
        return None
    cand = []
    for fn in os.listdir(MASTER_DIR):
        lower = fn.lower()
        if ("직접비" in fn or "direct" in lower) and ("마스터" in fn or "master" in lower):
            if lower.endswith(".csv") or lower.endswith(".xlsx") or lower.endswith(".xls"):
                cand.append(os.path.join(MASTER_DIR, fn))
    if not cand:
        # '직접비마스터' 같은 축약 파일명도 허용
        for fn in os.listdir(MASTER_DIR):
            lower = fn.lower()
            if ("직접비" in fn) and (lower.endswith(".csv") or lower.endswith(".xlsx") or lower.endswith(".xls")):
                cand.append(os.path.join(MASTER_DIR, fn))
    if not cand:
        return None
    # 최신 수정일 우선
    cand.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cand[0]

def load_direct_cost_map() -> Dict[str,str]:
    """Master 폴더의 직접비 마스터만 사용(필수)."""
    fpath = _find_direct_cost_master_file()
    if not fpath:
        raise FileNotFoundError("직접비 마스터 파일을 찾을 수 없습니다. (Master 폴더에 '직접비마스터*.csv/xlsx')")
    if fpath.lower().endswith(".csv"):
        df = pd.read_csv(fpath, encoding="utf-8-sig")
    else:
        df = pd.read_excel(fpath)
    cols = list(df.columns)
    key_col = None
    val_col = None
    for c in cols:
        sc = str(c)
        if key_col is None and ("계정명" in sc or "세부" in sc or "원천" in sc or "계정" in sc or "항목" in sc):
            key_col = c
        if val_col is None and ("계정전환" in sc or "대분류" in sc or "카테고리" in sc or "분류" in sc):
            val_col = c
    if key_col is None or val_col is None:
        if len(cols) >= 2:
            key_col, val_col = cols[0], cols[1]
        else:
            raise ValueError(f"직접비 마스터의 컬럼을 인식하지 못했습니다. (필요: 계정명/계정전환) 현재: {cols}")
    mapping: dict = {}
    for _, r in df[[key_col, val_col]].dropna().iterrows():
        k = str(r[key_col]).strip()
        v = str(r[val_col]).strip()
        if k and v:
            mapping[k] = v
    if not mapping:
        raise ValueError("직접비 마스터에 매핑 데이터가 없습니다.")
    print(f"[OK] 직접비 마스터 사용: {os.path.basename(fpath)} (entries={len(mapping)})")
    return mapping

def canonical_channel(name: str, std_channels: List[str]) -> str:
    """입력 채널명 정규화 → 표준 채널명 반환. 매칭 실패 시 원문 반환."""
    if not isinstance(name, str):
        return name
    s = str(name).strip()
    # '(브랜드)백화점' → '백화점'
    s = re.sub(r"^\([^)]*\)", "", s).strip()
    # 공백/특수문자 정리
    s_norm = re.sub(r"\s+", "", s)
    # 직접 매핑 사전 (빈번한 변형 케이스)
    alias = {
        "백화점":"백화점",
        "면세":"면세점",
        "면세점":"면세점",
        "직영가두":"직영점(가두)",
        "직영점가두":"직영점(가두)",
        "자사몰":"자사몰",
        "온라인자사몰":"자사몰",
        "제휴몰":"제휴몰",
        "대리점":"대리점",
        "아울렛":"아울렛",
        "사입":"사입",
        "수출":"수출",
        "직영몰":"직영몰",
        "온라인직영몰":"직영몰",
    }
    if s_norm in alias:
        return alias[s_norm]
    # 표준 채널명과 부분일치로 매핑
    for std in std_channels:
        key = re.sub(r"\s+","",str(std))
        if key and key in s_norm:
            return std
    return s  # fallback

def detect_brand_from_filename(path:str) -> str|None:
    """
    파일명에서 브랜드 코드 추출
    예: 202511R_M.csv -> M, 202511R_ST.csv -> ST, 202511R_I.csv -> I
    """
    base = os.path.basename(path)
    # 패턴: YYYYMMR_브랜드코드.csv 또는 YYYYMMR브랜드코드.csv
    # 예: 202511R_M.csv, 202511R_ST.csv, 202511R_I.csv
    m = re.search(r"2025\d{2}R[_\-]?([A-Za-z]+)", base)
    if m:
        code = m.group(1).upper()
        # 단일 알파벳 코드는 그대로 반환 (I, M, V, W, X)
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
        # 그 외는 원문 반환 (ST 등)
        return code
    return None

def read_plan_csv(path:str) -> pd.DataFrame:
    """
    계획 CSV는 포맷이 다양할 수 있으므로 다음을 시도:
     - 기본: 첫 컬럼을 '구분'으로 보고, 나머지 컬럼을 채널/지표로 간주
     - 일부 파일은 멀티헤더일 수 있으므로 header=None 로 읽은 뒤 헤더를 추정
    """
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp949")
    # 비어있는 컬럼명 정리 및 중복 컬럼명 방지
    cols = []
    seen = {}
    for i, c in enumerate(df.columns):
        name = str(c) if not pd.isna(c) else f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}.{seen[name]}"
        else:
            seen[name] = 0
        cols.append(name)
    df.columns = cols
    # 첫 컬럼명 정규화(구분)
    first_col = df.columns[0]
    if first_col != "구분":
        # 다른 동일명 충돌 방지: 기존 '구분' 있으면 우선 고유화
        if "구분" in df.columns:
            idxs = [i for i, c in enumerate(df.columns) if c == "구분"]
            for k in idxs[1:]:
                df.columns.values[k] = f"구분.{k}"
        df = df.rename(columns={first_col: "구분"})
    # 불필요한 완전 공백 행 제거
    if "구분" in df.columns:
        df = df[~df["구분"].isna()]
        df["구분"] = df["구분"].astype(str).str.strip()
    df = df.reset_index(drop=True)
    return df

def extract_numeric(x):
    if pd.isna(x):
        return 0.0
    if isinstance(x,(int,float)):
        return float(x)
    s = str(x).replace(",","").replace(" ","").strip()
    if s in ("", "-", "nan", "None"):
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

def build_channel_columns(df: pd.DataFrame, std_channels: List[str], sap_to_name: dict) -> Dict[str, List[str]]:
    """
    채널마스터의 sap → 표준명 매핑만을 사용하여 입력 컬럼을 표준 채널로 묶음.
    """
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", str(s))
    groups: Dict[str, List[str]] = {ch: [] for ch in std_channels}
    norm_map = {norm(k): v for k, v in sap_to_name.items()}
    for col in df.columns:
        if col == "구분":
            continue
        n = norm(col)
        if n in norm_map:
            std_name = norm_map[n]
            if std_name in groups:
                groups[std_name].append(col)
            else:
                if "기타" in groups:
                    groups["기타"].append(col)
    return groups

def sum_by_channels(row: pd.Series, ch_groups: Dict[str,List[str]]) -> Dict[str,float]:
    """
    채널별 합계 계산
    표준 채널이 아닌 경우 "기타"로 매핑
    """
    out = {ch: 0.0 for ch in STD_CHANNELS}  # 모든 표준 채널 초기화
    for ch, cols in ch_groups.items():
        if ch in out:  # 표준 채널인 경우만 처리
        if not cols:
            out[ch] = 0.0
        else:
            out[ch] = float(sum(extract_numeric(row[c]) for c in cols if c in row.index))
        else:
            # 표준 채널이 아닌 경우 "기타"로 매핑
            if "기타" in out:
                out["기타"] += float(sum(extract_numeric(row[c]) for c in cols if c in row.index))
    return out

def map_direct_cost(account_name: str, dc_map: Dict[str,str]) -> str|None:
    """
    원본 계정/구분명을 직접비 대분류로 매핑. 매핑 불가 시 None.
    우선 정확히 매칭, 없으면 키워드 포함 여부로 매핑.
    """
    if account_name in dc_map:
        return dc_map[account_name]
    for k, v in dc_map.items():
        if k and k in account_name:
            return v
    return None

def collect_files() -> List[str]:
    files = []
    if not os.path.exists(RAW_DIR):
        return files
    for fn in os.listdir(RAW_DIR):
        if re.match(r"202511R.*\.csv$", fn, re.IGNORECASE):
            files.append(os.path.join(RAW_DIR, fn))
    return sorted(list(set(files)))

def collect_files_list() -> List[str]:
    out = []
    if not os.path.exists(RAW_DIR):
        return out
    for fn in os.listdir(RAW_DIR):
        if re.match(r"202511R.*\.csv$", fn, re.IGNORECASE) and ("전처리완료" not in fn):
            out.append(os.path.join(RAW_DIR, fn))
    return sorted(out)

def process():
    std_channels, sap_to_name = load_channel_master()
    dc_map = load_direct_cost_map()
    file_list = collect_files_list()
    if not file_list:
        raise RuntimeError(f"[ERROR] {RAW_DIR} 에 202511R*.csv 파일이 없습니다.")

    print(f"[INFO] 처리 대상 파일 수: {len(file_list)}")

    # 결과를 쌓기 위한 리스트
    result_rows: List[Dict[str, object]] = []

    for path in file_list:
        print(f"[INFO] 파일 처리: {os.path.basename(path)}")
        df = read_plan_csv(path)
        brand_code = detect_brand_from_filename(path) or "NA"
        # 포맷 판별: 2열(인덱스 1)이 채널sap 인지 확인
        channel_col = df.columns[1] if len(df.columns) >= 2 else None
        long_format = False
        if channel_col:
            # df[channel_col] 값 중 sap_to_name 키와 일치하는 값이 존재하면 long 포맷으로 간주
            sample_vals = df[channel_col].astype(str).str.strip().head(50).tolist()
            hit = sum(1 for v in sample_vals if v in sap_to_name)
            long_format = hit >= max(1, len(sample_vals)//10)  # 대략 10% 이상 매칭되면 long

        # 채널 그룹핑(와이드 포맷일 때만 사용)
        ch_groups = None
        if not long_format:
            ch_groups = build_channel_columns(df, std_channels, sap_to_name)

        # 관심 있는 '구분' 라벨 정의 (표준 라벨)
        target_rows_order = [
            "TAG가 [v+]",
            "실판매액 [v+]",
            "매출원가",
            "매출총이익",
            "직접비",
            "인건비",
            "물류운송비",
            "로열티",
            "임차관리비",
            "감가상각비",
            "기타",
            "직접이익",
            "영업비",
            "영업이익"
        ]
        # 누적 버퍼
        brand_agg = {lab: {ch:0.0 for ch in STD_CHANNELS} for lab in target_rows_order}

        # 1) 값 적재
        if long_format:
            # 롱 포맷: 1열=구분, 2열=채널sap, 3열 이후 숫자 합산
            value_cols = [c for c in df.columns[2:] if c != "구분"]
            df["_채널표준"] = df[channel_col].astype(str).str.strip().map(lambda x: sap_to_name.get(x, "기타"))
            for idx, row in df.iterrows():
                label = str(row.get("구분","")).strip()
                if not label:
                    continue
                label_no_space = label.replace(" ", "")
                normalized_label = None
                if "TAG" in label_no_space and ("[v+]" in label_no_space or "v+" in label_no_space):
                    normalized_label = "TAG가 [v+]"
                elif ("실판매액" in label_no_space) and ("[v+]" in label_no_space or "v+" in label_no_space):
                    normalized_label = "실판매액 [v+]"
                elif "매출원가" in label_no_space:
                    normalized_label = "매출원가"
                elif "영업비" in label_no_space:
                    normalized_label = "영업비"
                else:
                    cat = map_direct_cost(label_no_space, dc_map)
                    if cat:
                        normalized_label = cat
                if not normalized_label:
                    continue
                ch_std = row["_채널표준"]
                if ch_std not in STD_CHANNELS:
                    ch_std = "기타"
                val_sum = float(sum(extract_numeric(row[c]) for c in value_cols))
                if normalized_label not in brand_agg:
                    brand_agg[normalized_label] = {c:0.0 for c in STD_CHANNELS}
                brand_agg[normalized_label][ch_std] += val_sum
        else:
            # 와이드 포맷(기존): 구분별로 채널 컬럼 묶음 합산
            for idx, row in df.iterrows():
                label = str(row.get("구분","")).strip()
                if not label:
                    continue
                label_no_space = label.replace(" ", "")
                normalized_label = label  # 기본값

                if "TAG" in label_no_space and ("[v+]" in label_no_space or "v+" in label_no_space):
                    normalized_label = "TAG가 [v+]"
                elif ("실판매액" in label_no_space) and ("[v+]" in label_no_space or "v+" in label_no_space):
                    normalized_label = "실판매액 [v+]"
                elif "매출원가" in label_no_space:
                    normalized_label = "매출원가"
                elif "영업비" in label_no_space:
                    normalized_label = "영업비"
                else:
                    # 직접비 세부항목 매핑 시도
                    cat = map_direct_cost(label_no_space, dc_map)
                    if cat:
                        normalized_label = cat
                    else:
                        continue  # 관심없는 행은 스킵

                # 채널 합산
                sums = sum_by_channels(row, ch_groups)
                for ch, val in sums.items():
                    if normalized_label not in brand_agg:
                        # 신규 라벨이면 등록
                        brand_agg[normalized_label] = {c:0.0 for c in STD_CHANNELS}
                    brand_agg[normalized_label][ch] += val

        # 2) 파생 항목 계산
        # 매출총이익 = 실판매액[v+] - 매출원가
        for ch in STD_CHANNELS:
            gmv = brand_agg["실판매액 [v+]"].get(ch,0.0)
            cogs = brand_agg["매출원가"].get(ch,0.0)
            brand_agg["매출총이익"][ch] = gmv - cogs

        # 직접비 = (인건비+물류운송비+로열티+임차관리비+감가상각비+기타)
        for ch in STD_CHANNELS:
            direct = 0.0
            for sub in ["인건비","물류운송비","로열티","임차관리비","감가상각비","기타"]:
                direct += brand_agg.get(sub,{}).get(ch,0.0)
            brand_agg["직접비"][ch] = direct

        # 직접이익 = 매출총이익 - 직접비
        for ch in STD_CHANNELS:
            brand_agg["직접이익"][ch] = brand_agg["매출총이익"].get(ch,0.0) - brand_agg["직접비"].get(ch,0.0)

        # 영업이익 = 직접이익 - 영업비
        for ch in STD_CHANNELS:
            brand_agg["영업이익"][ch] = brand_agg["직접이익"].get(ch,0.0) - brand_agg["영업비"].get(ch,0.0)

        # 3) 결과 행 구성 (브랜드별)
        for lab in target_rows_order:
            row_out = {
                "브랜드": brand_code,
                "구분": lab
            }
            total = 0.0
            for ch in STD_CHANNELS:
                val = brand_agg.get(lab,{}).get(ch,0.0)
                row_out[ch] = round(val, 0)
            # '전체' = 채널 합계 - '수출'
            sum_except_export = sum(row_out.get(ch,0.0) for ch in STD_CHANNELS if ch != "수출")
            row_out["전체"] = round(sum_except_export, 0)
            result_rows.append(row_out)

    # 4) 결과 DataFrame & 정렬
    res_df = pd.DataFrame(result_rows)
    # 구분 순서 유지
    res_df["구분"] = pd.Categorical(res_df["구분"], categories=[
        "TAG가 [v+]",
        "실판매액 [v+]",
        "매출원가",
        "매출총이익",
        "직접비",
        "인건비","물류운송비","로열티","임차관리비","감가상각비","기타",
        "직접이익",
        "영업비",
        "영업이익"
    ], ordered=True)

    # 컬럼 순서
    cols = ["브랜드","구분"] + STD_CHANNELS + ["전체"]
    # 누락된 채널 컬럼 보정
    for ch in STD_CHANNELS:
        if ch not in res_df.columns:
            res_df[ch] = 0.0
    if "전체" not in res_df.columns:
        res_df["전체"] = 0.0
    res_df = res_df[cols]
    # 브랜드/구분/채널값이 전부 0인 행은 제거
    value_cols = STD_CHANNELS + ["전체"]
    res_df = res_df[res_df[value_cols].sum(axis=1) != 0]

    # 저장 경로 결정 (새 구조: raw/YYYYMM/plan/plan_YYYYMM_전처리완료.csv)
    # 파일명에서 연월 추출: 202511R_*.csv -> 202511
    year_month = None
    for fn in os.listdir(RAW_DIR):
        if re.match(r"202511R.*\.csv$", fn, re.IGNORECASE) and ("전처리완료" not in fn):
            # 202511R 형식에서 연월 추출
            match = re.search(r'(\d{6})R', fn)
            if match:
                year_month = match.group(1)
                break
    
    if not year_month:
        # 기본값으로 202511 사용 (하드코딩된 값)
        year_month = "202511"
        print(f"[WARNING] 연월을 자동으로 추출할 수 없어 기본값 사용: {year_month}")
    
    from path_utils import get_plan_file_path
    output_path = get_plan_file_path(year_month)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    res_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[OK] 저장: {output_path} ({len(res_df)} rows)")

if __name__ == "__main__":
    process()


