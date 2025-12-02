"""
직접비 마스터 항목으로 집계하는 스크립트
===============================================================
1. 직접비 마스터 파일 로드
2. 각 직접비 항목을 마스터 항목으로 집계
3. 맨 우측에 직접비 합계 추가
"""

import pandas as pd
import os
import sys
from pathlib import Path
from typing import Dict

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 경로 설정
MASTER_DIR = project_root / "Master"
DIRECT_COST_MASTER_PATH = MASTER_DIR / "직접비마스터.csv"


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
    conversion_col = None  # 계정전환 컬럼
    
    for col in df.columns:
        col_str = str(col).strip()
        if account_col is None and ("계정명" in col_str or "세부" in col_str):
            account_col = col
        if conversion_col is None and ("계정전환" in col_str or "대분류" in col_str):
            conversion_col = col
    
    if account_col is None or conversion_col is None:
        # 기본값: 두 번째와 세 번째 컬럼
        if len(df.columns) >= 3:
            account_col, conversion_col = df.columns[1], df.columns[2]
        elif len(df.columns) >= 2:
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


def aggregate_direct_costs_by_master(input_file: str, output_file: str = None):
    """
    KE30 파일의 직접비 항목을 마스터 항목으로 집계
    
    Args:
        input_file: 입력 파일 경로
        output_file: 출력 파일 경로 (None이면 입력 파일명에 '_마스터집계' 추가)
    """
    print("\n" + "=" * 60)
    print("직접비 마스터 항목으로 집계 시작")
    print("=" * 60)
    
    # 직접비 마스터 로드
    direct_cost_master = load_direct_cost_master()
    
    # KE30 파일 읽기
    print(f"\n[읽기] {input_file}")
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    print(f"  원본 데이터: {len(df)}행 × {len(df.columns)}열")
    
    # 직접비 항목 컬럼 찾기
    direct_cost_columns = []
    for col in df.columns:
        if col in direct_cost_master:
            direct_cost_columns.append(col)
    
    print(f"\n[처리] 직접비 항목 {len(direct_cost_columns)}개 발견")
    
    # 마스터 항목별로 집계
    master_categories = {}
    for cost_col in direct_cost_columns:
        master_category = direct_cost_master.get(cost_col, '기타')
        if master_category not in master_categories:
            master_categories[master_category] = []
        master_categories[master_category].append(cost_col)
    
    print(f"\n[집계] 마스터 항목 {len(master_categories)}개:")
    for master_cat, cost_items in master_categories.items():
        print(f"  {master_cat}: {', '.join(cost_items)}")
    
    # 각 마스터 항목별로 합계 계산
    for master_cat, cost_items in master_categories.items():
        df[master_cat] = df[cost_items].sum(axis=1).astype(int)
    
    # 전체 직접비 합계 계산 (모든 직접비 항목 합계)
    all_direct_cost_cols = [col for col in df.columns if col in direct_cost_master]
    df['직접비 합계'] = df[all_direct_cost_cols].sum(axis=1).astype(int)
    
    # 출력 파일 경로 설정
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_마스터집계.csv"
    
    # 파일 저장
    print(f"\n[저장] {output_file}")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  저장 완료: {len(df)}행 × {len(df.columns)}열")
    print(f"  추가된 컬럼: {', '.join(master_categories.keys())}, 직접비 합계")
    
    print("\n" + "=" * 60)
    print("[COMPLETE] 직접비 마스터 집계 완료!")
    print("=" * 60)
    
    return df


if __name__ == "__main__":
    # 기본 파일 경로
    input_file = "raw/202511/current_year/20251117/ke30_20251117_202511_전처리완료_직접비계산.csv"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    aggregate_direct_costs_by_master(input_file)







