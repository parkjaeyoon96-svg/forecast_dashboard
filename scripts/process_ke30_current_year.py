"""
KE30 당년 로데이터 정제 스크립트
=================================

작업 흐름:
1. C:\ke30\ke30_YYYYMMDD_YYYYMM.xlsx 파일 읽기
2. CSV로 변환하여 저장 (원본 파일명 유지)
3. 데이터 전처리 실행
   - 브랜드 공란 제거
   - 아이템코드 필드 생성 (필터링 후, 집계 전)
   - 피벗 집계 (브랜드, 시즌, 유통채널, 고객, PH01-1, PH01-2, PH01-3, 아이템코드)
   - 마스터 파일 기반 컬럼 추가:
     * 채널명 (채널마스터.csv)
     * 아이템_대분류, 아이템_중분류, 아이템_소분류 (아이템마스터.csv)

작성일: 2025-11-14
수정일: 2025-11-16
"""

import pandas as pd
import os
from datetime import datetime
import sys
import re

# ================================
# 설정 (Configuration)
# ================================

# 경로 설정
KE30_INPUT_DIR = r"C:\ke30"
CSV_OUTPUT_DIR = r"C:\Users\AD0283\Desktop\AIproject\Project_Forcast\raw"
MASTER_DIR = r"C:\Users\AD0283\Desktop\AIproject\Project_Forcast\Master"

# 마스터 파일 경로
CHANNEL_MASTER_PATH = os.path.join(MASTER_DIR, "채널마스터.csv")
ITEM_MASTER_PATH = os.path.join(MASTER_DIR, "아이템마스터.csv")

# 파일명 패턴 (자동 감지용)
# 예: ke30_20251114_202511.xlsx
FILE_PREFIX = "ke30_"

# ================================
# 마스터 파일 로드
# ================================

def load_channel_master():
    """
    채널 마스터 파일 로드
    
    Returns:
        pd.DataFrame: 채널 마스터 데이터
    """
    try:
        df = pd.read_csv(CHANNEL_MASTER_PATH, encoding='utf-8-sig')
        print(f"[OK] 채널 마스터 로드: {len(df)}행")
        return df
    except Exception as e:
        print(f"[ERROR] 채널 마스터 로드 실패: {e}")
        raise


def load_item_master():
    """
    아이템 마스터 파일 로드
    
    Returns:
        pd.DataFrame: 아이템 마스터 데이터
    """
    try:
        df = pd.read_csv(ITEM_MASTER_PATH, encoding='utf-8-sig')
        print(f"[OK] 아이템 마스터 로드: {len(df)}행")
        return df
    except Exception as e:
        print(f"[ERROR] 아이템 마스터 로드 실패: {e}")
        raise


# ================================
# 1단계: 엑셀 → CSV 변환
# ================================

def find_latest_ke30_file():
    """
    C:\ke30 폴더에서 최신 ke30 엑셀 파일 찾기
    
    Returns:
        tuple: (파일 전체 경로, 파일명(확장자 제외))
    """
    if not os.path.exists(KE30_INPUT_DIR):
        raise FileNotFoundError(f"[ERROR] 폴더가 없습니다: {KE30_INPUT_DIR}")
    
    # ke30_로 시작하는 모든 엑셀 파일 찾기
    files = []
    for filename in os.listdir(KE30_INPUT_DIR):
        if filename.startswith(FILE_PREFIX) and (filename.endswith('.xlsx') or filename.endswith('.xls')):
            filepath = os.path.join(KE30_INPUT_DIR, filename)
            mtime = os.path.getmtime(filepath)
            # 확장자 제거한 파일명
            base_name = os.path.splitext(filename)[0]
            files.append((filepath, mtime, filename, base_name))
    
    if not files:
        raise FileNotFoundError(f"[ERROR] {KE30_INPUT_DIR} 폴더에 ke30 엑셀 파일이 없습니다!")
    
    # 최신 파일 선택 (수정 시간 기준)
    latest_file = sorted(files, key=lambda x: x[1], reverse=True)[0]
    print(f"[OK] 최신 파일 발견: {latest_file[2]}")
    
    return latest_file[0], latest_file[3]  # (전체경로, 파일명)


def convert_excel_to_csv(excel_path, output_csv_path):
    """
    엑셀 파일을 CSV로 변환
    
    Args:
        excel_path (str): 입력 엑셀 파일 경로
        output_csv_path (str): 출력 CSV 파일 경로
    
    Returns:
        pd.DataFrame: 읽은 데이터프레임
    """
    print(f"\n[READ] 엑셀 파일 읽는 중: {excel_path}")
    
    try:
        # 엑셀 파일 읽기 (첫 번째 시트)
        df = pd.read_excel(excel_path, sheet_name=0)
        
        print(f"   - 원본 데이터: {len(df)}행 × {len(df.columns)}열")
        print(f"   - 컬럼: {list(df.columns[:5])}... (처음 5개)")
        
        # CSV로 저장
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        
        print(f"[OK] CSV 변환 완료: {output_csv_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] 엑셀 읽기 오류: {e}")
        raise


# ================================
# 2단계: 데이터 전처리
# ================================

def preprocess_ke30_data(df_raw, channel_master, item_master):
    """
    KE30 데이터 전처리
    
    작업:
    1. 브랜드 공란 제거
    2. 아이템코드 필드 생성 (필터링 후)
    3. 피벗 집계 (브랜드, 시즌, 유통채널, 고객, PH01-1, PH01-2, PH01-3, 아이템코드)
    4. 컬럼 추가 (채널명, 아이템_대분류, 아이템_중분류, 아이템_소분류)
    
    Args:
        df_raw (pd.DataFrame): 원본 CSV 데이터
        channel_master (pd.DataFrame): 채널 마스터
        item_master (pd.DataFrame): 아이템 마스터
    
    Returns:
        pd.DataFrame: 정제된 데이터
    """
    df = df_raw.copy()
    
    print(f"\n[PROCESSING] 데이터 전처리 시작...")
    print(f"   원본: {len(df)}행")
    
    # ----------------
    # 1) 브랜드 공란 제거
    # ----------------
    if '브랜드' not in df.columns:
        print(f"[WARNING]  '브랜드' 컬럼이 없습니다. 사용 가능한 컬럼: {list(df.columns[:10])}")
        brand_cols = [col for col in df.columns if '브랜드' in str(col).lower() or 'brand' in str(col).lower()]
        if brand_cols:
            print(f"   유사 컬럼 발견: {brand_cols}")
            df.rename(columns={brand_cols[0]: '브랜드'}, inplace=True)
        else:
            raise ValueError("[ERROR] '브랜드' 컬럼을 찾을 수 없습니다!")
    
    before_count = len(df)
    df = df[df['브랜드'].notna()]
    df = df[df['브랜드'].astype(str).str.strip() != '']
    after_count = len(df)
    
    print(f"   브랜드 공란 제거: {before_count}행 → {after_count}행 ({before_count - after_count}행 삭제)")
    
    # ----------------
    # 1-1) PH01-1이 'E0100'인 행 제거
    # ----------------
    if 'PH01-1' in df.columns:
        before_e0100 = len(df)
        df = df[df['PH01-1'] != 'E0100']
        after_e0100 = len(df)
        if before_e0100 != after_e0100:
            print(f"   PH01-1='E0100' 제거: {before_e0100}행 → {after_e0100}행 ({before_e0100 - after_e0100}행 삭제)")
    
    # ----------------
    # 1-2) 유통채널이 '9'인 행 제거
    # ----------------
    if '유통채널' in df.columns:
        before_channel9 = len(df)
        # 유통채널이 9 또는 9.0인 경우 제거
        df = df[df['유통채널'].astype(str).str.replace('.0', '') != '9']
        after_channel9 = len(df)
        if before_channel9 != after_channel9:
            print(f"   유통채널='9' 제거: {before_channel9}행 → {after_channel9}행 ({before_channel9 - after_channel9}행 삭제)")
    
    # ----------------
    # 1-3) 유통코드 100369 제외
    # ----------------
    if '고객' in df.columns:
        before_customer = len(df)
        # 고객(유통코드)이 100369인 경우 제거
        df = df[df['고객'].astype(str).str.replace('.0', '') != '100369']
        after_customer = len(df)
        if before_customer != after_customer:
            print(f"   유통코드='100369' 제거: {before_customer}행 → {after_customer}행 ({before_customer - after_customer}행 삭제)")
    
    # ----------------
    # 1-4) 아이템코드 필드 생성 (필터링 후, 집계 전)
    # ----------------
    if '상품' in df.columns:
        print(f"   아이템코드 필드 생성 중...")
        
        def extract_item_code(row):
            """
            상품코드에서 아이템코드 추출
            
            로직:
            - 브랜드가 'ST'인 경우: 상품코드의 8번째부터 2개 문자
            - 그 외: 상품코드의 7번째부터 2개 문자
            """
            brand = str(row.get('브랜드', ''))
            product_code = str(row.get('상품', ''))
            
            if brand == 'ST':
                # 8번째부터 2개 (인덱스 7부터)
                return product_code[7:9] if len(product_code) >= 9 else ''
            else:
                # 7번째부터 2개 (인덱스 6부터)
                return product_code[6:8] if len(product_code) >= 8 else ''
        
        df['아이템코드'] = df.apply(extract_item_code, axis=1)
        print(f"   [OK] 아이템코드 필드 생성 완료")
    
    # ----------------
    # 2) 피벗 집계
    # ----------------
    
    # 필수 컬럼 (집계 기준: 상품 제외, 아이템코드 포함)
    required_index_cols = ['브랜드', '시즌', '유통채널', '고객', 'PH01-1', 'PH01-2', 'PH01-3', '아이템코드']
    required_value_cols = [
        '합계 : 1. 매출액 Actual',
        '합계 : 제품/상품 매출액 Actual',
        '합계 : 판매금액(TAG가)',
        '합계 : 실판매액',
        '합계 : 실판매액(V-)',
        '합계 : 수수료차감매출 Actual',
        '합계 : 출고매출액(V-) Actual',
        '합계 : 2. 매출원가 Actual',
        '합계 : 표준 매출원가'
    ]
    
    # 컬럼 존재 여부 확인 및 매핑
    available_index_cols = []
    for col in required_index_cols:
        if col in df.columns:
            available_index_cols.append(col)
        else:
            similar = [c for c in df.columns if col in str(c) or str(c) in col]
            if similar:
                print(f"   '{col}' → '{similar[0]}' 사용")
                df.rename(columns={similar[0]: col}, inplace=True)
                available_index_cols.append(col)
            else:
                print(f"[WARNING]  '{col}' 컬럼 없음 (스킵)")
    
    available_value_cols = []
    for col in required_value_cols:
        if col in df.columns:
            available_value_cols.append(col)
        else:
            similar = [c for c in df.columns if col.replace('합계 : ', '') in str(c)]
            if similar:
                print(f"   '{col}' → '{similar[0]}' 사용")
                df.rename(columns={similar[0]: col}, inplace=True)
                available_value_cols.append(col)
            else:
                print(f"[WARNING]  '{col}' 컬럼 없음 (스킵)")
    
    if not available_index_cols:
        raise ValueError("[ERROR] 인덱스 컬럼을 찾을 수 없습니다!")
    
    if not available_value_cols:
        raise ValueError("[ERROR] 값 컬럼을 찾을 수 없습니다!")
    
    print(f"   집계 기준 (행): {available_index_cols}")
    print(f"   집계 대상 (값): {len(available_value_cols)}개 컬럼")
    
    # 숫자 컬럼 변환
    for col in available_value_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # GROUP BY 집계
    df_agg = df.groupby(available_index_cols, as_index=False)[available_value_cols].sum()
    
    print(f"   집계 완료: {len(df_agg)}행")
    
    # ----------------
    # 3) 컬럼 추가 (마스터 매핑)
    # ----------------
    print(f"\n[PROCESSING] 마스터 파일 매핑 시작...")
    
    df_final = add_master_columns(df_agg, channel_master, item_master)
    
    return df_final


def add_master_columns(df, channel_master, item_master):
    """
    마스터 파일 기반으로 컬럼 추가
    
    1) 채널명
    2) 아이템_대분류
    3) 아이템_중분류
    4) 아이템_소분류
    5) 아이템코드
    
    Args:
        df (pd.DataFrame): 집계된 데이터
        channel_master (pd.DataFrame): 채널 마스터
        item_master (pd.DataFrame): 아이템 마스터
    
    Returns:
        pd.DataFrame: 컬럼 추가된 데이터
    """
    df_result = df.copy()
    
    # =====================================
    # 1) 채널명 추가
    # =====================================
    print(f"   1) 채널명 매핑...")
    
    # RF 고객 리스트 (채널마스터.csv의 E, F, G열)
    rf_customers = []
    for col in ['SAP_CD', 'MAIN_CD']:  # 실제 컬럼명에 따라 조정 필요
        if col in channel_master.columns:
            rf_customers.extend(channel_master[col].dropna().astype(str).tolist())
    
    # 채널번호 -> 채널명 매핑 딕셔너리
    channel_mapping = dict(zip(
        channel_master['채널번호'].astype(str),
        channel_master['채널명']
    ))
    
    def map_channel_name(row):
        """채널명 매핑 함수"""
        customer = str(row.get('고객', ''))
        channel_raw = row.get('유통채널', '')
        
        # 유통채널을 정수로 변환 (1.0 -> 1)
        try:
            if pd.notna(channel_raw):
                channel = str(int(float(channel_raw)))
            else:
                channel = ''
        except (ValueError, TypeError):
            channel = str(channel_raw)
        
        # ① 고객이 RF 리스트에 있으면 'RF' 반환
        if customer in rf_customers:
            return 'RF'
        
        # ② 유통채널로 매핑
        return channel_mapping.get(channel, channel)
    
    df_result['채널명'] = df_result.apply(map_channel_name, axis=1)
    print(f"      [OK] 채널명 추가 완료")
    
    # =====================================
    # 2) 아이템_대분류 (PRDT_HRRC2_NM)
    # =====================================
    print(f"   2) 아이템_대분류 매핑...")
    
    item_mapping_dict = dict(zip(
        item_master['PH01-3'],
        item_master['PRDT_HRRC2_NM']
    ))
    
    df_result['아이템_대분류'] = df_result['PH01-3'].map(item_mapping_dict)
    print(f"      [OK] 아이템_대분류 추가 완료")
    
    # =====================================
    # 3) 아이템_중분류
    # =====================================
    print(f"   3) 아이템_중분류 계산...")
    
    def calculate_item_mid_category(row):
        """
        아이템 중분류 계산
        
        로직:
        - PH01-1이 'L0100'인 경우:
          * 시즌이 '25F'면 '당시즌의류'
          * 시즌에 '26'이 포함되면 '차시즌의류'
          * 그 외 '과시즌의류'
        - PH01-1이 'L0100'이 아닌 경우:
          * 아이템_대분류와 동일한 값 반환 (Headwear, Bag, Shoes 등)
        """
        ph01_1 = row.get('PH01-1', '')
        season = str(row.get('시즌', ''))
        item_large = row.get('아이템_대분류', '')
        
        if ph01_1 == 'L0100':
            # 의류인 경우: 시즌 기반 분류
            if season == '25F':
                return '당시즌의류'
            elif '26' in season:
                return '차시즌의류'
            else:
                return '과시즌의류'
        else:
            # 의류가 아닌 경우: 아이템_대분류와 동일
            return item_large
    
    df_result['아이템_중분류'] = df_result.apply(calculate_item_mid_category, axis=1)
    print(f"      [OK] 아이템_중분류 추가 완료")
    
    # =====================================
    # 4) 아이템_소분류 (PRDT_HRRC3_NM)
    # =====================================
    print(f"   4) 아이템_소분류 매핑...")
    
    item_mapping_dict_hrrc3 = dict(zip(
        item_master['PH01-3'],
        item_master['PRDT_HRRC3_NM']
    ))
    
    df_result['아이템_소분류'] = df_result['PH01-3'].map(item_mapping_dict_hrrc3)
    print(f"      [OK] 아이템_소분류 추가 완료")
    
    # 아이템코드는 이미 Step 2에서 생성되어 집계 기준에 포함되어 있으므로 여기서는 추가하지 않음
    
    print(f"\n[OK] 마스터 매핑 완료! 총 {len(df_result)}행")
    
    return df_result


# ================================
# 메인 실행 함수
# ================================

def main():
    """
    메인 실행 함수
    """
    print("=" * 60)
    print("KE30 당년 로데이터 정제 스크립트")
    print("=" * 60)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # ----------------
        # Step 0: 마스터 파일 로드
        # ----------------
        print("[INFO] 마스터 파일 로드 중...")
        channel_master = load_channel_master()
        item_master = load_item_master()
        print()
        
        # ----------------
        # Step 1: 엑셀 파일 찾기
        # ----------------
        excel_path, base_filename = find_latest_ke30_file()
        
        # ----------------
        # Step 2: 날짜 폴더 생성 (새 구조: raw/YYYYMM/present/YYYYMMDD/)
        # ----------------
        # 파일명에서 날짜 추출: ke30_20251117_202511.xlsx -> 20251117
        date_match = re.search(r'ke30_(\d{8})_', base_filename)
        if not date_match:
            raise ValueError(f"[ERROR] 파일명에서 날짜를 추출할 수 없습니다: {base_filename}")
        
        date_str = date_match.group(1)  # YYYYMMDD
        year_month = date_str[:6]  # YYYYMM
        
        # 새 구조: raw/YYYYMM/present/YYYYMMDD/
        from path_utils import get_current_year_dir
        date_output_dir = get_current_year_dir(date_str)
        os.makedirs(date_output_dir, exist_ok=True)
        print(f"[OK] 날짜 폴더 생성/확인: {year_month}/present/{date_str}")
        
        # ----------------
        # Step 3: CSV 변환 (날짜 폴더에 저장)
        # ----------------
        csv_output_path = os.path.join(date_output_dir, f"{base_filename}.csv")
        df_raw = convert_excel_to_csv(excel_path, csv_output_path)
        
        # ----------------
        # Step 4: 데이터 전처리 (마스터 파일 포함)
        # ----------------
        df_processed = preprocess_ke30_data(df_raw, channel_master, item_master)
        
        # ----------------
        # Step 5: 정제 완료 파일 저장 (날짜 폴더에 저장)
        # ----------------
        processed_output_path = os.path.join(date_output_dir, f"{base_filename}_전처리완료.csv")
        df_processed.to_csv(processed_output_path, index=False, encoding='utf-8-sig')
        
        print(f"\n[OK] 정제 완료 파일 저장: {processed_output_path}")
        print(f"   최종 데이터: {len(df_processed)}행 × {len(df_processed.columns)}열")
        print(f"\n[DATE_FOLDER] {date_str}")  # 배치 파일에서 날짜 추출용
        
        # ----------------
        # Step 5: 결과 요약
        # ----------------
        print(f"\n" + "=" * 60)
        print("[REPORT] 처리 결과 요약")
        print("=" * 60)
        print(f"[OK] 원본 파일: {os.path.basename(excel_path)}")
        print(f"[OK] 저장 폴더: {date_folder}/")
        print(f"[OK] CSV 변환 파일: {date_folder}/{base_filename}.csv")
        print(f"[OK] 정제 완료 파일: {date_folder}/{base_filename}_전처리완료.csv")
        print(f"[OK] 최종 행 수: {len(df_processed):,}행")
        print(f"[OK] 컬럼 수: {len(df_processed.columns)}개")
        
        # 브랜드별 집계
        if '브랜드' in df_processed.columns:
            print(f"\n[INFO] 브랜드별 집계:")
            brand_summary = df_processed.groupby('브랜드').size().sort_values(ascending=False)
            for brand, count in brand_summary.head(10).items():
                print(f"   {brand}: {count:,}행")
        
        # 채널명별 집계
        if '채널명' in df_processed.columns:
            print(f"\n[INFO] 채널명별 집계:")
            channel_summary = df_processed.groupby('채널명').size().sort_values(ascending=False)
            for channel, count in channel_summary.head(10).items():
                print(f"   {channel}: {count:,}행")
        
        # 아이템_대분류별 집계
        if '아이템_대분류' in df_processed.columns:
            print(f"\n[INFO] 아이템 대분류별 집계:")
            item_summary = df_processed.groupby('아이템_대분류').size().sort_values(ascending=False)
            for item, count in item_summary.items():
                print(f"   {item}: {count:,}행")
        
        print(f"\n[COMPLETE] 완료!")
        
        return df_processed
        
    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ================================
# 직접 실행 시
# ================================

if __name__ == "__main__":
    result_df = main()
    
    # 선택: 결과 미리보기
    print(f"\n[DATA] 데이터 미리보기 (처음 5행):")
    print(result_df.head())
    
    print(f"\n[DATA] 컬럼 목록:")
    for i, col in enumerate(result_df.columns, 1):
        print(f"   {i}. {col}")


