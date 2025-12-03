"""
KE30 당년 로데이터 정제 스크립트
=================================

작업 흐름:
[1차 전처리] -> ke30_YYYYMMDD_YYYYMM_전처리완료.csv 저장
1. C:\ke30\ke30_YYYYMMDD_YYYYMM.xlsx 파일 읽기
2. CSV로 변환하여 저장 (원본 파일명 유지)
3. 데이터 전처리 실행:
   - 브랜드 공란 제거
   - PH01-1이 'E0100'인 행 제거 (저장품 제거)
   - 유통채널이 '9'인 행 제거 (수출 제외)
   - 아이템코드 필드 생성 (필터링 후, 집계 전)
4. 피벗 집계 (브랜드, 시즌, 유통채널, 고객, PH01-1, PH01-2, PH01-3, 아이템코드)
5. 마스터 파일 기반 컬럼 추가:
   - 채널명 추가 (유통채널 옆에 위치)
   - 아이템_대분류 추가
   - 아이템_중분류 추가
   - 아이템_소분류 추가
6. 1차 전처리 완료 파일 저장 (raw/YYYYMM/current_year/YYYYMMDD/)

[2-1차 전처리] -> ke30_YYYYMMDD_YYYYMM_Shop_item.csv 저장
1. 원가 계산 필드 추가:
   - 표준제간비 = 제간비율 x 표준매출원가
   - 재고평가감 환입 = IFERROR(-Tag매출*((표준매출원가/판매tag금액*1.1)-평가율)/1.1,0)
   - 매출원가(평가감환입반영) = 표준매출원가+표준제간비+재고평가감 환입
   - 매출총이익 = 출고매출 - 매출원가(평가감환입반영)
2. 2차 집계 (행: 브랜드, 유통코드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드)
   값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익

[2-2차 전처리] -> ke30_YYYYMMDD_YYYYMM_Shop.csv 저장
1. 원가 계산 필드 추가 (동일)
2. 2차 집계 (행: 브랜드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드)
   값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익

작성일: 2025-11-14
수정일: 2025-11-21
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
JEONGANBI_RATE_MASTER_PATH = os.path.join(MASTER_DIR, "표준제간비율.csv")
EVALUATION_RATE_MASTER_PATH = os.path.join(MASTER_DIR, "평가율마스터.csv")

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


def load_jeonganbi_rate_master():
    """
    표준제간비율 마스터 파일 로드
    
    Returns:
        pd.DataFrame: 표준제간비율 마스터 데이터
    """
    try:
        df = pd.read_csv(JEONGANBI_RATE_MASTER_PATH, encoding='utf-8-sig')
        print(f"[OK] 표준제간비율 마스터 로드: {len(df)}행")
        return df
    except Exception as e:
        print(f"[ERROR] 표준제간비율 마스터 로드 실패: {e}")
        raise


def load_evaluation_rate_master():
    """
    평가율 마스터 파일 로드
    
    Returns:
        pd.DataFrame: 평가율 마스터 데이터
    """
    try:
        df = pd.read_csv(EVALUATION_RATE_MASTER_PATH, encoding='utf-8-sig')
        print(f"[OK] 평가율 마스터 로드: {len(df)}행")
        return df
    except Exception as e:
        print(f"[ERROR] 평가율 마스터 로드 실패: {e}")
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


def find_ke30_file_by_date(update_date: str):
    """
    지정된 업데이트일자에 해당하는 KE30 엑셀 파일 찾기
    
    Args:
        update_date: YYYYMMDD 형식의 업데이트일자
    
    Returns:
        tuple: (파일 전체 경로, 파일명(확장자 제외)) 또는 (None, None) if not found
    """
    if not os.path.exists(KE30_INPUT_DIR):
        raise FileNotFoundError(f"[ERROR] 폴더가 없습니다: {KE30_INPUT_DIR}")
    
    # ke30_로 시작하고 지정된 날짜를 포함하는 엑셀 파일 찾기
    files = []
    for filename in os.listdir(KE30_INPUT_DIR):
        if filename.startswith(FILE_PREFIX) and update_date in filename and (filename.endswith('.xlsx') or filename.endswith('.xls')):
            filepath = os.path.join(KE30_INPUT_DIR, filename)
            mtime = os.path.getmtime(filepath)
            # 확장자 제거한 파일명
            base_name = os.path.splitext(filename)[0]
            files.append((filepath, mtime, filename, base_name))
    
    if not files:
        return None, None
    
    # 최신 파일 선택 (수정 시간 기준)
    latest_file = sorted(files, key=lambda x: x[1], reverse=True)[0]
    print(f"[OK] 지정된 날짜의 파일 발견: {latest_file[2]}")
    
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
    2. PH01-1이 'E0100'인 행 제거
    3. 유통채널이 '9'인 행 제거
    4. 아이템코드 필드 생성 (필터링 후, 집계 전)
    6. 피벗 집계 (브랜드, 시즌, 유통채널, 고객, PH01-1, PH01-2, PH01-3, 아이템코드)
    7. 컬럼 추가 (채널명, 아이템_대분류, 아이템_중분류, 아이템_소분류)
    
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
    # 1) 브랜드 공란 제거 + 특정 브랜드(C) 제외
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
    after_blank = len(df)
    print(f"   브랜드 공란 제거: {before_count}행 → {after_blank}행 ({before_count - after_blank}행 삭제)")

    # 브랜드가 'C'인 행 제거 (최초 전처리 단계에서 완전히 제외)
    before_c = len(df)
    df = df[df['브랜드'].astype(str).str.strip() != 'C']
    after_c = len(df)
    if before_c != after_c:
        print(f"   브랜드='C' 제거: {before_c}행 → {after_c}행 ({before_c - after_c}행 삭제)")
    
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
    # 1-3) 아이템코드 필드 생성 (필터링 후, 집계 전)
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
    
    1) 채널명 (유통채널 옆에 위치)
    2) 아이템_대분류 (PH01-1의 우측에 위치)
    3) 아이템_중분류 (PH01-2의 우측에 위치)
    4) 아이템_소분류 (아이템코드의 우측에 위치)
    
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
    
    # 채널명을 유통채널 옆에 위치시키기
    cols = list(df_result.columns)
    if "유통채널" in cols and "채널명" in cols:
        # 유통채널의 인덱스 찾기
        channel_idx = cols.index("유통채널")
        # 채널명 제거
        cols.remove("채널명")
        # 유통채널 다음에 채널명 삽입
        cols.insert(channel_idx + 1, "채널명")
        df_result = df_result[cols]
    
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
    
    # 아이템_대분류를 PH01-1의 우측에 위치시키기
    cols = list(df_result.columns)
    if "PH01-1" in cols and "아이템_대분류" in cols:
        # PH01-1의 인덱스 찾기
        ph01_1_idx = cols.index("PH01-1")
        # 아이템_대분류 제거
        cols.remove("아이템_대분류")
        # PH01-1 다음에 아이템_대분류 삽입
        cols.insert(ph01_1_idx + 1, "아이템_대분류")
        df_result = df_result[cols]
    
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
    
    # 아이템_중분류를 PH01-2의 우측에 위치시키기
    cols = list(df_result.columns)
    if "PH01-2" in cols and "아이템_중분류" in cols:
        # PH01-2의 인덱스 찾기
        ph01_2_idx = cols.index("PH01-2")
        # 아이템_중분류 제거
        cols.remove("아이템_중분류")
        # PH01-2 다음에 아이템_중분류 삽입
        cols.insert(ph01_2_idx + 1, "아이템_중분류")
        df_result = df_result[cols]
    
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
    
    # 아이템_소분류를 아이템코드의 우측에 위치시키기
    cols = list(df_result.columns)
    if "아이템코드" in cols and "아이템_소분류" in cols:
        # 아이템코드의 인덱스 찾기
        item_code_idx = cols.index("아이템코드")
        # 아이템_소분류 제거
        cols.remove("아이템_소분류")
        # 아이템코드 다음에 아이템_소분류 삽입
        cols.insert(item_code_idx + 1, "아이템_소분류")
        df_result = df_result[cols]
    
    print(f"      [OK] 아이템_소분류 추가 완료")
    
    # 아이템코드는 이미 Step 2에서 생성되어 집계 기준에 포함되어 있으므로 여기서는 추가하지 않음
    
    print(f"\n[OK] 마스터 매핑 완료! 총 {len(df_result)}행")
    
    return df_result


def add_cost_calculation_fields(df, jeonganbi_master, evaluation_master, analysis_month):
    """
    표준제간비, 재고평가감 환입, 매출원가, 매출총이익 필드 추가
    
    Args:
        df: 데이터프레임
        jeonganbi_master: 제간비율 마스터
        evaluation_master: 평가율 마스터
        analysis_month: 분석월 (YYYYMM 형식, 예: "202511")
    
    Returns:
        pd.DataFrame: 필드 추가된 데이터프레임
    """
    print(f"\n[PROCESSING] 원가 계산 필드 추가 시작...")
    
    df_result = df.copy()
    
    # 필요한 컬럼 확인
    required_cols = {
        '표준매출원가': ['합계 : 표준 매출원가', '표준 매출원가', '표준매출원가'],
        'TAG매출': ['합계 : 판매금액(TAG가)', '판매금액(TAG가)', 'TAG매출'],
        '출고매출': ['합계 : 출고매출액(V-) Actual', '출고매출액(V-) Actual', '출고매출']
    }
    
    # 컬럼명 매핑
    col_mapping = {}
    for new_name, patterns in required_cols.items():
        for col in df_result.columns:
            col_str = str(col).strip()
            for pattern in patterns:
                if pattern in col_str or col_str in pattern:
                    col_mapping[new_name] = col
                    break
            if new_name in col_mapping:
                break
    
    # 필수 컬럼 확인
    if '표준매출원가' not in col_mapping:
        print("[WARNING] 표준매출원가 컬럼을 찾을 수 없습니다. 원가 계산 필드를 건너뜁니다.")
        return df_result
    
    표준매출원가_col = col_mapping.get('표준매출원가')
    TAG매출_col = col_mapping.get('TAG매출')
    출고매출_col = col_mapping.get('출고매출')
    
    print(f"   컬럼 매핑:")
    print(f"     표준매출원가: {표준매출원가_col}")
    print(f"     TAG매출: {TAG매출_col}")
    print(f"     출고매출: {출고매출_col}")
    
    # 제간비율 마스터 딕셔너리 생성 (브랜드+시즌 -> 비율)
    jeonganbi_map = {}
    
    # 컬럼명 찾기 (공백 제거하여 매칭)
    brand_col = None
    season_col = None
    rate_col = None
    
    for col in jeonganbi_master.columns:
        col_clean = str(col).strip()
        if '브랜드' in col_clean:
            brand_col = col
        elif '대상시즌' in col_clean or '시즌' in col_clean:
            season_col = col
        elif '비율' in col_clean:
            rate_col = col
    
    if brand_col and season_col and rate_col:
        print(f"   제간비율 마스터 컬럼: 브랜드={brand_col}, 시즌={season_col}, 비율={rate_col}")
        
        for _, row in jeonganbi_master.iterrows():
            brand = str(row[brand_col]).strip()
            season = str(row[season_col]).strip()
            rate_val = row[rate_col]
            
            if pd.notna(rate_val) and brand and season:
                # 퍼센트 문자열 처리 ('5%' -> 0.05, '5' -> 0.05, '0.05' -> 0.05)
                rate_str = str(rate_val).strip().replace(',', '').replace(' ', '')
                
                try:
                    # 퍼센트 기호가 있는 경우
                    if '%' in rate_str:
                        rate = float(rate_str.replace('%', '')) / 100
                    else:
                        rate = float(rate_str)
                        # 값이 1보다 크면 퍼센트로 간주하여 100으로 나눔 (예: 5 -> 0.05)
                        if rate > 1:
                            rate = rate / 100
                        # 값이 이미 소수 형태인 경우 (0~1 사이)는 그대로 사용
                except (ValueError, TypeError):
                    rate = 0
            else:
                rate = 0
            
            key = f"{brand}_{season}"
            jeonganbi_map[key] = rate
        
        print(f"   제간비율 매핑: {len(jeonganbi_map)}개 조합")
        # 샘플 출력 (최대 5개)
        sample_count = 0
        for key, rate in list(jeonganbi_map.items())[:5]:
            print(f"     {key}: {rate}")
            sample_count += 1
    else:
        print(f"   [WARNING] 제간비율 마스터 컬럼을 찾을 수 없습니다.")
        print(f"     현재 컬럼: {list(jeonganbi_master.columns)}")
    
    # 평가율 마스터 딕셔너리 생성 (브랜드+시즌+평가감환입월 -> 평가율)
    # 평가감 환입월 = 분석월의 1달 전 (예: 분석월 2025.11 -> 평가감 환입월 2025.10)
    evaluation_map = {}
    if '브랜드' in evaluation_master.columns and '시즌' in evaluation_master.columns:
        # 분석월의 1달 전 계산
        year = int(analysis_month[:4])
        month = int(analysis_month[4:6])
        
        # 1달 전 계산
        if month == 1:
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month - 1
        
        # 평가감 환입월을 YYYY.MM 형식으로 변환 (예: 202510 -> 2025.10)
        evaluation_month_formatted = f"{prev_year}.{prev_month:02d}"
        
        # 평가감 환입월 컬럼 찾기 (정확한 매칭 우선, 부분 매칭도 시도)
        month_col = None
        for col in evaluation_master.columns:
            col_str = str(col).strip()
            # 정확한 매칭
            if col_str == evaluation_month_formatted:
                month_col = col
                break
            # 부분 매칭 (예: "2025.10"이 포함된 경우)
            elif evaluation_month_formatted in col_str:
                month_col = col
                break
        
        # "2025.10" -> "2025.1" 형식 매칭 (10월이 "2025.1"로 표기된 경우)
        if not month_col and evaluation_month_formatted.endswith('.10'):
            alt_format = evaluation_month_formatted.replace('.10', '.1')
            for col in evaluation_master.columns:
                col_str = str(col).strip()
                if col_str == alt_format:
                    month_col = col
                    break
        
        if month_col:
            print(f"   평가율 컬럼 사용: {month_col} (평가감 환입월: {evaluation_month_formatted})")
            for _, row in evaluation_master.iterrows():
                brand = str(row['브랜드']).strip()
                season = str(row['시즌']).strip()
                
                # 브랜드나 시즌이 비어있으면 스킵
                if not brand or not season or brand == 'nan' or season == 'nan':
                    continue
                
                eval_rate_str = str(row[month_col]).strip() if pd.notna(row[month_col]) else ''
                
                # 평가율 문자열에서 % 제거하고 숫자로 변환 (예: "15%" -> 0.15)
                if eval_rate_str and '%' in eval_rate_str:
                    eval_rate = float(eval_rate_str.replace('%', '').replace(' ', '')) / 100
                elif eval_rate_str and eval_rate_str != 'nan':
                    try:
                        eval_rate = float(eval_rate_str)
                        # 1보다 크면 퍼센트로 간주 (예: 15 -> 0.15)
                        if eval_rate > 1:
                            eval_rate = eval_rate / 100
                    except:
                        eval_rate = 0
                else:
                    eval_rate = 0
                
                key = f"{brand}_{season}"
                evaluation_map[key] = eval_rate
            print(f"   평가율 매핑: {len(evaluation_map)}개 조합")
        else:
            print(f"   [WARNING] 평가감 환입월 컬럼을 찾을 수 없습니다: {evaluation_month_formatted} (분석월: {analysis_month})")
    
    # 1) 표준제간비 계산
    print("   1) 표준제간비 계산...")
    def calculate_jeonganbi(row):
        brand = str(row.get('브랜드', '')).strip()
        season = str(row.get('시즌', '')).strip()
        key = f"{brand}_{season}"
        rate = jeonganbi_map.get(key, 0)
        표준매출원가_val = row.get(표준매출원가_col, 0)
        if pd.isna(표준매출원가_val):
            표준매출원가_val = 0
        return float(표준매출원가_val) * rate
    
    df_result['표준제간비'] = df_result.apply(calculate_jeonganbi, axis=1)
    # 소숫점 1자리에서 반올림하여 정수로 변환
    df_result['표준제간비'] = df_result['표준제간비'].apply(lambda x: int(round(x, 1)) if pd.notna(x) else 0)
    
    # 2) 재고평가감 환입 계산
    print("   2) 재고평가감 환입 계산...")
    if not TAG매출_col:
        print("   [WARNING] TAG매출 컬럼을 찾을 수 없어 재고평가감 환입을 계산할 수 없습니다.")
        df_result['재고평가감 환입'] = 0
    else:
        def calculate_evaluation_return(row):
            brand = str(row.get('브랜드', '')).strip()
            season = str(row.get('시즌', '')).strip()
            key = f"{brand}_{season}"
            평가율 = evaluation_map.get(key, None)  # 평가율이 없으면 None 반환
            
            # 평가율이 없으면 재고평가감 환입을 0으로 처리
            if 평가율 is None:
                return 0
            
            TAG매출_val = row[TAG매출_col] if TAG매출_col in row.index else 0
            표준매출원가_val = row[표준매출원가_col] if 표준매출원가_col in row.index else 0
            
            if pd.isna(TAG매출_val) or TAG매출_val == 0:
                return 0
            if pd.isna(표준매출원가_val):
                표준매출원가_val = 0
            
            try:
                # IFERROR(-Tag매출*((표준매출원가/판매tag금액*1.1)-평가율)/1.1,0)
                import numpy as np
                result = -float(TAG매출_val) * ((float(표준매출원가_val) / float(TAG매출_val) * 1.1) - 평가율) / 1.1
                return result if not pd.isna(result) and not np.isinf(result) else 0
            except Exception as e:
                return 0
        
        df_result['재고평가감 환입'] = df_result.apply(calculate_evaluation_return, axis=1)
        # 소숫점 1자리에서 반올림하여 정수로 변환
        df_result['재고평가감 환입'] = df_result['재고평가감 환입'].apply(lambda x: int(round(x, 1)) if pd.notna(x) else 0)
        
        # 계산 결과 통계
        non_zero_count = len(df_result[df_result['재고평가감 환입'] != 0])
        print(f"   재고평가감 환입이 0이 아닌 행: {non_zero_count}개")
    
    # 3) 매출원가(평가감환입반영) 계산
    print("   3) 매출원가(평가감환입반영) 계산...")
    표준매출원가_vals = df_result[표준매출원가_col].fillna(0)
    표준제간비_vals = df_result['표준제간비'].fillna(0)
    재고평가감환입_vals = df_result['재고평가감 환입'].fillna(0)
    
    df_result['매출원가(평가감환입반영)'] = 표준매출원가_vals + 표준제간비_vals + 재고평가감환입_vals
    # 소숫점 1자리에서 반올림하여 정수로 변환
    df_result['매출원가(평가감환입반영)'] = df_result['매출원가(평가감환입반영)'].apply(lambda x: int(round(x, 1)) if pd.notna(x) else 0)
    
    # 4) 매출총이익 계산
    print("   4) 매출총이익 계산...")
    if 출고매출_col:
        출고매출_vals = df_result[출고매출_col].fillna(0)
        매출원가_vals = df_result['매출원가(평가감환입반영)'].fillna(0)
        df_result['매출총이익'] = 출고매출_vals - 매출원가_vals
        # 소숫점 1자리에서 반올림하여 정수로 변환
        df_result['매출총이익'] = df_result['매출총이익'].apply(lambda x: int(round(x, 1)) if pd.notna(x) else 0)
    else:
        df_result['매출총이익'] = 0
        print("   [WARNING] 출고매출 컬럼을 찾을 수 없어 매출총이익을 0으로 설정합니다.")
    
    # 컬럼 위치 조정: 표준매출원가 오른쪽에 필드 추가
    cols = list(df_result.columns)
    if 표준매출원가_col in cols:
        표준매출원가_idx = cols.index(표준매출원가_col)
        
        # 추가할 필드들
        new_fields = ['표준제간비', '재고평가감 환입', '매출원가(평가감환입반영)', '매출총이익']
        
        # 기존 위치에서 제거
        for field in new_fields:
            if field in cols:
                cols.remove(field)
        
        # 표준매출원가 오른쪽에 순서대로 삽입
        insert_idx = 표준매출원가_idx + 1
        for i, field in enumerate(new_fields):
            if field in df_result.columns:
                cols.insert(insert_idx + i, field)
        
        df_result = df_result[cols]
    
    print(f"   [OK] 원가 계산 필드 추가 완료")
    
    return df_result


def aggregate_by_requested_fields(df):
    """
    매출총이익 필드 추가 후 요청된 기준으로 집계
    
    행: 브랜드, 유통코드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드
    값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익
    
    Args:
        df: 원가 계산 필드가 추가된 데이터프레임
    
    Returns:
        pd.DataFrame: 집계된 데이터프레임
    """
    print(f"\n[PROCESSING] 추가 집계 시작 (브랜드, 유통코드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드)...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        '브랜드',
        '유통코드',
        '채널명',
        '아이템_중분류',
        '아이템_소분류',
        '아이템코드'
    ]
    
    # 컬럼명 매핑 (다양한 형태의 컬럼명 지원)
    col_mapping = {}
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            # 유통코드의 경우 '고객' 컬럼을 찾아서 사용
            if col == '유통코드':
                if '고객' in df.columns:
                    available_groupby.append('고객')
                    print(f"   '{col}' → '고객' 사용")
                else:
                    print(f"   [WARNING] '{col}' 컬럼 없음 (스킵)")
            else:
                # 유사한 컬럼명 찾기
                similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('-', '') == col.replace(' ', '').replace('-', '')]
                if similar:
                    available_groupby.append(similar[0])
                    print(f"   '{col}' → '{similar[0]}' 사용")
                else:
                    print(f"   [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 찾기 (더 구체적인 패턴을 먼저 찾기 위해 순서 중요)
    value_col_patterns = {
        '판매금액(TAG가)': ['합계 : 판매금액(TAG가)', '판매금액(TAG가)', 'TAG매출'],
        # 실판매액(V-)를 먼저 찾아야 함 (실판매액보다 구체적)
        '실판매액V-': ['합계 : 실판매액(V-)', '실판매액(V-)', '실판매액V-'],
        # 실판매액은 실판매액(V-)가 아닌 컬럼만 찾아야 함
        '실판매액': ['합계 : 실판매액', '실판매액'],
        '출고매출액V-': ['합계 : 출고매출액(V-) Actual', '출고매출액(V-) Actual', '출고매출액V-'],
        '매출원가(평가감환입)': ['매출원가(평가감환입반영)', '매출원가(평가감환입)', '매출원가'],
        '매출총이익': ['매출총이익']
    }
    
    available_agg = []
    used_columns = set()  # 이미 사용된 컬럼 추적
    
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
                    print(f"   '{target_name}' → '{col}' 사용")
                    found = True
                    break
                # 부분 매칭 (더 구체적인 패턴 체크)
                elif pattern in col_str:
                    # 실판매액의 경우, 실판매액(V-)가 포함된 컬럼은 제외
                    if target_name == '실판매액' and ('(V-)' in col_str or 'V-' in col_str):
                        continue
                    available_agg.append(col)
                    col_mapping[target_name] = col
                    used_columns.add(col)
                    print(f"   '{target_name}' → '{col}' 사용")
                    found = True
                    break
            if found:
                break
        if not found:
            print(f"   [WARNING] '{target_name}' 컬럼 없음 (스킵)")
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    print(f"   집계 기준 (행): {available_groupby}")
    print(f"   집계 대상 (값): {len(available_agg)}개 컬럼")
    
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
    for target_name in ['판매금액(TAG가)', '실판매액', '실판매액V-', '출고매출액V-', '매출원가(평가감환입)', '매출총이익']:
        if target_name in df_aggregated.columns:
            ordered_cols.append(target_name)
    # 나머지 컬럼
    for col in df_aggregated.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df_aggregated = df_aggregated[ordered_cols]
    
    # 집계 후 행 수
    after_count = len(df_aggregated)
    
    print(f"   [OK] 집계 완료: {before_count:,}행 → {after_count:,}행")
    
    # 집계 결과 요약
    if '브랜드' in df_aggregated.columns:
        print(f"\n   [INFO] 브랜드별 집계 건수:")
        brand_counts = df_aggregated.groupby('브랜드').size().sort_values(ascending=False)
        for brand, count in brand_counts.items():
            print(f"      {brand}: {count:,}건")
    
    if '채널명' in df_aggregated.columns:
        print(f"\n   [INFO] 채널명별 집계 건수:")
        channel_counts = df_aggregated.groupby('채널명').size().sort_values(ascending=False)
        for channel, count in channel_counts.items():
            print(f"      {channel}: {count:,}건")
    
    return df_aggregated


def aggregate_by_brand_channel(df):
    """
    브랜드, 채널명별로 집계
    
    행: 브랜드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드
    값: 판매금액(TAG가), 실판매액, 실판매액V-, 출고매출액V-, 매출원가(평가감환입), 매출총이익
    
    Args:
        df: 원가 계산 필드가 추가된 데이터프레임
    
    Returns:
        pd.DataFrame: 집계된 데이터프레임
    """
    print(f"\n[PROCESSING] 브랜드/채널별 집계 시작...")
    
    # 집계 기준 컬럼
    groupby_cols = [
        '브랜드',
        '채널명',
        '아이템_중분류',
        '아이템_소분류',
        '아이템코드'
    ]
    
    # 집계 대상 컬럼 찾기
    value_col_patterns = {
        '판매금액(TAG가)': ['합계 : 판매금액(TAG가)', '판매금액(TAG가)', 'TAG매출'],
        '실판매액V-': ['합계 : 실판매액(V-)', '실판매액(V-)', '실판매액V-'],
        '실판매액': ['합계 : 실판매액', '실판매액'],
        '출고매출액V-': ['합계 : 출고매출액(V-) Actual', '출고매출액(V-) Actual', '출고매출액V-'],
        '매출원가(평가감환입)': ['매출원가(평가감환입반영)', '매출원가(평가감환입)', '매출원가'],
        '매출총이익': ['매출총이익']
    }
    
    col_mapping = {}
    used_columns = set()
    available_agg = []
    
    # 집계 기준 컬럼 확인
    available_groupby = []
    for col in groupby_cols:
        if col in df.columns:
            available_groupby.append(col)
        else:
            similar = [c for c in df.columns if col in str(c) or str(c).replace(' ', '').replace('-', '') == col.replace(' ', '').replace('-', '')]
            if similar:
                available_groupby.append(similar[0])
                print(f"   '{col}' → '{similar[0]}' 사용")
            else:
                print(f"   [WARNING] '{col}' 컬럼 없음 (스킵)")
    
    # 집계 대상 컬럼 찾기
    for target_name, patterns in value_col_patterns.items():
        found = False
        for pattern in patterns:
            for col in df.columns:
                if col in used_columns:
                    continue
                    
                col_str = str(col).strip()
                
                if col_str == pattern:
                    available_agg.append(col)
                    col_mapping[target_name] = col
                    used_columns.add(col)
                    print(f"   '{target_name}' → '{col}' 사용")
                    found = True
                    break
                elif pattern in col_str:
                    if target_name == '실판매액' and ('(V-)' in col_str or 'V-' in col_str):
                        continue
                    available_agg.append(col)
                    col_mapping[target_name] = col
                    used_columns.add(col)
                    print(f"   '{target_name}' → '{col}' 사용")
                    found = True
                    break
            if found:
                break
        if not found:
            print(f"   [WARNING] '{target_name}' 컬럼 없음 (스킵)")
    
    if not available_groupby:
        raise ValueError("[ERROR] 집계 기준 컬럼을 찾을 수 없습니다.")
    
    if not available_agg:
        raise ValueError("[ERROR] 집계 대상 컬럼을 찾을 수 없습니다.")
    
    print(f"   집계 기준 (행): {available_groupby}")
    print(f"   집계 대상 (값): {len(available_agg)}개 컬럼")
    
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
    
    # 컬럼 순서 정리
    ordered_cols = []
    for col in groupby_cols:
        if col in df_aggregated.columns:
            ordered_cols.append(col)
    for target_name in ['판매금액(TAG가)', '실판매액', '실판매액V-', '출고매출액V-', '매출원가(평가감환입)', '매출총이익']:
        if target_name in df_aggregated.columns:
            ordered_cols.append(target_name)
    for col in df_aggregated.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df_aggregated = df_aggregated[ordered_cols]
    
    # 집계 후 행 수
    after_count = len(df_aggregated)
    
    print(f"   [OK] 집계 완료: {before_count:,}행 → {after_count:,}행")
    
    return df_aggregated


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
        jeonganbi_master = load_jeonganbi_rate_master()
        evaluation_master = load_evaluation_rate_master()
        print()
        
        # ----------------
        # Step 1: 엑셀 파일 찾기
        # ----------------
        excel_path, base_filename = find_latest_ke30_file()
        
        # ----------------
        # Step 2: 날짜 폴더 생성 (새 구조: raw/YYYYMM/current_year/YYYYMMDD/)
        # ----------------
        # 파일명에서 날짜 추출: ke30_20251117_202511.xlsx -> 20251117
        date_match = re.search(r'ke30_(\d{8})_', base_filename)
        if not date_match:
            raise ValueError(f"[ERROR] 파일명에서 날짜를 추출할 수 없습니다: {base_filename}")
        
        date_str = date_match.group(1)  # YYYYMMDD
        year_month = date_str[:6]  # YYYYMM
        
        # 새 구조: raw/YYYYMM/current_year/YYYYMMDD/
        from path_utils import get_current_year_dir
        date_output_dir = get_current_year_dir(date_str)
        os.makedirs(date_output_dir, exist_ok=True)
        print(f"[OK] 날짜 폴더 생성/확인: {year_month}/current_year/{date_str}")
        
        # ----------------
        # Step 3: CSV 변환 (날짜 폴더에 저장)
        # ----------------
        csv_output_path = os.path.join(date_output_dir, f"{base_filename}.csv")
        df_raw = convert_excel_to_csv(excel_path, csv_output_path)
        
        # ----------------
        # Step 4: [1차 전처리] 데이터 전처리 (마스터 파일 포함)
        # ----------------
        df_processed = preprocess_ke30_data(df_raw, channel_master, item_master)
        
        # ----------------
        # Step 5: [1차 전처리] 정제 완료 파일 저장 (원가 계산 전까지)
        # ----------------
        preprocessed_output_path = os.path.join(date_output_dir, f"{base_filename}_전처리완료.csv")
        df_processed.to_csv(preprocessed_output_path, index=False, encoding='utf-8-sig')
        print(f"\n[OK] [1차 전처리] 파일 저장: {preprocessed_output_path}")
        print(f"   데이터: {len(df_processed)}행 × {len(df_processed.columns)}열")
        
        # ----------------
        # Step 6: [2-1차 전처리] 원가 계산 필드 추가
        # ----------------
        df_with_cost = add_cost_calculation_fields(df_processed, jeonganbi_master, evaluation_master, year_month)
        
        # ----------------
        # Step 7: [2-1차 전처리] 브랜드/채널/아이템별 집계 (Shop_item)
        # ----------------
        df_shop_item = aggregate_by_requested_fields(df_with_cost)
        
        # ----------------
        # Step 8: [2-1차 전처리] Shop_item 파일 저장
        # ----------------
        shop_item_output_path = os.path.join(date_output_dir, f"{base_filename}_Shop_item.csv")
        df_shop_item.to_csv(shop_item_output_path, index=False, encoding='utf-8-sig')
        print(f"\n[OK] [2-1차 전처리] Shop_item 파일 저장: {shop_item_output_path}")
        print(f"   데이터: {len(df_shop_item)}행 × {len(df_shop_item.columns)}열")
        
        # ----------------
        # Step 9: [2-2차 전처리] 브랜드/채널별 집계 (Shop)
        # ----------------
        df_shop = aggregate_by_brand_channel(df_with_cost)
        
        # ----------------
        # Step 10: [2-2차 전처리] Shop 파일 저장
        # ----------------
        shop_output_path = os.path.join(date_output_dir, f"{base_filename}_Shop.csv")
        df_shop.to_csv(shop_output_path, index=False, encoding='utf-8-sig')
        print(f"\n[OK] [2-2차 전처리] Shop 파일 저장: {shop_output_path}")
        print(f"   데이터: {len(df_shop)}행 × {len(df_shop.columns)}열")
        
        print(f"\n[DATE_FOLDER] {date_str}")  # 배치 파일에서 날짜 추출용
        
        # ----------------
        # Step 11: 결과 요약
        # ----------------
        print(f"\n" + "=" * 60)
        print("[REPORT] 처리 결과 요약")
        print("=" * 60)
        print(f"[OK] 원본 파일: {os.path.basename(excel_path)}")
        print(f"[OK] 저장 폴더: {year_month}/current_year/{date_str}/")
        print(f"\n[1차 전처리 완료]")
        print(f"   - CSV 변환 파일: {year_month}/current_year/{date_str}/{base_filename.replace('.xlsx', '.csv')}")
        print(f"   - 전처리 완료 파일: {year_month}/current_year/{date_str}/{base_filename.replace('.xlsx', '')}_전처리완료.csv")
        print(f"     * 행 수: {len(df_processed):,}행, 컬럼 수: {len(df_processed.columns)}개")
        print(f"\n[2-1차 전처리 완료] Shop_item")
        print(f"   - 파일: {year_month}/current_year/{date_str}/{base_filename.replace('.xlsx', '')}_Shop_item.csv")
        print(f"     * 행 수: {len(df_shop_item):,}행, 컬럼 수: {len(df_shop_item.columns)}개")
        print(f"     * 집계 기준: 브랜드, 유통코드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드")
        print(f"\n[2-2차 전처리 완료] Shop")
        print(f"   - 파일: {year_month}/current_year/{date_str}/{base_filename.replace('.xlsx', '')}_Shop.csv")
        print(f"     * 행 수: {len(df_shop):,}행, 컬럼 수: {len(df_shop.columns)}개")
        print(f"     * 집계 기준: 브랜드, 채널명, 아이템_중분류, 아이템_소분류, 아이템코드")
        
        # 브랜드별 집계 (Shop_item 기준)
        if '브랜드' in df_shop_item.columns:
            print(f"\n[INFO] Shop_item 브랜드별 집계 건수:")
            brand_summary = df_shop_item.groupby('브랜드').size().sort_values(ascending=False)
            for brand, count in brand_summary.head(10).items():
                print(f"   {brand}: {count:,}건")
        
        # 채널명별 집계 (Shop 기준)
        if '채널명' in df_shop.columns:
            print(f"\n[INFO] Shop 채널명별 집계 건수:")
            channel_summary = df_shop.groupby('채널명').size().sort_values(ascending=False)
            for channel, count in channel_summary.head(10).items():
                print(f"   {channel}: {count:,}건")
        
        # 아이템_중분류별 집계 (Shop_item 기준)
        if '아이템_중분류' in df_shop_item.columns:
            print(f"\n[INFO] Shop_item 아이템 중분류별 집계 건수:")
            item_summary = df_shop_item.groupby('아이템_중분류').size().sort_values(ascending=False)
            for item, count in item_summary.items():
                print(f"   {item}: {count:,}건")
        
        print(f"\n[COMPLETE] 완료!")
        
        return df_shop_item
        
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


