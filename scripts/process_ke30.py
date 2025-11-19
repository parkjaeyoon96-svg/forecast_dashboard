"""
SAP KE30 자재별 손익 데이터 전처리 스크립트
"""
import pandas as pd
import numpy as np
import json
from datetime import datetime
import os

def load_ke30_data(file_path='raw/sap_ke30.csv'):
    """KE30 CSV 파일 로드"""
    try:
        # CSV 인코딩 자동 감지 (SAP는 보통 cp949 또는 utf-8)
        try:
            df = pd.read_csv(file_path, encoding='cp949')
        except:
            df = pd.read_csv(file_path, encoding='utf-8')
        
        print(f"✅ KE30 데이터 로드: {len(df)}건")
        print(f"   컬럼: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
        return None
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return None

def clean_ke30_data(df):
    """데이터 정제"""
    print("\n🔧 데이터 정제 중...")
    
    # 1. 컬럼명 정리 (공백 제거, 소문자 변환)
    df.columns = df.columns.str.strip()
    
    # 2. 숫자 컬럼 정리 (쉼표 제거, 문자를 숫자로 변환)
    numeric_columns = df.select_dtypes(include=['object']).columns
    for col in numeric_columns:
        # 숫자처럼 보이는 컬럼만 변환
        try:
            # 쉼표, 공백 제거 후 숫자 변환 시도
            df[col] = df[col].astype(str).str.replace(',', '').str.replace(' ', '')
            df[col] = pd.to_numeric(df[col], errors='ignore')
        except:
            pass
    
    # 3. 결측치 처리
    df = df.fillna(0)
    
    # 4. 중복 제거
    before_count = len(df)
    df = df.drop_duplicates()
    after_count = len(df)
    if before_count != after_count:
        print(f"   중복 제거: {before_count - after_count}건")
    
    print(f"✅ 데이터 정제 완료: {len(df)}건")
    return df

def aggregate_ke30_data(df):
    """
    자재별 데이터를 집계하고 대시보드용 KPI 계산
    """
    print("\n📊 데이터 집계 중...")
    
    # 실제 컬럼명에 맞게 수정하세요
    # 현재는 샘플 데이터 기준
    
    # 1. 자재별 집계
    agg_config = {
        '수량': 'sum',
        '매출액': 'sum',
        '매출원가': 'sum',
        '판매관리비': 'sum',
        '영업이익': 'sum',
        '고정비': 'sum',
        '변동비': 'sum',
    }
    
    material_summary = df.groupby(['자재코드', '자재명']).agg(agg_config).reset_index()
    
    # 계산 컬럼 추가
    material_summary['매출총이익'] = material_summary['매출액'] - material_summary['매출원가']
    material_summary['이익률'] = (material_summary['영업이익'] / material_summary['매출액'] * 100).round(2)
    material_summary['원가율'] = (material_summary['매출원가'] / material_summary['매출액'] * 100).round(2)
    
    print(f"   - 자재별 집계: {len(material_summary)}건")
    
    # 2. 일자별 집계 (트렌드 분석용)
    if '일자' in df.columns:
        daily_summary = df.groupby('일자').agg({
            '매출액': 'sum',
            '매출원가': 'sum',
            '영업이익': 'sum',
        }).reset_index()
        daily_summary = daily_summary.sort_values('일자')
        print(f"   - 일자별 집계: {len(daily_summary)}건")
    else:
        daily_summary = None
    
    print(f"✅ 데이터 집계 완료")
    
    return {
        'material_summary': material_summary,
        'daily_summary': daily_summary,
        'raw_data': df
    }

def enrich_with_master(df, master_data):
    """마스터 데이터와 조인하여 정보 보강"""
    print("\n🔗 마스터 데이터 조인 중...")
    
    if master_data is None:
        print("   ⚠️ 마스터 데이터 없음 - 스킵")
        return df
    
    # 예시: 제품 마스터와 조인
    # df = df.merge(master_data, left_on='자재코드', right_on='제품코드', how='left')
    
    return df

def validate_data(df):
    """데이터 검증"""
    print("\n✓ 데이터 검증 중...")
    
    issues = []
    
    # 1. 필수 컬럼 체크
    # required_columns = ['자재코드', '매출액']
    # missing_columns = [col for col in required_columns if col not in df.columns]
    # if missing_columns:
    #     issues.append(f"필수 컬럼 누락: {missing_columns}")
    
    # 2. 음수 체크 (필요한 경우)
    # if '매출액' in df.columns:
    #     negative_count = (df['매출액'] < 0).sum()
    #     if negative_count > 0:
    #         issues.append(f"음수 매출액: {negative_count}건")
    
    # 3. 결측치 체크
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        issues.append(f"결측치 발견: {null_counts[null_counts > 0].to_dict()}")
    
    if issues:
        print("   ⚠️ 검증 이슈:")
        for issue in issues:
            print(f"      - {issue}")
    else:
        print("   ✅ 검증 통과")
    
    return issues

def save_processed_data(df, output_path='data/ke30_processed.json'):
    """처리된 데이터 저장"""
    print(f"\n💾 데이터 저장 중: {output_path}")
    
    # JSON으로 변환
    data_dict = {
        'data': df.to_dict('records'),
        'metadata': {
            'record_count': len(df),
            'columns': df.columns.tolist(),
            'processed_at': datetime.now().isoformat(),
            'data_types': df.dtypes.astype(str).to_dict()
        },
        'summary': {
            # 요약 통계 (예시)
            # 'total_sales': float(df['매출액'].sum()) if '매출액' in df.columns else 0,
            # 'total_cost': float(df['매출원가'].sum()) if '매출원가' in df.columns else 0,
        }
    }
    
    # 디렉토리 생성
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 저장 완료: {len(df)}건")

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("SAP KE30 데이터 전처리 시작")
    print("=" * 60)
    
    # 1. 데이터 로드
    df = load_ke30_data('raw/sap_ke30.csv')
    if df is None:
        print("\n❌ 데이터 로드 실패. 종료합니다.")
        print("\n사용법:")
        print("  1. SAP에서 KE30 데이터를 CSV로 다운로드")
        print("  2. raw/sap_ke30.csv 파일로 저장")
        print("  3. 이 스크립트 실행: python scripts/process_ke30.py")
        return
    
    print("\n📋 원본 데이터 미리보기:")
    print(df.head())
    print(f"\n데이터 타입:\n{df.dtypes}")
    
    # 2. 데이터 정제
    df = clean_ke30_data(df)
    
    # 3. 데이터 집계
    df = aggregate_ke30_data(df)
    
    # 4. 마스터 데이터 조인 (선택적)
    # master_data = load_master_data()  # 별도 함수 필요
    # df = enrich_with_master(df, master_data)
    
    # 5. 데이터 검증
    issues = validate_data(df)
    
    # 6. 결과 저장
    save_processed_data(df)
    
    print("\n" + "=" * 60)
    print("✅ KE30 데이터 전처리 완료!")
    print("=" * 60)
    print(f"\n처리 결과:")
    print(f"  - 최종 레코드 수: {len(df)}")
    print(f"  - 출력 파일: data/ke30_processed.json")
    print(f"  - 검증 이슈: {len(issues)}건")
    
    if issues:
        print("\n⚠️ 데이터를 확인해주세요!")

if __name__ == "__main__":
    main()

