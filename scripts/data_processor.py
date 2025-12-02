"""
대용량 엑셀 데이터 처리 스크립트
130,983줄 이상의 로데이터를 처리하여 대시보드용 JSON/JS로 변환
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime
import os
import sys
from pathlib import Path

class DataProcessor:
    def __init__(self, raw_data_path='../raw_data', master_data_path='../master_data'):
        self.raw_data_path = Path(raw_data_path)
        self.master_data_path = Path(master_data_path)
        self.processed_data_path = Path('../processed_data')
        
        # 출력 폴더 생성
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        (self.processed_data_path / 'weekly').mkdir(exist_ok=True)
        (self.processed_data_path / 'monthly').mkdir(exist_ok=True)
        
        self.load_masters()
    
    def load_masters(self):
        """마스터 데이터 로드"""
        print("마스터 데이터 로딩 중...")
        
        try:
            self.channel_master = pd.read_csv(
                self.master_data_path / 'channel_master.csv',
                encoding='utf-8-sig'
            )
            self.item_master = pd.read_csv(
                self.master_data_path / 'item_master.csv',
                encoding='utf-8-sig'
            )
            self.brand_master = pd.read_csv(
                self.master_data_path / 'brand_master.csv',
                encoding='utf-8-sig'
            )
            
            with open(self.master_data_path / 'mapping_rules.json', 'r', encoding='utf-8') as f:
                self.rules = json.load(f)
            
            print("✅ 마스터 데이터 로드 완료")
        except Exception as e:
            print(f"❌ 마스터 데이터 로드 실패: {e}")
            sys.exit(1)
    
    def load_raw_data(self, file_path, chunk_size=50000):
        """대용량 엑셀 파일 청크 단위로 로드"""
        print(f"로드 데이터 로딩 중... (청크 크기: {chunk_size:,}줄)")
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
        
        chunks = []
        total_rows = 0
        
        try:
            # 엑셀 파일 청크 단위로 읽기
            excel_file = pd.ExcelFile(file_path)
            
            # 모든 시트 처리
            for sheet_name in excel_file.sheet_names:
                print(f"  시트 '{sheet_name}' 처리 중...")
                
                for chunk in pd.read_excel(file_path, sheet_name=sheet_name, chunksize=chunk_size):
                    chunk = self.optimize_dtypes(chunk)
                    chunks.append(chunk)
                    total_rows += len(chunk)
                    print(f"    처리됨: {total_rows:,}줄")
            
            df = pd.concat(chunks, ignore_index=True)
            print(f"✅ 전체 데이터 로드 완료: {len(df):,}줄")
            return df
            
        except Exception as e:
            print(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def optimize_dtypes(self, df):
        """메모리 사용 최적화"""
        # 숫자 컬럼 최적화
        numeric_cols = ['매출액', '수량', '단가', '비용', '직접이익', 'Sales', 'Quantity', 'Price', 'Cost', 'DirectProfit']
        for col in df.columns:
            if col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
        
        # 날짜 컬럼 변환
        date_cols = ['일자', 'Date', '날짜']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # 카테고리 타입으로 변환 (메모리 절약)
        text_cols = ['브랜드', 'Brand', '채널', 'Channel', '아이템', 'Item']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')
        
        return df
    
    def clean_and_map(self, df):
        """데이터 정제 및 마스터 매핑"""
        print("데이터 정제 및 매핑 중...")
        
        original_len = len(df)
        
        # 컬럼명 통일 (영문/한글 모두 지원)
        column_mapping = {
            'Brand': '브랜드', 'Channel': '채널', 'Item': '아이템',
            'Sales': '매출액', 'Quantity': '수량', 'Price': '단가',
            'Cost': '비용', 'DirectProfit': '직접이익', 'Date': '일자'
        }
        df = df.rename(columns=column_mapping)
        
        # 필수 컬럼 확인
        required_cols = ['브랜드', '채널', '일자', '매출액']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"필수 컬럼이 없습니다: {missing_cols}")
        
        # 마스터 데이터와 조인
        df = df.merge(
            self.channel_master[['채널명', '채널코드', '채널그룹']],
            left_on='채널', right_on='채널명', how='left'
        )
        df = df.merge(
            self.item_master[['아이템명', '아이템코드', '아이템그룹']],
            left_on='아이템', right_on='아이템명', how='left'
        )
        df = df.merge(
            self.brand_master[['브랜드명', '브랜드키']],
            left_on='브랜드', right_on='브랜드명', how='left'
        )
        
        # 매핑되지 않은 데이터 제거
        before_drop = len(df)
        df = df.dropna(subset=['브랜드키', '채널코드'])
        after_drop = len(df)
        
        if before_drop != after_drop:
            print(f"  ⚠️ 매핑 실패 데이터 {before_drop - after_drop:,}줄 제거됨")
        
        # 채널 그룹핑 규칙 적용
        if '채널그룹' not in df.columns or df['채널그룹'].isna().any():
            df['채널그룹'] = df['채널'].map(self.rules.get('channel_grouping', {})).fillna(df['채널'])
        
        # 직접이익이 없으면 계산
        if '직접이익' not in df.columns or df['직접이익'].isna().any():
            if '매출액' in df.columns and '비용' in df.columns:
                df['직접이익'] = df['매출액'] - df['비용']
            else:
                df['직접이익'] = 0
        
        print(f"✅ 정제 완료: {len(df):,}줄 (원본: {original_len:,}줄)")
        return df
    
    def get_week_number(self, date):
        """날짜를 주차 번호로 변환"""
        if pd.isna(date):
            return None
        day = date.day
        if day <= 7:
            return 1
        elif day <= 14:
            return 2
        elif day <= 21:
            return 3
        elif day <= 28:
            return 4
        else:
            return 5
    
    def calculate_aggregations(self, df, year, month):
        """주차별 및 월별 집계"""
        print("집계 계산 중...")
        
        # 주차 계산
        df['주차'] = df['일자'].apply(self.get_week_number)
        df = df.dropna(subset=['주차'])
        
        # 브랜드별 집계
        result = {
            'year': year,
            'month': month,
            'brands': {}
        }
        
        for brand_key in df['브랜드키'].unique():
            brand_data = df[df['브랜드키'] == brand_key]
            brand_result = {
                'weekly': {},
                'channels': {},
                'items': {}
            }
            
            # 주차별 집계
            for week in range(1, 6):
                week_data = brand_data[brand_data['주차'] == week]
                if len(week_data) > 0:
                    brand_result['weekly'][week] = {
                        'sales': float(week_data['매출액'].sum()),
                        'profit': float(week_data['직접이익'].sum()),
                        'quantity': int(week_data.get('수량', pd.Series([0])).sum())
                    }
            
            # 채널별 집계
            for channel in brand_data['채널코드'].unique():
                channel_data = brand_data[brand_data['채널코드'] == channel]
                brand_result['channels'][channel] = {
                    'sales': float(channel_data['매출액'].sum()),
                    'profit': float(channel_data['직접이익'].sum()),
                    'forecast': 0  # 나중에 계산
                }
            
            # 아이템별 집계
            for item in brand_data['아이템코드'].unique():
                item_data = brand_data[brand_data['아이템코드'] == item]
                brand_result['items'][item] = {
                    'sales': float(item_data['매출액'].sum()),
                    'profit': float(item_data['직접이익'].sum())
                }
            
            result['brands'][brand_key] = brand_result
        
        print("✅ 집계 완료")
        return result
    
    def apply_business_logic(self, aggregated_data):
        """비즈니스 로직 적용"""
        print("비즈니스 로직 적용 중...")
        
        for brand_key, brand_data in aggregated_data['brands'].items():
            # 평가감율 및 직접비율 가져오기
            eval_rate = self.rules['evaluation_rate'].get(brand_key, 1.0)
            profit_rate = self.rules['direct_profit_rate'].get(brand_key, 0.1)
            
            # 주차별 예상값 계산
            for week, data in brand_data['weekly'].items():
                # 월말 예상 = 실적 / 평가감율
                data['forecast'] = data['sales'] / eval_rate if eval_rate > 0 else data['sales']
                data['margin'] = (data['profit'] / data['sales'] * 100) if data['sales'] > 0 else 0
            
            # 채널별 예상값 계산
            for channel, data in brand_data['channels'].items():
                data['forecast'] = data['sales'] / eval_rate if eval_rate > 0 else data['sales']
        
        print("✅ 비즈니스 로직 적용 완료")
        return aggregated_data
    
    def process_file(self, input_file, output_file=None):
        """메인 처리 함수"""
        print(f"\n{'='*60}")
        print(f"데이터 처리 시작: {Path(input_file).name}")
        print(f"{'='*60}\n")
        
        start_time = datetime.now()
        
        # 1. 데이터 로드
        df = self.load_raw_data(input_file)
        
        # 2. 날짜에서 년/월 추출
        if '일자' in df.columns:
            date_col = df['일자'].dropna().iloc[0] if len(df['일자'].dropna()) > 0 else datetime.now()
            year = date_col.year
            month = date_col.month
        else:
            year = datetime.now().year
            month = datetime.now().month
        
        # 3. 데이터 정제
        df = self.clean_and_map(df)
        
        # 4. 집계
        aggregated = self.calculate_aggregations(df, year, month)
        
        # 5. 비즈니스 로직 적용
        final_data = self.apply_business_logic(aggregated)
        
        # 6. 결과 저장
        if output_file is None:
            output_file = self.processed_data_path / 'weekly' / f"{year:04d}{month:02d}.json"
        
        self.save_as_json(final_data, output_file)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"✅ 처리 완료! (소요 시간: {elapsed:.2f}초)")
        print(f"   결과 파일: {output_file}")
        print(f"{'='*60}\n")
        
        return final_data
    
    def save_as_json(self, data, output_path):
        """JSON 파일로 저장"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        file_size = output_path.stat().st_size / 1024  # KB
        print(f"✅ 저장 완료: {output_path} ({file_size:.2f} KB)")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='대용량 엑셀 데이터 처리')
    parser.add_argument('input', help='입력 엑셀 파일 경로')
    parser.add_argument('-o', '--output', help='출력 JSON 파일 경로')
    parser.add_argument('--raw-path', default='../raw_data', help='로데이터 경로')
    parser.add_argument('--master-path', default='../master_data', help='마스터 데이터 경로')
    
    args = parser.parse_args()
    
    processor = DataProcessor(
        raw_data_path=args.raw_path,
        master_data_path=args.master_path
    )
    
    processor.process_file(args.input, args.output)


if __name__ == "__main__":
    main()





































