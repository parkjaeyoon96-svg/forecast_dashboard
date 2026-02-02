"""
주요 채널별 손익데이터 처리 스크립트

- 당년: 채널별집계파일 (미지정 채널 제외)
- 전년: 채널별집계파일 (공통 채널 제외)
- 계획: 계획전처리파일 (내수합계 제외)

표기 단위:
- 매출/직접이익: 억원 (소수점 1자리)
- 할인율: 소수점 1자리
- 전년대비/계획대비: 정수 (%)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import os
import sys

# 스크립트 디렉토리를 path에 추가
script_dir = Path(__file__).parent
sys.path.append(str(script_dir))


def get_project_root() -> Path:
    """프로젝트 루트 경로 반환"""
    return Path(__file__).parent.parent


class ChannelProfitLossProcessor:
    """채널별 손익데이터 처리기"""
    
    # 채널 순서 정의
    CHANNEL_ORDER = [
        '백화점', '면세점', 'RF', '직영점(가두)', '자사몰', 
        '제휴몰', '대리점', '사입', '직영몰', '아울렛', '기타'
    ]
    
    # 제외할 채널
    EXCLUDE_CURRENT = ['미지정']  # 당년: 미지정 채널 제외
    EXCLUDE_PREVIOUS = ['공통']   # 전년: 공통 채널 제외
    EXCLUDE_PLAN = ['내수합계']   # 계획: 내수합계 제외
    
    def __init__(self, base_date: str = None, target_month: str = None):
        """
        Args:
            base_date: 기준 날짜 (YYYYMMDD 형식, 예: '20251124')
            target_month: 대상 월 (YYYYMM 형식, 예: '202511')
        """
        self.project_root = get_project_root()
        self.base_date = base_date or datetime.now().strftime('%Y%m%d')
        self.target_month = target_month or datetime.now().strftime('%Y%m')
        
        # 데이터 경로 설정
        self.raw_path = self.project_root / 'raw' / self.target_month
        
        # 결과 데이터 저장용
        self.current_year_data = None  # 당년
        self.previous_year_data = None  # 전년
        self.plan_data = None  # 계획
        
    def load_current_year_data(self, use_forecast: bool = True) -> pd.DataFrame:
        """
        당년 채널별 집계 데이터 로드
        
        Args:
            use_forecast: True면 forecast 파일 사용 (월말 예상), False면 ke30 파일 사용 (현재 실적)
        """
        if use_forecast:
            file_path = self.raw_path / 'current_year' / self.base_date / f'forecast_{self.base_date}_{self.target_month}_Shop.csv'
        else:
            file_path = self.raw_path / 'current_year' / self.base_date / f'ke30_{self.base_date}_{self.target_month}_Shop.csv'
        
        if not file_path.exists():
            # forecast 파일이 없으면 ke30 파일 시도
            if use_forecast:
                file_path = self.raw_path / 'current_year' / self.base_date / f'ke30_{self.base_date}_{self.target_month}_Shop.csv'
                if not file_path.exists():
                    print(f"⚠️ 당년 데이터 파일을 찾을 수 없습니다")
                    return None
            else:
                print(f"⚠️ 당년 데이터 파일을 찾을 수 없습니다: {file_path}")
                return None
            
        print(f"📂 당년 데이터 로드 중: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 컬럼명 표준화
        df = df.rename(columns={
            '합계 : 판매금액(TAG가)': 'TAG가',
            '합계 : 실판매액': '실판매액',
            '합계 : 실판매액(V-)': '실판매액_V-',
            '합계 : 출고매출액(V-) Actual': '출고매출액',
            '매출원가(평가감환입반영)': '매출원가',
            '직접비 합계': '직접비',
        })
        
        # 미지정 채널 제외
        df = df[~df['채널명'].isin(self.EXCLUDE_CURRENT)]
        
        # 숫자 컬럼 변환
        numeric_cols = ['TAG가', '실판매액', '실판매액_V-', '출고매출액', '매출원가', '매출총이익', '직접비', '직접이익']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 유통채널 숫자형 변환
        df['유통채널'] = pd.to_numeric(df['유통채널'], errors='coerce')
        
        self.current_year_data = df
        print(f"✅ 당년 데이터 로드 완료: {len(df)} 행")
        return df
    
    def load_previous_year_data(self) -> pd.DataFrame:
        """전년 채널별 집계 데이터 로드"""
        file_path = self.raw_path / 'previous_year' / f'previous_rawdata_{self.target_month}_Shop.csv'
        
        if not file_path.exists():
            print(f"⚠️ 전년 데이터 파일을 찾을 수 없습니다: {file_path}")
            return None
            
        print(f"📂 전년 데이터 로드 중: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 컬럼명 표준화
        df = df.rename(columns={
            '브랜드코드': '브랜드',
            'TAG매출액': 'TAG가',
            '실매출액': '실판매액',
            '부가세제외 실판매액': '실판매액_V-',
            '매출원가(환입후매출원가+평가감(추가))': '매출원가',
            '직접비 합계': '직접비',
        })
        
        # ★★★ 전년 직접이익: 공통 채널 포함 전체 채널 직접이익 합계 ★★★
        # 공통 채널 제외하지 않음 (직접이익 계산 시 공통 채널 포함)
        # df = df[~df['채널명'].isin(self.EXCLUDE_PREVIOUS)]  # 주석 처리
        
        # 숫자 컬럼 변환
        numeric_cols = ['TAG가', '실판매액', '실판매액_V-', '매출원가', '매출총이익', '직접비', '직접이익']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        self.previous_year_data = df
        print(f"✅ 전년 데이터 로드 완료: {len(df)} 행")
        return df
    
    def load_plan_data(self) -> pd.DataFrame:
        """계획 데이터 로드"""
        file_path = self.raw_path / 'plan' / f'plan_{self.target_month}_전처리완료.csv'
        
        if not file_path.exists():
            print(f"⚠️ 계획 데이터 파일을 찾을 수 없습니다: {file_path}")
            return None
            
        print(f"📂 계획 데이터 로드 중: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        print(f"  ℹ️ 계획 데이터 컬럼: {list(df.columns)[:10]}...")  # 처음 10개만 출력
        print(f"  ℹ️ 계획 데이터 행 수: {len(df)}")
        
        # 컬럼명 표준화
        df = df.rename(columns={
            '채널': '채널명',
            'TAG가 [v+]': 'TAG가',
            '실판매액 [v+]': '실판매액',
            '실판매액 [v-]': '실판매액_V-',
            '수수료차감매출 [v-]': '출고매출액',
            '할인율(%)': '할인율_원본',
        })
        
        # ★★★ 계획 데이터: 롱 포맷 (브랜드, Version, 채널명, TAG가, 실판매액, ..., 직접이익, ...) ★★★
        # 채널명 컬럼이 있으면 롱 포맷
        if '채널명' in df.columns:
            print(f"  ✓ 계획 데이터: 롱 포맷 확인")
            print(f"  ✓ 채널명 예시: {df['채널명'].unique()[:8].tolist()}")
            print(f"  ✓ 브랜드 예시: {df['브랜드'].unique().tolist()}")
        
        # 숫자형 변환 (쉼표 제거)
        numeric_cols = ['TAG가', '실판매액', '실판매액_V-', '출고매출액', '매출원가', '매출총이익', '직접비', '직접이익']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('-', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 숫자형 변환 (쉼표 제거)
        numeric_cols = ['TAG가', '실판매액', '실판매액_V-', '출고매출액', '매출원가', '매출총이익', '직접비', '직접이익']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('-', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        self.plan_data = df
        print(f"✅ 계획 데이터 로드 완료: {len(df)} 행")
        return df
    
    def calculate_discount_rate(self, tag_price: float, actual_price: float) -> float:
        """할인율 계산: (TAG가 - 실판매액) / TAG가 * 100"""
        if tag_price == 0 or pd.isna(tag_price):
            return 0.0
        return ((tag_price - actual_price) / tag_price) * 100
    
    def calculate_profit_rate(self, direct_profit: float, actual_price: float) -> float:
        """직접이익율 계산: 직접이익 / 실판매출 * 1.1 * 100"""
        if actual_price == 0 or pd.isna(actual_price):
            return 0.0
        # 직접이익/실판매출*1.1*100 (예: 98.5/343.2*1.1*100 = 31.6%)
        return (direct_profit / actual_price) * 1.1 * 100
    
    def to_억원(self, value: float) -> float:
        """원 단위를 억원 단위로 변환 (소수점 2자리)"""
        return round(value / 100000000, 2)
    
    def aggregate_by_channel(self, df: pd.DataFrame, brand: str = None, is_plan_data: bool = False) -> pd.DataFrame:
        """채널별 집계"""
        if df is None or df.empty:
            # 빈 DataFrame을 반환할 때는 최소한 '채널명' 컬럼을 포함한 빈 DataFrame 반환
            return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
        
        # 브랜드 필터링
        if brand:
            df = df[df['브랜드'] == brand]
        
        # ★★★ 계획 데이터 처리 ★★★
        if is_plan_data:
            # 계획 데이터 형식 확인:
            # 1. 롱 포맷 (지표 컬럼): 브랜드, 채널명, TAG가, 실판매액, 직접이익 등 (구분 컬럼 없음)
            # 2. 롱 포맷 (구분 컬럼): 브랜드, 구분, 채널명, 값
            # 3. 와이드 포맷: 브랜드, 구분, 백화점, 면세점, ... (채널이 컬럼)
            
            # 케이스 1: 지표 컬럼이 직접 있는 롱 포맷 (구분 컬럼 없음)
            if '채널명' in df.columns and '실판매액' in df.columns and '구분' not in df.columns:
                print(f"  ✓ 계획 데이터: 롱 포맷 (지표 컬럼)")
                # 내수합계 제외하고 채널별 집계
                df_channels = df[df['채널명'] != '내수합계']
                
                # 채널별 집계
                agg_dict = {
                    'TAG가': 'sum',
                    '실판매액': 'sum',
                    '직접이익': 'sum'
                }
                available_cols = {k: v for k, v in agg_dict.items() if k in df_channels.columns}
                
                if available_cols:
                    grouped = df_channels.groupby('채널명').agg(available_cols).reset_index()
                    print(f"  ✓ 계획 데이터 집계: {len(grouped)}개 채널")
                    for _, row in grouped.iterrows():
                        print(f"    - {row['채널명']}: 매출 {row['실판매액']/100000000:.1f}억원")
                else:
                    return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
            
            # 케이스 2: 구분 컬럼이 있는 롱 포맷
            elif '구분' in df.columns and '채널명' in df.columns:
                # 롱 포맷: 브랜드, 구분, 채널명, 값 형태
                # 실판매액 데이터 (구분에 '실판매액' 포함)
                revenue_df = df[df['구분'].str.contains('실판매액', na=False, case=False)].copy()
                # 직접이익 데이터
                profit_df = df[df['구분'].str.contains('직접이익', na=False, case=False)].copy()
                # TAG가 데이터
                tag_df = df[df['구분'].str.contains('TAG가', na=False, case=False)].copy()
                
                # 값 컬럼 찾기 (브랜드, 구분, 채널명 제외한 숫자형 컬럼)
                value_cols = [col for col in df.columns 
                             if col not in ['브랜드', '구분', '채널명'] 
                             and pd.api.types.is_numeric_dtype(df[col])]
                
                if not value_cols:
                    print(f"⚠️ 계획 데이터에서 값 컬럼을 찾을 수 없습니다. 컬럼: {list(df.columns)}")
                    return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
                
                # 첫 번째 숫자형 컬럼을 값으로 사용 (일반적으로 하나의 값 컬럼만 있음)
                value_col = value_cols[0]
                
                # 각 구분별로 채널명 기준 집계
                grouped = pd.DataFrame()
                
                if not revenue_df.empty:
                    revenue_grouped = revenue_df.groupby('채널명')[value_col].sum().reset_index()
                    revenue_grouped.rename(columns={value_col: '실판매액'}, inplace=True)
                    grouped = revenue_grouped
                    print(f"  ✓ 계획 매출 데이터: {len(revenue_grouped)}개 채널")
                
                if not profit_df.empty:
                    profit_grouped = profit_df.groupby('채널명')[value_col].sum().reset_index()
                    profit_grouped.rename(columns={value_col: '직접이익'}, inplace=True)
                    if grouped.empty:
                        grouped = profit_grouped
                    else:
                        grouped = grouped.merge(profit_grouped, on='채널명', how='outer')
                    print(f"  ✓ 계획 직접이익 데이터: {len(profit_grouped)}개 채널")
                
                if not tag_df.empty:
                    tag_grouped = tag_df.groupby('채널명')[value_col].sum().reset_index()
                    tag_grouped.rename(columns={value_col: 'TAG가'}, inplace=True)
                    if grouped.empty:
                        grouped = tag_grouped
                    else:
                        grouped = grouped.merge(tag_grouped, on='채널명', how='outer')
                
                # 누락된 컬럼은 0으로 채우기
                for col in ['TAG가', '실판매액', '직접이익']:
                    if col not in grouped.columns:
                        grouped[col] = 0.0
                
                if grouped.empty:
                    print(f"⚠️ 계획 데이터 집계 결과가 비어있습니다")
                    return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
                
                print(f"  ✓ 계획 데이터 집계 완료: {len(grouped)}개 채널")
            else:
                # 와이드 포맷: 브랜드, 구분, 백화점, 면세점, ... 형태 (채널이 컬럼)
                # 행열 전환 필요: 구분을 행으로, 채널을 열로
                print(f"  ℹ️ 계획 데이터가 와이드 포맷입니다. 행열 전환 수행...")
                
                # 브랜드, 구분 제외한 컬럼이 채널명
                channel_cols = [col for col in df.columns if col not in ['브랜드', '구분']]
                
                if not channel_cols:
                    print(f"⚠️ 계획 데이터에서 채널 컬럼을 찾을 수 없습니다.")
                    return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
                
                # 행열 전환: 구분을 인덱스로, 채널을 컬럼으로
                # 실판매액 데이터
                revenue_df = df[df['구분'].str.contains('실판매액', na=False, case=False)].copy()
                # 직접이익 데이터
                profit_df = df[df['구분'].str.contains('직접이익', na=False, case=False)].copy()
                # TAG가 데이터
                tag_df = df[df['구분'].str.contains('TAG가', na=False, case=False)].copy()
                
                # 각 구분별로 채널 컬럼을 행으로 변환
                grouped = pd.DataFrame()
                
                if not revenue_df.empty:
                    # 채널 컬럼을 행으로 변환
                    revenue_melted = revenue_df.melt(
                        id_vars=['브랜드', '구분'],
                        value_vars=channel_cols,
                        var_name='채널명',
                        value_name='실판매액'
                    )
                    # 숫자형 변환
                    revenue_melted['실판매액'] = pd.to_numeric(revenue_melted['실판매액'], errors='coerce').fillna(0)
                    # 채널명 기준 집계
                    revenue_grouped = revenue_melted.groupby('채널명')['실판매액'].sum().reset_index()
                    grouped = revenue_grouped
                    print(f"  ✓ 계획 매출 데이터: {len(revenue_grouped)}개 채널")
                
                if not profit_df.empty:
                    profit_melted = profit_df.melt(
                        id_vars=['브랜드', '구분'],
                        value_vars=channel_cols,
                        var_name='채널명',
                        value_name='직접이익'
                    )
                    profit_melted['직접이익'] = pd.to_numeric(profit_melted['직접이익'], errors='coerce').fillna(0)
                    profit_grouped = profit_melted.groupby('채널명')['직접이익'].sum().reset_index()
                    if grouped.empty:
                        grouped = profit_grouped
                    else:
                        grouped = grouped.merge(profit_grouped, on='채널명', how='outer')
                    print(f"  ✓ 계획 직접이익 데이터: {len(profit_grouped)}개 채널")
                
                if not tag_df.empty:
                    tag_melted = tag_df.melt(
                        id_vars=['브랜드', '구분'],
                        value_vars=channel_cols,
                        var_name='채널명',
                        value_name='TAG가'
                    )
                    tag_melted['TAG가'] = pd.to_numeric(tag_melted['TAG가'], errors='coerce').fillna(0)
                    tag_grouped = tag_melted.groupby('채널명')['TAG가'].sum().reset_index()
                    if grouped.empty:
                        grouped = tag_grouped
                    else:
                        grouped = grouped.merge(tag_grouped, on='채널명', how='outer')
                
                # 누락된 컬럼은 0으로 채우기
                for col in ['TAG가', '실판매액', '직접이익']:
                    if col not in grouped.columns:
                        grouped[col] = 0.0
                
                if grouped.empty:
                    print(f"⚠️ 계획 데이터 집계 결과가 비어있습니다")
                    return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
                
                print(f"  ✓ 계획 데이터 집계 완료: {len(grouped)}개 채널")
        else:
            # 일반 데이터 처리 (기존 로직)
            # 채널별 집계
            agg_dict = {
                'TAG가': 'sum',
                '실판매액': 'sum',
                '직접이익': 'sum'
            }
            
            # 존재하는 컬럼만 집계
            available_cols = {k: v for k, v in agg_dict.items() if k in df.columns}
            
            if not available_cols:
                return pd.DataFrame(columns=['채널명', 'TAG가', '실판매액', '직접이익', '할인율', '직접이익율'])
            
            grouped = df.groupby('채널명').agg(available_cols).reset_index()
        
        # 할인율 계산: (TAG가 - 실판매액) / TAG가 * 100
        if 'TAG가' in grouped.columns and '실판매액' in grouped.columns:
            grouped['할인율'] = grouped.apply(
                lambda row: self.calculate_discount_rate(row['TAG가'], row['실판매액']), 
                axis=1
            )
        else:
            grouped['할인율'] = 0.0
        
        # 직접이익율 계산: 직접이익 / 실판매출 * 1.1 * 100
        if '직접이익' in grouped.columns and '실판매액' in grouped.columns:
            grouped['직접이익율'] = grouped.apply(
                lambda row: self.calculate_profit_rate(row['직접이익'], row['실판매액']), 
                axis=1
            )
        else:
            grouped['직접이익율'] = 0.0
        
        return grouped
    
    def process_channel_data(self, brand: str = None, metric: str = '매출') -> pd.DataFrame:
        """
        채널별 손익 데이터 처리
        
        Args:
            brand: 브랜드 코드 (None이면 전체)
            metric: '매출' 또는 '직접이익'
            
        Returns:
            채널별 손익 DataFrame
        """
        # 데이터 로드
        if self.current_year_data is None:
            self.load_current_year_data()
        if self.previous_year_data is None:
            self.load_previous_year_data()
        if self.plan_data is None:
            self.load_plan_data()
        
        # 채널별 집계
        current_agg = self.aggregate_by_channel(self.current_year_data, brand, is_plan_data=False)
        previous_agg = self.aggregate_by_channel(self.previous_year_data, brand, is_plan_data=False)
        
        # ★★★ 계획 데이터: 매출은 채널별로, 직접이익은 채널별 직접이익 컬럼 사용 ★★★
        if metric == '매출':
            # 매출 모드: 채널별 계획 데이터 사용
            plan_agg = self.aggregate_by_channel(self.plan_data, brand, is_plan_data=True)
        else:
            # 직접이익 모드: 계획 데이터에서 채널별 직접이익 컬럼 사용
            plan_agg = self.aggregate_by_channel(self.plan_data, brand, is_plan_data=True)
            # 계획 데이터에 직접이익 컬럼이 없으면 실판매액으로 대체 (폴백)
            if '직접이익' not in plan_agg.columns and '실판매액' in plan_agg.columns:
                plan_agg['직접이익'] = plan_agg['실판매액']
                print(f"  ⚠️ 계획 데이터에 직접이익 컬럼이 없어 실판매액을 사용합니다")
            elif '직접이익' not in plan_agg.columns:
                plan_agg['직접이익'] = 0.0
                print(f"  ⚠️ 계획 데이터에 직접이익 컬럼이 없습니다")
            
            # 직접이익율 계산: 계획 데이터에 직접이익율 컬럼이 있으면 사용, 없으면 계산
            if '직접이익율' not in plan_agg.columns:
                # 직접이익율: 전년 데이터의 직접이익율을 사용 (계획 데이터에 직접이익율이 없으므로)
                # 전년 데이터에서 채널별 직접이익율 가져오기
                if previous_agg is not None and not previous_agg.empty:
                    for idx, row in plan_agg.iterrows():
                        channel = row['채널명']
                        prev_channel_row = previous_agg[previous_agg['채널명'] == channel]
                        if not prev_channel_row.empty and '직접이익율' in prev_channel_row.columns:
                            plan_agg.at[idx, '직접이익율'] = prev_channel_row['직접이익율'].values[0]
                        else:
                            # 전년 데이터가 없으면 기본값 계산
                            if '실판매액' in plan_agg.columns:
                                plan_agg.at[idx, '직접이익율'] = self.calculate_profit_rate(row['직접이익'], row['실판매액'])
                            else:
                                plan_agg.at[idx, '직접이익율'] = 0.0
                else:
                    # 전년 데이터가 없으면 기본값 계산
                    if '실판매액' in plan_agg.columns:
                        plan_agg['직접이익율'] = plan_agg.apply(
                            lambda row: self.calculate_profit_rate(row['직접이익'], row['실판매액']), 
                            axis=1
                        )
                    else:
                        plan_agg['직접이익율'] = 0.0
        
        # 결과 DataFrame 생성
        result_data = []
        
        # 값 컬럼 및 비율 컬럼 선택
        if metric == '매출':
            value_col = '실판매액'
            rate_col = '할인율'  # 매출 모드: 할인율
        else:
            value_col = '직접이익'
            rate_col = '직접이익율'  # 직접이익 모드: 직접이익율
        
        for channel in self.CHANNEL_ORDER:
            row = {'채널': channel}
            
            # 전년 데이터
            # previous_agg가 비어있거나 '채널명' 컬럼이 없으면 빈 결과 반환
            if previous_agg.empty or '채널명' not in previous_agg.columns:
                prev_row = pd.DataFrame()
            else:
                prev_row = previous_agg[previous_agg['채널명'] == channel]
            if not prev_row.empty:
                prev_value = prev_row[value_col].values[0] if value_col in prev_row.columns else 0
                prev_rate = prev_row[rate_col].values[0] if rate_col in prev_row.columns else 0
                row['전년_매출'] = self.to_억원(prev_value)
                row['전년_할인율'] = round(prev_rate, 1)
            else:
                row['전년_매출'] = 0.0
                row['전년_할인율'] = 0.0
            
            # 계획 데이터
            # plan_agg가 비어있거나 '채널명' 컬럼이 없으면 빈 결과 반환
            if plan_agg.empty or '채널명' not in plan_agg.columns:
                plan_row = pd.DataFrame()
            else:
                plan_row = plan_agg[plan_agg['채널명'] == channel]
            if not plan_row.empty:
                plan_value = plan_row[value_col].values[0] if value_col in plan_row.columns else 0
                plan_rate = plan_row[rate_col].values[0] if rate_col in plan_row.columns else 0
                row['계획_매출'] = self.to_억원(plan_value)
                row['계획_할인율'] = round(plan_rate, 1)
            else:
                row['계획_매출'] = 0.0
                row['계획_할인율'] = 0.0
            
            # 당년 데이터
            # current_agg가 비어있거나 '채널명' 컬럼이 없으면 빈 결과 반환
            if current_agg.empty or '채널명' not in current_agg.columns:
                curr_row = pd.DataFrame()
            else:
                curr_row = current_agg[current_agg['채널명'] == channel]
            if not curr_row.empty:
                curr_value = curr_row[value_col].values[0] if value_col in curr_row.columns else 0
                curr_rate = curr_row[rate_col].values[0] if rate_col in curr_row.columns else 0
                row['당년_매출'] = self.to_억원(curr_value)
                row['당년_할인율'] = round(curr_rate, 1)
            else:
                row['당년_매출'] = 0.0
                row['당년_할인율'] = 0.0
            
            # 전년대비 (%) - 정수로 표시
            if row['전년_매출'] > 0:
                row['전년대비'] = int(round((row['당년_매출'] / row['전년_매출']) * 100))
            elif row['전년_매출'] < 0 and row['당년_매출'] != 0:
                # 전년이 음수인 경우 부호 고려
                row['전년대비'] = int(round((row['당년_매출'] / row['전년_매출']) * 100))
            else:
                row['전년대비'] = 0
            
            # 계획대비 (%) - 정수로 표시
            if row['계획_매출'] > 0:
                row['계획대비'] = int(round((row['당년_매출'] / row['계획_매출']) * 100))
            elif row['계획_매출'] < 0 and row['당년_매출'] != 0:
                # 계획이 음수인 경우 부호 고려
                row['계획대비'] = int(round((row['당년_매출'] / row['계획_매출']) * 100))
            else:
                row['계획대비'] = 0
            
            result_data.append(row)
        
        result_df = pd.DataFrame(result_data)
        
        # 값이 없는 채널 제거 (전년, 계획, 당년 모두 0인 경우)
        result_df = result_df[
            (result_df['전년_매출'] != 0) | 
            (result_df['계획_매출'] != 0) | 
            (result_df['당년_매출'] != 0)
        ]
        
        return result_df
    
    def get_available_brands(self) -> list:
        """사용 가능한 브랜드 목록 반환"""
        brands = set()
        
        if self.current_year_data is not None:
            brands.update(self.current_year_data['브랜드'].unique())
        if self.previous_year_data is not None:
            brands.update(self.previous_year_data['브랜드'].unique())
        if self.plan_data is not None:
            brands.update(self.plan_data['브랜드'].unique())
        
        return sorted(list(brands))
    
    def export_to_excel(self, output_path: str = None, brand: str = None):
        """엑셀 파일로 내보내기"""
        if output_path is None:
            output_dir = self.project_root / 'output'
            output_dir.mkdir(exist_ok=True)
            brand_suffix = f"_{brand}" if brand else "_전체"
            output_path = output_dir / f'채널별_손익데이터_{self.target_month}{brand_suffix}.xlsx'
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 매출 데이터
            sales_df = self.process_channel_data(brand=brand, metric='매출')
            sales_df.to_excel(writer, sheet_name='매출', index=False)
            
            # 직접이익 데이터
            profit_df = self.process_channel_data(brand=brand, metric='직접이익')
            profit_df.to_excel(writer, sheet_name='직접이익', index=False)
        
        print(f"✅ 엑셀 파일 저장 완료: {output_path}")
        return output_path
    
    def export_to_json(self, output_path: str = None, brand: str = None, include_all_brands: bool = True) -> dict:
        """
        JSON 형식으로 내보내기
        
        Args:
            output_path: 출력 파일 경로
            brand: 특정 브랜드만 출력 (None이면 전체)
            include_all_brands: True면 모든 브랜드별 데이터 포함
        """
        # 브랜드 코드 -> 대시보드 브랜드명 매핑
        brand_name_map = {
            'M': 'MLB',
            'I': 'MLB_KIDS',
            'X': 'DISCOVERY',
            'V': 'DUVETICA',
            'ST': 'SERGIO',
            'W': 'SUPRA'
        }
        
        result = {
            'metadata': {
                'base_date': self.base_date,
                'target_month': self.target_month,
                'created_at': datetime.now().isoformat()
            },
            'channel_order': self.CHANNEL_ORDER
        }
        
        if include_all_brands:
            # ★ 브랜드별 데이터 구조 (Dashboard.html에서 바로 사용 가능) ★
            channel_revenue_data = {}
            channel_profit_data = {}
            brand_revenue_totals = {}
            brand_profit_totals = {}
            
            for brand_code in ['M', 'I', 'X', 'V', 'ST', 'W']:
                brand_name = brand_name_map.get(brand_code, brand_code)
                
                # 매출 데이터
                revenue_df = self.process_channel_data(brand=brand_code, metric='매출')
                revenue_channels = []
                for _, row in revenue_df.iterrows():
                    revenue_channels.append({
                        'channel': row['채널'],
                        'prev': row['전년_매출'],
                        'target': row['계획_매출'],
                        'forecast': row['당년_매출'],
                        'prevRate': row['전년_할인율'],
                        'targetRate': row['계획_할인율'],
                        'forecastRate': row['당년_할인율'],
                        'yoy': row['전년대비'],
                        'achievement': row['계획대비']
                    })
                channel_revenue_data[brand_name] = revenue_channels
                
                # ★★★ 매출 합계: 계획은 내수합계에서 가져오기 ★★★
                prev_revenue = round(revenue_df['전년_매출'].sum(), 1)
                forecast_revenue = round(revenue_df['당년_매출'].sum(), 1)
                
                # 계획 매출: 내수합계에서 가져오기
                target_revenue = 0.0
                if self.plan_data is not None:
                    brand_plan_df = self.plan_data[self.plan_data['브랜드'] == brand_code]
                    내수합계_df = brand_plan_df[brand_plan_df['채널명'] == '내수합계']
                    if not 내수합계_df.empty and '실판매액' in 내수합계_df.columns:
                        target_revenue = round(self.to_억원(내수합계_df['실판매액'].sum()), 1)
                
                brand_revenue_totals[brand_name] = {
                    'prev': prev_revenue,
                    'target': target_revenue,
                    'forecast': forecast_revenue
                }
                
                # 직접이익 데이터
                profit_df = self.process_channel_data(brand=brand_code, metric='직접이익')
                profit_channels = []
                for _, row in profit_df.iterrows():
                    profit_channels.append({
                        'channel': row['채널'],
                        'prev': row['전년_매출'],  # 직접이익
                        'target': row['계획_매출'],
                        'forecast': row['당년_매출'],
                        'prevRate': row['전년_할인율'],  # 직접이익율
                        'targetRate': row['계획_할인율'],
                        'forecastRate': row['당년_할인율'],
                        'yoy': row['전년대비'],
                        'achievement': row['계획대비']
                    })
                channel_profit_data[brand_name] = profit_channels
                
                # ★★★ 직접이익 합계 계산 로직 ★★★
                # 전년: 공통 채널 포함 전체 채널 직접이익 합계 (원본 데이터에서 직접 합산)
                prev_direct_profit = 0.0
                if self.previous_year_data is not None:
                    brand_prev_df = self.previous_year_data[self.previous_year_data['브랜드'] == brand_code]
                    if '직접이익' in brand_prev_df.columns:
                        # 공통 채널 포함하여 전체 합산
                        prev_direct_profit = brand_prev_df['직접이익'].sum()
                
                # 당년: 전체 채널 포함 전체 채널 직접이익 합계 (원본 데이터에서 직접 합산)
                forecast_direct_profit = 0.0
                if self.current_year_data is not None:
                    brand_forecast_df = self.current_year_data[self.current_year_data['브랜드'] == brand_code]
                    if '직접이익' in brand_forecast_df.columns:
                        # 전체 채널 포함하여 전체 합산
                        forecast_direct_profit = brand_forecast_df['직접이익'].sum()
                
                # 계획: 내수합계의 직접이익 합계 (내수합계 행에서 직접 가져오기)
                target_direct_profit = 0.0
                if self.plan_data is not None:
                    brand_plan_df = self.plan_data[self.plan_data['브랜드'] == brand_code]
                    내수합계_df = brand_plan_df[brand_plan_df['채널명'] == '내수합계']
                    if not 내수합계_df.empty:
                        # 직접이익 컬럼이 있으면 사용
                        if '직접이익' in 내수합계_df.columns:
                            target_direct_profit = 내수합계_df['직접이익'].sum()
                
                brand_profit_totals[brand_name] = {
                    'prev': round(self.to_억원(prev_direct_profit), 1),
                    'target': round(self.to_억원(target_direct_profit), 1),
                    'forecast': round(self.to_억원(forecast_direct_profit), 1)
                }
            
            # 전체 합산 데이터도 추가
            total_revenue_df = self.process_channel_data(brand=None, metric='매출')
            total_profit_df = self.process_channel_data(brand=None, metric='직접이익')
            
            result['channelRevenueData'] = channel_revenue_data
            result['channelProfitData'] = channel_profit_data
            result['brandRevenueTotals'] = brand_revenue_totals
            result['brandProfitTotals'] = brand_profit_totals
            
            # 전체 합산 (기존 호환성 유지)
            result['매출'] = total_revenue_df.to_dict('records')
            result['직접이익'] = total_profit_df.to_dict('records')
            
            print(f"✅ 브랜드별 데이터 생성 완료: {list(brand_name_map.values())}")
        else:
            # 단일 브랜드 또는 전체만
            result['metadata']['brand'] = brand if brand else '전체'
            result['매출'] = self.process_channel_data(brand=brand, metric='매출').to_dict('records')
            result['직접이익'] = self.process_channel_data(brand=brand, metric='직접이익').to_dict('records')
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"✅ JSON 파일 저장 완료: {output_path}")
        
        return result
    
    def export_to_dashboard_js(self, output_path: str = None) -> str:
        """
        대시보드용 JavaScript 파일로 내보내기
        Dashboard.html의 channelRevenueData, channelProfitData 형식에 맞춤
        """
        # 브랜드 코드 -> 대시보드 브랜드명 매핑
        brand_name_map = {
            'M': 'MLB',
            'I': 'MLB_KIDS',
            'X': 'DISCOVERY',
            'V': 'DUVETICA',
            'ST': 'SERGIO',
            'W': 'SUPRA'
        }
        
        channel_revenue_data = {}
        channel_profit_data = {}
        brand_revenue_totals = {}
        brand_profit_totals = {}
        
        # 할인율 데이터도 추가
        channel_discount_data = {}
        
        # 직접이익율 데이터도 추가
        channel_profit_rate_data = {}
        
        for brand_code in ['M', 'I', 'X', 'V', 'ST', 'W']:
            brand_name = brand_name_map.get(brand_code, brand_code)
            
            # 매출 데이터 (할인율 포함)
            revenue_df = self.process_channel_data(brand=brand_code, metric='매출')
            revenue_channels = []
            discount_channels = []
            
            for _, row in revenue_df.iterrows():
                revenue_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_매출'],
                    'target': row['계획_매출'],
                    'forecast': row['당년_매출'],
                    'prevRate': row['전년_할인율'],  # 할인율
                    'targetRate': row['계획_할인율'],
                    'forecastRate': row['당년_할인율'],
                    'yoy': row['전년대비'],
                    'achievement': row['계획대비']
                })
                discount_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_할인율'],
                    'target': row['계획_할인율'],
                    'forecast': row['당년_할인율']
                })
            
            channel_revenue_data[brand_name] = revenue_channels
            channel_discount_data[brand_name] = discount_channels
            
            # 매출 합계
            brand_revenue_totals[brand_name] = {
                'prev': round(revenue_df['전년_매출'].sum(), 1),
                'target': round(revenue_df['계획_매출'].sum(), 1),
                'forecast': round(revenue_df['당년_매출'].sum(), 1)
            }
            
            # 직접이익 데이터 (직접이익율 포함)
            profit_df = self.process_channel_data(brand=brand_code, metric='직접이익')
            profit_channels = []
            profit_rate_channels = []
            
            for _, row in profit_df.iterrows():
                profit_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_매출'],  # 직접이익 모드에서는 이 값이 직접이익
                    'target': row['계획_매출'],
                    'forecast': row['당년_매출'],
                    'prevRate': row['전년_할인율'],  # 직접이익율
                    'targetRate': row['계획_할인율'],
                    'forecastRate': row['당년_할인율'],
                    'yoy': row['전년대비'],
                    'achievement': row['계획대비']
                })
                profit_rate_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_할인율'],  # 직접이익율
                    'target': row['계획_할인율'],
                    'forecast': row['당년_할인율']
                })
            
            channel_profit_data[brand_name] = profit_channels
            channel_profit_rate_data[brand_name] = profit_rate_channels
            
            # 직접이익 합계
            brand_profit_totals[brand_name] = {
                'prev': round(profit_df['전년_매출'].sum(), 1),
                'target': round(profit_df['계획_매출'].sum(), 1),
                'forecast': round(profit_df['당년_매출'].sum(), 1)
            }
        
        # JavaScript 코드 생성
        js_content = f"""// 채널별 손익 데이터 (자동 생성)
// 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 기준일: {self.base_date}, 대상월: {self.target_month}

// 채널별 매출 데이터 (단위: 억원, prevRate/targetRate/forecastRate: 할인율 %)
var channelRevenueDataFromFile = {json.dumps(channel_revenue_data, ensure_ascii=False, indent=2)};

// 채널별 직접이익 데이터 (단위: 억원, prevRate/targetRate/forecastRate: 직접이익율 %)
var channelProfitDataFromFile = {json.dumps(channel_profit_data, ensure_ascii=False, indent=2)};

// 채널별 할인율 데이터 (단위: %)
var channelDiscountDataFromFile = {json.dumps(channel_discount_data, ensure_ascii=False, indent=2)};

// 채널별 직접이익율 데이터 (단위: %)
var channelProfitRateDataFromFile = {json.dumps(channel_profit_rate_data, ensure_ascii=False, indent=2)};

// 브랜드별 매출 합계 (단위: 억원)
var brandRevenueTotalsFromFile = {json.dumps(brand_revenue_totals, ensure_ascii=False, indent=2)};

// 브랜드별 직접이익 합계 (단위: 억원)
var brandProfitTotalsFromFile = {json.dumps(brand_profit_totals, ensure_ascii=False, indent=2)};

// 전역 객체에 할당
if (typeof window !== 'undefined') {{
  window.channelRevenueDataFromFile = channelRevenueDataFromFile;
  window.channelProfitDataFromFile = channelProfitDataFromFile;
  window.channelDiscountDataFromFile = channelDiscountDataFromFile;
  window.channelProfitRateDataFromFile = channelProfitRateDataFromFile;
  window.brandRevenueTotalsFromFile = brandRevenueTotalsFromFile;
  window.brandProfitTotalsFromFile = brandProfitTotalsFromFile;
}}
"""
        
        # 파일 저장
        if output_path is None:
            output_path = self.project_root / 'public' / f'channel_profit_loss_{self.base_date}.js'
        else:
            output_path = Path(output_path)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(js_content)
        
        print(f"✅ 대시보드용 JS 파일 저장 완료: {output_path}")
        return str(output_path)
    
    def append_to_main_data_js(self, main_js_path: str = None):
        """
        기존 data_YYYYMMDD.js 파일에 채널별 손익 데이터 추가
        """
        if main_js_path is None:
            main_js_path = self.project_root / 'public' / f'data_{self.base_date}.js'
        else:
            main_js_path = Path(main_js_path)
        
        if not main_js_path.exists():
            print(f"⚠️ 메인 데이터 파일을 찾을 수 없습니다: {main_js_path}")
            return None
        
        # 브랜드 코드 -> 대시보드 브랜드명 매핑
        brand_name_map = {
            'M': 'MLB',
            'I': 'MLB_KIDS',
            'X': 'DISCOVERY',
            'V': 'DUVETICA',
            'ST': 'SERGIO',
            'W': 'SUPRA'
        }
        
        channel_revenue_data = {}
        channel_profit_data = {}
        brand_revenue_totals = {}
        brand_profit_totals = {}
        channel_discount_data = {}
        channel_profit_rate_data = {}
        
        for brand_code in ['M', 'I', 'X', 'V', 'ST', 'W']:
            brand_name = brand_name_map.get(brand_code, brand_code)
            
            # 매출 데이터 (할인율 포함)
            revenue_df = self.process_channel_data(brand=brand_code, metric='매출')
            revenue_channels = []
            discount_channels = []
            
            for _, row in revenue_df.iterrows():
                revenue_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_매출'],
                    'target': row['계획_매출'],
                    'forecast': row['당년_매출'],
                    'prevRate': row['전년_할인율'],  # 할인율
                    'targetRate': row['계획_할인율'],
                    'forecastRate': row['당년_할인율'],
                    'yoy': row['전년대비'],
                    'achievement': row['계획대비']
                })
                discount_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_할인율'],
                    'target': row['계획_할인율'],
                    'forecast': row['당년_할인율']
                })
            
            channel_revenue_data[brand_name] = revenue_channels
            channel_discount_data[brand_name] = discount_channels
            
            brand_revenue_totals[brand_name] = {
                'prev': round(revenue_df['전년_매출'].sum(), 1),
                'target': round(revenue_df['계획_매출'].sum(), 1),
                'forecast': round(revenue_df['당년_매출'].sum(), 1)
            }
            
            # 직접이익 데이터 (직접이익율 포함)
            profit_df = self.process_channel_data(brand=brand_code, metric='직접이익')
            profit_channels = []
            profit_rate_channels = []
            
            for _, row in profit_df.iterrows():
                profit_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_매출'],
                    'target': row['계획_매출'],
                    'forecast': row['당년_매출'],
                    'prevRate': row['전년_할인율'],  # 직접이익율
                    'targetRate': row['계획_할인율'],
                    'forecastRate': row['당년_할인율'],
                    'yoy': row['전년대비'],
                    'achievement': row['계획대비']
                })
                profit_rate_channels.append({
                    'channel': row['채널'],
                    'prev': row['전년_할인율'],  # 직접이익율
                    'target': row['계획_할인율'],
                    'forecast': row['당년_할인율']
                })
            
            channel_profit_data[brand_name] = profit_channels
            channel_profit_rate_data[brand_name] = profit_rate_channels
            
            brand_profit_totals[brand_name] = {
                'prev': round(profit_df['전년_매출'].sum(), 1),
                'target': round(profit_df['계획_매출'].sum(), 1),
                'forecast': round(profit_df['당년_매출'].sum(), 1)
            }
        
        # 기존 파일 읽기
        with open(main_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 이미 채널별 손익 데이터가 있는지 확인
        if 'channelProfitLossData' in content:
            print(f"ℹ️ 채널별 손익 데이터가 이미 존재합니다. 업데이트합니다.")
            # 기존 데이터 제거 (정규식으로 처리)
            import re
            content = re.sub(
                r'// === 채널별 손익 데이터 \(자동 생성\) ===.*?// === 채널별 손익 데이터 끝 ===\n?',
                '',
                content,
                flags=re.DOTALL
            )
        
        # 새 데이터 추가
        new_data = f"""
// === 채널별 손익 데이터 (자동 생성) ===
// 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 기준일: {self.base_date}, 대상월: {self.target_month}
// 매출: prevRate/targetRate/forecastRate = 할인율(%)
// 직접이익: prevRate/targetRate/forecastRate = 직접이익율(%)

var channelProfitLossData = {{
  channelRevenueData: {json.dumps(channel_revenue_data, ensure_ascii=False, indent=2)},
  channelProfitData: {json.dumps(channel_profit_data, ensure_ascii=False, indent=2)},
  channelDiscountData: {json.dumps(channel_discount_data, ensure_ascii=False, indent=2)},
  channelProfitRateData: {json.dumps(channel_profit_rate_data, ensure_ascii=False, indent=2)},
  brandRevenueTotals: {json.dumps(brand_revenue_totals, ensure_ascii=False, indent=2)},
  brandProfitTotals: {json.dumps(brand_profit_totals, ensure_ascii=False, indent=2)}
}};

if (typeof window !== 'undefined') {{
  window.channelProfitLossData = channelProfitLossData;
}}
// === 채널별 손익 데이터 끝 ===
"""
        
        # 파일 끝에 추가
        content = content.rstrip() + '\n' + new_data
        
        with open(main_js_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ 메인 데이터 파일에 채널별 손익 데이터 추가 완료: {main_js_path}")
        return str(main_js_path)

    def print_summary(self, brand: str = None, metric: str = '매출'):
        """요약 출력"""
        df = self.process_channel_data(brand=brand, metric=metric)
        
        brand_label = brand if brand else '전체'
        print(f"\n{'='*80}")
        print(f"📊 채널별 손익 현황 - {self.target_month} ({brand_label} 브랜드) - {metric}")
        print(f"{'='*80}")
        
        # 메트릭에 따라 레이블 변경
        if metric == '매출':
            value_label = '매출'
            rate_label = '할인율'
            yoy_label = '매출 YOY'
        else:
            value_label = '직접이익'
            rate_label = '이익율'
            yoy_label = '직접이익 YOY'
        
        # 테이블 헤더
        header = f"{'채널':<12} | {'전년':^15} | {'계획':^15} | {'당년':^15} | {yoy_label:^17}"
        subheader = f"{'':<12} | {value_label:^7} {rate_label:^7} | {value_label:^7} {rate_label:^7} | {value_label:^7} {rate_label:^7} | {'전년대비':^8} {'계획대비':^8}"
        print(header)
        print(subheader)
        print('-' * 80)
        
        for _, row in df.iterrows():
            line = f"{row['채널']:<12} | {row['전년_매출']:>7.1f} {row['전년_할인율']:>6.1f}% | {row['계획_매출']:>7.1f} {row['계획_할인율']:>6.1f}% | {row['당년_매출']:>7.1f} {row['당년_할인율']:>6.1f}% | {row['전년대비']:>7}% {row['계획대비']:>8}%"
            print(line)
        
        print('-' * 80)
        
        # 합계 계산
        total_prev = df['전년_매출'].sum()
        total_plan = df['계획_매출'].sum()
        total_curr = df['당년_매출'].sum()
        
        total_prev_ratio = int(round((total_curr / total_prev) * 100)) if total_prev != 0 else 0
        total_plan_ratio = int(round((total_curr / total_plan) * 100)) if total_plan != 0 else 0
        
        print(f"{'합계':<12} | {total_prev:>7.1f} {'-':>7} | {total_plan:>7.1f} {'-':>7} | {total_curr:>7.1f} {'-':>7} | {total_prev_ratio:>7}% {total_plan_ratio:>8}%")
        print(f"{'='*80}\n")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='채널별 손익데이터 처리')
    parser.add_argument('--base-date', '-d', default='20251124', help='기준 날짜 (YYYYMMDD)')
    parser.add_argument('--target-month', '-m', default='202511', help='대상 월 (YYYYMM)')
    parser.add_argument('--brand', '-b', help='브랜드 코드 (예: I, M, ST, V, W, X)')
    parser.add_argument('--metric', default='매출', choices=['매출', '직접이익'], help='지표 선택')
    parser.add_argument('--output', '-o', help='출력 파일 경로')
    parser.add_argument('--format', '-f', default='print', choices=['print', 'excel', 'json', 'js', 'dashboard'], help='출력 형식')
    
    args = parser.parse_args()
    
    # 프로세서 생성
    processor = ChannelProfitLossProcessor(
        base_date=args.base_date,
        target_month=args.target_month
    )
    
    # 데이터 로드
    processor.load_current_year_data()
    processor.load_previous_year_data()
    processor.load_plan_data()
    
    # 출력 형식에 따라 처리
    if args.format == 'print':
        processor.print_summary(brand=args.brand, metric=args.metric)
        
        # 사용 가능한 브랜드 출력
        brands = processor.get_available_brands()
        print(f"📋 사용 가능한 브랜드: {', '.join(brands)}")
        
    elif args.format == 'excel':
        processor.export_to_excel(output_path=args.output, brand=args.brand)
        
    elif args.format == 'json':
        result = processor.export_to_json(output_path=args.output, brand=args.brand)
        if not args.output:
            print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.format == 'js':
        # 별도 JS 파일로 내보내기
        processor.export_to_dashboard_js(output_path=args.output)
        
    elif args.format == 'dashboard':
        # 메인 데이터 JS 파일에 통합 (실패해도 계속 진행)
        try:
            processor.append_to_main_data_js()
            print(f"\n💡 Dashboard.html에서 데이터를 사용하려면 window.channelProfitLossData를 참조하세요.")
        except Exception as e:
            print(f"⚠️ 메인 데이터 JS 파일 통합 실패 (계속 진행): {e}")

        # ★★★ JSON 파일로도 저장 (브랜드별 데이터 포함) - 항상 실행 ★★★
        try:
            json_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "public", "data", args.base_date)
            os.makedirs(json_dir, exist_ok=True)
            json_path = os.path.join(json_dir, "channel_profit_loss.json")
            
            # include_all_brands=True로 브랜드별 데이터 포함
            result = processor.export_to_json(output_path=json_path, include_all_brands=True)
            if result:
                print(f"  ✅ JSON 저장 완료 (브랜드별 데이터 포함): {json_path}")
            else:
                print(f"  ⚠️ JSON 저장 실패: result가 None입니다")
                sys.exit(1)  # JSON 저장 실패 시 오류 코드 반환
        except Exception as e:
            print(f"  ❌ JSON 저장 중 에러 발생: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)  # 예외 발생 시 오류 코드 반환


if __name__ == "__main__":
    main()

