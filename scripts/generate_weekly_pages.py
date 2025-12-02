"""
매주 새로운 대시보드 페이지 자동 생성 스크립트
매월 업로드된 CSV를 기반으로 주차별 HTML 페이지 생성
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
import shutil

class WeeklyPageGenerator:
    def __init__(self, base_dir='.'):
        self.base_dir = Path(base_dir)
        self.template_path = self.base_dir / 'Dashboard.html'
        self.pages_dir = self.base_dir / 'pages'
        self.data_dir = self.base_dir / 'data'
        
        # 필요한 폴더 생성
        self.pages_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        (self.data_dir / 'weekly').mkdir(exist_ok=True)
        (self.data_dir / 'monthly').mkdir(exist_ok=True)
    
    def get_current_week_info(self):
        """현재 주차 정보 반환"""
        today = datetime.now()
        year = today.year
        month = today.month
        day = today.day
        week = min((day - 1) // 7 + 1, 5)  # 최대 5주차
        
        return {
            'year': year,
            'month': month,
            'week': week,
            'date': today.strftime('%Y-%m-%d')
        }
    
    def read_template(self):
        """템플릿 HTML 읽기"""
        if not self.template_path.exists():
            raise FileNotFoundError(f"템플릿 파일을 찾을 수 없습니다: {self.template_path}")
        
        with open(self.template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def generate_page(self, year, month, week, data_file=None):
        """특정 주차의 페이지 생성"""
        print(f"\n{'='*60}")
        print(f"📄 페이지 생성: {year}년 {month}월 {week}주차")
        print(f"{'='*60}")
        
        # 템플릿 읽기
        template_html = self.read_template()
        
        # 데이터 파일 경로 (있는 경우)
        if data_file is None:
            data_file = f'data/weekly/{year:04d}{month:02d}_week{week}.json'
        
        # HTML 수정: 데이터 파일 경로 업데이트
        modified_html = template_html
        
        # 데이터 파일 경로를 HTML에 주입 (script 태그 찾기)
        if '<script>' in modified_html or '<script type="text/javascript">' in modified_html:
            # 기존 데이터 로딩 부분을 찾아서 교체
            data_script = f'''
    <script>
        // 주차별 데이터 파일 정보
        const WEEK_INFO = {{
            year: {year},
            month: {month},
            week: {week},
            dataFile: '{data_file}'
        }};
        
        // 페이지 타이틀 업데이트
        document.title = `대시보드 - {year}년 {month}월 {week}주차`;
    </script>
'''
            # head 태그 끝에 추가
            if '</head>' in modified_html:
                modified_html = modified_html.replace('</head>', f'{data_script}</head>')
        
        # 주차 정보를 body에 표시 (optional)
        week_header = f'''
    <div class="week-info" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 20px; text-align: center; font-size: 18px; font-weight: bold; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
        📊 {year}년 {month}월 {week}주차 대시보드
    </div>
'''
        if '<body>' in modified_html:
            modified_html = modified_html.replace('<body>', f'<body>\n{week_header}')
        
        # 출력 파일명
        output_filename = f'{year:04d}{month:02d}_week{week}.html'
        output_path = self.pages_dir / output_filename
        
        # 파일 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(modified_html)
        
        print(f"✅ 생성 완료: {output_path}")
        print(f"📊 접속 URL: /pages/{output_filename}")
        print(f"{'='*60}\n")
        
        return output_path
    
    def generate_index_page(self):
        """전체 주차 목록을 보여주는 인덱스 페이지 생성"""
        print("\n📑 인덱스 페이지 생성 중...")
        
        # pages 폴더의 모든 HTML 파일 찾기
        page_files = sorted(self.pages_dir.glob('*.html'), reverse=True)
        
        html_content = '''<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>주간 대시보드 목록</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 40px 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 50px;
        }
        
        .header h1 {
            font-size: 48px;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header p {
            font-size: 18px;
            opacity: 0.9;
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 25px;
            margin-top: 30px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            cursor: pointer;
            text-decoration: none;
            color: inherit;
            display: block;
        }
        
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.3);
        }
        
        .card-icon {
            font-size: 48px;
            margin-bottom: 15px;
        }
        
        .card-title {
            font-size: 24px;
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }
        
        .card-subtitle {
            font-size: 14px;
            color: #666;
            margin-bottom: 5px;
        }
        
        .card-date {
            font-size: 12px;
            color: #999;
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px solid #eee;
        }
        
        .no-pages {
            text-align: center;
            color: white;
            font-size: 20px;
            padding: 60px;
            background: rgba(255,255,255,0.1);
            border-radius: 15px;
        }
        
        @media (max-width: 768px) {
            .header h1 {
                font-size: 36px;
            }
            
            .grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 주간 대시보드</h1>
            <p>매주 업데이트되는 판매 데이터 대시보드</p>
        </div>
        
        <div class="grid">
'''
        
        if not page_files:
            html_content += '''
            <div class="no-pages">
                <p>아직 생성된 주간 페이지가 없습니다.</p>
                <p style="margin-top: 10px; font-size: 16px;">CSV 데이터를 업로드하고 스크립트를 실행하세요.</p>
            </div>
'''
        else:
            for page_file in page_files:
                # 파일명에서 정보 추출 (예: 202411_week2.html)
                filename = page_file.stem
                try:
                    parts = filename.split('_week')
                    if len(parts) == 2:
                        year_month = parts[0]
                        week = parts[1]
                        year = year_month[:4]
                        month = year_month[4:6]
                        
                        html_content += f'''
            <a href="pages/{page_file.name}" class="card">
                <div class="card-icon">📈</div>
                <div class="card-title">{year}년 {month}월</div>
                <div class="card-subtitle">{week}주차</div>
                <div class="card-date">생성일: {datetime.fromtimestamp(page_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M')}</div>
            </a>
'''
                except:
                    continue
        
        html_content += '''
        </div>
    </div>
</body>
</html>
'''
        
        # index.html 저장
        index_path = self.base_dir / 'index.html'
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ 인덱스 페이지 생성 완료: {index_path}")
        return index_path
    
    def generate_current_week(self):
        """현재 주차 페이지 생성"""
        week_info = self.get_current_week_info()
        self.generate_page(
            week_info['year'],
            week_info['month'],
            week_info['week']
        )
        self.generate_index_page()
    
    def generate_all_weeks_for_month(self, year, month):
        """특정 월의 모든 주차 페이지 생성"""
        print(f"\n{'='*60}")
        print(f"📅 {year}년 {month}월 전체 주차 페이지 생성")
        print(f"{'='*60}\n")
        
        for week in range(1, 6):
            self.generate_page(year, month, week)
        
        self.generate_index_page()
        
        print(f"\n✅ {year}년 {month}월 전체 주차 페이지 생성 완료!\n")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='주간 대시보드 페이지 생성')
    parser.add_argument('--current', action='store_true', help='현재 주차 페이지만 생성')
    parser.add_argument('--year', type=int, help='년도 (전체 월 생성시)')
    parser.add_argument('--month', type=int, help='월 (전체 월 생성시)')
    parser.add_argument('--week', type=int, help='주차 (특정 주차만 생성시)')
    
    args = parser.parse_args()
    
    generator = WeeklyPageGenerator()
    
    if args.current:
        # 현재 주차만 생성
        generator.generate_current_week()
    elif args.year and args.month and args.week:
        # 특정 주차 생성
        generator.generate_page(args.year, args.month, args.week)
        generator.generate_index_page()
    elif args.year and args.month:
        # 특정 월 전체 주차 생성
        generator.generate_all_weeks_for_month(args.year, args.month)
    else:
        # 기본: 현재 주차 생성
        print("옵션이 지정되지 않았습니다. 현재 주차 페이지를 생성합니다.\n")
        generator.generate_current_week()


if __name__ == "__main__":
    main()
















