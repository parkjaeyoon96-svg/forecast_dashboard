"""
당년 데이터 처리 전체 파이프라인 실행 스크립트
"""
import os
import sys
from pathlib import Path
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    # 인자 확인
    analysis_month = None
    update_date = None
    
    if len(sys.argv) >= 3:
        # 분석월과 업데이트일자가 모두 지정된 경우
        analysis_month = sys.argv[1]  # YYYYMM
        update_date = sys.argv[2]     # YYYYMMDD
        print("=" * 80)
        print("  Current Year Data Processing (Full Pipeline)")
        print("  날짜 지정 모드")
        print("=" * 80)
        print(f"  분석월: {analysis_month}")
        print(f"  업데이트일자: {update_date}")
        print()
    else:
        # 최신 파일 자동 선택 모드
        print("=" * 80)
        print("  Current Year Data Processing (Full Pipeline)")
        print("  최신 파일 자동 선택 모드")
        print("=" * 80)
        print()
    
    # Step 1: KE30 Full Pipeline
    print("[Step 1/3] Running KE30 full pipeline (전처리 + 직접비 계산)...")
    from scripts.process_ke30_full_pipeline import main as run_ke30_pipeline
    try:
        if analysis_month and update_date:
            # 날짜가 지정된 경우
            run_ke30_pipeline(analysis_month=analysis_month, update_date=update_date)
        else:
            # 최신 파일 자동 선택
            run_ke30_pipeline()
    except Exception as e:
        print(f"[ERROR] KE30 full pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    print()
    
    # Step 2: Find date folder from metadata
    if update_date:
        # 날짜가 지정된 경우 해당 날짜 사용
        date_folder = update_date
        print(f"[Step 2/3] Using specified date folder: {date_folder}")
    else:
        # 기존 로직: 최신 파일 찾기
        print("[Step 2/3] Finding date folder from metadata...")
        raw_dir = project_root / "raw"
        date_folder = None
        metadata_path_found = None
        
        # Find latest date folder with metadata.json
        if raw_dir.exists():
            for year_month_dir in sorted(raw_dir.iterdir(), reverse=True):
                if year_month_dir.is_dir() and year_month_dir.name.isdigit() and len(year_month_dir.name) == 6:
                    current_year_dir = year_month_dir / "current_year"
                    if current_year_dir.exists():
                        for date_dir in sorted(current_year_dir.iterdir(), reverse=True):
                            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                                metadata_path = date_dir / "metadata.json"
                                if metadata_path.exists():
                                    date_folder = date_dir.name
                                    metadata_path_found = metadata_path
                                    break
                if date_folder:
                    break
        
        if not date_folder:
            print("[WARNING] Date folder with metadata.json not found.")
            print("Preprocessing completed.")
            return 0
    
    print(f"Date folder found: {date_folder}")
    
    # metadata.json에서 분석월 읽기 (날짜 지정 모드일 때도)
    if not analysis_month:
        metadata_path_to_read = metadata_path_found if 'metadata_path_found' in locals() else None
        if not metadata_path_to_read:
            # 날짜 지정 모드: 해당 날짜의 metadata.json 찾기
            raw_dir = project_root / "raw"
            if raw_dir.exists():
                for year_month_dir in raw_dir.iterdir():
                    if year_month_dir.is_dir() and year_month_dir.name.isdigit() and len(year_month_dir.name) == 6:
                        current_year_dir = year_month_dir / "current_year" / date_folder
                        metadata_path_to_read = current_year_dir / "metadata.json"
                        if metadata_path_to_read.exists():
                            break
        
        if metadata_path_to_read and metadata_path_to_read.exists():
            try:
                with open(metadata_path_to_read, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    analysis_month = metadata.get('analysis_month')
                    if analysis_month:
                        print(f"Analysis month from metadata.json: {analysis_month}")
            except Exception as e:
                print(f"[WARNING] Failed to read analysis_month from metadata.json: {e}")
    
    print()
    
    # Step 3: Convert KE30 to Forecast
    print("[Step 3/3] Converting KE30 to Forecast...")
    try:
        # convert_ke30_to_forecast.py를 직접 실행
        import subprocess
        cmd = [sys.executable, str(project_root / "scripts" / "convert_ke30_to_forecast.py"), date_folder]
        # 분석월이 지정된 경우 함께 전달
        if analysis_month:
            cmd.extend(['--analysis-month', analysis_month])
        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=False
        )
        if result.returncode != 0:
            print(f"[WARNING] KE30 to Forecast conversion failed (exit code: {result.returncode})")
    except Exception as e:
        print(f"[WARNING] KE30 to Forecast conversion failed: {e}")
        import traceback
        traceback.print_exc()
    else:
        print()
        print("=" * 80)
        print("  Complete!")
        print("=" * 80)
        print()
        print("Generated files:")
        print(f"  - raw/*/current_year/{date_folder}/ke30_*_전처리완료.csv")
        print(f"  - raw/*/current_year/{date_folder}/ke30_*_Shop_item.csv")
        print(f"  - raw/*/current_year/{date_folder}/ke30_*_Shop.csv")
        print(f"  - raw/*/current_year/{date_folder}/forecast_*_Shop.csv")
        print(f"  - raw/*/current_year/{date_folder}/forecast_*_Shop_item.csv")
    
    print()
    print("=" * 80)
    print("  Current Year Data Processing Complete")
    print("=" * 80)
    print()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

