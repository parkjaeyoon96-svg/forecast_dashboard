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
    print("=" * 80)
    print("  Current Year Data Processing (Full Pipeline)")
    print("=" * 80)
    print()
    
    # Step 1: KE30 Full Pipeline
    print("[Step 1/3] Running KE30 full pipeline (전처리 + 직접비 계산)...")
    from scripts.process_ke30_full_pipeline import main as run_ke30_pipeline
    try:
        run_ke30_pipeline()
    except Exception as e:
        print(f"[ERROR] KE30 full pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    print()
    
    # Step 2: Find date folder from metadata
    print("[Step 2/3] Finding date folder from metadata...")
    raw_dir = project_root / "raw"
    date_folder = None
    
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
                                break
                if date_folder:
                    break
    
    if not date_folder:
        print("[WARNING] Date folder with metadata.json not found.")
        print("Preprocessing completed.")
        return 0
    
    print(f"Date folder found: {date_folder}")
    print()
    
    # Step 3: Convert KE30 to Forecast
    print("[Step 3/3] Converting KE30 to Forecast...")
    try:
        # convert_ke30_to_forecast.py를 직접 실행
        import subprocess
        result = subprocess.run(
            [sys.executable, str(project_root / "scripts" / "convert_ke30_to_forecast.py"), date_folder],
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

