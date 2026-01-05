"""
전년/계획 데이터 업데이트 전체 프로세스 실행 스크립트
"""
import os
import sys
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    print("=" * 80)
    print("  Previous Year / Plan Data Update")
    print("=" * 80)
    print()
    
    # 분석월 추출
    print("[INFO] 분석월 폴더에서 분석월 추출 중...")
    raw_dir = project_root / "raw"
    if not raw_dir.exists():
        print("[ERROR] raw 폴더를 찾을 수 없습니다.")
        return 1
    
    # YYYYMM 형식의 폴더 찾기
    folders = [f for f in os.listdir(raw_dir) 
               if os.path.isdir(raw_dir / f) and f.isdigit() and len(f) == 6]
    
    if not folders:
        print("[ERROR] 분석월 폴더를 찾을 수 없습니다.")
        print("raw 폴더에 YYYYMM 형식의 폴더가 있어야 합니다 (예: 202511)")
        print()
        print("현재 raw 폴더 구조:")
        for item in os.listdir(raw_dir):
            print(f"  - {item}")
        return 1
    
    folders.sort(reverse=True)
    year_month = folders[0]
    print(f"Analysis Month: {year_month} (from folder name: raw/{year_month})")
    print()
    
    # Step 1: Download previous year data
    print("[Step 1/5] Downloading previous year data (Snowflake)...")
    from scripts.download_previous_year_rawdata import main as download_prev
    try:
        sys.argv = ['download_previous_year_rawdata.py', year_month]
        download_prev()
    except Exception as e:
        print(f"[ERROR] Previous year data download failed: {e}")
        return 1
    print()
    
    # Step 2: Process previous year data
    print("[Step 2/5] Processing previous year data...")
    previous_year_file = raw_dir / year_month / "previous_year" / f"rawdata_{year_month}_ALL.csv"
    if not previous_year_file.exists():
        print(f"[ERROR] Previous year data file not found: {previous_year_file}")
        return 1
    
    from scripts.process_previous_year_rawdata import main as process_prev
    try:
        sys.argv = ['process_previous_year_rawdata.py', str(previous_year_file)]
        process_prev()
    except Exception as e:
        print(f"[ERROR] Previous year data processing failed: {e}")
        return 1
    print()
    
    # Step 3: Process plan data
    print("[Step 3/5] Processing plan data...")
    from scripts.process_plan_data import main as process_plan
    try:
        sys.argv = ['process_plan_data.py', year_month]
        process_plan()
    except Exception as e:
        print(f"[ERROR] Plan data processing failed: {e}")
        return 1
    print()
    
    # Step 4: Download previous year cumulative sales
    print("[Step 4/5] Downloading previous year cumulative sales (Snowflake)...")
    from scripts.download_previous_year_cumulative_sales import main as download_cumulative
    try:
        sys.argv = ['download_previous_year_cumulative_sales.py', year_month]
        download_cumulative()
    except Exception as e:
        print(f"[ERROR] Previous year cumulative sales download failed: {e}")
        return 1
    print()
    
    # Step 5: Calculate progress days
    print("[Step 5/5] Calculating progress days...")
    from scripts.calculate_progress_days import main as calc_progress
    try:
        sys.argv = ['calculate_progress_days.py', year_month]
        calc_progress()
    except Exception as e:
        print(f"[ERROR] Progress days calculation failed: {e}")
        return 1
    print()
    
    print("=" * 80)
    print("  Complete!")
    print("=" * 80)
    print()
    print("Generated files:")
    print(f"  - raw/{year_month}/previous_year/previous_rawdata_{year_month}_전처리완료.csv")
    print(f"  - raw/{year_month}/previous_year/previous_rawdata_{year_month}_Shop.csv")
    print(f"  - raw/{year_month}/previous_year/previous_rawdata_{year_month}_Shop_Item.csv")
    print(f"  - raw/{year_month}/plan/plan_{year_month}_전처리완료.csv")
    print(f"  - raw/{year_month}/previous_year/cumulative_sales_*.csv")
    print(f"  - raw/{year_month}/previous_year/progress_days_{year_month}.csv")
    print()
    print("Note: Direct cost rates will be extracted when running 당년데이터_처리실행.bat")
    print()
    print("=" * 80)
    print("  Previous Year / Plan Data Update Complete")
    print("=" * 80)
    print()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)






