"""
개별 브랜드별 인사이트 파일들을 읽어서 통합 파일을 생성하는 스크립트

사용법:
    python scripts/merge_insights_data.py --date 20251117
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any

# 브랜드 목록
BRANDS = ['MLB', 'MLB_KIDS', 'DISCOVERY', 'DUVETICA', 'SERGIO', 'SUPRA']

def load_json_file(file_path: Path) -> Dict[str, Any]:
    """JSON 파일 로드"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARNING] 파일을 찾을 수 없습니다: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON 파싱 오류: {file_path} - {e}")
        return None

def merge_insights_data(date_str: str):
    """개별 브랜드 인사이트 파일들을 통합 파일로 병합"""
    base_dir = Path("public") / "data" / date_str / "ai_insights"
    
    if not base_dir.exists():
        print(f"[ERROR] 디렉토리가 없습니다: {base_dir}")
        return
    
    # 통합 데이터 구조
    merged_data = {}
    
    # 1. 전체 현황 데이터 로드
    overview_file = base_dir / f"insights_data_overview_{date_str}.json"
    if overview_file.exists():
        overview_data = load_json_file(overview_file)
        if overview_data and "overview" in overview_data:
            merged_data["overview"] = overview_data["overview"]
            print(f"[OK] 전체 현황 데이터 로드 완료")
    
    # 2. 각 브랜드별 데이터 로드
    for brand in BRANDS:
        brand_file = base_dir / f"insights_data_{brand}_{date_str}.json"
        if brand_file.exists():
            brand_data = load_json_file(brand_file)
            if brand_data and brand in brand_data:
                merged_data[brand] = brand_data[brand]
                print(f"[OK] {brand} 브랜드 데이터 로드 완료")
            else:
                print(f"[WARNING] {brand} 파일에 브랜드 데이터가 없습니다.")
        else:
            print(f"[WARNING] {brand} 브랜드 파일이 없습니다: {brand_file}")
    
    # 3. 통합 파일 저장
    output_file = base_dir / f"insights_data_{date_str}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[SUCCESS] 통합 파일 생성 완료: {output_file}")
    print(f"[INFO] 포함된 브랜드: {', '.join([k for k in merged_data.keys() if k != 'overview'])}")

def main():
    parser = argparse.ArgumentParser(description="개별 브랜드 인사이트 파일들을 통합 파일로 병합")
    parser.add_argument("--date", type=str, required=True, help="날짜 (YYYYMMDD 형식)")
    
    args = parser.parse_args()
    
    merge_insights_data(args.date)

if __name__ == "__main__":
    main()


































