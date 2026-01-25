#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
브랜드별 인사이트 개별 파일들을 통합 파일에 병합하는 스크립트

사용법:
    python scripts/merge_brand_insights.py 20260112
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
PUBLIC_DIR = ROOT / "public"

BRAND_CODE_MAP = {
    'MLB': 'MLB',
    'MLB_KIDS': 'MLB_KIDS',
    'DISCOVERY': 'DISCOVERY',
    'DUVETICA': 'DUVETICA',
    'SERGIO': 'SERGIO',
    'SUPRA': 'SUPRA'
}


def merge_brand_insights(date_str: str):
    """브랜드별 인사이트 개별 파일들을 통합 파일에 병합"""
    
    base_dir = PUBLIC_DIR / "data" / date_str / "ai_insights"
    
    if not base_dir.exists():
        print(f"[ERROR] 디렉토리를 찾을 수 없습니다: {base_dir}")
        return False
    
    # 통합 파일 로드
    combined_file = base_dir / f"insights_data_{date_str}.json"
    
    if not combined_file.exists():
        print(f"[ERROR] 통합 파일을 찾을 수 없습니다: {combined_file}")
        return False
    
    with open(combined_file, 'r', encoding='utf-8') as f:
        combined_data = json.load(f)
    
    print(f"[읽기] {combined_file}")
    print(f"  현재 키: {list(combined_data.keys())}")
    
    # 브랜드별 개별 파일 로드 및 병합
    merged_count = 0
    for brand_code, brand_name in BRAND_CODE_MAP.items():
        brand_file = base_dir / f"insights_data_{brand_name}_{date_str}.json"
        
        if brand_file.exists():
            with open(brand_file, 'r', encoding='utf-8') as f:
                brand_data = json.load(f)
            
            # 브랜드 데이터가 브랜드 키로 감싸져 있는 경우
            if brand_name in brand_data:
                combined_data[brand_name] = brand_data[brand_name]
                merged_count += 1
                print(f"  ✓ {brand_name} 병합 완료")
            # 평면 구조인 경우
            elif isinstance(brand_data, dict) and brand_data:
                combined_data[brand_name] = brand_data
                merged_count += 1
                print(f"  ✓ {brand_name} 병합 완료 (평면 구조)")
            else:
                print(f"  ⚠ {brand_name} 데이터 형식 확인 필요")
        else:
            print(f"  ⚠ {brand_name} 파일 없음: {brand_file}")
    
    # 통합 파일 저장
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[저장] {combined_file}")
    print(f"  병합된 브랜드 수: {merged_count}")
    print(f"  최종 키: {list(combined_data.keys())}")
    
    return True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python scripts/merge_brand_insights.py <date>")
        print("예시: python scripts/merge_brand_insights.py 20260112")
        sys.exit(1)
    
    date_str = sys.argv[1]
    success = merge_brand_insights(date_str)
    sys.exit(0 if success else 1)








