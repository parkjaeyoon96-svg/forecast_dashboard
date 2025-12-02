#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
대시보드 데이터 생성 통합 스크립트
==================================

전체 파이프라인:
1. 전처리 데이터 확인
2. 각 영역별 파이썬 스크립트 실행 → data.js 생성
3. data.js → JSON 파일 변환
4. 완료

사용법:
    python generate_dashboard_data.py 20251124
    python generate_dashboard_data.py 20251124 --skip-preprocess
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = ROOT / "scripts"
PUBLIC_DIR = ROOT / "public"


def run_script(script_name: str, args: list = None, description: str = ""):
    """파이썬 스크립트 실행"""
    script_path = SCRIPTS_DIR / script_name
    
    if not script_path.exists():
        print(f"  ⚠️ 스크립트 없음: {script_name}")
        return False
    
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    print(f"\n{'='*60}")
    print(f"[실행] {description or script_name}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=False,
            text=True
        )
        
        if result.returncode != 0:
            print(f"  ❌ 실패 (exit code: {result.returncode})")
            return False
        
        print(f"  ✅ 완료")
        return True
        
    except Exception as e:
        print(f"  ❌ 오류: {e}")
        return False


def generate_dashboard_data(date_str: str, skip_preprocess: bool = False):
    """대시보드 데이터 전체 생성"""
    
    print("\n" + "="*60)
    print("  📊 대시보드 데이터 생성 시작")
    print("="*60)
    print(f"  날짜: {date_str}")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    year_month = date_str[:6]  # YYYYMM
    
    results = {}
    
    # Step 1: 브랜드 KPI 업데이트
    results['brand_kpi'] = run_script(
        'update_brand_kpi.py',
        [date_str],
        '브랜드 KPI 업데이트'
    )
    
    # Step 2: 전체현황 데이터 업데이트
    results['overview'] = run_script(
        'update_overview_data.py',
        [date_str],
        '전체현황 데이터 업데이트'
    )
    
    # Step 3: 브랜드 손익계산서 생성
    results['brand_pl'] = run_script(
        'create_brand_pl_data.py',
        [date_str],
        '브랜드 손익계산서 생성'
    )
    
    # Step 4: 브랜드 레이더 차트 데이터
    results['brand_radar'] = run_script(
        'update_brand_radar.py',
        [date_str],
        '브랜드 레이더 차트 데이터'
    )
    
    # Step 5: 채널 손익 데이터
    results['channel_pl'] = run_script(
        'process_channel_profit_loss.py',
        ['--base-date', date_str, '--target-month', year_month, '--format', 'dashboard'],
        '채널 손익 데이터'
    )
    
    # Step 6: 주간 매출 추세 다운로드
    results['weekly_trend'] = run_script(
        'download_weekly_sales_trend.py',
        [year_month],
        '주간 매출 추세 다운로드'
    )
    
    # Step 7: 재고 분석 다운로드
    results['stock_analysis'] = run_script(
        'download_brand_stock_analysis.py',
        [date_str],
        '재고 분석 다운로드'
    )
    
    # Step 8: 트리맵 데이터 생성
    results['treemap'] = run_script(
        'create_treemap_data_v2.py',
        [date_str],
        '트리맵 데이터 생성'
    )
    
    # Step 9: JSON 변환 (★ 핵심 ★)
    print("\n" + "="*60)
    print("[핵심] data.js → JSON 변환")
    print("="*60)
    
    results['json_export'] = run_script(
        'export_to_json.py',
        [date_str],
        'JSON 파일 변환'
    )
    
    # 결과 요약
    print("\n" + "="*60)
    print("  📋 처리 결과 요약")
    print("="*60)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for step, success in results.items():
        icon = "✅" if success else "❌"
        print(f"  {icon} {step}")
    
    print(f"\n  성공: {success_count}/{total_count}")
    
    # JSON 파일 확인
    json_dir = PUBLIC_DIR / "data" / date_str
    if json_dir.exists():
        print(f"\n  📁 생성된 JSON 파일:")
        total_size = 0
        for f in sorted(json_dir.glob("*.json")):
            size_kb = f.stat().st_size / 1024
            total_size += size_kb
            print(f"    ✓ {f.name} ({size_kb:.1f} KB)")
        print(f"\n    총 크기: {total_size:.1f} KB")
    
    print("\n" + "="*60)
    print("  ✅ 대시보드 데이터 생성 완료!")
    print("="*60)
    print(f"\n  Dashboard URL:")
    print(f"  http://localhost:3000/Dashboard.html?date={date_str}")
    
    return all(results.values())


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python generate_dashboard_data.py <date>")
        print("예시: python generate_dashboard_data.py 20251124")
        sys.exit(1)
    
    date_str = sys.argv[1]
    skip_preprocess = '--skip-preprocess' in sys.argv
    
    success = generate_dashboard_data(date_str, skip_preprocess)
    sys.exit(0 if success else 1)















