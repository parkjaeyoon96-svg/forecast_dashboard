#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.js 구조 단순화 스크립트 v2
- IIFE 제거
- 조건문 제거
- 직접 window 객체에 할당
- 올바른 순서로 배치
"""

import re
import sys
from datetime import datetime
from pathlib import Path

def extract_json_block(content, var_name, start_line):
    """JavaScript 변수에서 JSON 객체/배열 추출"""
    lines = content.split('\n')
    start_idx = start_line - 1
    line = lines[start_idx]
    
    match = re.search(rf'var\s+{var_name}\s*=\s*', line)
    if not match:
        return None
    
    json_start = line[match.end():]
    brace_count = json_start.count('{') + json_start.count('[') - json_start.count('}') - json_start.count(']')
    
    json_lines = [json_start]
    idx = start_idx + 1
    
    while idx < len(lines) and brace_count > 0:
        line = lines[idx]
        brace_count += line.count('{') + line.count('[') - line.count('}') - line.count(']')
        json_lines.append(line)
        idx += 1
    
    json_str = '\n'.join(json_lines)
    json_str = json_str.rstrip().rstrip(';')
    
    return json_str


def extract_window_d_block(content, property_name, start_line):
    """window.D.property = {...} 형태에서 JSON 추출 (실제 데이터만)"""
    lines = content.split('\n')
    start_idx = start_line - 1
    line = lines[start_idx]
    
    match = re.search(rf'window\.D\.{property_name}\s*=\s*', line)
    if not match:
        return None
    
    rest = line[match.end():].strip()
    
    # 다른 변수 참조인 경우 스킵 (예: brandPLData;)
    if rest and not rest.startswith('{') and not rest.startswith('['):
        print(f"  [스킵] window.D.{property_name} = {rest[:30]}... (변수 참조)")
        return None
    
    json_start = rest
    brace_count = json_start.count('{') + json_start.count('[') - json_start.count('}') - json_start.count(']')
    
    json_lines = [json_start]
    idx = start_idx + 1
    
    while idx < len(lines) and brace_count > 0:
        line = lines[idx]
        brace_count += line.count('{') + line.count('[') - line.count('}') - line.count(']')
        json_lines.append(line)
        idx += 1
    
    json_str = '\n'.join(json_lines)
    json_str = json_str.rstrip().rstrip(';')
    
    return json_str


def rebuild_data_js(date_str):
    """data.js를 단순한 구조로 재생성"""
    
    # 백업 파일에서 읽기 (원본)
    backup_file = Path(f'public/data_{date_str}_complex_backup.js')
    if not backup_file.exists():
        backup_file = Path(f'public/data_{date_str}_backup.js')
    if not backup_file.exists():
        backup_file = Path(f'public/data_{date_str}.js')
    
    output_file = Path(f'public/data_{date_str}.js')
    
    print(f"[시작] {backup_file} → {output_file} 구조 단순화...")
    
    with open(backup_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    # 각 변수의 시작 줄 번호 찾기
    var_locations = {}
    window_d_locations = {}
    
    for i, line in enumerate(lines, 1):
        match = re.match(r'^\s*var\s+(\w+)\s*=', line)
        if match:
            var_locations[match.group(1)] = i
        
        match = re.match(r'^\s*window\.D\.(\w+)\s*=', line)
        if match:
            window_d_locations[match.group(1)] = i
    
    print(f"[발견] var 변수: {list(var_locations.keys())}")
    print(f"[발견] window.D 속성: {list(window_d_locations.keys())}")
    
    # 데이터 추출
    extracted_vars = {}
    for var_name, line_num in var_locations.items():
        data = extract_json_block(content, var_name, line_num)
        if data:
            extracted_vars[var_name] = data
            print(f"  ✓ {var_name}: {len(data):,} chars")
    
    window_d_data = {}
    for prop_name, line_num in window_d_locations.items():
        data = extract_window_d_block(content, prop_name, line_num)
        if data:
            window_d_data[prop_name] = data
            print(f"  ✓ window.D.{prop_name}: {len(data):,} chars")
    
    # 단순화된 JS 파일 생성
    output_lines = []
    output_lines.append(f'// 대시보드 데이터 (단순화된 구조)')
    output_lines.append(f'// 자동 생성: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    output_lines.append(f'// 날짜: {date_str}')
    output_lines.append('')
    
    # 1. 브랜드별 분석 데이터 (먼저!)
    output_lines.append('// ========== 브랜드별 분석 데이터 ==========')
    output_lines.append('')
    
    for var_name, data in extracted_vars.items():
        output_lines.append(f'window.{var_name} = {data};')
        output_lines.append('')
    
    # 2. 전체현황 데이터 (window.D)
    output_lines.append('// ========== 전체현황 데이터 ==========')
    output_lines.append('')
    output_lines.append('window.D = {};')
    output_lines.append('')
    
    for prop_name, data in window_d_data.items():
        output_lines.append(f'window.D.{prop_name} = {data};')
        output_lines.append('')
    
    # 3. brandPLData를 window.D에도 연결 (마지막에)
    output_lines.append('// brandPLData를 window.D에도 연결')
    output_lines.append('window.D.brandPLData = window.brandPLData;')
    output_lines.append('')
    
    # 로드 완료 로그
    output_lines.append("console.log('[Data.js] 모든 대시보드 데이터 로드 완료');")
    output_lines.append('')
    
    # 파일 저장
    output_content = '\n'.join(output_lines)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output_content)
    
    print(f"\n[완료] {output_file}")
    print(f"[크기] {len(output_content):,} bytes, {len(output_lines):,} lines")
    
    return output_file


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python rebuild_data_js.py <date>")
        print("예시: python rebuild_data_js.py 20251124")
        sys.exit(1)
    
    rebuild_data_js(sys.argv[1])
