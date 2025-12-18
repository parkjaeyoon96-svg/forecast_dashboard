#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.js 구조 단순화 스크립트 v2
- IIFE 완전 제거
- var를 window. 할당으로 변환
"""

import re
import sys
from datetime import datetime

def simplify_data_js(date_str):
    """data.js를 단순한 구조로 변환"""
    
    # 백업 파일에서 읽기 (원본)
    backup_file = f'public/data_{date_str}_backup.js'
    output_file = f'public/data_{date_str}.js'
    
    print(f"[시작] {backup_file} → {output_file} 변환...")
    
    with open(backup_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    new_lines = []
    
    # 헤더 추가
    new_lines.append(f'// 대시보드 데이터 (단순화된 구조)')
    new_lines.append(f'// 자동 생성 일시: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    new_lines.append('')
    
    skip_until_close = False
    brace_depth = 0
    in_iife = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        
        # IIFE 시작 스킵
        if stripped == '(function(){':
            in_iife = True
            i += 1
            continue
        
        # IIFE 종료 스킵
        if stripped == '})();':
            in_iife = False
            i += 1
            continue
        
        # 조건부 초기화 스킵
        if "if (typeof window." in stripped and "=== 'undefined')" in stripped:
            # 다음 줄의 중괄호 카운트
            brace_depth = 1
            i += 1
            while i < len(lines) and brace_depth > 0:
                inner_line = lines[i]
                brace_depth += inner_line.count('{') - inner_line.count('}')
                i += 1
            continue
        
        # var 선언을 window. 할당으로 변환
        if stripped.startswith('var '):
            # var varName = value 형태
            match = re.match(r'^(\s*)var\s+(\w+)\s*=\s*(.*)$', line)
            if match:
                indent = ''  # 인덴트 제거
                var_name = match.group(2)
                rest = match.group(3)
                new_lines.append(f'window.{var_name} = {rest}')
                i += 1
                continue
        
        # window. 할당은 그대로 유지 (인덴트 제거)
        if stripped.startswith('window.'):
            new_lines.append(stripped)
            i += 1
            continue
        
        # console.log 유지
        if 'console.log' in stripped:
            new_lines.append(stripped)
            i += 1
            continue
        
        # 빈 줄은 유지 (연속 빈 줄 방지)
        if stripped == '':
            if new_lines and new_lines[-1].strip() != '':
                new_lines.append('')
            i += 1
            continue
        
        # 주석 유지
        if stripped.startswith('//') or stripped.startswith('/*'):
            new_lines.append(line)
            i += 1
            continue
        
        # 나머지는 그대로 추가 (인덴트가 있으면 제거)
        if in_iife and line.startswith('  '):
            new_lines.append(line[2:])  # 2칸 인덴트 제거
        else:
            new_lines.append(line)
        
        i += 1
    
    content = '\n'.join(new_lines)
    
    # 파일 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"[완료] {output_file} 저장됨")
    
    # 결과 확인
    with open(output_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"[확인] 총 {len(lines)}줄")
    print(f"[확인] 처음 10줄:")
    for i, line in enumerate(lines[:10], 1):
        print(f"  {i}: {line.rstrip()}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python simplify_data_js_v2.py <date>")
        sys.exit(1)
    
    simplify_data_js(sys.argv[1])































