#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.js 구조 단순화 스크립트
- IIFE 제거
- 모든 데이터를 직접 window 객체에 할당
- 조건문 제거
"""

import re
import sys
from datetime import datetime

def simplify_data_js(input_file, output_file=None):
    """data.js를 단순한 구조로 변환"""
    
    if output_file is None:
        output_file = input_file
    
    print(f"[시작] {input_file} 구조 단순화...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 원본 백업
    backup_file = input_file.replace('.js', '_backup.js')
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"[백업] {backup_file}")
    
    # 1. IIFE 시작 제거: (function(){
    content = re.sub(r'^\(function\(\)\s*\{\s*\n', '', content, count=1)
    
    # 2. IIFE 종료 제거: })();
    content = re.sub(r'\n\s*\}\)\(\);\s*\n', '\n', content)
    
    # 3. var 선언을 window. 할당으로 변환
    # var brandNames = { -> window.brandNames = {
    var_pattern = r'^(\s*)var\s+(\w+)\s*=\s*'
    
    lines = content.split('\n')
    new_lines = []
    inside_var = False
    current_var = None
    brace_count = 0
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # var 선언 시작 확인
        var_match = re.match(r'^(\s*)var\s+(\w+)\s*=\s*(.*)$', line)
        if var_match and not inside_var:
            indent = var_match.group(1)
            var_name = var_match.group(2)
            rest = var_match.group(3)
            
            # window.로 변환
            new_line = f'{indent}window.{var_name} = {rest}'
            new_lines.append(new_line)
            
            # 중괄호/대괄호 카운트
            brace_count = rest.count('{') + rest.count('[') - rest.count('}') - rest.count(']')
            if brace_count > 0:
                inside_var = True
                current_var = var_name
        elif inside_var:
            new_lines.append(line)
            brace_count += line.count('{') + line.count('[') - line.count('}') - line.count(']')
            if brace_count <= 0:
                inside_var = False
                current_var = None
        else:
            # 조건부 window 초기화 제거
            # if (typeof window.D === 'undefined') { window.D = {}; }
            if 'if (typeof window.' in line and "=== 'undefined')" in line:
                # 다음 줄들을 확인해서 조건문 블록 스킵
                j = i + 1
                brace = 1
                while j < len(lines) and brace > 0:
                    brace += lines[j].count('{') - lines[j].count('}')
                    j += 1
                i = j - 1
            # 이미 window. 할당인 경우 그대로 유지
            elif line.strip().startswith('window.'):
                new_lines.append(line)
            # 빈 줄이나 주석은 유지
            elif line.strip() == '' or line.strip().startswith('//') or line.strip().startswith('/*'):
                new_lines.append(line)
            # console.log 유지
            elif 'console.log' in line:
                new_lines.append(line)
            else:
                new_lines.append(line)
        
        i += 1
    
    content = '\n'.join(new_lines)
    
    # 4. 중복 window.D 초기화 정리
    # window.D = window.DASHBOARD_DATA || {}; 같은 패턴 제거
    content = re.sub(r'\s*window\.D\s*=\s*window\.DASHBOARD_DATA\s*\|\|\s*\{\};\s*\n', '\n', content)
    
    # 5. DASHBOARD_DATA 관련 코드 제거
    content = re.sub(r'\s*window\.DASHBOARD_DATA\.brandPLData\s*=\s*brandPLData;\s*\n', '\n', content)
    content = re.sub(r'\s*window\.DASHBOARD_DATA\s*=\s*\{\};\s*\n', '\n', content)
    
    # 6. 헤더 추가
    header = f"""// 대시보드 데이터 (단순화된 구조)
// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// 모든 데이터는 window 객체에 직접 할당됨

"""
    
    # 기존 헤더 제거
    content = re.sub(r'^//.*\n//.*\n\n?', '', content, count=1)
    
    content = header + content
    
    # 7. 파일 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"[완료] {output_file} 저장됨")
    print(f"[확인] 원본: {backup_file}")
    
    return output_file


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python simplify_data_js.py <date>")
        print("예시: python simplify_data_js.py 20251124")
        sys.exit(1)
    
    date_str = sys.argv[1]
    input_file = f'public/data_{date_str}.js'
    
    simplify_data_js(input_file)















