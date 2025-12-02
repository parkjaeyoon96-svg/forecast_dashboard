#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.js 최종 정리 스크립트
- 조건부 window 할당 제거
- 직접 할당으로 변환
"""

import re
import sys

def cleanup_data_js(date_str):
    """data.js 조건부 할당 제거"""
    
    file_path = f'public/data_{date_str}.js'
    
    print(f"[시작] {file_path} 정리...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. if (typeof window !== 'undefined') { ... } 블록 제거하고 내부만 유지
    # 패턴: if (typeof window !== 'undefined') {\n내용\n}
    pattern = r"if \(typeof window !== 'undefined'\) \{\n([^}]+)\n\}"
    
    def replace_conditional(match):
        inner = match.group(1)
        # 내부 코드만 반환 (들여쓰기 제거)
        lines = inner.split('\n')
        cleaned = []
        for line in lines:
            # 앞의 공백 제거
            cleaned.append(line.strip())
        return '\n'.join(cleaned)
    
    content = re.sub(pattern, replace_conditional, content, flags=re.MULTILINE)
    
    # 2. 중복 헤더 제거
    # 두 번째 헤더 제거
    lines = content.split('\n')
    new_lines = []
    seen_header = False
    skip_next_empty = False
    
    for i, line in enumerate(lines):
        # 중복 헤더 스킵
        if '// 모든 대시보드 데이터' in line or '// 대시보드 데이터' in line:
            if seen_header:
                skip_next_empty = True
                continue
            seen_header = True
        
        # 자동 생성 일시 헤더도 중복이면 스킵
        if '// 자동 생성 일시:' in line:
            if len([l for l in new_lines if '// 자동 생성 일시:' in l]) > 0:
                continue
        
        # 빈 줄 스킵 (중복 헤더 후)
        if skip_next_empty and line.strip() == '':
            skip_next_empty = False
            continue
        
        new_lines.append(line)
    
    content = '\n'.join(new_lines)
    
    # 3. DASHBOARD_DATA 관련 코드 제거
    content = re.sub(r'window\.DASHBOARD_DATA\.brandPLData = brandPLData;\n?', '', content)
    content = re.sub(r'window\.DASHBOARD_DATA = \{\};\n?', '', content)
    
    # 4. 연속 빈 줄 정리 (3줄 이상 -> 2줄)
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    # 5. 마지막에 console.log 확인 및 추가
    if "console.log('[Data.js]" not in content:
        content = content.rstrip() + "\n\nconsole.log('[Data.js] 모든 대시보드 데이터 로드 완료');\n"
    
    # 저장
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"[완료] {file_path} 저장됨")
    
    # 확인
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"[확인] 총 {len(lines)}줄")
    
    # 조건부 할당 확인
    conditional_count = content.count("if (typeof window")
    print(f"[확인] 조건부 할당 남은 개수: {conditional_count}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python cleanup_data_js.py <date>")
        sys.exit(1)
    
    cleanup_data_js(sys.argv[1])















