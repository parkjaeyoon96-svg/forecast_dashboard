"""data.js 파일 구조 확인"""
import re

with open('public/data_20251124.js', 'r', encoding='utf-8') as f:
    content = f.read()

print(f"파일 총 길이: {len(content)} 문자")
print(f"파일 총 줄 수: {content.count(chr(10)) + 1}")

# IIFE 구조 확인
iife_start = content.find('(function(){')
iife_end = content.rfind('})();')

print(f"\nIIFE 시작 위치: {iife_start}")
print(f"IIFE 종료 위치: {iife_end}")

if iife_end > 0:
    after_iife = content[iife_end+5:].strip()
    print(f"IIFE 이후 내용 길이: {len(after_iife)} 문자")
    if after_iife:
        print(f"IIFE 이후 첫 500자:\n{after_iife[:500]}")
    else:
        print("IIFE 이후 내용 없음")

# window 할당 확인
print("\n=== window 할당 확인 ===")
window_assignments = re.findall(r'window\.(brandPLData|brandKPI|D\.brandPLData|D\.overviewPL)', content)
print(f"window 할당 목록: {window_assignments}")

# brandPLData 위치 확인
brandpl_match = re.search(r'var brandPLData = \{', content)
if brandpl_match:
    print(f"\nvar brandPLData 위치: {brandpl_match.start()}")
    
window_brandpl_match = re.search(r'window\.brandPLData = brandPLData', content)
if window_brandpl_match:
    print(f"window.brandPLData 할당 위치: {window_brandpl_match.start()}")
    # IIFE 안인지 밖인지 확인
    if window_brandpl_match.start() < iife_end:
        print("  -> IIFE 안에 있음 (정상)")
    else:
        print("  -> IIFE 밖에 있음 (문제!)")






























