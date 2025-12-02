"""data_20251124.js의 brandPLData를 JSON 파일로 업데이트하는 스크립트"""
import json
import sys

def find_matching_brace(content, start_pos):
    """중괄호 균형을 맞춰서 닫는 중괄호 위치 찾기"""
    count = 0
    i = start_pos
    while i < len(content):
        if content[i] == '{':
            count += 1
        elif content[i] == '}':
            count -= 1
            if count == 0:
                return i
        i += 1
    return -1

def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else '20251124'
    
    json_path = f'public/brand_pl_data_{date_str}.json'
    js_path = f'public/data_{date_str}.js'
    
    # JSON 파일 읽기
    with open(json_path, 'r', encoding='utf-8') as f:
        brand_pl_data = json.load(f)
    
    # data.js 파일 읽기
    with open(js_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # brandPLData 시작 위치 찾기
    marker = 'var brandPLData = {'
    start_idx = content.find(marker)
    
    if start_idx == -1:
        print(f'❌ {marker}를 찾을 수 없습니다.')
        return
    
    # 중괄호 시작 위치
    brace_start = start_idx + len(marker) - 1
    
    # 매칭되는 닫는 중괄호 찾기
    brace_end = find_matching_brace(content, brace_start)
    
    if brace_end == -1:
        print('❌ 닫는 중괄호를 찾을 수 없습니다.')
        return
    
    # 새로운 brandPLData JSON 문자열
    new_brand_pl_str = json.dumps(brand_pl_data, ensure_ascii=False, indent=2)
    
    # 내용 교체
    new_content = content[:start_idx] + 'var brandPLData = ' + new_brand_pl_str + content[brace_end+1:]
    
    # 파일 저장
    with open(js_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f'✅ {js_path}의 brandPLData 업데이트 완료!')
    print(f'MLB 매출원가 전년: {brand_pl_data["MLB"]["cog"]["prev"]}억원')

if __name__ == '__main__':
    main()
