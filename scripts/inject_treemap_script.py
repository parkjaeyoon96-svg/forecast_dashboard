"""
Dashboard.html에 treemap_data.js 스크립트 자동 추가
"""

import os
import re

# 경로 설정
DASHBOARD_PATH = r"C:\Users\AD0283\Desktop\AIproject\Project_Forcast\public\Dashboard.html"
SCRIPT_TAG = '  <script defer src="./treemap_data.js"></script>'

def inject_script():
    """
    Dashboard.html에 treemap_data.js 스크립트 추가
    """
    if not os.path.exists(DASHBOARD_PATH):
        print(f"❌ Dashboard.html 파일이 없습니다: {DASHBOARD_PATH}")
        return False
    
    # 파일 읽기
    with open(DASHBOARD_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 이미 추가되어 있는지 확인
    if 'treemap_data.js' in content:
        print(f"✅ treemap_data.js는 이미 추가되어 있습니다.")
        return True
    
    # data.js 스크립트 태그 찾기
    pattern = r'(<script defer src="\.\/data\.js"><\/script>)'
    match = re.search(pattern, content)
    
    if not match:
        print(f"⚠️  data.js 스크립트 태그를 찾을 수 없습니다.")
        print(f"   수동으로 추가해주세요:")
        print(f"   {SCRIPT_TAG}")
        return False
    
    # treemap_data.js 스크립트 추가 (data.js 다음에)
    new_content = content.replace(
        match.group(0),
        match.group(0) + '\n' + SCRIPT_TAG
    )
    
    # 파일 저장
    with open(DASHBOARD_PATH, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ treemap_data.js 스크립트가 추가되었습니다!")
    print(f"   파일: {DASHBOARD_PATH}")
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("Dashboard.html 스크립트 자동 추가")
    print("=" * 60)
    print()
    
    success = inject_script()
    
    if success:
        print(f"\n🎉 완료! 이제 Dashboard.html이 treemap_data.js를 로드합니다.")
    else:
        print(f"\n❌ 실패! 수동으로 추가해주세요.")






