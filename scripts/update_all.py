"""
전체 대시보드 데이터 업데이트 통합 스크립트
3가지 데이터 소스를 처리합니다:
1. SAP KE30 데이터 (수동 업로드 CSV)
2. Snowflake 데이터 (자동 다운로드)
3. 마스터 데이터 (Excel 또는 CSV)
"""
import subprocess
import sys
import os
from datetime import datetime
import json

def print_section(title):
    """섹션 제목 출력"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def run_script(script_name, description):
    """Python 스크립트 실행"""
    print(f"\n🚀 {description}...")
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.returncode != 0:
            print(f"⚠️ 경고: {script_name} 실행 중 문제 발생")
            if result.stderr:
                print(result.stderr)
            return False
        
        print(f"✅ {description} 완료")
        return True
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False

def check_files():
    """필수 파일 존재 여부 확인"""
    print_section("필수 파일 확인")
    
    required_files = {
        'raw/sap_ke30.csv': 'SAP KE30 손익 데이터',
        # 'raw/master_data.xlsx': '마스터 데이터',  # 또는 .csv
    }
    
    missing_files = []
    
    for file_path, description in required_files.items():
        if os.path.exists(file_path):
            size = os.path.getsize(file_path) / 1024  # KB
            print(f"✅ {description}: {file_path} ({size:.1f} KB)")
        else:
            print(f"❌ {description}: {file_path} - 파일 없음")
            missing_files.append((file_path, description))
    
    if missing_files:
        print("\n⚠️ 다음 파일들을 준비해주세요:")
        for file_path, description in missing_files:
            print(f"   - {description}: {file_path}")
        return False
    
    return True

def process_all_data():
    """모든 데이터 처리"""
    results = {}
    
    # 1. Snowflake 데이터 가져오기 (선택적)
    print_section("1/3: Snowflake 데이터")
    if os.path.exists('scripts/fetch_snowflake.py'):
        results['snowflake'] = run_script(
            'scripts/fetch_snowflake.py',
            'Snowflake 데이터 다운로드'
        )
    else:
        print("ℹ️  Snowflake 스크립트 없음 - 스킵")
        results['snowflake'] = None
    
    # 2. 마스터 데이터 처리 (선택적)
    print_section("2/3: 마스터 데이터")
    if os.path.exists('scripts/process_master_data.py'):
        results['master'] = run_script(
            'scripts/process_master_data.py',
            '마스터 데이터 처리'
        )
    else:
        print("ℹ️  마스터 데이터 스크립트 없음 - 스킵")
        results['master'] = None
    
    # 3. KE30 데이터 처리 (필수)
    print_section("3/3: SAP KE30 데이터")
    results['ke30'] = run_script(
        'scripts/process_ke30.py',
        'KE30 데이터 전처리'
    )
    
    return results

def create_metadata(results):
    """메타데이터 생성"""
    print_section("메타데이터 생성")
    
    metadata = {
        'last_updated': datetime.now().isoformat(),
        'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_sources': {
            'ke30': 'success' if results.get('ke30') else 'failed',
            'snowflake': 'success' if results.get('snowflake') else 'skipped',
            'master': 'success' if results.get('master') else 'skipped',
        },
        'files': {
            'ke30': 'data/ke30_processed.json',
            'snowflake': 'data/snowflake_data.json',
            'master': 'data/master_data.json',
        }
    }
    
    os.makedirs('data', exist_ok=True)
    with open('data/metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print("✅ 메타데이터 저장 완료")
    return metadata

def print_summary(results, metadata):
    """처리 결과 요약"""
    print_section("처리 완료 요약")
    
    print("\n📊 데이터 처리 결과:")
    for source, status in metadata['data_sources'].items():
        icon = "✅" if status == "success" else "⚠️" if status == "skipped" else "❌"
        print(f"   {icon} {source.upper()}: {status}")
    
    print(f"\n⏰ 업데이트 시간: {metadata['update_date']}")
    
    # 생성된 파일 확인
    print("\n📁 생성된 파일:")
    for name, path in metadata['files'].items():
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024  # KB
            print(f"   ✅ {name}: {path} ({size:.1f} KB)")
        else:
            print(f"   ⚠️ {name}: {path} - 생성 안됨")

def print_next_steps():
    """다음 단계 안내"""
    print("\n" + "=" * 60)
    print("  다음 단계")
    print("=" * 60)
    print("\n1. 데이터 확인:")
    print("   - data/ 폴더의 JSON 파일들을 확인하세요")
    print("\n2. Git 커밋 & 푸시:")
    print("   git add .")
    print('   git commit -m "데이터 업데이트"')
    print("   git push")
    print("\n3. 배포 확인:")
    print("   - 1-2분 후 Vercel에서 자동 배포됩니다")
    print("   - 대시보드에서 업데이트된 데이터를 확인하세요")

def main():
    """메인 실행"""
    print("\n" + "=" * 60)
    print("  📊 대시보드 데이터 통합 업데이트")
    print("=" * 60)
    print(f"\n시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 필수 파일 확인
    if not check_files():
        print("\n❌ 필수 파일이 없습니다. 준비 후 다시 실행하세요.")
        return
    
    # 2. 모든 데이터 처리
    results = process_all_data()
    
    # 3. 메타데이터 생성
    metadata = create_metadata(results)
    
    # 4. 결과 요약
    print_summary(results, metadata)
    
    # 5. 다음 단계 안내
    print_next_steps()
    
    print("\n" + "=" * 60)
    print("  ✅ 모든 처리 완료!")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()














