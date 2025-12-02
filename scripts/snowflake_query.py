"""
Snowflake 쿼리 실행 스크립트 (API용)

사용법:
    python scripts/snowflake_query.py "SELECT * FROM table_name LIMIT 10"
    
출력: JSON 형식의 쿼리 결과
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드
env_path = project_root / '.env'
load_dotenv(env_path)

def execute_query(query: str):
    """
    Snowflake 쿼리 실행 및 결과를 JSON으로 반환
    
    Args:
        query: 실행할 SQL 쿼리
        
    Returns:
        dict: 쿼리 결과 (JSON 형식)
    """
    conn = None
    try:
        # Snowflake 연결
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        
        # 쿼리 실행
        cursor = conn.cursor()
        cursor.execute(query)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 데이터 가져오기
        data = cursor.fetchall()
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        conn.close()
        
        # JSON으로 변환
        result = {
            'success': True,
            'columns': columns,
            'data': df.to_dict('records'),
            'rowCount': len(df)
        }
        
        return result
        
    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e)
        }
        return error_result

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({
            'success': False,
            'error': '쿼리가 필요합니다.'
        }))
        sys.exit(1)
    
    query = sys.argv[1]
    result = execute_query(query)
    print(json.dumps(result, ensure_ascii=False, indent=2))


