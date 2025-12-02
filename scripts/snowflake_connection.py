"""
Snowflake 데이터베이스 연결 및 쿼리 실행 스크립트

사용법:
    python scripts/snowflake_connection.py
    
환경 변수:
    SNOWFLAKE_ACCOUNT: Snowflake 계정명
    SNOWFLAKE_USERNAME: 사용자명
    SNOWFLAKE_PASSWORD: 비밀번호
    SNOWFLAKE_WAREHOUSE: 웨어하우스명
    SNOWFLAKE_DATABASE: 데이터베이스명
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    # .env 파일이 없으면 환경 변수에서 직접 읽기
    print("⚠️ .env 파일을 찾을 수 없습니다. 환경 변수에서 직접 읽습니다.")
    # 환경 변수가 없으면 기본값 사용 (사용자가 직접 설정해야 함)
    if not os.getenv('SNOWFLAKE_ACCOUNT'):
        print("⚠️ 환경 변수가 설정되지 않았습니다. .env 파일을 생성하거나 환경 변수를 설정하세요.")

def get_snowflake_connection():
    """
    Snowflake 데이터베이스 연결 생성
    
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake 연결 객체
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("✅ Snowflake 연결 성공!")
        return conn
    except Exception as e:
        print(f"❌ Snowflake 연결 실패: {e}")
        raise

def execute_query(conn, query: str):
    """
    Snowflake 쿼리 실행 및 결과를 pandas DataFrame으로 반환
    
    Args:
        conn: Snowflake 연결 객체
        query: 실행할 SQL 쿼리
        
    Returns:
        pd.DataFrame: 쿼리 결과
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 데이터 가져오기
        data = cursor.fetchall()
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        raise

def list_tables(conn, schema: str = None):
    """
    데이터베이스의 테이블 목록 조회
    
    Args:
        conn: Snowflake 연결 객체
        schema: 스키마명 (None이면 현재 스키마)
        
    Returns:
        pd.DataFrame: 테이블 목록
    """
    if schema:
        query = f"SHOW TABLES IN SCHEMA {schema}"
    else:
        query = "SHOW TABLES"
    
    return execute_query(conn, query)

def list_schemas(conn):
    """
    데이터베이스의 스키마 목록 조회
    
    Args:
        conn: Snowflake 연결 객체
        
    Returns:
        pd.DataFrame: 스키마 목록
    """
    query = "SHOW SCHEMAS"
    return execute_query(conn, query)

def test_connection():
    """
    Snowflake 연결 테스트
    """
    print("=" * 50)
    print("Snowflake 연결 테스트 시작")
    print("=" * 50)
    
    # 환경 변수 확인
    print("\n📋 환경 변수 확인:")
    print(f"  Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"  Username: {os.getenv('SNOWFLAKE_USERNAME')}")
    print(f"  Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    print(f"  Database: {os.getenv('SNOWFLAKE_DATABASE')}")
    
    conn = None
    try:
        # 연결 생성
        conn = get_snowflake_connection()
        
        # 현재 데이터베이스 확인
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        print(f"\n✅ 현재 설정:")
        print(f"  Database: {result[0]}")
        print(f"  Schema: {result[1]}")
        print(f"  Warehouse: {result[2]}")
        cursor.close()
        
        # 스키마 목록 조회
        print("\n📂 스키마 목록:")
        schemas_df = list_schemas(conn)
        print(schemas_df.to_string(index=False))
        
        # 테이블 목록 조회 (첫 번째 스키마)
        if len(schemas_df) > 0:
            first_schema = schemas_df.iloc[0]['name']
            print(f"\n📊 테이블 목록 (스키마: {first_schema}):")
            tables_df = list_tables(conn, first_schema)
            if len(tables_df) > 0:
                print(tables_df[['name', 'kind', 'rows']].to_string(index=False))
            else:
                print("  테이블이 없습니다.")
        
        print("\n" + "=" * 50)
        print("✅ 연결 테스트 완료!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n🔌 연결 종료")

if __name__ == "__main__":
    test_connection()

