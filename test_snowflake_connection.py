"""
Snowflake 연결 및 간단한 쿼리 테스트
"""
import os
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

load_dotenv()

print("=" * 70)
print("🔍 Snowflake 연결 테스트")
print("=" * 70)

try:
    print("\n1. 연결 시도 중...")
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USERNAME'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        network_timeout=60,
        login_timeout=30
    )
    print("✅ 연결 성공!")
    
    print("\n2. 간단한 쿼리 테스트...")
    cursor = conn.cursor()
    
    # 테스트 쿼리 1: 주차 데이터 확인
    test_query = """
    SELECT COUNT(*) as cnt
    FROM FNF.PRCS.DB_SH_S_W
    WHERE END_DT BETWEEN '2025-09-15'::DATE AND '2025-11-16'::DATE
      AND BRD_CD != 'A'
    """
    
    start = datetime.now()
    cursor.execute(test_query)
    result = cursor.fetchone()
    elapsed = (datetime.now() - start).total_seconds()
    
    print(f"✅ 쿼리 성공! ({elapsed:.2f}초)")
    print(f"   조회된 레코드 수: {result[0]:,}건")
    
    # 테스트 쿼리 2: 주차 목록 확인
    print("\n3. 주차 목록 확인...")
    week_query = """
    SELECT DISTINCT END_DT
    FROM FNF.PRCS.DB_SH_S_W
    WHERE END_DT BETWEEN '2025-09-15'::DATE AND '2025-11-16'::DATE
    ORDER BY END_DT
    """
    
    start = datetime.now()
    cursor.execute(week_query)
    weeks = cursor.fetchall()
    elapsed = (datetime.now() - start).total_seconds()
    
    print(f"✅ 주차 조회 성공! ({elapsed:.2f}초)")
    print(f"   주차 수: {len(weeks)}개")
    for week in weeks:
        print(f"   - {week[0]}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✅ 모든 테스트 통과!")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ 오류 발생: {e}")
    import traceback
    traceback.print_exc()

