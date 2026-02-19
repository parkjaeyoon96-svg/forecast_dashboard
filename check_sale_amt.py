import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake 연결
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

cursor = conn.cursor()

# SALE_AMT 데이터 확인
query = """
SELECT 
    PRDT_CD,
    ITEM,
    AC_SALE_NML_SALE_AMT_CNS,
    AC_SALE_RET_SALE_AMT_CNS,
    (AC_SALE_NML_SALE_AMT_CNS + AC_SALE_RET_SALE_AMT_CNS) AS SALE_AMT,
    AC_SALE_NML_TAG_AMT_CNS,
    AC_SALE_RET_TAG_AMT_CNS,
    (AC_SALE_NML_TAG_AMT_CNS + AC_SALE_RET_TAG_AMT_CNS) AS SALE_TAG
FROM FNF.PRCS.DW_SCS_DACUM
WHERE SESN = '25F'
  AND BRD_CD = 'M'
  AND DATEADD(DAY, -1, CURRENT_DATE()) BETWEEN START_DT AND END_DT
LIMIT 10
"""

cursor.execute(query)
rows = cursor.fetchall()

print("=== SALE_AMT 데이터 확인 ===")
print(f"총 {len(rows)}건")
print()

for row in rows:
    prdt_cd, item, nml_amt, ret_amt, sale_amt, nml_tag, ret_tag, sale_tag = row
    print(f"상품코드: {prdt_cd}, 아이템: {item}")
    print(f"  정상판매액: {nml_amt:,}")
    print(f"  반품판매액: {ret_amt:,}")
    print(f"  실판매액(SALE_AMT): {sale_amt:,}")
    print(f"  TAG매출(SALE_TAG): {sale_tag:,}")
    if sale_amt and sale_tag:
        discount_rate = (1 - (sale_tag / sale_amt)) * 100
        print(f"  할인율: {discount_rate:.1f}%")
    print()

cursor.close()
conn.close()









