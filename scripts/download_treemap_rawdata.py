"""
트리맵 원본 데이터 다운로드 (스노우플레이크)
================================================

작성일: 2025-01
"""

import os
import pandas as pd
from datetime import datetime
from snowflake_connection import get_snowflake_connection
from path_utils import get_current_year_file_path, extract_year_month_from_date

ROOT = os.path.dirname(os.path.dirname(__file__))

def get_treemap_query(start_date: str, end_date: str) -> str:
    """
    트리맵 데이터 쿼리 생성
    
    Args:
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
    
    Returns:
        str: SQL 쿼리
    """
    query = f"""
select
    brd_cd AS "브랜드코드",
 
    /* 🔹 시즌 */
    case 
        when brd_cd = 'ST' then substr(prdt_cd, 3, 3)
        else substr(prdt_cd, 2, 3)
    end AS "시즌",
 
    chnl_cd AS "채널코드",
    cust_cd AS "고객코드",
    prdt_hrrc_cd1,
    prdt_hrrc_cd2,
    prdt_hrrc_cd3,
 
    /* 🔹 아이템코드 */
    case
        when brd_cd = 'ST' then substr(prdt_cd, 8, 2)
        else substr(prdt_cd, 7, 2)
    end AS "아이템코드",
 
    sum(tag_sale_amt) AS "TAG매출",
    sum(act_sale_amt) AS "실판매출"
from fnf.sap_fnf.dw_copa_d
where pst_dt between '{start_date}' and '{end_date}'
  and corp_cd = '1000'
  and brd_cd <> 'A'
  and chnl_cd <> '9'
  and prdt_hrrc_cd1 <> 'E0100'
group by
    brd_cd,
    case 
        when brd_cd = 'ST' then substr(prdt_cd, 3, 3)
        else substr(prdt_cd, 2, 3)
    end,
    chnl_cd,
    cust_cd,
    prdt_hrrc_cd1,
    prdt_hrrc_cd2,
    prdt_hrrc_cd3,
    case
        when brd_cd = 'ST' then substr(prdt_cd, 8, 2)
        else substr(prdt_cd, 7, 2)
    end
"""
    return query

def download_treemap_data(start_date: str, end_date: str, output_path: str):
    """
    트리맵 데이터 다운로드
    
    Args:
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        output_path: 저장 경로
    """
    print(f"[다운로드] 트리맵 원본 데이터")
    print(f"  기간: {start_date} ~ {end_date}")
    
    # 스노우플레이크 연결
    conn = get_snowflake_connection()
    
    try:
        # 쿼리 실행
        query = get_treemap_query(start_date, end_date)
        df = pd.read_sql(query, conn)
        
        print(f"  데이터: {len(df)}행 × {len(df.columns)}열")
        
        # 저장
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"[저장] {output_path}")
        
        return df
        
    finally:
        conn.close()

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="트리맵 원본 데이터 다운로드")
    parser.add_argument("start_date", help="시작일 (YYYY-MM-DD)")
    parser.add_argument("end_date", help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--output", help="출력 파일 경로")
    
    args = parser.parse_args()
    
    # 출력 경로 설정
    if args.output:
        output_path = args.output
    else:
        # 종료일 기준으로 경로 생성
        date_str = args.end_date.replace('-', '')
        year_month = extract_year_month_from_date(date_str)
        filename = f"treemap_raw_{date_str}.csv"
        output_path = get_current_year_file_path(date_str, filename)
    
    # 다운로드
    download_treemap_data(args.start_date, args.end_date, output_path)
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())



