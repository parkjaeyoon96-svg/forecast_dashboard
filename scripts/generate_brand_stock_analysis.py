# -*- coding: utf-8 -*-
"""
당시즌의류/ACC 재고주수 분석 데이터 처리 및 JSON 파일 생성
- 당시즌의류: raw/202511/ETC/당시즌의류_브랜드별현황_YYYYMMDD.csv
- ACC판매율 분석: raw/202511/ETC/ACC_재고주수분석_YYYYMMDD.csv

출력: public/brand_stock_analysis_YYYYMMDD.js
"""

import pandas as pd
import json
import os
import re
from datetime import datetime, timedelta


def safe_float(value):
    """안전하게 float 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value):
    """안전하게 int 변환 (빈값, NaN 처리)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_percentage(value):
    """퍼센트 문자열을 처리 (예: "169%" -> "169%", 빈값 -> None)"""
    if pd.isna(value) or value == '' or value is None:
        return None
    value_str = str(value).strip()
    if value_str == '':
        return None
    # 숫자만 있는 경우 퍼센트로 변환
    try:
        num = float(value_str.replace('%', ''))
        return f"{int(num)}%"
    except (ValueError, TypeError):
        return value_str


def process_clothing_csv(file_path):
    """
    당시즌의류 브랜드별 현황 CSV 처리
    
    CSV 컬럼:
    - 브랜드, 대분류, 중분류, 아이템코드, 아이템명(한글), 발주(TAG), 전년비(발주), 
      주간판매매출(TAG), 전년비(주간), 누적판매매출(TAG), 전년비(누적), 
      누적판매율당년, 누적판매율차이, 전년마감판매율
    """
    print(f"[당시즌의류] 파일 읽는 중: {file_path}")
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"[당시즌의류] 총 {len(df)}개 행 로드됨")
    print(f"[당시즌의류] 컬럼: {list(df.columns)}")
    
    # 브랜드별로 데이터 그룹화
    result = {}
    
    for _, row in df.iterrows():
        brand = str(row['브랜드']).strip()
        
        if brand not in result:
            result[brand] = []
        
        item_data = {
            "category": str(row['대분류']).strip() if pd.notna(row['대분류']) else "",
            "subCategory": str(row['중분류']).strip() if pd.notna(row['중분류']) else "",
            "itemCode": str(row['아이템코드']).strip() if pd.notna(row['아이템코드']) else "",
            "itemName": str(row['아이템명(한글)']).strip() if pd.notna(row['아이템명(한글)']) else "",
            "orderTag": safe_float(row['발주(TAG)']),
            "orderYoY": safe_float(row['전년비(발주)']),
            "weeklySalesTag": safe_float(row['주간판매매출(TAG)']),
            "weeklyYoY": safe_float(row['전년비(주간)']),
            "cumSalesTag": safe_float(row['누적판매매출(TAG)']),
            "cumYoY": safe_float(row['전년비(누적)']),
            "cumSalesRate": safe_float(row['누적판매율당년']),
            "cumSalesRateDiff": safe_float(row['누적판매율차이']),
            "pyClosingSalesRate": safe_float(row['전년마감판매율'])
        }
        
        result[brand].append(item_data)
    
    print(f"[당시즌의류] 브랜드 수: {len(result)}")
    for brand, items in result.items():
        print(f"  - {brand}: {len(items)}개 아이템")
    
    return result


def process_acc_csv(file_path):
    """
    ACC 재고주수 분석 CSV 처리
    
    CSV 컬럼:
    - 브랜드코드, 카테고리, 아이템, 아이템명, 판매수량, 판매매출, 전년비, 비중, 
      4주평균판매량, 재고, 재고주수, 전년재고주수, 재고주수차이(당년-전년)
    """
    print(f"[ACC 재고주수] 파일 읽는 중: {file_path}")
    
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"[ACC 재고주수] 총 {len(df)}개 행 로드됨")
    print(f"[ACC 재고주수] 컬럼: {list(df.columns)}")
    
    # 브랜드별로 데이터 그룹화
    result = {}
    
    for _, row in df.iterrows():
        brand = str(row['브랜드코드']).strip()
        
        if brand not in result:
            result[brand] = []
        
        item_data = {
            "category": str(row['카테고리']).strip() if pd.notna(row['카테고리']) else "",
            "itemCode": str(row['아이템']).strip() if pd.notna(row['아이템']) else "",
            "itemName": str(row['아이템명']).strip() if pd.notna(row['아이템명']) else "",
            "saleQty": safe_int(row['판매수량']),
            "saleAmt": safe_int(row['판매매출']),
            "yoyRate": parse_percentage(row['전년비']),
            "shareRate": str(row['비중']).strip() if pd.notna(row['비중']) else "0%",
            "avg4wSaleQty": safe_float(row['4주평균판매량']),
            "stockQty": safe_int(row['재고']),
            "stockWeeks": safe_float(row['재고주수']),
            "pyStockWeeks": safe_float(row['전년재고주수']),
            "stockWeeksDiff": safe_float(row['재고주수차이(당년-전년)'])
        }
        
        result[brand].append(item_data)
    
    print(f"[ACC 재고주수] 브랜드 수: {len(result)}")
    for brand, items in result.items():
        print(f"  - {brand}: {len(items)}개 아이템")
    
    return result


def generate_js_file(clothing_data, acc_data, update_date, output_path, project_root=None):
    """
    JavaScript 파일 생성
    """
    # project_root가 없으면 output_path에서 추출
    if project_root is None:
        # output_path가 public/brand_stock_analysis_*.js 형식이면
        # public 폴더의 부모가 project_root
        if 'public' in output_path:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(output_path)))
        else:
            # 스크립트 위치에서 추출
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
    
    now = datetime.now()
    
    # 주간 날짜 계산 (월요일 ~ 일요일)
    # update_date 기준으로 전주(업데이트일자 전날까지)의 월요일과 일요일 계산
    # 업데이트일자가 월요일이면, 전주 일요일이 마지막 분석 주의 종료일
    update_dt = datetime.strptime(update_date, '%Y%m%d')
    
    # 당년 주간: 업데이트일 전 주 (월~일)
    cy_week_end = update_dt - timedelta(days=1)  # 업데이트 전날 (일요일)
    cy_week_start = cy_week_end - timedelta(days=6)  # 월요일
    
    # 전년 동주차: 364일 전 (52주 = 364일, 같은 요일)
    py_week_end = cy_week_end - timedelta(days=364)
    py_week_start = cy_week_start - timedelta(days=364)
    
    # 날짜 포맷팅
    cy_week_start = cy_week_start.strftime('%Y-%m-%d')
    cy_week_end = cy_week_end.strftime('%Y-%m-%d')
    py_week_start = py_week_start.strftime('%Y-%m-%d')
    py_week_end = py_week_end.strftime('%Y-%m-%d')
    
    # 현재 시즌 판단 (월 기준)
    current_month = update_dt.month
    if current_month >= 8 or current_month <= 2:
        cy_season = f"{update_dt.year % 100}F" if current_month >= 8 else f"{(update_dt.year - 1) % 100}F"
        py_season = f"{(int(cy_season[:2]) - 1)}F"
    else:
        cy_season = f"{update_dt.year % 100}S"
        py_season = f"{(int(cy_season[:2]) - 1)}S"
    
    # 전년 시즌 마감일 (F/W는 다음해 2월 말)
    if 'F' in cy_season:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year + 1}-02-28"
    else:
        season_year = 2000 + int(cy_season[:2])
        py_season_end = f"{season_year}-07-31"
    
    metadata = {
        "updateDate": f"{update_date[:4]}-{update_date[4:6]}-{update_date[6:8]}",
        "cyWeekStart": cy_week_start,
        "cyWeekEnd": cy_week_end,
        "pyWeekStart": py_week_start,
        "pyWeekEnd": py_week_end,
        "cySeason": cy_season,
        "pySeason": py_season,
        "pySeasonEnd": py_season_end,
        "generatedAt": now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # JavaScript 파일 내용 생성
    js_content = f'''// 브랜드별 현황 - 당시즌의류/ACC 재고주수 분석 데이터
// 자동 생성 일시: {now.strftime('%Y-%m-%d %H:%M:%S')}
// 업데이트 일자: {update_date[:4]}-{update_date[4:6]}-{update_date[6:8]}
// 당년 주간: {cy_week_start} ~ {cy_week_end}
// 전년 동주차: {py_week_start} ~ {py_week_end}

(function() {{
  // 메타데이터
  var brandStockMetadata = {json.dumps(metadata, ensure_ascii=False, indent=2)};
  
  // 당시즌의류 브랜드별 현황 (ACC 제외)
  var clothingBrandStatus = {json.dumps(clothing_data, ensure_ascii=False, indent=2)};
  
  // ACC 재고주수 분석
  var accStockAnalysis = {json.dumps(acc_data, ensure_ascii=False, indent=2)};
  
  // 브랜드별 요약 통계 (당시즌의류)
  var clothingSummary = {{}};
  for (var brand in clothingBrandStatus) {{
    var items = clothingBrandStatus[brand];
    clothingSummary[brand] = {{
      itemCount: items.length,
      totalOrderTag: items.reduce(function(sum, item) {{ return sum + (item.orderTag || 0); }}, 0),
      totalWeeklySales: items.reduce(function(sum, item) {{ return sum + (item.weeklySalesTag || 0); }}, 0),
      totalCumSales: items.reduce(function(sum, item) {{ return sum + (item.cumSalesTag || 0); }}, 0)
    }};
  }}
  
  // 브랜드별 요약 통계 (ACC)
  var accSummary = {{}};
  for (var brand in accStockAnalysis) {{
    var items = accStockAnalysis[brand];
    accSummary[brand] = {{
      itemCount: items.length,
      totalSaleQty: items.reduce(function(sum, item) {{ return sum + (item.saleQty || 0); }}, 0),
      totalSaleAmt: items.reduce(function(sum, item) {{ return sum + (item.saleAmt || 0); }}, 0),
      totalStockQty: items.reduce(function(sum, item) {{ return sum + (item.stockQty || 0); }}, 0)
    }};
  }}
  
  // 전역 객체에 할당
  if (typeof window !== 'undefined') {{
    window.brandStockMetadata = brandStockMetadata;
    window.clothingBrandStatus = clothingBrandStatus;
    window.accStockAnalysis = accStockAnalysis;
    window.clothingSummary = clothingSummary;
    window.accSummary = accSummary;
  }}
}})();
'''
    
    # 파일 저장
    print(f"\n[출력] JavaScript 파일 저장 중: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    print(f"[출력] 파일 저장 완료!")
    
    # JSON 파일도 별도로 저장 (다른 데이터와 동일한 경로: /public/data/{dateStr}/stock_analysis.json)
    json_dir = os.path.join(project_root, 'public', 'data', update_date)
    os.makedirs(json_dir, exist_ok=True)
    json_output_path = os.path.join(json_dir, 'stock_analysis.json')
    
    # 브랜드별 요약 통계 계산 (당시즌의류)
    clothing_summary = {}
    for brand, items in clothing_data.items():
        clothing_summary[brand] = {
            "itemCount": len(items),
            "totalOrderTag": sum(item.get("orderTag", 0) or 0 for item in items),
            "totalWeeklySales": sum(item.get("weeklySalesTag", 0) or 0 for item in items),
            "totalCumSales": sum(item.get("cumSalesTag", 0) or 0 for item in items)
        }
    
    # 브랜드별 요약 통계 계산 (ACC)
    acc_summary = {}
    for brand, items in acc_data.items():
        acc_summary[brand] = {
            "itemCount": len(items),
            "totalSaleQty": sum(item.get("saleQty", 0) or 0 for item in items),
            "totalSaleAmt": sum(item.get("saleAmt", 0) or 0 for item in items),
            "totalStockQty": sum(item.get("stockQty", 0) or 0 for item in items)
        }
    
    # Dashboard.html에서 기대하는 구조: brandStockMetadata, clothingBrandStatus, accStockAnalysis, clothingSummary, accSummary
    json_data = {
        "brandStockMetadata": metadata,
        "clothingBrandStatus": clothing_data,
        "accStockAnalysis": acc_data,
        "clothingSummary": clothing_summary,
        "accSummary": acc_summary
    }
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    print(f"[출력] JSON 파일 저장됨: {json_output_path}")
    
    return metadata


def extract_date_from_filename(filename):
    """
    파일명에서 날짜 추출 (예: 당시즌의류_브랜드별현황_20251124.csv -> 20251124)
    """
    match = re.search(r'(\d{8})', filename)
    if match:
        return match.group(1)
    return None


def main():
    """
    메인 실행 함수
    """
    import sys
    
    try:
        print("=" * 60)
        print("당시즌의류/ACC 재고주수 분석 데이터 처리")
        print("=" * 60)
        
        # 프로젝트 루트 디렉토리
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        
        # 명령줄 인자로 날짜 받기 (선택적)
        update_date = None
        if len(sys.argv) > 1:
            date_arg = sys.argv[1]
            # YYYYMMDD 형식인지 확인
            if re.match(r'^\d{8}$', date_arg):
                update_date = date_arg
                print(f"[인자] 날짜 파라미터: {update_date}")
            else:
                print(f"[경고] 잘못된 날짜 형식: {date_arg} (YYYYMMDD 형식이어야 함)")
        
        # 기본 날짜 설정 (오늘 또는 가장 최근 파일의 날짜)
        default_date = datetime.now().strftime('%Y%m%d')
        
        # raw 폴더에서 날짜 추출
        raw_base = os.path.join(project_root, 'raw')
        
        # 분석월 폴더에서 CSV 파일 찾기 (metadata.json에서 analysis_month 읽기)
        analysis_month = None
        if update_date:
            # metadata.json에서 analysis_month 읽기 시도
            # 여러 가능한 경로 시도
            possible_paths = [
                os.path.join(raw_base, update_date[:6], 'current_year', update_date, 'metadata.json'),
                os.path.join(raw_base, '202511', 'current_year', update_date, 'metadata.json'),  # 20251201은 202511 폴더에 있을 수 있음
            ]
            
            # 모든 year_month 폴더에서 찾기
            if os.path.exists(raw_base):
                for folder in os.listdir(raw_base):
                    if re.match(r'\d{6}', folder):
                        possible_paths.append(os.path.join(raw_base, folder, 'current_year', update_date, 'metadata.json'))
            
            for metadata_path in possible_paths:
                if os.path.exists(metadata_path):
                    try:
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                            analysis_month = metadata.get('analysis_month')
                            if analysis_month:
                                break
                    except:
                        pass
            
            # metadata.json에서 못 찾으면 날짜에서 추출 (YYYYMMDD -> YYYYMM)
            if not analysis_month:
                analysis_month = update_date[:6]
        else:
            # 날짜가 없으면 최신 폴더 찾기
            year_month_folders = []
            if os.path.exists(raw_base):
                for folder in os.listdir(raw_base):
                    if re.match(r'\d{6}', folder):
                        year_month_folders.append(folder)
            if year_month_folders:
                analysis_month = sorted(year_month_folders)[-1]
            else:
                analysis_month = '202511'
        
        etc_folder = os.path.join(raw_base, analysis_month, 'ETC')
        print(f"[설정] 분석월: {analysis_month}")
        print(f"[설정] ETC 폴더 경로: {etc_folder}")
        
        # CSV 파일 찾기
        clothing_csv = None
        acc_csv = None
        found_date = None
        
        if os.path.exists(etc_folder):
            for file in os.listdir(etc_folder):
                if '당시즌의류_브랜드별현황' in file and file.endswith('.csv'):
                    file_date = extract_date_from_filename(file)
                    # 날짜가 지정된 경우 해당 날짜의 파일만 찾기
                    if not update_date or (file_date and file_date == update_date):
                        clothing_csv = os.path.join(etc_folder, file)
                        if file_date:
                            found_date = file_date
                elif 'ACC_재고주수분석' in file and file.endswith('.csv'):
                    file_date = extract_date_from_filename(file)
                    # 날짜가 지정된 경우 해당 날짜의 파일만 찾기
                    if not update_date or (file_date and file_date == update_date):
                        acc_csv = os.path.join(etc_folder, file)
                        if not found_date and file_date:
                            found_date = file_date
        
        if not clothing_csv or not acc_csv:
            print("[오류] CSV 파일을 찾을 수 없습니다.")
            print(f"  - 당시즌의류 CSV: {clothing_csv}")
            print(f"  - ACC 재고주수 CSV: {acc_csv}")
            print(f"  - 검색 경로: {etc_folder}")
            return 1
        
        # 날짜 결정: 인자로 받은 날짜 > CSV 파일에서 추출한 날짜 > 기본값
        if not update_date:
            update_date = found_date if found_date else default_date
        
        print(f"[설정] 업데이트 일자: {update_date}")
        print(f"[설정] 당시즌의류 CSV: {clothing_csv}")
        print(f"[설정] ACC 재고주수 CSV: {acc_csv}")
        print()
        
        # CSV 파일 처리
        clothing_data = process_clothing_csv(clothing_csv)
        print()
        acc_data = process_acc_csv(acc_csv)
        print()
        
        # JavaScript 파일 생성
        output_path = os.path.join(project_root, 'public', f'brand_stock_analysis_{update_date}.js')
        metadata = generate_js_file(clothing_data, acc_data, update_date, output_path, project_root)
        
        print()
        print("=" * 60)
        print("처리 완료!")
        print("=" * 60)
        print(f"[요약] 업데이트 일자: {metadata['updateDate']}")
        print(f"[요약] 당년 주간: {metadata['cyWeekStart']} ~ {metadata['cyWeekEnd']}")
        print(f"[요약] 전년 동주차: {metadata['pyWeekStart']} ~ {metadata['pyWeekEnd']}")
        print(f"[요약] 시즌: 당년 {metadata['cySeason']}, 전년 {metadata['pySeason']}")
        print(f"[요약] 당시즌의류 브랜드 수: {len(clothing_data)}")
        print(f"[요약] ACC 재고주수 브랜드 수: {len(acc_data)}")
        print(f"[출력 파일] {output_path}")
        
        return 0
    except Exception as e:
        import traceback
        print(f"\n[오류] 예외 발생: {e}")
        print(f"[오류] 상세 정보:")
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    exit_code = main()
    sys.exit(exit_code if exit_code is not None else 0)





