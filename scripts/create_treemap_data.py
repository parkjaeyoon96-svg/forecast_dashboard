"""
채널별, 아이템별 트리맵 데이터 + 할인율 메트릭 + 모든 대시보드 데이터 생성 스크립트

생성물 (public/):
- treemap_data_YYYYMMDD.js   (브랜드→채널→아이템 실판매액)
- data_YYYYMMDD.js           (모든 대시보드 데이터: KPI, PL 테이블, 브랜드별 집계 등)

할인율 정의:
- 할인율 = 1 - (실판매액 합 / TAG매출 합)
"""

import os
import json
import pandas as pd
from datetime import datetime

# 경로
ROOT = os.path.dirname(os.path.dirname(__file__))
INPUT_DIR = os.path.join(ROOT, "raw")
OUTPUT_DIR = os.path.join(ROOT, "public")

# 브랜드 코드 → 이름
BRAND_MAPPING = {
    'M': 'MLB',
    'I': 'MLB KIDS',
    'X': 'DISCOVERY',
    'V': 'DUVETICA',
    'ST': 'SERGIO',
    'W': 'SUPRA'
}

def find_latest_processed_file(date_str: str = None) -> str:
    """
    전처리 완료 파일 찾기 (새 구조: raw/YYYYMM/present/YYYYMMDD/)
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열 (예: "20251117")
                  None이면 최신 파일 반환
    """
    if not os.path.exists(INPUT_DIR):
        raise FileNotFoundError(f"[ERROR] 폴더 없음: {INPUT_DIR}")
    
    # 날짜가 지정된 경우 해당 폴더에서 찾기
    if date_str:
        from path_utils import get_current_year_dir
        date_folder = get_current_year_dir(date_str)
        
        if not os.path.exists(date_folder):
            raise FileNotFoundError(f"[ERROR] 날짜 폴더 없음: {date_folder}")
        
        files = []
        for fn in os.listdir(date_folder):
            if fn.endswith("_전처리완료.csv"):
                path = os.path.join(date_folder, fn)
                files.append((path, os.path.getmtime(path)))
        
        if not files:
            raise FileNotFoundError(f"[ERROR] 전처리 완료 CSV가 없습니다: {date_folder}")
        
        files.sort(key=lambda x: x[1], reverse=True)
        print(f"[OK] 전처리 파일 ({date_str}): {os.path.basename(files[0][0])}")
        return files[0][0]
    
    # 날짜가 지정되지 않은 경우: 모든 월별 폴더의 present/YYYYMMDD/에서 최신 파일 찾기
    files = []
    for month_item in os.listdir(INPUT_DIR):
        month_path = os.path.join(INPUT_DIR, month_item)
        if not os.path.isdir(month_path) or not month_item.isdigit() or len(month_item) != 6:
            continue
        
        # YYYYMM 형식의 월별 폴더
        present_dir = os.path.join(month_path, "present")
        if not os.path.exists(present_dir):
            continue
        
        # present 폴더 내의 날짜별 폴더 찾기
        for date_item in os.listdir(present_dir):
            date_folder = os.path.join(present_dir, date_item)
            if os.path.isdir(date_folder) and date_item.isdigit() and len(date_item) == 8:
                for fn in os.listdir(date_folder):
                    if fn.endswith("_전처리완료.csv"):
                        path = os.path.join(date_folder, fn)
                        files.append((path, os.path.getmtime(path)))
        
        # 기존 방식 호환: 루트에 직접 있는 파일도 확인
        for fn in os.listdir(month_path):
            if fn.endswith("_전처리완료.csv"):
                path = os.path.join(month_path, fn)
                files.append((path, os.path.getmtime(path)))
    
    # 기존 구조 호환: raw/YYYYMMDD/ 형식도 확인
    for item in os.listdir(INPUT_DIR):
        item_path = os.path.join(INPUT_DIR, item)
        if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
            # 날짜 폴더 (YYYYMMDD 형식) - 기존 구조
            for fn in os.listdir(item_path):
        if fn.endswith("_전처리완료.csv"):
                    path = os.path.join(item_path, fn)
            files.append((path, os.path.getmtime(path)))
        elif os.path.isfile(item_path) and item.endswith("_전처리완료.csv"):
            # 루트 폴더에 직접 있는 파일 (기존 방식 호환)
            files.append((item_path, os.path.getmtime(item_path)))
    
    if not files:
        raise FileNotFoundError(f"[ERROR] 전처리 완료 CSV가 없습니다: {INPUT_DIR}")
    
    files.sort(key=lambda x: x[1], reverse=True)
    print(f"[OK] 전처리 파일: {os.path.basename(files[0][0])}")
    return files[0][0]

def load_processed(path: str) -> pd.DataFrame:
    print(f"[READ] {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"   - {len(df)} rows, {len(df.columns)} cols")
    return df

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # 부분 매칭
    for col in df.columns:
        if "TAG" in col and "판매" in col:
            return col
    return None

def aggregate_all(df: pd.DataFrame):
    # 필요한 컬럼
    brand_col = '브랜드'
    channel_col = '채널명'
    item_col = '아이템_중분류'
    sales_col = pick_col(df, ['합계 : 실판매액', '실판매액'])
    tag_col = pick_col(df, ['합계 : 판매금액(TAG가)', '판매금액(TAG가)'])
    cogs_col = pick_col(df, ['합계 : 2. 매출원가 Actual', '매출원가', '합계 : 표준 매출원가'])
    
    if not sales_col or not tag_col:
        raise ValueError("[ERROR] 실판매액/판매금액(TAG가) 컬럼을 찾을 수 없습니다.")

    # 유효 데이터 (매출원가 포함)
    base_cols = [brand_col, channel_col, item_col, sales_col, tag_col]
    if cogs_col:
        base_cols.append(cogs_col)
    
    base = df[base_cols].copy()
    for c in [sales_col, tag_col]:
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0)
    if cogs_col:
        base[cogs_col] = pd.to_numeric(base[cogs_col], errors="coerce").fillna(0)

    # 1) treemap: 브랜드→채널→아이템 (실판매액)
    treemap_sum = base.groupby([brand_col, channel_col, item_col], as_index=False)[sales_col].sum()

    # 2) 채널 메트릭 (브랜드, 채널)
    ch_sum = base.groupby([brand_col, channel_col], as_index=False)[[sales_col, tag_col]].sum()
    if cogs_col:
        ch_cogs = base.groupby([brand_col, channel_col], as_index=False)[cogs_col].sum()
        ch_sum = ch_sum.merge(ch_cogs, on=[brand_col, channel_col], how='left')
    ch_sum.rename(columns={sales_col: '실판매액', tag_col: 'TAG매출'}, inplace=True)
    if cogs_col:
        ch_sum.rename(columns={cogs_col: '매출원가'}, inplace=True)
        ch_sum['매출원가'] = ch_sum['매출원가'].fillna(0)
    ch_sum['할인율'] = ch_sum.apply(lambda r: (1 - (r['실판매액']/r['TAG매출'])) if r['TAG매출'] > 0 else 0.0, axis=1)
    if cogs_col:
        ch_sum['매출총이익'] = ch_sum['실판매액'] - ch_sum['매출원가']
        ch_sum['매출총이익율'] = ch_sum.apply(lambda r: (r['매출총이익']/r['실판매액']*100) if r['실판매액'] > 0 else 0.0, axis=1)

    # 3) 채널-아이템 메트릭 (브랜드, 채널, 아이템)
    chi_sum = base.groupby([brand_col, channel_col, item_col], as_index=False)[[sales_col, tag_col]].sum()
    chi_sum.rename(columns={sales_col: '실판매액', tag_col: 'TAG매출'}, inplace=True)
    chi_sum['할인율'] = chi_sum.apply(lambda r: (1 - (r['실판매액']/r['TAG매출'])) if r['TAG매출'] > 0 else 0.0, axis=1)

    # 4) 아이템 메트릭 (브랜드, 아이템)
    it_sum = base.groupby([brand_col, item_col], as_index=False)[[sales_col, tag_col]].sum()
    it_sum.rename(columns={sales_col: '실판매액', tag_col: 'TAG매출'}, inplace=True)
    it_sum['할인율'] = it_sum.apply(lambda r: (1 - (r['실판매액']/r['TAG매출'])) if r['TAG매출'] > 0 else 0.0, axis=1)

    # 5) 아이템-채널 메트릭 (브랜드, 아이템, 채널)
    itc_sum = base.groupby([brand_col, item_col, channel_col], as_index=False)[[sales_col, tag_col]].sum()
    itc_sum.rename(columns={sales_col: '실판매액', tag_col: 'TAG매출'}, inplace=True)
    itc_sum['할인율'] = itc_sum.apply(lambda r: (1 - (r['실판매액']/r['TAG매출'])) if r['TAG매출'] > 0 else 0.0, axis=1)

    # 6) 브랜드별 집계 (KPI용)
    brand_sum = base.groupby([brand_col], as_index=False)[[sales_col, tag_col]].sum()
    if cogs_col:
        brand_cogs = base.groupby([brand_col], as_index=False)[cogs_col].sum()
        brand_sum = brand_sum.merge(brand_cogs, on=[brand_col], how='left')
    brand_sum.rename(columns={sales_col: '실판매액', tag_col: 'TAG매출'}, inplace=True)
    if cogs_col:
        brand_sum.rename(columns={cogs_col: '매출원가'}, inplace=True)
        brand_sum['매출원가'] = brand_sum['매출원가'].fillna(0)
        brand_sum['매출총이익'] = brand_sum['실판매액'] - brand_sum['매출원가']
        brand_sum['매출총이익율'] = brand_sum.apply(lambda r: (r['매출총이익']/r['실판매액']*100) if r['실판매액'] > 0 else 0.0, axis=1)

    # 7) 채널별 PL 테이블 (브랜드, 채널별 손익) - ch_sum과 동일하지만 별도로 유지
    channel_pl_df = ch_sum.copy()
    channel_pl_df['할인율'] = channel_pl_df.apply(lambda r: (1 - (r['실판매액']/r['TAG매출'])) if r['TAG매출'] > 0 else 0.0, axis=1)

    return treemap_sum, ch_sum, chi_sum, it_sum, itc_sum, brand_sum, channel_pl_df

def to_nested_treemap(df: pd.DataFrame) -> dict:
    out: dict = {}
    for b in df['브랜드'].unique():
        out[b] = {}
        df_b = df[df['브랜드'] == b]
        for c in df_b['채널명'].unique():
            out[b][c] = {}
            df_bc = df_b[df_b['채널명'] == c]
            for _, row in df_bc.iterrows():
                out[b][c][row['아이템_중분류']] = int(row.iloc[-1])  # 실판매액
    return out

def to_nested_metrics_channel(df: pd.DataFrame) -> dict:
    out = {}
    for b in df['브랜드'].unique():
        out[b] = {}
        df_b = df[df['브랜드'] == b]
        for _, row in df_b.iterrows():
            ch = str(row['채널명'])
            out[b][ch] = {
                'sales': int(row['실판매액']),
                'tag': int(row['TAG매출']),
                'discountRate': float(row['할인율'])
            }
    return out

def to_nested_metrics_channel_item(df: pd.DataFrame) -> dict:
    out = {}
    for b in df['브랜드'].unique():
        out[b] = {}
        df_b = df[df['브랜드'] == b]
        for c in df_b['채널명'].unique():
            out[b][c] = {}
            df_bc = df_b[df_b['채널명'] == c]
            for _, row in df_bc.iterrows():
                item = str(row['아이템_중분류'])
                out[b][c][item] = {
                    'sales': int(row['실판매액']),
                    'tag': int(row['TAG매출']),
                    'discountRate': float(row['할인율'])
                }
    return out

def to_nested_metrics_item(df: pd.DataFrame) -> dict:
    out = {}
    for b in df['브랜드'].unique():
        out[b] = {}
        df_b = df[df['브랜드'] == b]
        for _, row in df_b.iterrows():
            item = str(row['아이템_중분류'])
            out[b][item] = {
                'sales': int(row['실판매액']),
                'tag': int(row['TAG매출']),
                'discountRate': float(row['할인율'])
            }
    return out

def to_nested_metrics_item_channel(df: pd.DataFrame) -> dict:
    out = {}
    for b in df['브랜드'].unique():
        out[b] = {}
        df_b = df[df['브랜드'] == b]
        for i in df_b['아이템_중분류'].unique():
            out[b][i] = {}
            df_bi = df_b[df_b['아이템_중분류'] == i]
            for _, row in df_bi.iterrows():
                ch = str(row['채널명'])
                out[b][i][ch] = {
                    'sales': int(row['실판매액']),
                    'tag': int(row['TAG매출']),
                    'discountRate': float(row['할인율'])
                }
    return out

def save_treemap_js(treemap_data: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"// 트리맵 데이터\n// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("const brandNames = ")
        f.write(json.dumps(BRAND_MAPPING, ensure_ascii=False, indent=2))
        f.write(";\n\n")
        f.write("const channelItemSalesData = ")
        f.write(json.dumps(treemap_data, ensure_ascii=False, indent=2))
        f.write(";\n\n")
        f.write("if (typeof window !== 'undefined') { window.brandNames = brandNames; window.channelItemSalesData = channelItemSalesData; }\n")
        f.write("if (typeof module !== 'undefined' && module.exports) { module.exports = { brandNames, channelItemSalesData }; }\n")
        f.write("console.log('[Treemap Data] Ready');\n")
    print(f"[OK] treemap_data.js 저장: {out_path}")

def to_brand_kpi(brand_sum: pd.DataFrame) -> dict:
    """브랜드별 KPI 데이터 변환"""
    out = {}
    for _, row in brand_sum.iterrows():
        brand = str(row['브랜드'])
        out[brand] = {
            'revenue': int(row['실판매액']),
            'tag': int(row['TAG매출']),
            'cogs': int(row.get('매출원가', 0)),
            'grossProfit': int(row.get('매출총이익', row['실판매액'])),
            'grossProfitRate': float(row.get('매출총이익율', 0))
        }
    return out

def to_channel_pl(channel_pl_df: pd.DataFrame) -> dict:
    """채널별 PL 테이블 데이터 변환"""
    out = {}
    for _, row in channel_pl_df.iterrows():
        brand = str(row['브랜드'])
        channel = str(row['채널명'])
        if brand not in out:
            out[brand] = {}
        out[brand][channel] = {
            'revenue': int(row['실판매액']),
            'tag': int(row['TAG매출']),
            'cogs': int(row.get('매출원가', 0)),
            'grossProfit': int(row.get('매출총이익', row['실판매액'])),
            'grossProfitRate': float(row.get('매출총이익율', 0)),
            'discountRate': float(row.get('할인율', 0))
        }
    return out

def save_data_js(treemap_data: dict, metrics: dict, brand_kpi: dict, channel_pl: dict, out_path: str):
    """
    window 전역만 갱신하도록 즉시 실행(IIFE) 형태로 기록 (재선언 충돌 방지)
    metrics: dict with keys: channelMetrics, channelItemMetrics, itemMetrics, itemChannelMetrics
    brand_kpi: 브랜드별 KPI 데이터
    channel_pl: 채널별 PL 테이블 데이터
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("// 모든 대시보드 데이터 (window 전역 할당 전용)\n")
        f.write(f"// 자동 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("(function(){\n")
        f.write("  var brandNames = ")
        f.write(json.dumps(BRAND_MAPPING, ensure_ascii=False, indent=2))
        f.write(";\n")
        f.write("  var channelItemSalesData = ")
        f.write(json.dumps(treemap_data, ensure_ascii=False, indent=2))
        f.write(";\n")
        for key in ["channelMetrics", "channelItemMetrics", "itemMetrics", "itemChannelMetrics"]:
            f.write(f"  var {key} = ")
            f.write(json.dumps(metrics.get(key, {}), ensure_ascii=False, indent=2))
            f.write(";\n")
        f.write("  var brandKPI = ")
        f.write(json.dumps(brand_kpi, ensure_ascii=False, indent=2))
        f.write(";\n")
        f.write("  var channelPL = ")
        f.write(json.dumps(channel_pl, ensure_ascii=False, indent=2))
            f.write(";\n")
        f.write("  if (typeof window !== 'undefined') {\n")
        f.write("    window.brandNames = brandNames;\n")
        f.write("    window.channelItemSalesData = channelItemSalesData;\n")
        f.write("    window.channelMetrics = channelMetrics;\n")
        f.write("    window.channelItemMetrics = channelItemMetrics;\n")
        f.write("    window.itemMetrics = itemMetrics;\n")
        f.write("    window.itemChannelMetrics = itemChannelMetrics;\n")
        f.write("    window.brandKPI = brandKPI;\n")
        f.write("    window.channelPL = channelPL;\n")
        f.write("  }\n")
        f.write("  console.log('[Data.js] 모든 대시보드 데이터 로드 완료');\n")
        f.write("})();\n")
    print(f"[OK] data.js 저장: {out_path}")

def main(date_str: str = None):
    """
    메인 함수
    
    Args:
        date_str: YYYYMMDD 형식의 날짜 문자열 (예: "20251117")
                  None이면 최신 파일 사용
    """
    print("="*60)
    print("트리맵 데이터 + 할인율 메트릭 생성")
    print("="*60)
    if date_str:
        print(f"날짜 지정: {date_str}")
    path = find_latest_processed_file(date_str)
    df = load_processed(path)

    treemap_sum, ch_sum, chi_sum, it_sum, itc_sum, brand_sum, channel_pl_df = aggregate_all(df)

    treemap_data = to_nested_treemap(treemap_sum)
    channel_metrics = to_nested_metrics_channel(ch_sum)
    channel_item_metrics = to_nested_metrics_channel_item(chi_sum)
    item_metrics = to_nested_metrics_item(it_sum)
    item_channel_metrics = to_nested_metrics_item_channel(itc_sum)
    brand_kpi = to_brand_kpi(brand_sum)
    channel_pl_data = to_channel_pl(channel_pl_df)

    # 날짜별 파일명 생성
    if date_str:
        treemap_js_path = os.path.join(OUTPUT_DIR, f"treemap_data_{date_str}.js")
        data_js_path = os.path.join(OUTPUT_DIR, f"data_{date_str}.js")
    else:
    treemap_js_path = os.path.join(OUTPUT_DIR, "treemap_data.js")
    data_js_path = os.path.join(OUTPUT_DIR, "data.js")

    save_treemap_js(treemap_data, treemap_js_path)
    save_data_js(
        treemap_data,
        {
            "channelMetrics": channel_metrics,
            "channelItemMetrics": channel_item_metrics,
            "itemMetrics": item_metrics,
            "itemChannelMetrics": item_channel_metrics
        },
        brand_kpi,
        channel_pl_data,
        data_js_path
    )

    print("[COMPLETE] done")

if __name__ == "__main__":
    import sys
    date_str = None
    if len(sys.argv) > 1:
        date_str = sys.argv[1]  # 명령줄 인자로 날짜 전달
    main(date_str)



