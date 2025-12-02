import json
import os
import glob
from typing import Optional, Tuple


def find_latest_metadata() -> Optional[str]:
    """
    raw\\YYYYMM\\current_year\\YYYYMMDD\\metadata.json 구조에서
    가장 최근 YYYYMM / YYYYMMDD 조합의 metadata.json 경로를 찾는다.
    """
    # 예: raw\202511\current_year\20251201\metadata.json
    patterns = glob.glob(os.path.join("raw", "*", "current_year", "*", "metadata.json"))
    candidates = []

    for path in patterns:
        # path: raw\YYYYMM\current_year\YYYYMMDD\metadata.json
        parts = path.replace("/", os.sep).split(os.sep)
        # 기대: ["raw", "YYYYMM", "current_year", "YYYYMMDD", "metadata.json"]
        if len(parts) < 5:
            continue

        _, ym, cur, d, filename = parts[-5:]

        if cur != "current_year":
            continue
        if not (ym.isdigit() and len(ym) == 6):
            continue
        if not (d.isdigit() and len(d) == 8):
            continue

        candidates.append((ym, d, path))

    if not candidates:
        return None

    # (YYYYMM, YYYYMMDD) 기준으로 가장 큰 것 선택
    candidates.sort()
    _, _, latest_path = candidates[-1]
    return latest_path


def read_date_and_month(path: str) -> Tuple[str, str]:
    """
    metadata.json에서 update_date, analysis_month를 읽어온다.
    없으면 폴더명/앞 6자리로 보정.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 폴더명에서 날짜 추출: raw\YYYYMM\current_year\YYYYMMDD\metadata.json
    parts = path.replace("/", os.sep).split(os.sep)
    d = parts[-2] if len(parts) >= 2 else ""
    if not (d.isdigit() and len(d) == 8):
        d = ""

    date_str = data.get("update_date") or d
    year_month = data.get("analysis_month") or (date_str[:6] if len(date_str) >= 6 else "")

    return date_str, year_month


def main() -> int:
    import sys
    
    latest = find_latest_metadata()
    if not latest:
        # 배치 파일에서 이 경우를 감지해서 에러 처리
        return 1

    date_str, year_month = read_date_and_month(latest)
    if not date_str:
        return 1

    # 인자에 따라 date 또는 month 출력
    if len(sys.argv) > 1 and sys.argv[1] == "month":
        print(year_month)
    else:
        print(date_str)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


