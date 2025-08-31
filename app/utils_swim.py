import re
from typing import Optional

def convert_to_seconds(result: str) -> float:
    if not result:
        return 0.0
    parts = result.strip().split(":")
    try:
        if len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        if len(parts) == 1:
            return float(parts[0])
    except ValueError:
        return 0.0
    return 0.0

_re_dist_piece = re.compile(r"[0-9]+公尺\S*")
_re_suffix = re.compile(r"-(計時決賽|預賽|決賽)")

def simplify_category(raw: str) -> str:
    if not raw:
        return ""
    m = _re_dist_piece.search(raw)
    if not m:
        return raw.strip()
    s = m.group(0)
    s = _re_suffix.sub("", s)
    return s

# 完整 WA 基準表
WA_BASE = {
    "男": {
        "50公尺自由式": 20.91,
        "100公尺自由式": 46.40,
        "200公尺自由式": 102.00,
        "400公尺自由式": 220.07,
        "800公尺自由式": 452.12,
        "1500公尺自由式": 870.67,
        "50公尺仰式": 23.55,
        "100公尺仰式": 51.60,
        "200公尺仰式": 111.92,
        "50公尺蛙式": 25.95,
        "100公尺蛙式": 56.88,
        "200公尺蛙式": 125.48,
        "50公尺蝶式": 22.27,
        "100公尺蝶式": 49.45,
        "200公尺蝶式": 110.34,
        "200公尺混合式": 114.00,
        "400公尺混合式": 242.50,
    },
    "女": {
        "50公尺自由式": 23.61,
        "100公尺自由式": 51.71,
        "200公尺自由式": 112.23,
        "400公尺自由式": 235.38,
        "800公尺自由式": 484.79,
        "1500公尺自由式": 920.48,
        "50公尺仰式": 26.86,
        "100公尺仰式": 57.13,
        "200公尺仰式": 123.14,
        "50公尺蛙式": 29.16,
        "100公尺蛙式": 64.13,
        "200公尺蛙式": 137.55,
        "50公尺蝶式": 24.43,
        "100公尺蝶式": 55.18,
        "200公尺蝶式": 121.81,
        "200公尺混合式": 126.12,
        "400公尺混合式": 264.38,
    }
}

def calc_wa(seconds: float, event: str, gender: str) -> Optional[int]:
    if not seconds or seconds <= 0:
        return None
    base = WA_BASE.get(gender, {}).get(event)
    if not base:
        return None
    pts = 1000.0 * (base / float(seconds)) ** 3
    return int(round(pts))
