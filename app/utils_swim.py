import re
from typing import Optional

def convert_to_seconds(result: str) -> float:
    """把 '1:33.50' 或 '93.5' 轉成秒數(float)。不合法回 0.0"""
    if not result:
        return 0.0
    s = result.strip()
    try:
        if ":" in s:
            mm, ss = s.split(":", 1)
            return float(mm) * 60.0 + float(ss)
        return float(s)
    except Exception:
        return 0.0

_MEET_REPLACEMENTS = [
    (re.compile(r"^\d{4}\s*"), ""),
    (re.compile(r"^\d{3}\s*"), ""),
    (re.compile(r"^.*?年"), ""),
    (re.compile(r"\(游泳項目\)"), ""),
]

_MEET_MAP = {
    "臺中市114年市長盃水上運動競賽(游泳項目)": "台中市長盃",
    "全國冬季短水道游泳錦標賽": "全國冬短",
    "全國總統盃暨美津濃游泳錦標賽": "全國總統盃",
    "全國總統盃暨美津濃分齡游泳錦標賽": "全國總統盃",
    "冬季短水道": "冬短",
    "全國運動會臺南市游泳代表隊選拔賽": "台南全運會選拔",
    "全國青少年游泳錦標賽": "全國青少",
    "臺中市議長盃": "台中議長盃",
    "臺中市市長盃": "台中市長盃",
    "春季游泳錦標賽": "春長",
    "全國E世代青少年": "E世代",
    "臺南市市長盃短水道": "台南市長盃",
    "臺南市中小學": "台南中小學",
    "臺南市委員盃": "台南委員盃",
    "臺南市全國運動會游泳選拔賽": "台南全運會選拔",
    "游泳錦標賽": "",
}

def simplify_category(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    for k, v in _MEET_MAP.items():
        if k in s:
            s = s.replace(k, v)
    for pat, repl in _MEET_REPLACEMENTS:
        s = pat.sub(repl, s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def normalize_distance_item(item: str) -> str:
    if not item:
        return ""
    m = re.search(r"(\d{2,3}公尺(?:自由式|蛙式|仰式|蝶式|混合式))", item)
    return m.group(1) if m else item

WA_BASE = {"F": {}, "M": {}}

def calc_wa(seconds: float, event: str, gender: str) -> Optional[int]:
    if not seconds or seconds <= 0:
        return None
    base = WA_BASE.get(gender, {}).get(event)
    if not base:
        return None
    pts = 1000.0 * (base / float(seconds)) ** 3
    return int(round(pts))
