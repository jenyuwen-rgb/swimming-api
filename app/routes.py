# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re
from .db import SessionLocal

router = APIRouter()
TABLE = "swimming_scores"

# ------------------------ DB session ------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------------------ helpers ------------------------
WINTER_SHORT = "冬季短水道"

def parse_seconds(s: Optional[str]) -> Optional[float]:
    """
    支援 "37.28" 或 "1:15.23" -> 75.23 秒
    非法回傳 None
    """
    if not s:
        return None
    v = s.strip()
    try:
        if ":" in v:
            m, sec = v.split(":")
            return int(m) * 60 + float(sec)
        return float(v)
    except Exception:
        return None

def clean_meet_name(name: Optional[str]) -> str:
    return (name or "").strip()

def year_in_range(y: Optional[str]) -> bool:
    s = (y or "").strip()
    return len(s) == 8 and s.isdigit()

def is_numeric_group(g: Optional[str]) -> bool:
    s = (g or "").strip()
    return bool(s) and s.isdigit()

# ------------------------ health/debug ------------------------
@router.get("/api/health")
def health():
    return {"ok": "true"}

# 供前端（現行）使用：/api/results、/api/pb、/api/summary
# -------------------------------------------------------------

@router.get("/api/results")
def results(
    name: str = Query(..., description="選手姓名（完整）"),
    stroke: str = Query(..., description="項目（exact match；例如 50公尺蛙式）"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    回傳某選手在某項目的明細（依年份 ASC），給『詳細成績』與『成績趨勢』使用。
    """
    sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS item,
               "成績"::text AS r, COALESCE("名次"::text,'') AS rk, COALESCE("組別"::text,'') AS grp
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT :lim OFFSET :ofs
    """
    rows = db.execute(text(sql), {"name": name, "stroke": stroke, "lim": limit, "ofs": cursor}).mappings().all()
    items: List[Dict[str, Any]] = []
    for t in rows:
        sec = parse_seconds(t["r"])
        items.append({
            "年份": t["y"],
            "賽事名稱": clean_meet_name(t["m"]),
            "項目": t["item"],
            "成績": t["r"],
            "名次": t["rk"],
            "組別": t["grp"],
            "seconds": sec,
        })
    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}

@router.get("/api/pb")
def pb(
    name: str = Query(...),
    stroke: str = Query(...),
    year_min: Optional[str] = Query(None, description="YYYYMMDD；若給，PB 僅在此區間內"),
    year_max: Optional[str] = Query(None, description="YYYYMMDD；若給，PB 僅在此區間內"),
    db: Session = Depends(get_db),
):
    """
    取某人的 PB（限定同項目；可選時間區間；排除冬季短水道）
    """
    cond_year = ""
    params = {"name": name, "stroke": stroke}
    if year_min and year_in_range(year_min):
        cond_year += ' AND "年份" >= :ymin'
        params["ymin"] = year_min
    if year_max and year_in_range(year_max):
        cond_year += ' AND "年份" <= :ymax'
        params["ymax"] = year_max

    sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name
          AND "項目" = :stroke
          AND ("賽事名稱" IS NULL OR "賽事名稱" NOT ILIKE :winter)
          {cond_year}
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    params["winter"] = f"%{WINTER_SHORT}%"
    rows = db.execute(text(sql), params).mappings().all()

    best: Optional[Tuple[float, str, str]] = None
    for t in rows:
        sec = parse_seconds(t["r"])
        if sec is None:
            continue
        if (best is None) or (sec < best[0]):
            best = (sec, t["y"], clean_meet_name(t["m"]))
    if not best:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "pb_year": None, "pb_meet": None}
    return {"name": name, "stroke": stroke, "pb_seconds": best[0], "pb_year": best[1], "pb_meet": best[2]}

# ------------------------ /api/summary ------------------------
@router.get("/api/summary")
def summary(
    name: str = Query(...),
    stroke: str = Query(...),
    limit: int = Query(500, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    前端主頁面用：成績分析＋四式統計＋趨勢（自己）＋ leaderTrend（榜首，限制在你資料區間，排除冬短）
    """
    # 1) 先抓你在該項目的所有成績（做分析 & 趨勢 & 決定年份區間）
    sql_me = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" = :stroke
        ORDER BY "年份" ASC
    """
    rows_me = db.execute(text(sql_me), {"name": name, "stroke": stroke}).mappings().all()

    items: List[Dict[str, Any]] = []
    secs: List[float] = []
    y_min, y_max = None, None
    for t in rows_me:
        sec = parse_seconds(t["r"])
        y = t["y"]
        items.append({"年份": y, "賽事名稱": clean_meet_name(t["m"]), "項目": stroke, "姓名": name, "成績": t["r"], "seconds": sec})
        if sec is not None:
            secs.append(sec)
        if year_in_range(y):
            if (y_min is None) or (y < y_min): y_min = y
            if (y_max is None) or (y > y_max): y_max = y

    # 分析
    meet_count = len(items)
    avg_seconds = sum([s for s in secs if s is not None]) / len(secs) if secs else None
    pb_seconds = min(secs) if secs else None

    # 趨勢（自己）
    trend_points = [{"year": it["年份"], "seconds": it["seconds"]} for it in items if it["seconds"] is not None]

    # 若尚無區間，leaderTrend 也沒法算
    leader_points: List[Dict[str, Any]] = []

    # 搭配 /api/rank 的規則找到榜首（相同邏輯，取得 leader name）
    leader_name = None
    try:
        rk = rank(name=name, stroke=stroke, db=db)  # 直接呼叫下方函式取得資料（不走網路）
        leader_name = rk.get("leader", {}).get("name")
    except Exception:
        leader_name = None

    if leader_name and y_min and y_max:
        sql_ld = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :ld
              AND "項目" = :stroke
              AND "年份" >= :ymin AND "年份" <= :ymax
              AND ("賽事名稱" IS NULL OR "賽事名稱" NOT ILIKE :winter)
            ORDER BY "年份" ASC
        """
        rs = db.execute(text(sql_ld), {"ld": leader_name, "stroke": stroke, "ymin": y_min, "ymax": y_max, "winter": f"%{WINTER_SHORT}%"}).mappings().all()
        for t in rs:
            sec = parse_seconds(t["r"])
            if sec is not None:
                leader_points.append({"year": t["y"], "seconds": sec})

    return {
        "analysis": {"meetCount": meet_count, "avg_seconds": avg_seconds, "pb_seconds": pb_seconds},
        "family": {},  #（若要四式統計，可沿用你原本版本；此處略）
        "trend": {"points": trend_points},
        "leaderTrend": {"points": leader_points},
        "items": items[cursor: cursor + limit],
        "nextCursor": (cursor + limit) if (cursor + limit) < len(items) else None,
    }

# ------------------------ /api/rank ------------------------
@router.get("/api/rank")
def rank(
    name: str = Query(..., description="輸入選手姓名"),
    stroke: str = Query(..., description="項目（exact match）"),
    db: Session = Depends(get_db),
):
    """
    1) 找出輸入選手在該項目的所有出賽列，決定年份區間 [y_min, y_max]
    2) 依「同一場賽事 + 同一項目（exact）」與『組別規則』蒐集對手姓名（排除自己），對手池去重
    3) PB 計算（限定同項目、在 [y_min, y_max]、排除冬季短水道）
    4) 排序、組出 top / you / leader / denominator
    5) 回傳供前端排行卡片與 leaderTrend 取用
    """
    # 1) 你的所有出賽（該項目），與年份區間
    sql_me = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, COALESCE("組別"::text,'') AS g
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" = :stroke
        ORDER BY "年份" ASC
    """
    rows_me = db.execute(text(sql_me), {"name": name, "stroke": stroke}).mappings().all()
    if not rows_me:
        return {
            "name": name, "stroke": stroke,
            "denominator": 0, "rank": None, "percentile": None,
            "top": [], "leader": None, "you": None,
            "around": [], "opponents": []
        }

    y_min, y_max = None, None
    my_keys: List[Tuple[str, str, Optional[str], bool]] = []  # (meet, stroke, group_if_needed, use_group)
    for t in rows_me:
        y = t["y"]
        if year_in_range(y):
            if (y_min is None) or (y < y_min): y_min = y
            if (y_max is None) or (y > y_max): y_max = y
        g = (t["g"] or "").strip()
        use_group = (not is_numeric_group(g)) and (g != "")
        my_keys.append((t["m"], stroke, g if use_group else None, use_group))

    # 2) 對手池：逐 key 查詢
    opp_names: set = set()
    for meet, item, grp, use_group in my_keys:
        if use_group:
            sql_opp = f"""
                SELECT DISTINCT "姓名"::text AS n
                FROM {TABLE}
                WHERE "賽事名稱" = :m AND "項目" = :it AND COALESCE("組別"::text,'') = :g
                  AND "姓名" <> :me
            """
            rs = db.execute(text(sql_opp), {"m": meet, "it": item, "g": grp, "me": name}).all()
        else:
            sql_opp = f"""
                SELECT DISTINCT "姓名"::text AS n
                FROM {TABLE}
                WHERE "賽事名稱" = :m AND "項目" = :it
                  AND "姓名" <> :me
            """
            rs = db.execute(text(sql_opp), {"m": meet, "it": item, "me": name}).all()
        for r in rs:
            opp_names.add(r[0])

    # 3) 計算 PB（含你自己），限定年份區間＆排除冬短
    def get_pb_one(person: str) -> Optional[Tuple[float, str, str]]:
        cond = ' AND "年份" >= :ymin AND "年份" <= :ymax'
        sql_pb = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :nm AND "項目" = :it
              AND ("賽事名稱" IS NULL OR "賽事名稱" NOT ILIKE :winter)
              {cond}
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql_pb), {
            "nm": person, "it": stroke, "ymin": y_min, "ymax": y_max, "winter": f"%{WINTER_SHORT}%"
        }).mappings().all()
        best: Optional[Tuple[float, str, str]] = None
        for t in rows:
            sec = parse_seconds(t["r"])
            if sec is None:
                continue
            if (best is None) or (sec < best[0]):
                best = (sec, t["y"], clean_meet_name(t["m"]))
        return best

    people = [name] + sorted(list(opp_names))
    recs: List[Dict[str, Any]] = []
    for p in people:
        best = get_pb_one(p)
        if best:
            recs.append({"name": p, "pb_seconds": best[0], "pb_year": best[1], "pb_meet": best[2]})

    if not recs:
        return {
            "name": name, "stroke": stroke,
            "denominator": 0, "rank": None, "percentile": None,
            "top": [], "leader": None, "you": None,
            "around": [], "opponents": sorted(list(opp_names))
        }

    # 排序 & 找名次
    recs.sort(key=lambda x: x["pb_seconds"])
    denom = len(recs)
    your_idx = next((i for i, r in enumerate(recs) if r["name"] == name), None)

    # 百分位：越小越好（你的名次 / 總人數 -> 轉為 0-100 大越好）
    percentile = None
    if your_idx is not None and denom > 0:
        rank_1based = your_idx + 1
        percentile = (1 - ((rank_1based - 1) / denom)) * 100

    # around：你附近幾位
    around: List[Dict[str, Any]] = []
    if your_idx is not None:
        for j in range(max(0, your_idx - 3), min(denom, your_idx + 3)):
            if j != your_idx:
                around.append({**recs[j], "rank": j + 1})

    # top 10
    top = [{**recs[i], "rank": i + 1} for i in range(min(10, denom))]

    you = None
    if your_idx is not None:
        you = {**recs[your_idx], "rank": your_idx + 1}

    leader = {**recs[0], "rank": 1}

    return {
        "name": name,
        "stroke": stroke,
        "denominator": denom,
        "rank": you["rank"] if you else None,
        "percentile": percentile,
        "leader": leader,
        "you": you,
        "around": around,
        "top": top,
        "opponents": sorted(list(opp_names)),
    }