# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re
from .db import SessionLocal

router = APIRouter()
TABLE = "swimming_scores"

# ----------------- DB session -----------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------- helpers -----------------
def parse_seconds(s: Optional[str]) -> Optional[float]:
    """把 'MM:SS.ss' 或 'SS.ss' 轉成秒；失敗回 None"""
    if not s:
        return None
    s = s.strip()
    try:
        if ":" in s:
            m, sec = s.split(":")
            return int(m) * 60 + float(sec)
        return float(s)
    except Exception:
        return None

def is_winter_short_course(meet: str) -> bool:
    """冬季短水道的成績不計入 PB"""
    if not meet:
        return False
    s = str(meet)
    return ("冬季短水道" in s) or ("短水道" in s and "冬" in s)

# ----------------- health -----------------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# ----------------- /results -----------------
@router.get("/results")
def results(
    name: str = Query(...),
    stroke: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    指定選手＋項目（泳姿＋距離）的明細，年份倒序（最新在前），並附上 is_pb 供前端標紅。
    """
    try:
        pat = f"%{stroke.strip()}%"
        # 先抓全部（或分頁）資料：倒序
        sql = f"""
            SELECT
                "年份"::text AS y,
                "賽事名稱"::text AS m,
                "項目"::text AS i,
                "成績"::text AS r,
                COALESCE("名次"::text,'')  AS rk,
                COALESCE("水道"::text,'')  AS ln,
                COALESCE("組別"::text,'')  AS g,
                "姓名"::text AS n
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" DESC
            LIMIT :limit OFFSET :offset
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat, "limit": limit, "offset": cursor}).mappings().all()

        # 為了標 PB，需要計算整體 PB（排除冬短）
        sql_all = f"""
            SELECT "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        all_rows = db.execute(text(sql_all), {"name": name, "pat": pat}).mappings().all()
        pb_sec = None
        for rr in all_rows:
            if is_winter_short_course(rr["m"]): 
                continue
            s = parse_seconds(rr["r"])
            if s is None or s <= 0:
                continue
            if pb_sec is None or s < pb_sec:
                pb_sec = s

        items: List[Dict[str, Any]] = []
        for r in rows:
            sec = parse_seconds(r["r"])
            items.append({
                "年份": r["y"],
                "賽事名稱": r["m"],
                "項目": r["i"],
                "姓名": r["n"],
                "成績": r["r"],
                "名次": r["rk"],
                "水道": r["ln"],
                "組別": r["g"],
                "seconds": sec,
                "is_pb": (sec is not None and pb_sec is not None and sec == pb_sec),
            })
        next_cursor = cursor + limit if len(rows) == limit else None
        return {"items": items, "nextCursor": next_cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"results failed: {e}")

# ----------------- /pb -----------------
@router.get("/pb")
def pb(name: str = Query(...), stroke: str = Query(...), db: Session = Depends(get_db)):
    """單純回傳該選手在該泳姿＋距離下（排除冬短）的 PB。"""
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
        best = None  # (sec, y, m)
        for r in rows:
            if is_winter_short_course(r["m"]):
                continue
            s = parse_seconds(r["r"])
            if s is None or s <= 0:
                continue
            if best is None or s < best[0]:
                best = (s, r["y"], r["m"])
        if not best:
            return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}
        return {"name": name, "stroke": stroke, "pb_seconds": best[0], "year": best[1], "from_meet": best[2]}
    except Exception:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

# ----------------- /summary -----------------
@router.get("/summary")
def summary(
    name: str = Query(...),
    stroke: str = Query(...),
    limit: int = Query(500, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 全量資料（算 analysis 與 trend）
    sql_all = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    all_rows = db.execute(text(sql_all), {"name": name, "pat": pat}).mappings().all()

    vals, pb_sec = [], None
    for r in all_rows:
        s = parse_seconds(r["r"])
        if s is not None and s > 0:
            vals.append(s)
            if not is_winter_short_course(r["m"]):
                pb_sec = s if pb_sec is None or s < pb_sec else pb_sec

    trend_points = [{"year": r["y"], "seconds": parse_seconds(r["r"])} for r in all_rows if parse_seconds(r["r"])]

    # 分頁明細（倒序，並標 is_pb）
    sql_page = f"""
        SELECT "年份"::text AS y,"賽事名稱"::text AS m,"項目"::text AS i,
               "成績"::text AS r,"姓名"::text AS n,
               COALESCE("名次"::text,'') AS rk,
               COALESCE("水道"::text,'') AS ln,
               COALESCE("組別"::text,'') AS g
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" DESC
        LIMIT :limit OFFSET :offset
    """
    page_rows = db.execute(text(sql_page), {"name": name, "pat": pat, "limit": limit, "offset": cursor}).mappings().all()

    items = []
    for r in page_rows:
        sec = parse_seconds(r["r"])
        items.append({
            "年份": r["y"], "賽事名稱": r["m"], "項目": r["i"], "姓名": r["n"],
            "成績": r["r"], "名次": r["rk"], "水道": r["ln"], "組別": r["g"],
            "seconds": sec, "is_pb": (sec is not None and pb_sec is not None and sec == pb_sec),
        })
    next_cursor = cursor + limit if len(page_rows) == limit else None

    analysis = {
        "meetCount": len(all_rows),
        "avg_seconds": (sum(vals) / len(vals)) if vals else None,
        "pb_seconds": pb_sec,
    }

    # ---- 新增：四式專項統計（PB 依「該泳式最常參加的距離」計算） ----
    family_out: Dict[str, Any] = {}
    for fam in ["蛙式", "仰式", "自由式", "蝶式"]:
        pf = f"%{fam}%"
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m,
                   "成績"::text AS r, "項目"::text AS i
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pf
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"name": name, "pf": pf}).mappings().all()

        count = len(rows)
        dist_count: Dict[str, int] = {}
        best_by_dist: Dict[str, Tuple[float, str, str]] = {}  # dist -> (sec, year, meet)

        for row in rows:
            m = re.search(r"(\d+)\s*公尺", str(row["i"] or ""))
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            s = parse_seconds(row["r"])
            if s is None or s <= 0 or is_winter_short_course(row["m"]):
                continue
            if dist:
                cur = best_by_dist.get(dist)
                if cur is None or s < cur[0]:
                    best_by_dist[dist] = (s, row["y"], row["m"])

        # 最多距離（並列時取字典序較小，如 100公尺 優先於 200公尺）
        mostDist, mostCount = "", 0
        for d, c in dist_count.items():
            if c > mostCount or (c == mostCount and d < mostDist):
                mostDist, mostCount = d, c

        pb_tuple = best_by_dist.get(mostDist)
        if pb_tuple is None and best_by_dist:
            pb_tuple = min(best_by_dist.values(), key=lambda t: t[0])

        family_out[fam] = {
            "count": count,
            "mostDist": mostDist,
            "mostCount": mostCount,
            "pb_seconds": pb_tuple[0] if pb_tuple else None,
            "year": pb_tuple[1] if pb_tuple else None,
            "from_meet": pb_tuple[2] if pb_tuple else None,
        }
    # ---- 新增結束 ----

    return {
        "analysis": analysis,
        "trend": {"points": trend_points},
        "items": items,
        "nextCursor": next_cursor,
        "family": family_out,   # 前端讀的就是這個
    }
# ----------------- /rank -----------------
@router.get("/rank")
def rank_api(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    對手池規則：
    - 必須與輸入選手「同年份＋同賽事＋同項目＋同組別」**至少兩次**才納入。
    - PB 計算：同泳姿＋距離、排除冬短、且剔除早於輸入選手第一筆日期(t0)的成績。
    """
    pat = f"%{stroke.strip()}%"

    # t0：輸入選手在該泳姿＋距離的第一筆日期（最早年份字串）
    t0_sql = f"""SELECT MIN("年份"::text) FROM {TABLE} WHERE "姓名"=:name AND "項目" ILIKE :pat"""
    t0 = db.execute(text(t0_sql), {"name": name, "pat": pat}).scalar()
    t0 = str(t0) if t0 else None

    # 取輸入選手"同場"清單（y,m,i,g）
    base_sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS i, COALESCE("組別"::text,'') AS g
        FROM {TABLE}
        WHERE "姓名"=:name AND "項目" ILIKE :pat
        GROUP BY "年份","賽事名稱","項目","組別"
        LIMIT 5000
    """
    base = db.execute(text(base_sql), {"name": name, "pat": pat}).mappings().all()

    # 統計每個對手與我「同場」的次數
    counters: Dict[str, int] = {}
    for b in base:
        q = f"""
            SELECT DISTINCT "姓名"::text AS nm
            FROM {TABLE}
            WHERE "年份"=:y AND "賽事名稱"=:m AND "項目"=:i AND COALESCE("組別"::text,'')=:g
              AND "姓名"<>:name
        """
        rows = db.execute(text(q), {"y": b["y"], "m": b["m"], "i": b["i"], "g": b["g"], "name": name}).all()
        for r in rows:
            nm = r[0]
            counters[nm] = counters.get(nm, 0) + 1

    # 至少兩次同場才納入
    pool = [nm for nm, cnt in counters.items() if cnt >= 2]
    if name not in pool:
        pool.append(name)  # 也把本人放進來

    if not pool:
        return {"denominator": 0, "rank": None, "percentile": None, "leader": None, "you": None, "top": [], "leaderTrend": []}

    # 個人 PB（依規則）
    def best_of(player: str) -> Optional[Tuple[float, str, str]]:
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名"=:p AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"p": player, "pat": pat}).mappings().all()
        best = None
        for row in rows:
            if t0 and str(row["y"]) < t0:
                continue  # 早於輸入選手第一筆，剔除
            if is_winter_short_course(row["m"]):
                continue
            s = parse_seconds(row["r"])
            if s is None or s <= 0:
                continue
            if best is None or s < best[0]:
                best = (s, row["y"], row["m"])
        return best

    board: List[Dict[str, Any]] = []
    for nm in pool:
        b = best_of(nm)
        if b:
            board.append({"name": nm, "pb_seconds": b[0], "pb_year": b[1], "pb_meet": b[2]})

    if not board:
        return {"denominator": 0, "rank": None, "percentile": None, "leader": None, "you": None, "top": [], "leaderTrend": []}

    # 依 PB 排序
    board.sort(key=lambda x: x["pb_seconds"])
    for i, row in enumerate(board, start=1):
        row["rank"] = i

    denominator = len(board)
    you = next((x for x in board if x["name"] == name), None)
    rank_no = you["rank"] if you else None
    percentile = (100.0 * (denominator - rank_no) / denominator) if rank_no else None
    leader = board[0]
    top10 = board[:10]

    # 榜首趨勢線（裁 t0）
    leader_trend: List[Dict[str, Any]] = []
    q_leader = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :p AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    for row in db.execute(text(q_leader), {"p": leader["name"], "pat": pat}).mappings():
        if t0 and str(row["y"]) < t0:
            continue
        s = parse_seconds(row["r"])
        if s is None or s <= 0:
            continue
        leader_trend.append({"year": row["y"], "seconds": s, "meet": row["m"]})

    return {
        "denominator": denominator,
        "rank": rank_no,
        "percentile": percentile,
        "leader": leader,
        "you": you,
        "top": top10,
        "leaderTrend": leader_trend,
    }