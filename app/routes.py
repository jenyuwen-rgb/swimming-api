# app/routes.py  —— 完整檔
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re, math
from .db import SessionLocal

router = APIRouter()  # 前綴統一在 main.py 以 prefix="/api" 掛載

TABLE = "swimming_scores"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- helpers ----------
def parse_seconds(s: Optional[str]) -> Optional[float]:
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

def is_winter_short_course(meet_name: str) -> bool:
    if not meet_name:
        return False
    name = str(meet_name)
    return ("冬季短水道" in name) or ("冬短" in name) or ("短水道" in name)

def year8_min_max(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    ys = [str(r.get("年份") or r.get("year8") or "") for r in rows if (r.get("年份") or r.get("year8"))]
    ys = [y for y in ys if len(y) == 8 and y.isdigit()]
    if not ys:
        return None, None
    return min(ys), max(ys)

# ---------- health / debug ----------
@router.get("/health")
def health(): return {"ok": "true"}

@router.get("/debug/ping")
def ping(): return {"ping": "pong"}

@router.get("/debug/rowcount")
def debug_rowcount(db: Session = Depends(get_db)):
    n = db.execute(text(f'SELECT COUNT(*) FROM {TABLE}')).scalar() or 0
    return {"table": TABLE, "rows": int(n)}

# ---------- core data ----------
@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目（支援模糊，如：50蛙、100自由）"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"
    sql = f"""
        SELECT
          "年份"::text AS year8,
          "賽事名稱"::text AS meet,
          "項目"::text AS item,
          "成績"::text AS result,
          COALESCE("名次"::text,'') AS rnk,
          COALESCE("水道"::text,'') AS lane,
          "姓名"::text AS swimmer,
          COALESCE("組別"::text,'') AS group
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), {"name": name, "pat": pat, "limit": limit, "offset": cursor}).mappings().all()
    items = []
    for r in rows:
        sec = parse_seconds(r["result"])
        items.append({
            "年份": r["year8"], "賽事名稱": r["meet"], "項目": r["item"], "姓名": r["swimmer"],
            "成績": r["result"], "名次": r["rnk"], "水道": r["lane"], "組別": r["group"], "seconds": sec
        })
    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}

@router.get("/pb")
def pb(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"
    sql = f"""
      SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
      FROM {TABLE}
      WHERE "姓名"=:name AND "項目" ILIKE :pat
      ORDER BY "年份" ASC
      LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
    best = None
    for r in rows:
        sec = parse_seconds(r["result"])
        if sec is None: 
            continue
        if is_winter_short_course(r["meet"]):  # 剔除冬季短水道
            continue
        if best is None or sec < best[0]:
            best = (sec, r["year8"], r["meet"])
    if not best:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}
    return {"name": name, "stroke": stroke, "pb_seconds": best[0], "year": best[1], "from_meet": best[2]}

@router.get("/stats/family")
def stats_family(name: str = Query(...), db: Session = Depends(get_db)):
    families = ["蛙式","仰式","自由式","蝶式"]
    out: Dict[str, Any] = {}
    for fam in families:
        pat = f"%{fam}%"
        sql = f"""
          SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r, "項目"::text AS item
          FROM {TABLE}
          WHERE "姓名"=:name AND "項目" ILIKE :pat
          ORDER BY "年份" ASC
          LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
        count, dist_count, best = 0, {}, None
        for row in rows:
            count += 1
            m = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1
            sec = parse_seconds(row["r"])
            if sec is None or is_winter_short_course(row["m"]):
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], row["m"])
        mostDist, mostCount = "", 0
        for d, c in dist_count.items():
            if c > mostCount:
                mostDist, mostCount = d, c
        out[fam] = {
            "count": count,
            "pb_seconds": best[0] if best else None,
            "year": best[1] if best else None,
            "from_meet": best[2] if best else None,
            "mostDist": mostDist, "mostCount": mostCount
        }
    return out

# ---------- opponents & rank helpers ----------
def opponent_pool(db: Session, name: str, stroke: str) -> List[Dict[str, Any]]:
    """
    以「同年份＋同賽事名稱＋同項目」為基礎，若組別非數字則加上同組別，彙整曾同場對手。
    """
    pat = f"%{stroke.strip()}%"
    # 撈出輸入選手所有比賽（作為對手池的條件來源）
    base_sql = f"""
      SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item,
             COALESCE("組別"::text,'') AS grp
      FROM {TABLE}
      WHERE "姓名"=:name AND "項目" ILIKE :pat
      LIMIT 5000
    """
    base_meets = db.execute(text(base_sql), {"name": name, "pat": pat}).mappings().all()
    if not base_meets:
        return []
    pool = set()
    for bm in base_meets:
        cond = f""""年份" = :y AND "賽事名稱" = :m AND "項目" = :i"""
        params = {"y": bm["year8"], "m": bm["meet"], "i": bm["item"]}
        # 組別若非數字，列入同組別
        if bm["grp"] and not re.fullmatch(r"\d+", bm["grp"]):
            cond += """ AND COALESCE("組別"::text,'') = :g"""
            params["g"] = bm["grp"]
        sql = f'SELECT DISTINCT "姓名"::text AS n FROM {TABLE} WHERE {cond}'
        for r in db.execute(text(sql), params).all():
            if r[0] != name:
                pool.add(r[0])
    return [{"name": n} for n in sorted(pool)]

def best_pb_in_range(db: Session, target: str, stroke: str, y_min: Optional[str], y_max: Optional[str]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """限定同泳姿+距離、剔除冬短、且只取 y_min~y_max 區間。"""
    pat = f"%{stroke.strip()}%"
    sql = f"""
      SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
      FROM {TABLE}
      WHERE "姓名"=:name AND "項目" ILIKE :pat
        AND "年份" BETWEEN :ymin AND :ymax
      ORDER BY "年份" ASC
      LIMIT 4000
    """
    rows = db.execute(text(sql), {"name": target, "pat": pat, "ymin": y_min or "00000000", "ymax": y_max or "99999999"}).mappings().all()
    best = None
    for r in rows:
        sec = parse_seconds(r["r"])
        if sec is None or is_winter_short_course(r["m"]):
            continue
        if best is None or sec < best[0]:
            best = (sec, r["y"], r["m"])
    if not best:
        return None, None, None
    return best

# ---------- summary ----------
@router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(500, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    # 1) 明細（同泳姿＋距離）
    pat = f"%{stroke.strip()}%"
    sql = f"""
      SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item,
             "成績"::text AS result, COALESCE("名次"::text,'') AS rnk,
             COALESCE("水道"::text,'') AS lane, COALESCE("組別"::text,'') AS grp
      FROM {TABLE}
      WHERE "姓名"=:name AND "項目" ILIKE :pat
      ORDER BY "年份" ASC
      LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), {"name": name, "pat": pat, "limit": limit, "offset": cursor}).mappings().all()
    items, valid_secs = [], []
    for r in rows:
        sec = parse_seconds(r["result"])
        items.append({
            "年份": r["year8"], "賽事名稱": r["meet"], "項目": r["item"], "姓名": name,
            "成績": r["result"], "名次": r["rnk"], "水道": r["lane"], "組別": r["grp"], "seconds": sec
        })
        if isinstance(sec, (int, float)) and sec > 0 and not is_winter_short_course(r["meet"]):
            valid_secs.append(sec)
    next_cursor = cursor + limit if len(rows) == limit else None

    # 分析
    meet_count = len(items)
    avg_seconds = (sum(valid_secs) / len(valid_secs)) if valid_secs else None
    pb_seconds = (min(valid_secs) if valid_secs else None)

    # 四式專項統計
    fam = stats_family(name=name, db=db)

    # 趨勢（自己）
    trend_points = [{"year": it["年份"], "seconds": it["seconds"]} for it in items if it["seconds"]]

    # 榜首趨勢：先決定年份區間，再取 leader 的完整同泳姿＋距離，限於該區間，仍剔除冬短
    all_me_rows = db.execute(text(f"""
        SELECT "年份"::text AS y FROM {TABLE}
        WHERE "姓名"=:n AND "項目" ILIKE :p
        ORDER BY "年份" ASC
        LIMIT 5000
    """), {"n": name, "p": pat}).mappings().all()
    y_min, y_max = year8_min_max(all_me_rows)

    # 對手池 & 榜首
    ops = opponent_pool(db, name, stroke)
    # 取每個對手在區間內的 PB
    rated = []
    for op in ops:
        sec, yy, mm = best_pb_in_range(db, op["name"], stroke, y_min, y_max)
        if sec is None:
            continue
        rated.append({"name": op["name"], "pb_seconds": sec, "pb_year": yy, "pb_meet": mm})
    rated.sort(key=lambda x: x["pb_seconds"])
    leader = rated[0]["name"] if rated else None

    leader_points = []
    if leader:
        lrows = db.execute(text(f"""
          SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
          FROM {TABLE}
          WHERE "姓名"=:n AND "項目" ILIKE :p
            AND "年份" BETWEEN :ymin AND :ymax
          ORDER BY "年份" ASC
          LIMIT 4000
        """), {"n": leader, "p": pat, "ymin": y_min or "00000000", "ymax": y_max or "99999999"}).mappings().all()
        for r in lrows:
            sec = parse_seconds(r["r"])
            if sec is None or is_winter_short_course(r["m"]):
                continue
            leader_points.append({"year": r["y"], "seconds": sec})

    return {
        "analysis": {"meetCount": meet_count, "avg_seconds": avg_seconds, "pb_seconds": pb_seconds},
        "family": fam,
        "trend": { "points": trend_points },
        "leaderTrend": { "points": leader_points },
        "items": items,
        "nextCursor": next_cursor
    }

# ---------- rank ----------
@router.get("/rank")
def rank_api(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    # 年份區間（用於限制對手 PB）
    pat = f"%{stroke.strip()}%"
    my_all = db.execute(text(f"""
      SELECT "年份"::text AS y FROM {TABLE}
      WHERE "姓名"=:n AND "項目" ILIKE :p
      ORDER BY "年份" ASC
      LIMIT 5000
    """), {"n": name, "p": pat}).mappings().all()
    y_min, y_max = year8_min_max(my_all)

    ops = opponent_pool(db, name, stroke)
    rated = []
    for op in ops:
        sec, yy, mm = best_pb_in_range(db, op["name"], stroke, y_min, y_max)
        if sec is None:
            continue
        rated.append({"name": op["name"], "pb_seconds": sec, "pb_year": yy, "pb_meet": mm})
    rated.sort(key=lambda x: x["pb_seconds"])

    # 自己名次
    my_sec, my_y, my_m = best_pb_in_range(db, name, stroke, y_min, y_max)
    denom = len(rated) + (1 if my_sec is not None and all(r["name"] != name for r in rated) else 0)
    my_rank = None
    if my_sec is not None:
        merged = rated + [{"name": name, "pb_seconds": my_sec}]
        merged.sort(key=lambda x: x["pb_seconds"])
        for idx, r in enumerate(merged, 1):
            if r["name"] == name and my_rank is None:
                my_rank = idx
                break

    return {
        "name": name, "stroke": stroke,
        "denominator": denom if denom else None,
        "rank": my_rank, 
        "percentile": (100.0 * (denom - my_rank) / denom) if (denom and my_rank) else None,
        "leader": ({"name": rated[0]["name"], "pb_seconds": rated[0]["pb_seconds"], "rank": 1} if rated else None),
        "you": ({"name": name, "pb_seconds": my_sec, "pb_year": my_y, "pb_meet": my_m, "rank": my_rank} if my_sec is not None else None),
        "top": rated[:10],
    }