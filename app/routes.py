from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import re
from .db import SessionLocal

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

TABLE = "swimming_scores"

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

def is_short_course(meet: str) -> bool:
    # 冬季短水道不列入 PB
    return "冬季短水道" in (meet or "")

# ---------- 基本 ----------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# ---------- 成績查詢（明細） ----------
@router.get("/results")
def results(
    name: str = Query(...),
    stroke: str = Query(..., description="泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"
    sql = f"""
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet,
               "項目"::text AS item, "成績"::text AS result,
               COALESCE("名次"::text,'') AS rank,
               COALESCE("水道"::text,'') AS lane,
               "姓名"::text AS swimmer
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
            "成績": r["result"], "名次": r["rank"], "水道": r["lane"], "泳池長度": "", "seconds": sec,
        })
    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}

# ---------- PB（排除冬短） ----------
@router.get("/pb")
def pb(name: str, stroke: str, db: Session = Depends(get_db)):
    pat = f"%{stroke.strip()}%"
    sql = f"""
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
    best = None
    for r in rows:
        sec = parse_seconds(r["result"])
        if sec is None or is_short_course(r["meet"]):
            continue
        if best is None or sec < best[0]:
            best = (sec, r["year8"], r["meet"])
    if not best:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}
    return {"name": name, "stroke": stroke, "pb_seconds": best[0], "year": best[1], "from_meet": best[2]}

# ---------- 四式統計（不分距離） ----------
@router.get("/stats/family")
def stats_family(name: str, db: Session = Depends(get_db)):
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    out: Dict[str, Any] = {}
    for fam in families:
        pat = f"%{fam}%"
        sql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r, "項目"::text AS item
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
        count = 0
        dist_count: Dict[str, int] = {}
        best = None
        for row in rows:
            count += 1
            mm = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{mm.group(1)}公尺" if mm else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1
            sec = parse_seconds(row["r"])
            if sec is not None and (best is None or sec < best[0]):
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
            "mostDist": mostDist,
            "mostCount": mostCount,
        }
    return out

# ---------- 排行（含 leader.trend） ----------
@router.get("/rank")
def rank(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="泳姿＋距離"),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 依「同年份＋同賽事名稱＋同項目（組別為非數字時也必須相同組別）」來蒐集對手池
    sql_meets = f"""
        SELECT DISTINCT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS i, COALESCE("組別"::text,'') AS g
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
    """
    base_meets = db.execute(text(sql_meets), {"name": name, "pat": pat}).mappings().all()
    if not base_meets:
        return {"name": name, "stroke": stroke, "denominator": 0, "rank": None, "top": [], "you": None}

    opponents = {}
    for bm in base_meets:
        cond = '"年份" = :y AND "賽事名稱" = :m AND "項目" = :i'
        params = {"y": bm["y"], "m": bm["m"], "i": bm["i"]}
        if bm["g"] and not bm["g"].isdigit():
            cond += ' AND "組別" = :g'
            params["g"] = bm["g"]
        sql_swimmers = f'SELECT DISTINCT "姓名"::text AS n FROM {TABLE} WHERE {cond}'
        for r in db.execute(text(sql_swimmers), params).mappings().all():
            opponents[r["n"]] = True

    # 計算每位對手在該 stroke 的 PB（排除冬短）
    opp_rows = []
    for opp in opponents.keys():
        sql_scores = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :n AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rs = db.execute(text(sql_scores), {"n": opp, "pat": pat}).mappings().all()
        best = None
        for row in rs:
            sec = parse_seconds(row["r"])
            if sec is None or is_short_course(row["m"]):
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], row["m"])
        if best:
            opp_rows.append({"name": opp, "pb_seconds": best[0], "pb_year": best[1], "pb_meet": best[2]})

    if not opp_rows:
        return {"name": name, "stroke": stroke, "denominator": 0, "rank": None, "top": [], "you": None}

    opp_rows.sort(key=lambda x: x["pb_seconds"])
    for idx, row in enumerate(opp_rows, start=1):
        row["rank"] = idx

    me = next((o for o in opp_rows if o["name"] == name), None)

    # 產生榜首趨勢（完整歷年該泳姿＋距離的所有成績，非僅年度最佳）
    leader = opp_rows[0]
    sql_leader_trend = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :n AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    lt_rows = db.execute(text(sql_leader_trend), {"n": leader["name"], "pat": pat}).mappings().all()
    leader_trend = []
    for row in lt_rows:
        sec = parse_seconds(row["r"])
        if sec is None:
            continue
        # 趨勢線：包含所有成績（不排除冬短），前端只是視覺比較
        leader_trend.append({"x": row["y"], "label": f'{row["y"][2:4]}/{row["y"][4:6]}', "y": sec})
    leader_trend.sort(key=lambda p: p["x"])

    # 把 trend 附在榜首（top[0]）上
    opp_rows[0]["trend"] = leader_trend

    return {
        "name": name,
        "stroke": stroke,
        "denominator": len(opp_rows),
        "rank": me["rank"] if me else None,
        "you": me,
        "top": opp_rows[:10],
    }

# ----------（你現有的其他 debug/summary 等端點若有，保留即可） ----------
