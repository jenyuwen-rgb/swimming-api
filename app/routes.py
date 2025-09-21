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

def distance_from_item(item: str) -> Optional[str]:
    m = re.search(r"(\d+)\s*公尺", str(item or ""))
    return f"{m.group(1)}公尺" if m else None

def same_numeric_group(g: Optional[str]) -> bool:
    """組別是數字"""
    if g is None:
        return False
    s = str(g).strip()
    return bool(re.fullmatch(r"\d+", s))

# ----------------- health & debug -----------------
@router.get("/health")
def health(): return {"ok": "true"}

@router.get("/debug/ping")
def ping(): return {"ping": "pong"}

@router.get("/debug/rowcount")
def debug_rowcount(db: Session = Depends(get_db)):
    n = db.execute(text(f'SELECT COUNT(*) FROM {TABLE}')).scalar() or 0
    return {"table": TABLE, "rows": int(n)}

# ----------------- /results -----------------
@router.get("/results")
def results(
    name: str = Query(...),
    stroke: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT "年份"::text AS year8,"賽事名稱"::text AS meet,"項目"::text AS item,
                   "成績"::text AS result,COALESCE("名次"::text,'') AS rank,
                   COALESCE("水道"::text,'') AS lane,COALESCE("組別"::text,'') AS grp,
                   "姓名"::text AS swimmer
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT :limit OFFSET :offset
        """
        params = {"name": name, "pat": pat, "limit": limit, "offset": cursor}
        rows = db.execute(text(sql), params).mappings().all()
        items = []
        for r in rows:
            sec = parse_seconds(r["result"])
            items.append({"年份": r["year8"],"賽事名稱": r["meet"],"項目": r["item"],
                          "姓名": r["swimmer"],"成績": r["result"],"名次": r["rank"],
                          "水道": r["lane"],"組別": r["grp"],"泳池長度": "","seconds": sec})
        next_cursor = cursor + limit if len(rows) == limit else None
        return {"items": items, "nextCursor": next_cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"results failed: {e}")

# ----------------- /pb -----------------
@router.get("/pb")
def pb(name: str, stroke: str, db: Session = Depends(get_db)):
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
        best = None
        for r in rows:
            if is_winter_short_course(r["m"]): continue
            sec = parse_seconds(r["r"])
            if sec is None: continue
            if best is None or sec < best[0]: best = (sec, r["y"], r["m"])
        if not best:
            return {"pb_seconds": None}
        return {"pb_seconds": best[0], "year": best[1], "from_meet": best[2]}
    except Exception:
        return {"pb_seconds": None}

# ----------------- /summary -----------------
@router.get("/summary")
def summary(name: str, stroke: str, db: Session = Depends(get_db)):
    pat = f"%{stroke.strip()}%"
    sql = f"""
        SELECT "年份"::text AS y,"賽事名稱"::text AS m,"項目"::text AS item,"成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
    items, secs = [], []
    for r in rows:
        sec = parse_seconds(r["r"])
        if sec: secs.append(sec)
        items.append({"年份": r["y"],"賽事名稱": r["m"],"項目": r["item"],
                      "姓名": name,"成績": r["r"],"seconds": sec})
    pb_seconds = min((s for r in rows if not is_winter_short_course(r["m"]) 
                     for s in [parse_seconds(r["r"])] if s), default=None)
    trend = [{"year": r["y"], "seconds": parse_seconds(r["r"])} for r in rows if parse_seconds(r["r"])]
    return {"analysis":{"meetCount":len(items),"avg_seconds": (sum(secs)/len(secs)) if secs else None,"pb_seconds":pb_seconds},
            "trend":{"points":trend},"items":items}

# ----------------- /rank -----------------
@router.get("/rank")
def rank_api(name: str, stroke: str, db: Session = Depends(get_db)):
    pat = f"%{stroke.strip()}%"
    q_meets = f"""
        SELECT "年份"::text AS y,"賽事名稱"::text AS m,"項目"::text AS i,COALESCE("組別"::text,'') AS g
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        GROUP BY "年份","賽事名稱","項目","組別"
    """
    base = db.execute(text(q_meets), {"name": name, "pat": pat}).mappings().all()
    opponents = {}
    for m in base:
        if same_numeric_group(m["g"]):
            q = f"""SELECT DISTINCT "姓名"::text AS nm FROM {TABLE}
                    WHERE "年份"=:y AND "賽事名稱"=:m AND "項目"=:i AND "姓名"<>:name"""
            rows = db.execute(text(q), {"y": m["y"], "m": m["m"], "i": m["i"], "name": name}).all()
        else:
            q = f"""SELECT DISTINCT "姓名"::text AS nm FROM {TABLE}
                    WHERE "年份"=:y AND "賽事名稱"=:m AND "項目"=:i AND "組別"=:g AND "姓名"<>:name"""
            rows = db.execute(text(q), {"y": m["y"], "m": m["m"], "i": m["i"], "g": m["g"], "name": name}).all()
        for r in rows: opponents[r[0]]={"name":r[0]}
    if not opponents: return {"denominator":0,"rank":None,"top":[],"leaderTrend":[]}

    def best_of(player:str):
        q=f"""SELECT "年份"::text AS y,"賽事名稱"::text AS m,"成績"::text AS r
              FROM {TABLE} WHERE "姓名"=:p AND "項目" ILIKE :pat ORDER BY "年份" ASC"""
        rs=db.execute(text(q),{"p":player,"pat":pat}).mappings().all()
        best=None
        for r in rs:
            if is_winter_short_course(r["m"]):continue
            s=parse_seconds(r["r"]); 
            if not s:continue
            if best is None or s<best[0]:best=(s,r["y"],r["m"])
        return best

    board=[]
    all_names=list(opponents.keys())
    if name not in all_names: all_names.append(name)
    for nm in all_names:
        b=best_of(nm)
        if b: board.append({"name":nm,"pb_seconds":b[0],"pb_year":b[1],"pb_meet":b[2]})
    if not board: return {"denominator":0,"rank":None,"top":[],"leaderTrend":[]}

    board.sort(key=lambda x:x["pb_seconds"])
    for i,r in enumerate(board,start=1): r["rank"]=i
    you=next((x for x in board if x["name"]==name),None)
    leader=board[0]
    leader_trend=[]
    q=f"""SELECT "年份"::text AS y,"賽事名稱"::text AS m,"成績"::text AS r
          FROM {TABLE} WHERE "姓名"=:p AND "項目" ILIKE :pat ORDER BY "年份" ASC"""
    for r in db.execute(text(q),{"p":leader["name"],"pat":pat}).mappings():
        s=parse_seconds(r["r"])
        if s: leader_trend.append({"year":r["y"],"seconds":s,"meet":r["m"]})
    return {"denominator":len(board),"rank":you["rank"] if you else None,
            "leader":{"name":leader["name"],"pb_seconds":leader["pb_seconds"],"rank":1},
            "you":you,"top":board[:10],"leaderTrend":leader_trend}