# app/routes.py
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

_MEET_MAP = [
    ("臺中市114年市長盃水上運動競賽(游泳項目)", "台中市長盃"),
    ("全國冬季短水道游泳錦標賽", "全國冬短"),
    ("全國總統盃暨美津濃游泳錦標賽", "全國總統盃"),
    ("全國總統盃暨美津濃分齡游泳錦標賽", "全國總統盃"),
    ("冬季短水道", "冬短"),
    ("全國運動會臺南市游泳代表隊選拔賽", "台南全運會選拔"),
    ("全國青少年游泳錦標賽", "全國青少年"),
    ("臺中市議長盃", "台中議長盃"),
    ("臺中市市長盃", "台中市長盃"),
    ("(游泳項目)", ""),
    ("春季游泳錦標賽", "春長"),
    ("全國E世代青少年", "E世代"),
    ("臺南市市長盃短水道", "台南市長盃"),
    ("臺南市中小學", "台南中小學"),
    ("臺南市委員盃", "台南委員盃"),
    ("臺南市全國運動會游泳選拔賽", "台南全運會選拔"),
]
_MEET_REGEX = [
    (re.compile(r"^\d{4}\s*"), ""),
    (re.compile(r"^\d{3}\s*"), ""),
    (re.compile(r"(?<!青少年)游泳錦標賽"), ""),
    (re.compile(r"\s*游泳錦標賽\s*$"), ""),
]
def clean_meet_name(name: Optional[str]) -> str:
    if not name:
        return ""
    out = name.strip()
    for src, repl in _MEET_MAP:
        if src in out:
            out = out.replace(src, repl)
    for pat, repl in _MEET_REGEX:
        out = pat.sub(repl, out)
    return re.sub(r"\s{2,}", " ", out).strip()

@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

@router.get("/debug/ping")
def ping() -> Dict[str, str]:
    return {"ping": "pong"}

@router.get("/debug/columns")
def debug_columns(db: Session = Depends(get_db)) -> Dict[str, Any]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :t
        ORDER BY ordinal_position
    """
    cols = [r[0] for r in db.execute(text(sql), {"t": TABLE}).all()]
    return {"table": TABLE, "columns": cols}

@router.get("/debug/strokes")
def debug_strokes(
    name: str = Query(..., description="選手姓名"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    sql = f"""
        SELECT DISTINCT "項目"::text AS item
        FROM {TABLE}
        WHERE "姓名" = :name
        ORDER BY 1
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name}).all()
    return {"name": name, "strokes": [r[0] for r in rows]}

@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目（支援模糊，如：50蛙、100自由）"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT
                "年份"::text      AS year8,
                "賽事名稱"::text   AS meet,
                "項目"::text       AS item,
                "成績"::text       AS result,
                COALESCE("名次"::text, '') AS rank,
                COALESCE("水道"::text, '') AS lane,
                "姓名"::text       AS swimmer
            FROM {TABLE}
            WHERE "姓名" = :name
              AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT :limit OFFSET :offset
        """
        params = {"name": name, "pat": pat, "limit": limit, "offset": cursor}
        rows = db.execute(text(sql), params).mappings().all()

        items: List[Dict[str, Any]] = []
        for r in rows:
            sec = parse_seconds(r["result"])
            items.append(
                {
                    "年份": r["year8"],
                    "賽事名稱": clean_meet_name(r["meet"]),
                    "項目": r["item"],
                    "姓名": r["swimmer"],
                    "成績": r["result"],
                    "名次": r["rank"],
                    "水道": r["lane"],
                    "泳池長度": "",
                    "seconds": sec,
                }
            )

        next_cursor = cursor + limit if len(rows) == limit else None
        return {"debug_sql": sql, "params": params, "items": items, "nextCursor": next_cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"results failed: {e}")

@router.get("/pb")
def pb(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目（支援模糊，如：50蛙、100自由）"),
    db: Session = Depends(get_db),
):
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()

        best = None  # (sec, year8, meet)
        for r in rows:
            sec = parse_seconds(r["result"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, r["year8"], clean_meet_name(r["meet"]))

        if not best:
            return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

        return {"name": name, "stroke": stroke, "pb_seconds": best[0], "year": best[1], "from_meet": best[2]}
    except Exception:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

@router.get("/stats/family")
def stats_family(
    name: str = Query(..., description="選手姓名"),
    db: Session = Depends(get_db),
):
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    out: Dict[str, Any] = {}

    for fam in families:
        pat = f"%{fam}%"
        sql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS item, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()

        count = 0
        best = None  # (sec, y, m)
        dist_count: Dict[str, int] = {}

        for r in rows:
            # 抓距離
            dist = ""
            m = re.search(r"(\d+)\s*公尺", r["item"] or "")
            if m:
                dist = f"{m.group(1)}公尺"
                dist_count[dist] = dist_count.get(dist, 0) + 1

            sec = parse_seconds(r["r"])
            if sec is None:
                continue
            count += 1
            if best is None or sec < best[0]:
                best = (sec, r["y"], clean_meet_name(r["m"]))

        # 最多距離
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
