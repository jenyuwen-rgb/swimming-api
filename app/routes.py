# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import re
import logging

from .db import SessionLocal

router = APIRouter()
logger = logging.getLogger(__name__)

# ---------- helpers ----------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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
    (r"^\d{4}\s*", ""), (r"^\d{3}\s*", ""), (r"^.*?年", ""),
    ("臺中市114年市長盃水上運動競賽(游泳項目)", "台中市長盃"),
    ("全國冬季短水道游泳錦標賽", "全國冬短"),
    ("全國總統盃暨美津濃游泳錦標賽", "全國總統盃"),
    ("全國總統盃暨美津濃分齡游泳錦標賽", "全國總統盃"),
    ("冬季短水道", "冬短"),
    ("全國運動會臺南市游泳代表隊選拔賽", "台南全運會選拔"),
    ("全國青少年游泳錦標賽", "全國青少"),
    ("臺中市議長盃", "台中議長盃"),
    ("臺中市市長盃", "台中市長盃"),
    ("(游泳項目)", ""),
    ("春季游泳錦標賽", "春長"),
    ("全國E世代青少年", "E世代"),
    ("臺南市市長盃短水道", "台南市長盃"),
    ("臺南市中小學", "台南中小學"),
    ("臺南市委員盃", "台南委員盃"),
    ("臺南市全國運動會游泳選拔賽", "台南全運會選拔"),
    ("游泳錦標賽", ""),
]

def clean_meet_name(name: str) -> str:
    if not name:
        return name
    out = name
    for pat, repl in _MEET_MAP:
        if pat.startswith("^") or pat.endswith("年"):
            out = re.sub(pat, repl, out)
        else:
            out = out.replace(pat, repl)
    return re.sub(r"\s{2,}", " ", out).strip()

# ---------- routes (注意：main.py 會用 app.include_router(router, prefix="/api") ----------

@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

@router.get("/debug/ping")
def ping() -> Dict[str, str]:
    return {"ping": "pong"}

@router.get("/debug/columns")
def dbg_columns(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'swimming_scores'
        ORDER BY ordinal_position
    """)).fetchall()
    return {"table": "swimming_scores", "columns": [r[0] for r in rows]}

@router.get("/debug/strokes")
def dbg_strokes(name: str = Query(...), db: Session = Depends(get_db)):
    sql = """
        SELECT DISTINCT "項目"::text
        FROM public.swimming_scores
        WHERE "姓名" = :name
        ORDER BY 1
    """
    rows = db.execute(text(sql), {"name": name}).fetchall()
    return {"name": name, "strokes": [r[0] for r in rows]}

# 主要查詢（stroke 模糊比對）
@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名（全名比對）"),
    stroke: str = Query(..., description="項目（支援模糊，例如：50蛙、200混）"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        base_sql = """
            SELECT
                "年份"::text      AS year8,
                "賽事名稱"::text   AS meet,
                "項目"::text       AS item,
                "成績"::text       AS result,
                COALESCE("名次"::text, '') AS rank,
                "姓名"::text       AS swimmer
            FROM public.swimming_scores
            WHERE "姓名" = :name
              AND "項目" ILIKE :stroke_like
            ORDER BY COALESCE(NULLIF(TRIM("年份"),''),'0')::int ASC
            LIMIT :limit OFFSET :offset
        """
        params = {
            "name": name,
            "stroke_like": f"%{stroke}%",
            "limit": limit,
            "offset": cursor,
        }
        rows = db.execute(text(base_sql), params).mappings().all()

        items = []
        for r in rows:
            items.append({
                "年份": r["year8"],
                "賽事名稱": clean_meet_name(r["meet"] or ""),
                "項目": r["item"],
                "姓名": r["swimmer"],
                "成績": r["result"],
                "名次": r["rank"],
                "泳池長度": "",              # 這張表沒有此欄位，固定回空字串
                "seconds": parse_seconds(r["result"]),
            })

        next_cursor = cursor + limit if len(rows) == limit else None
        return {"items": items, "nextCursor": next_cursor}
    except Exception:
        raise HTTPException(status_code=500, detail="results failed")

# 個人最佳（stroke 模糊比對）
@router.get("/pb")
def pb(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    try:
        sql = """
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
            FROM public.swimming_scores
            WHERE "姓名" = :name
              AND "項目" ILIKE :stroke_like
            ORDER BY COALESCE(NULLIF(TRIM("年份"),''),'0')::int ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "stroke_like": f"%{stroke}%"}).mappings().all()

        best = None  # (sec, year8, meet)
        for r in rows:
            sec = parse_seconds(r["result"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, r["year8"], clean_meet_name(r["meet"] or ""))

        if not best:
            return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

        return {
            "name": name,
            "stroke": stroke,
            "pb_seconds": best[0],
            "year": best[1],
            "from_meet": best[2],
        }
    except Exception as e:
        logger.exception("pb error: %s", e)
        raise HTTPException(status_code=500, detail="pb failed")
