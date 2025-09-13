# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional

from .db import SessionLocal
from .utils_swim import convert_to_seconds, simplify_category

router = APIRouter()

# ---------- DB dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Health ----------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# ---------- Debug ----------
@router.get("/debug/ping")
def debug_ping() -> Dict[str, str]:
    return {"ping": "pong"}

@router.get("/debug/columns")
def debug_columns(db: Session = Depends(get_db)) -> Dict[str, Any]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'swimming_scores'
        ORDER BY ordinal_position
    """
    cols = [r[0] for r in db.execute(text(sql)).all()]
    return {"table": "swimming_scores", "columns": cols}

@router.get("/debug/strokes")
def debug_strokes(
    name: str = Query(..., description="選手姓名"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    sql = """
        SELECT DISTINCT "項目"::text AS item
        FROM swimming_scores
        WHERE "姓名" = :name
        ORDER BY item
    """
    rows = db.execute(text(sql), {"name": name}).mappings().all()
    return {"name": name, "strokes": [r["item"] for r in rows]}

# ---------- Results ----------
@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名（精準比對）"),
    stroke: str = Query(..., description="項目（支援關鍵字，例如 50蛙）"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    base_sql = """
        SELECT
            "年份"::text      AS year8,
            "賽事名稱"::text   AS meet,
            "項目"::text       AS item,
            "成績"::text       AS result,
            COALESCE("名次"::text, '')        AS rank,
            COALESCE("泳池長度"::text, '')    AS pool_len,
            "姓名"::text       AS swimmer
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" ILIKE :stroke
        ORDER BY "年份" ASC
        LIMIT :limit OFFSET :offset
    """
    params = {
        "name": name,
        "stroke": f"%{stroke}%",
        "limit": limit,
        "offset": cursor,
    }

    rows = db.execute(text(base_sql), params).mappings().all()

    items: List[Dict[str, Any]] = []
    for r in rows:
        sec: Optional[float] = convert_to_seconds(r["result"])
        items.append(
            {
                "年份": r["year8"],
                "賽事名稱": simplify_category(r["meet"] or ""),
                "項目": r["item"],
                "姓名": r["swimmer"],
                "成績": r["result"],
                "名次": r["rank"],
                "泳池長度": r["pool_len"],
                "seconds": sec if sec is not None else None,
            }
        )

    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}

# ---------- PB ----------
@router.get("/pb")
def pb(
    name: str = Query(..., description="選手姓名（精準比對）"),
    stroke: str = Query(..., description="項目（支援關鍵字，例如 50蛙）"),
    db: Session = Depends(get_db),
):
    sql = """
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" ILIKE :stroke
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "stroke": f"%{stroke}%"}).mappings().all()

    best = None  # (sec, year8, meet)
    for r in rows:
        sec = convert_to_seconds(r["result"])
        if not sec:
            continue
        if best is None or sec < best[0]:
            best = (sec, r["year8"], simplify_category(r["meet"] or ""))

    if not best:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

    return {
        "name": name,
        "stroke": stroke,
        "pb_seconds": best[0],
        "year": best[1],
        "from_meet": best[2],
    }
