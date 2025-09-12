# app/routes.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional

from .db import SessionLocal
from .utils_swim import convert_to_seconds, simplify_category

router = APIRouter()


# ---------- DB session ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------- routes（此檔案內不加 /api） ----------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}


@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目（例：50公尺蛙式）"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    # 不加 schema 前綴，直接用 swimming_scores（你 Supabase 的實際表名）
    sql = """
        SELECT
            "年份"::text            AS year8,
            "賽事名稱"::text         AS meet,
            "項目"::text             AS item,
            COALESCE("成績"::text,'') AS result,
            COALESCE("名次"::text,'') AS rank,
            COALESCE("泳池長度"::text,'') AS pool_len,
            "姓名"::text             AS swimmer
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT :limit OFFSET :offset
    """
    params = {"name": name, "stroke": stroke, "limit": limit, "offset": cursor}
    rows = db.execute(text(sql), params).mappings().all()

    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append(
            {
                "年份": r["year8"],
                "賽事名稱": simplify_category(r["meet"] or ""),
                "項目": r["item"],
                "姓名": r["swimmer"],
                "成績": r["result"],
                "名次": r["rank"],
                "泳池長度": r["pool_len"],
                "seconds": convert_to_seconds(r["result"] or ""),
            }
        )

    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}


@router.get("/pb")
def pb(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目（例：50公尺蛙式）"),
    db: Session = Depends(get_db),
):
    sql = """
        SELECT
            "年份"::text AS year8,
            "賽事名稱"::text AS meet,
            COALESCE("成績"::text,'') AS result
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "stroke": stroke}).mappings().all()

    best: Optional[Dict[str, Any]] = None
    best_sec: Optional[float] = None

    for r in rows:
        sec = convert_to_seconds(r["result"] or "")
        if sec <= 0:
            continue
        if best_sec is None or sec < best_sec:
            best_sec = sec
            best = {"year": r["year8"], "from_meet": simplify_category(r["meet"] or "")}

    return {
        "name": name,
        "stroke": stroke,
        "pb_seconds": best_sec,
        "year": best["year"] if best else None,
        "from_meet": best["from_meet"] if best else None,
    }


# ---------- debug ----------
@router.get("/debug/ping")
def debug_ping():
    return {"ping": "pong"}


@router.get("/debug/columns")
def debug_columns(db: Session = Depends(get_db)):
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'swimming_scores'
        ORDER BY ordinal_position
    """
    cols = [r[0] for r in db.execute(text(q)).all()]
    return {"table": "swimming_scores", "columns": cols}


@router.get("/debug/strokes")
def debug_strokes(name: str = Query(...), db: Session = Depends(get_db)):
    q = """
        SELECT DISTINCT "項目"::text AS item
        FROM swimming_scores
        WHERE "姓名" = :name
        ORDER BY item
    """
    rows = db.execute(text(q), {"name": name}).mappings().all()
    return {"name": name, "strokes": [r["item"] for r in rows]}
