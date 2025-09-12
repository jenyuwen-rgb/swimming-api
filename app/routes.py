# app/routes.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any

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

# ---------- routes（此檔案內「不要」加 /api 前綴） ----------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# 取得某位選手有哪些「項目」(去重)
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

# 目前表結構
@router.get("/debug/columns")
def debug_columns(db: Session = Depends(get_db)):
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'swimming_scores'
        ORDER BY ordinal_position
    """
    cols = [r[0] for r in db.execute(text(q)).all()]
    return {"table": "swimming_scores", "columns": cols}

# 成績列表：直接模糊比對 (ILIKE)
@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目關鍵字，例如 50公尺蛙式"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    sql = """
        SELECT
            "年份"::text              AS year8,
            "賽事名稱"::text           AS meet,
            "項目"::text               AS item,
            COALESCE("成績"::text,'')  AS result,
            COALESCE("名次"::text,'')  AS rank,
            COALESCE("泳池長度"::text,'') AS pool_len,
            "姓名"::text               AS swimmer
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT :limit OFFSET :offset
    """
    params = {
        "name": name,
        "pat": f"%{stroke}%",
        "limit": limit,
        "offset": cursor,
    }
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

# PB 查詢：同樣模糊比對
@router.get("/pb")
def pb(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    sql = """
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "pat": f"%{stroke}%"}).mappings().all()

    best = None  # (sec, year8, meet)
    for r in rows:
        sec = convert_to_seconds(r["result"] or "")
        if sec <= 0:
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
