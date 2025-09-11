from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from .db import SessionLocal
from .utils_swim import convert_to_seconds, simplify_category, normalize_distance_item, calc_wa

router = APIRouter()

# Dependency - 提供 DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/health")
def health():
    return {"ok": True}

@router.get("/results")
def results(
    name: str = Query(..., description="選手姓名（精確匹配）"),
    stroke: str = Query("", description="項目（例：50公尺蛙式；可留空）"),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    回傳欄位：
    - 年份(8碼) 例 20250726
    - 賽事名稱（原始 & 簡化）
    - 項目（原始 & 正規化距離）
    - 成績（字串）、seconds（數值）
    - 名次、泳池長度、姓名
    """
    offset = int(cursor)
    params = {"name": name, "limit": limit, "offset": offset}

    base_sql = """
        SELECT
            "年份"::text AS year8,
            "賽事名稱"::text AS meet,
            "項目"::text AS item,
            "成績"::text AS result,
            COALESCE("名次"::text, '') AS rank,
            COALESCE("泳池長度"::text, '') AS pool_len,
            "姓名"::text AS swimmer
        FROM swim_results
        WHERE "姓名" = :name
    """
    if stroke:
        base_sql += ' AND "項目" = :stroke'
        params["stroke"] = stroke

    # 以年份(8碼)排序（舊→新）；前端會再自行調整呈現順序
    base_sql += ' ORDER BY "年份" ASC LIMIT :limit OFFSET :offset'

    rows = db.execute(text(base_sql), params).fetchall()

    items = []
    for r in rows:
        year8 = r.year8
        meet = r.meet
        item = r.item
        seconds = convert_to_seconds(r.result)
        items.append(
            {
                "年份": int(year8) if year8 and year8.isdigit() else year8,
                "賽事名稱": meet,
                "賽事簡稱": simplify_category(meet),
                "項目": item,
                "項目正規化": normalize_distance_item(item),
                "成績": r.result,
                "seconds": seconds,
                "名次": r.rank,
                "泳池長度": r.pool_len,
                "姓名": r.swimmer,
            }
        )

    next_cursor = offset + len(items)
    return {"items": items, "nextCursor": (next_cursor if len(items) == limit else None)}

@router.get("/pb")
def best_pb(
    name: str,
    stroke: str,
    gender: str = "F",  # optional
    db: Session = Depends(get_db),
):
    """
    取該選手某一「項目」PB
    """
    sql = """
        SELECT "成績"::text AS result, "賽事名稱"::text AS meet, "年份"::text AS year8
        FROM swim_results
        WHERE "姓名" = :name AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    rows = db.execute(text(sql), {"name": name, "stroke": stroke}).fetchall()
    best = None
    best_row = None
    for r in rows:
        s = convert_to_seconds(r.result or "")
        if s > 0 and (best is None or s < best):
            best = s
            best_row = r
    if best is None:
        raise HTTPException(404, "no result")
    wa = calc_wa(best, stroke, gender)
    return {
        "name": name,
        "stroke": stroke,
        "pb_seconds": round(best, 2),
        "from_meet": best_row.meet,
        "year": best_row.year8,
        "wa": wa,
    }
