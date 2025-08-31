from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from .db import engine
from .utils_swim import convert_to_seconds, simplify_category, calc_wa

router = APIRouter()

EXCLUDE_SHORT = """ AND "賽事名稱" NOT ILIKE '%短水%' AND "賽事名稱" NOT ILIKE '%冬短%' """

@router.get("/health")
def health():
    with engine.connect() as c:
        c.execute(text("select 1"))
    return {"ok": True}

@router.get("/results")
def results(name: str, stroke: str, limit: int = 50, cursor: int = 0, exclude_shortcourse: int = 1):
    where = """"姓名"=:name AND "項目" ILIKE :stroke"""
    if exclude_shortcourse:
        where += EXCLUDE_SHORT
    sql = f"""
      SELECT "年份","賽事名稱","項目","姓名","成績","名次"
      FROM public."swimming_scores"
      WHERE {where}
      ORDER BY "年份" DESC OFFSET :ofs LIMIT :lim
    """
    with engine.connect() as c:
        items = [dict(r._mapping) for r in c.execute(
            text(sql), {"name": name, "stroke": f"%{stroke}%", "ofs": cursor, "lim": limit}
        )]
    for it in items:
        it["seconds"] = convert_to_seconds(it["成績"] or "")
        it["項目簡"] = simplify_category(it["項目"] or "")
    next_cursor = cursor + len(items) if len(items) == limit else None
    return {"items": items, "nextCursor": next_cursor}

@router.get("/pb")
def pb(name: str, stroke: str, gender: str = "男", exclude_shortcourse: int = 1):
    where = """"姓名"=:name AND "項目" ILIKE :stroke"""
    if exclude_shortcourse:
        where += EXCLUDE_SHORT
    sql = f"""
      SELECT "成績","賽事名稱","年份"
      FROM public."swimming_scores"
      WHERE {where}
    """
    best, row = None, None
    with engine.connect() as c:
        for r in c.execute(text(sql), {"name": name, "stroke": f"%{stroke}%"}):
            s = convert_to_seconds(r[0] or "")
            if s > 0 and (best is None or s < best):
                best, row = s, r
    if best is None:
        raise HTTPException(404, "no result")
    wa = calc_wa(best, stroke, gender)
    return {"pb_seconds": round(best, 2), "from_meet": row[1], "year": row[2], "wa": wa}
