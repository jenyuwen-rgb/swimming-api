# app/routes.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import re

from .db import SessionLocal

router = APIRouter()

# ---------- DB session ----------
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

_MEET_REPLACEMENTS = [
    (re.compile(r"^\d{4}\s*"), ""),
    (re.compile(r"^\d{3}\s*"), ""),
    (re.compile(r"^.*?年"), ""),
    (re.compile(r"\(游泳項目\)"), ""),
]

_MEET_MAP = {
    "臺中市114年市長盃水上運動競賽(游泳項目)": "台中市長盃",
    "全國冬季短水道游泳錦標賽": "全國冬短",
    "全國總統盃暨美津濃游泳錦標賽": "全國總統盃",
    "全國總統盃暨美津濃分齡游泳錦標賽": "全國總統盃",
    "冬季短水道": "冬短",
    "全國運動會臺南市游泳代表隊選拔賽": "台南全運會選拔",
    "全國青少年游泳錦標賽": "全國青少",
    "臺中市議長盃": "台中議長盃",
    "臺中市市長盃": "台中市長盃",
    "春季游泳錦標賽": "春長",
    "全國E世代青少年": "E世代",
    "臺南市市長盃短水道": "台南市長盃",
    "臺南市中小學": "台南中小學",
    "臺南市委員盃": "台南委員盃",
    "臺南市全國運動會游泳選拔賽": "台南全運會選拔",
    "游泳錦標賽": "",
}

def clean_meet_name(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    # 先明確對照替換
    for k, v in _MEET_MAP.items():
        if k in s:
            s = s.replace(k, v)
    # 再套一般規則
    for pat, repl in _MEET_REPLACEMENTS:
        s = pat.sub(repl, s)
    return re.sub(r"\s{2,}", " ", s).strip()

# ---------- routes ----------
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
    # 依你目前資料表名稱：swimming_scores
    base_sql = """
        SELECT
            "年份"::text                AS year8,
            "賽事名稱"::text             AS meet,
            "項目"::text                 AS item,
            "成績"::text                 AS result,
            COALESCE("名次"::text, '')  AS rank,
            COALESCE("泳池長度"::text, '') AS pool_len,
            "姓名"::text                 AS swimmer
        FROM swimming_scores
        WHERE "姓名" = :name
          AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT :limit OFFSET :offset
    """
    params = {"name": name, "stroke": stroke, "limit": limit, "offset": cursor}
    rows = db.execute(text(base_sql), params).mappings().all()

    items: List[Dict[str, Any]] = []
    for r in rows:
        sec = parse_seconds(r["result"])
        items.append(
            {
                "年份": r["year8"],
                "賽事名稱": clean_meet_name(r["meet"] or ""),
                "項目": r["item"],
                "姓名": r["swimmer"],
                "成績": r["result"],
                "名次": r["rank"],
                "泳池長度": r["pool_len"],
                "seconds": sec,
            }
        )

    next_cursor = cursor + limit if len(rows) == limit else None
    return {"items": items, "nextCursor": next_cursor}

@router.get("/pb")
def pb(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    sql = """
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
        FROM swimming_scores
        WHERE "姓名" = :name AND "項目" = :stroke
        ORDER BY "年份" ASC
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name, "stroke": stroke}).mappings().all()

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
