# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
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

# -------------------- utilities --------------------

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

def is_winter_short_course(meet: Optional[str]) -> bool:
    """賽事名稱含『冬季短水道』就視為冬短，不能拿來當 PB。"""
    return "冬季短水道" in str(meet or "")

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

# -------------------- basic & debug --------------------

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

@router.get("/debug/rowcount")
def debug_rowcount(db: Session = Depends(get_db)):
    sql = f'SELECT COUNT(*) FROM {TABLE}'
    n = db.execute(text(sql)).scalar() or 0
    return {"table": TABLE, "rows": int(n)}

@router.get("/debug/names")
def debug_names(
    q: str = Query("", description="模糊查詢關鍵字（例如 心妤 / 溫 / 温）"),
    db: Session = Depends(get_db),
):
    pat = f"%{q.strip()}%" if q else "%"
    sql = f"""
        SELECT DISTINCT "姓名"::text AS name,
               LENGTH("姓名"::text) AS len,
               LENGTH(TRIM("姓名"::text)) AS trim_len
        FROM {TABLE}
        WHERE "姓名" ILIKE :pat
        ORDER BY 1
        LIMIT 200
    """
    rows = db.execute(text(sql), {"pat": pat}).mappings().all()
    return {"q": q, "items": rows}

@router.get("/debug/name_detail")
def debug_name_detail(
    name: str = Query(..., description="完整姓名，檢查是否有前後空白"),
    db: Session = Depends(get_db),
):
    sql = f"""
        SELECT
          "姓名"::text AS name,
          LENGTH("姓名"::text) AS len,
          LENGTH(TRIM("姓名"::text)) AS trim_len
        FROM {TABLE}
        WHERE "姓名" = :name
        LIMIT 1
    """
    row = db.execute(text(sql), {"name": name}).mappings().first()
    return {"input": name, "info": (row or {})}

@router.get("/debug/names_sample")
def debug_names_sample(db: Session = Depends(get_db)):
    sql = f'''
        SELECT DISTINCT "姓名"::text AS name
        FROM {TABLE}
        WHERE "姓名" IS NOT NULL AND LENGTH(TRIM("姓名"::text))>0
        ORDER BY 1
        LIMIT 50
    '''
    rows = db.execute(text(sql)).all()
    return {"sample": [r[0] for r in rows]}

@router.get("/debug/dbhint")
def debug_dbhint():
    import os, re
    url = os.getenv("DATABASE_URL", "")
    masked = re.sub(r"://([^:]+):[^@]+@", r"://\\1:***@", url)
    return {"DATABASE_URL_hint": masked}

# -------------------- core APIs --------------------

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
        return {"items": items, "nextCursor": next_cursor}
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

        best = None  # (sec, year8, meet_clean)
        for r in rows:
            if is_winter_short_course(r["meet"]):
                continue
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
            SELECT
              "年份"::text      AS y,
              "賽事名稱"::text   AS m,
              "成績"::text       AS r,
              "項目"::text       AS item
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()

        count = 0
        dist_count: Dict[str, int] = {}
        best = None  # (sec, y, m_clean)

        for row in rows:
            count += 1
            raw_item = str(row["item"] or "")
            mmm = re.search(r"(\d+)\s*公尺", raw_item)
            dist = f"{mmm.group(1)}公尺" if mmm else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            if is_winter_short_course(row["m"]):
                continue
            sec = parse_seconds(row["r"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], clean_meet_name(row["m"]))

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

@router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"
    # 明細
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
        s = parse_seconds(r["result"])
        items.append({
            "年份": r["year8"],
            "賽事名稱": clean_meet_name(r["meet"]),
            "項目": r["item"],
            "姓名": r["swimmer"],
            "成績": r["result"],
            "名次": r["rank"],
            "水道": r["lane"],
            "泳池長度": "",
            "seconds": s,
        })
    next_cursor = cursor + limit if len(rows) == limit else None

    # 分析（PB 要排除冬季短水道）
    valid_for_avg = [x["seconds"] for x in items if isinstance(x["seconds"], (int, float)) and x["seconds"] > 0]
    avg_seconds = sum(valid_for_avg)/len(valid_for_avg) if valid_for_avg else None

    pb_seconds = None
    for r in rows:
        if is_winter_short_course(r["meet"]):
            continue
        s = parse_seconds(r["result"])
        if s is None:
            continue
        if (pb_seconds is None) or (s < pb_seconds):
            pb_seconds = s

    trend_points = [{"year": it["年份"], "seconds": it["seconds"]} for it in items if it["seconds"]]

    # 四式（不分距離）—沿用上方 stats_family 規則：PB 排除冬季短水道
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    fam_out: Dict[str, Any] = {}
    for fam in families:
        pf = f"%{fam}%"
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r, "項目"::text AS item
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 2000
        """
        rws = db.execute(text(q), {"name": name, "pat": pf}).mappings().all()
        count = 0
        dist_count: Dict[str, int] = {}
        best = None
        for row in rws:
            count += 1
            mm = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{mm.group(1)}公尺" if mm else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1
            if is_winter_short_course(row["m"]):
                continue
            sec = parse_seconds(row["r"])
            if sec is not None and (best is None or sec < best[0]):
                best = (sec, row["y"], clean_meet_name(row["m"]))
        mostDist, mostCount = "", 0
        for d, c in dist_count.items():
            if c > mostCount:
                mostDist, mostCount = d, c
        fam_out[fam] = {
            "count": count,
            "pb_seconds": best[0] if best else None,
            "year": best[1] if best else None,
            "from_meet": best[2] if best else None,
            "mostDist": mostDist,
            "mostCount": mostCount,
        }

    return {
        "analysis": {
            "meetCount": len(items),
            "avg_seconds": avg_seconds,
            "pb_seconds": pb_seconds,
        },
        "family": fam_out,
        "trend": {"points": trend_points},
        "items": items,
        "nextCursor": next_cursor,
    }

# -------------------- rank with leader trend --------------------

@router.get("/rank")
def rank_api(
    name: str = Query(..., description="基準選手"),
    stroke: str = Query(..., description="同距離＋同泳式（例：50公尺蛙式）"),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 1) 你的場次
    my_sql = f"""
        SELECT DISTINCT
            "年份"::text      AS y,
            "賽事名稱"::text   AS m,
            "項目"::text       AS i,
            COALESCE("組別"::text, '') AS g
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
    """
    my_rows = db.execute(text(my_sql), {"name": name, "pat": pat}).mappings().all()
    if not my_rows:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；若你的組別非數字則需同組別）",
            "denominator": None,
            "rank": None,
            "percentile": None,
            "top": [],
            "around": [],
            "you": None,
            "leader": None,
        }

    # 2) 對手池（組別規則）
    pool_sql = f"""
        WITH my AS (
            SELECT DISTINCT "年份"::text y, "賽事名稱"::text m, "項目"::text i, COALESCE("組別"::text,'') g
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
        )
        SELECT DISTINCT t."姓名"::text AS name
        FROM {TABLE} t
        JOIN my ON t."年份"::text = my.y
               AND t."賽事名稱"::text = my.m
               AND t."項目"::text = my.i
        WHERE
            (
                my.g ~ '^[0-9]+$'
                OR COALESCE(t."組別"::text,'') = my.g
            )
    """
    pool_names = [r[0] for r in db.execute(text(pool_sql), {"name": name, "pat": pat}).all()]
    pool_names = [n for n in pool_names if n and n != name]
    if not pool_names:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；若你的組別非數字則需同組別）",
            "denominator": 1,
            "rank": 1,
            "percentile": 100.0,
            "top": [],
            "around": [],
            "you": {"name": name, "pb_seconds": None, "rank": 1},
            "leader": None,
        }

    # 3) pool+你 在同 stroke 的所有成績（供 PB）
    names_for_sql = tuple(set(pool_names + [name]))
    in_clause = f"{names_for_sql}" if len(names_for_sql) > 1 else f"('{names_for_sql[0]}')"
    all_sql = f"""
        SELECT "姓名"::text AS name,
               "年份"::text AS y,
               "賽事名稱"::text AS m,
               "成績"::text AS r
    FROM {TABLE}
        WHERE "項目" ILIKE :pat
          AND "姓名" IN {in_clause}
        ORDER BY "姓名","年份"
        LIMIT 200000
    """
    all_rows = db.execute(text(all_sql), {"pat": pat}).mappings().all()

    # 4) PB（排除冬季短水道）
    def compute_pbs(rows) -> Dict[str, Tuple[float, str, str]]:
        best: Dict[str, Tuple[float, str, str]] = {}
        for rr in rows:
            if is_winter_short_course(rr["m"]):
                continue
            sec = parse_seconds(rr["r"])
            if sec is None:
                continue
            nm = rr["name"]
            if (nm not in best) or (sec < best[nm][0]):
                best[nm] = (sec, rr["y"], clean_meet_name(rr["m"]))
        return best

    best_map = compute_pbs(all_rows)
    opponents_best = [
        {"name": nm, "pb_seconds": best_map[nm][0], "pb_year": best_map[nm][1], "pb_meet": best_map[nm][2]}
        for nm in pool_names if nm in best_map
    ]
    you_pb = best_map.get(name)

    denom = len(opponents_best) + (1 if you_pb else 0)
    if denom == 0:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；若你的組別非數字則需同組別）",
            "denominator": 0,
            "rank": None,
            "percentile": None,
            "top": [],
            "around": [],
            "you": None,
            "leader": None,
        }

    sorted_all = sorted(
        ([{"name": name, "pb_seconds": you_pb[0], "pb_year": you_pb[1], "pb_meet": you_pb[2]}] if you_pb else [])
        + opponents_best,
        key=lambda x: x["pb_seconds"],
    )

    my_rank = None
    for idx, rec in enumerate(sorted_all, start=1):
        if rec["name"] == name:
            my_rank = idx
            break
    percentile = (100.0 * (denom - my_rank) / denom) if (my_rank and denom) else None

    leader = sorted_all[0] if sorted_all else None
    top = sorted_all[:10]

    around = []
    if my_rank:
        for j in range(max(1, my_rank - 3), min(denom, my_rank + 3) + 1):
            if j == my_rank:
                continue
            around.append({"name": sorted_all[j - 1]["name"], "pb_seconds": sorted_all[j - 1]["pb_seconds"], "rank": j})

    # 5) leader 趨勢（同 stroke，趨勢可包含冬短；僅 PB 排除冬短）
    leader_detail = None
    if leader:
        lsql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :nm AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        lrows = db.execute(text(lsql), {"nm": leader["name"], "pat": pat}).mappings().all()
        trend = []
        for rr in lrows:
            sec = parse_seconds(rr["r"])
            if sec is not None:
                trend.append({"year": rr["y"], "seconds": sec})
        leader_detail = {
            "name": leader["name"],
            "pb_seconds": leader["pb_seconds"],
            "rank": 1,
            "trend": trend,
        }

    you_detail = None
    if you_pb:
        you_detail = {"name": name, "pb_seconds": you_pb[0], "rank": my_rank}

    return {
        "name": name,
        "stroke": stroke,
        "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；若你的組別非數字則需同組別）",
        "denominator": denom,
        "rank": my_rank,
        "percentile": percentile,
        "top": top,
        "around": around,
        "you": you_detail,
        "leader": leader_detail,
    }
