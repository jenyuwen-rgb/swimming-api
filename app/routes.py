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

def is_winter_short_course(meet_name: Optional[str]) -> bool:
    """判斷是否為冬季短水道賽事（用於 PB 排除）"""
    if not meet_name:
        return False
    return "冬季短水道" in str(meet_name)

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
            # PB 計算排除冬季短水道
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
        best: Optional[Tuple[float,str,str]] = None  # (sec, y, m)

        for row in rows:
            count += 1
            raw_item = str(row["item"] or "")
            m = re.search(r"(\d+)\s*公尺", raw_item)
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            # PB 計算排除冬季短水道
            if is_winter_short_course(row["m"]):
                continue
            sec = parse_seconds(row["r"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], clean_meet_name(row["m"]))

        # 算最多距離
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

# ---------- RANK：含 leader 的完整趨勢資料 ----------
@router.get("/rank")
def rank_api(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="泳姿＋距離，例如：50公尺蛙式"),
    db: Session = Depends(get_db),
):
    """
    依『同年份＋同賽事名稱＋同項目 (+組別規則)』建立對手池。
    - 組別欄位若不是純數字 → 也必須相同組別
    - 若是數字 → 不加組別限制
    以對手池中每位選手在相同泳姿＋距離的 PB 排名（PB 計算排除冬季短水道）。
    另外回傳榜首在相同泳姿＋距離的『完整成績序列』（leaderTrendFull）。
    """
    # 先抓出輸入選手在該 stroke 的所有參賽場次，以建立對手池條件
    pat = f"%{stroke.strip()}%"
    base_sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS item,
               COALESCE("組別"::text, '') AS grp
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        LIMIT 5000
    """
    base_rows = db.execute(text(base_sql), {"name": name, "pat": pat}).mappings().all()
    if not base_rows:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
            "denominator": None,
            "rank": None,
            "percentile": None,
            "leader": None,
            "you": None,
            "top": [],
            "around": [],
            "opponents": [],
            "leaderTrendFull": [],
        }

    # 按場次建立 where 子句（含組別規則）
    conds = []
    params = {}
    for i, r in enumerate(base_rows):
        y = r["y"]; m = r["m"]; item = r["item"]; g = (r["grp"] or "").strip()
        keyy, keym, keyi, keyg = f"y{i}", f"m{i}", f"it{i}", f"g{i}"
        if g and not re.fullmatch(r"\d+", g):
            conds.append(f""" ("年份"::text = :{keyy} AND "賽事名稱"::text = :{keym} AND "項目"::text = :{keyi} AND COALESCE("組別"::text,'') = :{keyg}) """)
            params[keyg] = g
        else:
            conds.append(f""" ("年份"::text = :{keyy} AND "賽事名稱"::text = :{keym} AND "項目"::text = :{keyi}) """)
        params[keyy] = y; params[keym] = m; params[keyi] = item

    pool_sql = f"""
        SELECT DISTINCT "姓名"::text AS n
        FROM {TABLE}
        WHERE ({' OR '.join(conds)})
          AND "項目" ILIKE :pat
    """
    params["pat"] = pat
    pool_names = [r[0] for r in db.execute(text(pool_sql), params).all()]
    # 移除自己
    pool_names = [n for n in pool_names if n != name]

    # 對手池每人 PB（排除冬短）
    def pb_of(who: str) -> Optional[Tuple[float,str,str]]:
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :n AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"n": who, "pat": pat}).mappings().all()
        best = None
        for row in rows:
            if is_winter_short_course(row["m"]):
                continue
            sec = parse_seconds(row["r"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], clean_meet_name(row["m"]))
        return best

    ranked: List[Dict[str, Any]] = []
    for op in pool_names:
        b = pb_of(op)
        if b:
            ranked.append({"name": op, "pb": b[0], "pb_year": b[1], "pb_meet": b[2]})

    # 包含自己
    self_best = pb_of(name)
    if self_best:
        ranked.append({"name": name, "pb": self_best[0], "pb_year": self_best[1], "pb_meet": self_best[2]})

    # 依 PB 由小到大排序
    ranked.sort(key=lambda x: x["pb"])
    if not ranked or not self_best:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
            "denominator": len(ranked) or None,
            "rank": None,
            "percentile": None,
            "leader": ranked[0] if ranked else None,
            "you": None,
            "top": ranked[:10],
            "around": [],
            "opponents": pool_names,
            "leaderTrendFull": [],
        }

    denom = len(ranked)
    my_idx = next((i for i, x in enumerate(ranked) if x["name"] == name and abs(x["pb"] - self_best[0]) < 1e-9), None)
    my_rank = (my_idx + 1) if my_idx is not None else None
    leader = ranked[0] if ranked else None

    # 取榜首完整趨勢線（同泳姿＋距離的所有成績）
    leaderTrendFull: List[Dict[str, Any]] = []
    if leader:
        q_full = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r, "項目"::text AS item
            FROM {TABLE}
            WHERE "姓名" = :n AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q_full), {"n": leader["name"], "pat": pat}).mappings().all()
        for row in rows:
            sec = parse_seconds(row["r"])
            if sec and sec > 0:
                leaderTrendFull.append({
                    "year": row["y"],
                    "seconds": sec,
                    "meet": clean_meet_name(row["m"]),
                    "item": row["item"],
                })

    # around（鄰近）
    around = []
    if my_idx is not None:
        for i in range(max(0, my_idx - 3), min(denom, my_idx + 3 + 1)):
            if i == my_idx:
                continue
            x = ranked[i]
            around.append({"name": x["name"], "pb_seconds": x["pb"], "rank": i + 1})

    return {
        "name": name,
        "stroke": stroke,
        "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
        "denominator": denom,
        "rank": my_rank,
        "percentile": (100.0 * (denom - (my_rank or denom)) / denom) if my_rank else None,
        "leader": {"name": leader["name"], "pb_seconds": leader["pb"], "rank": 1} if leader else None,
        "you": {"name": name, "pb_seconds": self_best[0], "rank": my_rank} if my_rank else None,
        "top": ranked[:10],
        "around": around,
        "opponents": pool_names,
        "leaderTrendFull": leaderTrendFull,
    }

# ---------- debug helpers ----------
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

@router.get("/debug/rowcount")
def debug_rowcount(db: Session = Depends(get_db)):
    sql = f'SELECT COUNT(*) FROM {TABLE}'
    n = db.execute(text(sql)).scalar() or 0
    return {"table": TABLE, "rows": int(n)}

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

@router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    # 明細
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
    items = []
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

    # 分析（出賽、平均、PB；PB 排除冬短）
    meet_count = len(items)
    valid_secs = [x["seconds"] for x in items if isinstance(x["seconds"], (int, float)) and x["seconds"] > 0]
    avg_seconds = sum(valid_secs)/len(valid_secs) if valid_secs else None

    pb_seconds = None
    for it in rows:
        if is_winter_short_course(it["meet"]):
            continue
        sec = parse_seconds(it["result"])
        if sec is None:
            continue
        if pb_seconds is None or sec < pb_seconds:
            pb_seconds = sec

    # 四式專項統計
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    fam_out = {}
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
        dist_count: Dict[str,int] = {}
        best = None
        for row in rws:
            count += 1
            mm = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{mm.group(1)}公尺" if mm else ""
            if dist:
                dist_count[dist] = (dist_count.get(dist, 0) + 1)
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

    trend_points = [{"year": x["年份"], "seconds": x["seconds"]} for x in items if x["seconds"]]

    return {
        "analysis": {
            "meetCount": meet_count,
            "avg_seconds": avg_seconds,
            "pb_seconds": pb_seconds,
        },
        "family": fam_out,
        "trend": {"points": trend_points},
        "items": items,
        "nextCursor": next_cursor,
    }
