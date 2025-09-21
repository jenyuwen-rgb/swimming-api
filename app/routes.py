# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re
from .db import SessionLocal

router = APIRouter()

TABLE = "swimming_scores"

# ------------- DB session -------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------- helpers -------------
def parse_seconds(s: Optional[str]) -> Optional[float]:
    """把 'MM:SS.ss' 或 'SS.ss' 轉成秒；失敗回 None"""
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

# 簡化賽事（僅用於統計/展示；明細會保留原始名稱）
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

def is_winter_short_course(meet: str) -> bool:
    """冬季短水道的成績不計入 PB"""
    if not meet:
        return False
    s = str(meet)
    return ("冬季短水道" in s) or ("短水道" in s and "冬" in s)

def stroke_family(stroke_text: str) -> str:
    """從項目抓出泳式家族：蛙/仰/自/蝶，用於 family 統計"""
    s = str(stroke_text or "")
    for fam in ["蛙式", "仰式", "自由式", "蝶式"]:
        if fam in s:
            return fam
    return ""

def distance_from_item(item: str) -> Optional[str]:
    m = re.search(r"(\d+)\s*公尺", str(item or ""))
    return f"{m.group(1)}公尺" if m else None

def same_numeric_group(g: Optional[str]) -> bool:
    """組別是數字（或只含數字）"""
    if g is None:
        return False
    s = str(g).strip()
    return bool(re.fullmatch(r"\d+", s))

# ------------- health & debug -------------
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
def debug_strokes(name: str = Query(...), db: Session = Depends(get_db)) -> Dict[str, Any]:
    sql = f"""
        SELECT DISTINCT "項目"::text AS item
        FROM {TABLE}
        WHERE "姓名" = :name
        ORDER BY 1
        LIMIT 2000
    """
    rows = db.execute(text(sql), {"name": name}).all()
    return {"name": name, "strokes": [r[0] for r in rows]}

@router.get("/debug/names")
def debug_names(q: str = Query("", description="模糊關鍵字"), db: Session = Depends(get_db)):
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
def debug_name_detail(name: str = Query(...), db: Session = Depends(get_db)):
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
    n = db.execute(text(f'SELECT COUNT(*) FROM {TABLE}')).scalar() or 0
    return {"table": TABLE, "rows": int(n)}

@router.get("/debug/dbhint")
def debug_dbhint():
    import os, re as _re
    url = os.getenv("DATABASE_URL", "")
    masked = _re.sub(r"://([^:]+):[^@]+@", r"://\\1:***@", url)
    return {"DATABASE_URL_hint": masked}

# ------------- /api/results （給你除錯用；正式前端吃 /summary） -------------
@router.get("/results")
def results(
    name: str = Query(...),
    stroke: str = Query(..., description="例如 50公尺蛙式，可模糊"),
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
                COALESCE("組別"::text, '') AS grp,
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
                    "賽事名稱": r["meet"],  # 明細保留原始名稱
                    "項目": r["item"],
                    "姓名": r["swimmer"],
                    "成績": r["result"],
                    "名次": r["rank"],
                    "水道": r["lane"],
                    "組別": r["grp"],
                    "泳池長度": "",
                    "seconds": sec,
                }
            )
        next_cursor = cursor + limit if len(rows) == limit else None
        return {"items": items, "nextCursor": next_cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"results failed: {e}")

# ------------- /api/pb （仍可用；summary 會內含 PB） -------------
@router.get("/pb")
def pb(name: str = Query(...), stroke: str = Query(...), db: Session = Depends(get_db)):
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

# ------------- /api/summary -------------
@router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    # 1) 明細（指定泳姿＋距離；保留原始賽事名稱）
    pat = f"%{stroke.strip()}%"
    sql = f"""
        SELECT
            "年份"::text      AS year8,
            "賽事名稱"::text   AS meet,
            "項目"::text       AS item,
            "成績"::text       AS result,
            COALESCE("名次"::text, '') AS rank,
            COALESCE("水道"::text, '') AS lane,
            COALESCE("組別"::text, '') AS grp,
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
    valid_secs: List[float] = []
    for r in rows:
        sec = parse_seconds(r["result"])
        if sec is not None and sec > 0:
            valid_secs.append(sec)
        items.append({
            "年份": r["year8"],
            "賽事名稱": r["meet"],      # 明細維持原始字樣
            "項目": r["item"],
            "姓名": r["swimmer"],
            "成績": r["result"],
            "名次": r["rank"],
            "水道": r["lane"],
            "組別": r["grp"],
            "泳池長度": "",
            "seconds": sec,
        })
    next_cursor = cursor + limit if len(rows) == limit else None

    # 分析（出賽、平均、PB；PB 排除冬短）
    meet_count = len(items)
    avg_seconds = (sum(valid_secs) / len(valid_secs)) if valid_secs else None

    # PB：剔除冬短
    pb_seconds = None
    for r in rows:
        if is_winter_short_course(r["meet"]):
            continue
        s = parse_seconds(r["result"])
        if s is None or s <= 0:
            continue
        if pb_seconds is None or s < pb_seconds:
            pb_seconds = s

    # 2) 四式專項統計（不分距離）
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
        best = None  # (sec, y, m)
        for row in rws:
            count += 1
            d = distance_from_item(row["item"])
            if d:
                dist_count[d] = dist_count.get(d, 0) + 1
            sec = parse_seconds(row["r"])
            if sec is None:
                continue
            if is_winter_short_course(row["m"]):
                continue
            if best is None or sec < best[0]:
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

    # 3) 趨勢點（前端會自行找 PB 紅點）
    trend_points = [{"year": it["年份"], "seconds": it["seconds"]} for it in items if it["seconds"]]

    
    # ---- leaderTrend: 與 /rank 相同邏輯，找出對手池的榜首並回傳其完整歷史 ----
    leader_trend_points = []
    try:
        q_meets = f"""
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item, COALESCE("組別"::text, '') AS grp
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            GROUP BY "年份","賽事名稱","項目","組別"
            LIMIT 5000
        """
        base_meets = db.execute(text(q_meets), {"name": name, "pat": pat}).mappings().all()

        opponents = {}
        for m in base_meets:
            year8 = m["year8"]; meet = m["meet"]; item = m["item"]; grp = m["grp"]
            if same_numeric_group(grp):
                q_opp = f"""
                    SELECT DISTINCT "姓名"::text AS nm
                    FROM {TABLE}
                    WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i
                    LIMIT 5000
                """
                rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item}).mappings().all()
            else:
                q_opp = f"""
                    SELECT DISTINCT "姓名"::text AS nm
                    FROM {TABLE}
                    WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i AND COALESCE("組別"::text,'') = :g
                    LIMIT 5000
                """
                rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item, "g": grp}).mappings().all()
            for r in rows:
                nm = r["nm"]
                if nm and nm != name:
                    opponents[nm] = True

        def best_of(player: str):
            q = f"""
                SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                FROM {TABLE}
                WHERE "姓名" = :player AND "項目" ILIKE :pat
                ORDER BY "年份" ASC
                LIMIT 5000
            """
            rows = db.execute(text(q), {"player": player, "pat": pat}).mappings().all()
            best = None
            for row in rows:
                if is_winter_short_course(row["m"]):
                    continue
                sec = parse_seconds(row["r"])
                if sec is None or sec <= 0:
                    continue
                if best is None or sec < best[0]:
                    best = (sec, row["y"], clean_meet_name(row["m"]))
            return best

        board = []
        all_names = list(opponents.keys())
        if name not in all_names:
            all_names.append(name)
        for nm in all_names:
            b = best_of(nm)
            if b:
                board.append({"name": nm, "pb": b[0]})
        board.sort(key=lambda x: x["pb"])
        if board:
            leader_name = board[0]["name"]
            q_leader = f"""
                SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                FROM {TABLE}
                WHERE "姓名" = :player AND "項目" ILIKE :pat
                ORDER BY "年份" ASC
                LIMIT 5000
            """
            for row in db.execute(text(q_leader), {"player": leader_name, "pat": pat}).mappings():
                sec = parse_seconds(row["r"])
                if sec is None or sec <= 0:
                    continue
                leader_trend_points.append({"year": row["y"], "seconds": sec, "meet": row["m"]})
    except Exception:
        leader_trend_points = []

    # ---- leaderTrend: 與 /rank 相同邏輯，找出對手池的榜首並回傳其完整歷史 ----
    leader_trend_points = []
    try:
        q_meets = f"""
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item, COALESCE("組別"::text, '') AS grp
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            GROUP BY "年份","賽事名稱","項目","組別"
            LIMIT 5000
        """
        base_meets = db.execute(text(q_meets), {"name": name, "pat": pat}).mappings().all()

        opponents = {}
        for m in base_meets:
            year8 = m["year8"]; meet = m["meet"]; item = m["item"]; grp = m["grp"]
            if same_numeric_group(grp):
                q_opp = f"""
                    SELECT DISTINCT "姓名"::text AS nm
                    FROM {TABLE}
                    WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i
                    LIMIT 5000
                """
                rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item}).mappings().all()
            else:
                q_opp = f"""
                    SELECT DISTINCT "姓名"::text AS nm
                    FROM {TABLE}
                    WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i AND COALESCE("組別"::text,'') = :g
                    LIMIT 5000
                """
                rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item, "g": grp}).mappings().all()
            for r in rows:
                nm = r["nm"]
                if nm and nm != name:
                    opponents[nm] = True

        def best_of(player: str):
            q = f"""
                SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                FROM {TABLE}
                WHERE "姓名" = :player AND "項目" ILIKE :pat
                ORDER BY "年份" ASC
                LIMIT 5000
            """
            rows = db.execute(text(q), {"player": player, "pat": pat}).mappings().all()
            best = None
            for row in rows:
                if is_winter_short_course(row["m"]):
                    continue
                sec = parse_seconds(row["r"])
                if sec is None or sec <= 0:
                    continue
                if best is None or sec < best[0]:
                    best = (sec, row["y"], clean_meet_name(row["m"]))
            return best

        board = []
        all_names = list(opponents.keys())
        if name not in all_names:
            all_names.append(name)
        for nm in all_names:
            b = best_of(nm)
            if b:
                board.append({"name": nm, "pb": b[0]})
        board.sort(key=lambda x: x["pb"])
        if board:
            leader_name = board[0]["name"]
            q_leader = f"""
                SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                FROM {TABLE}
                WHERE "姓名" = :player AND "項目" ILIKE :pat
                ORDER BY "年份" ASC
                LIMIT 5000
            """
            for row in db.execute(text(q_leader), {"player": leader_name, "pat": pat}).mappings():
                sec = parse_seconds(row["r"])
                if sec is None or sec <= 0:
                    continue
                leader_trend_points.append({"year": row["y"], "seconds": sec, "meet": row["m"]})
    except Exception:
        leader_trend_points = []

        # ---- leaderTrend: 與 /rank 相同邏輯，找出對手池的榜首並回傳其完整歷史 ----
        leader_trend_points = []
        try:
            q_meets = f"""
                SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item, COALESCE("組別"::text, '') AS grp
                FROM {TABLE}
                WHERE "姓名" = :name AND "項目" ILIKE :pat
                GROUP BY "年份","賽事名稱","項目","組別"
                LIMIT 5000
            """
            base_meets = db.execute(text(q_meets), {"name": name, "pat": pat}).mappings().all()

            opponents = {}
            for m in base_meets:
                year8 = m["year8"]; meet = m["meet"]; item = m["item"]; grp = m["grp"]
                if same_numeric_group(grp):
                    q_opp = f"""
                        SELECT DISTINCT "姓名"::text AS nm
                        FROM {TABLE}
                        WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i
                        LIMIT 5000
                    """
                    rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item}).mappings().all()
                else:
                    q_opp = f"""
                        SELECT DISTINCT "姓名"::text AS nm
                        FROM {TABLE}
                        WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :i AND COALESCE("組別"::text,'') = :g
                        LIMIT 5000
                    """
                    rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item, "g": grp}).mappings().all()
                for r in rows:
                    nm = r["nm"]
                    if nm and nm != name:
                        opponents[nm] = True

            def best_of(player: str):
                q = f"""
                    SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                    FROM {TABLE}
                    WHERE "姓名" = :player AND "項目" ILIKE :pat
                    ORDER BY "年份" ASC
                    LIMIT 5000
                """
                rows = db.execute(text(q), {"player": player, "pat": pat}).mappings().all()
                best = None
                for row in rows:
                    if is_winter_short_course(row["m"]):
                        continue
                    sec = parse_seconds(row["r"])
                    if sec is None or sec <= 0:
                        continue
                    if best is None or sec < best[0]:
                        best = (sec, row["y"], clean_meet_name(row["m"]))
                return best

            board = []
            all_names = list(opponents.keys())
            if name not in all_names:
                all_names.append(name)
            for nm in all_names:
                b = best_of(nm)
                if b:
                    board.append({"name": nm, "pb": b[0]})
            board.sort(key=lambda x: x["pb"])
            if board:
                leader_name = board[0]["name"]
                q_leader = f"""
                    SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
                    FROM {TABLE}
                    WHERE "姓名" = :player AND "項目" ILIKE :pat
                    ORDER BY "年份" ASC
                    LIMIT 5000
                """
                for row in db.execute(text(q_leader), {"player": leader_name, "pat": pat}).mappings():
                    sec = parse_seconds(row["r"])
                    if sec is None or sec <= 0:
                        continue
                    leader_trend_points.append({"year": row["y"], "seconds": sec, "meet": row["m"]})
        except Exception:
            leader_trend_points = []

    return {
    "analysis": {
        "meetCount": meet_count,
        "avg_seconds": avg_seconds,
        "pb_seconds": pb_seconds,
    },
    "family": fam_out,
    "trend": {"points": trend_points},
    "leaderTrend": {"points": leader_trend_points},
    "items": items,
    "nextCursor": next_cursor,
}
@router.get("/rank")
def rank_api(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    db: Session = Depends(get_db),
):
    """
    1) 先找出「輸入選手」在該泳姿＋距離下的所有出賽場次（year, meet, item, group）。
    2) 對每一場，在同 (year, meet, item) 下抓到所有共同參賽者名單；
       若該場的 group 不是純數字，則同時要求 group 相同。
    3) 把所有對手去重後，為每人計算 PB（限定同泳姿＋距離、剔除冬季短水道）。
    4) 依 PB 做全體排序，回傳：
       - denominator（分母）、rank（你的名次）、percentile、leader（榜首）、top（前10）
       - leaderTrend（榜首完整歷年同泳姿＋距離的所有成績）
    """
    pat = f"%{stroke.strip()}%"

    # 1) 取輸入選手在該泳姿＋距離的出賽清單（含組別）
    q_meets = f"""
        SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item,
               COALESCE("組別"::text, '') AS grp
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        GROUP BY "年份","賽事名稱","項目","組別"
        LIMIT 5000
    """
    base_meets = db.execute(text(q_meets), {"name": name, "pat": pat}).mappings().all()

    # 2) 建立對手池
    opponents: Dict[str, Dict[str, Any]] = {}  # name -> { name }
    for m in base_meets:
        year8 = m["year8"]; meet = m["meet"]; item = m["item"]; grp = m["grp"]
        # 這一場若組別不是數字，就限定同組別；是數字則不限制
        if same_numeric_group(grp):
            q_opp = f"""
                SELECT DISTINCT "姓名"::text AS nm
                FROM {TABLE}
                WHERE "年份"::text = :y AND "賽事名稱"::text = :m
                  AND "項目"::text = :i
                  AND "姓名"::text <> :name
                LIMIT 10000
            """
            rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item, "name": name}).all()
        else:
            q_opp = f"""
                SELECT DISTINCT "姓名"::text AS nm
                FROM {TABLE}
                WHERE "年份"::text = :y AND "賽事名稱"::text = :m
                  AND "項目"::text = :i
                  AND COALESCE("組別"::text,'') = :g
                  AND "姓名"::text <> :name
                LIMIT 10000
            """
            rows = db.execute(text(q_opp), {"y": year8, "m": meet, "i": item, "g": grp, "name": name}).all()
        for r in rows:
            nm = r[0]
            if nm not in opponents:
                opponents[nm] = {"name": nm}

    # 若完全抓不到，直接回空
    if not opponents:
        return {
            "name": name, "stroke": stroke, "criteria": "同年份＋同賽事名稱＋同項目（必要時再加同組別）",
            "denominator": 0, "rank": None, "percentile": None,
            "leader": None, "you": None, "around": [], "top": [], "leaderTrend": []
        }

    # 3) 幫每位對手算 PB（同泳姿＋距離、剔除冬短）
    def best_of(player: str) -> Optional[Tuple[float, str, str]]:
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :player AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"player": player, "pat": pat}).mappings().all()
        best = None
        for row in rows:
            if is_winter_short_course(row["m"]):
                continue
            sec = parse_seconds(row["r"])
            if sec is None or sec <= 0:
                continue
            if best is None or sec < best[0]:
                best = (sec, row["y"], clean_meet_name(row["m"]))
        return best

    board: List[Dict[str, Any]] = []
    # 也把本人納入排名
    all_names = list(opponents.keys())
    if name not in all_names:
        all_names.append(name)

    for nm in all_names:
        b = best_of(nm)
        if b:
            board.append({"name": nm, "pb": b[0], "pb_year": b[1], "pb_meet": b[2]})

    if not board:
        return {
            "name": name, "stroke": stroke, "criteria": "同年份＋同賽事名稱＋同項目（必要時再加同組別）",
            "denominator": 0, "rank": None, "percentile": None,
            "leader": None, "you": None, "around": [], "top": [], "leaderTrend": []
        }

    # 排序
    board.sort(key=lambda x: x["pb"])
    for i, row in enumerate(board, start=1):
        row["rank"] = i

    denominator = len(board)
    you = next((x for x in board if x["name"] == name), None)
    rank_no = you["rank"] if you else None
    percentile = (100 * (denominator - rank_no) / denominator) if rank_no else None

    top10 = board[:10]
    leader = board[0]

    # 鄰近名單（含你前後幾名）
    around = []
    if you:
        idx = you["rank"] - 1
        for j in range(max(0, idx - 3), min(denominator, idx + 3)):
            if j != idx:
                around.append(board[j])

    # 4) 榜首完整趨勢線（同泳姿＋距離的所有成績；不做冬短剔除，因為你要看趨勢）
    leader_trend: List[Dict[str, Any]] = []
    q_leader = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :player AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    for row in db.execute(text(q_leader), {"player": leader["name"], "pat": pat}).mappings():
        sec = parse_seconds(row["r"])
        if sec is None or sec <= 0:
            continue
        leader_trend.append({"year": row["y"], "seconds": sec, "meet": row["m"]})

    return {
        "name": name,
        "stroke": stroke,
        "criteria": "同年份＋同賽事名稱＋同項目（必要時再加同組別）",
        "denominator": denominator,
        "rank": rank_no,
        "percentile": percentile,
        "leader": {"name": leader["name"], "pb_seconds": leader["pb"], "rank": 1},
        "you": {"name": name, "pb_seconds": you["pb"] if you else None,
                "pb_year": you["pb_year"] if you else None,
                "pb_meet": you["pb_meet"] if you else None,
                "rank": rank_no},
        "around": around,
        "top": top10,
        "leaderTrend": leader_trend,
    }
