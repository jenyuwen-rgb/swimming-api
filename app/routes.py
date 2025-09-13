# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
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

        # 先組 items（含 seconds），等等標記 PB
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

        # 找本頁結果中的 PB（>0 的最小秒數）
        valid_secs = [x["seconds"] for x in items if isinstance(x["seconds"], (int, float)) and x["seconds"] > 0]
        pb_seconds = min(valid_secs) if valid_secs else None

        # 標記 is_pb
        for x in items:
            x["is_pb"] = (pb_seconds is not None and isinstance(x["seconds"], (int, float)) and x["seconds"] == pb_seconds)

        next_cursor = cursor + limit if len(rows) == limit else None
        return {
            "debug_sql": sql,
            "params": params,
            "pb_seconds": pb_seconds,
            "items": items,
            "nextCursor": next_cursor,
        }
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
        best = None  # (sec, y, m)

        for row in rows:
            # 1) 出賽數：不看成績格式，直接累加
            count += 1

            # 2) 距離：從「項目」抓出 (\d+)公尺
            raw_item = str(row["item"] or "")
            m = re.search(r"(\d+)\s*公尺", raw_item)
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            # 3) PB：只有成績可解析時才參與
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
    # 只回一些不敏感片段，幫你確認連到哪個 DB
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
    # 1) 取指定泳姿＋距離的明細（供「成績與專項分析」、「成績趨勢」、「詳細成績」）
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
    secs = []
    for r in rows:
        s = parse_seconds(r["result"])
        if isinstance(s, (int, float)):
            secs.append({"sec": s, "y": r["year8"]})
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

    # 分析（出賽、平均、PB）
    meet_count = len(items)
    valid = [x["seconds"] for x in items if isinstance(x["seconds"], (int, float)) and x["seconds"] > 0]
    avg_seconds = sum(valid)/len(valid) if valid else None
    pb_seconds = min(valid) if valid else None

    # 2) 四式專項統計（不分距離）
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
        dist_count = {}
        best = None
        for row in rws:
            count += 1
            mm = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{mm.group(1)}公尺" if mm else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1
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

    # 3) 趨勢點（給前端直接畫）
    trend_points = [{"year": x["年份"], "seconds": x["seconds"]} for x in items if x["seconds"]]

    return {
        "analysis": {
            "meetCount": meet_count,
            "avg_seconds": avg_seconds,
            "pb_seconds": pb_seconds,
        },
        "family": fam_out,
        "trend": {
            "points": trend_points,
        },
        "items": items,
        "nextCursor": next_cursor,
    }
@router.get("/rank")
def rank_api(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="同距離＋同泳式（例如：50公尺蛙式）"),
    db: Session = Depends(get_db),
):
    """
    對手池：以「同年份＋同賽事名稱＋同項目」為準（你實際參加過的那些組別）
    對手名單：從上述所有組別把參賽者（姓名）蒐集起來（含自己）
    排名依據：各自在同 stroke（同距離＋同泳式）下的全期 PB（最小秒數）
    回傳：名次、分母、百分位、榜首、以及鄰近名單
    """
    try:
        pat = f"%{stroke.strip()}%"

        # 1) 你參加過的「同年份＋同賽事名稱＋同項目」清單（限定 stroke）
        ev_sql = f"""
            SELECT DISTINCT "年份"::text AS y, "賽事名稱"::text AS m, "項目"::text AS i
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            LIMIT 5000
        """
        ev_rows = db.execute(text(ev_sql), {"name": name, "pat": pat}).mappings().all()
        if not ev_rows:
            return {
                "name": name, "stroke": stroke,
                "denominator": 0, "rank": None, "percentile": None,
                "leader": None, "around": [], "opponents": []
            }

        # 2) 蒐集對手名單（含自己），條件 = 同年份＋同賽事名稱＋同項目
        #    先做一個暫存表（用 tuple 進行多組 OR-條件）
        combos = [(r["y"], r["m"], r["i"]) for r in ev_rows]
        # 動態組 WHERE (y,m,i) IN ((...),(...))
        tuple_sql_parts = []
        params: Dict[str, Any] = {"name": name, "pat": pat}
        for idx, (yy, mm, ii) in enumerate(combos):
            params[f"y{idx}"] = yy
            params[f"m{idx}"] = mm
            params[f"i{idx}"] = ii
            tuple_sql_parts.append(f"(:y{idx}, :m{idx}, :i{idx})")

        opp_sql = f"""
            SELECT DISTINCT s."姓名"::text AS n
            FROM {TABLE} s
            WHERE (s."年份"::text, s."賽事名稱"::text, s."項目"::text) IN (
                {", ".join(tuple_sql_parts)}
            )
            LIMIT 20000
        """
        opp_rows = db.execute(text(opp_sql), params).all()
        names = [r[0] for r in opp_rows]
        # 保險去重
        names = sorted(set(names))
        if not names:
            return {
                "name": name, "stroke": stroke,
                "denominator": 0, "rank": None, "percentile": None,
                "leader": None, "around": [], "opponents": []
            }

        # 3) 取出對手名單在「同 stroke」下的所有成績，計算各自 PB
        #    用 IN (...) 動態參數
        in_placeholders = ", ".join([f":n{idx}" for idx in range(len(names))])
        score_params = {"pat": pat, **{f"n{idx}": n for idx, n in enumerate(names)}}
        sc_sql = f"""
            SELECT "姓名"::text AS n, "成績"::text AS r
            FROM {TABLE}
            WHERE "項目" ILIKE :pat
              AND "姓名" IN ({in_placeholders})
            LIMIT 500000
        """
        sc_rows = db.execute(text(sc_sql), score_params).mappings().all()

        # 聚合 PB
        best_by_name: Dict[str, float] = {}
        for row in sc_rows:
            sec = parse_seconds(row["r"])
            if sec is None:
                continue
            n = row["n"]
            if (n not in best_by_name) or (sec < best_by_name[n]):
                best_by_name[n] = sec

        # 4) 形成排行榜（只保留有 PB 的人）
        board = [{"name": n, "pb_seconds": best_by_name[n]} for n in best_by_name.keys()]
        if not board:
            return {
                "name": name, "stroke": stroke,
                "denominator": 0, "rank": None, "percentile": None,
                "leader": None, "around": [], "opponents": names
            }

        board.sort(key=lambda x: x["pb_seconds"])
        denom = len(board)

        # 找自己的 PB 與名次
        you_idx = next((i for i, x in enumerate(board) if x["name"] == name), None)
        you_rank = (you_idx + 1) if you_idx is not None else None
        percentile = (100.0 * (denom - you_rank) / denom) if you_rank else None  # 越小越好，越接近 100% 表示越前面

        # 榜首
        leader = board[0]

        # 鄰近（各取 3 名上下）
        around = []
        if you_idx is not None:
            lo = max(0, you_idx - 3)
            hi = min(denom, you_idx + 4)
            for i in range(lo, hi):
                if i == you_idx:
                    continue
                item = board[i].copy()
                item["rank"] = i + 1
                around.append(item)

        # 最後附上你自己的資料
        you = None
        if you_idx is not None:
            you = board[you_idx].copy()
            you["rank"] = you_rank

        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
            "denominator": denom,
            "rank": you_rank,
            "percentile": percentile,
            "leader": {"name": leader["name"], "pb_seconds": leader["pb_seconds"], "rank": 1},
            "you": you,
            "around": around,
            "opponents": names,  # 原始對手名單（含無 PB 者）
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"rank failed: {e}")
        
@router.get("/debug/common_meets")
def debug_common_meets(
    name: str = Query(..., description="對手姓名"),
    base: str = Query("温心妤", description="基準選手姓名"),
    db: Session = Depends(get_db),
):
    sql = f"""
        SELECT DISTINCT
          a."年份"::text AS year,
          a."賽事名稱"::text AS meet,
          a."項目"::text AS item
        FROM {TABLE} a
        JOIN {TABLE} b
          ON a."年份" = b."年份"
         AND a."賽事名稱" = b."賽事名稱"
         AND a."項目" = b."項目"
        WHERE a."姓名" = :base
          AND b."姓名" = :name
        ORDER BY year, meet, item
    """
    rows = db.execute(text(sql), {"base": base, "name": name}).mappings().all()
    return {"base": base, "name": name, "common": rows}
@router.get("/debug/opponents")
def debug_opponents(
    base: str = Query("温心妤", description="基準選手"),
    stroke: str = Query(..., description="指定項目（例如 50公尺蛙式）"),
    db: Session = Depends(get_db),
):
    # 找所有與 base 在同項目的對手
    sql = f"""
        SELECT DISTINCT b."姓名"::text AS opponent,
                        a."年份"::text AS year,
                        a."賽事名稱"::text AS meet,
                        a."項目"::text AS item
        FROM {TABLE} a
        JOIN {TABLE} b
          ON a."年份" = b."年份"
         AND a."賽事名稱" = b."賽事名稱"
         AND a."項目" = b."項目"
        WHERE a."姓名" = :base
          AND a."項目" ILIKE :pat
          AND b."姓名" <> :base
        ORDER BY b."姓名", a."年份"
    """
    rows = db.execute(text(sql), {"base": base, "pat": f"%{stroke}%"}).mappings().all()

    opponents = {}
    for r in rows:
        opp = r["opponent"]
        if opp not in opponents:
            opponents[opp] = {"opponent": opp, "pb": None, "pb_year": None, "pb_meet": None, "meets": []}
        opponents[opp]["meets"].append({"year": r["year"], "meet": r["meet"], "item": r["item"]})

    # 幫每個對手抓 PB
    for opp in opponents.values():
        sql_pb = f"""
            SELECT "年份"::text AS year, "賽事名稱"::text AS meet, "成績"::text AS result
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
        """
        rows_pb = db.execute(text(sql_pb), {"name": opp["opponent"], "pat": f"%{stroke}%"}).mappings().all()
        best = None
        for r in rows_pb:
            sec = parse_seconds(r["result"])
            if sec is None:
                continue
            if best is None or sec < best[0]:
                best = (sec, r["year"], clean_meet_name(r["meet"]))
        if best:
            opp["pb"] = best[0]
            opp["pb_year"] = best[1]
            opp["pb_meet"] = best[2]

    # 依 PB 排序，沒有 PB 的排最後
    sorted_opps = sorted(opponents.values(), key=lambda x: (x["pb"] is None, x["pb"]))

    return {"base": base, "stroke": stroke, "opponents": sorted_opps}
