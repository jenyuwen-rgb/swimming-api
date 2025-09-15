# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re

from .db import SessionLocal

router = APIRouter()

# -------------------- DB session --------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

TABLE = "swimming_scores"

# -------------------- helpers --------------------
def parse_seconds(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip()
    try:
        if ":" in s:
            m, sec = s.split(":")
            return int(m) * 60 + float(sec)
        return float(s)
    except Exception:
        return None

SHORT_COURSE_PAT = re.compile(r"(冬季短水道|短水道)", re.I)

def is_short_course(meet_name: Optional[str]) -> bool:
    return bool(meet_name and SHORT_COURSE_PAT.search(str(meet_name)))

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
    out = str(name).strip()
    for src, repl in _MEET_MAP:
        if src in out:
            out = out.replace(src, repl)
    for pat, repl in _MEET_REGEX:
        out = pat.sub(repl, out)
    return re.sub(r"\s{2,}", " ", out).strip()

def user_year_range(db: Session, name: str, stroke_like: str) -> Tuple[Optional[str], Optional[str]]:
    """
    取該選手在指定泳姿＋距離之下的最小/最大 年份(YYYYMMDD)；若無資料回 (None, None)
    """
    sql = f"""
        SELECT MIN("年份")::text AS ymin, MAX("年份")::text AS ymax
        FROM {TABLE}
        WHERE "姓名" = :name
          AND "項目" ILIKE :pat
    """
    row = db.execute(text(sql), {"name": name, "pat": f"%{stroke_like}%"}).mappings().first()
    if not row or not row["ymin"] or not row["ymax"]:
        return (None, None)
    return (row["ymin"], row["ymax"])

def sec_min_record(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best = None
    for r in rows:
        sec = parse_seconds(r.get("result"))
        if sec is None:
            continue
        if is_short_course(r.get("meet")):
            # 剔除冬季短水道，不納入 PB
            continue
        if best is None or sec < best["seconds"]:
            best = {
                "seconds": sec,
                "year8": r.get("year8"),
                "meet": clean_meet_name(r.get("meet")),
            }
    return best

# -------------------- health/debug --------------------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

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
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    指定選手 + 指定泳姿距離的明細（供「成績分析 / 趨勢 / 細節」）
    """
    try:
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
        params = {
            "name": name,
            "pat": f"%{stroke}%",
            "limit": limit,
            "offset": cursor,
        }
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
    """
    取指定泳姿＋距離下的 PB（剔除冬季短水道）
    """
    try:
        sql = f"""
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(sql), {"name": name, "pat": f"%{stroke}%"}).mappings().all()
        best = sec_min_record(rows)
        if not best:
            return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}
        return {"name": name, "stroke": stroke, "pb_seconds": best["seconds"], "year": best["year8"], "from_meet": best["meet"]}
    except Exception:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

@router.get("/stats/family")
def stats_family(
    name: str = Query(..., description="選手姓名"),
    db: Session = Depends(get_db),
):
    """
    四式統計（不分距離），PB 亦剔除冬季短水道
    """
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    out: Dict[str, Any] = {}
    for fam in families:
        sql = f"""
            SELECT
              "年份"::text      AS year8,
              "賽事名稱"::text   AS meet,
              "成績"::text       AS result,
              "項目"::text       AS item
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(sql), {"name": name, "pat": f"%{fam}%"}).mappings().all()

        count = 0
        dist_count: Dict[str, int] = {}
        best = None  # (seconds, year8, meet)
        for r in rows:
            count += 1
            m = re.search(r"(\d+)\s*公尺", str(r["item"] or ""))
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            sec = parse_seconds(r["result"])
            if sec is None:
                continue
            if is_short_course(r["meet"]):
                continue
            if best is None or sec < best[0]:
                best = (sec, r["year8"], clean_meet_name(r["meet"]))

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

# -------------------- /summary 與 /rank --------------------
@router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(500, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    前端主要讀取：成績分析 + 趨勢資料（含 PB 紅點）+ 四式統計。
    並額外回傳 leaderTrend（榜首的同泳姿＋距離，且限制在【輸入選手的歷年區間】內；剔除冬季短水道）。
    """
    # 1) 取使用者明細
    items_res = results(name=name, stroke=stroke, limit=limit, cursor=cursor, db=db)  # type: ignore
    items: List[Dict[str, Any]] = items_res.get("items", [])  # already cleaned
    next_cursor = items_res.get("nextCursor")

    # 2) 分析
    secs = [x["seconds"] for x in items if isinstance(x.get("seconds"), (int, float)) and x["seconds"] > 0]
    avg_seconds = sum(secs) / len(secs) if secs else None
    pb_seconds = min(secs) if secs else None
    pb_point = None
    if pb_seconds is not None:
        for it in items:
            if it["seconds"] == pb_seconds:
                pb_point = {"year": it["年份"], "seconds": it["seconds"]}
                break

    # 3) 四式統計
    fam = stats_family(name=name, db=db)  # type: ignore

    # 4) 趨勢
    trend_points = [{"year": it["年份"], "seconds": it["seconds"]} for it in items if it.get("seconds")]
    # 排序（YYYYMMDD asc）
    trend_points.sort(key=lambda x: x["year"])

    # 5) leaderTrend（依 /rank 找出榜首，再撈榜首的資料；且限制在使用者的年份區間）
    leaderTrend = {"name": None, "points": []}  # default
    try:
        # 使用者年份區間
        ymin, ymax = user_year_range(db, name, stroke)
        # 若沒有年份區間，直接回基本結構
        if ymin and ymax:
            # 找榜首（和 /rank 一致：在「同年份＋同賽事＋同項目」的對手池內、PB 同泳姿距離且剔除短水道）
            rank_data = rank(name=name, stroke=stroke, db=db)  # type: ignore
            leader = (rank_data or {}).get("leader")  # {"name":..., "pb_seconds":..., "rank":1}
            if leader and leader.get("name"):
                leader_name = leader["name"]

                # 撈榜首同泳姿＋距離，且年份限制在 [ymin, ymax]
                sql = f"""
                    SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
                    FROM {TABLE}
                    WHERE "姓名" = :leader
                      AND "項目" ILIKE :pat
                      AND "年份" BETWEEN :ymin AND :ymax
                    ORDER BY "年份" ASC
                    LIMIT 5000
                """
                lrows = db.execute(
                    text(sql),
                    {"leader": leader_name, "pat": f"%{stroke}%", "ymin": ymin, "ymax": ymax},
                ).mappings().all()

                pts: List[Dict[str, Any]] = []
                for r in lrows:
                    if is_short_course(r["meet"]):
                        continue  # 剔除冬短
                    sec = parse_seconds(r["result"])
                    if sec:
                        pts.append({"year": r["year8"], "seconds": sec})
                pts.sort(key=lambda x: x["year"])
                leaderTrend = {"name": leader_name, "points": pts}
    except Exception:
        pass

    return {
        "analysis": {"meetCount": len(items), "avg_seconds": avg_seconds, "pb_seconds": pb_seconds, "pb_point": pb_point},
        "family": fam,
        "trend": {"points": trend_points},
        "leaderTrend": leaderTrend,
        "items": items,
        "nextCursor": next_cursor,
    }

@router.get("/rank")
def rank(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    db: Session = Depends(get_db),
):
    """
    排行（同年份＋同賽事名稱＋同項目 -> 對手池）
    - PB 僅計入同泳姿＋距離，且剔除冬季短水道
    - 另外 **新增條件**：對手的 PB 計算與排行只採用「輸入選手在該泳姿距離的歷年區間內」的成績
    """
    # 先找使用者的年份區間（無則無法排行）
    ymin, ymax = user_year_range(db, name, stroke)
    if not ymin or not ymax:
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
        }

    # 1) 取出使用者曾經參與的（年、賽事、項目）組合（限制在該泳姿＋距離 & 年份區間）
    user_meets_sql = f"""
        SELECT DISTINCT "年份"::text AS year8, "賽事名稱"::text AS meet, "項目"::text AS item
        FROM {TABLE}
        WHERE "姓名" = :name
          AND "項目" ILIKE :pat
          AND "年份" BETWEEN :ymin AND :ymax
        LIMIT 10000
    """
    user_meets = db.execute(
        text(user_meets_sql),
        {"name": name, "pat": f"%{stroke}%", "ymin": ymin, "ymax": ymax},
    ).mappings().all()

    if not user_meets:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
            "denominator": 0,
            "rank": None,
            "percentile": None,
            "leader": None,
            "you": None,
            "top": [],
            "around": [],
            "opponents": [],
        }

    # 2) 對手池：在這些（年份、賽事、項目）中出現過的所有姓名（含自己）
    opp_sql = f"""
        SELECT DISTINCT "姓名"::text AS name
        FROM {TABLE}
        WHERE ("年份","賽事名稱","項目") IN (
            SELECT "年份","賽事名稱","項目"
            FROM {TABLE}
            WHERE "姓名" = :name
              AND "項目" ILIKE :pat
              AND "年份" BETWEEN :ymin AND :ymax
        )
    """
    opp_rows = db.execute(
        text(opp_sql),
        {"name": name, "pat": f"%{stroke}%", "ymin": ymin, "ymax": ymax},
    ).mappings().all()
    opp_names = [r["name"] for r in opp_rows]

    # 3) 計算每個對手的 PB（僅限：同泳姿＋距離、非冬短、且年份 BETWEEN [ymin, ymax]）
    def pb_for(swimmer: str) -> Optional[Dict[str, Any]]:
        sql = f"""
            SELECT "年份"::text AS year8, "賽事名稱"::text AS meet, "成績"::text AS result
            FROM {TABLE}
            WHERE "姓名" = :swimmer
              AND "項目" ILIKE :pat
              AND "年份" BETWEEN :ymin AND :ymax
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(
            text(sql),
            {"swimmer": swimmer, "pat": f"%{stroke}%", "ymin": ymin, "ymax": ymax},
        ).mappings().all()
        best = sec_min_record(rows)
        if not best:
            return None
        return {"name": swimmer, "pb_seconds": best["seconds"], "pb_year": best["year8"], "pb_meet": best["meet"]}

    opp_pbs: List[Dict[str, Any]] = []
    for n in opp_names:
        p = pb_for(n)
        if p:
            opp_pbs.append(p)

    if not opp_pbs:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手）",
            "denominator": 0,
            "rank": None,
            "percentile": None,
            "leader": None,
            "you": None,
            "top": [],
            "around": [],
            "opponents": opp_names,
        }

    opp_pbs.sort(key=lambda x: x["pb_seconds"])
    denom = len(opp_pbs)

    # 找自己的 PB 記錄
    you = next((x for x in opp_pbs if x["name"] == name), None)

    # 排名
    myRank = None
    if you:
        myRank = opp_pbs.index(you) + 1

    # 百分位（越小越快 → 以名次換算百分位）
    percentile = None
    if myRank and denom:
        percentile = (1 - (myRank - 1) / denom) * 100

    leader = {"name": opp_pbs[0]["name"], "pb_seconds": opp_pbs[0]["pb_seconds"], "rank": 1}
    top = opp_pbs[:10]

    # around（你前後各三名）
    around: List[Dict[str, Any]] = []
    if myRank:
        i = myRank - 1
        left = max(i - 3, 0)
        right = min(i + 3, denom - 1)
        for j in range(left, right + 1):
            if 0 <= j < denom and j != i:
                item = dict(opp_pbs[j])
                item["rank"] = j + 1
                around.append(item)

    return {
        "name": name,
        "stroke": stroke,
        "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；PB 限同泳姿距離、剔除冬短，且只採用你的歷年區間）",
        "denominator": denom,
        "rank": myRank,
        "percentile": percentile,
        "leader": leader,
        "you": (you | {"rank": myRank}) if you else None,
        "top": [{**x, "rank": i + 1} for i, x in enumerate(top)],
        "around": around,
        "opponents": opp_names,
    }