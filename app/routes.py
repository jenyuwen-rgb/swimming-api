# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re
from .db import SessionLocal

router = APIRouter()
TABLE = "swimming_scores"

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

def is_winter_short_course(meet: str) -> bool:
    """冬季短水道相關關鍵字一律視為短水，不列入 PB"""
    m = str(meet or "")
    return ("冬季短水道" in m) or ("冬短" in m)

def within_range(y: str, start_y: Optional[str], end_y: Optional[str]) -> bool:
    if not y or len(y) < 8:
        return False
    if start_y and y < start_y:
        return False
    if end_y and y > end_y:
        return False
    return True

# ---------- health ----------
@router.get("/api/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# ---------- 基礎查詢：輸入選手單一泳姿距離明細 + 分析 + 四式統計 ----------
@router.get("/api/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="指定泳姿＋距離，如：50公尺蛙式"),
    limit: int = Query(200, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 1) 取輸入選手在該泳姿距離的所有明細（供分析、趨勢、表格）
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
    valid_secs: List[float] = []
    min_year: Optional[str] = None
    max_year: Optional[str] = None

    for r in rows:
        sec = parse_seconds(r["result"])
        y = r["year8"]
        min_year = y if (min_year is None or y < min_year) else min_year
        max_year = y if (max_year is None or y > max_year) else max_year

        if isinstance(sec, (int, float)) and sec > 0:
            valid_secs.append(sec)

        items.append(
            {
                "年份": y,
                "賽事名稱": r["meet"],
                "項目": r["item"],
                "姓名": r["swimmer"],
                "成績": r["result"],
                "名次": r["rank"],
                "水道": r["lane"],
                "泳池長度": "",
                "seconds": sec,
            }
        )

    # 分析（出賽、平均、PB；PB 剔除冬季短水道）
    meet_count = len(items)
    avg_seconds = sum(s for s in valid_secs) / len(valid_secs) if valid_secs else None

    # PB（排除冬短）
    pb_sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 3000
    """
    pb_rows = db.execute(text(pb_sql), {"name": name, "pat": pat}).mappings().all()
    best: Optional[Tuple[float, str, str]] = None
    for rr in pb_rows:
        if is_winter_short_course(rr["m"]):
            continue
        sec = parse_seconds(rr["r"])
        if sec is None:
            continue
        if best is None or sec < best[0]:
            best = (sec, rr["y"], rr["m"])

    next_cursor = cursor + limit if len(rows) == limit else None

    # 2) 四式（不分距離）
    families = ["蛙式", "仰式", "自由式", "蝶式"]
    fam_out: Dict[str, Any] = {}
    for fam in families:
        pf = f"%{fam}%"
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r, "項目"::text AS item
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 3000
        """
        rws = db.execute(text(q), {"name": name, "pat": pf}).mappings().all()
        count = 0
        dist_count: Dict[str, int] = {}
        best_fam: Optional[Tuple[float, str, str]] = None
        for row in rws:
            count += 1
            m = re.search(r"(\d+)\s*公尺", str(row["item"] or ""))
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1
            sec = parse_seconds(row["r"])
            if sec is not None and (best_fam is None or sec < best_fam[0]):
                best_fam = (sec, row["y"], row["m"])
        mostDist, mostCount = "", 0
        for d, c in dist_count.items():
            if c > mostCount:
                mostDist, mostCount = d, c
        fam_out[fam] = {
            "count": count,
            "pb_seconds": best_fam[0] if best_fam else None,
            "year": best_fam[1] if best_fam else None,
            "from_meet": best_fam[2] if best_fam else None,
            "mostDist": mostDist,
            "mostCount": mostCount,
        }

    # 3) 趨勢資料（輸入選手）
    trend_points = []
    for it in items:
        if it["seconds"]:
            trend_points.append({"year": it["年份"], "seconds": it["seconds"]})
    trend_points.sort(key=lambda x: x["year"])

    return {
        "analysis": {
            "meetCount": meet_count,
            "avg_seconds": avg_seconds,
            "pb_seconds": best[0] if best else None,
            "pb_year": best[1] if best else None,
            "pb_meet": best[2] if best else None,
            "range": {"start": min_year, "end": max_year},
        },
        "family": fam_out,
        "trend": {"points": trend_points},
        "items": items,
        "nextCursor": next_cursor,
    }

# ---------- 排行：同場同項目對手池 + 區間內 PB + 榜首趨勢（區間內） ----------
@router.get("/api/rank")
def rank_api(
    name: str = Query(..., description="輸入選手姓名"),
    stroke: str = Query(..., description="泳姿＋距離，如：50公尺蛙式"),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 先抓輸入選手該泳姿距離的出賽區間（start ~ end）
    range_sql = f"""
        SELECT MIN("年份")::text AS start_y, MAX("年份")::text AS end_y
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
    """
    r_range = db.execute(text(range_sql), {"name": name, "pat": pat}).mappings().first()
    start_y = (r_range or {}).get("start_y")
    end_y = (r_range or {}).get("end_y")
    if not start_y or not end_y:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；PB 限定在你的區間內且排除冬短）",
            "denominator": 0,
            "rank": None,
            "percentile": None,
            "leader": None,
            "you": None,
            "top": [],
            "around": [],
            "leaderTrend": [],
        }

    # 取輸入選手所有(年、賽事、項目、組別) — 形成對手池條件
    base_sql = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS meet, "項目"::text AS item,
               COALESCE(TRIM("組別"::text),'') AS grp
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        GROUP BY 1,2,3,4
        ORDER BY 1
        LIMIT 5000
    """
    base_rows = db.execute(text(base_sql), {"name": name, "pat": pat}).mappings().all()

    # 依每場比賽蒐集對手（同年+同賽事+同項目；組別規則）
    opponent_names: set = set()
    for b in base_rows:
        y, m, item, grp = b["y"], b["meet"], b["item"], b["grp"]
        if grp and not grp.isdigit():
            cond = 'AND COALESCE(TRIM("組別"::text), \'\') = :grp'
            params = {"y": y, "m": m, "item": item, "grp": grp, "base": name}
        else:
            cond = ""
            params = {"y": y, "m": m, "item": item, "base": name}

        opp_sql = f"""
            SELECT DISTINCT "姓名"::text AS n
            FROM {TABLE}
            WHERE "年份" = :y AND "賽事名稱" = :m AND "項目" = :item
              AND "姓名" <> :base
              {cond}
        """
        rs = db.execute(text(opp_sql), params).all()
        for r in rs:
            opponent_names.add(r[0])

    if not opponent_names:
        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；PB 限定在你的區間內且排除冬短）",
            "denominator": 0,
            "rank": None,
            "percentile": None,
            "leader": None,
            "you": None,
            "top": [],
            "around": [],
            "leaderTrend": [],
        }

    # 幫對手與輸入選手計算「PB（同泳姿＋距離，剔除冬短，且限定在 base 的區間內）」
    def get_pb_in_range(person: str) -> Optional[Tuple[float, str, str]]:
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :person AND "項目" ILIKE :pat
              AND "年份" BETWEEN :start_y AND :end_y
            ORDER BY "年份" ASC
            LIMIT 3000
        """
        rows = db.execute(
            text(q), {"person": person, "pat": pat, "start_y": start_y, "end_y": end_y}
        ).mappings().all()
        best_local: Optional[Tuple[float, str, str]] = None
        for rr in rows:
            if is_winter_short_course(rr["m"]):
                continue
            sec = parse_seconds(rr["r"])
            if sec is None:
                continue
            if best_local is None or sec < best_local[0]:
                best_local = (sec, rr["y"], rr["m"])
        return best_local

    # 先算輸入選手 PB（同一條規則）
    you_pb = get_pb_in_range(name)

    # 對手 PB 清單
    ranked: List[Dict[str, Any]] = []
    for opp in opponent_names:
        pb = get_pb_in_range(opp)
        if pb:
            ranked.append(
                {"name": opp, "pb_seconds": pb[0], "pb_year": pb[1], "pb_meet": pb[2]}
            )

    # 排序 + 取前 10 + 找 leader
    ranked.sort(key=lambda x: x["pb_seconds"])
    denominator = len(ranked) + (1 if you_pb else 0)  # 含自己（若有 PB）
    leader = ranked[0] if ranked else None

    # 你的名次（與分位）
    your_rank = None
    if you_pb:
        # 插入你後再排序，找 index
        merged = ranked + [{"name": name, "pb_seconds": you_pb[0]}]
        merged.sort(key=lambda x: x["pb_seconds"])
        for i, row in enumerate(merged, 1):
            if row.get("name") == name:
                your_rank = i
                break
    percentile = (1 - (your_rank - 1) / denominator) * 100 if your_rank and denominator else None

    # 周邊 6 人（名次附近）
    around: List[Dict[str, Any]] = []
    if you_pb and denominator:
        merged = ranked + [{"name": name, "pb_seconds": you_pb[0]}]
        merged.sort(key=lambda x: x["pb_seconds"])
        idx = next((i for i, r in enumerate(merged) if r["name"] == name), None)
        if idx is not None:
            for j in range(max(0, idx - 3), min(len(merged), idx + 3)):
                if merged[j]["name"] != name:
                    around.append(
                        {"name": merged[j]["name"], "pb_seconds": merged[j]["pb_seconds"], "rank": j + 1}
                    )

    # 榜首趨勢（同泳姿＋距離＋限定在 base 區間內）
    leader_trend: List[Dict[str, Any]] = []
    if leader:
        lt_sql = f"""
            SELECT "年份"::text AS y, "成績"::text AS r, "賽事名稱"::text AS m
            FROM {TABLE}
            WHERE "姓名" = :leader AND "項目" ILIKE :pat
              AND "年份" BETWEEN :start_y AND :end_y
            ORDER BY "年份" ASC
            LIMIT 3000
        """
        lrows = db.execute(
            text(lt_sql),
            {"leader": leader["name"], "pat": pat, "start_y": start_y, "end_y": end_y},
        ).mappings().all()
        for rr in lrows:
            sec = parse_seconds(rr["r"])
            if sec and sec > 0:
                leader_trend.append({"year": rr["y"], "seconds": sec})
        leader_trend.sort(key=lambda x: x["year"])

    # top10（帶 rank 序號）
    top10 = []
    for i, r in enumerate(ranked[:10], 1):
        top10.append(
            {
                "rank": i,
                "name": r["name"],
                "pb": r["pb_seconds"],
                "pb_year": r["pb_year"],
                "pb_meet": r["pb_meet"],
            }
        )

    return {
        "name": name,
        "stroke": stroke,
        "criteria": "同年份＋同賽事名稱＋同項目（依你實際參賽場次蒐集對手；PB 限定在你的區間內且排除冬短）",
        "denominator": denominator,
        "rank": your_rank,
        "percentile": percentile,
        "leader": {"name": leader["name"], "pb_seconds": leader["pb_seconds"], "rank": 1} if leader else None,
        "you": {
            "name": name,
            "pb_seconds": you_pb[0],
            "pb_year": you_pb[1],
            "pb_meet": you_pb[2],
            "rank": your_rank,
        } if you_pb else None,
        "top": top10,
        "around": around,
        "leaderTrend": leader_trend,
        "range": {"start": start_y, "end": end_y},
    }