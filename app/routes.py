
# -*- coding: utf-8 -*-
"""
routes.py
固定前綴策略：主路徑 /api，同時提供相容路徑 /api/api（避免前端路徑不一致造成 404）。
環境變數：
- DB_PATH：SQLite 檔案路徑（預設 /mnt/data/swim_results.db）
- API_ALLOW_ORIGINS：CORS 白名單，逗號分隔（預設 '*')
"""
from __future__ import annotations
import os
import sqlite3
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.getenv("DB_PATH", "/mnt/data/swim_results.db")

# ---------- helpers ----------
def to_seconds(s: str) -> Optional[float]:
    """
    允許格式：
    - 1:23.45
    - 59.78
    - 1:02
    - 1:02:03 （若有時）=> 1*3600 + 2*60 + 3 但游泳通常不會用到
    - 非數字字串回傳 None
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # 移除可能的中文或非時間字
    if not any(ch.isdigit() for ch in s):
        return None
    # 支援 1:23.45 或 59.78
    try:
        if s.count(":") == 0:
            return float(s)
        parts = s.split(":")
        secs = 0.0
        for p in parts:
            # 最後一段可含有小數
            pass
        # 從右往左累加
        mul = 1.0
        for p in reversed(parts):
            secs += float(p) * mul
            mul *= 60.0
        return secs
    except Exception:
        return None

def q(sql: str, args: Tuple=()) -> List[sqlite3.Row]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(sql, args)
        rows = cur.fetchall()
        return rows
    finally:
        con.close()

def style_from_event(event: str) -> Optional[str]:
    if not event:
        return None
    if "蛙" in event:
        return "蛙式"
    if "仰" in event:
        return "仰式"
    if "蝶" in event:
        return "蝶式"
    if "自由" in event:
        return "自由式"
    if "混合" in event:
        return "混合式"
    return None

def distance_from_event(event: str) -> Optional[int]:
    # 取前面的數字，例：'100公尺蛙式' -> 100
    if not event:
        return None
    m = None
    # 支援「200 公尺」或「200公尺」
    for pat in [r"(\\d+)\\s*公尺", r"(\\d+)m"]:
        m = re.search(pat, event)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None

# ---------- FastAPI ----------
app = FastAPI(title="Swim API", version="v2025.09.16.01")
_router = APIRouter()  # 無前綴，之後同時掛 /api 與 /api/api 以避免路徑漂移

# CORS
allow = os.getenv("API_ALLOW_ORIGINS", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allow.split(",")] if allow != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@_router.get("/health")
def health():
    # 確認 DB 可開啟
    ok = os.path.exists(DB_PATH)
    return {"ok": "true", "db": DB_PATH, "db_exists": ok}

# --------- /summary ---------
@_router.get("/summary")
def summary(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目，例如 50公尺蛙式"),
    limit: int = Query(200, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """
    回傳：items, nextCursor, analysis, family, trend, leaderTrend
    items 依年份排序（升冪），支援分頁（cursor 為 offset，只取 limit 筆）
    """
    # 單人單項所有成績（原始）
    rows = q(
        """
        SELECT 年份, 賽事名稱, 項目, 姓名, 成績
        FROM swimming_scores
        WHERE 姓名 = ? AND 項目 = ?
        ORDER BY 年份 ASC
        """,
        (name, stroke),
    )
    # 轉 seconds
    items_all = []
    for r in rows:
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0:
            continue
        items_all.append({
            "年份": str(r["年份"]),
            "賽事名稱": r["賽事名稱"],
            "項目": r["項目"],
            "姓名": r["姓名"],
            "seconds": sec,
        })
    # 分頁
    end = cursor + limit
    items = items_all[cursor:end]
    next_cursor = end if end < len(items_all) else None

    # analysis
    secs = [x["seconds"] for x in items_all]
    meet_count = len(items_all)
    avg_seconds = sum(secs) / len(secs) if secs else None
    pb_seconds = min(secs) if secs else None

    # family stats（四式，不分距離）
    fam: Dict[str, Dict[str, Any]] = {}
    # 取此人所有成績（不分 stroke）
    rows_all_person = q(
        """
        SELECT 年份, 賽事名稱, 項目, 姓名, 成績
        FROM swimming_scores
        WHERE 姓名 = ?
        """,
        (name,),
    )
    by_style: Dict[str, Dict[str, Any]] = {}
    for r in rows_all_person:
        st = style_from_event(r["項目"]) or "其他"
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0:
            continue
        by_style.setdefault(st, {"count": 0, "seconds": [], "dists": {}})
        by_style[st]["count"] += 1
        by_style[st]["seconds"].append(sec)
        dist = distance_from_event(r["項目"])
        if dist:
            by_style[st]["dists"][dist] = by_style[st]["dists"].get(dist, 0) + 1
    # 轉為指定輸出
    family_out = {}
    for st in ["蛙式", "仰式", "自由式", "蝶式"]:
        info = by_style.get(st, {"count": 0, "seconds": [], "dists": {}})
        pb = min(info["seconds"]) if info["seconds"] else None
        # 找最多距離
        mostDist = None
        mostCount = None
        if info["dists"]:
            mostDist = max(info["dists"], key=lambda k: info["dists"][k])
            mostCount = info["dists"][mostDist]
        family_out[st] = {"count": info["count"], "mostDist": mostDist, "mostCount": mostCount, "pb_seconds": pb}

    # trend：此人此項，依「年份」取每個年份的最好成績
    best_by_year: Dict[str, float] = {}
    for it in items_all:
        y = str(it["年份"])
        s = it["seconds"]
        if y not in best_by_year or s < best_by_year[y]:
            best_by_year[y] = s
    trend_points = [{"year": y, "seconds": best_by_year[y]} for y in sorted(best_by_year.keys())]

    # leaderTrend：在每個年份裡，所有選手在該項目的最好成績
    all_rows_stroke = q(
        """
        SELECT 年份, 成績
        FROM swimming_scores
        WHERE 項目 = ?
        """,
        (stroke,),
    )
    best_leader_by_year: Dict[str, float] = {}
    for r in all_rows_stroke:
        y = str(r["年份"])
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0:
            continue
        if y not in best_leader_by_year or sec < best_leader_by_year[y]:
            best_leader_by_year[y] = sec
    leader_points = [{"year": y, "seconds": best_leader_by_year[y]} for y in sorted(best_leader_by_year.keys())]

    return {
        "items": items,
        "nextCursor": next_cursor,
        "analysis": {"avg_seconds": avg_seconds, "pb_seconds": pb_seconds, "meetCount": meet_count},
        "family": family_out,
        "trend": {"points": trend_points},
        "leaderTrend": {"points": leader_points},
    }

# --------- /rank ---------
@_router.get("/rank")
def rank(
    name: str = Query(..., description="選手姓名"),
    stroke: str = Query(..., description="項目，例如 50公尺蛙式"),
) -> Dict[str, Any]:
    """
    以「相同項目」計算每位選手的 PB，回傳前 10 名、你的名次、分母、百分位
    """
    rows = q(
        """
        SELECT 姓名, 年份, 賽事名稱, 成績
        FROM swimming_scores
        WHERE 項目 = ?
        """,
        (stroke,),
    )
    best: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        nm = r["姓名"]
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0:
            continue
        prev = best.get(nm)
        if not prev or sec < prev["pb_seconds"]:
            best[nm] = {"name": nm, "pb_seconds": sec, "pb_year": str(r["年份"]), "pb_meet": r["賽事名稱"]}
    # 排序
    arr = sorted(best.values(), key=lambda x: x["pb_seconds"])
    denominator = len(arr)
    top = arr[:10]
    you = None
    rank_num = None
    for i, rec in enumerate(arr, start=1):
        if rec["name"] == name:
            you = {"name": rec["name"], "rank": i, **rec}
            rank_num = i
            break
    percentile = None
    if denominator and rank_num:
        # 百分位：越前面越高。例：第 1 名在 100 人中 => 100% ；第 50 名 => 51%
        percentile = (denominator - rank_num + 1) / denominator * 100.0

    return {"denominator": denominator, "rank": rank_num, "percentile": percentile, "top": top, "you": you}

# 將無前綴的 router 掛兩次：/api 與 /api/api（相容層）
app.include_router(_router, prefix="/api")
app.include_router(_router, prefix="/api/api")
