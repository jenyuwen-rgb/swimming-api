# -*- coding: utf-8 -*-
"""
app/routes.py
固定前綴策略：主路徑 /api，同時提供相容層 /api/api。
環境變數：
- DB_PATH：SQLite 檔案路徑（預設 /mnt/data/swim_results.db）
- API_ALLOW_ORIGINS：CORS 白名單，逗號分隔（預設 '*')
"""
from __future__ import annotations
import os, re, sqlite3
from typing import Dict, Any, Optional
from fastapi import FastAPI, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.getenv("DB_PATH", "/mnt/data/swim_results.db")

def to_seconds(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s or not any(ch.isdigit() for ch in s):
        return None
    try:
        if ":" not in s:
            return float(s)
        parts = s.split(":")
        mul, secs = 1.0, 0.0
        for p in reversed(parts):
            secs += float(p) * mul
            mul *= 60.0
        return secs
    except Exception:
        return None

def q(sql: str, args=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(sql, args)
        return cur.fetchall()
    finally:
        con.close()

def style_from_event(event: str) -> Optional[str]:
    if not event: return None
    if "蛙" in event: return "蛙式"
    if "仰" in event: return "仰式"
    if "蝶" in event: return "蝶式"
    if "自由" in event: return "自由式"
    if "混合" in event: return "混合式"
    return None

def distance_from_event(event: str) -> Optional[int]:
    if not event: return None
    m = re.search(r"(\\d+)\\s*公尺", event) or re.search(r"(\\d+)m", event)
    return int(m.group(1)) if m else None

app = FastAPI(title="Swim API", version="v2025.09.16.02")
router = APIRouter()  # ← 對外匯出

# CORS
allow = os.getenv("API_ALLOW_ORIGINS", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allow.split(",")] if allow != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@router.get("/health")
def health():
    ok = os.path.exists(DB_PATH)
    return {"ok": "true", "db": DB_PATH, "db_exists": ok}

@router.get("/summary")
def summary(name: str, stroke: str, limit: int = 200, cursor: int = 0) -> Dict[str, Any]:
    rows = q("""SELECT 年份, 賽事名稱, 項目, 姓名, 成績
                FROM swimming_scores WHERE 姓名=? AND 項目=? ORDER BY 年份 ASC""",
             (name, stroke))
    items_all = []
    for r in rows:
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0:
            continue
        items_all.append({"年份": str(r["年份"]), "賽事名稱": r["賽事名稱"],
                          "項目": r["項目"], "姓名": r["姓名"], "seconds": sec})
    end = cursor + limit
    items = items_all[cursor:end]
    next_cursor = end if end < len(items_all) else None

    secs = [x["seconds"] for x in items_all]
    avg_seconds = sum(secs)/len(secs) if secs else None
    pb_seconds = min(secs) if secs else None

    rows_all_person = q("SELECT 年份, 賽事名稱, 項目, 姓名, 成績 FROM swimming_scores WHERE 姓名=?",(name,))
    by_style: Dict[str, Dict[str, Any]] = {}
    for r in rows_all_person:
        st = style_from_event(r["項目"]) or "其他"
        sec = to_seconds(r["成績"])
        if sec is None or sec <= 0: continue
        by_style.setdefault(st, {"count":0,"seconds":[],"dists":{}})
        by_style[st]["count"] += 1
        by_style[st]["seconds"].append(sec)
        dist = distance_from_event(r["項目"])
        if dist: by_style[st]["dists"][dist] = by_style[st]["dists"].get(dist,0)+1
    family_out = {}
    for st in ["蛙式","仰式","自由式","蝶式"]:
        info = by_style.get(st,{"count":0,"seconds":[],"dists":{}})
        pb = min(info["seconds"]) if info["seconds"] else None
        mostDist, mostCount = None,None
        if info["dists"]:
            mostDist = max(info["dists"], key=lambda k: info["dists"][k])
            mostCount = info["dists"][mostDist]
        family_out[st]={"count":info["count"],"mostDist":mostDist,"mostCount":mostCount,"pb_seconds":pb}

    best_by_year: Dict[str,float] = {}
    for it in items_all:
        y,s=it["年份"],it["seconds"]
        if y not in best_by_year or s<best_by_year[y]: best_by_year[y]=s
    trend_points=[{"year":y,"seconds":best_by_year[y]} for y in sorted(best_by_year.keys())]

    all_rows_stroke = q("SELECT 年份, 成績 FROM swimming_scores WHERE 項目=?",(stroke,))
    best_leader_by_year: Dict[str,float] = {}
    for r in all_rows_stroke:
        sec=to_seconds(r["成績"]); y=str(r["年份"])
        if sec is None or sec<=0: continue
        if y not in best_leader_by_year or sec<best_leader_by_year[y]: best_leader_by_year[y]=sec
    leader_points=[{"year":y,"seconds":best_leader_by_year[y]} for y in sorted(best_leader_by_year.keys())]

    return {"items":items,"nextCursor":next_cursor,
            "analysis":{"avg_seconds":avg_seconds,"pb_seconds":pb_seconds,"meetCount":len(items_all)},
            "family":family_out,"trend":{"points":trend_points},"leaderTrend":{"points":leader_points}}

@router.get("/rank")
def rank(name:str, stroke:str)->Dict[str,Any]:
    rows=q("SELECT 姓名, 年份, 賽事名稱, 成績 FROM swimming_scores WHERE 項目=?",(stroke,))
    best={}
    for r in rows:
        nm=r["姓名"]; sec=to_seconds(r["成績"])
        if sec is None or sec<=0: continue
        prev=best.get(nm)
        if not prev or sec<prev["pb_seconds"]:
            best[nm]={"name":nm,"pb_seconds":sec,"pb_year":str(r["年份"]),"pb_meet":r["賽事名稱"]}
    arr=sorted(best.values(), key=lambda x:x["pb_seconds"])
    denominator=len(arr); top=arr[:10]; you=None; rank_num=None
    for i,rec in enumerate(arr, start=1):
        if rec["name"]==name:
            you={"name":rec["name"],"rank":i,**rec}; rank_num=i; break
    percentile=None
    if denominator and rank_num:
        percentile=(denominator-rank_num+1)/denominator*100.0
    return {"denominator":denominator,"rank":rank_num,"percentile":percentile,"top":top,"you":you}

# 掛載路由（提供 /api 與 /api/api）
app.include_router(router, prefix="/api")
app.include_router(router, prefix="/api/api")
