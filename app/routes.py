# app/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re, datetime
from .db import SessionLocal

router = APIRouter()
TABLE = "swimming_scores"

# ----------------- DB session -----------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------- helpers -----------------
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

def is_winter_short_course(meet: str) -> bool:
    """冬季短水道的成績不計入 PB / 快腿統計"""
    if not meet: return False
    s = str(meet)
    return ("冬季短水道" in s) or ("短水道" in s and "冬" in s)

def sex_norm(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = str(s)
    if "女" in s: return "女"
    if "男" in s: return "男"
    return None

# ---- WA Points ----
# 公式： points = 1000 * (BaseTime / Time)^3
# BaseTime 依「性別 × 泳程 × 池別」。
# 這裡放常見泳程示例；你可擴充成完整表。
WA_BASE_50 = {
    # 長水道 50m pool
    "男": {
        "50自由式": 20.91, "100自由式": 46.86, "200自由式": 102.00,
        "50蛙式": 25.95, "100蛙式": 56.88, "200蛙式": 126.12,
        "50仰式": 24.00, "100仰式": 51.85, "200仰式": 112.53,
        "50蝶式": 22.27, "100蝶式": 49.45, "200蝶式": 110.73,
        "200混合式": 112.98, "400混合式": 246.00,
    },
    "女": {
        "50自由式": 23.61, "100自由式": 51.71, "200自由式": 112.98,
        "50蛙式": 29.16, "100蛙式": 64.13, "200蛙式": 139.11,
        "50仰式": 26.98, "100仰式": 57.45, "200仰式": 124.12,
        "50蝶式": 24.43, "100蝶式": 55.48, "200蝶式": 125.83,
        "200混合式": 120.19, "400混合式": 255.00,
    },
}
WA_BASE_25 = {
    # 短水道 25m pool（示例）
    "男": {
        "50自由式": 20.16, "100自由式": 44.84, "200自由式": 98.07,
        "50蛙式": 25.25, "100蛙式": 55.28, "200蛙式": 122.41,
        "50仰式": 22.58, "100仰式": 49.28, "200仰式": 107.13,
        "50蝶式": 21.75, "100蝶式": 48.08, "200蝶式": 108.20,
        "200混合式": 110.34, "400混合式": 240.00,
    },
    "女": {
        "50自由式": 23.19, "100自由式": 50.25, "200自由式": 109.34,
        "50蛙式": 28.56, "100蛙式": 62.36, "200蛙式": 135.57,
        "50仰式": 26.34, "100仰式": 56.06, "200仰式": 121.10,
        "50蝶式": 24.05, "100蝶式": 54.03, "200蝶式": 122.50,
        "200混合式": 117.60, "400混合式": 249.80,
    },
}

def stroke_key_from_item(item: str) -> Optional[str]:
    """
    把「50公尺蛙式」「100公尺自由式」「200公尺混合式」轉成 WA key，如「50蛙式」「100自由式」「200混合式」。
    若無法辨識回 None。
    """
    if not item: return None
    s = re.sub(r"\s+", "", str(item))
    m = re.search(r"(\d+)\s*公尺\s*(自由式|蛙式|仰式|蝶式|混合式)", s)
    if not m: return None
    dist = m.group(1)
    style = m.group(2)
    return f"{dist}{style}"

def wa_points(gender: Optional[str], pool: int, item: str, seconds: Optional[float]) -> Optional[float]:
    """根據性別＋池別（50/25）＋泳程，將秒數換算 WA points。缺資訊回 None。"""
    g = sex_norm(gender)
    if not g or not seconds or seconds <= 0: return None
    key = stroke_key_from_item(item)
    if not key: return None
    base_map = WA_BASE_50 if int(pool) == 50 else WA_BASE_25
    base = base_map.get(g, {}).get(key)
    if not base: return None
    try:
        return 1000.0 * (float(base) / float(seconds)) ** 3
    except Exception:
        return None

# ----------------- health -----------------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true"}

# ----------------- /results -----------------
@router.get("/results")
def results(
    name: str = Query(...),
    stroke: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    指定選手＋項目（泳姿＋距離）的明細，年份倒序（最新在前），並附上 is_pb。
    同步回傳『性別』『出生年』（若為 NULL 以空字串回傳）。
    """
    try:
        pat = f"%{stroke.strip()}%"
        # 先抓分頁資料：倒序
        sql = f"""
            SELECT
                "年份"::text AS y,
                "賽事名稱"::text AS m,
                "項目"::text AS i,
                "成績"::text AS r,
                COALESCE("名次"::text,'')  AS rk,
                COALESCE("水道"::text,'')  AS ln,
                COALESCE("組別"::text,'')  AS g,
                "姓名"::text AS n,
                COALESCE("性別"::text,'') AS gender,
                COALESCE("出生年"::text,'') AS birth_year
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" DESC
            LIMIT :limit OFFSET :offset
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat, "limit": limit, "offset": cursor}).mappings().all()

        # 為了標 PB，需要計算整體 PB（排除冬短）
        sql_all = f"""
            SELECT "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        all_rows = db.execute(text(sql_all), {"name": name, "pat": pat}).mappings().all()
        pb_sec = None
        for rr in all_rows:
            if is_winter_short_course(rr["m"]): 
                continue
            s = parse_seconds(rr["r"])
            if s is None or s <= 0:
                continue
            if pb_sec is None or s < pb_sec:
                pb_sec = s

        items: List[Dict[str, Any]] = []
        for r in rows:
            sec = parse_seconds(r["r"])
            items.append({
                "年份": r["y"],
                "賽事名稱": r["m"],
                "項目": r["i"],
                "姓名": r["n"],
                "性別": r["gender"],
                "出生年": r["birth_year"],
                "成績": r["r"],
                "名次": r["rk"],
                "水道": r["ln"],
                "組別": r["g"],
                "seconds": sec,
                "is_pb": (sec is not None and pb_sec is not None and sec == pb_sec),
            })
        next_cursor = cursor + limit if len(rows) == limit else None
        return {"items": items, "nextCursor": next_cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"results failed: {e}")

# ----------------- /pb -----------------
@router.get("/pb")
def pb(name: str = Query(...), stroke: str = Query(...), db: Session = Depends(get_db)):
    """單純回傳該選手在該泳姿＋距離下（排除冬短）的 PB。"""
    try:
        pat = f"%{stroke.strip()}%"
        sql = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(sql), {"name": name, "pat": pat}).mappings().all()
        best = None  # (sec, y, m)
        for r in rows:
            if is_winter_short_course(r["m"]):
                continue
            s = parse_seconds(r["r"])
            if s is None or s <= 0:
                continue
            if best is None or s < best[0]:
                best = (s, r["y"], r["m"])
        if not best:
            return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}
        return {"name": name, "stroke": stroke, "pb_seconds": best[0], "year": best[1], "from_meet": best[2]}
    except Exception:
        return {"name": name, "stroke": stroke, "pb_seconds": None, "year": None, "from_meet": None}

# ----------------- /summary -----------------
@router.get("/summary")
def summary(
    name: str = Query(...),
    stroke: str = Query(...),
    pool: int = Query(50, ge=25, le=50, description="WA points 池別：50=長水道，25=短水道"),
    limit: int = Query(500, ge=1, le=2000),
    cursor: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    pat = f"%{stroke.strip()}%"

    # 全量資料（算 analysis 與 trend）
    sql_all = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    all_rows = db.execute(text(sql_all), {"name": name, "pat": pat}).mappings().all()

    vals, pb_sec = [], None
    for r in all_rows:
        s = parse_seconds(r["r"])
        if s is not None and s > 0:
            vals.append(s)
            if not is_winter_short_course(r["m"]):
                pb_sec = s if pb_sec is None or s < pb_sec else pb_sec

    trend_points = [{"year": r["y"], "seconds": parse_seconds(r["r"])} for r in all_rows if parse_seconds(r["r"])]

    # 分頁明細（倒序，並標 is_pb）＋ 性別/出生年
    sql_page = f"""
        SELECT "年份"::text AS y,"賽事名稱"::text AS m,"項目"::text AS i,
               "成績"::text AS r,"姓名"::text AS n,
               COALESCE("名次"::text,'') AS rk,
               COALESCE("水道"::text,'') AS ln,
               COALESCE("組別"::text,'') AS g,
               COALESCE("性別"::text,'') AS gender,
               COALESCE("出生年"::text,'') AS birth_year
        FROM {TABLE}
        WHERE "姓名" = :name AND "項目" ILIKE :pat
        ORDER BY "年份" DESC
        LIMIT :limit OFFSET :offset
    """
    page_rows = db.execute(
        text(sql_page), {"name": name, "pat": pat, "limit": limit, "offset": cursor}
    ).mappings().all()

    # 性別（抓一筆有值的）
    base_info_sql = f"""
        SELECT NULLIF("性別"::text,'') AS gender
        FROM {TABLE}
        WHERE "姓名"=:name
        ORDER BY "年份" DESC
        LIMIT 1
    """
    g_row = db.execute(text(base_info_sql), {"name": name}).mappings().first()
    gender = g_row["gender"] if g_row and g_row["gender"] else None

    items = []
    for r in page_rows:
        sec = parse_seconds(r["r"])
        items.append({
            "年份": r["y"], "賽事名稱": r["m"], "項目": r["i"], "姓名": r["n"],
            "性別": r["gender"], "出生年": r["birth_year"],
            "成績": r["r"], "名次": r["rk"], "水道": r["ln"], "組別": r["g"],
            "seconds": sec, "is_pb": (sec is not None and pb_sec is not None and sec == pb_sec),
        })
    next_cursor = cursor + limit if len(page_rows) == limit else None

    # WA points（用本次查詢泳程的 PB 換算）
    wa_pts = wa_points(gender, pool, stroke, pb_sec)

    analysis = {
        "meetCount": len(all_rows),
        "avg_seconds": (sum(vals) / len(vals)) if vals else None,
        "pb_seconds": pb_sec,
        "wa_points": wa_pts,
    }

    # ---- 四式專項統計 ----
    family_out: Dict[str, Any] = {}
    for fam in ["蛙式", "仰式", "自由式", "蝶式"]:
        pf = f"%{fam}%"
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m,
                   "成績"::text AS r, "項目"::text AS i
            FROM {TABLE}
            WHERE "姓名" = :name AND "項目" ILIKE :pf
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"name": name, "pf": pf}).mappings().all()

        count = len(rows)
        dist_count: Dict[str, int] = {}
        best_by_dist: Dict[str, Tuple[float, str, str]] = {}  # dist -> (sec, year, meet)

        for row in rows:
            m = re.search(r"(\d+)\s*公尺", str(row["i"] or ""))
            dist = f"{m.group(1)}公尺" if m else ""
            if dist:
                dist_count[dist] = dist_count.get(dist, 0) + 1

            s = parse_seconds(row["r"])
            if s is None or s <= 0 or is_winter_short_course(row["m"]):
                continue
            if dist:
                cur = best_by_dist.get(dist)
                if cur is None or s < cur[0]:
                    best_by_dist[dist] = (s, row["y"], row["m"])

        mostDist, mostCount = "", 0
        for d, c in dist_count.items():
            if c > mostCount or (c == mostCount and d < mostDist):
                mostDist, mostCount = d, c

        pb_tuple = best_by_dist.get(mostDist)
        if pb_tuple is None and best_by_dist:
            pb_tuple = min(best_by_dist.values(), key=lambda t: t[0])

        family_out[fam] = {
            "count": count,
            "mostDist": mostDist,
            "mostCount": mostCount,
            "pb_seconds": pb_tuple[0] if pb_tuple else None,
            "year": pb_tuple[1] if pb_tuple else None,
            "from_meet": pb_tuple[2] if pb_tuple else None,
        }
    # ---- 四式統計結束 ----

    return {
        "analysis": analysis,
        "trend": {"points": trend_points},
        "items": items,
        "nextCursor": next_cursor,
        "family": family_out,
    }

# ----------------- /rank -----------------
@router.get("/rank")
def rank_api(
    name: str = Query(...),
    stroke: str = Query(...),
    ageTol: int = Query(1, ge=0, le=5, description="年齡誤差；0=同年、1=±1"),
    db: Session = Depends(get_db),
):
    """
    對手池規則：
    - 同性別，且出生年 = 輸入選手出生年 ± ageTol（若性別/出生年缺失則盡可能放寬；兩者皆缺則只回你自己）。
    - 取消「同場至少 2 次」限制。
    - PB 計算：同泳姿＋距離、排除冬短、且剔除早於輸入選手第一筆日期(t0)的成績。
    """
    pat = f"%{stroke.strip()}%"

    # 取得輸入選手的性別與出生年（盡量取有值的一筆）
    base_info_sql = f"""
        SELECT
            NULLIF("性別"::text,'') AS gender,
            NULLIF("出生年"::text,'') AS birth_year
        FROM {TABLE}
        WHERE "姓名" = :name
        ORDER BY (CASE WHEN "出生年" IS NULL THEN 1 ELSE 0 END), "年份" DESC
        LIMIT 1
    """
    row = db.execute(text(base_info_sql), {"name": name}).mappings().first()
    gender = (row["gender"] if row else None) or None
    byear = None
    try:
        byear = int(row["birth_year"]) if row and row["birth_year"] else None
    except Exception:
        byear = None

    # t0（第一筆該項目日期）
    t0_sql = f"""SELECT MIN("年份"::text) FROM {TABLE} WHERE "姓名"=:name AND "項目" ILIKE :pat"""
    t0 = db.execute(text(t0_sql), {"name": name, "pat": pat}).scalar()
    t0 = str(t0) if t0 else None

    # 建立對手池（同泳姿＋距離，套性別/出生年 ± ageTol）
    where_clauses = ['"項目" ILIKE :pat', '"姓名" <> :name']
    params: Dict[str, Any] = {"pat": pat, "name": name}

    if gender:
        where_clauses.append('COALESCE("性別"::text, \'\') = :gender')
        params["gender"] = gender

    if byear is not None:
        # 僅納入能轉成整數的出生年，並做 between 篩選
        where_clauses.append('CAST(NULLIF("出生年"::text, \'\') AS INT) BETWEEN :by_min AND :by_max')
        params["by_min"] = byear - ageTol
        params["by_max"] = byear + ageTol

    pool_sql = f"""
        SELECT DISTINCT "姓名"::text AS nm
        FROM {TABLE}
        WHERE {" AND ".join(where_clauses)}
        LIMIT 20000
    """
    pool_rows = db.execute(text(pool_sql), params).all()
    pool = [r[0] for r in pool_rows]

    # 確保把自己放入比較
    if name not in pool:
        pool.append(name)

    if not pool:
        return {"denominator": 0, "rank": None, "percentile": None, "leader": None, "you": None, "top": [], "leaderTrend": []}

    def best_of(player: str) -> Optional[Tuple[float, str, str]]:
        q = f"""
            SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
            FROM {TABLE}
            WHERE "姓名"=:p AND "項目" ILIKE :pat
            ORDER BY "年份" ASC
            LIMIT 5000
        """
        rows = db.execute(text(q), {"p": player, "pat": pat}).mappings().all()
        best = None
        for row in rows:
            if t0 and str(row["y"]) < t0:
                continue
            if is_winter_short_course(row["m"]):
                continue
            s = parse_seconds(row["r"])
            if s is None or s <= 0:
                continue
            if best is None or s < best[0]:
                best = (s, row["y"], row["m"])
        return best

    board: List[Dict[str, Any]] = []
    for nm in pool:
        b = best_of(nm)
        if b:
            board.append({"name": nm, "pb_seconds": b[0], "pb_year": b[1], "pb_meet": b[2]})

    if not board:
        return {"denominator": 0, "rank": None, "percentile": None, "leader": None, "you": None, "top": [], "leaderTrend": []}

    board.sort(key=lambda x: x["pb_seconds"])
    for i, row in enumerate(board, start=1):
        row["rank"] = i

    denominator = len(board)
    you = next((x for x in board if x["name"] == name), None)
    rank_no = you["rank"] if you else None
    percentile = (100.0 * (denominator - rank_no) / denominator) if rank_no else None
    leader = board[0]
    top10 = board[:10]

    # 領先者趨勢（仍套 t0 與排冬短）
    leader_trend: List[Dict[str, Any]] = []
    q_leader = f"""
        SELECT "年份"::text AS y, "賽事名稱"::text AS m, "成績"::text AS r
        FROM {TABLE}
        WHERE "姓名" = :p AND "項目" ILIKE :pat
        ORDER BY "年份" ASC
        LIMIT 5000
    """
    for row in db.execute(text(q_leader), {"p": leader["name"], "pat": pat}).mappings():
        if t0 and str(row["y"]) < t0:
            continue
        s = parse_seconds(row["r"])
        if s is None or s <= 0:
            continue
        leader_trend.append({"year": row["y"], "seconds": s, "meet": row["m"]})

    return {
        "denominator": denominator,
        "rank": rank_no,
        "percentile": percentile,
        "leader": leader,
        "you": you,
        "top": top10,
        "leaderTrend": leader_trend,
    }

# ----------------- /groups （各分組：歷史最快＋近三年最快） -----------------
# ----------------- /groups （各分組：歷史最快＋近三年最快） -----------------
@router.get("/groups")
def groups_api(
    name: str = Query(...),
    stroke: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    回傳各分組 4 根柱：
    - 'All-Time'：資料庫內該分組(關鍵字可在【組別】或【項目】欄)＋同性別＋同泳程 的歷史最快（排除冬季短水道）
    - This Year / Last-1 / Last-2：該年份該分組的最快（排除冬季短水道）
    * 同『輸入選手性別』。
    """
    try:
        # 先找輸入選手性別
        row = db.execute(text(f"""
            SELECT NULLIF("性別"::text,'') AS g
            FROM {TABLE} WHERE "姓名"=:n
            ORDER BY "年份" DESC LIMIT 1
        """), {"n": name}).mappings().first()
        gender = row["g"] if row and row["g"] else None
        if not gender:
            return {"gender": None, "groups": []}

        THIS = datetime.date.today().year
        YEARS = [THIS, THIS-1, THIS-2]
        pat = f"%{stroke.strip()}%"

        # 分組→關鍵字別名（你可視資料再擴充）
        GROUP_ALIASES = {
            "18以上": ["18以上", "18歲以上", "公開", "公開組", "大專", "社會組", "公開男子組", "公開女子組"],
            "高中": ["高中", "高男", "高女"],
            "國中": ["國中", "國男", "國女", "國中男子組", "國中女子組"],
            "國小高年級": ["國小高年級", "小高", "小高年級"],
            "國小中年級": ["國小中年級", "小中", "小中年級"],
            "國小低年級": ["國小低年級", "小低", "小低年級"],
        }

        def best_in_year(group_name: str, y: Optional[int]) -> Optional[float]:
            # 允許關鍵字出現在「組別」或「項目」
            aliases = GROUP_ALIASES.get(group_name, [group_name])
            like_clauses = []
            params: Dict[str, Any] = {"g": gender, "pat": pat}

            for idx, kw in enumerate(aliases):
                k = f"kw{idx}"
                params[k] = f"%{kw}%"
                like_clauses.append(f'("組別" ILIKE :{k} OR "項目" ILIKE :{k})')

            where = [
                '"性別" = :g',
                '"項目" ILIKE :pat',
                f"({' OR '.join(like_clauses)})",
                # 排冬季短水道（與你現有邏輯一致）
                '("賽事名稱" NOT ILIKE \'%冬季短水道%\' AND NOT ("賽事名稱" ILIKE \'%短水道%\' AND "賽事名稱" ILIKE \'%冬%\'))',
            ]
            if y is not None:
                where.append('"年份"::text LIKE :y||\'%\'')
                params["y"] = str(y)

            sql = f"""
                SELECT MIN(sec) FROM (
                  SELECT CASE
                    WHEN POSITION(':' IN "成績"::text)>0
                      THEN SPLIT_PART("成績"::text,':',1)::int*60 + SPLIT_PART("成績"::text,':',2)::float
                      ELSE NULLIF("成績"::text,'')::float
                  END AS sec
                  FROM {TABLE}
                  WHERE {" AND ".join(where)}
                ) t
                WHERE sec IS NOT NULL AND sec>0
            """
            v = db.execute(text(sql), params).scalar()
            return float(v) if v else None

        out = []
        for group_name in GROUP_ALIASES.keys():
            bars = []
            bars.append({"label": "All-Time", "seconds": best_in_year(group_name, None)})
            for y in YEARS:
                bars.append({"label": str(y), "seconds": best_in_year(group_name, y)})
            out.append({"group": group_name, "bars": bars})

        return {"gender": gender, "groups": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"groups failed: {e}")