from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db import engine

router = APIRouter()

# === Rank API ===
@router.get("/api/rank")
def rank(
    name: str = Query(..., description="輸入選手姓名"),
    stroke: str = Query(..., description="距離＋泳式，如 50公尺蛙式"),
):
    with engine.connect() as conn:
        # 先找到輸入選手所有符合 stroke 的場次
        sql = text("""
            SELECT 年份, 賽事名稱, 項目, 組別, 成績, seconds
            FROM public."swimming_scores"
            WHERE 姓名 = :name AND 項目 LIKE :pat
        """)
        my_rows = conn.execute(sql, {"name": name, "pat": f"%{stroke}%"}).mappings().all()
        if not my_rows:
            return {"name": name, "stroke": stroke, "error": "no records"}

        # 建立對手池
        opponents = {}
        for row in my_rows:
            conds = {"year": row["年份"], "meet": row["賽事名稱"], "item": row["項目"]}
            if not row["組別"].isdigit():
                conds["group"] = row["組別"]

            q = """
                SELECT 姓名, 年份, 賽事名稱, 項目, 組別, 成績, seconds
                FROM public."swimming_scores"
                WHERE 年份 = :year AND 賽事名稱 = :meet AND 項目 = :item
            """
            if "group" in conds:
                q += " AND 組別 = :group"

            res = conn.execute(text(q), conds).mappings().all()
            for r in res:
                if not r["seconds"] or r["seconds"] <= 0:
                    continue
                opp = r["姓名"]
                if opp not in opponents:
                    opponents[opp] = []
                opponents[opp].append(r)

        # 計算每位對手 PB
        ranks = []
        for opp, rows in opponents.items():
            pb_row = min(rows, key=lambda x: x["seconds"])
            ranks.append({
                "name": opp,
                "pb": pb_row["seconds"],
                "pb_year": pb_row["年份"],
                "pb_meet": pb_row["賽事名稱"],
            })

        # 依 PB 排序
        ranks.sort(key=lambda x: x["pb"])

        # 找輸入選手位置
        denom = len(ranks)
        my_idx = next((i for i, r in enumerate(ranks) if r["name"] == name), None)

        leader = ranks[0] if ranks else None
        you = ranks[my_idx] if my_idx is not None else None

        around = []
        if my_idx is not None:
            for i in range(max(0, my_idx - 3), min(denom, my_idx + 4)):
                if i != my_idx:
                    around.append(ranks[i])

        return {
            "name": name,
            "stroke": stroke,
            "criteria": "同年份＋同賽事名稱＋同項目（組別若非數字則納入比對）",
            "denominator": denom,
            "rank": (my_idx + 1) if my_idx is not None else None,
            "percentile": (100 * (my_idx + 1) / denom) if my_idx is not None else None,
            "leader": leader,
            "you": you,
            "around": around,
            "top": ranks[:10],
        }
