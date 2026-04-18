"""
Task4-1: 给定日期范围，批量生成 flight_instance 记录
对所有 flight_route 在指定日期范围内各生成一条 instance，
价格和余座直接从 tickets.csv 导入的数据中继承（若已存在则跳过）。
"""
from datetime import date, timedelta
from psycopg2.extras import execute_values
import db


def generate_instances(start_date: date, end_date: date) -> dict:
    """
    为所有 flight_route 在 [start_date, end_date] 内生成 flight_instance。
    已存在的 (route_id, flight_date) 组合自动跳过（ON CONFLICT DO NOTHING）。

    返回: {"inserted": int, "skipped": int}
    """
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")

    conn = db.get_conn()
    with conn.cursor() as cur:
        # 取所有航线的默认价格和余座（用现有 instance 的均值作为模板，若无则用固定默认值）
        cur.execute("""
            SELECT
                r.route_id,
                COALESCE(AVG(fi.economy_price), 500.00)  AS economy_price,
                COALESCE(AVG(fi.business_price), 1200.00) AS business_price,
                COALESCE(MAX(fi.economy_remain), 120)    AS economy_remain,
                COALESCE(MAX(fi.business_remain), 30)    AS business_remain
            FROM flight_route r
            LEFT JOIN flight_instance fi ON fi.route_id = r.route_id
            GROUP BY r.route_id
        """)
        routes = cur.fetchall()

    if not routes:
        return {"inserted": 0, "skipped": 0}

    # 构造所有 (route_id, date, prices, seats) 组合
    rows = []
    delta = end_date - start_date
    for offset in range(delta.days + 1):
        d = start_date + timedelta(days=offset)
        for row in routes:
            rows.append((
                row[0],                          # route_id
                d,                               # flight_date
                float(row[2]),                   # business_price
                int(row[4]),                     # business_remain
                float(row[1]),                   # economy_price
                int(row[3]),                     # economy_remain
            ))

    conn = db.get_conn()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO flight_instance
                (route_id, flight_date, business_price, business_remain,
                 economy_price, economy_remain)
            VALUES %s
            ON CONFLICT (route_id, flight_date) DO NOTHING
            """,
            rows,
            page_size=2000,
        )
        inserted = cur.rowcount  # 实际插入行数（跳过的不计）

    db.commit()
    total = len(rows)
    return {"inserted": inserted, "skipped": total - inserted}
