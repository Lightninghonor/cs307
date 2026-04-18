"""
Task4-3: 订票流程
Task4-4: 订单搜索与删除

订票使用 SELECT ... FOR UPDATE 防止并发超卖，整个流程在单事务内完成。
"""
from datetime import date
from typing import Optional
import db


# ─────────────────────────────────────────────
# Task4-3  订票
# ─────────────────────────────────────────────

def get_instance_detail(instance_id: int) -> Optional[dict]:
    """查看指定 flight_instance 的价格和余座（不加锁，仅展示用）"""
    sql = """
        SELECT
            fi.instance_id,
            r.flight_no,
            al.name          AS airline_name,
            ap_src.city      AS source_city,
            ap_dst.city      AS destination_city,
            r.departure_time,
            r.arrival_time,
            r.arrival_day_offset,
            fi.flight_date,
            fi.economy_price,
            fi.economy_remain,
            fi.business_price,
            fi.business_remain
        FROM flight_instance fi
        JOIN flight_route r   ON r.route_id        = fi.route_id
        JOIN airline al       ON al.airline_id      = r.airline_id
        JOIN airport ap_src   ON ap_src.airport_id  = r.source_airport_id
        JOIN airport ap_dst   ON ap_dst.airport_id  = r.destination_airport_id
        WHERE fi.instance_id = %(instance_id)s
    """
    with db.cursor() as cur:
        cur.execute(sql, {"instance_id": instance_id})
        row = cur.fetchone()
        return dict(row) if row else None


def book_ticket(passenger_id: int, instance_id: int, cabin_class: str) -> dict:
    """
    执行订票事务：
      1. SELECT ... FOR UPDATE 锁定 flight_instance 行
      2. 检查余座 > 0
      3. 扣减余座
      4. 插入 booking_order
      5. COMMIT

    返回: {"order_id": int, "price": float}
    抛出: ValueError（余座不足 / 舱位非法）
          RuntimeError（乘客不存在）
    """
    cabin_class = cabin_class.lower().strip()
    if cabin_class not in ("economy", "business"):
        raise ValueError(f"无效舱位: {cabin_class}，请输入 economy 或 business")

    remain_col = f"{cabin_class}_remain"
    price_col  = f"{cabin_class}_price"

    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # 1. 检查乘客存在
            cur.execute(
                "SELECT passenger_id FROM passenger WHERE passenger_id = %s",
                (passenger_id,)
            )
            if cur.fetchone() is None:
                raise RuntimeError(f"乘客 ID {passenger_id} 不存在")

            # 2. 锁定 flight_instance 行
            cur.execute(
                f"""
                SELECT instance_id, {remain_col}, {price_col}
                FROM flight_instance
                WHERE instance_id = %s
                FOR UPDATE
                """,
                (instance_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"flight_instance {instance_id} 不存在")

            remain = row[1]
            price  = float(row[2])

            # 3. 检查余座
            if remain <= 0:
                raise ValueError(f"{cabin_class} 舱已无余座")

            # 4. 扣减余座
            cur.execute(
                f"""
                UPDATE flight_instance
                SET {remain_col} = {remain_col} - 1
                WHERE instance_id = %s
                """,
                (instance_id,)
            )

            # 5. 插入订单
            cur.execute(
                """
                INSERT INTO booking_order (passenger_id, instance_id, cabin_class)
                VALUES (%s, %s, %s)
                RETURNING order_id
                """,
                (passenger_id, instance_id, cabin_class)
            )
            order_id = cur.fetchone()[0]

        db.commit()
        return {"order_id": order_id, "price": price, "cabin_class": cabin_class}

    except Exception:
        db.rollback()
        raise


# ─────────────────────────────────────────────
# Task4-4  订单搜索与删除
# ─────────────────────────────────────────────

def list_orders(passenger_id: int) -> list[dict]:
    """查询指定乘客的所有订单"""
    sql = """
        SELECT
            bo.order_id,
            bo.cabin_class,
            bo.order_time,
            r.flight_no,
            al.name          AS airline_name,
            ap_src.city      AS source_city,
            ap_dst.city      AS destination_city,
            r.departure_time,
            r.arrival_time,
            r.arrival_day_offset,
            fi.flight_date,
            CASE bo.cabin_class
                WHEN 'economy'  THEN fi.economy_price
                WHEN 'business' THEN fi.business_price
            END              AS price
        FROM booking_order bo
        JOIN flight_instance fi ON fi.instance_id  = bo.instance_id
        JOIN flight_route r     ON r.route_id       = fi.route_id
        JOIN airline al         ON al.airline_id     = r.airline_id
        JOIN airport ap_src     ON ap_src.airport_id = r.source_airport_id
        JOIN airport ap_dst     ON ap_dst.airport_id = r.destination_airport_id
        WHERE bo.passenger_id = %(passenger_id)s
        ORDER BY bo.order_time DESC
    """
    with db.cursor() as cur:
        cur.execute(sql, {"passenger_id": passenger_id})
        return [dict(row) for row in cur.fetchall()]


def cancel_order(order_id: int, passenger_id: int) -> bool:
    """
    取消订单（归还余座）。
    passenger_id 用于鉴权，确保只能取消自己的订单。
    返回 True 表示成功，False 表示订单不存在或无权限。
    """
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # 查订单并锁行
            cur.execute(
                """
                SELECT bo.order_id, bo.instance_id, bo.cabin_class
                FROM booking_order bo
                WHERE bo.order_id = %s AND bo.passenger_id = %s
                FOR UPDATE
                """,
                (order_id, passenger_id)
            )
            row = cur.fetchone()
            if row is None:
                return False

            instance_id  = row[1]
            cabin_class  = row[2]
            remain_col   = f"{cabin_class}_remain"

            # 归还余座
            cur.execute(
                f"""
                UPDATE flight_instance
                SET {remain_col} = {remain_col} + 1
                WHERE instance_id = %s
                """,
                (instance_id,)
            )

            # 删除订单
            cur.execute(
                "DELETE FROM booking_order WHERE order_id = %s",
                (order_id,)
            )

        db.commit()
        return True

    except Exception:
        db.rollback()
        raise
