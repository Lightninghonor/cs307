"""
高级功能3：联系人管理 + 为联系人订票
"""
import db
import service_booking as booking


def add_contact(owner_id: int, name: str, mobile_number: str) -> int:
    """
    为乘客添加联系人。
    返回新建的 contact_id。
    """
    name = name.strip()
    mobile_number = mobile_number.strip()
    if not name:
        raise ValueError("联系人姓名不能为空")

    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO contact (owner_id, name, mobile_number)
            VALUES (%s, %s, %s)
            RETURNING contact_id
            """,
            (owner_id, name, mobile_number)
        )
        contact_id = cur.fetchone()[0]
    db.commit()
    return contact_id


def list_contacts(owner_id: int) -> list[dict]:
    """查询乘客的所有联系人"""
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT contact_id, name, mobile_number
            FROM contact
            WHERE owner_id = %s
            ORDER BY contact_id
            """,
            (owner_id,)
        )
        return [dict(r) for r in cur.fetchall()]


def delete_contact(contact_id: int, owner_id: int) -> bool:
    """删除联系人，owner_id 鉴权。返回 True 表示成功。"""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM contact WHERE contact_id = %s AND owner_id = %s",
            (contact_id, owner_id)
        )
        deleted = cur.rowcount
    db.commit()
    return deleted > 0


def book_for_contact(owner_id: int, contact_id: int,
                     instance_id: int, cabin_class: str) -> dict:
    """
    以联系人身份订票（订单的 passenger_id 记为 owner_id，
    contact_id 记录在订单备注中）。

    实现方式：booking_order 新增 contact_id 可选列，
    若为联系人订票则填入，否则为 NULL。
    返回与 book_ticket 相同的 dict，额外含 contact_id。
    """
    # 验证联系人属于该乘客
    with db.cursor() as cur:
        cur.execute(
            "SELECT contact_id, name FROM contact WHERE contact_id = %s AND owner_id = %s",
            (contact_id, owner_id)
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"联系人 {contact_id} 不存在或不属于乘客 {owner_id}")

    cabin_class = cabin_class.lower().strip()
    if cabin_class not in ("economy", "business"):
        raise ValueError(f"无效舱位: {cabin_class}")

    remain_col = f"{cabin_class}_remain"
    price_col  = f"{cabin_class}_price"

    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # 锁定航班实例
            cur.execute(
                f"""
                SELECT instance_id, {remain_col}, {price_col}
                FROM flight_instance
                WHERE instance_id = %s
                FOR UPDATE
                """,
                (instance_id,)
            )
            inst = cur.fetchone()
            if inst is None:
                raise ValueError(f"航班实例 {instance_id} 不存在")
            if inst[1] <= 0:
                raise ValueError(f"{cabin_class} 舱已无余座")

            price = float(inst[2])

            # 扣减余座
            cur.execute(
                f"UPDATE flight_instance SET {remain_col} = {remain_col} - 1 WHERE instance_id = %s",
                (instance_id,)
            )

            # 插入订单，记录 contact_id
            cur.execute(
                """
                INSERT INTO booking_order (passenger_id, instance_id, cabin_class, contact_id)
                VALUES (%s, %s, %s, %s)
                RETURNING order_id
                """,
                (owner_id, instance_id, cabin_class, contact_id)
            )
            order_id = cur.fetchone()[0]

        db.commit()
        return {"order_id": order_id, "price": price,
                "cabin_class": cabin_class, "contact_id": contact_id}
    except Exception:
        db.rollback()
        raise
