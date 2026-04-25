"""
高级功能2：登录认证
- passenger 表新增 password_hash 字段（bcrypt）
- 提供注册密码、登录验证接口
- 登录后返回 session dict，后续操作通过 session 鉴权
"""
import hashlib
import os
import db


def _hash_password(password: str) -> str:
    """SHA-256 哈希（无需额外依赖；生产环境建议用 bcrypt）"""
    salt = os.urandom(16).hex()
    h = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"{salt}:{h}"


def _verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(":", 1)
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest() == h


def set_password(passenger_id: int, password: str) -> None:
    """为乘客设置（或重置）密码"""
    if len(password) < 4:
        raise ValueError("密码至少4位")
    hashed = _hash_password(password)
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE passenger SET password_hash = %s WHERE passenger_id = %s",
            (hashed, passenger_id)
        )
        if cur.rowcount == 0:
            db.rollback()
            raise RuntimeError(f"乘客 {passenger_id} 不存在")
    db.commit()


def login(passenger_id: int, password: str) -> dict:
    """
    验证登录，成功返回 session dict。
    session = {"passenger_id": int, "name": str}
    失败抛出 ValueError。
    """
    with db.cursor() as cur:
        cur.execute(
            "SELECT passenger_id, name, password_hash FROM passenger WHERE passenger_id = %s",
            (passenger_id,)
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError("乘客 ID 不存在")
    if not row["password_hash"]:
        raise ValueError("该账户未设置密码，请先注册密码")
    if not _verify_password(password, row["password_hash"]):
        raise ValueError("密码错误")

    return {"passenger_id": row["passenger_id"], "name": row["name"]}
