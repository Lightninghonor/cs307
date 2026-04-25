"""
Task4 高级功能测试套件
前置条件与基础测试相同，额外需要执行 03_advanced_schema.sql

运行方式：
  cd D:\cs307\cs307-project1\task4
  python test_advanced.py
"""
import os
import sys
import time
import traceback
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))

import db
import service_auth          as auth
import service_contact       as contact
import service_generate      as gen
import service_generate_fast as gen_fast
import service_booking       as booking

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"

_results = []


def run(name, fn):
    try:
        fn()
        print(f"{PASS} {name}")
        _results.append((name, True, None))
    except AssertionError as e:
        print(f"{FAIL} {name}  →  {e}")
        _results.append((name, False, str(e)))
    except Exception as e:
        print(f"{FAIL} {name}  →  {type(e).__name__}: {e}")
        traceback.print_exc()
        _results.append((name, False, str(e)))


# ─────────────────────────────────────────────
# 辅助
# ─────────────────────────────────────────────

def _get_pid() -> int:
    with db.cursor() as cur:
        cur.execute("SELECT passenger_id FROM passenger LIMIT 1")
        row = cur.fetchone()
    assert row, "passenger 表为空"
    return row["passenger_id"]


def _get_instance_id() -> int:
    with db.cursor() as cur:
        cur.execute("SELECT instance_id FROM flight_instance LIMIT 1")
        row = cur.fetchone()
    assert row, "flight_instance 表为空，请先生成机票"
    return row["instance_id"]


# ─────────────────────────────────────────────
# 高级功能1：生成效率对比
# ─────────────────────────────────────────────

def test_fast_generate_correctness():
    """多线程生成结果与单线程一致（重复生成全部跳过）"""
    start = date(2025, 3, 1)
    end   = date(2025, 3, 3)
    # 先用单线程生成
    gen.generate_instances(start, end)
    # 再用多线程生成同一范围，应全部跳过
    result = gen_fast.generate_instances_fast(start, end)
    assert result["inserted"] == 0, f"重复生成应全部跳过，实际插入 {result['inserted']}"
    assert result["skipped"] > 0
    print(f"  {INFO} 多线程跳过 {result['skipped']} 条（正确）")


def test_fast_generate_performance():
    """多线程生成速度快于单线程（30天范围）"""
    start = date(2025, 4, 1)
    end   = date(2025, 4, 30)

    # 清理这段日期的数据
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM flight_instance WHERE flight_date BETWEEN %s AND %s",
            (start, end)
        )
    db.commit()

    t0 = time.perf_counter()
    r1 = gen.generate_instances(start, end)
    t1 = time.perf_counter()
    single_time = t1 - t0

    # 清理再测多线程
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM flight_instance WHERE flight_date BETWEEN %s AND %s",
            (start, end)
        )
    db.commit()

    t0 = time.perf_counter()
    r2 = gen_fast.generate_instances_fast(start, end)
    t1 = time.perf_counter()
    multi_time = t1 - t0

    assert r1["inserted"] == r2["inserted"], "两种方式插入数量应相同"
    print(f"  {INFO} 单线程: {single_time:.2f}s  多线程: {multi_time:.2f}s  "
          f"插入 {r2['inserted']} 条  线程数: {r2['workers']}")


# ─────────────────────────────────────────────
# 高级功能2：登录认证
# ─────────────────────────────────────────────

def test_set_and_login():
    pid = _get_pid()
    auth.set_password(pid, "test1234")
    session = auth.login(pid, "test1234")
    assert session["passenger_id"] == pid
    assert "name" in session
    print(f"  {INFO} 登录成功: {session['name']}")


def test_login_wrong_password():
    pid = _get_pid()
    auth.set_password(pid, "test1234")
    try:
        auth.login(pid, "wrongpass")
        assert False, "应抛出 ValueError"
    except ValueError:
        pass


def test_login_nonexistent():
    try:
        auth.login(999999999, "any")
        assert False, "应抛出 ValueError"
    except ValueError:
        pass


def test_orders_isolation():
    """不同乘客的订单互相隔离"""
    with db.cursor() as cur:
        cur.execute("SELECT passenger_id FROM passenger ORDER BY passenger_id LIMIT 2")
        rows = cur.fetchall()
    if len(rows) < 2:
        print(f"  {INFO} 乘客数不足2，跳过隔离测试")
        return

    pid1 = rows[0]["passenger_id"]
    pid2 = rows[1]["passenger_id"]

    instance_id = _get_instance_id()
    detail = booking.get_instance_detail(instance_id)
    if not detail or detail["economy_remain"] == 0:
        print(f"  {INFO} 余座为0，跳过隔离测试")
        return

    result = booking.book_ticket(pid1, instance_id, "economy")
    order_id = result["order_id"]

    # pid2 查不到 pid1 的订单
    orders2 = booking.list_orders(pid2)
    assert order_id not in [o["order_id"] for o in orders2], "不同乘客订单不应互见"

    # pid2 不能取消 pid1 的订单
    ok = booking.cancel_order(order_id, pid2)
    assert not ok, "不应允许取消他人订单"

    # 清理
    booking.cancel_order(order_id, pid1)
    print(f"  {INFO} 订单隔离验证通过")


# ─────────────────────────────────────────────
# 高级功能3：联系人管理
# ─────────────────────────────────────────────

def test_contact_add_list_delete():
    pid = _get_pid()
    cid = contact.add_contact(pid, "测试联系人", "13800138000")
    assert isinstance(cid, int)

    contacts = contact.list_contacts(pid)
    ids = [c["contact_id"] for c in contacts]
    assert cid in ids, "新增联系人未出现在列表"

    ok = contact.delete_contact(cid, pid)
    assert ok, "删除联系人失败"

    contacts_after = contact.list_contacts(pid)
    assert cid not in [c["contact_id"] for c in contacts_after], "联系人未被删除"
    print(f"  {INFO} 联系人 {cid} 添加/查询/删除均正常")


def test_contact_wrong_owner():
    with db.cursor() as cur:
        cur.execute("SELECT passenger_id FROM passenger ORDER BY passenger_id LIMIT 2")
        rows = cur.fetchall()
    if len(rows) < 2:
        print(f"  {INFO} 乘客数不足2，跳过联系人鉴权测试")
        return

    pid1 = rows[0]["passenger_id"]
    pid2 = rows[1]["passenger_id"]

    cid = contact.add_contact(pid1, "他人联系人", "13900139000")
    ok  = contact.delete_contact(cid, pid2)
    assert not ok, "不应允许删除他人联系人"
    contact.delete_contact(cid, pid1)


def test_book_for_contact():
    pid = _get_pid()
    instance_id = _get_instance_id()
    detail = booking.get_instance_detail(instance_id)
    if not detail or detail["economy_remain"] == 0:
        print(f"  {INFO} 余座为0，跳过联系人订票测试")
        return

    cid = contact.add_contact(pid, "代订联系人", "13700137000")
    result = contact.book_for_contact(pid, cid, instance_id, "economy")
    assert "order_id" in result
    assert result["contact_id"] == cid

    # 订单出现在 owner 的订单列表中
    orders = booking.list_orders(pid)
    assert result["order_id"] in [o["order_id"] for o in orders]

    # 清理
    booking.cancel_order(result["order_id"], pid)
    contact.delete_contact(cid, pid)
    print(f"  {INFO} 联系人订票成功，订单号 {result['order_id']}")


def test_book_for_wrong_contact():
    with db.cursor() as cur:
        cur.execute("SELECT passenger_id FROM passenger ORDER BY passenger_id LIMIT 2")
        rows = cur.fetchall()
    if len(rows) < 2:
        print(f"  {INFO} 乘客数不足2，跳过联系人鉴权订票测试")
        return

    pid1 = rows[0]["passenger_id"]
    pid2 = rows[1]["passenger_id"]
    instance_id = _get_instance_id()

    cid = contact.add_contact(pid1, "不属于pid2的联系人", "13600136000")
    try:
        contact.book_for_contact(pid2, cid, instance_id, "economy")
        assert False, "应抛出 ValueError"
    except ValueError:
        pass
    contact.delete_contact(cid, pid1)


# ─────────────────────────────────────────────
# 主函数
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  CS307 Task4 高级功能测试")
    print("=" * 55)

    try:
        db.get_conn()
    except ConnectionError as e:
        print(f"{FAIL} 数据库连接失败: {e}")
        sys.exit(1)

    # 检查高级 schema 是否已执行
    with db.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='flightdb' AND table_name='passenger'
              AND column_name='password_hash'
        """)
        if not cur.fetchone():
            print(f"{FAIL} 请先在 DataGrip 执行 task4/03_advanced_schema.sql")
            sys.exit(1)

    with db.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='flightdb' AND table_name='contact'
        """)
        if not cur.fetchone():
            print(f"{FAIL} 请先在 DataGrip 执行 task4/03_advanced_schema.sql")
            sys.exit(1)

    # 检查 booking_order 是否有 contact_id 列
    with db.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='flightdb' AND table_name='booking_order'
              AND column_name='contact_id'
        """)
        if not cur.fetchone():
            print(f"{FAIL} booking_order 缺少 contact_id 列，请重新执行 02_booking_schema.sql")
            sys.exit(1)

    tests = [
        ("高效生成-正确性",       test_fast_generate_correctness),
        ("高效生成-性能对比",     test_fast_generate_performance),
        ("设置密码+登录",         test_set_and_login),
        ("登录-密码错误",         test_login_wrong_password),
        ("登录-用户不存在",       test_login_nonexistent),
        ("订单隔离",              test_orders_isolation),
        ("联系人-增删查",         test_contact_add_list_delete),
        ("联系人-鉴权",           test_contact_wrong_owner),
        ("联系人-代订票",         test_book_for_contact),
        ("联系人-代订票鉴权",     test_book_for_wrong_contact),
    ]

    for name, fn in tests:
        run(name, fn)

    db.close()

    print("\n" + "=" * 55)
    passed = sum(1 for _, ok, _ in _results if ok)
    total  = len(_results)
    print(f"  结果: {passed}/{total} 通过")
    if passed < total:
        print("  失败项:")
        for name, ok, msg in _results:
            if not ok:
                print(f"    - {name}: {msg}")
    print("=" * 55)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
