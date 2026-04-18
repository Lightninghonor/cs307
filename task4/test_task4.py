"""
Task4 基础功能测试套件
运行前提：
  1. PostgreSQL 已启动，project1 数据库已建好
  2. 已执行 01_create_schema.sql 和 task4/02_booking_schema.sql
  3. 已通过 import_csv_to_postgres.py 导入数据
  4. 设置环境变量（见下方说明）

运行方式：
  cd task4
  python test_task4.py
"""
import os
import sys
import traceback
from datetime import date, time

sys.path.insert(0, os.path.dirname(__file__))

import db
import service_generate as gen
import service_search   as search
import service_booking  as booking

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
# 辅助：获取测试用的真实数据
# ─────────────────────────────────────────────

def _get_sample_passenger_id() -> int:
    with db.cursor() as cur:
        cur.execute("SELECT passenger_id FROM passenger LIMIT 1")
        row = cur.fetchone()
    assert row, "passenger 表为空，请先导入数据"
    return row["passenger_id"]


def _get_sample_cities() -> tuple[str, str]:
    """返回数据库中存在的一对 (source_city, dest_city)"""
    with db.cursor() as cur:
        cur.execute("""
            SELECT ap_src.city, ap_dst.city
            FROM flight_route r
            JOIN airport ap_src ON ap_src.airport_id = r.source_airport_id
            JOIN airport ap_dst ON ap_dst.airport_id = r.destination_airport_id
            LIMIT 1
        """)
        row = cur.fetchone()
    assert row, "flight_route 表为空，请先导入数据"
    return row["city"], row["city_1"] if "city_1" in row else list(row.values())[1]


def _get_sample_instance_id() -> int:
    with db.cursor() as cur:
        cur.execute("SELECT instance_id FROM flight_instance LIMIT 1")
        row = cur.fetchone()
    assert row, "flight_instance 表为空，请先生成机票"
    return row["instance_id"]


# ─────────────────────────────────────────────
# 测试用例
# ─────────────────────────────────────────────

def test_db_connect():
    conn = db.get_conn()
    assert conn and not conn.closed, "数据库连接失败"


def test_schema_exists():
    with db.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'flightdb'
        """)
        tables = {r["table_name"] for r in cur.fetchall()}
    required = {"region", "airline", "airport", "passenger",
                "flight_route", "flight_instance", "booking_order"}
    missing = required - tables
    assert not missing, f"缺少表: {missing}"


def test_data_imported():
    with db.cursor() as cur:
        for tbl in ("region", "airline", "airport", "passenger", "flight_route"):
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}")
            cnt = cur.fetchone()["cnt"]
            assert cnt > 0, f"{tbl} 表无数据"
    print(f"  {INFO} 基础表均有数据")


def test_generate_instances():
    start = date(2025, 1, 1)
    end   = date(2025, 1, 3)
    result = gen.generate_instances(start, end)
    assert "inserted" in result and "skipped" in result
    # 再次生成同一范围，应全部跳过
    result2 = gen.generate_instances(start, end)
    assert result2["inserted"] == 0, "重复生成应全部跳过"
    print(f"  {INFO} 首次插入 {result['inserted']} 条，重复跳过 {result2['skipped']} 条")


def test_generate_invalid_date():
    try:
        gen.generate_instances(date(2025, 1, 5), date(2025, 1, 1))
        assert False, "应抛出 ValueError"
    except ValueError:
        pass


def test_search_basic():
    # 先确保有 instance 数据
    with db.cursor() as cur:
        cur.execute("""
            SELECT ap_src.city AS sc, ap_dst.city AS dc, fi.flight_date
            FROM flight_instance fi
            JOIN flight_route r   ON r.route_id        = fi.route_id
            JOIN airport ap_src   ON ap_src.airport_id  = r.source_airport_id
            JOIN airport ap_dst   ON ap_dst.airport_id  = r.destination_airport_id
            LIMIT 1
        """)
        row = cur.fetchone()
    assert row, "flight_instance 无数据，请先运行 test_generate_instances"

    results = search.search_tickets(row["sc"], row["dc"], row["flight_date"])
    assert isinstance(results, list)
    assert len(results) >= 1
    first = results[0]
    assert "instance_id" in first
    assert "economy_price" in first
    # 验证按 economy_price 升序
    prices = [r["economy_price"] for r in results]
    assert prices == sorted(prices), "结果未按 economy_price 升序"
    print(f"  {INFO} 搜索到 {len(results)} 条航班")


def test_search_no_result():
    results = search.search_tickets("不存在城市A", "不存在城市B", date(2099, 1, 1))
    assert results == [], "不存在的城市应返回空列表"


def test_search_with_time_filter():
    with db.cursor() as cur:
        cur.execute("""
            SELECT ap_src.city AS sc, ap_dst.city AS dc, fi.flight_date,
                   r.departure_time
            FROM flight_instance fi
            JOIN flight_route r   ON r.route_id        = fi.route_id
            JOIN airport ap_src   ON ap_src.airport_id  = r.source_airport_id
            JOIN airport ap_dst   ON ap_dst.airport_id  = r.destination_airport_id
            WHERE r.arrival_day_offset = 0
            LIMIT 1
        """)
        row = cur.fetchone()
    if not row:
        print(f"  {INFO} 无当日到达航班，跳过时间过滤测试")
        return

    results = search.search_tickets(
        row["sc"], row["dc"], row["flight_date"],
        depart_from=time(0, 0), depart_to=time(23, 59)
    )
    assert isinstance(results, list)


def test_book_and_cancel():
    passenger_id = _get_sample_passenger_id()
    instance_id  = _get_sample_instance_id()

    # 查当前余座
    detail_before = booking.get_instance_detail(instance_id)
    assert detail_before, "instance 不存在"
    remain_before = detail_before["economy_remain"]

    if remain_before == 0:
        print(f"  {INFO} economy 余座为 0，跳过订票测试")
        return

    # 订票
    result = booking.book_ticket(passenger_id, instance_id, "economy")
    assert "order_id" in result
    order_id = result["order_id"]

    # 验证余座减少
    detail_after = booking.get_instance_detail(instance_id)
    assert detail_after["economy_remain"] == remain_before - 1, "余座未正确扣减"

    # 查订单
    orders = booking.list_orders(passenger_id)
    order_ids = [o["order_id"] for o in orders]
    assert order_id in order_ids, "订单未出现在列表中"

    # 取消订单
    ok = booking.cancel_order(order_id, passenger_id)
    assert ok, "取消订单失败"

    # 验证余座恢复
    detail_restored = booking.get_instance_detail(instance_id)
    assert detail_restored["economy_remain"] == remain_before, "取消后余座未恢复"

    print(f"  {INFO} 订单 {order_id} 创建并取消成功，余座正确恢复")


def test_book_invalid_cabin():
    passenger_id = _get_sample_passenger_id()
    instance_id  = _get_sample_instance_id()
    try:
        booking.book_ticket(passenger_id, instance_id, "vip")
        assert False, "应抛出 ValueError"
    except ValueError:
        pass


def test_book_nonexistent_passenger():
    instance_id = _get_sample_instance_id()
    try:
        booking.book_ticket(999999999, instance_id, "economy")
        assert False, "应抛出 RuntimeError"
    except RuntimeError:
        pass


def test_cancel_wrong_passenger():
    """乘客不能取消他人订单"""
    passenger_id = _get_sample_passenger_id()
    instance_id  = _get_sample_instance_id()

    detail = booking.get_instance_detail(instance_id)
    if not detail or detail["economy_remain"] == 0:
        print(f"  {INFO} 余座为 0，跳过鉴权测试")
        return

    result = booking.book_ticket(passenger_id, instance_id, "economy")
    order_id = result["order_id"]

    # 用另一个 passenger_id 尝试取消
    ok = booking.cancel_order(order_id, passenger_id + 1)
    assert not ok, "不应允许他人取消订单"

    # 清理：用正确 passenger 取消
    booking.cancel_order(order_id, passenger_id)


# ─────────────────────────────────────────────
# 主函数
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  CS307 Task4 基础功能测试")
    print("=" * 55)

    # 连接数据库
    try:
        db.get_conn()
    except ConnectionError as e:
        print(f"{FAIL} 数据库连接失败: {e}")
        print("请设置 DATABASE_URL 或 DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD")
        sys.exit(1)

    tests = [
        ("DB连接",              test_db_connect),
        ("Schema完整性",        test_schema_exists),
        ("基础数据已导入",      test_data_imported),
        ("生成机票-正常",       test_generate_instances),
        ("生成机票-日期非法",   test_generate_invalid_date),
        ("搜索机票-基础",       test_search_basic),
        ("搜索机票-无结果",     test_search_no_result),
        ("搜索机票-时间过滤",   test_search_with_time_filter),
        ("订票+取消-完整流程",  test_book_and_cancel),
        ("订票-非法舱位",       test_book_invalid_cabin),
        ("订票-乘客不存在",     test_book_nonexistent_passenger),
        ("取消-鉴权",           test_cancel_wrong_passenger),
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
