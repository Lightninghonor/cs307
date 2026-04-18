"""
Task4 CLI 主入口
单次启动，菜单驱动，完成所有基础功能。
"""
import sys
import os
from datetime import date, time

# 将 task4 目录加入路径
sys.path.insert(0, os.path.dirname(__file__))

import db
import service_generate as gen
import service_search   as search
import service_booking  as booking


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def _input(prompt: str) -> str:
    return input(prompt).strip()


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _parse_time(s: str) -> time:
    return time.fromisoformat(s)


def _print_table(rows: list[dict], cols: list[tuple]):
    """
    cols: [(header, key, width), ...]
    """
    header = "  ".join(h.ljust(w) for h, _, w in cols)
    sep    = "  ".join("-" * w for _, _, w in cols)
    print(header)
    print(sep)
    for row in rows:
        line = "  ".join(str(row.get(k, "")).ljust(w)[:w] for _, k, w in cols)
        print(line)
    print(f"（共 {len(rows)} 条）")


# ─────────────────────────────────────────────
# 菜单处理函数
# ─────────────────────────────────────────────

def menu_generate():
    """[1] 生成机票（管理员功能）"""
    print("\n── 生成机票 ──")
    try:
        start = _parse_date(_input("开始日期 (YYYY-MM-DD): "))
        end   = _parse_date(_input("结束日期 (YYYY-MM-DD): "))
    except ValueError as e:
        print(f"日期格式错误: {e}")
        return

    print("正在生成，请稍候...")
    try:
        result = gen.generate_instances(start, end)
        print(f"完成：新增 {result['inserted']} 条，跳过已有 {result['skipped']} 条")
    except Exception as e:
        print(f"生成失败: {e}")


def menu_search():
    """[2] 搜索机票"""
    print("\n── 搜索机票 ──")
    source_city = _input("出发城市（必填）: ")
    dest_city   = _input("到达城市（必填）: ")
    date_str    = _input("出行日期 (YYYY-MM-DD，必填): ")

    try:
        flight_date = _parse_date(date_str)
    except ValueError:
        print("日期格式错误")
        return

    airline_name = _input("航空公司（可选，回车跳过）: ") or None

    def _opt_time(prompt):
        s = _input(prompt)
        if not s:
            return None
        try:
            return _parse_time(s)
        except ValueError:
            print(f"时间格式错误，已忽略: {s}")
            return None

    depart_from = _opt_time("出发时间 >= (HH:MM，可选): ")
    depart_to   = _opt_time("出发时间 <= (HH:MM，可选): ")
    arrive_from = _opt_time("到达时间 >= (HH:MM，可选，仅当日到达): ")
    arrive_to   = _opt_time("到达时间 <= (HH:MM，可选，仅当日到达): ")

    try:
        results = search.search_tickets(
            source_city, dest_city, flight_date,
            airline_name, depart_from, depart_to, arrive_from, arrive_to
        )
    except Exception as e:
        print(f"查询失败: {e}")
        return

    if not results:
        print("未找到符合条件的航班")
        return

    _print_table(results, [
        ("实例ID",    "instance_id",       8),
        ("航班号",    "flight_no",         10),
        ("航空公司",  "airline_name",      16),
        ("出发机场",  "source_airport",    20),
        ("到达机场",  "destination_airport", 20),
        ("出发时间",  "departure_time",    10),
        ("到达时间",  "arrival_time",      10),
        ("+1天",      "arrival_day_offset", 5),
        ("经济舱价",  "economy_price",     10),
        ("经济余座",  "economy_remain",    8),
        ("商务舱价",  "business_price",    10),
        ("商务余座",  "business_remain",   8),
    ])


def menu_book():
    """[3] 订票"""
    print("\n── 订票 ──")
    try:
        passenger_id = int(_input("乘客 ID: "))
        instance_id  = int(_input("航班实例 ID（来自搜索结果）: "))
    except ValueError:
        print("ID 必须为整数")
        return

    # 展示价格详情
    detail = booking.get_instance_detail(instance_id)
    if not detail:
        print(f"航班实例 {instance_id} 不存在")
        return

    print(f"\n  航班: {detail['flight_no']}  {detail['source_city']} → {detail['destination_city']}")
    print(f"  日期: {detail['flight_date']}  {detail['departure_time']} → {detail['arrival_time']}"
          + ("(+1)" if detail['arrival_day_offset'] else ""))
    print(f"  经济舱: ¥{detail['economy_price']}  余座 {detail['economy_remain']}")
    print(f"  商务舱: ¥{detail['business_price']}  余座 {detail['business_remain']}")

    cabin = _input("选择舱位 (economy / business): ").lower()
    confirm = _input(f"确认以 {cabin} 舱订票？(y/n): ")
    if confirm.lower() != "y":
        print("已取消")
        return

    try:
        result = booking.book_ticket(passenger_id, instance_id, cabin)
        print(f"订票成功！订单号: {result['order_id']}  价格: ¥{result['price']}")
    except (ValueError, RuntimeError) as e:
        print(f"订票失败: {e}")
    except Exception as e:
        print(f"系统错误: {e}")


def menu_orders():
    """[4] 我的订单"""
    print("\n── 我的订单 ──")
    try:
        passenger_id = int(_input("乘客 ID: "))
    except ValueError:
        print("ID 必须为整数")
        return

    try:
        orders = booking.list_orders(passenger_id)
    except Exception as e:
        print(f"查询失败: {e}")
        return

    if not orders:
        print("暂无订单")
        return

    _print_table(orders, [
        ("订单号",   "order_id",          8),
        ("舱位",     "cabin_class",        8),
        ("航班号",   "flight_no",         10),
        ("出发城市", "source_city",       12),
        ("到达城市", "destination_city",  12),
        ("日期",     "flight_date",       12),
        ("出发时间", "departure_time",    10),
        ("价格",     "price",             10),
        ("下单时间", "order_time",        22),
    ])

    action = _input("\n输入订单号取消订单（回车跳过）: ")
    if not action:
        return
    try:
        order_id = int(action)
        ok = booking.cancel_order(order_id, passenger_id)
        print("取消成功" if ok else "订单不存在或无权限")
    except ValueError:
        print("订单号必须为整数")
    except Exception as e:
        print(f"取消失败: {e}")


# ─────────────────────────────────────────────
# 主循环
# ─────────────────────────────────────────────

MENU = """
╔══════════════════════════════╗
║   CS307 航班票务系统 Task4   ║
╠══════════════════════════════╣
║  1. 生成机票（按日期范围）   ║
║  2. 搜索机票                 ║
║  3. 订票                     ║
║  4. 我的订单（查看/取消）    ║
║  0. 退出                     ║
╚══════════════════════════════╝
"""

HANDLERS = {
    "1": menu_generate,
    "2": menu_search,
    "3": menu_book,
    "4": menu_orders,
}


def main():
    print("正在连接数据库...")
    try:
        db.get_conn()
        print("连接成功")
    except ConnectionError as e:
        print(e)
        print("请设置环境变量 DATABASE_URL 或 DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD")
        sys.exit(1)

    try:
        while True:
            print(MENU)
            choice = _input("请选择: ")
            if choice == "0":
                print("再见！")
                break
            handler = HANDLERS.get(choice)
            if handler:
                handler()
            else:
                print("无效选项，请重新输入")
    finally:
        db.close()


if __name__ == "__main__":
    main()
