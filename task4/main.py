"""
Task4 CLI 主入口（含高级功能）
- 登录认证：用户只能查看自己的订单
- 联系人管理：添加/查看/删除联系人，为联系人订票
- 高效生成：多线程并行生成 flight_instance
- 单次启动完成所有功能
"""
import sys
import os
from datetime import date, time

sys.path.insert(0, os.path.dirname(__file__))

import db
import service_generate      as gen
import service_generate_fast as gen_fast
import service_search        as search
import service_booking       as booking
import service_auth          as auth
import service_contact       as contact


# ─────────────────────────────────────────────
# 全局登录状态
# ─────────────────────────────────────────────
_session: dict | None = None   # {"passenger_id": int, "name": str}


def _logged_in() -> bool:
    return _session is not None


def _require_login() -> bool:
    if not _logged_in():
        print("请先登录（选项 L）")
        return False
    return True


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def _input(prompt: str) -> str:
    return input(prompt).strip()


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _parse_time(s: str) -> time:
    return time.fromisoformat(s)


def _opt_time(prompt: str) -> time | None:
    s = _input(prompt)
    if not s:
        return None
    try:
        return _parse_time(s)
    except ValueError:
        print(f"时间格式错误，已忽略: {s}")
        return None


def _print_table(rows: list[dict], cols: list[tuple]):
    if not rows:
        print("（无数据）")
        return
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

def menu_login():
    global _session
    print("\n── 登录 ──")
    try:
        pid = int(_input("乘客 ID: "))
    except ValueError:
        print("ID 必须为整数")
        return
    pwd = _input("密码: ")
    try:
        _session = auth.login(pid, pwd)
        print(f"登录成功，欢迎 {_session['name']}！")
    except ValueError as e:
        print(f"登录失败: {e}")


def menu_logout():
    global _session
    if _session:
        print(f"已退出登录（{_session['name']}）")
        _session = None
    else:
        print("当前未登录")


def menu_set_password():
    print("\n── 设置密码 ──")
    try:
        pid = int(_input("乘客 ID: "))
    except ValueError:
        print("ID 必须为整数")
        return
    pwd = _input("新密码（至少4位）: ")
    try:
        auth.set_password(pid, pwd)
        print("密码设置成功，请重新登录")
    except (ValueError, RuntimeError) as e:
        print(f"失败: {e}")


def menu_generate():
    print("\n── 生成机票 ──")
    try:
        start = _parse_date(_input("开始日期 (YYYY-MM-DD): "))
        end   = _parse_date(_input("结束日期 (YYYY-MM-DD): "))
    except ValueError as e:
        print(f"日期格式错误: {e}")
        return
    mode = _input("模式：[1] 普通  [2] 高效多线程（默认2）: ") or "2"
    print("正在生成，请稍候...")
    try:
        if mode == "1":
            result = gen.generate_instances(start, end)
            print(f"完成：新增 {result['inserted']} 条，跳过 {result['skipped']} 条")
        else:
            result = gen_fast.generate_instances_fast(start, end)
            print(f"完成：新增 {result['inserted']} 条，跳过 {result['skipped']} 条"
                  f"（{result['workers']} 线程，共 {result['total_dates']} 天）")
    except Exception as e:
        print(f"生成失败: {e}")


def menu_search():
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
    depart_from  = _opt_time("出发时间 >= (HH:MM，可选): ")
    depart_to    = _opt_time("出发时间 <= (HH:MM，可选): ")
    arrive_from  = _opt_time("到达时间 >= (HH:MM，可选，仅当日到达): ")
    arrive_to    = _opt_time("到达时间 <= (HH:MM，可选，仅当日到达): ")

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
        ("实例ID",   "instance_id",        8),
        ("航班号",   "flight_no",          10),
        ("航空公司", "airline_name",       16),
        ("出发机场", "source_airport",     18),
        ("到达机场", "destination_airport",18),
        ("出发时间", "departure_time",     10),
        ("到达时间", "arrival_time",       10),
        ("+1天",     "arrival_day_offset",  5),
        ("经济舱价", "economy_price",      10),
        ("经济余座", "economy_remain",      8),
        ("商务舱价", "business_price",     10),
        ("商务余座", "business_remain",     8),
    ])


def menu_book():
    if not _require_login():
        return
    print("\n── 订票 ──")
    try:
        instance_id = int(_input("航班实例 ID（来自搜索结果）: "))
    except ValueError:
        print("ID 必须为整数")
        return

    detail = booking.get_instance_detail(instance_id)
    if not detail:
        print(f"航班实例 {instance_id} 不存在")
        return

    print(f"\n  航班: {detail['flight_no']}  {detail['source_city']} → {detail['destination_city']}")
    print(f"  日期: {detail['flight_date']}  {detail['departure_time']} → {detail['arrival_time']}"
          + ("(+1)" if detail['arrival_day_offset'] else ""))
    print(f"  经济舱: ¥{detail['economy_price']}  余座 {detail['economy_remain']}")
    print(f"  商务舱: ¥{detail['business_price']}  余座 {detail['business_remain']}")

    # 选择订票对象
    pid = _session["passenger_id"]
    contacts = contact.list_contacts(pid)
    booker_id   = pid
    contact_id  = None

    if contacts:
        print(f"\n  为谁订票？")
        print(f"  [0] 本人（{_session['name']}）")
        for c in contacts:
            print(f"  [{c['contact_id']}] {c['name']}  {c['mobile_number']}")
        choice = _input("  输入编号（默认0本人）: ") or "0"
        if choice != "0":
            try:
                contact_id = int(choice)
            except ValueError:
                print("无效选择，默认为本人订票")

    cabin = _input("选择舱位 (economy / business): ").lower()
    confirm = _input(f"确认订票？(y/n): ")
    if confirm.lower() != "y":
        print("已取消")
        return

    try:
        if contact_id:
            result = contact.book_for_contact(pid, contact_id, instance_id, cabin)
            print(f"为联系人订票成功！订单号: {result['order_id']}  价格: ¥{result['price']}")
        else:
            result = booking.book_ticket(pid, instance_id, cabin)
            print(f"订票成功！订单号: {result['order_id']}  价格: ¥{result['price']}")
    except (ValueError, RuntimeError) as e:
        print(f"订票失败: {e}")


def menu_orders():
    if not _require_login():
        return
    print("\n── 我的订单 ──")
    pid = _session["passenger_id"]

    try:
        orders = booking.list_orders(pid)
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
        ok = booking.cancel_order(int(action), pid)
        print("取消成功" if ok else "订单不存在或无权限")
    except ValueError:
        print("订单号必须为整数")
    except Exception as e:
        print(f"取消失败: {e}")


def menu_contacts():
    if not _require_login():
        return
    pid = _session["passenger_id"]
    print("\n── 联系人管理 ──")
    print("  [1] 查看联系人  [2] 添加联系人  [3] 删除联系人  [0] 返回")
    choice = _input("选择: ")

    if choice == "1":
        contacts = contact.list_contacts(pid)
        _print_table(contacts, [
            ("ID",   "contact_id",    6),
            ("姓名", "name",         16),
            ("手机", "mobile_number",16),
        ])

    elif choice == "2":
        name   = _input("联系人姓名: ")
        mobile = _input("手机号: ")
        try:
            cid = contact.add_contact(pid, name, mobile)
            print(f"添加成功，联系人 ID: {cid}")
        except (ValueError, Exception) as e:
            print(f"添加失败: {e}")

    elif choice == "3":
        try:
            cid = int(_input("要删除的联系人 ID: "))
            ok  = contact.delete_contact(cid, pid)
            print("删除成功" if ok else "联系人不存在或无权限")
        except ValueError:
            print("ID 必须为整数")


# ─────────────────────────────────────────────
# 主循环
# ─────────────────────────────────────────────

def _menu_text() -> str:
    status = f"已登录: {_session['name']}" if _logged_in() else "未登录"
    return f"""
╔══════════════════════════════════╗
║   CS307 航班票务系统 Task4       ║
║   {status:<30}║
╠══════════════════════════════════╣
║  L. 登录        X. 退出登录      ║
║  P. 设置密码                     ║
╠══════════════════════════════════╣
║  1. 生成机票（按日期范围）       ║
║  2. 搜索机票                     ║
║  3. 订票          （需登录）     ║
║  4. 我的订单      （需登录）     ║
║  5. 联系人管理    （需登录）     ║
╠══════════════════════════════════╣
║  0. 退出程序                     ║
╚══════════════════════════════════╝"""

HANDLERS = {
    "l": menu_login,
    "x": menu_logout,
    "p": menu_set_password,
    "1": menu_generate,
    "2": menu_search,
    "3": menu_book,
    "4": menu_orders,
    "5": menu_contacts,
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
            print(_menu_text())
            choice = _input("请选择: ").lower()
            if choice == "0":
                print("再见！")
                break
            handler = HANDLERS.get(choice)
            if handler:
                handler()
            else:
                print("无效选项")
    finally:
        db.close()


if __name__ == "__main__":
    main()
