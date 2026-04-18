# Task4 基础功能说明

## 文件结构

```
task4/
├── 02_booking_schema.sql   订单表建表 SQL
├── db.py                   数据库连接管理
├── service_generate.py     Task4-1 机票生成
├── service_search.py       Task4-2 机票搜索
├── service_booking.py      Task4-3/4 订票与订单管理
├── main.py                 CLI 主入口
└── test_task4.py           自动化测试套件
```

---

## 02_booking_schema.sql

在 `01_create_schema.sql` 执行完毕后运行，新增订单表。

**新增表：`booking_order`**

| 列名 | 类型 | 说明 |
|---|---|---|
| `order_id` | int, 自增主键 | 订单唯一标识 |
| `passenger_id` | int, FK→passenger | 下单乘客 |
| `instance_id` | int, FK→flight_instance | 对应航班实例 |
| `cabin_class` | varchar(10) | 舱位，只允许 `economy` 或 `business` |
| `order_time` | timestamptz | 下单时间，默认 `now()` |

**执行方式**：在 DataGrip 连接 project1 后打开此文件，`Ctrl+A` → `Ctrl+Enter`。

---

## db.py

数据库连接管理模块，其他所有模块通过此文件访问数据库。

**连接参数（优先级从高到低）：**

| 方式 | 说明 |
|---|---|
| 环境变量 `DATABASE_URL` | 完整 DSN，如 `postgresql://postgres:密码@localhost:5432/project1` |
| 环境变量 `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | 分项设置 |
| 默认值 | host=localhost, port=5432, dbname=project1, user=postgres |

**注意**：`search_path` 自动设为 `flightdb,public`，无需手动指定。

**对外接口：**

```python
db.get_conn()   # 获取连接（懒加载，断线自动重连）
db.cursor()     # 获取游标，返回字典格式结果
db.commit()     # 提交事务
db.rollback()   # 回滚事务
db.close()      # 关闭连接
```

---

## service_generate.py — Task4-1 机票生成

**函数：`generate_instances(start_date, end_date) -> dict`**

为数据库中所有 `flight_route` 在指定日期范围内批量生成 `flight_instance` 记录。

| 参数 | 类型 | 说明 |
|---|---|---|
| `start_date` | `datetime.date` | 开始日期（含） |
| `end_date` | `datetime.date` | 结束日期（含） |

**返回值：**
```python
{"inserted": int, "skipped": int}
# inserted: 实际新增条数
# skipped:  已存在跳过条数（ON CONFLICT DO NOTHING）
```

**异常：** `start_date > end_date` 时抛出 `ValueError`。

**价格和余座来源：** 取该航线现有 `flight_instance` 的均值；若该航线无历史数据，默认经济舱 500 元、商务舱 1200 元、余座各 120/30。

---

## service_search.py — Task4-2 机票搜索

**函数：`search_tickets(...) -> list[dict]`**

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `source_city` | str | 是 | 出发城市，需与数据库 `airport.city` 完全匹配 |
| `dest_city` | str | 是 | 到达城市 |
| `flight_date` | `datetime.date` | 是 | 出行日期 |
| `airline_name` | str | 否 | 航空公司名称，支持模糊匹配 |
| `depart_from` | `datetime.time` | 否 | 出发时间下限 |
| `depart_to` | `datetime.time` | 否 | 出发时间上限 |
| `arrive_from` | `datetime.time` | 否 | 到达时间下限（仅对当日到达航班有效） |
| `arrive_to` | `datetime.time` | 否 | 到达时间上限（仅对当日到达航班有效） |

**返回值：** 按 `economy_price` 升序排列的航班列表，每条包含：
`instance_id`, `flight_no`, `airline_name`, `source_airport`, `destination_airport`, `departure_time`, `arrival_time`, `arrival_day_offset`, `flight_date`, `economy_price`, `economy_remain`, `business_price`, `business_remain`

**注意：** 到达时间过滤只对 `arrival_day_offset=0`（当日到达）的航班生效，跨日航班不参与到达时间筛选。

---

## service_booking.py — Task4-3/4 订票与订单管理

### `get_instance_detail(instance_id) -> dict | None`

查看航班实例的价格和余座，仅用于展示，不加锁。

### `book_ticket(passenger_id, instance_id, cabin_class) -> dict`

执行完整订票事务。

| 参数 | 类型 | 说明 |
|---|---|---|
| `passenger_id` | int | 乘客 ID，必须在 passenger 表中存在 |
| `instance_id` | int | 航班实例 ID，来自搜索结果 |
| `cabin_class` | str | `"economy"` 或 `"business"` |

**返回值：** `{"order_id": int, "price": float, "cabin_class": str}`

**异常：**
- `ValueError`：舱位非法 / 余座为 0 / instance 不存在
- `RuntimeError`：乘客 ID 不存在

**事务保证：** 使用 `SELECT ... FOR UPDATE` 锁定航班实例行，防止并发超卖。失败自动回滚。

### `list_orders(passenger_id) -> list[dict]`

查询指定乘客的所有订单，按下单时间倒序排列。

### `cancel_order(order_id, passenger_id) -> bool`

取消订单并归还余座。`passenger_id` 用于鉴权，只能取消自己的订单。

返回 `True` 表示成功，`False` 表示订单不存在或无权限。

---

## main.py — CLI 主入口

**运行方式：**
```cmd
cd D:\cs307\cs307-project1\task4
python main.py
```

**前置条件：** 需设置数据库连接环境变量：
```cmd
set DATABASE_URL=postgresql://postgres:密码@localhost:5432/project1
```

**菜单功能：**

| 选项 | 功能 | 对应 Task |
|---|---|---|
| 1 | 按日期范围批量生成机票 | Task4-1 |
| 2 | 搜索机票（多条件过滤） | Task4-2 |
| 3 | 订票（含价格确认） | Task4-3 |
| 4 | 查看订单 / 取消订单 | Task4-4 |
| 0 | 退出 | — |

---

## test_task4.py — 自动化测试套件

**运行方式：**
```cmd
cd D:\cs307\cs307-project1\task4
python test_task4.py
```

**测试用例列表：**

| 用例 | 验证内容 |
|---|---|
| DB连接 | 数据库可正常连接 |
| Schema完整性 | 8 张表全部存在 |
| 基础数据已导入 | 各表行数 > 0 |
| 生成机票-正常 | 批量插入成功，重复跳过 |
| 生成机票-日期非法 | start > end 时抛出 ValueError |
| 搜索机票-基础 | 返回结果按 economy_price 升序 |
| 搜索机票-无结果 | 不存在城市返回空列表 |
| 搜索机票-时间过滤 | 时间参数正确传递 |
| 订票+取消-完整流程 | 余座正确扣减和恢复 |
| 订票-非法舱位 | 抛出 ValueError |
| 订票-乘客不存在 | 抛出 RuntimeError |
| 取消-鉴权 | 不能取消他人订单 |
