"""
Task4-2: 乘客搜索票务
必填：出发城市、到达城市、日期
可选：航空公司名称、出发时间范围、到达时间范围（含跨日处理）
"""
from datetime import date, time
from typing import Optional
import db


def search_tickets(
    source_city: str,
    dest_city: str,
    flight_date: date,
    airline_name: Optional[str] = None,
    depart_from: Optional[time] = None,
    depart_to: Optional[time] = None,
    arrive_from: Optional[time] = None,
    arrive_to: Optional[time] = None,
) -> list[dict]:
    """
    搜索符合条件的 flight_instance，按 economy_price 升序返回。

    到达时间过滤需考虑 arrival_day_offset：
      - 若 arrive_to 指定，则只筛选 arrival_day_offset=0 的航班（当日到达）
      - 若需要筛选次日到达，调用方可不传 arrive_to 或自行处理
    """
    sql = """
        SELECT
            fi.instance_id,
            r.flight_no,
            al.name                         AS airline_name,
            ap_src.name                     AS source_airport,
            ap_src.city                     AS source_city,
            ap_dst.name                     AS destination_airport,
            ap_dst.city                     AS destination_city,
            r.departure_time,
            r.arrival_time,
            r.arrival_day_offset,
            fi.flight_date,
            fi.economy_price,
            fi.economy_remain,
            fi.business_price,
            fi.business_remain
        FROM flight_instance fi
        JOIN flight_route r       ON r.route_id          = fi.route_id
        JOIN airline al           ON al.airline_id        = r.airline_id
        JOIN airport ap_src       ON ap_src.airport_id   = r.source_airport_id
        JOIN airport ap_dst       ON ap_dst.airport_id   = r.destination_airport_id
        WHERE ap_src.city  = %(source_city)s
          AND ap_dst.city  = %(dest_city)s
          AND fi.flight_date = %(flight_date)s
    """
    params: dict = {
        "source_city": source_city.strip(),
        "dest_city": dest_city.strip(),
        "flight_date": flight_date,
    }

    if airline_name:
        sql += " AND al.name ILIKE %(airline_name)s"
        params["airline_name"] = f"%{airline_name.strip()}%"

    if depart_from:
        sql += " AND r.departure_time >= %(depart_from)s"
        params["depart_from"] = depart_from
    if depart_to:
        sql += " AND r.departure_time <= %(depart_to)s"
        params["depart_to"] = depart_to

    # 到达时间过滤：仅对当日到达（offset=0）的航班有意义
    if arrive_from or arrive_to:
        sql += " AND r.arrival_day_offset = 0"
        if arrive_from:
            sql += " AND r.arrival_time >= %(arrive_from)s"
            params["arrive_from"] = arrive_from
        if arrive_to:
            sql += " AND r.arrival_time <= %(arrive_to)s"
            params["arrive_to"] = arrive_to

    sql += " ORDER BY fi.economy_price ASC"

    with db.cursor() as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
