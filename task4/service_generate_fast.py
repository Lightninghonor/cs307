"""
Task4 高级功能1：高效批量生成 flight_instance
策略：
  - 将日期范围按 CPU 核数分片，多线程并行插入
  - 每个线程独立连接，page_size=5000 大批量写入
  - 对比 service_generate.py 的单线程版本，大范围日期时速度提升明显
"""
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
import os
import psycopg2

import db


def _worker(dsn: str, schema: str, routes: list, dates: list[date]) -> dict:
    """单个线程的插入任务，使用独立连接"""
    rows = [
        (r[0], d, float(r[2]), int(r[4]), float(r[1]), int(r[3]))
        for d in dates
        for r in routes
    ]
    if not rows:
        return {"inserted": 0, "skipped": 0}

    conn = psycopg2.connect(dsn, options=f"-c search_path={schema},public")
    conn.autocommit = False
    try:
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
                page_size=5000,
            )
            inserted = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    total = len(rows)
    return {"inserted": inserted, "skipped": total - inserted}


def generate_instances_fast(start_date: date, end_date: date, workers: int = None) -> dict:
    """
    多线程高效生成 flight_instance。

    参数：
      start_date: 开始日期
      end_date:   结束日期
      workers:    并行线程数，默认取 min(CPU核数, 8)

    返回: {"inserted": int, "skipped": int, "workers": int, "total_dates": int}
    """
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")

    if workers is None:
        workers = min(os.cpu_count() or 4, 8)

    # 取所有航线模板价格
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                r.route_id,
                COALESCE(AVG(fi.economy_price),  500.00)  AS economy_price,
                COALESCE(AVG(fi.business_price), 1200.00) AS business_price,
                COALESCE(MAX(fi.economy_remain),  120)    AS economy_remain,
                COALESCE(MAX(fi.business_remain),  30)    AS business_remain
            FROM flight_route r
            LEFT JOIN flight_instance fi ON fi.route_id = r.route_id
            GROUP BY r.route_id
        """)
        routes = cur.fetchall()

    if not routes:
        return {"inserted": 0, "skipped": 0, "workers": 0, "total_dates": 0}

    # 构造日期列表并按 workers 分片
    all_dates = [start_date + timedelta(days=i)
                 for i in range((end_date - start_date).days + 1)]
    chunks = [all_dates[i::workers] for i in range(workers)]
    chunks = [c for c in chunks if c]  # 去掉空分片

    # 获取 DSN 用于子线程独立连接
    url = os.getenv("DATABASE_URL")
    if url:
        dsn = url
    else:
        dsn = (
            f"host={os.getenv('DB_HOST','localhost')} "
            f"port={os.getenv('DB_PORT','5432')} "
            f"dbname={os.getenv('DB_NAME','project1')} "
            f"user={os.getenv('DB_USER','postgres')} "
            f"password={os.getenv('DB_PASSWORD','')}"
        )

    total_inserted = 0
    total_skipped  = 0

    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futures = {
            pool.submit(_worker, dsn, "flightdb", routes, chunk): chunk
            for chunk in chunks
        }
        for fut in as_completed(futures):
            result = fut.result()  # 若线程抛异常会在此重新抛出
            total_inserted += result["inserted"]
            total_skipped  += result["skipped"]

    return {
        "inserted":    total_inserted,
        "skipped":     total_skipped,
        "workers":     len(chunks),
        "total_dates": len(all_dates),
    }
