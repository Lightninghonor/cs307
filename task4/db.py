"""
数据库连接管理模块
使用环境变量 DATABASE_URL 或默认连接参数
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# 默认连接参数（可通过环境变量覆盖）
DEFAULT_DSN = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "project1"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "options": "-c search_path=flightdb,public",
}

_conn = None


def get_conn():
    """获取全局数据库连接（懒加载，断线自动重连）"""
    global _conn
    url = os.getenv("DATABASE_URL")
    try:
        if _conn is None or _conn.closed:
            if url:
                _conn = psycopg2.connect(url, options="-c search_path=flightdb,public")
            else:
                _conn = psycopg2.connect(**DEFAULT_DSN)
            _conn.autocommit = False
    except psycopg2.OperationalError as e:
        raise ConnectionError(f"无法连接数据库: {e}") from e
    return _conn


def cursor(dict_cursor=True):
    """返回游标，dict_cursor=True 时结果为字典列表"""
    factory = RealDictCursor if dict_cursor else None
    return get_conn().cursor(cursor_factory=factory)


def commit():
    get_conn().commit()


def rollback():
    get_conn().rollback()


def close():
    global _conn
    if _conn and not _conn.closed:
        _conn.close()
    _conn = None
