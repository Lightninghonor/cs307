# cs307

## Python 导入脚本

这个项目现在有一个可直接用的 PostgreSQL 导入脚本：`import_csv_to_postgres.py`。

先安装依赖：

```bash
pip install -r requirements.txt
```

然后把你的数据库连接信息放到环境变量里，最省事的是直接设置 `DATABASE_URL`：

```powershell
$env:DATABASE_URL = "postgresql://用户名:密码@127.0.0.1:5432/数据库名"
```

如果你不用 `DATABASE_URL`，也可以分别设置 `PGHOST`、`PGPORT`、`PGDATABASE`、`PGUSER`、`PGPASSWORD`。

运行导入：

```bash
py import_csv_to_postgres.py --schema flightdb --reset
```

如果你的 schema 名称不是 `flightdb`，就把 `--schema` 改成你的名字。脚本会自动兼容两版表结构：`passenger.name` 和 `passenger.first_name/last_name` 都能处理，`flight_instance.flight_date` 和 `flight_instance.flight_data` 也都能识别。