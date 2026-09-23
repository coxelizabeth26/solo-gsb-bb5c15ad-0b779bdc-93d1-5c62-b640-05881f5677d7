#!/bin/sh
set -e

# 容器启动时确保依赖已安装（已满足时为快速 no-op）
pip install --no-cache-dir -r requirements.txt

# 显式初始化 SQLite 数据库与全部表结构（幂等：已存在时为 no-op），
# 在绑定端口前完成建表；应用启动钩子中也会再执行一次 init_db，双保险。
python -c "from app import db; db.init_db(); print('SQLite initialized at', db.DB_PATH)"

# 启动服务；应用启动钩子会自动创建 SQLite 数据库与表结构
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
