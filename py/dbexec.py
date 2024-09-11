from pyodbc import Connection, Row
from typing import Any

class DbExec:
    def __init__(self, cn: Connection) -> None:
        self.cn = cn

    def execute_fetchall(self, sql: str, *params: Any) -> list[Row]:
        with self.cn.cursor() as cur:
            return cur.execute(sql, params).fetchall()
