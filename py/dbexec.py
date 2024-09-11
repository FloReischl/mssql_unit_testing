from pyodbc import Connection, Row, Cursor
from typing import Any
import pandas as pd

class Result(object):
    def __init__(self, cn: Connection, sql: str, params: Any) -> None:
        with cn.execute(sql, params) as cur:
            self.all_sets = list[list[Row]]()
            
            self.rows = cur.fetchall()
            self.all_sets.append(self.rows)

            while (cur.nextset()):
                self.all_sets.append(cur.fetchall())

class DbExec:
    def __init__(self, cn: Connection) -> None:
        self.cn = cn

    def execute_fetchall(self, sql: str, *params: Any) -> list[Row]:
        with self.cn.cursor() as cur:
            return cur.execute(sql, params).fetchall()

    def exec_result(self, sql: str, params: Any) -> Result:
        return Result(self.cn, sql, params)

    def exec_cur(self, sql: str, params: Any) -> Cursor:
        return self.cn.execute(sql, params)