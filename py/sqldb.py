from typing import Any
import pyodbc
from pyodbc import Row, Connection, Cursor

class SqlDb:
    def __init__(self, cnstr: str) -> None:
        self.cnstr = cnstr
        self.connection = pyodbc.connect(cnstr)

    def execute_fetchall(self, sql: str, *params: Any) -> list[Row]:
        with self.connection.cursor() as cur:
            return cur.execute(sql, params).fetchall()
    
    