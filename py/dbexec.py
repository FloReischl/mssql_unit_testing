from pyodbc import Connection, Row, Cursor, connect as odbc_connect
from typing import Any
import pandas as pd
from pandas import DataFrame

class Result(object):
    def __init__(self, all_df: list[DataFrame]) -> None:
        self.df = next((x for x in all_df), None)
        self.all_df = list[DataFrame](all_df)

class DbExec:
    def __init__(self, cn: (Connection | str)) -> None:
        if isinstance(cn, str):
            self.cn = odbc_connect(cn)
        elif isinstance(cn, Connection):
            self.cn = cn
        else:
            raise Exception("cn parameter must be ODBC Connection or a connection string")

    def get_rows(self, sql: str, *params: Any) -> list[Row]:
        with self.cn.cursor() as cur:
            return cur.execute(sql, params).fetchall()

    def get_df(self, sql: str, params: Any = []) -> DataFrame:
        with self.cn.execute(sql, params) as cur:
            return self._df_from_cursor(cur)

    def get_all_df(self, sql: str, params: Any = []) -> list[DataFrame]:
        result = list[DataFrame]()
        with self.cn.execute(sql, params) as cur:
            result.append(self._df_from_cursor(cur))
            while cur.nextset():
                result.append(self._df_from_cursor(cur))
        return result

    def get_result(self, sql: str, params: Any = []) -> Result:
        all_df = self.get_all_df(sql, params=params)
        return Result(all_df=all_df)

    def _df_from_cursor(self, cur: Cursor) -> DataFrame:
        rows = cur.fetchall()
        cols = list([x[0] for x in cur.description])
        return pd.DataFrame.from_records(rows, columns=cols)

if __name__ == '__main__':
    import myconfig
    cnstr = myconfig.sandbox_cnstr
    with odbc_connect(cnstr) as cn:
        dbx = DbExec(cn)
        # df = dbx.exec_df("SELECT TOP(10) * FROM dbo.Dim_City;")
        result = dbx.get_result("SELECT TOP(10) * FROM dbo.Dim_City;")
        df = result.df
        print('\n\n')
        print(df)
        print('#done')