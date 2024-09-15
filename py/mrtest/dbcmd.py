from pyodbc import Connection, Row, Cursor, ProgrammingError, connect as odbc_connect
from pandas import DataFrame
from typing import Iterable

class DbCmd:
    """
    Command to execute SQL statements against a SQL Server
    """
    def __init__(self, cnOrStr: (Connection | str), sql: str, params: Iterable = None) -> None:
        """
        Creates a command instance for a given connection, SQL statement and set of parameters

        Parameters
        ----------
        cnOrStr : (Connection | str)
                  An open connection or connection string. if a connection is provided, 
                  the close must be handled outside of the comand.
        sql     : str
                  The SQL string to be executed against the database.
        params  : Iterable
                  None or the ODBC parameter value to be used to executed the SQL command.
        """
        self.cn = cnOrStr if isinstance(cnOrStr, Connection) else odbc_connect(str(cnOrStr))
        self.sql = sql
        self.params = list(params or [])

    def exec_no_read(self) -> None:
        """
        Executes the SQL command without returning any database response.

        Returns
        -------
        None
        """
        with self.cn.execute(self.sql, self.params):
            pass

    def exec_df(self) -> DataFrame:
        """
        Executes the SQL command and returns the first result set as (DataFrame).

        Returns
        -------
        (DataFram | None):
            The first result set from the command execution.
        """
        with self.cn.execute(self.sql, self.params) as cur:
            return self._df_from_cursor(cur)

    def exec_all_df(self) -> list[DataFrame]:
        """
        Executes the SQL command and returns all result sets as list of (DataFrame).

        Returns
        -------
        list[DataFram]:
            All result sets from the command execution.
        """
        result = list[DataFrame]()
        with self.cn.execute(self.sql, self.params) as cur:
            self._df_from_cursor(cur, result)
            while cur.nextset():
                self._df_from_cursor(cur, result)
        return result

    def exec_rows(self) -> list[Row]:
        """
        Executes the SQL command and returns the first result set as list of (Row).

        Returns
        -------
        (list[Row]):
            The first result set from the command execution.
        """
        with self.cn.execute(self.sql, self.params) as cur:
            return cur.fetchall()

    def exec_cur(self) -> Cursor:
        """
        Executes the SQL command and returns the result (Cursor).

        Returns
        -------
        (list[Row]):
            The Cursor response from the command execution.
        """
        return self.cn.execute(self.sql, self.params)

    def _df_from_cursor(self, cur: Cursor, target: list[DataFrame] = None) -> DataFrame:
        try:
            rows = cur.fetchall()
            cols = list([x[0] for x in cur.description])
            df = DataFrame.from_records(rows, columns=cols)
            if target != None: target.append(df)
            return df
        except ProgrammingError as ex:
            if (len(ex.args) >= 1 and ex.args[0] == 'No results.  Previous SQL was not a query.'):
                return None
            raise
