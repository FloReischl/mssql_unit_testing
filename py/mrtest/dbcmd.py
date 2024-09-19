from sqlalchemy import Connection, CursorResult, create_engine, text, Sequence, Row
from pandas import DataFrame
from typing import Iterable

class DbCmd:
    """
    Command to execute SQL statements against a SQL Server
    """
    def __init__(self, cnOrUrl: (Connection | str), sql: str, params: dict = None) -> None:
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
        self.cn = cnOrUrl if isinstance(cnOrUrl, Connection) else create_engine(str(cnOrUrl)).connect()
        self.sql = sql
        self.params = params or dict()

    def exec_no_read(self) -> None:
        """
        Executes the SQL command without returning any database response.

        Returns
        -------
        None
        """
        with self.cn.execute(text(self.sql), self.params):
            pass

    def exec_df(self) -> DataFrame:
        """
        Executes the SQL command and returns the first result set as (DataFrame).

        Returns
        -------
        (DataFram | None):
            The first result set from the command execution.
        """
        with self.cn.execute(text(self.sql), self.params) as cur_res:
            return self._df_from_cursor(cur_res)

    def exec_all_df(self) -> list[DataFrame]:
        """
        Executes the SQL command and returns all result sets as list of (DataFrame).

        Returns
        -------
        list[DataFram]:
            All result sets from the command execution.
        """
        result = list[DataFrame]()
        with self.cn.execute(text(self.sql), self.params) as cur_res:
            self._df_from_cursor(cur_res, result)
            while cur_res.cursor.nextset():
                self._df_from_cursor(cur_res, result)
        return result

    def exec_rows(self) -> list[Row]:
        """
        Executes the SQL command and returns the first result set as list of (Row).

        Returns
        -------
        (list[Row]):
            The first result set from the command execution.
        """
        with self.cn.execute(text(self.sql), self.params) as cur_res:
            return list[Row](cur_res.fetchall())
    
    def exec_first(self) -> Row:
        """
        Executes the SQL command and returns the first row of the result. If no row was returned, an error is raised.

        Returns
        -------
        The first row of the result.
        """
        return self.exec_rows()[0]
    
    def exec_first_or_none(self) -> Row:
        """
        Executes the SQL command and returns the first row of the result. If no rows, (None) will be returned.

        Returns
        -------
        The first row or None.
        """
        return next(iter(self.exec_rows()), None)
    
    def exec_single(self) -> Row:
        """
        Executes the SQL command and returns the single row that was returned by the database. If no or more than exactly one row was returned, an error is raised.

        Returns
        -------
        The single row of the result.
        """
        rows = self.exec_rows()
        if len(rows) > 1: raise Exception("More than one rows returned from the database.")
        if len(rows) == 0: raise Exception("No row returned from the databae.")
        return rows[0]

    def exec_cur(self) -> CursorResult:
        """
        Executes the SQL command and returns the result (Cursor). The caller needs to handle the (cursor.close) handling.

        Returns
        -------
        (list[Row]):
            The Cursor response from the command execution.
        """
        return self.cn.execute(text(self.sql), self.params)

    def _df_from_cursor(self, cur_res: CursorResult, target: list[DataFrame] = None) -> DataFrame:
        try:
            df = DataFrame(cur_res)
            # rows = cur_res.fetchall()
            # cols = list([x[0] for x in cur_res.description])
            # df = DataFrame.from_records(rows, columns=cols)
            if target != None: target.append(df)
            return df
        except Exception as ex:
            raise
