from sqlalchemy import Row
import pandas as pd
from .session import Session
from .dbcmd import DbCmd
from .dbmock import DbMock

def db_exec_df(sql: str, params) -> pd.DataFrame:
    cn = Session._get_cn()
    return DbCmd(cn, sql, params).exec_df()

def db_exec_single(sql: str, params: dict = None) -> Row:
    cn = Session._get_cn()
    return DbCmd(cn, sql, params).exec_single()

def db_exec_first_or_none(sql: str, params: dict = None) -> Row:
    cn = Session._get_cn()
    return DbCmd(cn, sql, params).exec_first_or_none()

def mock_table(schema_name: str, table_name: str):
    mock = Session._get_mock()
    mock.mock_table(schema_name, table_name)
