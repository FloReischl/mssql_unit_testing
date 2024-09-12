from schema import Schema
import sql
from dbitem import DbItems
from systype import SysType
from procedure import Procedure, ProcParameter
from table import Table, Column
from typing import Any
from pyodbc import Row
from dbexec import DbExec

class Model:
    def __init__(self, dbx: DbExec) -> None:
        self.dbx = dbx
        self.db_name: str = None
        self._load_db_name()
        self.systypes = DbItems[SysType]()
        self._load_systypes()
        self.schemas = DbItems[Schema]()
        self._load_schemas()

    def get_procedures(self) -> DbItems[Procedure]:
        params = list[ProcParameter]()
        for row in self.dbx.get_rows(sql.SELECT_SYS_PARAMETERS):
            params.append(ProcParameter(row, self.systypes))

        result = DbItems[Procedure]()
        for row in self.dbx.get_rows(sql.SELECT_SYS_PROCEDURES):
            pp = DbItems([x for x in params if x.object_id == row.object_id])
            result.append(Procedure(row, self.schemas, pp))
        return result
    
    def get_tables(self) -> DbItems[Table]:
        all_cols = list[Column]()
        for row in self.dbx.get_rows(sql.SELECT_SYS_COLUMNS):
            all_cols.append(Column(row))
        
        result = DbItems[Table]()
        for row in self.dbx.get_rows(sql.SELECT_SYS_TABLES):
            object_id = row.object_id
            cols = DbItems[Column]([x for x in all_cols if x.object_id == object_id])
            result.append(Table(row, self.schemas, cols))
        return result

    def _load_db_name(self):
        row = self.dbx.get_rows(sql.SELECT_DB_NAME)[0]
        self.db_name = row.name

    def _load_systypes(self):
        for row in self.dbx.get_df(sql.SELECT_SYS_TYPES).iterrows():
            self.systypes.append(SysType(row[1]))
        
        for st in self.systypes:
            st.post_init(self.systypes)

    def _load_schemas(self):
        for row in self.dbx.get_rows(sql.SELECT_SYS_SCHEMAS):
            self.schemas.append(Schema(row))
