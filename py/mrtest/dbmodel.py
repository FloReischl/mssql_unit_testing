from .schema import Schema
from .sql import SELECT_DB_NAME \
                 ,SELECT_SYS_COLUMNS \
                 ,SELECT_SYS_PARAMETERS \
                 ,SELECT_SYS_PROCEDURES \
                 ,SELECT_SYS_SCHEMAS \
                 ,SELECT_SYS_TABLES \
                 ,SELECT_SYS_TYPES
from .dbitem import DbItems
from .systype import SysType
from .procedure import Procedure, ProcParameter
from .table import Table, Column
from typing import Any
from .dbcmd import DbCmd
# from pyodbc import Connection
from sqlalchemy import Connection

class DbModel:
    """
    Represents the database model

    Attributes
    ----------
    db_name         : str
                      Name of the database.
    systypes        : list[Systype]
                      List of all discovered data types (sys.types).
    schemas         : list[Schema]
                      List of all database schemas holding user objects.
    """
    def __init__(self, cn: Connection) -> None:
        self.cn = cn
        self.db_name: str = None
        self._load_db_name()
        self.systypes = DbItems[SysType]()
        self._load_systypes()
        self.schemas = DbItems[Schema]()
        self._load_schemas()

    def get_procedures(self) -> DbItems[Procedure]:
        """
        Gets the list of all stored procedures.
        """
        params = list[ProcParameter]()
        for row in DbCmd(self.cn, SELECT_SYS_PARAMETERS).exec_rows():
            params.append(ProcParameter(row, self.systypes))

        result = DbItems[Procedure]()
        for row in DbCmd(self.cn, SELECT_SYS_PROCEDURES).exec_rows():
            pp = DbItems([x for x in params if x.object_id == row.object_id])
            result.append(Procedure(row, self.schemas, pp))
        return result
    
    def get_tables(self) -> DbItems[Table]:
        """
        Gets a list of all tables.
        """
        all_cols = list[Column]()
        for row in DbCmd(self.cn, SELECT_SYS_COLUMNS).exec_rows():
            all_cols.append(Column(row, self.systypes))
        
        result = DbItems[Table]()
        for row in DbCmd(self.cn, SELECT_SYS_TABLES).exec_rows():
            object_id = row.object_id
            cols = DbItems[Column]([x for x in all_cols if x.object_id == object_id])
            result.append(Table(row, self.schemas, cols))
        return result

    def _load_db_name(self):
        row = DbCmd(self.cn, SELECT_DB_NAME).exec_rows()[0]
        self.db_name = row.name

    def _load_systypes(self):
        for i,row in DbCmd(self.cn, SELECT_SYS_TYPES).exec_df().iterrows():
            self.systypes.append(SysType(row))
        
        for st in self.systypes:
            st._post_init(self.systypes)

    def _load_schemas(self):
        for row in DbCmd(self.cn, SELECT_SYS_SCHEMAS).exec_rows():
            self.schemas.append(Schema(row))
