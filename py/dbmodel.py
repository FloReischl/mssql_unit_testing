import sql
from dbitem import DbItems
from systype import SysType
from procedure import Procedure, ProcParameter
from typing import Any
from pyodbc import Row
from dbexec import DbExec


class DbModel:
    def __init__(self, dbx: DbExec) -> None:
        self.dbx = dbx
        self.systypes = DbItems[SysType]()
        self._load_systypes()

    def _load_systypes(self):
        for row in self.dbx.execute_fetchall(sql.SELECT_SYS_TYPES):
            self.systypes.append(SysType(row))
        
        for st in self.systypes:
            st.post_init(self.systypes)

    def get_procedures(self) -> DbItems[Procedure]:
        params = list[ProcParameter]()
        for row in self.dbx.execute_fetchall(sql.SELECT_SYS_PARAMETERS):
            params.append(ProcParameter(row, self.systypes))

        result = DbItems[Procedure]()
        for row in self.dbx.execute_fetchall(sql.SELECT_SYS_PROCEDURES):
            pp = DbItems([x for x in params if x.object_id == row.object_id])
            result.append(Procedure(row, pp))
        return result
