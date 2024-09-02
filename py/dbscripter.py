from io import StringIO
from typing import Any
from sqldb import SqlDb
from pyodbc import Row

#region sql
class Sql:
#region procedures_and_params
    procedures_and_params = """
select
    SCHEMA_NAME(o.schema_id)
    ,o.name
    ,p.name
    ,p.parameter_id
    ,t.name
    ,p.[precision]
    ,p.scale
    ,p.is_output
    ,p.is_cursor_ref
    ,p.has_default_value
    ,p.default_value
    ,p.is_readonly
    ,p.is_nullable
    -- ,p.*
    -- ,t.*
from sys.objects o
    left join sys.parameters p 
        JOIN sys.types t on p.system_type_id = t.system_type_id
    on o.object_id = p.object_id
where [type] = 'P'
order by
    SCHEMA_NAME(o.schema_id)
    ,o.name
    ,p.parameter_id
"""
#endregion

#region
    proc_meta = """
select *
from sys.procedures
where [type] = 'P'
order by name
"""
#endregion

#region
    proc_params_meta = """
select
    p.name
   ,p.parameter_id
   ,p.system_type_id
   ,p.user_type_id
   ,p.max_length
   ,p.precision
   ,p.scale
   ,p.is_output
   --,p.is_cursor_ref
   --,p.default_value
   ,p.is_readonly
   ,p.is_nullable
from sys.parameters p
where p.object_id = ?
order by p.parameter_id;
"""
#endregion

#endregion

def get_py_name(sql_name: str):
    count = len(sql_name)
    result = ''
    for i in range(count):
        c = sql_name[i]
        if c >= 'a' and c <= 'z': result += c
        elif c >= 'A' and c <= 'Z': result += c
        else: result += '_'
    return result

class ProcParamMeta:
    def __init__(self, row: Row) -> None:
        self.name: str = row.name
        self.py_name = get_py_name(self.name.replace('@', ''))
        self.parameter_id: int = row.parameter_id
        self.system_type_id: int = row.system_type_id
        self.user_type_id: int = row.user_type_id
        self.max_length: int = row.max_length
        self.precision: int = row.precision
        self.scale: int = row.scale
        self.is_output: bool = row.is_output == 1
        self.is_readonly: bool = row.is_readonly == 1
        self.is_nullable: bool = row.is_nullable

    def __repr__(self) -> str:
        return f'"{self.name}"'

class ProcMeta:
    def __init__(self, row: Row) -> None:
        self.object_id: int = row.object_id
        self.schema_id: int = row.schema_id
        self.name: str = row.name
        self.py_name = get_py_name(self.name)
        self.params: list[ProcParamMeta]

    def __repr__(self) -> str:
        return f'name = {self.name}'

    def load_params(self, db: SqlDb):
        self.params = []
        rows = db.execute_fetchall(Sql.proc_params_meta, self.object_id)
        for row in rows:
            param = ProcParamMeta(row)
            self.params.append(param)

class DbScripter:
    def __init__(self, db: SqlDb) -> None:
        self.db = db
        self.strm : StringIO = None

    def script_procs(self, strm: StringIO):
        self.strm

        strm.write("class StoredProcedures:\n")

        for proc in self.get_procs():
            strm.write(f'    def {proc.py_name}n()')
            strm.write('\n')

    def get_procs(self) -> list[ProcMeta]:
        procs: list[ProcMeta] = []

        rows = self.db.execute_fetchall(Sql.proc_meta)
        for row in rows:
            proc = ProcMeta(row)
            proc.load_params(self.db)
            procs.append(proc)

        return procs

if __name__ == '__main__':
    import myconfig
    db = SqlDb(myconfig.connectionString)
    scripter = DbScripter(db)
    strm = StringIO()
    scripter.script_procs(strm=strm)
    print(strm.getvalue())
    print('done')