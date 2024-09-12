from io import StringIO, TextIOWrapper
from schema import Schema
from db import Database
from utils import py_name
from dbscripter import DbScripter
from typing import Any
from procedure import ProcParameter
from systype import TypeCode

class PyScripter:
    def __init__(self, db: Database) -> None:
        self.db = db
    
    def script_all_routines(self):
        model = self.db.model
        for schema in model.schemas:
            print(self.script_schema_routines(schema))

    def script_schema_tables(self, schema: Schema, fs: TextIOWrapper):
        model = self.db.model

        def writeline(s: str = None): fs.write(f"{s or ''}\n")
        def write(s: str = None): fs.write(f"{s or ''}")

        # writeline("from dbexec import DbExec")
        # writeline("import pyodbc")
        # writeline("from typing import Any")
        # writeline("from datetime import datetime, date, time")
        # writeline("from uuid import UUID")
        writeline()
        writeline(f"class {py_name(model.db_name.title())}{py_name(schema.name.title())}Tables:")

        tables = [x for x in model.get_tables() if x.schema_id == schema.schema_id]
        for table in tables:
            writeline(f"    class {py_name(table.name)}:")
            writeline(f"        def __str__(self): return '{table.name}'")
            writeline(f"        def __repr__(self): return '{repr(table)}'")
            writeline()
            
            for col in table.columns:
                writeline(f"        {py_name(col.name)} = '{col.name}'")
            writeline()

    def script_schema_routines(self, schema: Schema, fs: TextIOWrapper):
        model = self.db.model

        def writeline(s: str = None):
            fs.write(f"{s or ''}\n")
        def write(s: str = None):
            fs.write(f"{s or ''}")
        
        writeline("from dbexec import DbExec, Result")
        writeline("import pyodbc")
        writeline("from typing import Any")
        writeline("from datetime import datetime, date, time")
        writeline("from uuid import UUID")
        writeline()
        writeline(f"class {py_name(model.db_name.title())}{py_name(schema.name.title())}Routines:")
        writeline("    def __init__(self, cnstr):")
        writeline("        cn = pyodbc.connect(cnstr)")
        writeline("        self.dbx = DbExec(cn)")
        writeline()

        procs = [x for x in model.get_procedures() if x.schema_id == schema.schema_id]
        dbs = DbScripter(self.db)
        for proc in procs:
            pnames = []
            write(f"    def {py_name(proc.name)}(self")
            for i,param in enumerate(proc.params):
                pname = py_name(param.name)
                pnames.append(pname)
                write(", ")
                write(f"{pname}: {self._py_param_def(param)}")
            writeline(") -> Result:")
            writeline("        sql = \"\"\"")
            writeline(dbs.get_exec_proc_sql(proc).strip())
            writeline("\"\"\"")
            writeline(f"        return self.dbx.get_result(sql, [ {", ".join(pnames)} ])")
            writeline()

    def _py_param_def(self, param: ProcParameter):
        st = param.sys_type
        if st.is_string(): return "str"
        if st.is_natural_number(): return "int"
        if st.is_fixed_number() or st.is_floating_number(): return "float"
        if st.code in [ TypeCode.DATETIME, TypeCode.DATETIME2, TypeCode.DATETIMEOFFSET ]: return "datetime"
        if st.code == TypeCode.DATE: return "date"
        if st.code == TypeCode.TIME: return "time"
        if st.code == TypeCode.UNIQUEIDENTIFIER: return "UUID"
        if st.is_binary(): return "bytes"
        return "object"


if __name__ == "__main__":
    import myconfig
    # cnstr = myconfig.connectionString
    cnstr = myconfig.cyber_cnstr
    db = Database(cnstr=cnstr)
    schema = db.model.schemas.get_by_name("[dbo]")
    scripter = PyScripter(db)
    with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\routines.py', mode='w') as fs:
        s = scripter.script_schema_routines(schema, fs)

    with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\tables.py', mode='w') as fs:
        scripter.script_schema_tables(schema, fs)
    print('#done')
