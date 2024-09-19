from io import TextIOWrapper
from mrtest import Schema
from mrtest import py_name, quotename
from dbscripter import DbScripter
from typing import Any
from mrtest import TypeCode, SysType
from mrtest import DbModel

class PyScripter:
    def __init__(self, model: DbModel) -> None:
        self.model = model

    def script_schema_procedures(self, class_name: str, schema: Schema, fs: TextIOWrapper, ignore: set = None):
        ignore = ignore or set()
        # ignonre SSMS diagram designer procedures
        ignore.add('sp_helpdiagramdefinition')
        ignore.add('sp_creatediagram')
        ignore.add('sp_renamediagram')
        ignore.add('sp_alterdiagram')
        ignore.add('sp_dropdiagram')

        model = self.model

        def writeline(s: str = None):
            fs.write(f"{s or ''}\n")
        def write(s: str = None):
            fs.write(f"{s or ''}")
        
        writeline("from mrtest import DbCmd, Session")
        writeline("from sqlalchemy import Connection, text, create_engine")
        writeline("from typing import Any")
        writeline("from datetime import datetime, date, time")
        writeline("from uuid import UUID")
        writeline()
        writeline(f"class {class_name}:")
        # writeline("    def __init__(self, cnOrUrl: (Connection | str)):")
        # writeline("        self.cn = cnOrUrl if isinstance(cnOrUrl, Connection) else create_engine(str(cnOrUrl)).connect()")
        writeline("    def __init__(self, cn: (Connection | str)):")
        writeline("        if isinstance(cn, Connection): self.cn = cn")
        writeline("        elif isinstance(cn, Session): self.cn = cn.cn")
        writeline("        else: create_engine(str(cn)).connect()")
        writeline()

        procs = [x for x in model.get_procedures() if x.schema_id == schema.schema_id]
        procs.sort(key=lambda x: repr(x))
        dbs = DbScripter(self.model)
        for proc in procs:
            if proc.name in ignore or proc.unique_name in ignore:
                continue
            pnames = []
            write(f"    def {py_name(proc.name)}(self")
            for i,param in enumerate(proc.params):
                pname = py_name(param.name)
                pnames.append("'" + pname + "': " + pname)
                write(", ")
                write(f"{pname}: {self._py_type_def(param.sys_type)}")
            writeline(") -> DbCmd:")
            writeline("        sql = \"\"\"")
            writeline(dbs.get_exec_proc_sql(proc).strip())
            writeline("\"\"\"")
            writeline(f"        return DbCmd(self.cn, sql, {{ {", ".join(pnames)} }})")
            writeline()

    def _py_type_def(self, st: SysType):
        if st.is_string(): return "str"
        if st.is_natural_number(): return "int"
        if st.is_fixed_number() or st.is_floating_number(): return "float"
        if st.code in [ TypeCode.DATETIME, TypeCode.DATETIME2, TypeCode.DATETIMEOFFSET ]: return "datetime"
        if st.code == TypeCode.DATE: return "date"
        if st.code == TypeCode.TIME: return "time"
        if st.code == TypeCode.UNIQUEIDENTIFIER: return "UUID"
        if st.is_binary(): return "bytes"
        return "object"


