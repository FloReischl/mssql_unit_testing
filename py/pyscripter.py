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
    
    def script_all_routines(self):
        for schema in self.model.schemas:
            class_name = f"{py_name(self.model.db_name.title())}{py_name(schema.name.title())}Routines"
            print(self.script_schema_procedures(class_name, schema))

    def script_schema_tables(self, class_name: str, schema: Schema, fs: TextIOWrapper):
        model = self.model

        def writeline(s: str = None): fs.write(f"{s or ''}\n")
        def write(s: str = None): fs.write(f"{s or ''}")

        writeline("from mrtest import DbCmd")
        writeline("from pyodbc import Connection, BinaryNull")
        writeline("from datetime import datetime, date, time")
        writeline("from uuid import UUID")
        writeline("from pandas import DataFrame")
        writeline()
        writeline(f"class {class_name}:")

        dbs = DbScripter(model=model)
        tables = [x for x in model.get_tables() if x.schema_id == schema.schema_id]
        for table in tables:
            s_name = table.schema.name
            t_name = py_name(table.name)
            writeline(f"    class {py_name(table.name)}:")

            writeline("        # table")
            writeline(f"        TableName = '{table.name}'")
            writeline(f"        SchemaName = '{s_name}'")
            writeline(f"        QualifiedName = '{quotename(s_name)}.{quotename(t_name)}'")
            writeline("        # columns")            
            for col in table.columns:
                writeline(f"        {py_name(col.name)} = '{col.name}'")
            writeline()

            writeline("        def __init__(self, cnOrStr: (Connection | str)):")
            writeline("            self.cnOrStr = cnOrStr")
            writeline()

            # crud
            p_insert = "self"
            p_update = "self"
            p_delete = "self"
            p_insert_nullable = ""
            p_update_nullable = ""
            p_insert_names = []
            p_update_names = []
            p_delete_names = []

            for col in table.columns:
                p_name = py_name(col.name)
                type_def = self._py_type_def(col.sys_type)
                if col.is_pk_column:
                    p_update += f", {p_name}: {type_def}"
                    p_delete += f", {p_name}: {type_def}"
                    p_delete_names.append(p_name)
                    if not col.is_identity:
                        p_insert += f", {p_name}: {type_def}"
                        p_insert_names.append(p_name)
                else:
                    if col.is_nullable:
                        nv = 'None' if not col.sys_type.is_binary() else 'BinaryNull'
                        p_insert_nullable += f", {p_name}: {type_def} = {nv}"
                        p_insert_names.append(p_name)
                        p_update_nullable += f", {p_name}: {type_def} = {nv}"
                    elif not col.is_identity:
                        p_insert += f", {p_name}: {type_def}"
                        p_insert_names.append(p_name)
                        p_update += f", {p_name}: {type_def}"
                p_update_names.append(p_name)

            writeline(f"        def insert({p_insert}{p_insert_nullable}) -> DataFrame:")
            writeline(f"            sql = \"\"\"\n{dbs.get_table_insert_sql(table)}\n\"\"\"")
            writeline(f"            return DbCmd(self.cnOrStr, sql, [ {", ".join(p_insert_names)} ]).exec_df()")
            writeline()
            writeline(f"        def update({p_update}{p_update_nullable}) -> DataFrame:")
            writeline(f"            sql = \"\"\"\n{dbs.get_table_update_sql(table)}\n\"\"\"")
            writeline(f"            return DbCmd(self.cnOrStr, sql, [ {", ".join(p_update_names)} ]).exec_df()")
            writeline()
            writeline(f"        def delete({p_delete}) -> DataFrame:")
            writeline(f"            sql = \"\"\"\n{dbs.get_table_delete_sql(table)}\n\"\"\"")
            writeline(f"            return DbCmd(self.cnOrStr, sql, [ {", ".join(p_delete_names)} ]).exec_df()")
            writeline()

    def script_schema_procedures(self, class_name: str, schema: Schema, fs: TextIOWrapper, ignore: set = None):
        ignore = ignore or set()
        model = self.model

        def writeline(s: str = None):
            fs.write(f"{s or ''}\n")
        def write(s: str = None):
            fs.write(f"{s or ''}")
        
        writeline("from mrtest import DbCmd")
        writeline("from pyodbc import Connection")
        writeline("from typing import Any")
        writeline("from datetime import datetime, date, time")
        writeline("from uuid import UUID")
        writeline()
        writeline(f"class {class_name}:")
        writeline("    def __init__(self, cnOrStr: (Connection | str)):")
        writeline("        self.cnOrStr = cnOrStr")
        writeline()
        writeline("    def custom_sql(self, sql: str, params: Any = None):")
        writeline("        return DbCmd(self.cnOrStr, sql, params)")
        writeline()

        procs = [x for x in model.get_procedures() if x.schema_id == schema.schema_id]
        dbs = DbScripter(self.model)
        for proc in procs:
            if proc.name in ignore or proc.unique_name in ignore:
                continue
            pnames = []
            write(f"    def {py_name(proc.name)}(self")
            for i,param in enumerate(proc.params):
                pname = py_name(param.name)
                pnames.append(pname)
                write(", ")
                write(f"{pname}: {self._py_type_def(param.sys_type)}")
            writeline(") -> DbCmd:")
            writeline("        sql = \"\"\"")
            writeline(dbs.get_exec_proc_sql(proc).strip())
            writeline("\"\"\"")
            writeline(f"        return DbCmd(self.cnOrStr, sql, [ {", ".join(pnames)} ])")
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


