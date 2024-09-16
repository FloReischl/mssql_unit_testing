from io import StringIO
from mrtest import DbModel, Procedure, Table
from mrtest import quotename, sql_name

class DbScripter:
    def __init__(self, model: DbModel) -> None:
        self.model = model
    
    def get_alchemy_exec_proc_sql(self, proc: Procedure):
        schemas = self.model.schemas
        sql = StringIO()

        sql.write("DECLARE @_return_value INT;\n")
        for i, p in enumerate(proc.params):
            sql.write(f"DECLARE {repr(p)} = :{sql_name(p.name)};\n")
        sql.write("\n")

        schema = schemas.get_by_id(proc.schema_id)
        sql.write(f'EXECUTE @_return_value = {repr(proc)}')
        for i, p in enumerate(proc.params):
            sql.write("\n    ")
            if i != 0: sql.write(",")
            sql.write(f"{p.name} = {p.name}")
            if p.is_output: sql.write(" OUTPUT")
        sql.write(";\n\n")

        p_out = [x for x in proc.params if x.is_output]
        sql.write("SELECT @_return_value [return_value]")
        for i,p in enumerate(p_out):
            sql.write(f", {p.name} {quotename(sql_name(p.name + "_out"))}")
        sql.write(";\n")

        sql.seek(0)
        return sql.read()


    def get_exec_proc_sql(self, proc: Procedure):
        schemas = self.model.schemas
        sql = StringIO()

        sql.write("DECLARE @_return_value INT;\n")
        for i, p in enumerate(proc.params):
            sql.write(f"DECLARE {repr(p)} = ?;\n")
        sql.write("\n")

        schema = schemas.get_by_id(proc.schema_id)
        sql.write(f'EXECUTE @_return_value = {repr(proc)}')
        for i, p in enumerate(proc.params):
            sql.write("\n    ")
            if i != 0: sql.write(",")
            sql.write(f"{p.name} = {p.name}")
            if p.is_output: sql.write(" OUTPUT")
        sql.write(";\n\n")

        p_out = [x for x in proc.params if x.is_output]
        sql.write("SELECT @_return_value [return_value]")
        for i,p in enumerate(p_out):
            sql.write(f", {p.name} {quotename(sql_name(p.name + "_out"))}")
        sql.write(";\n")

        sql.seek(0)
        return sql.read()

    def get_table_insert_sql(self, table: Table) -> str:
        declare = ""
        columns = ""
        values = ""
        for i,col in enumerate(table.columns):
            if col.is_identity: continue

            q_name = quotename(col.name)
            v_name = "@" + sql_name(col.name)

            declare += "\n    ," if len(declare) else "\n    "
            declare += f"{v_name} {col.get_sql_field_decl()} = ?"

            columns += "\n    ," if len(columns) else "\n    "
            columns += q_name

            values += "\n    ," if len(values) else "\n    "
            values += v_name

        select = "\n\nSELECT SCOPE_IDENTITY() [scope_identity];" if any(x.is_identity for x in table.columns) else ""
        sql = f"DECLARE{declare}\n;\n\nINSERT INTO {repr(table)} ({columns}\n)\nVALUES ({values}\n);{select}"
        return sql
    
    def get_table_update_sql(self, table: Table) -> str:
        schema = table.schema
        sql = StringIO()

        declare = ""
        assign = ""
        where = ""
        for i,col in enumerate(table.columns):
            q_name = quotename(col.name)
            v_name = "@" + sql_name(col.name)

            declare += "    ," if len(declare) else "    "
            declare += f"{v_name} {col.get_sql_field_decl()} = ?\n"

            if not col.is_pk_column:
                assign += "    ," if len(assign) else "    "
                assign += f"{q_name} = {v_name}\n"
            else:
                where += "    ," if len(where) else "    "
                where += f"{q_name} = {v_name}\n"

        sql.write(f"DECLARE\n{declare};\n\nUPDATE {repr(table)} SET \n{assign} WHERE\n{where};\n\nSELECT @@ROWCOUNT [rowcount];")

        sql.seek(0)
        return sql.read()

    def get_table_delete_sql(self, table: Table):
        declare = ""
        assign = ""
        # columns = ""
        # values = ""
        for i,col in enumerate(table.columns):
            if not col.is_pk_column: continue

            q_name = quotename(col.name)
            v_name = "@" + sql_name(col.name)

            declare += "\n    ," if len(declare) else "\n    "
            declare += f"{v_name} {col.get_sql_field_decl()} = ?"

            assign += "\n    ," if len(assign) else "\n    "
            assign += f"{q_name} = {v_name}"

        sql = f"DECLARE{declare}\n;\n\nDELETE {repr(table)}\nWHERE{assign}\n;\n\nSELECT @@ROWCOUNT [rowcount];"
        return sql


if __name__ == '__main__':
    # import myconfig
    # db = Database(myconfig.connectionString)
    # scripter = DbScripter(db)
    # strm = StringIO()
    # scripter.script_procs(strm=strm)
    # print(strm.getvalue())
    print('done')