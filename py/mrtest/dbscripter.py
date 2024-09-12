from io import StringIO
from .dbmodel import DbModel
from .procedure import Procedure
from .table import Table, Column
from .utils import quote, sql_name

class DbScripter:
    def __init__(self, model: DbModel) -> None:
        self.model = model

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
        if p_out:
            sql.write("SELECT @_return_value [@_return_value]")
            for i,p in enumerate(p_out):
                sql.write(f", {p.name} {quote(p.name + "_out")}")
            sql.write(";\n")

        sql.seek(0)
        return sql.read()

    def get_table_insert_sql(self, table: Table) -> str:
        schema = table.schema
        sql = StringIO()

        sql.write(f"INSERT INTO {repr(table)} (")
        values = ""
        for i,col in enumerate(table.columns):
            if col.is_identity: continue

            if len(values) == 0:
                sql.write(quote(col.name))
                values += "?"
            else:
                sql.write(f", {quote(col.name)}")
                values += ", ?"
        sql.write(f") VALUES ({values});")
        if any([x.is_identity for x in table.columns]):
            sql.write("\n\nSELECT SCOPE_IDENTITY() as [scope_identity];")
        sql.seek(0)
        return sql.read()
    
    def get_table_update_sql(self, table: Table) -> str:
        schema = table.schema
        sql = StringIO()

        declare = "DECLARE\n"
        assign = ""
        where = ""
        for i,col in enumerate([x for x in table.columns if not x.is_pk_column]):
            declare += "    "
            assign += "    "
            where += "    "

            if i != 0:
                declare += ","
                assign += ","
                where += ","

TODO CREATE UPDATE STATEMENT
            declare += "@" + sql_name(col.name)
            assign += f"{quote(col.name)} = ?\n"
            where += f"{quote(col.name)} = ?\n"

        sql.write(f"UPDATE {repr(table)} SET \n{assign} WHERE\n{where}\n;")

        sql.seek(0)
        return sql.read()


if __name__ == '__main__':
    # import myconfig
    # db = Database(myconfig.connectionString)
    # scripter = DbScripter(db)
    # strm = StringIO()
    # scripter.script_procs(strm=strm)
    # print(strm.getvalue())
    print('done')