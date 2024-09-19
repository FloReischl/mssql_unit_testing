from io import StringIO
from mrtest import DbModel, Procedure
from mrtest import quotename, sql_name

class DbScripter:
    def __init__(self, model: DbModel) -> None:
        self.model = model
    
    def get_exec_proc_sql(self, proc: Procedure):
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

