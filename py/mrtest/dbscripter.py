from io import StringIO
from .dbmodel import DbModel
from .procedure import Procedure
from .utils import quote

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


if __name__ == '__main__':
    # import myconfig
    # db = Database(myconfig.connectionString)
    # scripter = DbScripter(db)
    # strm = StringIO()
    # scripter.script_procs(strm=strm)
    # print(strm.getvalue())
    print('done')