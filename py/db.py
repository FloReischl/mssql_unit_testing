from dbexec import DbExec
import myconfig
from model import Model
from typing import Any
import pyodbc

class Database:
    def __init__(self, cnstr: str) -> None:
        self.cnstr = cnstr
        self.connection = pyodbc.connect(cnstr)
        self.dbx: DbExec = DbExec(self.connection)
        self.model: Model = Model(self.dbx)

if __name__ == '__main__':
    from dbscripter import DbScripter
    db = Database(myconfig.connectionString)
    procs = db.model.get_procedures()
    scripter = DbScripter(db)
    for proc in procs:
        print(scripter.get_exec_proc_sql(proc))

    # proc =  [x for x in procs if x.name == "usp_multi_result_and_out_param"][0]
    proc = procs.get_by_name("[examples].[usp_multi_result_and_out_param]")
    sql = scripter.get_exec_proc_sql(proc)
    
    result = db.dbx.exec_result(sql, ( 'ALFKI', None ))
    for set in result.all_sets:
        print(set[0].cursor_description)
        print(set)
        print('\n\n')

    print('done')
