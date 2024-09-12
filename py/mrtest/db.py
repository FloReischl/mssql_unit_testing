# from .dbexec import DbExec
# from .model import Model
# from typing import Any
# import pyodbc

# class Database:
#     def __init__(self, cnstr: str) -> None:
#         self.cnstr = cnstr
#         self.connection = pyodbc.connect(cnstr)
#         self.dbx: DbExec = DbExec(self.connection)
#         self.model: Model = Model(self.dbx)
    
#     def close(self):
#         self.connection.close()

# if __name__ == '__main__':
#     import myconfig
#     from .dbscripter import DbScripter
#     cnstr = myconfig.connectionString
#     cnstr = myconfig.sandbox_cnstr
#     db = Database(cnstr)
#     procs = db.model.get_procedures()
#     scripter = DbScripter(db)
#     for proc in procs:
#         print(scripter.get_exec_proc_sql(proc))

#     # proc =  [x for x in procs if x.name == "usp_multi_result_and_out_param"][0]
#     proc = procs.get_by_name("[examples].[usp_multi_result_and_out_param]")
#     sql = scripter.get_exec_proc_sql(proc)
    
#     result = db.dbx.get_result(sql, ( 'ALFKI', None ))
#     for df in result.all_df:
#         print(df)
#         print('\n\n')

#     print('done')
