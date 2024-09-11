from dbexec import DbExec
import sql
import myconfig
from dbmodel import DbModel
from typing import Any
import pyodbc

class SqlDb:
    def __init__(self, cnstr: str) -> None:
        self.cnstr = cnstr
        self.connection = pyodbc.connect(cnstr)
        self.dbx = DbExec(self.connection)
        self.model = DbModel(self.dbx)

if __name__ == '__main__':
    db = SqlDb(myconfig.connectionString)
    for p in db.model.get_procedures():
        print(p.name)
        for pp in p.params:
            # print(f'   {pp.name} {pp.sys_type.name}')
            print(f'   {pp.__repr__()}')
