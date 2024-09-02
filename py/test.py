from typing import Tuple
import pyodbc
import myconfig


# class Metadata:
#     def __init__(self, cur_desc: Tuple[Tuple]) -> None:
#         self.cur_desc = cur_desc
#         self.col_indexes = {}
#     def get_col_index(self, name: str):
#         if not name in self.col_indexes:
#             for i in range(len(self.cur_desc)):
#                 if name.lower() == str(self.cur_desc[i][0]).lower():
#                     self.col_indexes[name] = i
#                     return i
#             raise Exception(f"invalid column name '{name}'")
#         return self.col_indexes[name]

# class Record(object):
#     def __init__(self, meta: Metadata, row: pyodbc.Row) -> None:
#         self.meta = meta
#         self.row = row

#     def __getattribute__(self, name: str):
#         row:pyodbc.Row = object.__getattribute__(self, 'row')
#         meta: Metadata = object.__getattribute__(self, 'meta')
#         index = meta.get_col_index(name)
#         return 'a'
#     #     # return row[i]


class Helper:
    def __init__(self, cnstr: str, open: bool = True) -> None:
        self.cnstr = cnstr
        self.cn: pyodbc.Connection = None
        if open: self.open()
    
    def open(self):
        assert self.cn == None
        self.cn = pyodbc.connect(self.cnstr)
    
    def buildRoutinesClass(self):
        with self.cn.cursor() as cur:
            routines = cur.execute("SELECT * FROM INFORMATION_SCHEMA.ROUTINES ")

if __name__ == '__main__':
    connectionString = myconfig.connectionString

    # cn = pyodbc.connect(connectionString)
    # cur = cn.cursor()
    # first = cur.execute("select TOP(1) * from customers").fetchone()
    # meta = Metadata(first.cursor_description)
    # i = meta.get_col_index('customerid')
    # rec = Record(meta, first)
    # print (rec.customerid)
    # print (rec.customerid)
    # print('done')
