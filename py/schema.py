from dbitem import DbItem, DbItems
from pyodbc import Row
from utils import quote

class Schema(DbItem):
    def __init__(self, info: Row) -> None:
        super().__init__()
        self.name: str = info.name
        self.schema_id: int = info.schema_id

        self.unique_name = self.__repr__();
        self.id = self.schema_id

    def __repr__(self) -> str:
        return quote(self.name)
    
    