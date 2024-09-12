from .dbitem import DbItem, DbItems
from .systype import SysType, TypeCode
from pyodbc import Row
from .utils import quote
from .schema import Schema

class Column(DbItem):
    def __init__(self, row: Row):
        self.object_id = row.object_id
        self.name = row.name
        self.column_id = row.column_id
        self.system_type_id = row.system_type_id
        self.user_type_id = row.user_type_id
        self.max_length = row.max_length
        self.precision = row.precision
        self.scale = row.scale
        self.is_nullable = row.is_nullable == 1
        self.is_identity: bool = row.is_identity == 1
        self.is_computed: bool = row.is_computed == 1
        self.is_rowguidcol: bool = row.is_rowguidcol == 1

        self.unique_name = quote(self.name)
        self.id = self.column_id
        super().__init__()

class Table(DbItem):
    def __init__(self, row: Row, schemas: DbItems, columns: DbItems):
        self.name: str = row.name
        self.object_id: int = row.object_id
        self.schema_id: int = row.schema_id

        self.columns: DbItems[Column] = columns
        self.id = self.object_id
        self.schema: Schema = schemas.get_by_id(self.schema_id)
        self.unique_name = repr(self)
        super().__init__()
    
    def __repr__(self) -> str:
        return f"{repr(self.schema)}.{quote(self.name)}"