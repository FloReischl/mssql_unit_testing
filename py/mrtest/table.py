from .dbitem import DbItem, DbItems
from .systype import SysType, TypeCode
from pyodbc import Row
from .utils import quote, sql_field_decl
from .schema import Schema

class Column(DbItem):
    """
    Represents a table column.

    Attributes
    ----------
    object_id       : int
                      id of the parent table.
    name            : str
                      name of the column.
    column_id       : int
                      index of the column.
    system_type_id  : int
                      id of the sytem type.
    user_type_id    : int
                      id of the user type.
    max_length      : int
                      max length of the parameter (string or binary)
    precision       : int
                      precision of the parameter (numeric or date)
    scale           : int
                      scale (numeric)
    is_nullable     : bool
                      column is nullable
    is_identity     : bool
                      column is the identity value
    is_computed     : bool
                      gets if the column is computed.
    is_rowguidcol   : bool
                      gets if the column is the rowguidcol
    is_pk_column    : bool
                      gets if the column is part of hte primary key.
    sys_type        : Systype
                      gets the columns sys.type
    """
    def __init__(self, row: Row, systypes: DbItems[SysType]):
        # super().__init__(row, systypes)
        super().__init__()
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
        self.is_pk_column: bool = row.is_pk_column == 1

        self.sys_type = systypes.get_by_id(self.user_type_id)
        self.unique_name = quote(self.name)
        self.id = self.column_id
    
    def get_sql_field_decl(self):
        return sql_field_decl(self.sys_type, self.max_length, self.precision, self.scale)

class Table(DbItem):
    """
    Represents a database table. Access via (DbModel.get_taples).
    
    Attributes
    ----------
    object_id       : int
                      the object-id
    name            : str
                      the name of the table.
    columns         : list[Column]
                      gets the table columns.
    id              : int
                      the object-id
    schema          : Schema
                      gets the parent schema
    """
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