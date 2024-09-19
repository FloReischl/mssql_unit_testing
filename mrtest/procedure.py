from .dbitem import DbItem, DbItems
from .systype import SysType, TypeCode
from sqlalchemy import Row
from .utils import quote, sql_field_decl
from .schema import Schema

class ProcParameter(DbItem):
    """
    Represents a stored procedure parameter.

    Attributes
    ----------
    object_id       : int
                      The object-id
    name            : str
                      The name of the parameter.
    parameter_id    : int
                      index of the parameter.
    system_type_id  : int
                      id of the sytem type
    user_type_id    : int
                      id of the user type
    max_length      : int
                      max length of the parameter (string or binary)
    precision       : int
                      precision of the parameter (numeric or date)
    scale           : int
                      scale (numeric)
    is_output       : bool
                      parameter is output parameter
    has_default     : bool
                      parameter has default value
    is_readonly     : bool
                      parameter is read-only
    is_nullable     : bool
                      parameter is nullable
    """
    def __init__(self, row: Row, systypes: DbItems[SysType]):
        # super().__init__(row, systypes)
        super().__init__()
        self.object_id = row.object_id
        self.name = row.name
        self.parameter_id = row.parameter_id
        self.system_type_id = row.system_type_id
        self.user_type_id = row.user_type_id
        self.max_length = row.max_length
        self.precision = row.precision
        self.scale = row.scale
        self.is_output = row.is_output == 1
        self.has_default_value = row.has_default_value == 1
        # TODO: self.default_value
        self.is_readonly = row.is_readonly == 1
        self.is_nullable = row.is_nullable == 1

        self.sys_type = systypes.get_by_id(self.user_type_id)
        self.unique_name = self.name
        self.id = self.parameter_id

    def __repr__(self) -> str:
        t = sql_field_decl(self.sys_type, self.max_length, self.precision, self.scale)
        return f'{self.name} {t}'

class Procedure(DbItem):
    """
    Represents a stored procedure.
    """
    def __init__(self, row: Row, schemas: DbItems, params: DbItems) -> None:
        """
        Internal constructor. Use (DbModel.get_procedures) to access stored procedures.

        Attributes
        ----------
        name          : str
                        name of the procedure
        object_id     : int
                        object-id
        schema_id     : int
                        schema-id
        type          : str
                        object type-name
        schema        : Schema
                        parent schema.
        params        : list[ProcParameter]
                        list of the procedures parameters.
        id            : int
                        object-id.
        unique_name   : str
                        unique name for the procedure.
        """
        super().__init__()
        self.name = row.name
        self.object_id: int = row.object_id
        self.schema_id: int = row.schema_id
        self.type: str = row.type

        self.schema: Schema = schemas.get_by_id(self.schema_id)
        self.params = list[ProcParameter](params)
        self.id = self.object_id
        self.unique_name = self.__repr__()

    def __repr__(self) -> str:
        return f"{repr(self.schema)}.{quote(self.name)}"
