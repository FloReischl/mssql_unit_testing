from dbitem import DbItem, DbItems
from systype import SysType, TypeCode
from pyodbc import Row
from utils import quote
from schema import Schema
# from io import StringIO

class ProcParameter(DbItem):
    def __init__(self, row: Row, systypes: DbItems[SysType]):
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
        st = self.sys_type
        c = st.code

        if c in [ TypeCode.DATETIME2, TypeCode.DATE, TypeCode.TIME ]:
            t = f'{st.name}({self.scale})'
        elif st.is_fixed_number():
            t = f'{st.name}({self.precision}, {self.scale})'
        elif st.is_string() and not st.is_legacy() and self.max_length == -1:
            t = f'{st.name}(max)'
        elif st.is_string() and not st.is_legacy():
            t = f'{st.name}({self.max_length})'
        else:
            t = st.name

        return f'{self.name} {t}'

class Procedure(DbItem):
    def __init__(self, row: Row, schemas: DbItems, params: DbItems) -> None:
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
