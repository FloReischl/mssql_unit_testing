from dbitem import DbItem, DbItems
from pyodbc import Row
from enum import Enum

class TypeCode(Enum):
    IMAGE = 34
    TEXT = 35
    UNIQUEIDENTIFIER = 36
    DATE = 40
    TIME = 41
    DATETIME2 = 42
    DATETIMEOFFSET = 43
    TINYINT = 48
    SMALLINT = 52
    INT = 56
    SMALLDATETIME = 58
    REAL = 59
    MONEY = 60
    DATETIME = 61
    FLOAT = 62
    SQL_VARIANT = 98
    NTEXT = 99
    BIT = 104
    DECIMAL = 106
    NUMERIC = 108
    SMALLMONEY = 122
    BIGINT = 127
    HIERARCHYID = 128
    GEOMETRY = 129
    GEOGRAPHY = 130
    VARBINARY = 165
    VARCHAR = 167
    BINARY = 173
    CHAR = 175
    TIMESTAMP = 189
    NVARCHAR = 231
    NCHAR = 239
    XML = 241
    SYSNAME = 256
    TABLE_TYPE = 243

class SysType(DbItem):
    def __init__(self, info: Row) -> None:
        super().__init__()
        self.name: str = info.name
        self.system_type_id: int = info.system_type_id
        self.user_type_id: int = info.user_type_id
        self.schema_id: int = info.schema_id
        self.max_length: int = info.max_length
        self.precision: int = info.precision
        self.scale: int = info.scale
        self.is_nullable: bool = info.is_nullable == 1
        self.is_user_defined: bool = info.is_user_defined == 1
        self.is_table_type: bool = info.is_table_type == 1
        self.code = TypeCode.TABLE_TYPE if self.is_table_type else [x for x in list(TypeCode) if x.value == self.user_type_id][0]

        self.base_type: SysType = None
        self.id = self.user_type_id
        self.unique_name = self.name

    def get_base_type(self):
        return self.base_type or self
    
    def post_init(self, all: DbItems[DbItem]):
        # self.base_type = self if not self.is_user_defined or self.is_table_type else all.get_by_id(self.system_type_id)
        self.base_type = all.try_get_by_id(self.system_type_id) or self
        self.base_code = self.base_type.code

    def is_legacy(self) -> bool:
        return self.code in [ TypeCode.DATETIME, TypeCode.IMAGE, TypeCode.TEXT, TypeCode.NTEXT ]

    def is_string(self) -> bool:
        return self.base_code in [ TypeCode.CHAR, TypeCode.NCHAR, TypeCode.VARCHAR, TypeCode.NVARCHAR, TypeCode.TEXT, TypeCode.NTEXT ]
    
    def is_binary(self) -> bool:
        return self.base_code in [ TypeCode.BINARY, TypeCode.VARBINARY, TypeCode.IMAGE ]

    def is_number(self):
        return self.is_natural_number() or self.is_fixed_number() or self.is_floating_number()

    def is_natural_number(self) -> bool:
        return self.base_code in [ TypeCode.BIT, TypeCode.TINYINT, TypeCode.SMALLINT, TypeCode.INT, TypeCode.BIGINT ]
    
    def is_fixed_number(self) -> bool:
        return self.base_code in [ TypeCode.DECIMAL, TypeCode.NUMERIC, TypeCode.MONEY, TypeCode.SMALLMONEY ]
    
    def is_floating_number(self) -> bool:
        return self.base_code in [ TypeCode.REAL, TypeCode.FLOAT ]
    
    def is_date(self) -> bool:
        return self.base_code in [ TypeCode.DATE, TypeCode.DATETIME, TypeCode.DATETIME2, TypeCode.DATETIMEOFFSET, TypeCode.TIME ]
