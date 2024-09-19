from .dbitem import DbItem
from sqlalchemy import Row
from .utils import quote

class Schema(DbItem):
    """
    Represents a database schema. Use (DbModel.schemas) to access schemas.
    """
    def __init__(self, info: Row) -> None:
        """
        Internal constructor. Use (DbModel.schemas) to access schemas.
        """
        super().__init__()
        self.name: str = info.name
        self.schema_id: int = info.schema_id

        self.unique_name = self.__repr__();
        self.id = self.schema_id

    def __repr__(self) -> str:
        return quote(self.name)
    
    