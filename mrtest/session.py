from sqlalchemy import Connection
from .dbmock import DbMock

class Session:
    _s_cn: Connection = None
    _s_mock: DbMock = None

    def __init__(self) -> None:
        self.cn = Session._s_cn
        self.mock = Session._s_mock

    def _get_cn() -> Connection:
        existing = Session._s_cn
        assert existing and not existing.closed
        return existing
    
    def _try_set(cn: Connection, mock: DbMock=None):
        existing = Session._s_cn
        if not existing or existing.closed:
            Session._s_cn = cn
            Session._s_mock = mock
        elif not Session._s_mock:
            Session._s_mock = mock

    def _get_mock() -> DbMock:
        existing = Session._s_mock
        if not existing or existing.cn.closed:
            existing = DbMock(Session._get_cn())
            Session._s_mock = existing
        return existing
