from typing import Iterable
from .systype import TypeCode, SysType
from sqlalchemy import create_engine, Connection
from pytest import FixtureRequest

_alpha_chars = { 
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        }

def quote(name: str):
    """
    quotes a given object name.

    Parameters
    ----------
    name        : str
                  the name to be quoted.

    Returns
    -------
    str: The quoted name.
    """
    return "[" + name.replace("]", "]]") + "]"

def sql_name(name: str):
    """
    gets a valid SQL name, that can be used for a variable.

    Parameters
    ----------
    name        : str
                  the name template.
    
    Returns
    -------
    str: The valid SQL name.
    """
    return py_name(name)

def sql_field_decl(st: SysType, max_length: int = None, precision: int = None, scale: int = None) -> str:
    """
    gets the SQL type declaration to be used for parameters or columns.

    Arguments
    ---------
    st          : SysType
                  The system type to create a field declaration for.
    max_length  : int
                  the max length for the field declaration; if any.
    precision   : int
                  the precision
    scale       : int
                  the scale

    Returns
    -------
    str: The SQL field declaration.
    """
    c = st.code
    if c in [ TypeCode.DATETIME2, TypeCode.DATE, TypeCode.TIME ]:
        return f'{st.name}({scale})'
    elif st.is_fixed_number():
        return f'{st.name}({precision}, {scale})'
    elif st.code == TypeCode.IMAGE:
        return 'varbinary(max)'
    elif st.code == TypeCode.TEXT:
        return 'varchar(max)'
    elif st.code == TypeCode.NTEXT:
        return 'nvarchar(max)'
    elif st.is_string() and not st.is_legacy() and max_length == -1:
        return f'{st.name}(max)'
    elif st.is_string() and not st.is_legacy():
        return f'{st.name}({max_length})'
    return st.name

def py_name(name: str):
    """
    gets a valid Python name, that can be used for a variable or parameter.

    Parameters
    ----------
    name        : str
                  the name template.
    
    Returns
    -------
    str: The valid Python name.
    """
    name = name.replace('[', '').replace("@", "")
    
    chars = []

    if not name[0] in _alpha_chars:
        chars.append("_")

    for c in name:
        if c >= 'a' and c <= 'z': chars.append(c)
        elif c >= 'A' and c <= 'Z': chars.append(c)
        elif c.isdigit(): chars.append(c)
        else:
            chars.append("_")
        
    return "".join(chars)

def fixture_connection(request: FixtureRequest, cn_candidate: Connection, url: str):
    """
    Helper function to setup a pytest.fixture that creates, reuses and closes a single connection instance

    Paramters
    ---------
    request         : FixtureRequest
                      The fixture request from conftest.py, to register tear-down
    cn_candidate    : Connection
                      A candidate connection that might already be available and reusable
    url             : str
                      The sqlalchemy connection url to create a new connection

    Returns
    -------
    (Connection): an active connection
    """
    from .dbmock import DbMock

    cn = cn_candidate

    def tear_down():
        nonlocal cn
        cn.close()

    if cn == None or cn.closed:
        cn = create_engine(url).connect()

        mock = DbMock(cn)
        mock.reset_database()
        cn.commit()

        request.addfinalizer(tear_down)

    return cn

def fixture_mock(request: FixtureRequest, cn: Connection):
    """
    Helper function to setup a pytest.fixture that creates a (DbMock)

    Paramters
    ---------
    request         : FixtureRequest
                      The fixture request from conftest.py, to register tear-down
    cn              : Connection
                      A connection to be used for the mock

    Returns
    -------
    (Connection): an active connection
    """
    from .dbmock import DbMock

    result = DbMock(cn)
    def tear_down():
        result.restore_all_tables()
        cn.commit()
    request.addfinalizer(tear_down)

    return result
