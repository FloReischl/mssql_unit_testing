from pyodbc import Connection
from io import StringIO

def quote(name: str):
    return "[" + name.replace("]", "]]") + "]"

def py_name(name: str):
    name = name.replace('[', '').replace("@", "")
    chars = []

    for c in name:
        if c >= 'a' and c <= 'z': chars.append(c)
        elif c >= 'A' and c <= 'Z': chars.append(c)
        elif c.isdigit(): chars.append(c)
        else:
            chars.append("_")
        
    return "".join(chars)


# def db_exec_fetch_all()