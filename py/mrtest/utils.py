alpha_chars = { 
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        }

def quote(name: str):
    return "[" + name.replace("]", "]]") + "]"

def py_name(name: str):
    name = name.replace('[', '').replace("@", "")
    
    chars = []

    if not name[0] in alpha_chars:
        chars.append("_")

    for c in name:
        if c >= 'a' and c <= 'z': chars.append(c)
        elif c >= 'A' and c <= 'Z': chars.append(c)
        elif c.isdigit(): chars.append(c)
        else:
            chars.append("_")
        
    return "".join(chars)


# def db_exec_fetch_all()