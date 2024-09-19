from typing import Iterable


def single(iter: Iterable, /):
    """
    Returns true, if exact one item in the giben iterable; otherwise false.
    """
    matched = False
    for i,x in enumerate(iter):
        if not matched and x: matched = True
        elif matched and x: return False
    return matched
