from collections.abc import Iterator, Iterable
from typing import TypeVar, Generic, MutableSequence


class DbItem:
    def __init__(self):
        self.id: int = None
        self.unique_name: str = None

T = TypeVar('T', bound=DbItem)

class DbItems(MutableSequence[T]):
    def __init__(self, items: Iterable = None) -> None:
        if items:
            self.items = list(items)
        else:
            self.items = list()
            
    def append(self, item: T):
        self.items.append(item)

    def __delitem__(self, key):
        self.items.__delitem__(key)
    
    def __getitem__(self, i) -> T:
        return self.items.__getitem__(i)
    
    def __len__(self):
        return self.items.__len__()
    
    def __setitem__(self, key, value: T):
        self.items.__setitem__(key, value)
    
    def insert(self, index, item: T):
        self.items.insert(index, item)

    def __iter__(self) -> Iterator[T]:
        return self.items.__iter__()

    def get_by_id(self, id: int) -> T:
        return [i for i in self.items if i.id == id][0]
    
    def try_get_by_id(self, id: int) -> T:
        candidates = list([i for i in self.items if i.id == id])
        return candidates[0] if len(candidates) == 1 else None
    
    def get_by_name(self, unique_name: str) -> T:
        return [i for i in self.items if i.unique_name == unique_name][0]

