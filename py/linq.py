# from typing import Iterable


# def first(collection: Iterable):
#     return next(iter(collection))

# def first_or_none(collection: Iterable):
#     return next(iter(collection), None)

# def _has_single(collection):
#     result = False
#     for i,x in enumerate(collection):
#         result = True
#         if i == 1: return False
#     return result

# def single(collection):
#     if _has_single(collection):
#         return next(iter(collection))
#     else:
#         raise Exception(f"Collection contains {len(collection)} items.")

# def single_or_none(collection):
#     if _has_single(collection):
#         return next(iter(collection))
#     else:
#         return None



