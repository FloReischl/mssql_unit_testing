
foo = [ 'bar' ]

print(foo[0])
print(next(iter(foo)))

foo.clear()
print(next(iter(foo)))


# from generated.routines import Dev_Cyber_DatapoolDboRoutines

# import myconfig
# from mrtest import DbMock
# from uuid import uuid4

# cnstr = myconfig.cyber_cnstr

# routines = Dev_Cyber_DatapoolDboRoutines(cnstr=cnstr)
# result = routines.usp_merge_Dim_City(uuid4())

# print('#done')
