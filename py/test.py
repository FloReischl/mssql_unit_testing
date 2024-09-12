from generated.routines import Dev_Cyber_DatapoolDboRoutines
import myconfig
from uuid import uuid4
from dbmock import DbMock

cnstr = myconfig.cyber_cnstr
routines = Dev_Cyber_DatapoolDboRoutines(cnstr=cnstr)
# result = routines.usp_merge_Dim_City(uuid4())


print('#done')
