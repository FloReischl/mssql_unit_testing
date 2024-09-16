import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text



engine = create_engine("mssql+pyodbc://sqlaircyber2.munichre.com:1433/dev_sandbox?driver=ODBC+Driver+17+for+SQL+Server")
with engine.connect() as cn:
    with cn.execute(text("execute usp_get_city @city_id = :city_id"), { "city_id": 666 }) as result:
        cn.close()
        df = pd.DataFrame(result)
        result.close()
        print(df)
        pass

# from generated.routines import Dev_Cyber_DatapoolDboRoutines

# import myconfig
# from mrtest import DbMock
# from uuid import uuid4

# cnstr = myconfig.cyber_cnstr

# routines = Dev_Cyber_DatapoolDboRoutines(cnstr=cnstr)
# result = routines.usp_merge_Dim_City(uuid4())

# print('#done')
