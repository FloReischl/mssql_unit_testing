from sqlalchemy import create_engine
from pytest import FixtureRequest, fixture
from mrtest import DbMock, fixture_connection, fixture_mock
import myconfig



# ####################################################################
# # northwind examples

# _examples_cn_url = "mssql+pyodbc://./testdb?driver=ODBC+Driver+17+for+SQL+Server"
# _examples_sa_con = None

# @fixture
# def examples_connection(request: FixtureRequest):
#     global _examples_sa_con, _examples_cn_url

#     close = False
#     if _examples_sa_con == None:
#         engine = create_engine(_examples_cn_url)
#         _examples_sa_con = engine.connect()
#         close = True
#         mock = DbMock(_examples_sa_con)
#         mock.reset_database()

#     def tear_down():
#         global _examples_sa_con
#         if close:
#             _examples_sa_con.close()
#             _examples_sa_con = None
#     request.addfinalizer(tear_down)

#     return _examples_sa_con

# @fixture
# def examples_mock(request: FixtureRequest, examples_connection):
#     result = DbMock(examples_connection)
    
#     def tear_down():
#         result.restore_tables()
#     request.addfinalizer(tear_down)

#     return result

####################################################################
# cyber_datapool

# _cyber_coninfo = ConInfo("sqlaircyber2.munichre.com", "dev_cyber_datapool")
_cyber_cn_url = "mssql+pyodbc://sqlaircyber2.munichre.com/dev_cyber_datapool?driver=ODBC+Driver+17+for+SQL+Server"
_cyber_con = None

@fixture
def connection(request: FixtureRequest):
    global _cyber_con, _cyber_cn_url
    return fixture_connection(request, _cyber_con, _cyber_cn_url)
    # close = False
    # if _cyber_con == None:
    #     engine = create_engine(_cyber_cn_url)
    #     _cyber_con = engine.connect()
    #     close = True
    #     mock = DbMock(_cyber_con)
    #     mock.reset_database()

    # def tear_down():
    #     global _cyber_con
    #     if close:
    #         _cyber_con.close()
    #         _cyber_con = None
    # request.addfinalizer(tear_down)

    # return _cyber_con

@fixture
def mock(request: FixtureRequest, connection):
    return fixture_mock(request, connection)
    # result = DbMock(cdp_connection)
    
    # def tear_down():
    #     result.restore_tables()
    # request.addfinalizer(tear_down)

    # return result

