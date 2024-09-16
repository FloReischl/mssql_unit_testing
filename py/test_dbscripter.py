import pytest
from mrtest import DbModel
from pyscripter import PyScripter
from pyodbc import Connection
from sqlalchemy import create_engine
import myconfig

root = "C:\\dev\\src\\mssql_unit_testing\\py\\generated\\"

class TestDbScripter:
    # def test_alchemy_cyber_dbo_procedures(self, cdp_connection: Connection):
    #     model = DbModel(cdp_connection)
    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
    #     # with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_routines.py', mode='w') as fs:
    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_alchemy_procedures.py', mode='w') as fs:
    #         scripter.script_alchemy_schema_procedures("CyberDataPoolDboAlchemyProcedures", schema, fs, ignore=ignore)

    # # script cyber data pool procs
    # def test_cyber_dbo_procedures(self, cdp_connection: Connection):
    #     model = DbModel(cdp_connection)
    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
    #     # with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_routines.py', mode='w') as fs:
    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_procedures.py', mode='w') as fs:
    #         scripter.script_schema_procedures("CyberDataPoolDboProcedures", schema, fs, ignore=ignore)

    # # script cyber tables
    # def test_cyber_dbo_tables(self, cdp_connection: Connection):
    #     model = DbModel(cdp_connection)
    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_tables.py', mode='w') as fs:
    #         scripter.script_schema_tables("CyberDataPoolDboTables", schema, fs)

    def test_alchemy_example_procedures(self, examples_sa_con: Connection):
        model = DbModel(examples_sa_con)
        schema = model.schemas.get_by_name("[examples]")
        scripter = PyScripter(model)

        ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
        with open(root + 'examples_alchemy_procedures.py', mode='w') as fs:
            scripter.script_alchemy_schema_procedures("ExamplesAlchemyProcedures", schema, fs, ignore=ignore)


    # # script cyber data pool procs
    # def test_script_examples_schema_procedures(self, examples_connection: Connection):
    #     model = DbModel(examples_connection)
    #     schema = model.schemas.get_by_name("[examples]")
    #     scripter = PyScripter(model)

    #     ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\examples_procedures.py', mode='w') as fs:
    #         scripter.script_schema_procedures("TstDbExamplesProcedures", schema, fs, ignore=ignore)

    # # script cyber tables
    # def test_script_examples_schema_tables(self, examples_connection: Connection):
    #     model = DbModel(examples_connection)
    #     schema = model.schemas.get_by_name("[examples]")
    #     scripter = PyScripter(model)

    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\examples_tables.py', mode='w') as fs:
    #         scripter.script_schema_tables("TstDbExamplesTables", schema, fs)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_script_sandbox_mock_procedures'
                 ])
