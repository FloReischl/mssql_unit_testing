import pytest
from mrtest import DbModel
from pyscripter import PyScripter
from pyodbc import Connection
import mrtest.myconfig as myconfig

class TestDbScripter:
    # # script sandbox procs
    # def test_script_sandbox_schema_routines(self, sandbox_connection):
    #     model = DbModel(sandbox_connection)

    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
    #     with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\sandbox_routines.py', mode='w') as fs:
    #         scripter.script_schema_routines("SandboxDboRoutines", schema, fs, ignore=ignore)

    # # script mock procedures
    # def test_script_sandbox_mock_procedures(self, examples_connection):
    #     model = DbModel(examples_connection)

    #     schema = model.schemas.get_by_name("[mrtest]")
    #     scripter = PyScripter(model)

    #     with open('C:\\dev\\src\\mssql_unit_testing\\py\\mrtest\\dbmockprocs.py', mode='w') as fs:
    #         scripter.script_schema_routines("MockProcedures", schema, fs)

    # # script cyber data pool procs
    # def test_script_schema_routines(self, model: DbModel):
    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
    #     # with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_routines.py', mode='w') as fs:
    #     with open('C:\\dev\\src\\mssql_unit_testing\\py\\generated\\cyberdatapool_routines.py', mode='w') as fs:
    #         scripter.script_schema_routines("CyberDataPoolDboRoutines", schema, fs, ignore=ignore)

    # # script cyber tables
    # def test_script_schema_tables(self, model: DbModel):
    #     schema = model.schemas.get_by_name("[dbo]")
    #     scripter = PyScripter(model)

    #     with open('C:\\dev\\src\\mssql_unit_testing\\py\\generated\\cyberdatapool_tables.py', mode='w') as fs:
    #         scripter.script_schema_tables("CyberDataPoolDboTables", schema, fs)

    # script cyber data pool procs
    def test_script_examples_schema_routines(self, examples_connection: Connection):
        model = DbModel(examples_connection)
        schema = model.schemas.get_by_name("[examples]")
        scripter = PyScripter(model)

        ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
        with open('C:\\dev\\src\\mssql_unit_testing\\py\\generated\\examples_routines.py', mode='w') as fs:
            scripter.script_schema_routines("TstDbExamplesRoutines", schema, fs, ignore=ignore)

    # script cyber tables
    def test_script_examples_schema_tables(self, examples_connection: Connection):
        model = DbModel(examples_connection)
        schema = model.schemas.get_by_name("[examples]")
        scripter = PyScripter(model)

        with open('C:\\dev\\src\\mssql_unit_testing\\py\\generated\\examples_tables.py', mode='w') as fs:
            scripter.script_schema_tables("TstDbExamplesTables", schema, fs)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_script_sandbox_mock_procedures'
                 ])
