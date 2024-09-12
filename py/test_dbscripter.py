import pytest
from mrtest import DbModel, DbExec
from mrtest.pyscripter import PyScripter
import mrtest.myconfig as myconfig

class TestDbScripter:
    def test_script_sandbox_schema_routines(self):
        dbx = DbExec(myconfig.sandbox_cnstr)
        model = DbModel(dbx)

        schema = model.schemas.get_by_name("[dbo]")
        scripter = PyScripter(model)

        ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
        with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\sandbox_routines.py', mode='w') as fs:
            s = scripter.script_schema_routines("SandboxDboRoutines", schema, fs, ignore=ignore)

    def test_script_schema_routines(self, model: DbModel):
        schema = model.schemas.get_by_name("[dbo]")
        scripter = PyScripter(model)

        ignore = { 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram' }
        with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_routines.py', mode='w') as fs:
            s = scripter.script_schema_routines("CyberDataPoolDboRoutines", schema, fs, ignore=ignore)

    def test_script_schema_tables(self, model: DbModel):
        schema = model.schemas.get_by_name("[dbo]")
        scripter = PyScripter(model)

        with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_tables.py', mode='w') as fs:
            scripter.script_schema_tables("CyberDataPoolDboTables", schema, fs)

if __name__ == '__main__':
    pytest.main([__file__
                #  , '-k'
                 ])
