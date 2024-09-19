import pytest
from mrtest import DbModel
from pyscripter import PyScripter
from sqlalchemy import create_engine, Connection
import myconfig

root = "C:\\dev\\src\\mssql_unit_testing\\py\\generated\\"

class TestDbScripter:
    def test_alchemy_cyber_dbo_procedures(self, connection: Connection):
        model = DbModel(connection)
        schema = model.schemas.get_by_name("[dbo]")
        scripter = PyScripter(model)

        with open('C:\\dev\\src\\my\\mssql_unit_testing\\py\\generated\\cyberdatapool_procedures.py', mode='w') as fs:
            scripter.script_schema_procedures("CyberDataPoolDboProcedures", schema, fs)

    # def test_alchemy_example_procedures(self, examples_sa_con: Connection):
    #     model = DbModel(examples_sa_con)
    #     schema = model.schemas.get_by_name("[examples]")
    #     scripter = PyScripter(model)

    #     with open(root + 'examples_procedures.py', mode='w') as fs:
    #         scripter.script_schema_procedures("ExamplesProcedures", schema, fs)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_script_sandbox_mock_procedures'
                 ])
