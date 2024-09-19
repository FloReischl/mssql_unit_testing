import pytest
from mrtest import DbModel
from pyscripter import PyScripter

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #
#   test is used to create the CyberDataPoolDboProcedures proxy class   #
#                  for database procedure execution.                    #
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #

class TestDbScripter:
    def test_generate_cyber_dbo_procedures(self, connection):
        model = DbModel(connection)
        schema = model.schemas.get_by_name("[dbo]")
        scripter = PyScripter(model)

        with open('C:\\dev\\src\\cybase\\cybase-datapool-test\\generated\\cyberdatapool_procedures.py', mode='w') as fs:
            scripter.script_schema_procedures("CyberDataPoolDboProcedures", schema, fs)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_script_sandbox_mock_procedures'
                 ])
