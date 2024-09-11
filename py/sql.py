

SELECT_SYS_TYPES = """
select
    t.name
    ,t.system_type_id
    ,t.user_type_id
    ,t.schema_id
    ,t.max_length
    ,t.[precision]
    ,t.scale
    ,t.is_nullable
    ,t.user_type_id
    ,t.is_user_defined
    ,t.is_table_type
from sys.types t;
"""

SELECT_SYS_PROCEDURES = """
SELECT
    p.name
    ,p.object_id
    ,p.schema_id
    ,p.[type]
from sys.procedures p;
"""

SELECT_SYS_PARAMETERS = """
SELECT
    p.object_id
    ,p.name
    ,p.parameter_id
    ,p.system_type_id
    ,p.user_type_id
    ,p.max_length
    ,p.[precision]
    ,p.scale
    ,p.is_output
    ,p.has_default_value
    -- ,p.default_value
    ,p.is_readonly
    ,p.is_nullable
from sys.parameters p
order by p.object_id, p.parameter_id;
"""
