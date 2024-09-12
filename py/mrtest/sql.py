
SELECT_DB_NAME = "SELECT DB_NAME() as name"

SELECT_SYS_SCHEMAS = """
select
    s.schema_id
    ,s.name
from sys.schemas s
where exists (
    select *
    from sys.objects o
    where s.schema_id = o.schema_id
        and o.is_ms_shipped = 0)
"""

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

SELECT_SYS_TABLES = """
select
    name
    ,object_id
    ,schema_id
    --,*
from sys.tables;
"""

SELECT_SYS_COLUMNS = """
with pk_cols as (
    select 
        pk.object_id, ixc.column_id
    from sys.indexes pk
        join sys.index_columns ixc on pk.object_id = ixc.object_id and pk.index_id = ixc.index_id
    where pk.is_primary_key = 1
)
select
    c.object_id
    ,c.name
    ,c.column_id
    ,c.system_type_id
    ,c.user_type_id
    ,c.max_length
    ,c.precision
    ,c.scale
    ,c.is_nullable
    ,c.is_identity
    ,c.is_computed
    ,c.is_rowguidcol
    ,iif(pk_cols.column_id is null, 0, 1) as is_pk_column
    --,*
from sys.columns c
    join sys.objects o on c.object_id = o.object_id and o.is_ms_shipped = 0
    left join pk_cols on o.object_id = pk_cols.object_id and c.column_id = pk_cols.column_id
order by o.name, c.column_id
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

SELECT_OBJECT_BY_NAME_AND_SCHEMA = """
select o.name
from sys.all_objects o
    join sys.schemas s on o.schema_id = s.schema_id
where s.name = ? and o.name = ?
"""

MOCK_POSTFIX = "_hidden_781EC10A"

SELECT_ALL_MOCKED_OBJECTS = f"""
select
    s.name schema_name
    ,o.name object_name
    ,o.type
from sys.all_objects o
    join sys.schemas s on o.schema_id = s.schema_id
where o.name like '%´{MOCK_POSTFIX}'
"""
