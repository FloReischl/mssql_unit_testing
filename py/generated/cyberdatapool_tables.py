from mrtest import DbCmd
from pyodbc import Connection, BinaryNull
from datetime import datetime, date, time
from uuid import UUID
from pandas import DataFrame

class CyberDataPoolDboTables:
    class Dim_Claim_Status:
        # table
        TableName = 'Dim_Claim_Status'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Claim_Status]'
        # columns
        Claim_Status_ID = 'Claim_Status_ID'
        Claim_Status = 'Claim_Status'
        Claim_Unified_Status_ID = 'Claim_Unified_Status_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Claim_Status: str, Claim_Unified_Status_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_Status nvarchar(100) = ?
    ,@Claim_Unified_Status_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Claim_Status] (
    [Claim_Status]
    ,[Claim_Unified_Status_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Claim_Status
    ,@Claim_Unified_Status_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_Status, Claim_Unified_Status_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Claim_Status_ID: int, Claim_Status: str, Claim_Unified_Status_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_Status_ID bigint = ?
    ,@Claim_Status nvarchar(100) = ?
    ,@Claim_Unified_Status_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Claim_Status] SET 
    [Claim_Status] = @Claim_Status
    ,[Claim_Unified_Status_ID] = @Claim_Unified_Status_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Claim_Status_ID] = @Claim_Status_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_Status_ID, Claim_Status, Claim_Unified_Status_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Claim_Status_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Claim_Status_ID bigint = ?
;

DELETE [dbo].[Dim_Claim_Status]
WHERE
    [Claim_Status_ID] = @Claim_Status_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_Status_ID ]).exec_df()

    class dnb:
        # table
        TableName = 'dnb'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[dnb]'
        # columns
        dnb_id = 'dnb_id'
        name = 'name'
        previous_names = 'previous_names'
        primary_address = 'primary_address'
        primary_postcode = 'primary_postcode'
        primary_city = 'primary_city'
        continental_region = 'continental_region'
        region = 'region'
        country = 'country'
        country_iso2code = 'country_iso2code'
        phone = 'phone'
        email_address = 'email_address'
        website = 'website'
        group_size = 'group_size'
        group_hierarchy_level = 'group_hierarchy_level'
        subsidiary_count = 'subsidiary_count'
        legal_identifiers = 'legal_identifiers'
        legal_identifier_types = 'legal_identifier_types'
        trade_register_number = 'trade_register_number'
        operating_status = 'operating_status'
        operating_status_start_date = 'operating_status_start_date'
        legal_form = 'legal_form'
        legal_form_type = 'legal_form_type'
        company_size = 'company_size'
        company_incorporation_date = 'company_incorporation_date'
        company_start_date = 'company_start_date'
        naics2022_primary_code = 'naics2022_primary_code'
        ussic_primary_code = 'ussic_primary_code'
        employees = 'employees'
        employees_as_of_date = 'employees_as_of_date'
        employees_data_reliability = 'employees_data_reliability'
        revenue_usd = 'revenue_usd'
        revenue_reporting_date = 'revenue_reporting_date'
        revenue_statement_duration_months = 'revenue_statement_duration_months'
        revenue_data_reliability = 'revenue_data_reliability'
        revenue_type = 'revenue_type'
        revenue_original_currency = 'revenue_original_currency'
        revenue_original = 'revenue_original'
        ultimate_parent_company_mr_company_id = 'ultimate_parent_company_mr_company_id'
        ultimate_parent_company_mr_company_name = 'ultimate_parent_company_mr_company_name'
        ultimate_parent_company_country_iso2code = 'ultimate_parent_company_country_iso2code'
        ultimate_parent_company_employees = 'ultimate_parent_company_employees'
        ultimate_parent_company_revenue_usd = 'ultimate_parent_company_revenue_usd'
        ultimate_parent_company_revenue_reporting_date = 'ultimate_parent_company_revenue_reporting_date'
        ultimate_parent_company_revenue_statement_duration_months = 'ultimate_parent_company_revenue_statement_duration_months'
        ultimate_parent_company_revenue_original_currency = 'ultimate_parent_company_revenue_original_currency'
        ultimate_parent_company_revenue_original = 'ultimate_parent_company_revenue_original'
        direct_parent_mr_company_id = 'direct_parent_mr_company_id'
        direct_parent_mr_company_name = 'direct_parent_mr_company_name'
        direct_parent_country_iso2code = 'direct_parent_country_iso2code'
        direct_parent_employees = 'direct_parent_employees'
        direct_parent_revenue_usd = 'direct_parent_revenue_usd'
        direct_parent_revenue_reporting_date = 'direct_parent_revenue_reporting_date'
        direct_parent_revenue_statement_duration_months = 'direct_parent_revenue_statement_duration_months'
        direct_parent_revenue_original_currency = 'direct_parent_revenue_original_currency'
        direct_parent_revenue_original = 'direct_parent_revenue_original'
        aka_names = 'aka_names'
        create_time = 'create_time'
        change_time = 'change_time'
        changed_by = 'changed_by'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, dnb_id: str, name: str = None, previous_names: str = None, primary_address: str = None, primary_postcode: str = None, primary_city: str = None, continental_region: str = None, region: str = None, country: str = None, country_iso2code: str = None, phone: str = None, email_address: str = None, website: str = None, group_size: int = None, group_hierarchy_level: int = None, subsidiary_count: int = None, legal_identifiers: str = None, legal_identifier_types: str = None, trade_register_number: str = None, operating_status: str = None, operating_status_start_date: str = None, legal_form: str = None, legal_form_type: str = None, company_size: str = None, company_incorporation_date: str = None, company_start_date: str = None, naics2022_primary_code: str = None, ussic_primary_code: str = None, employees: int = None, employees_as_of_date: str = None, employees_data_reliability: str = None, revenue_usd: int = None, revenue_reporting_date: str = None, revenue_statement_duration_months: int = None, revenue_data_reliability: str = None, revenue_type: str = None, revenue_original_currency: str = None, revenue_original: int = None, ultimate_parent_company_mr_company_id: str = None, ultimate_parent_company_mr_company_name: str = None, ultimate_parent_company_country_iso2code: str = None, ultimate_parent_company_employees: int = None, ultimate_parent_company_revenue_usd: int = None, ultimate_parent_company_revenue_reporting_date: str = None, ultimate_parent_company_revenue_statement_duration_months: int = None, ultimate_parent_company_revenue_original_currency: str = None, ultimate_parent_company_revenue_original: int = None, direct_parent_mr_company_id: str = None, direct_parent_mr_company_name: str = None, direct_parent_country_iso2code: str = None, direct_parent_employees: int = None, direct_parent_revenue_usd: int = None, direct_parent_revenue_reporting_date: str = None, direct_parent_revenue_statement_duration_months: int = None, direct_parent_revenue_original_currency: str = None, direct_parent_revenue_original: int = None, aka_names: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @dnb_id nvarchar(100) = ?
    ,@name nvarchar(2048) = ?
    ,@previous_names nvarchar(4096) = ?
    ,@primary_address nvarchar(2048) = ?
    ,@primary_postcode nvarchar(100) = ?
    ,@primary_city nvarchar(400) = ?
    ,@continental_region nvarchar(100) = ?
    ,@region nvarchar(400) = ?
    ,@country nvarchar(200) = ?
    ,@country_iso2code nchar(4) = ?
    ,@phone nvarchar(100) = ?
    ,@email_address nvarchar(200) = ?
    ,@website nvarchar(400) = ?
    ,@group_size int = ?
    ,@group_hierarchy_level int = ?
    ,@subsidiary_count int = ?
    ,@legal_identifiers nvarchar(2048) = ?
    ,@legal_identifier_types nvarchar(4096) = ?
    ,@trade_register_number nvarchar(100) = ?
    ,@operating_status nvarchar(40) = ?
    ,@operating_status_start_date nvarchar(100) = ?
    ,@legal_form nvarchar(400) = ?
    ,@legal_form_type nvarchar(400) = ?
    ,@company_size nvarchar(100) = ?
    ,@company_incorporation_date nvarchar(100) = ?
    ,@company_start_date nvarchar(100) = ?
    ,@naics2022_primary_code nvarchar(2048) = ?
    ,@ussic_primary_code nvarchar(2048) = ?
    ,@employees int = ?
    ,@employees_as_of_date nvarchar(100) = ?
    ,@employees_data_reliability nvarchar(100) = ?
    ,@revenue_usd bigint = ?
    ,@revenue_reporting_date nvarchar(100) = ?
    ,@revenue_statement_duration_months int = ?
    ,@revenue_data_reliability nvarchar(100) = ?
    ,@revenue_type nvarchar(100) = ?
    ,@revenue_original_currency nvarchar(6) = ?
    ,@revenue_original bigint = ?
    ,@ultimate_parent_company_mr_company_id nvarchar(100) = ?
    ,@ultimate_parent_company_mr_company_name nvarchar(2048) = ?
    ,@ultimate_parent_company_country_iso2code nchar(4) = ?
    ,@ultimate_parent_company_employees int = ?
    ,@ultimate_parent_company_revenue_usd bigint = ?
    ,@ultimate_parent_company_revenue_reporting_date nvarchar(100) = ?
    ,@ultimate_parent_company_revenue_statement_duration_months int = ?
    ,@ultimate_parent_company_revenue_original_currency nvarchar(6) = ?
    ,@ultimate_parent_company_revenue_original bigint = ?
    ,@direct_parent_mr_company_id nvarchar(100) = ?
    ,@direct_parent_mr_company_name nvarchar(2048) = ?
    ,@direct_parent_country_iso2code nchar(4) = ?
    ,@direct_parent_employees int = ?
    ,@direct_parent_revenue_usd bigint = ?
    ,@direct_parent_revenue_reporting_date nvarchar(100) = ?
    ,@direct_parent_revenue_statement_duration_months int = ?
    ,@direct_parent_revenue_original_currency nvarchar(6) = ?
    ,@direct_parent_revenue_original bigint = ?
    ,@aka_names nvarchar(4096) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

INSERT INTO [dbo].[dnb] (
    [dnb_id]
    ,[name]
    ,[previous_names]
    ,[primary_address]
    ,[primary_postcode]
    ,[primary_city]
    ,[continental_region]
    ,[region]
    ,[country]
    ,[country_iso2code]
    ,[phone]
    ,[email_address]
    ,[website]
    ,[group_size]
    ,[group_hierarchy_level]
    ,[subsidiary_count]
    ,[legal_identifiers]
    ,[legal_identifier_types]
    ,[trade_register_number]
    ,[operating_status]
    ,[operating_status_start_date]
    ,[legal_form]
    ,[legal_form_type]
    ,[company_size]
    ,[company_incorporation_date]
    ,[company_start_date]
    ,[naics2022_primary_code]
    ,[ussic_primary_code]
    ,[employees]
    ,[employees_as_of_date]
    ,[employees_data_reliability]
    ,[revenue_usd]
    ,[revenue_reporting_date]
    ,[revenue_statement_duration_months]
    ,[revenue_data_reliability]
    ,[revenue_type]
    ,[revenue_original_currency]
    ,[revenue_original]
    ,[ultimate_parent_company_mr_company_id]
    ,[ultimate_parent_company_mr_company_name]
    ,[ultimate_parent_company_country_iso2code]
    ,[ultimate_parent_company_employees]
    ,[ultimate_parent_company_revenue_usd]
    ,[ultimate_parent_company_revenue_reporting_date]
    ,[ultimate_parent_company_revenue_statement_duration_months]
    ,[ultimate_parent_company_revenue_original_currency]
    ,[ultimate_parent_company_revenue_original]
    ,[direct_parent_mr_company_id]
    ,[direct_parent_mr_company_name]
    ,[direct_parent_country_iso2code]
    ,[direct_parent_employees]
    ,[direct_parent_revenue_usd]
    ,[direct_parent_revenue_reporting_date]
    ,[direct_parent_revenue_statement_duration_months]
    ,[direct_parent_revenue_original_currency]
    ,[direct_parent_revenue_original]
    ,[aka_names]
    ,[create_time]
    ,[change_time]
    ,[changed_by]
)
VALUES (
    @dnb_id
    ,@name
    ,@previous_names
    ,@primary_address
    ,@primary_postcode
    ,@primary_city
    ,@continental_region
    ,@region
    ,@country
    ,@country_iso2code
    ,@phone
    ,@email_address
    ,@website
    ,@group_size
    ,@group_hierarchy_level
    ,@subsidiary_count
    ,@legal_identifiers
    ,@legal_identifier_types
    ,@trade_register_number
    ,@operating_status
    ,@operating_status_start_date
    ,@legal_form
    ,@legal_form_type
    ,@company_size
    ,@company_incorporation_date
    ,@company_start_date
    ,@naics2022_primary_code
    ,@ussic_primary_code
    ,@employees
    ,@employees_as_of_date
    ,@employees_data_reliability
    ,@revenue_usd
    ,@revenue_reporting_date
    ,@revenue_statement_duration_months
    ,@revenue_data_reliability
    ,@revenue_type
    ,@revenue_original_currency
    ,@revenue_original
    ,@ultimate_parent_company_mr_company_id
    ,@ultimate_parent_company_mr_company_name
    ,@ultimate_parent_company_country_iso2code
    ,@ultimate_parent_company_employees
    ,@ultimate_parent_company_revenue_usd
    ,@ultimate_parent_company_revenue_reporting_date
    ,@ultimate_parent_company_revenue_statement_duration_months
    ,@ultimate_parent_company_revenue_original_currency
    ,@ultimate_parent_company_revenue_original
    ,@direct_parent_mr_company_id
    ,@direct_parent_mr_company_name
    ,@direct_parent_country_iso2code
    ,@direct_parent_employees
    ,@direct_parent_revenue_usd
    ,@direct_parent_revenue_reporting_date
    ,@direct_parent_revenue_statement_duration_months
    ,@direct_parent_revenue_original_currency
    ,@direct_parent_revenue_original
    ,@aka_names
    ,@create_time
    ,@change_time
    ,@changed_by
);
"""
            return DbCmd(self.cnOrStr, sql, [ dnb_id, name, previous_names, primary_address, primary_postcode, primary_city, continental_region, region, country, country_iso2code, phone, email_address, website, group_size, group_hierarchy_level, subsidiary_count, legal_identifiers, legal_identifier_types, trade_register_number, operating_status, operating_status_start_date, legal_form, legal_form_type, company_size, company_incorporation_date, company_start_date, naics2022_primary_code, ussic_primary_code, employees, employees_as_of_date, employees_data_reliability, revenue_usd, revenue_reporting_date, revenue_statement_duration_months, revenue_data_reliability, revenue_type, revenue_original_currency, revenue_original, ultimate_parent_company_mr_company_id, ultimate_parent_company_mr_company_name, ultimate_parent_company_country_iso2code, ultimate_parent_company_employees, ultimate_parent_company_revenue_usd, ultimate_parent_company_revenue_reporting_date, ultimate_parent_company_revenue_statement_duration_months, ultimate_parent_company_revenue_original_currency, ultimate_parent_company_revenue_original, direct_parent_mr_company_id, direct_parent_mr_company_name, direct_parent_country_iso2code, direct_parent_employees, direct_parent_revenue_usd, direct_parent_revenue_reporting_date, direct_parent_revenue_statement_duration_months, direct_parent_revenue_original_currency, direct_parent_revenue_original, aka_names, create_time, change_time, changed_by ]).exec_df()

        def update(self, dnb_id: str, name: str = None, previous_names: str = None, primary_address: str = None, primary_postcode: str = None, primary_city: str = None, continental_region: str = None, region: str = None, country: str = None, country_iso2code: str = None, phone: str = None, email_address: str = None, website: str = None, group_size: int = None, group_hierarchy_level: int = None, subsidiary_count: int = None, legal_identifiers: str = None, legal_identifier_types: str = None, trade_register_number: str = None, operating_status: str = None, operating_status_start_date: str = None, legal_form: str = None, legal_form_type: str = None, company_size: str = None, company_incorporation_date: str = None, company_start_date: str = None, naics2022_primary_code: str = None, ussic_primary_code: str = None, employees: int = None, employees_as_of_date: str = None, employees_data_reliability: str = None, revenue_usd: int = None, revenue_reporting_date: str = None, revenue_statement_duration_months: int = None, revenue_data_reliability: str = None, revenue_type: str = None, revenue_original_currency: str = None, revenue_original: int = None, ultimate_parent_company_mr_company_id: str = None, ultimate_parent_company_mr_company_name: str = None, ultimate_parent_company_country_iso2code: str = None, ultimate_parent_company_employees: int = None, ultimate_parent_company_revenue_usd: int = None, ultimate_parent_company_revenue_reporting_date: str = None, ultimate_parent_company_revenue_statement_duration_months: int = None, ultimate_parent_company_revenue_original_currency: str = None, ultimate_parent_company_revenue_original: int = None, direct_parent_mr_company_id: str = None, direct_parent_mr_company_name: str = None, direct_parent_country_iso2code: str = None, direct_parent_employees: int = None, direct_parent_revenue_usd: int = None, direct_parent_revenue_reporting_date: str = None, direct_parent_revenue_statement_duration_months: int = None, direct_parent_revenue_original_currency: str = None, direct_parent_revenue_original: int = None, aka_names: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @dnb_id nvarchar(100) = ?
    ,@name nvarchar(2048) = ?
    ,@previous_names nvarchar(4096) = ?
    ,@primary_address nvarchar(2048) = ?
    ,@primary_postcode nvarchar(100) = ?
    ,@primary_city nvarchar(400) = ?
    ,@continental_region nvarchar(100) = ?
    ,@region nvarchar(400) = ?
    ,@country nvarchar(200) = ?
    ,@country_iso2code nchar(4) = ?
    ,@phone nvarchar(100) = ?
    ,@email_address nvarchar(200) = ?
    ,@website nvarchar(400) = ?
    ,@group_size int = ?
    ,@group_hierarchy_level int = ?
    ,@subsidiary_count int = ?
    ,@legal_identifiers nvarchar(2048) = ?
    ,@legal_identifier_types nvarchar(4096) = ?
    ,@trade_register_number nvarchar(100) = ?
    ,@operating_status nvarchar(40) = ?
    ,@operating_status_start_date nvarchar(100) = ?
    ,@legal_form nvarchar(400) = ?
    ,@legal_form_type nvarchar(400) = ?
    ,@company_size nvarchar(100) = ?
    ,@company_incorporation_date nvarchar(100) = ?
    ,@company_start_date nvarchar(100) = ?
    ,@naics2022_primary_code nvarchar(2048) = ?
    ,@ussic_primary_code nvarchar(2048) = ?
    ,@employees int = ?
    ,@employees_as_of_date nvarchar(100) = ?
    ,@employees_data_reliability nvarchar(100) = ?
    ,@revenue_usd bigint = ?
    ,@revenue_reporting_date nvarchar(100) = ?
    ,@revenue_statement_duration_months int = ?
    ,@revenue_data_reliability nvarchar(100) = ?
    ,@revenue_type nvarchar(100) = ?
    ,@revenue_original_currency nvarchar(6) = ?
    ,@revenue_original bigint = ?
    ,@ultimate_parent_company_mr_company_id nvarchar(100) = ?
    ,@ultimate_parent_company_mr_company_name nvarchar(2048) = ?
    ,@ultimate_parent_company_country_iso2code nchar(4) = ?
    ,@ultimate_parent_company_employees int = ?
    ,@ultimate_parent_company_revenue_usd bigint = ?
    ,@ultimate_parent_company_revenue_reporting_date nvarchar(100) = ?
    ,@ultimate_parent_company_revenue_statement_duration_months int = ?
    ,@ultimate_parent_company_revenue_original_currency nvarchar(6) = ?
    ,@ultimate_parent_company_revenue_original bigint = ?
    ,@direct_parent_mr_company_id nvarchar(100) = ?
    ,@direct_parent_mr_company_name nvarchar(2048) = ?
    ,@direct_parent_country_iso2code nchar(4) = ?
    ,@direct_parent_employees int = ?
    ,@direct_parent_revenue_usd bigint = ?
    ,@direct_parent_revenue_reporting_date nvarchar(100) = ?
    ,@direct_parent_revenue_statement_duration_months int = ?
    ,@direct_parent_revenue_original_currency nvarchar(6) = ?
    ,@direct_parent_revenue_original bigint = ?
    ,@aka_names nvarchar(4096) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

UPDATE [dbo].[dnb] SET 
    [name] = @name
    ,[previous_names] = @previous_names
    ,[primary_address] = @primary_address
    ,[primary_postcode] = @primary_postcode
    ,[primary_city] = @primary_city
    ,[continental_region] = @continental_region
    ,[region] = @region
    ,[country] = @country
    ,[country_iso2code] = @country_iso2code
    ,[phone] = @phone
    ,[email_address] = @email_address
    ,[website] = @website
    ,[group_size] = @group_size
    ,[group_hierarchy_level] = @group_hierarchy_level
    ,[subsidiary_count] = @subsidiary_count
    ,[legal_identifiers] = @legal_identifiers
    ,[legal_identifier_types] = @legal_identifier_types
    ,[trade_register_number] = @trade_register_number
    ,[operating_status] = @operating_status
    ,[operating_status_start_date] = @operating_status_start_date
    ,[legal_form] = @legal_form
    ,[legal_form_type] = @legal_form_type
    ,[company_size] = @company_size
    ,[company_incorporation_date] = @company_incorporation_date
    ,[company_start_date] = @company_start_date
    ,[naics2022_primary_code] = @naics2022_primary_code
    ,[ussic_primary_code] = @ussic_primary_code
    ,[employees] = @employees
    ,[employees_as_of_date] = @employees_as_of_date
    ,[employees_data_reliability] = @employees_data_reliability
    ,[revenue_usd] = @revenue_usd
    ,[revenue_reporting_date] = @revenue_reporting_date
    ,[revenue_statement_duration_months] = @revenue_statement_duration_months
    ,[revenue_data_reliability] = @revenue_data_reliability
    ,[revenue_type] = @revenue_type
    ,[revenue_original_currency] = @revenue_original_currency
    ,[revenue_original] = @revenue_original
    ,[ultimate_parent_company_mr_company_id] = @ultimate_parent_company_mr_company_id
    ,[ultimate_parent_company_mr_company_name] = @ultimate_parent_company_mr_company_name
    ,[ultimate_parent_company_country_iso2code] = @ultimate_parent_company_country_iso2code
    ,[ultimate_parent_company_employees] = @ultimate_parent_company_employees
    ,[ultimate_parent_company_revenue_usd] = @ultimate_parent_company_revenue_usd
    ,[ultimate_parent_company_revenue_reporting_date] = @ultimate_parent_company_revenue_reporting_date
    ,[ultimate_parent_company_revenue_statement_duration_months] = @ultimate_parent_company_revenue_statement_duration_months
    ,[ultimate_parent_company_revenue_original_currency] = @ultimate_parent_company_revenue_original_currency
    ,[ultimate_parent_company_revenue_original] = @ultimate_parent_company_revenue_original
    ,[direct_parent_mr_company_id] = @direct_parent_mr_company_id
    ,[direct_parent_mr_company_name] = @direct_parent_mr_company_name
    ,[direct_parent_country_iso2code] = @direct_parent_country_iso2code
    ,[direct_parent_employees] = @direct_parent_employees
    ,[direct_parent_revenue_usd] = @direct_parent_revenue_usd
    ,[direct_parent_revenue_reporting_date] = @direct_parent_revenue_reporting_date
    ,[direct_parent_revenue_statement_duration_months] = @direct_parent_revenue_statement_duration_months
    ,[direct_parent_revenue_original_currency] = @direct_parent_revenue_original_currency
    ,[direct_parent_revenue_original] = @direct_parent_revenue_original
    ,[aka_names] = @aka_names
    ,[create_time] = @create_time
    ,[change_time] = @change_time
    ,[changed_by] = @changed_by
 WHERE
    [dnb_id] = @dnb_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ dnb_id, name, previous_names, primary_address, primary_postcode, primary_city, continental_region, region, country, country_iso2code, phone, email_address, website, group_size, group_hierarchy_level, subsidiary_count, legal_identifiers, legal_identifier_types, trade_register_number, operating_status, operating_status_start_date, legal_form, legal_form_type, company_size, company_incorporation_date, company_start_date, naics2022_primary_code, ussic_primary_code, employees, employees_as_of_date, employees_data_reliability, revenue_usd, revenue_reporting_date, revenue_statement_duration_months, revenue_data_reliability, revenue_type, revenue_original_currency, revenue_original, ultimate_parent_company_mr_company_id, ultimate_parent_company_mr_company_name, ultimate_parent_company_country_iso2code, ultimate_parent_company_employees, ultimate_parent_company_revenue_usd, ultimate_parent_company_revenue_reporting_date, ultimate_parent_company_revenue_statement_duration_months, ultimate_parent_company_revenue_original_currency, ultimate_parent_company_revenue_original, direct_parent_mr_company_id, direct_parent_mr_company_name, direct_parent_country_iso2code, direct_parent_employees, direct_parent_revenue_usd, direct_parent_revenue_reporting_date, direct_parent_revenue_statement_duration_months, direct_parent_revenue_original_currency, direct_parent_revenue_original, aka_names, create_time, change_time, changed_by ]).exec_df()

        def delete(self, dnb_id: str) -> DataFrame:
            sql = """
DECLARE
    @dnb_id nvarchar(100) = ?
;

DELETE [dbo].[dnb]
WHERE
    [dnb_id] = @dnb_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ dnb_id ]).exec_df()

    class Dim_BI_Waiting_Period:
        # table
        TableName = 'Dim_BI_Waiting_Period'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_BI_Waiting_Period]'
        # columns
        BI_Waiting_Period_ID = 'BI_Waiting_Period_ID'
        BI_Waiting_Period = 'BI_Waiting_Period'
        Unit = 'Unit'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, BI_Waiting_Period_ID: int, BI_Waiting_Period: str, Unit: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @BI_Waiting_Period_ID bigint = ?
    ,@BI_Waiting_Period nvarchar(100) = ?
    ,@Unit nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_BI_Waiting_Period] (
    [BI_Waiting_Period_ID]
    ,[BI_Waiting_Period]
    ,[Unit]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @BI_Waiting_Period_ID
    ,@BI_Waiting_Period
    ,@Unit
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ BI_Waiting_Period_ID, BI_Waiting_Period, Unit, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, BI_Waiting_Period_ID: int, BI_Waiting_Period: str, Unit: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @BI_Waiting_Period_ID bigint = ?
    ,@BI_Waiting_Period nvarchar(100) = ?
    ,@Unit nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_BI_Waiting_Period] SET 
    [BI_Waiting_Period] = @BI_Waiting_Period
    ,[Unit] = @Unit
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [BI_Waiting_Period_ID] = @BI_Waiting_Period_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ BI_Waiting_Period_ID, BI_Waiting_Period, Unit, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, BI_Waiting_Period_ID: int) -> DataFrame:
            sql = """
DECLARE
    @BI_Waiting_Period_ID bigint = ?
;

DELETE [dbo].[Dim_BI_Waiting_Period]
WHERE
    [BI_Waiting_Period_ID] = @BI_Waiting_Period_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ BI_Waiting_Period_ID ]).exec_df()

    class validation_criticality:
        # table
        TableName = 'validation_criticality'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[validation_criticality]'
        # columns
        Rule_Name = 'Rule_Name'
        Column_Name = 'Column_Name'
        Criticality = 'Criticality'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Rule_Name: str, Column_Name: str, Criticality: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Rule_Name nvarchar(400) = ?
    ,@Column_Name nvarchar(400) = ?
    ,@Criticality int = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[validation_criticality] (
    [Rule_Name]
    ,[Column_Name]
    ,[Criticality]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Rule_Name
    ,@Column_Name
    ,@Criticality
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ Rule_Name, Column_Name, Criticality, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Rule_Name: str, Column_Name: str, Criticality: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Rule_Name nvarchar(400) = ?
    ,@Column_Name nvarchar(400) = ?
    ,@Criticality int = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[validation_criticality] SET 
    [Rule_Name] = @Rule_Name
    ,[Column_Name] = @Column_Name
    ,[Criticality] = @Criticality
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rule_Name, Column_Name, Criticality, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[validation_criticality]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class FXRates:
        # table
        TableName = 'FXRates'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[FXRates]'
        # columns
        rate_type = 'rate_type'
        from_currency = 'from_currency'
        to_currency = 'to_currency'
        valid_from = 'valid_from'
        exch_rate = 'exch_rate'
        valid_from_year = 'valid_from_year'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, rate_type: str, from_currency: str, to_currency: str, valid_from: date, exch_rate: float, valid_from_year: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @rate_type char(3) = ?
    ,@from_currency char(3) = ?
    ,@to_currency char(3) = ?
    ,@valid_from date(0) = ?
    ,@exch_rate decimal(20, 10) = ?
    ,@valid_from_year smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[FXRates] (
    [rate_type]
    ,[from_currency]
    ,[to_currency]
    ,[valid_from]
    ,[exch_rate]
    ,[valid_from_year]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @rate_type
    ,@from_currency
    ,@to_currency
    ,@valid_from
    ,@exch_rate
    ,@valid_from_year
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, rate_type: str, from_currency: str, to_currency: str, valid_from: date, exch_rate: float, valid_from_year: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @rate_type char(3) = ?
    ,@from_currency char(3) = ?
    ,@to_currency char(3) = ?
    ,@valid_from date(0) = ?
    ,@exch_rate decimal(20, 10) = ?
    ,@valid_from_year smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[FXRates] SET 
    [rate_type] = @rate_type
    ,[valid_from] = @valid_from
    ,[exch_rate] = @exch_rate
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [from_currency] = @from_currency
    ,[to_currency] = @to_currency
    ,[valid_from_year] = @valid_from_year
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, from_currency: str, to_currency: str, valid_from_year: int) -> DataFrame:
            sql = """
DECLARE
    @from_currency char(3) = ?
    ,@to_currency char(3) = ?
    ,@valid_from_year smallint = ?
;

DELETE [dbo].[FXRates]
WHERE
    [from_currency] = @from_currency
    ,[to_currency] = @to_currency
    ,[valid_from_year] = @valid_from_year
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ from_currency, to_currency, valid_from_year ]).exec_df()

    class exposure2not_reinsured:
        # table
        TableName = 'exposure2not_reinsured'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure2not_reinsured]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        exposure_id = 'exposure_id'
        reason_id = 'reason_id'
        reason = 'reason'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, exposure_id: int, reason_id: int, reason: str, ID_Arbeitsvorrat_MR_share: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@exposure_id bigint = ?
    ,@reason_id bigint = ?
    ,@reason nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[exposure2not_reinsured] (
    [ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[exposure_id]
    ,[reason_id]
    ,[reason]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@exposure_id
    ,@reason_id
    ,@reason
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, reason_id, reason, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, exposure_id: int, reason_id: int, reason: str, ID_Arbeitsvorrat_MR_share: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@exposure_id bigint = ?
    ,@reason_id bigint = ?
    ,@reason nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[exposure2not_reinsured] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[exposure_id] = @exposure_id
    ,[reason_id] = @reason_id
    ,[reason] = @reason
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, reason_id, reason, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[exposure2not_reinsured]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Dim_Loss_Event:
        # table
        TableName = 'Dim_Loss_Event'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Loss_Event]'
        # columns
        Loss_Event_ID = 'Loss_Event_ID'
        Loss_Event_ClientInfo = 'Loss_Event_ClientInfo'
        Loss_Event = 'Loss_Event'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Loss_Event_ClientInfo: str, Loss_Event: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Loss_Event] (
    [Loss_Event_ClientInfo]
    ,[Loss_Event]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Loss_Event_ClientInfo
    ,@Loss_Event
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Loss_Event_ClientInfo, Loss_Event, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Loss_Event_ID: int, Loss_Event_ClientInfo: str, Loss_Event: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Loss_Event_ID bigint = ?
    ,@Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Loss_Event] SET 
    [Loss_Event_ClientInfo] = @Loss_Event_ClientInfo
    ,[Loss_Event] = @Loss_Event
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Loss_Event_ID] = @Loss_Event_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Loss_Event_ID, Loss_Event_ClientInfo, Loss_Event, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Loss_Event_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Loss_Event_ID bigint = ?
;

DELETE [dbo].[Dim_Loss_Event]
WHERE
    [Loss_Event_ID] = @Loss_Event_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Loss_Event_ID ]).exec_df()

    class data_requirements:
        # table
        TableName = 'data_requirements'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[data_requirements]'
        # columns
        id = 'id'
        Table_Name = 'Table_Name'
        Column_Name = 'Column_Name'
        Version = 'Version'
        Origin = 'Origin'
        Column_Type = 'Column_Type'
        Is_Mandatory = 'Is_Mandatory'
        Dim_Table_Column = 'Dim_Table_Column'
        Default_Aggregation = 'Default_Aggregation'
        Sort_Index = 'Sort_Index'
        Column_Name_Normalized = 'Column_Name_Normalized'
        Priority = 'Priority'
        Group_Id = 'Group_Id'
        Group_Name = 'Group_Name'
        Dashboard_Column_Label = 'Dashboard_Column_Label'
        Claims_Policy_Related = 'Claims_Policy_Related'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Table_Name: str, Column_Name: str, Version: str, Origin: str, Column_Type: str, Is_Mandatory: str, Dim_Table_Column: str = None, Default_Aggregation: str = None, Sort_Index: int = None, Column_Name_Normalized: str = None, Priority: int = None, Group_Id: int = None, Group_Name: str = None, Dashboard_Column_Label: str = None, Claims_Policy_Related: str = None) -> DataFrame:
            sql = """
DECLARE
    @Table_Name nvarchar(200) = ?
    ,@Column_Name nvarchar(200) = ?
    ,@Version nvarchar(200) = ?
    ,@Origin nvarchar(200) = ?
    ,@Column_Type nvarchar(200) = ?
    ,@Is_Mandatory nvarchar(200) = ?
    ,@Dim_Table_Column nvarchar(200) = ?
    ,@Default_Aggregation nvarchar(20) = ?
    ,@Sort_Index int = ?
    ,@Column_Name_Normalized nvarchar(200) = ?
    ,@Priority int = ?
    ,@Group_Id int = ?
    ,@Group_Name nvarchar(200) = ?
    ,@Dashboard_Column_Label nvarchar(200) = ?
    ,@Claims_Policy_Related nvarchar(200) = ?
;

INSERT INTO [dbo].[data_requirements] (
    [Table_Name]
    ,[Column_Name]
    ,[Version]
    ,[Origin]
    ,[Column_Type]
    ,[Is_Mandatory]
    ,[Dim_Table_Column]
    ,[Default_Aggregation]
    ,[Sort_Index]
    ,[Column_Name_Normalized]
    ,[Priority]
    ,[Group_Id]
    ,[Group_Name]
    ,[Dashboard_Column_Label]
    ,[Claims_Policy_Related]
)
VALUES (
    @Table_Name
    ,@Column_Name
    ,@Version
    ,@Origin
    ,@Column_Type
    ,@Is_Mandatory
    ,@Dim_Table_Column
    ,@Default_Aggregation
    ,@Sort_Index
    ,@Column_Name_Normalized
    ,@Priority
    ,@Group_Id
    ,@Group_Name
    ,@Dashboard_Column_Label
    ,@Claims_Policy_Related
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Table_Name, Column_Name, Version, Origin, Column_Type, Is_Mandatory, Dim_Table_Column, Default_Aggregation, Sort_Index, Column_Name_Normalized, Priority, Group_Id, Group_Name, Dashboard_Column_Label, Claims_Policy_Related ]).exec_df()

        def update(self, id: int, Table_Name: str, Column_Name: str, Version: str, Origin: str, Column_Type: str, Is_Mandatory: str, Dim_Table_Column: str = None, Default_Aggregation: str = None, Sort_Index: int = None, Column_Name_Normalized: str = None, Priority: int = None, Group_Id: int = None, Group_Name: str = None, Dashboard_Column_Label: str = None, Claims_Policy_Related: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Table_Name nvarchar(200) = ?
    ,@Column_Name nvarchar(200) = ?
    ,@Version nvarchar(200) = ?
    ,@Origin nvarchar(200) = ?
    ,@Column_Type nvarchar(200) = ?
    ,@Is_Mandatory nvarchar(200) = ?
    ,@Dim_Table_Column nvarchar(200) = ?
    ,@Default_Aggregation nvarchar(20) = ?
    ,@Sort_Index int = ?
    ,@Column_Name_Normalized nvarchar(200) = ?
    ,@Priority int = ?
    ,@Group_Id int = ?
    ,@Group_Name nvarchar(200) = ?
    ,@Dashboard_Column_Label nvarchar(200) = ?
    ,@Claims_Policy_Related nvarchar(200) = ?
;

UPDATE [dbo].[data_requirements] SET 
    [Table_Name] = @Table_Name
    ,[Column_Name] = @Column_Name
    ,[Version] = @Version
    ,[Origin] = @Origin
    ,[Column_Type] = @Column_Type
    ,[Is_Mandatory] = @Is_Mandatory
    ,[Dim_Table_Column] = @Dim_Table_Column
    ,[Default_Aggregation] = @Default_Aggregation
    ,[Sort_Index] = @Sort_Index
    ,[Column_Name_Normalized] = @Column_Name_Normalized
    ,[Priority] = @Priority
    ,[Group_Id] = @Group_Id
    ,[Group_Name] = @Group_Name
    ,[Dashboard_Column_Label] = @Dashboard_Column_Label
    ,[Claims_Policy_Related] = @Claims_Policy_Related
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Table_Name, Column_Name, Version, Origin, Column_Type, Is_Mandatory, Dim_Table_Column, Default_Aggregation, Sort_Index, Column_Name_Normalized, Priority, Group_Id, Group_Name, Dashboard_Column_Label, Claims_Policy_Related ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[data_requirements]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Company_History:
        # table
        TableName = 'Company_History'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_History]'
        # columns
        id = 'id'
        Company_ID = 'Company_ID'
        Company_Name_Clean = 'Company_Name_Clean'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Industry_ID = 'Industry_ID'
        Company_Name = 'Company_Name'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Parent_Company_ID = 'Parent_Company_ID'
        Ultimate_Company_ID = 'Ultimate_Company_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(600) = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_History] (
    [Company_ID]
    ,[Company_Name_Clean]
    ,[Country_ISO2_ID]
    ,[City_Unified_Name_ID]
    ,[Industry_ID]
    ,[Company_Name]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Parent_Company_ID]
    ,[Ultimate_Company_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_ID
    ,@Company_Name_Clean
    ,@Country_ISO2_ID
    ,@City_Unified_Name_ID
    ,@Industry_ID
    ,@Company_Name
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Parent_Company_ID
    ,@Ultimate_Company_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(600) = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_History] SET 
    [Company_ID] = @Company_ID
    ,[Company_Name_Clean] = @Company_Name_Clean
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_Unified_Name_ID] = @City_Unified_Name_ID
    ,[Industry_ID] = @Industry_ID
    ,[Company_Name] = @Company_Name
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Parent_Company_ID] = @Parent_Company_ID
    ,[Ultimate_Company_ID] = @Ultimate_Company_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[Company_History]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Dim_City:
        # table
        TableName = 'Dim_City'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_City]'
        # columns
        City_ID = 'City_ID'
        City = 'City'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, City: str, City_Unified_Name_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @City nvarchar(400) = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_City] (
    [City]
    ,[City_Unified_Name_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @City
    ,@City_Unified_Name_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ City, City_Unified_Name_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, City_ID: int, City: str, City_Unified_Name_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @City_ID bigint = ?
    ,@City nvarchar(400) = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_City] SET 
    [City] = @City
    ,[City_Unified_Name_ID] = @City_Unified_Name_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [City_ID] = @City_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ City_ID, City, City_Unified_Name_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, City_ID: int) -> DataFrame:
            sql = """
DECLARE
    @City_ID bigint = ?
;

DELETE [dbo].[Dim_City]
WHERE
    [City_ID] = @City_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ City_ID ]).exec_df()

    class exposure_bdx_test:
        # table
        TableName = 'exposure_bdx_test'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx_test]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_RestorationData = 'Coverage_3_Sublimit_RestorationData'
        Coverage_4_Sublimit_Reputation = 'Coverage_4_Sublimit_Reputation'
        Coverage_5_Sublimit_Business_Interruption = 'Coverage_5_Sublimit_Business_Interruption'
        Coverage_6_Sublimit_CBI_IT_Service_Provider = 'Coverage_6_Sublimit_CBI_IT_Service_Provider'
        Coverage_7_Sublimit_CBI_Supply_Chain = 'Coverage_7_Sublimit_CBI_Supply_Chain'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        Coverage_15_Sublimit_System_Failure = 'Coverage_15_Sublimit_System_Failure'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Duplicate_ID = 'Duplicate_ID'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Is_Special_Acceptance = 'Is_Special_Acceptance'
        Client_Limit_Occ_Orig_Curr = 'Client_Limit_Occ_Orig_Curr'
        Full_Limit_Occ_Orig_Curr = 'Full_Limit_Occ_Orig_Curr'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Client_Limit_Occ_USD = 'Client_Limit_Occ_USD'
        Full_Limit_Occ_USD = 'Full_Limit_Occ_USD'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
;

INSERT INTO [dbo].[exposure_bdx_test] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_RestorationData]
    ,[Coverage_4_Sublimit_Reputation]
    ,[Coverage_5_Sublimit_Business_Interruption]
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider]
    ,[Coverage_7_Sublimit_CBI_Supply_Chain]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[Coverage_15_Sublimit_System_Failure]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Duplicate_ID]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Is_Special_Acceptance]
    ,[Client_Limit_Occ_Orig_Curr]
    ,[Full_Limit_Occ_Orig_Curr]
    ,[Policy_ID_Cleaned]
    ,[Client_Limit_Occ_USD]
    ,[Full_Limit_Occ_USD]
    ,[Insured_Name_Clean]
    ,[Company_ClientInfo_ID]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_RestorationData
    ,@Coverage_4_Sublimit_Reputation
    ,@Coverage_5_Sublimit_Business_Interruption
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,@Coverage_7_Sublimit_CBI_Supply_Chain
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@Coverage_15_Sublimit_System_Failure
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Duplicate_ID
    ,@ID_Arbeitsvorrat_MR_share
    ,@Is_Special_Acceptance
    ,@Client_Limit_Occ_Orig_Curr
    ,@Full_Limit_Occ_Orig_Curr
    ,@Policy_ID_Cleaned
    ,@Client_Limit_Occ_USD
    ,@Full_Limit_Occ_USD
    ,@Insured_Name_Clean
    ,@Company_ClientInfo_ID
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
;

UPDATE [dbo].[exposure_bdx_test] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_RestorationData] = @Coverage_3_Sublimit_RestorationData
    ,[Coverage_4_Sublimit_Reputation] = @Coverage_4_Sublimit_Reputation
    ,[Coverage_5_Sublimit_Business_Interruption] = @Coverage_5_Sublimit_Business_Interruption
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider] = @Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,[Coverage_7_Sublimit_CBI_Supply_Chain] = @Coverage_7_Sublimit_CBI_Supply_Chain
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[Coverage_15_Sublimit_System_Failure] = @Coverage_15_Sublimit_System_Failure
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Duplicate_ID] = @Duplicate_ID
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Is_Special_Acceptance] = @Is_Special_Acceptance
    ,[Client_Limit_Occ_Orig_Curr] = @Client_Limit_Occ_Orig_Curr
    ,[Full_Limit_Occ_Orig_Curr] = @Full_Limit_Occ_Orig_Curr
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Client_Limit_Occ_USD] = @Client_Limit_Occ_USD
    ,[Full_Limit_Occ_USD] = @Full_Limit_Occ_USD
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Company_ClientInfo_ID] = @Company_ClientInfo_ID
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_bdx_test]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_cause:
        # table
        TableName = 'claims_cause'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_cause]'
        # columns
        id = 'id'
        claim_id = 'claim_id'
        claim_cause = 'claim_cause'
        classification_quality = 'classification_quality'
        classification_score = 'classification_score'
        claim_cause_alternatives = 'claim_cause_alternatives'
        internal_external = 'internal_external'
        intention = 'intention'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, claim_id: int, claim_cause: str = None, classification_quality: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @claim_id bigint = ?
    ,@claim_cause nvarchar(100) = ?
    ,@classification_quality nvarchar(100) = ?
    ,@classification_score float = ?
    ,@claim_cause_alternatives nvarchar(1000) = ?
    ,@internal_external nvarchar(20) = ?
    ,@intention nvarchar(20) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[claims_cause] (
    [claim_id]
    ,[claim_cause]
    ,[classification_quality]
    ,[classification_score]
    ,[claim_cause_alternatives]
    ,[internal_external]
    ,[intention]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @claim_id
    ,@claim_cause
    ,@classification_quality
    ,@classification_score
    ,@claim_cause_alternatives
    ,@internal_external
    ,@intention
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ claim_id, claim_cause, classification_quality, classification_score, claim_cause_alternatives, internal_external, intention, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, claim_id: int, claim_cause: str = None, classification_quality: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@claim_id bigint = ?
    ,@claim_cause nvarchar(100) = ?
    ,@classification_quality nvarchar(100) = ?
    ,@classification_score float = ?
    ,@claim_cause_alternatives nvarchar(1000) = ?
    ,@internal_external nvarchar(20) = ?
    ,@intention nvarchar(20) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[claims_cause] SET 
    [claim_id] = @claim_id
    ,[claim_cause] = @claim_cause
    ,[classification_quality] = @classification_quality
    ,[classification_score] = @classification_score
    ,[claim_cause_alternatives] = @claim_cause_alternatives
    ,[internal_external] = @internal_external
    ,[intention] = @intention
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, claim_id, claim_cause, classification_quality, classification_score, claim_cause_alternatives, internal_external, intention, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[claims_cause]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class FXRates_20210302:
        # table
        TableName = 'FXRates_20210302'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[FXRates_20210302]'
        # columns
        rate_type = 'rate_type'
        from_currency = 'from_currency'
        to_currency = 'to_currency'
        valid_from = 'valid_from'
        exch_rate = 'exch_rate'
        valid_from_year = 'valid_from_year'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, rate_type: str = None, from_currency: str = None, to_currency: str = None, valid_from: date = None, exch_rate: float = None, valid_from_year: float = None) -> DataFrame:
            sql = """
DECLARE
    @rate_type varchar(255) = ?
    ,@from_currency varchar(255) = ?
    ,@to_currency varchar(255) = ?
    ,@valid_from date(0) = ?
    ,@exch_rate float = ?
    ,@valid_from_year float = ?
;

INSERT INTO [dbo].[FXRates_20210302] (
    [rate_type]
    ,[from_currency]
    ,[to_currency]
    ,[valid_from]
    ,[exch_rate]
    ,[valid_from_year]
)
VALUES (
    @rate_type
    ,@from_currency
    ,@to_currency
    ,@valid_from
    ,@exch_rate
    ,@valid_from_year
);
"""
            return DbCmd(self.cnOrStr, sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year ]).exec_df()

        def update(self, rate_type: str = None, from_currency: str = None, to_currency: str = None, valid_from: date = None, exch_rate: float = None, valid_from_year: float = None) -> DataFrame:
            sql = """
DECLARE
    @rate_type varchar(255) = ?
    ,@from_currency varchar(255) = ?
    ,@to_currency varchar(255) = ?
    ,@valid_from date(0) = ?
    ,@exch_rate float = ?
    ,@valid_from_year float = ?
;

UPDATE [dbo].[FXRates_20210302] SET 
    [rate_type] = @rate_type
    ,[from_currency] = @from_currency
    ,[to_currency] = @to_currency
    ,[valid_from] = @valid_from
    ,[exch_rate] = @exch_rate
    ,[valid_from_year] = @valid_from_year
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[FXRates_20210302]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure2mr_share:
        # table
        TableName = 'exposure2mr_share'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure2mr_share]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        exposure_id = 'exposure_id'
        re_contract_id = 're_contract_id'
        mr_share = 'mr_share'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, exposure_id: int, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, mr_share: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@exposure_id bigint = ?
    ,@re_contract_id bigint = ?
    ,@mr_share float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[exposure2mr_share] (
    [ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[exposure_id]
    ,[re_contract_id]
    ,[mr_share]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@exposure_id
    ,@re_contract_id
    ,@mr_share
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, re_contract_id, mr_share, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, exposure_id: int, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, mr_share: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@exposure_id bigint = ?
    ,@re_contract_id bigint = ?
    ,@mr_share float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[exposure2mr_share] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[exposure_id] = @exposure_id
    ,[re_contract_id] = @re_contract_id
    ,[mr_share] = @mr_share
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, re_contract_id, mr_share, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[exposure2mr_share]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class data_entry_meta_information_20220530:
        # table
        TableName = 'data_entry_meta_information_20220530'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[data_entry_meta_information_20220530]'
        # columns
        id = 'id'
        File_Name = 'File_Name'
        UW = 'UW'
        BU = 'BU'
        Number_Risks = 'Number_Risks'
        Number_Claims = 'Number_Claims'
        bdx_type = 'bdx_type'
        As_at_Date = 'As_at_Date'
        Subsystem = 'Subsystem'
        Program_IDs = 'Program_IDs'
        Additional_Program_IDs = 'Additional_Program_IDs'
        FSRI_ID = 'FSRI_ID'
        Client_Name = 'Client_Name'
        BuPa = 'BuPa'
        Treaty_Program_Name = 'Treaty_Program_Name'
        Begin_Date = 'Begin_Date'
        End_Date = 'End_Date'
        UW_Year = 'UW_Year'
        Is_From_CU_Collection = 'Is_From_CU_Collection'
        Is_Ignored = 'Is_Ignored'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[data_entry_meta_information_20220530] (
    [id]
    ,[File_Name]
    ,[UW]
    ,[BU]
    ,[Number_Risks]
    ,[Number_Claims]
    ,[bdx_type]
    ,[As_at_Date]
    ,[Subsystem]
    ,[Program_IDs]
    ,[Additional_Program_IDs]
    ,[FSRI_ID]
    ,[Client_Name]
    ,[BuPa]
    ,[Treaty_Program_Name]
    ,[Begin_Date]
    ,[End_Date]
    ,[UW_Year]
    ,[Is_From_CU_Collection]
    ,[Is_Ignored]
    ,[ID_Arbeitsvorrat]
    ,[Comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@File_Name
    ,@UW
    ,@BU
    ,@Number_Risks
    ,@Number_Claims
    ,@bdx_type
    ,@As_at_Date
    ,@Subsystem
    ,@Program_IDs
    ,@Additional_Program_IDs
    ,@FSRI_ID
    ,@Client_Name
    ,@BuPa
    ,@Treaty_Program_Name
    ,@Begin_Date
    ,@End_Date
    ,@UW_Year
    ,@Is_From_CU_Collection
    ,@Is_Ignored
    ,@ID_Arbeitsvorrat
    ,@Comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[data_entry_meta_information_20220530] SET 
    [id] = @id
    ,[File_Name] = @File_Name
    ,[UW] = @UW
    ,[BU] = @BU
    ,[Number_Risks] = @Number_Risks
    ,[Number_Claims] = @Number_Claims
    ,[bdx_type] = @bdx_type
    ,[As_at_Date] = @As_at_Date
    ,[Subsystem] = @Subsystem
    ,[Program_IDs] = @Program_IDs
    ,[Additional_Program_IDs] = @Additional_Program_IDs
    ,[FSRI_ID] = @FSRI_ID
    ,[Client_Name] = @Client_Name
    ,[BuPa] = @BuPa
    ,[Treaty_Program_Name] = @Treaty_Program_Name
    ,[Begin_Date] = @Begin_Date
    ,[End_Date] = @End_Date
    ,[UW_Year] = @UW_Year
    ,[Is_From_CU_Collection] = @Is_From_CU_Collection
    ,[Is_Ignored] = @Is_Ignored
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Comment] = @Comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[data_entry_meta_information_20220530]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Product:
        # table
        TableName = 'Dim_Product'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Product]'
        # columns
        Product_ID = 'Product_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Product_Name = 'Product_Name'
        Product_Unified_Name = 'Product_Unified_Name'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Technical_Client_ID: int, Product_Name: str, Product_Unified_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Technical_Client_ID bigint = ?
    ,@Product_Name nvarchar(1000) = ?
    ,@Product_Unified_Name nvarchar(200) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Product] (
    [Technical_Client_ID]
    ,[Product_Name]
    ,[Product_Unified_Name]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Technical_Client_ID
    ,@Product_Name
    ,@Product_Unified_Name
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Technical_Client_ID, Product_Name, Product_Unified_Name, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Product_ID: int, Technical_Client_ID: int, Product_Name: str, Product_Unified_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Product_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Product_Name nvarchar(1000) = ?
    ,@Product_Unified_Name nvarchar(200) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Product] SET 
    [Technical_Client_ID] = @Technical_Client_ID
    ,[Product_Name] = @Product_Name
    ,[Product_Unified_Name] = @Product_Unified_Name
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Product_ID] = @Product_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Product_ID, Technical_Client_ID, Product_Name, Product_Unified_Name, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Product_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Product_ID bigint = ?
;

DELETE [dbo].[Dim_Product]
WHERE
    [Product_ID] = @Product_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Product_ID ]).exec_df()

    class file_level_meta_information_20220530:
        # table
        TableName = 'file_level_meta_information_20220530'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_meta_information_20220530]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        Subsystem_ID = 'Subsystem_ID'
        Treaty_Programm_ID = 'Treaty_Programm_ID'
        FSRI_ID = 'FSRI_ID'
        UW_Year = 'UW_Year'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        Priority = 'Priority'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[file_level_meta_information_20220530] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[Subsystem_ID]
    ,[Treaty_Programm_ID]
    ,[FSRI_ID]
    ,[UW_Year]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[Priority]
    ,[Deadline]
    ,[Exclude_from_dashboards]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@Subsystem_ID
    ,@Treaty_Programm_ID
    ,@FSRI_ID
    ,@UW_Year
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@Priority
    ,@Deadline
    ,@Exclude_from_dashboards
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[file_level_meta_information_20220530] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[Subsystem_ID] = @Subsystem_ID
    ,[Treaty_Programm_ID] = @Treaty_Programm_ID
    ,[FSRI_ID] = @FSRI_ID
    ,[UW_Year] = @UW_Year
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[Priority] = @Priority
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[file_level_meta_information_20220530]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Country:
        # table
        TableName = 'Dim_Country'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Country]'
        # columns
        Country_ID = 'Country_ID'
        Country = 'Country'
        Country_ISO2_Code = 'Country_ISO2_Code'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Country: str, Country_ISO2_Code: str, Country_ISO2_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Country nvarchar(100) = ?
    ,@Country_ISO2_Code nvarchar(4) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Country] (
    [Country]
    ,[Country_ISO2_Code]
    ,[Country_ISO2_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Country
    ,@Country_ISO2_Code
    ,@Country_ISO2_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Country, Country_ISO2_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Country_ID: int, Country: str, Country_ISO2_Code: str, Country_ISO2_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Country_ID bigint = ?
    ,@Country nvarchar(100) = ?
    ,@Country_ISO2_Code nvarchar(4) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Country] SET 
    [Country] = @Country
    ,[Country_ISO2_Code] = @Country_ISO2_Code
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Country_ID] = @Country_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Country_ID, Country, Country_ISO2_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Country_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Country_ID bigint = ?
;

DELETE [dbo].[Dim_Country]
WHERE
    [Country_ID] = @Country_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Country_ID ]).exec_df()

    class Dim_Signal_Reserve:
        # table
        TableName = 'Dim_Signal_Reserve'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Signal_Reserve]'
        # columns
        Signal_Reserve_ID = 'Signal_Reserve_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Reserve_Amount = 'Reserve_Amount'
        Reserve_Meaning = 'Reserve_Meaning'
        Is_Signal_Reserve = 'Is_Signal_Reserve'
        Type_of_Reserve = 'Type_of_Reserve'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Technical_Client_ID: int, Reserve_Amount: float, Reserve_Meaning: str = None, Is_Signal_Reserve: int = None, Type_of_Reserve: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Technical_Client_ID bigint = ?
    ,@Reserve_Amount float = ?
    ,@Reserve_Meaning nvarchar(2048) = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Type_of_Reserve nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Signal_Reserve] (
    [Technical_Client_ID]
    ,[Reserve_Amount]
    ,[Reserve_Meaning]
    ,[Is_Signal_Reserve]
    ,[Type_of_Reserve]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Technical_Client_ID
    ,@Reserve_Amount
    ,@Reserve_Meaning
    ,@Is_Signal_Reserve
    ,@Type_of_Reserve
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Technical_Client_ID, Reserve_Amount, Reserve_Meaning, Is_Signal_Reserve, Type_of_Reserve, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Signal_Reserve_ID: int, Technical_Client_ID: int, Reserve_Amount: float, Reserve_Meaning: str = None, Is_Signal_Reserve: int = None, Type_of_Reserve: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Signal_Reserve_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Reserve_Amount float = ?
    ,@Reserve_Meaning nvarchar(2048) = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Type_of_Reserve nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Signal_Reserve] SET 
    [Technical_Client_ID] = @Technical_Client_ID
    ,[Reserve_Amount] = @Reserve_Amount
    ,[Reserve_Meaning] = @Reserve_Meaning
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
    ,[Type_of_Reserve] = @Type_of_Reserve
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Signal_Reserve_ID] = @Signal_Reserve_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Signal_Reserve_ID, Technical_Client_ID, Reserve_Amount, Reserve_Meaning, Is_Signal_Reserve, Type_of_Reserve, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Signal_Reserve_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Signal_Reserve_ID bigint = ?
;

DELETE [dbo].[Dim_Signal_Reserve]
WHERE
    [Signal_Reserve_ID] = @Signal_Reserve_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Signal_Reserve_ID ]).exec_df()

    class Client_Name:
        # table
        TableName = 'Client_Name'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Client_Name]'
        # columns
        Id = 'Id'
        Parent_Id = 'Parent_Id'
        Client = 'Client'
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Parent_Id: int, Client: str, Hierarchy_Level_Id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Parent_Id bigint = ?
    ,@Client nvarchar(400) = ?
    ,@Hierarchy_Level_Id int = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Client_Name] (
    [Parent_Id]
    ,[Client]
    ,[Hierarchy_Level_Id]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Parent_Id
    ,@Client
    ,@Hierarchy_Level_Id
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Parent_Id, Client, Hierarchy_Level_Id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Id: int, Parent_Id: int, Client: str, Hierarchy_Level_Id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Id bigint = ?
    ,@Parent_Id bigint = ?
    ,@Client nvarchar(400) = ?
    ,@Hierarchy_Level_Id int = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Client_Name] SET 
    [Id] = @Id
    ,[Parent_Id] = @Parent_Id
    ,[Client] = @Client
    ,[Hierarchy_Level_Id] = @Hierarchy_Level_Id
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Id, Parent_Id, Client, Hierarchy_Level_Id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Client_Name]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_cause_keywords:
        # table
        TableName = 'claims_cause_keywords'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_cause_keywords]'
        # columns
        id = 'id'
        cause_id = 'cause_id'
        claim_id = 'claim_id'
        key_word = 'key_word'
        score = 'score'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, cause_id: int, claim_id: int, key_word: str = None, score: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @cause_id bigint = ?
    ,@claim_id bigint = ?
    ,@key_word nvarchar(200) = ?
    ,@score float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[claims_cause_keywords] (
    [cause_id]
    ,[claim_id]
    ,[key_word]
    ,[score]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @cause_id
    ,@claim_id
    ,@key_word
    ,@score
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ cause_id, claim_id, key_word, score, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, cause_id: int, claim_id: int, key_word: str = None, score: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@cause_id bigint = ?
    ,@claim_id bigint = ?
    ,@key_word nvarchar(200) = ?
    ,@score float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[claims_cause_keywords] SET 
    [cause_id] = @cause_id
    ,[claim_id] = @claim_id
    ,[key_word] = @key_word
    ,[score] = @score
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, cause_id, claim_id, key_word, score, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[claims_cause_keywords]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class naics_cambridge:
        # table
        TableName = 'naics_cambridge'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[naics_cambridge]'
        # columns
        id = 'id'
        naics_code = 'naics_code'
        cambridge_code = 'cambridge_code'
        cambridge_name = 'cambridge_name'
        create_time = 'create_time'
        change_time = 'change_time'
        changed_by = 'changed_by'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, naics_code: str = None, cambridge_code: str = None, cambridge_name: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @naics_code nvarchar(12) = ?
    ,@cambridge_code nvarchar(8) = ?
    ,@cambridge_name nvarchar(400) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

INSERT INTO [dbo].[naics_cambridge] (
    [naics_code]
    ,[cambridge_code]
    ,[cambridge_name]
    ,[create_time]
    ,[change_time]
    ,[changed_by]
)
VALUES (
    @naics_code
    ,@cambridge_code
    ,@cambridge_name
    ,@create_time
    ,@change_time
    ,@changed_by
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ naics_code, cambridge_code, cambridge_name, create_time, change_time, changed_by ]).exec_df()

        def update(self, id: int, naics_code: str = None, cambridge_code: str = None, cambridge_name: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@naics_code nvarchar(12) = ?
    ,@cambridge_code nvarchar(8) = ?
    ,@cambridge_name nvarchar(400) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

UPDATE [dbo].[naics_cambridge] SET 
    [naics_code] = @naics_code
    ,[cambridge_code] = @cambridge_code
    ,[cambridge_name] = @cambridge_name
    ,[create_time] = @create_time
    ,[change_time] = @change_time
    ,[changed_by] = @changed_by
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, naics_code, cambridge_code, cambridge_name, create_time, change_time, changed_by ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[naics_cambridge]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class tDimFileMetaInfo:
        # table
        TableName = 'tDimFileMetaInfo'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimFileMetaInfo]'
        # columns
        FileMetaInfoKey = 'FileMetaInfoKey'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        runScript = 'runScript'
        runNorm = 'runNorm'
        runBvD = 'runBvD'
        runMRShares = 'runMRShares'
        runValidation = 'runValidation'
        runClaimsLinking = 'runClaimsLinking'
        Priority = 'Priority'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'
        UW_Year = 'UW_Year'
        Count_Missing_Priority_1 = 'Count_Missing_Priority_1'
        Count_Missing_Priority_2 = 'Count_Missing_Priority_2'
        Count_Missing_Priority_3 = 'Count_Missing_Priority_3'
        Count_OK_Priority_3 = 'Count_OK_Priority_3'
        completeness_traffic_light = 'completeness_traffic_light'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, BU: str, Client_Name: str, Orig_File_Name: str, bdx_type: str, Smart_matching_done: int, R_code_done: int, Q_A_done: int, Function_ran: int, Validation_issues_done: int, Four_Eye_Check_done: int, Signoff_done: int, Contact_Data_Team: str, Responsible_Four_Eye_Check: str, UW: str, PML_needs_to_be_done: int, PML_has_been_done: int, SRAC_needs_to_be_done: int, SRAC_has_been_done: int, runScript: int, runNorm: int, runBvD: int, runMRShares: int, runValidation: int, runClaimsLinking: int, Priority: str, Deadline: date, Exclude_from_dashboards: int, UW_Year: int, Count_Missing_Priority_1: int, Count_Missing_Priority_2: int, Count_Missing_Priority_3: int, Count_OK_Priority_3: int, completeness_traffic_light: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Smart_matching_done bit = ?
    ,@R_code_done bit = ?
    ,@Q_A_done bit = ?
    ,@Function_ran bit = ?
    ,@Validation_issues_done bit = ?
    ,@Four_Eye_Check_done bit = ?
    ,@Signoff_done bit = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done bit = ?
    ,@PML_has_been_done bit = ?
    ,@SRAC_needs_to_be_done bit = ?
    ,@SRAC_has_been_done bit = ?
    ,@runScript bit = ?
    ,@runNorm bit = ?
    ,@runBvD bit = ?
    ,@runMRShares bit = ?
    ,@runValidation bit = ?
    ,@runClaimsLinking bit = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline date(0) = ?
    ,@Exclude_from_dashboards bit = ?
    ,@UW_Year bigint = ?
    ,@Count_Missing_Priority_1 int = ?
    ,@Count_Missing_Priority_2 int = ?
    ,@Count_Missing_Priority_3 int = ?
    ,@Count_OK_Priority_3 int = ?
    ,@completeness_traffic_light varchar(10) = ?
;

INSERT INTO [dbo].[tDimFileMetaInfo] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[runScript]
    ,[runNorm]
    ,[runBvD]
    ,[runMRShares]
    ,[runValidation]
    ,[runClaimsLinking]
    ,[Priority]
    ,[Deadline]
    ,[Exclude_from_dashboards]
    ,[UW_Year]
    ,[Count_Missing_Priority_1]
    ,[Count_Missing_Priority_2]
    ,[Count_Missing_Priority_3]
    ,[Count_OK_Priority_3]
    ,[completeness_traffic_light]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@runScript
    ,@runNorm
    ,@runBvD
    ,@runMRShares
    ,@runValidation
    ,@runClaimsLinking
    ,@Priority
    ,@Deadline
    ,@Exclude_from_dashboards
    ,@UW_Year
    ,@Count_Missing_Priority_1
    ,@Count_Missing_Priority_2
    ,@Count_Missing_Priority_3
    ,@Count_OK_Priority_3
    ,@completeness_traffic_light
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, Priority, Deadline, Exclude_from_dashboards, UW_Year, Count_Missing_Priority_1, Count_Missing_Priority_2, Count_Missing_Priority_3, Count_OK_Priority_3, completeness_traffic_light ]).exec_df()

        def update(self, FileMetaInfoKey: int, ID_Arbeitsvorrat: str, BU: str, Client_Name: str, Orig_File_Name: str, bdx_type: str, Smart_matching_done: int, R_code_done: int, Q_A_done: int, Function_ran: int, Validation_issues_done: int, Four_Eye_Check_done: int, Signoff_done: int, Contact_Data_Team: str, Responsible_Four_Eye_Check: str, UW: str, PML_needs_to_be_done: int, PML_has_been_done: int, SRAC_needs_to_be_done: int, SRAC_has_been_done: int, runScript: int, runNorm: int, runBvD: int, runMRShares: int, runValidation: int, runClaimsLinking: int, Priority: str, Deadline: date, Exclude_from_dashboards: int, UW_Year: int, Count_Missing_Priority_1: int, Count_Missing_Priority_2: int, Count_Missing_Priority_3: int, Count_OK_Priority_3: int, completeness_traffic_light: str = None) -> DataFrame:
            sql = """
DECLARE
    @FileMetaInfoKey int = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Smart_matching_done bit = ?
    ,@R_code_done bit = ?
    ,@Q_A_done bit = ?
    ,@Function_ran bit = ?
    ,@Validation_issues_done bit = ?
    ,@Four_Eye_Check_done bit = ?
    ,@Signoff_done bit = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done bit = ?
    ,@PML_has_been_done bit = ?
    ,@SRAC_needs_to_be_done bit = ?
    ,@SRAC_has_been_done bit = ?
    ,@runScript bit = ?
    ,@runNorm bit = ?
    ,@runBvD bit = ?
    ,@runMRShares bit = ?
    ,@runValidation bit = ?
    ,@runClaimsLinking bit = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline date(0) = ?
    ,@Exclude_from_dashboards bit = ?
    ,@UW_Year bigint = ?
    ,@Count_Missing_Priority_1 int = ?
    ,@Count_Missing_Priority_2 int = ?
    ,@Count_Missing_Priority_3 int = ?
    ,@Count_OK_Priority_3 int = ?
    ,@completeness_traffic_light varchar(10) = ?
;

UPDATE [dbo].[tDimFileMetaInfo] SET 
    [FileMetaInfoKey] = @FileMetaInfoKey
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[runScript] = @runScript
    ,[runNorm] = @runNorm
    ,[runBvD] = @runBvD
    ,[runMRShares] = @runMRShares
    ,[runValidation] = @runValidation
    ,[runClaimsLinking] = @runClaimsLinking
    ,[Priority] = @Priority
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
    ,[UW_Year] = @UW_Year
    ,[Count_Missing_Priority_1] = @Count_Missing_Priority_1
    ,[Count_Missing_Priority_2] = @Count_Missing_Priority_2
    ,[Count_Missing_Priority_3] = @Count_Missing_Priority_3
    ,[Count_OK_Priority_3] = @Count_OK_Priority_3
    ,[completeness_traffic_light] = @completeness_traffic_light
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ FileMetaInfoKey, ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, Priority, Deadline, Exclude_from_dashboards, UW_Year, Count_Missing_Priority_1, Count_Missing_Priority_2, Count_Missing_Priority_3, Count_OK_Priority_3, completeness_traffic_light ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tDimFileMetaInfo]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class file_level_test:
        # table
        TableName = 'file_level_test'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_test]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        FSRI_ID = 'FSRI_ID'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, Smart_matching_done: str, R_code_done: str, Q_A_done: str, Function_ran: str, Validation_issues_done: str, Four_Eye_Check_done: str, Signoff_done: str, PML_needs_to_be_done: str, PML_has_been_done: str, SRAC_needs_to_be_done: str, SRAC_has_been_done: str, BU: str = None, Client_Name: str = None, Orig_File_Name: str = None, bdx_type: str = None, FSRI_ID: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, Deadline: datetime = None, Exclude_from_dashboards: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU varchar(5) = ?
    ,@Client_Name nvarchar(max) = ?
    ,@Orig_File_Name nvarchar(max) = ?
    ,@bdx_type nvarchar(max) = ?
    ,@FSRI_ID nvarchar(100) = ?
    ,@Smart_matching_done varchar(3) = ?
    ,@R_code_done varchar(3) = ?
    ,@Q_A_done varchar(3) = ?
    ,@Function_ran varchar(3) = ?
    ,@Validation_issues_done varchar(3) = ?
    ,@Four_Eye_Check_done varchar(3) = ?
    ,@Signoff_done varchar(3) = ?
    ,@Contact_Data_Team nvarchar(1000) = ?
    ,@Responsible_Four_Eye_Check nvarchar(1000) = ?
    ,@UW nvarchar(1000) = ?
    ,@PML_needs_to_be_done varchar(3) = ?
    ,@PML_has_been_done varchar(3) = ?
    ,@SRAC_needs_to_be_done varchar(3) = ?
    ,@SRAC_has_been_done varchar(3) = ?
    ,@Deadline datetime2(7) = ?
    ,@Exclude_from_dashboards bit = ?
;

INSERT INTO [dbo].[file_level_test] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[FSRI_ID]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[Deadline]
    ,[Exclude_from_dashboards]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@FSRI_ID
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@Deadline
    ,@Exclude_from_dashboards
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, Smart_matching_done: str, R_code_done: str, Q_A_done: str, Function_ran: str, Validation_issues_done: str, Four_Eye_Check_done: str, Signoff_done: str, PML_needs_to_be_done: str, PML_has_been_done: str, SRAC_needs_to_be_done: str, SRAC_has_been_done: str, BU: str = None, Client_Name: str = None, Orig_File_Name: str = None, bdx_type: str = None, FSRI_ID: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, Deadline: datetime = None, Exclude_from_dashboards: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU varchar(5) = ?
    ,@Client_Name nvarchar(max) = ?
    ,@Orig_File_Name nvarchar(max) = ?
    ,@bdx_type nvarchar(max) = ?
    ,@FSRI_ID nvarchar(100) = ?
    ,@Smart_matching_done varchar(3) = ?
    ,@R_code_done varchar(3) = ?
    ,@Q_A_done varchar(3) = ?
    ,@Function_ran varchar(3) = ?
    ,@Validation_issues_done varchar(3) = ?
    ,@Four_Eye_Check_done varchar(3) = ?
    ,@Signoff_done varchar(3) = ?
    ,@Contact_Data_Team nvarchar(1000) = ?
    ,@Responsible_Four_Eye_Check nvarchar(1000) = ?
    ,@UW nvarchar(1000) = ?
    ,@PML_needs_to_be_done varchar(3) = ?
    ,@PML_has_been_done varchar(3) = ?
    ,@SRAC_needs_to_be_done varchar(3) = ?
    ,@SRAC_has_been_done varchar(3) = ?
    ,@Deadline datetime2(7) = ?
    ,@Exclude_from_dashboards bit = ?
;

UPDATE [dbo].[file_level_test] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[FSRI_ID] = @FSRI_ID
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[file_level_test]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_bdx_1607:
        # table
        TableName = 'exposure_bdx_1607'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx_1607]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_RestorationData = 'Coverage_3_Sublimit_RestorationData'
        Coverage_4_Sublimit_Reputation = 'Coverage_4_Sublimit_Reputation'
        Coverage_5_Sublimit_Business_Interruption = 'Coverage_5_Sublimit_Business_Interruption'
        Coverage_6_Sublimit_CBI_IT_Service_Provider = 'Coverage_6_Sublimit_CBI_IT_Service_Provider'
        Coverage_7_Sublimit_CBI_Supply_Chain = 'Coverage_7_Sublimit_CBI_Supply_Chain'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        Coverage_15_Sublimit_System_Failure = 'Coverage_15_Sublimit_System_Failure'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Duplicate_ID = 'Duplicate_ID'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Is_Special_Acceptance = 'Is_Special_Acceptance'
        Client_Limit_Occ_Orig_Curr = 'Client_Limit_Occ_Orig_Curr'
        Full_Limit_Occ_Orig_Curr = 'Full_Limit_Occ_Orig_Curr'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Client_Limit_Occ_USD = 'Client_Limit_Occ_USD'
        Full_Limit_Occ_USD = 'Full_Limit_Occ_USD'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
;

INSERT INTO [dbo].[exposure_bdx_1607] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_RestorationData]
    ,[Coverage_4_Sublimit_Reputation]
    ,[Coverage_5_Sublimit_Business_Interruption]
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider]
    ,[Coverage_7_Sublimit_CBI_Supply_Chain]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[Coverage_15_Sublimit_System_Failure]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Duplicate_ID]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Is_Special_Acceptance]
    ,[Client_Limit_Occ_Orig_Curr]
    ,[Full_Limit_Occ_Orig_Curr]
    ,[Policy_ID_Cleaned]
    ,[Client_Limit_Occ_USD]
    ,[Full_Limit_Occ_USD]
    ,[Insured_Name_Clean]
    ,[Company_ClientInfo_ID]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_RestorationData
    ,@Coverage_4_Sublimit_Reputation
    ,@Coverage_5_Sublimit_Business_Interruption
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,@Coverage_7_Sublimit_CBI_Supply_Chain
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@Coverage_15_Sublimit_System_Failure
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Duplicate_ID
    ,@ID_Arbeitsvorrat_MR_share
    ,@Is_Special_Acceptance
    ,@Client_Limit_Occ_Orig_Curr
    ,@Full_Limit_Occ_Orig_Curr
    ,@Policy_ID_Cleaned
    ,@Client_Limit_Occ_USD
    ,@Full_Limit_Occ_USD
    ,@Insured_Name_Clean
    ,@Company_ClientInfo_ID
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
;

UPDATE [dbo].[exposure_bdx_1607] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_RestorationData] = @Coverage_3_Sublimit_RestorationData
    ,[Coverage_4_Sublimit_Reputation] = @Coverage_4_Sublimit_Reputation
    ,[Coverage_5_Sublimit_Business_Interruption] = @Coverage_5_Sublimit_Business_Interruption
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider] = @Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,[Coverage_7_Sublimit_CBI_Supply_Chain] = @Coverage_7_Sublimit_CBI_Supply_Chain
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[Coverage_15_Sublimit_System_Failure] = @Coverage_15_Sublimit_System_Failure
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Duplicate_ID] = @Duplicate_ID
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Is_Special_Acceptance] = @Is_Special_Acceptance
    ,[Client_Limit_Occ_Orig_Curr] = @Client_Limit_Occ_Orig_Curr
    ,[Full_Limit_Occ_Orig_Curr] = @Full_Limit_Occ_Orig_Curr
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Client_Limit_Occ_USD] = @Client_Limit_Occ_USD
    ,[Full_Limit_Occ_USD] = @Full_Limit_Occ_USD
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Company_ClientInfo_ID] = @Company_ClientInfo_ID
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_bdx_1607]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class file_level_test_table:
        # table
        TableName = 'file_level_test_table'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_test_table]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        FSRI_ID = 'FSRI_ID'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, FSRI_ID: str = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Deadline: datetime = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(8000) = ?
    ,@Orig_File_Name nvarchar(8000) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@FSRI_ID nvarchar(100) = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
;

INSERT INTO [dbo].[file_level_test_table] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[FSRI_ID]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[Deadline]
    ,[Exclude_from_dashboards]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@FSRI_ID
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@Deadline
    ,@Exclude_from_dashboards
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, FSRI_ID: str = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Deadline: datetime = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(8000) = ?
    ,@Orig_File_Name nvarchar(8000) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@FSRI_ID nvarchar(100) = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
;

UPDATE [dbo].[file_level_test_table] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[FSRI_ID] = @FSRI_ID
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[file_level_test_table]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Currency:
        # table
        TableName = 'Dim_Currency'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Currency]'
        # columns
        Currency_ID = 'Currency_ID'
        Currency = 'Currency'
        Is_Reporting_Currency = 'Is_Reporting_Currency'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Currency: str, Is_Reporting_Currency: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Currency nvarchar(6) = ?
    ,@Is_Reporting_Currency bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Currency] (
    [Currency]
    ,[Is_Reporting_Currency]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Currency
    ,@Is_Reporting_Currency
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Currency, Is_Reporting_Currency, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Currency_ID: int, Currency: str, Is_Reporting_Currency: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Currency_ID bigint = ?
    ,@Currency nvarchar(6) = ?
    ,@Is_Reporting_Currency bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Currency] SET 
    [Currency] = @Currency
    ,[Is_Reporting_Currency] = @Is_Reporting_Currency
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Currency_ID] = @Currency_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Currency_ID, Currency, Is_Reporting_Currency, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Currency_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Currency_ID bigint = ?
;

DELETE [dbo].[Dim_Currency]
WHERE
    [Currency_ID] = @Currency_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Currency_ID ]).exec_df()

    class re_modifications:
        # table
        TableName = 're_modifications'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[re_modifications]'
        # columns
        id = 'id'
        re_contract_id = 're_contract_id'
        modified_field = 'modified_field'
        previous_value = 'previous_value'
        modified_value = 'modified_value'
        modification_time = 'modification_time'
        changed_by = 'changed_by'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, re_contract_id: int, modified_field: str, previous_value: str = None, modified_value: str = None, modification_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @re_contract_id bigint = ?
    ,@modified_field nvarchar(200) = ?
    ,@previous_value nvarchar(2048) = ?
    ,@modified_value nvarchar(2048) = ?
    ,@modification_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

INSERT INTO [dbo].[re_modifications] (
    [re_contract_id]
    ,[modified_field]
    ,[previous_value]
    ,[modified_value]
    ,[modification_time]
    ,[changed_by]
)
VALUES (
    @re_contract_id
    ,@modified_field
    ,@previous_value
    ,@modified_value
    ,@modification_time
    ,@changed_by
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ re_contract_id, modified_field, previous_value, modified_value, modification_time, changed_by ]).exec_df()

        def update(self, id: int, re_contract_id: int, modified_field: str, previous_value: str = None, modified_value: str = None, modification_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@re_contract_id bigint = ?
    ,@modified_field nvarchar(200) = ?
    ,@previous_value nvarchar(2048) = ?
    ,@modified_value nvarchar(2048) = ?
    ,@modification_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

UPDATE [dbo].[re_modifications] SET 
    [re_contract_id] = @re_contract_id
    ,[modified_field] = @modified_field
    ,[previous_value] = @previous_value
    ,[modified_value] = @modified_value
    ,[modification_time] = @modification_time
    ,[changed_by] = @changed_by
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, re_contract_id, modified_field, previous_value, modified_value, modification_time, changed_by ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[re_modifications]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class logging_statements:
        # table
        TableName = 'logging_statements'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[logging_statements]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        bdx_type = 'bdx_type'
        log_type = 'log_type'
        processing_step = 'processing_step'
        issue = 'issue'
        log_message = 'log_message'
        comment = 'comment'
        number_of_issues = 'number_of_issues'
        reference_value = 'reference_value'
        warning_time = 'warning_time'
        warned_user = 'warned_user'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, bdx_type: str, log_type: str, processing_step: str, ID_Arbeitsvorrat_MR_share: str = None, issue: str = None, log_message: str = None, comment: str = None, number_of_issues: int = None, reference_value: int = None, warning_time: datetime = None, warned_user: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@log_type nvarchar(100) = ?
    ,@processing_step nvarchar(100) = ?
    ,@issue nvarchar(400) = ?
    ,@log_message nvarchar(2048) = ?
    ,@comment nvarchar(2048) = ?
    ,@number_of_issues int = ?
    ,@reference_value int = ?
    ,@warning_time datetime = ?
    ,@warned_user nvarchar(400) = ?
;

INSERT INTO [dbo].[logging_statements] (
    [ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[bdx_type]
    ,[log_type]
    ,[processing_step]
    ,[issue]
    ,[log_message]
    ,[comment]
    ,[number_of_issues]
    ,[reference_value]
    ,[warning_time]
    ,[warned_user]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@bdx_type
    ,@log_type
    ,@processing_step
    ,@issue
    ,@log_message
    ,@comment
    ,@number_of_issues
    ,@reference_value
    ,@warning_time
    ,@warned_user
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, bdx_type, log_type, processing_step, issue, log_message, comment, number_of_issues, reference_value, warning_time, warned_user ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, bdx_type: str, log_type: str, processing_step: str, ID_Arbeitsvorrat_MR_share: str = None, issue: str = None, log_message: str = None, comment: str = None, number_of_issues: int = None, reference_value: int = None, warning_time: datetime = None, warned_user: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@log_type nvarchar(100) = ?
    ,@processing_step nvarchar(100) = ?
    ,@issue nvarchar(400) = ?
    ,@log_message nvarchar(2048) = ?
    ,@comment nvarchar(2048) = ?
    ,@number_of_issues int = ?
    ,@reference_value int = ?
    ,@warning_time datetime = ?
    ,@warned_user nvarchar(400) = ?
;

UPDATE [dbo].[logging_statements] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[bdx_type] = @bdx_type
    ,[log_type] = @log_type
    ,[processing_step] = @processing_step
    ,[issue] = @issue
    ,[log_message] = @log_message
    ,[comment] = @comment
    ,[number_of_issues] = @number_of_issues
    ,[reference_value] = @reference_value
    ,[warning_time] = @warning_time
    ,[warned_user] = @warned_user
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, bdx_type, log_type, processing_step, issue, log_message, comment, number_of_issues, reference_value, warning_time, warned_user ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[logging_statements]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class dnb_matching:
        # table
        TableName = 'dnb_matching'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[dnb_matching]'
        # columns
        company_id = 'company_id'
        dnb_id = 'dnb_id'
        api_score = 'api_score'
        match_settings = 'match_settings'
        matching_api = 'matching_api'
        version_of_match_algorithm = 'version_of_match_algorithm'
        string_match_score_internal_dnb_name = 'string_match_score_internal_dnb_name'
        string_match_score_internal_matchedvalue = 'string_match_score_internal_matchedvalue'
        matched_value = 'matched_value'
        matched_value_source = 'matched_value_source'
        CDT_score = 'CDT_score'
        best_match = 'best_match'
        manual_match = 'manual_match'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, company_id: int, dnb_id: str, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, manual_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@dnb_id nvarchar(100) = ?
    ,@api_score float = ?
    ,@match_settings nvarchar(100) = ?
    ,@matching_api nvarchar(100) = ?
    ,@version_of_match_algorithm nvarchar(400) = ?
    ,@string_match_score_internal_dnb_name float = ?
    ,@string_match_score_internal_matchedvalue float = ?
    ,@matched_value nvarchar(600) = ?
    ,@matched_value_source nvarchar(128) = ?
    ,@CDT_score float = ?
    ,@best_match bit = ?
    ,@manual_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[dnb_matching] (
    [company_id]
    ,[dnb_id]
    ,[api_score]
    ,[match_settings]
    ,[matching_api]
    ,[version_of_match_algorithm]
    ,[string_match_score_internal_dnb_name]
    ,[string_match_score_internal_matchedvalue]
    ,[matched_value]
    ,[matched_value_source]
    ,[CDT_score]
    ,[best_match]
    ,[manual_match]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @company_id
    ,@dnb_id
    ,@api_score
    ,@match_settings
    ,@matching_api
    ,@version_of_match_algorithm
    ,@string_match_score_internal_dnb_name
    ,@string_match_score_internal_matchedvalue
    ,@matched_value
    ,@matched_value_source
    ,@CDT_score
    ,@best_match
    ,@manual_match
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, manual_match, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, company_id: int, dnb_id: str, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, manual_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@dnb_id nvarchar(100) = ?
    ,@api_score float = ?
    ,@match_settings nvarchar(100) = ?
    ,@matching_api nvarchar(100) = ?
    ,@version_of_match_algorithm nvarchar(400) = ?
    ,@string_match_score_internal_dnb_name float = ?
    ,@string_match_score_internal_matchedvalue float = ?
    ,@matched_value nvarchar(600) = ?
    ,@matched_value_source nvarchar(128) = ?
    ,@CDT_score float = ?
    ,@best_match bit = ?
    ,@manual_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[dnb_matching] SET 
    [api_score] = @api_score
    ,[match_settings] = @match_settings
    ,[matching_api] = @matching_api
    ,[version_of_match_algorithm] = @version_of_match_algorithm
    ,[string_match_score_internal_dnb_name] = @string_match_score_internal_dnb_name
    ,[string_match_score_internal_matchedvalue] = @string_match_score_internal_matchedvalue
    ,[matched_value] = @matched_value
    ,[matched_value_source] = @matched_value_source
    ,[CDT_score] = @CDT_score
    ,[best_match] = @best_match
    ,[manual_match] = @manual_match
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [company_id] = @company_id
    ,[dnb_id] = @dnb_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, manual_match, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, company_id: int, dnb_id: str) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@dnb_id nvarchar(100) = ?
;

DELETE [dbo].[dnb_matching]
WHERE
    [company_id] = @company_id
    ,[dnb_id] = @dnb_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, dnb_id ]).exec_df()

    class exposure_columns:
        # table
        TableName = 'exposure_columns'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_columns]'
        # columns
        id = 'id'
        name = 'name'
        type = 'type'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, name: str, type: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@name varchar(128) = ?
    ,@type varchar(128) = ?
;

INSERT INTO [dbo].[exposure_columns] (
    [id]
    ,[name]
    ,[type]
)
VALUES (
    @id
    ,@name
    ,@type
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, name, type ]).exec_df()

        def update(self, id: int, name: str, type: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@name varchar(128) = ?
    ,@type varchar(128) = ?
;

UPDATE [dbo].[exposure_columns] SET 
    [name] = @name
    ,[type] = @type
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, name, type ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[exposure_columns]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class internal_policy_ids:
        # table
        TableName = 'internal_policy_ids'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[internal_policy_ids]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Internal_Policy_ID_1 = 'Internal_Policy_ID_1'
        Internal_Policy_ID_2 = 'Internal_Policy_ID_2'
        renewed_flag1 = 'renewed_flag1'
        renewed_flag2 = 'renewed_flag2'
        cancelled_flag1 = 'cancelled_flag1'
        cancelled_flag2 = 'cancelled_flag2'
        create_time = 'create_time'
        change_time = 'change_time'
        changed_by = 'changed_by'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str, Internal_Policy_ID_1: int = None, Internal_Policy_ID_2: int = None, renewed_flag1: int = None, renewed_flag2: int = None, cancelled_flag1: int = None, cancelled_flag2: int = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Internal_Policy_ID_1 bigint = ?
    ,@Internal_Policy_ID_2 bigint = ?
    ,@renewed_flag1 bit = ?
    ,@renewed_flag2 bit = ?
    ,@cancelled_flag1 bit = ?
    ,@cancelled_flag2 bit = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

INSERT INTO [dbo].[internal_policy_ids] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[Internal_Policy_ID_1]
    ,[Internal_Policy_ID_2]
    ,[renewed_flag1]
    ,[renewed_flag2]
    ,[cancelled_flag1]
    ,[cancelled_flag2]
    ,[create_time]
    ,[change_time]
    ,[changed_by]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@Internal_Policy_ID_1
    ,@Internal_Policy_ID_2
    ,@renewed_flag1
    ,@renewed_flag2
    ,@cancelled_flag1
    ,@cancelled_flag2
    ,@create_time
    ,@change_time
    ,@changed_by
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Internal_Policy_ID_1, Internal_Policy_ID_2, renewed_flag1, renewed_flag2, cancelled_flag1, cancelled_flag2, create_time, change_time, changed_by ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, Internal_Policy_ID_1: int = None, Internal_Policy_ID_2: int = None, renewed_flag1: int = None, renewed_flag2: int = None, cancelled_flag1: int = None, cancelled_flag2: int = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Internal_Policy_ID_1 bigint = ?
    ,@Internal_Policy_ID_2 bigint = ?
    ,@renewed_flag1 bit = ?
    ,@renewed_flag2 bit = ?
    ,@cancelled_flag1 bit = ?
    ,@cancelled_flag2 bit = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
;

UPDATE [dbo].[internal_policy_ids] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Internal_Policy_ID_1] = @Internal_Policy_ID_1
    ,[Internal_Policy_ID_2] = @Internal_Policy_ID_2
    ,[renewed_flag1] = @renewed_flag1
    ,[renewed_flag2] = @renewed_flag2
    ,[cancelled_flag1] = @cancelled_flag1
    ,[cancelled_flag2] = @cancelled_flag2
    ,[create_time] = @create_time
    ,[change_time] = @change_time
    ,[changed_by] = @changed_by
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Internal_Policy_ID_1, Internal_Policy_ID_2, renewed_flag1, renewed_flag2, cancelled_flag1, cancelled_flag2, create_time, change_time, changed_by ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[internal_policy_ids]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class exposure_bdx_copy:
        # table
        TableName = 'exposure_bdx_copy'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx_copy]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_RestorationData = 'Coverage_3_Sublimit_RestorationData'
        Coverage_4_Sublimit_Reputation = 'Coverage_4_Sublimit_Reputation'
        Coverage_5_Sublimit_Business_Interruption = 'Coverage_5_Sublimit_Business_Interruption'
        Coverage_6_Sublimit_CBI_IT_Service_Provider = 'Coverage_6_Sublimit_CBI_IT_Service_Provider'
        Coverage_7_Sublimit_CBI_Supply_Chain = 'Coverage_7_Sublimit_CBI_Supply_Chain'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        Coverage_15_Sublimit_System_Failure = 'Coverage_15_Sublimit_System_Failure'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Is_Special_Acceptance = 'Is_Special_Acceptance'
        Client_Limit_Occ_Orig_Curr = 'Client_Limit_Occ_Orig_Curr'
        Full_Limit_Occ_Orig_Curr = 'Full_Limit_Occ_Orig_Curr'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
;

INSERT INTO [dbo].[exposure_bdx_copy] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_RestorationData]
    ,[Coverage_4_Sublimit_Reputation]
    ,[Coverage_5_Sublimit_Business_Interruption]
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider]
    ,[Coverage_7_Sublimit_CBI_Supply_Chain]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[Coverage_15_Sublimit_System_Failure]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Is_Special_Acceptance]
    ,[Client_Limit_Occ_Orig_Curr]
    ,[Full_Limit_Occ_Orig_Curr]
    ,[Policy_ID_Cleaned]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_RestorationData
    ,@Coverage_4_Sublimit_Reputation
    ,@Coverage_5_Sublimit_Business_Interruption
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,@Coverage_7_Sublimit_CBI_Supply_Chain
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@Coverage_15_Sublimit_System_Failure
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@BvD_ID
    ,@Duplicate_ID
    ,@ID_Arbeitsvorrat_MR_share
    ,@Is_Special_Acceptance
    ,@Client_Limit_Occ_Orig_Curr
    ,@Full_Limit_Occ_Orig_Curr
    ,@Policy_ID_Cleaned
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
;

UPDATE [dbo].[exposure_bdx_copy] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_RestorationData] = @Coverage_3_Sublimit_RestorationData
    ,[Coverage_4_Sublimit_Reputation] = @Coverage_4_Sublimit_Reputation
    ,[Coverage_5_Sublimit_Business_Interruption] = @Coverage_5_Sublimit_Business_Interruption
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider] = @Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,[Coverage_7_Sublimit_CBI_Supply_Chain] = @Coverage_7_Sublimit_CBI_Supply_Chain
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[Coverage_15_Sublimit_System_Failure] = @Coverage_15_Sublimit_System_Failure
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Is_Special_Acceptance] = @Is_Special_Acceptance
    ,[Client_Limit_Occ_Orig_Curr] = @Client_Limit_Occ_Orig_Curr
    ,[Full_Limit_Occ_Orig_Curr] = @Full_Limit_Occ_Orig_Curr
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_bdx_copy]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_extended:
        # table
        TableName = 'exposure_extended'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_extended]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Turnover_Year_ClientInfo = 'Turnover_Year_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        PII_Records_Stored = 'PII_Records_Stored'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd = 'Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd'
        Coverage_4_Sublimit_RestorationData = 'Coverage_4_Sublimit_RestorationData'
        Coverage_5_Sublimit_Reputation = 'Coverage_5_Sublimit_Reputation'
        Coverage_6_Sublimit_Business_Interruption = 'Coverage_6_Sublimit_Business_Interruption'
        Coverage_7_Sublimit_Contingent_Business_Interruption = 'Coverage_7_Sublimit_Contingent_Business_Interruption'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        BvD_ID = 'BvD_ID'
        Risk_ID = 'Risk_ID'
        Tower_ID = 'Tower_ID'
        Duplicate_ID = 'Duplicate_ID'
        Insured_Name_BvD = 'Insured_Name_BvD'
        name_alias_bvd = 'name_alias_bvd'
        name_alias_source_bvd = 'name_alias_source_bvd'
        addr_internat_bvd = 'addr_internat_bvd'
        ambest_id_bvd = 'ambest_id_bvd'
        branch_count_bvd = 'branch_count_bvd'
        Trade_Level_CodeNumber_Mapped_Cambridge_BvD = 'Trade_Level_CodeNumber_Mapped_Cambridge_BvD'
        Trade_Level_Name_Mapped_Cambridge_BvD = 'Trade_Level_Name_Mapped_Cambridge_BvD'
        category_of_company_bvd = 'category_of_company_bvd'
        city_internat_bvd = 'city_internat_bvd'
        corporate_group_size_bvd = 'corporate_group_size_bvd'
        country_bvd = 'country_bvd'
        county_bvd = 'county_bvd'
        Country_ISO2_BvD = 'Country_ISO2_BvD'
        direct_parent_bvdid_bvd = 'direct_parent_bvdid_bvd'
        direct_parent_name_internat_bvd = 'direct_parent_name_internat_bvd'
        ein_bvd = 'ein_bvd'
        email_bvd = 'email_bvd'
        employees_bvd = 'employees_bvd'
        eurovat_bvd = 'eurovat_bvd'
        hierarchy_level_bvd = 'hierarchy_level_bvd'
        inactive_bvd = 'inactive_bvd'
        incorporation_date_bvd = 'incorporation_date_bvd'
        legalfrm_bvd = 'legalfrm_bvd'
        lei_lei_bvd = 'lei_lei_bvd'
        listed_bvd = 'listed_bvd'
        mainexch_bvd = 'mainexch_bvd'
        naicsccod2017_bvd = 'naicsccod2017_bvd'
        phone_bvd = 'phone_bvd'
        postcode_bvd = 'postcode_bvd'
        previous_names_set_array_bvd = 'previous_names_set_array_bvd'
        sd_isin_bvd = 'sd_isin_bvd'
        sd_ticker_bvd = 'sd_ticker_bvd'
        slegalf_bvd = 'slegalf_bvd'
        state_us_bvd = 'state_us_bvd'
        subs_count_bvd = 'subs_count_bvd'
        traderegisternr_bvd = 'traderegisternr_bvd'
        Turnover_EUR_BvD = 'Turnover_EUR_BvD'
        Turnover_USD_BvD = 'Turnover_USD_BvD'
        type_of_entity_bvd = 'type_of_entity_bvd'
        ultimate_parent_bvdid_bvd = 'ultimate_parent_bvdid_bvd'
        ultimate_parent_ctryiso_bvd = 'ultimate_parent_ctryiso_bvd'
        ultimate_parent_name_bvd = 'ultimate_parent_name_bvd'
        ussicccod_bvd = 'ussicccod_bvd'
        vatnumber_bvd = 'vatnumber_bvd'
        website_bvd = 'website_bvd'
        Trade_Level_CodeNumber_Mapped_Cambridge = 'Trade_Level_CodeNumber_Mapped_Cambridge'
        Trade_Level_Name_Mapped_Cambridge = 'Trade_Level_Name_Mapped_Cambridge'
        MR_Limit_USD = 'MR_Limit_USD'
        MR_Premium_USD = 'MR_Premium_USD'
        MR_GrossNet_Premium_USD = 'MR_GrossNet_Premium_USD'
        MR_Share = 'MR_Share'
        Attachment_Band = 'Attachment_Band'
        Client_Limit_Band = 'Client_Limit_Band'
        Full_Limit_Band = 'Full_Limit_Band'
        Premium_Band = 'Premium_Band'
        SIR_Band = 'SIR_Band'
        Turnover_Deviation_BvD = 'Turnover_Deviation_BvD'
        Turnover_Dev_as_perc_of_Client_Turnover = 'Turnover_Dev_as_perc_of_Client_Turnover'
        Abs_Dev_perc = 'Abs_Dev_perc'
        Abs_Dev_Perc_Bands = 'Abs_Dev_Perc_Bands'
        Turnover_USD_Combined = 'Turnover_USD_Combined'
        Company_Segment_Index = 'Company_Segment_Index'
        Country_Combined = 'Country_Combined'
        MR_Limit_Band = 'MR_Limit_Band'
        Policy_Duration_Day = 'Policy_Duration_Day'
        Policy_Duration_Month = 'Policy_Duration_Month'
        Policy_Expiry_Year = 'Policy_Expiry_Year'
        Policy_Inception_Month = 'Policy_Inception_Month'
        Policy_Inception_Year = 'Policy_Inception_Year'
        Policy_Inception_Year_Month = 'Policy_Inception_Year_Month'
        PrimaryOrExcess = 'PrimaryOrExcess'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str = None, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, Trade_Level_Name_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, MR_Share: float = None, Attachment_Band: int = None, Client_Limit_Band: int = None, Full_Limit_Band: int = None, Premium_Band: int = None, SIR_Band: int = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Abs_Dev_Perc_Bands: float = None, Turnover_USD_Combined: float = None, Company_Segment_Index: int = None, Country_Combined: str = None, MR_Limit_Band: int = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Policy_Expiry_Year: int = None, Policy_Inception_Month: int = None, Policy_Inception_Year: int = None, Policy_Inception_Year_Month: str = None, PrimaryOrExcess: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Risk_ID nvarchar(100) = ?
    ,@Tower_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nchar(4) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nchar(4) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nchar(4) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge nvarchar(2048) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@MR_Share float = ?
    ,@Attachment_Band tinyint = ?
    ,@Client_Limit_Band tinyint = ?
    ,@Full_Limit_Band tinyint = ?
    ,@Premium_Band tinyint = ?
    ,@SIR_Band tinyint = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Abs_Dev_Perc_Bands decimal(6, 1) = ?
    ,@Turnover_USD_Combined float = ?
    ,@Company_Segment_Index tinyint = ?
    ,@Country_Combined nchar(4) = ?
    ,@MR_Limit_Band tinyint = ?
    ,@Policy_Duration_Day smallint = ?
    ,@Policy_Duration_Month smallint = ?
    ,@Policy_Expiry_Year smallint = ?
    ,@Policy_Inception_Month tinyint = ?
    ,@Policy_Inception_Year smallint = ?
    ,@Policy_Inception_Year_Month nchar(14) = ?
    ,@PrimaryOrExcess nvarchar(14) = ?
;

INSERT INTO [dbo].[exposure_extended] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Turnover_Year_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[PII_Records_Stored]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Country_ISO2]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd]
    ,[Coverage_4_Sublimit_RestorationData]
    ,[Coverage_5_Sublimit_Reputation]
    ,[Coverage_6_Sublimit_Business_Interruption]
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[BvD_ID]
    ,[Risk_ID]
    ,[Tower_ID]
    ,[Duplicate_ID]
    ,[Insured_Name_BvD]
    ,[name_alias_bvd]
    ,[name_alias_source_bvd]
    ,[addr_internat_bvd]
    ,[ambest_id_bvd]
    ,[branch_count_bvd]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD]
    ,[Trade_Level_Name_Mapped_Cambridge_BvD]
    ,[category_of_company_bvd]
    ,[city_internat_bvd]
    ,[corporate_group_size_bvd]
    ,[country_bvd]
    ,[county_bvd]
    ,[Country_ISO2_BvD]
    ,[direct_parent_bvdid_bvd]
    ,[direct_parent_name_internat_bvd]
    ,[ein_bvd]
    ,[email_bvd]
    ,[employees_bvd]
    ,[eurovat_bvd]
    ,[hierarchy_level_bvd]
    ,[inactive_bvd]
    ,[incorporation_date_bvd]
    ,[legalfrm_bvd]
    ,[lei_lei_bvd]
    ,[listed_bvd]
    ,[mainexch_bvd]
    ,[naicsccod2017_bvd]
    ,[phone_bvd]
    ,[postcode_bvd]
    ,[previous_names_set_array_bvd]
    ,[sd_isin_bvd]
    ,[sd_ticker_bvd]
    ,[slegalf_bvd]
    ,[state_us_bvd]
    ,[subs_count_bvd]
    ,[traderegisternr_bvd]
    ,[Turnover_EUR_BvD]
    ,[Turnover_USD_BvD]
    ,[type_of_entity_bvd]
    ,[ultimate_parent_bvdid_bvd]
    ,[ultimate_parent_ctryiso_bvd]
    ,[ultimate_parent_name_bvd]
    ,[ussicccod_bvd]
    ,[vatnumber_bvd]
    ,[website_bvd]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge]
    ,[Trade_Level_Name_Mapped_Cambridge]
    ,[MR_Limit_USD]
    ,[MR_Premium_USD]
    ,[MR_GrossNet_Premium_USD]
    ,[MR_Share]
    ,[Attachment_Band]
    ,[Client_Limit_Band]
    ,[Full_Limit_Band]
    ,[Premium_Band]
    ,[SIR_Band]
    ,[Turnover_Deviation_BvD]
    ,[Turnover_Dev_as_perc_of_Client_Turnover]
    ,[Abs_Dev_perc]
    ,[Abs_Dev_Perc_Bands]
    ,[Turnover_USD_Combined]
    ,[Company_Segment_Index]
    ,[Country_Combined]
    ,[MR_Limit_Band]
    ,[Policy_Duration_Day]
    ,[Policy_Duration_Month]
    ,[Policy_Expiry_Year]
    ,[Policy_Inception_Month]
    ,[Policy_Inception_Year]
    ,[Policy_Inception_Year_Month]
    ,[PrimaryOrExcess]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Turnover_Year_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@PII_Records_Stored
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Country_ISO2
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,@Coverage_4_Sublimit_RestorationData
    ,@Coverage_5_Sublimit_Reputation
    ,@Coverage_6_Sublimit_Business_Interruption
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@BvD_ID
    ,@Risk_ID
    ,@Tower_ID
    ,@Duplicate_ID
    ,@Insured_Name_BvD
    ,@name_alias_bvd
    ,@name_alias_source_bvd
    ,@addr_internat_bvd
    ,@ambest_id_bvd
    ,@branch_count_bvd
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,@Trade_Level_Name_Mapped_Cambridge_BvD
    ,@category_of_company_bvd
    ,@city_internat_bvd
    ,@corporate_group_size_bvd
    ,@country_bvd
    ,@county_bvd
    ,@Country_ISO2_BvD
    ,@direct_parent_bvdid_bvd
    ,@direct_parent_name_internat_bvd
    ,@ein_bvd
    ,@email_bvd
    ,@employees_bvd
    ,@eurovat_bvd
    ,@hierarchy_level_bvd
    ,@inactive_bvd
    ,@incorporation_date_bvd
    ,@legalfrm_bvd
    ,@lei_lei_bvd
    ,@listed_bvd
    ,@mainexch_bvd
    ,@naicsccod2017_bvd
    ,@phone_bvd
    ,@postcode_bvd
    ,@previous_names_set_array_bvd
    ,@sd_isin_bvd
    ,@sd_ticker_bvd
    ,@slegalf_bvd
    ,@state_us_bvd
    ,@subs_count_bvd
    ,@traderegisternr_bvd
    ,@Turnover_EUR_BvD
    ,@Turnover_USD_BvD
    ,@type_of_entity_bvd
    ,@ultimate_parent_bvdid_bvd
    ,@ultimate_parent_ctryiso_bvd
    ,@ultimate_parent_name_bvd
    ,@ussicccod_bvd
    ,@vatnumber_bvd
    ,@website_bvd
    ,@Trade_Level_CodeNumber_Mapped_Cambridge
    ,@Trade_Level_Name_Mapped_Cambridge
    ,@MR_Limit_USD
    ,@MR_Premium_USD
    ,@MR_GrossNet_Premium_USD
    ,@MR_Share
    ,@Attachment_Band
    ,@Client_Limit_Band
    ,@Full_Limit_Band
    ,@Premium_Band
    ,@SIR_Band
    ,@Turnover_Deviation_BvD
    ,@Turnover_Dev_as_perc_of_Client_Turnover
    ,@Abs_Dev_perc
    ,@Abs_Dev_Perc_Bands
    ,@Turnover_USD_Combined
    ,@Company_Segment_Index
    ,@Country_Combined
    ,@MR_Limit_Band
    ,@Policy_Duration_Day
    ,@Policy_Duration_Month
    ,@Policy_Expiry_Year
    ,@Policy_Inception_Month
    ,@Policy_Inception_Year
    ,@Policy_Inception_Year_Month
    ,@PrimaryOrExcess
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Trade_Level_CodeNumber_Mapped_Cambridge, Trade_Level_Name_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, MR_Share, Attachment_Band, Client_Limit_Band, Full_Limit_Band, Premium_Band, SIR_Band, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Abs_Dev_Perc_Bands, Turnover_USD_Combined, Company_Segment_Index, Country_Combined, MR_Limit_Band, Policy_Duration_Day, Policy_Duration_Month, Policy_Expiry_Year, Policy_Inception_Month, Policy_Inception_Year, Policy_Inception_Year_Month, PrimaryOrExcess ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str = None, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, Trade_Level_Name_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, MR_Share: float = None, Attachment_Band: int = None, Client_Limit_Band: int = None, Full_Limit_Band: int = None, Premium_Band: int = None, SIR_Band: int = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Abs_Dev_Perc_Bands: float = None, Turnover_USD_Combined: float = None, Company_Segment_Index: int = None, Country_Combined: str = None, MR_Limit_Band: int = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Policy_Expiry_Year: int = None, Policy_Inception_Month: int = None, Policy_Inception_Year: int = None, Policy_Inception_Year_Month: str = None, PrimaryOrExcess: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Risk_ID nvarchar(100) = ?
    ,@Tower_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nchar(4) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nchar(4) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nchar(4) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge nvarchar(2048) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@MR_Share float = ?
    ,@Attachment_Band tinyint = ?
    ,@Client_Limit_Band tinyint = ?
    ,@Full_Limit_Band tinyint = ?
    ,@Premium_Band tinyint = ?
    ,@SIR_Band tinyint = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Abs_Dev_Perc_Bands decimal(6, 1) = ?
    ,@Turnover_USD_Combined float = ?
    ,@Company_Segment_Index tinyint = ?
    ,@Country_Combined nchar(4) = ?
    ,@MR_Limit_Band tinyint = ?
    ,@Policy_Duration_Day smallint = ?
    ,@Policy_Duration_Month smallint = ?
    ,@Policy_Expiry_Year smallint = ?
    ,@Policy_Inception_Month tinyint = ?
    ,@Policy_Inception_Year smallint = ?
    ,@Policy_Inception_Year_Month nchar(14) = ?
    ,@PrimaryOrExcess nvarchar(14) = ?
;

UPDATE [dbo].[exposure_extended] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Turnover_Year_ClientInfo] = @Turnover_Year_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[PII_Records_Stored] = @PII_Records_Stored
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd] = @Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,[Coverage_4_Sublimit_RestorationData] = @Coverage_4_Sublimit_RestorationData
    ,[Coverage_5_Sublimit_Reputation] = @Coverage_5_Sublimit_Reputation
    ,[Coverage_6_Sublimit_Business_Interruption] = @Coverage_6_Sublimit_Business_Interruption
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption] = @Coverage_7_Sublimit_Contingent_Business_Interruption
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[BvD_ID] = @BvD_ID
    ,[Risk_ID] = @Risk_ID
    ,[Tower_ID] = @Tower_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Insured_Name_BvD] = @Insured_Name_BvD
    ,[name_alias_bvd] = @name_alias_bvd
    ,[name_alias_source_bvd] = @name_alias_source_bvd
    ,[addr_internat_bvd] = @addr_internat_bvd
    ,[ambest_id_bvd] = @ambest_id_bvd
    ,[branch_count_bvd] = @branch_count_bvd
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD] = @Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,[Trade_Level_Name_Mapped_Cambridge_BvD] = @Trade_Level_Name_Mapped_Cambridge_BvD
    ,[category_of_company_bvd] = @category_of_company_bvd
    ,[city_internat_bvd] = @city_internat_bvd
    ,[corporate_group_size_bvd] = @corporate_group_size_bvd
    ,[country_bvd] = @country_bvd
    ,[county_bvd] = @county_bvd
    ,[Country_ISO2_BvD] = @Country_ISO2_BvD
    ,[direct_parent_bvdid_bvd] = @direct_parent_bvdid_bvd
    ,[direct_parent_name_internat_bvd] = @direct_parent_name_internat_bvd
    ,[ein_bvd] = @ein_bvd
    ,[email_bvd] = @email_bvd
    ,[employees_bvd] = @employees_bvd
    ,[eurovat_bvd] = @eurovat_bvd
    ,[hierarchy_level_bvd] = @hierarchy_level_bvd
    ,[inactive_bvd] = @inactive_bvd
    ,[incorporation_date_bvd] = @incorporation_date_bvd
    ,[legalfrm_bvd] = @legalfrm_bvd
    ,[lei_lei_bvd] = @lei_lei_bvd
    ,[listed_bvd] = @listed_bvd
    ,[mainexch_bvd] = @mainexch_bvd
    ,[naicsccod2017_bvd] = @naicsccod2017_bvd
    ,[phone_bvd] = @phone_bvd
    ,[postcode_bvd] = @postcode_bvd
    ,[previous_names_set_array_bvd] = @previous_names_set_array_bvd
    ,[sd_isin_bvd] = @sd_isin_bvd
    ,[sd_ticker_bvd] = @sd_ticker_bvd
    ,[slegalf_bvd] = @slegalf_bvd
    ,[state_us_bvd] = @state_us_bvd
    ,[subs_count_bvd] = @subs_count_bvd
    ,[traderegisternr_bvd] = @traderegisternr_bvd
    ,[Turnover_EUR_BvD] = @Turnover_EUR_BvD
    ,[Turnover_USD_BvD] = @Turnover_USD_BvD
    ,[type_of_entity_bvd] = @type_of_entity_bvd
    ,[ultimate_parent_bvdid_bvd] = @ultimate_parent_bvdid_bvd
    ,[ultimate_parent_ctryiso_bvd] = @ultimate_parent_ctryiso_bvd
    ,[ultimate_parent_name_bvd] = @ultimate_parent_name_bvd
    ,[ussicccod_bvd] = @ussicccod_bvd
    ,[vatnumber_bvd] = @vatnumber_bvd
    ,[website_bvd] = @website_bvd
    ,[Trade_Level_CodeNumber_Mapped_Cambridge] = @Trade_Level_CodeNumber_Mapped_Cambridge
    ,[Trade_Level_Name_Mapped_Cambridge] = @Trade_Level_Name_Mapped_Cambridge
    ,[MR_Limit_USD] = @MR_Limit_USD
    ,[MR_Premium_USD] = @MR_Premium_USD
    ,[MR_GrossNet_Premium_USD] = @MR_GrossNet_Premium_USD
    ,[MR_Share] = @MR_Share
    ,[Attachment_Band] = @Attachment_Band
    ,[Client_Limit_Band] = @Client_Limit_Band
    ,[Full_Limit_Band] = @Full_Limit_Band
    ,[Premium_Band] = @Premium_Band
    ,[SIR_Band] = @SIR_Band
    ,[Turnover_Deviation_BvD] = @Turnover_Deviation_BvD
    ,[Turnover_Dev_as_perc_of_Client_Turnover] = @Turnover_Dev_as_perc_of_Client_Turnover
    ,[Abs_Dev_perc] = @Abs_Dev_perc
    ,[Abs_Dev_Perc_Bands] = @Abs_Dev_Perc_Bands
    ,[Turnover_USD_Combined] = @Turnover_USD_Combined
    ,[Company_Segment_Index] = @Company_Segment_Index
    ,[Country_Combined] = @Country_Combined
    ,[MR_Limit_Band] = @MR_Limit_Band
    ,[Policy_Duration_Day] = @Policy_Duration_Day
    ,[Policy_Duration_Month] = @Policy_Duration_Month
    ,[Policy_Expiry_Year] = @Policy_Expiry_Year
    ,[Policy_Inception_Month] = @Policy_Inception_Month
    ,[Policy_Inception_Year] = @Policy_Inception_Year
    ,[Policy_Inception_Year_Month] = @Policy_Inception_Year_Month
    ,[PrimaryOrExcess] = @PrimaryOrExcess
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Trade_Level_CodeNumber_Mapped_Cambridge, Trade_Level_Name_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, MR_Share, Attachment_Band, Client_Limit_Band, Full_Limit_Band, Premium_Band, SIR_Band, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Abs_Dev_Perc_Bands, Turnover_USD_Combined, Company_Segment_Index, Country_Combined, MR_Limit_Band, Policy_Duration_Day, Policy_Duration_Month, Policy_Expiry_Year, Policy_Inception_Month, Policy_Inception_Year, Policy_Inception_Year_Month, PrimaryOrExcess ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_extended]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Client_Name_Level:
        # table
        TableName = 'Client_Name_Level'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Client_Name_Level]'
        # columns
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Purpose = 'Purpose'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Hierarchy_Level_Id: int, Purpose: str) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_Id int = ?
    ,@Purpose nvarchar(200) = ?
;

INSERT INTO [dbo].[Client_Name_Level] (
    [Hierarchy_Level_Id]
    ,[Purpose]
)
VALUES (
    @Hierarchy_Level_Id
    ,@Purpose
);
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_Id, Purpose ]).exec_df()

        def update(self, Hierarchy_Level_Id: int, Purpose: str) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_Id int = ?
    ,@Purpose nvarchar(200) = ?
;

UPDATE [dbo].[Client_Name_Level] SET 
    [Hierarchy_Level_Id] = @Hierarchy_Level_Id
    ,[Purpose] = @Purpose
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_Id, Purpose ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Client_Name_Level]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class issues_file_level_meta_information:
        # table
        TableName = 'issues_file_level_meta_information'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[issues_file_level_meta_information]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Column = 'Column'
        Value = 'Value'
        Criticality = 'Criticality'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(100) = ?
    ,@Value nvarchar(100) = ?
    ,@Criticality bigint = ?
    ,@Comment nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[issues_file_level_meta_information] (
    [ID_Arbeitsvorrat]
    ,[Column]
    ,[Value]
    ,[Criticality]
    ,[Comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Column
    ,@Value
    ,@Criticality
    ,@Comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(100) = ?
    ,@Value nvarchar(100) = ?
    ,@Criticality bigint = ?
    ,@Comment nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[issues_file_level_meta_information] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Column] = @Column
    ,[Value] = @Value
    ,[Criticality] = @Criticality
    ,[Comment] = @Comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[issues_file_level_meta_information]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Client_Name:
        # table
        TableName = 'Dim_Client_Name'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Client_Name]'
        # columns
        Client_ID = 'Client_ID'
        Client_Name = 'Client_Name'
        Hierarchy_Level_ID = 'Hierarchy_Level_ID'
        Parent_ID = 'Parent_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Client_Name: str, Hierarchy_Level_ID: int, Parent_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Client_Name nvarchar(400) = ?
    ,@Hierarchy_Level_ID bigint = ?
    ,@Parent_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Client_Name] (
    [Client_Name]
    ,[Hierarchy_Level_ID]
    ,[Parent_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Client_Name
    ,@Hierarchy_Level_ID
    ,@Parent_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Client_Name, Hierarchy_Level_ID, Parent_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Client_ID: int, Client_Name: str, Hierarchy_Level_ID: int, Parent_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Client_ID bigint = ?
    ,@Client_Name nvarchar(400) = ?
    ,@Hierarchy_Level_ID bigint = ?
    ,@Parent_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Client_Name] SET 
    [Client_Name] = @Client_Name
    ,[Hierarchy_Level_ID] = @Hierarchy_Level_ID
    ,[Parent_ID] = @Parent_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Client_ID] = @Client_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Client_ID, Client_Name, Hierarchy_Level_ID, Parent_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Client_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Client_ID bigint = ?
;

DELETE [dbo].[Dim_Client_Name]
WHERE
    [Client_ID] = @Client_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Client_ID ]).exec_df()

    class data_entry_meta_information_tbl:
        # table
        TableName = 'data_entry_meta_information_tbl'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[data_entry_meta_information_tbl]'
        # columns
        id = 'id'
        File_Name = 'File_Name'
        UW = 'UW'
        BU = 'BU'
        Number_Risks = 'Number_Risks'
        Number_Claims = 'Number_Claims'
        bdx_type = 'bdx_type'
        As_at_Date = 'As_at_Date'
        Subsystem = 'Subsystem'
        Program_IDs = 'Program_IDs'
        Additional_Program_IDs = 'Additional_Program_IDs'
        FSRI_ID = 'FSRI_ID'
        Client_Name = 'Client_Name'
        BuPa = 'BuPa'
        Treaty_Program_Name = 'Treaty_Program_Name'
        Begin_Date = 'Begin_Date'
        End_Date = 'End_Date'
        UW_Year = 'UW_Year'
        Is_From_CU_Collection = 'Is_From_CU_Collection'
        Is_Ignored = 'Is_Ignored'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[data_entry_meta_information_tbl] (
    [id]
    ,[File_Name]
    ,[UW]
    ,[BU]
    ,[Number_Risks]
    ,[Number_Claims]
    ,[bdx_type]
    ,[As_at_Date]
    ,[Subsystem]
    ,[Program_IDs]
    ,[Additional_Program_IDs]
    ,[FSRI_ID]
    ,[Client_Name]
    ,[BuPa]
    ,[Treaty_Program_Name]
    ,[Begin_Date]
    ,[End_Date]
    ,[UW_Year]
    ,[Is_From_CU_Collection]
    ,[Is_Ignored]
    ,[ID_Arbeitsvorrat]
    ,[Comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@File_Name
    ,@UW
    ,@BU
    ,@Number_Risks
    ,@Number_Claims
    ,@bdx_type
    ,@As_at_Date
    ,@Subsystem
    ,@Program_IDs
    ,@Additional_Program_IDs
    ,@FSRI_ID
    ,@Client_Name
    ,@BuPa
    ,@Treaty_Program_Name
    ,@Begin_Date
    ,@End_Date
    ,@UW_Year
    ,@Is_From_CU_Collection
    ,@Is_Ignored
    ,@ID_Arbeitsvorrat
    ,@Comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[data_entry_meta_information_tbl] SET 
    [File_Name] = @File_Name
    ,[UW] = @UW
    ,[BU] = @BU
    ,[Number_Risks] = @Number_Risks
    ,[Number_Claims] = @Number_Claims
    ,[bdx_type] = @bdx_type
    ,[As_at_Date] = @As_at_Date
    ,[Subsystem] = @Subsystem
    ,[Program_IDs] = @Program_IDs
    ,[Additional_Program_IDs] = @Additional_Program_IDs
    ,[FSRI_ID] = @FSRI_ID
    ,[Client_Name] = @Client_Name
    ,[BuPa] = @BuPa
    ,[Treaty_Program_Name] = @Treaty_Program_Name
    ,[Begin_Date] = @Begin_Date
    ,[End_Date] = @End_Date
    ,[UW_Year] = @UW_Year
    ,[Is_From_CU_Collection] = @Is_From_CU_Collection
    ,[Is_Ignored] = @Is_Ignored
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Comment] = @Comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[data_entry_meta_information_tbl]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Company_ClientInfo_save:
        # table
        TableName = 'Company_ClientInfo_save'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_ClientInfo_save]'
        # columns
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'
        Company_ID = 'Company_ID'
        Company_Name_Clean_ID = 'Company_Name_Clean_ID'
        Company_Name_ClientInfo = 'Company_Name_ClientInfo'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_ClientInfo_ID = 'City_ClientInfo_ID'
        Industry_ClientInfo_ID = 'Industry_ClientInfo_ID'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source = 'Source'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean_ID bigint = ?
    ,@Company_Name_ClientInfo nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_ClientInfo_ID bigint = ?
    ,@Industry_ClientInfo_ID bigint = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_ClientInfo_save] (
    [Company_ID]
    ,[Company_Name_Clean_ID]
    ,[Company_Name_ClientInfo]
    ,[Country_ISO2_ID]
    ,[City_ClientInfo_ID]
    ,[Industry_ClientInfo_ID]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_ID
    ,@Company_Name_Clean_ID
    ,@Company_Name_ClientInfo
    ,@Country_ISO2_ID
    ,@City_ClientInfo_ID
    ,@Industry_ClientInfo_ID
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Company_ClientInfo_ID: int, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ClientInfo_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Company_Name_Clean_ID bigint = ?
    ,@Company_Name_ClientInfo nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_ClientInfo_ID bigint = ?
    ,@Industry_ClientInfo_ID bigint = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_ClientInfo_save] SET 
    [Company_ClientInfo_ID] = @Company_ClientInfo_ID
    ,[Company_ID] = @Company_ID
    ,[Company_Name_Clean_ID] = @Company_Name_Clean_ID
    ,[Company_Name_ClientInfo] = @Company_Name_ClientInfo
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_ClientInfo_ID] = @City_ClientInfo_ID
    ,[Industry_ClientInfo_ID] = @Industry_ClientInfo_ID
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source] = @Source
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ClientInfo_ID, Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Company_ClientInfo_save]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_State:
        # table
        TableName = 'Dim_State'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_State]'
        # columns
        State_ID = 'State_ID'
        State = 'State'
        State_Code = 'State_Code'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, State: str, State_Code: str, Country_ISO2_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @State nvarchar(400) = ?
    ,@State_Code nvarchar(10) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_State] (
    [State]
    ,[State_Code]
    ,[Country_ISO2_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @State
    ,@State_Code
    ,@Country_ISO2_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ State, State_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, State_ID: int, State: str, State_Code: str, Country_ISO2_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @State_ID bigint = ?
    ,@State nvarchar(400) = ?
    ,@State_Code nvarchar(10) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_State] SET 
    [State] = @State
    ,[State_Code] = @State_Code
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [State_ID] = @State_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ State_ID, State, State_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, State_ID: int) -> DataFrame:
            sql = """
DECLARE
    @State_ID bigint = ?
;

DELETE [dbo].[Dim_State]
WHERE
    [State_ID] = @State_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ State_ID ]).exec_df()

    class Company_save:
        # table
        TableName = 'Company_save'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_save]'
        # columns
        Company_ID = 'Company_ID'
        Company_Name_Clean = 'Company_Name_Clean'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Industry_ID = 'Industry_ID'
        Company_Name = 'Company_Name'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Parent_Company_ID = 'Parent_Company_ID'
        Ultimate_Company_ID = 'Ultimate_Company_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Name_Clean nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(2048) = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_save] (
    [Company_Name_Clean]
    ,[Country_ISO2_ID]
    ,[City_Unified_Name_ID]
    ,[Industry_ID]
    ,[Company_Name]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Parent_Company_ID]
    ,[Ultimate_Company_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_Name_Clean
    ,@Country_ISO2_ID
    ,@City_Unified_Name_ID
    ,@Industry_ID
    ,@Company_Name
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Parent_Company_ID
    ,@Ultimate_Company_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(2048) = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_save] SET 
    [Company_ID] = @Company_ID
    ,[Company_Name_Clean] = @Company_Name_Clean
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_Unified_Name_ID] = @City_Unified_Name_ID
    ,[Industry_ID] = @Industry_ID
    ,[Company_Name] = @Company_Name
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Parent_Company_ID] = @Parent_Company_ID
    ,[Ultimate_Company_ID] = @Ultimate_Company_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Company_save]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_bdx_copy:
        # table
        TableName = 'claims_bdx_copy'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx_copy]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Claim_ID_ClientInfo = 'Claim_ID_ClientInfo'
        Is_Claim_Closed = 'Is_Claim_Closed'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Claims_Description = 'Claims_Description'
        Type_of_Loss = 'Type_of_Loss'
        Country_of_Loss_Settlement = 'Country_of_Loss_Settlement'
        Value_as_Of_Date = 'Value_as_Of_Date'
        Loss_Currency = 'Loss_Currency'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        Threshold_Orig_Curr_unind = 'Threshold_Orig_Curr_unind'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        IsCensored = 'IsCensored'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Incurred_Insured_FGU_USD = 'Incurred_Insured_FGU_USD'
        Paid_Client_Share_USD = 'Paid_Client_Share_USD'
        Incurred_Client_Share_USD = 'Incurred_Client_Share_USD'
        Threshold_unind_USD = 'Threshold_unind_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Country_of_Loss_Settlement_ISO2 = 'Country_of_Loss_Settlement_ISO2'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        Internal_Claim_ID = 'Internal_Claim_ID'
        Is_Signal_Reserve = 'Is_Signal_Reserve'
        Loss_Event_ClientInfo = 'Loss_Event_ClientInfo'
        Loss_Event_normalized = 'Loss_Event_normalized'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Claim_Closed_Date = 'Claim_Closed_Date'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event_normalized nvarchar(2048) = ?
    ,@Product_Name_ClientInfo nvarchar(2048) = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
;

INSERT INTO [dbo].[claims_bdx_copy] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Claim_ID_ClientInfo]
    ,[Is_Claim_Closed]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Claims_Description]
    ,[Type_of_Loss]
    ,[Country_of_Loss_Settlement]
    ,[Value_as_Of_Date]
    ,[Loss_Currency]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[Threshold_Orig_Curr_unind]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[IsCensored]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Incurred_Insured_FGU_USD]
    ,[Paid_Client_Share_USD]
    ,[Incurred_Client_Share_USD]
    ,[Threshold_unind_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Country_of_Loss_Settlement_ISO2]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[Internal_Claim_ID]
    ,[Is_Signal_Reserve]
    ,[Loss_Event_ClientInfo]
    ,[Loss_Event_normalized]
    ,[Product_Name_ClientInfo]
    ,[Claim_Closed_Date]
    ,[Policy_ID_Cleaned]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Claim_ID_ClientInfo
    ,@Is_Claim_Closed
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Claims_Description
    ,@Type_of_Loss
    ,@Country_of_Loss_Settlement
    ,@Value_as_Of_Date
    ,@Loss_Currency
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@Threshold_Orig_Curr_unind
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@IsCensored
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Incurred_Insured_FGU_USD
    ,@Paid_Client_Share_USD
    ,@Incurred_Client_Share_USD
    ,@Threshold_unind_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Country_of_Loss_Settlement_ISO2
    ,@BvD_ID
    ,@Duplicate_ID
    ,@Internal_Claim_ID
    ,@Is_Signal_Reserve
    ,@Loss_Event_ClientInfo
    ,@Loss_Event_normalized
    ,@Product_Name_ClientInfo
    ,@Claim_Closed_Date
    ,@Policy_ID_Cleaned
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event_normalized nvarchar(2048) = ?
    ,@Product_Name_ClientInfo nvarchar(2048) = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
;

UPDATE [dbo].[claims_bdx_copy] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Claim_ID_ClientInfo] = @Claim_ID_ClientInfo
    ,[Is_Claim_Closed] = @Is_Claim_Closed
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Claims_Description] = @Claims_Description
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Country_of_Loss_Settlement] = @Country_of_Loss_Settlement
    ,[Value_as_Of_Date] = @Value_as_Of_Date
    ,[Loss_Currency] = @Loss_Currency
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[Threshold_Orig_Curr_unind] = @Threshold_Orig_Curr_unind
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[IsCensored] = @IsCensored
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Incurred_Insured_FGU_USD] = @Incurred_Insured_FGU_USD
    ,[Paid_Client_Share_USD] = @Paid_Client_Share_USD
    ,[Incurred_Client_Share_USD] = @Incurred_Client_Share_USD
    ,[Threshold_unind_USD] = @Threshold_unind_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Country_of_Loss_Settlement_ISO2] = @Country_of_Loss_Settlement_ISO2
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Internal_Claim_ID] = @Internal_Claim_ID
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
    ,[Loss_Event_ClientInfo] = @Loss_Event_ClientInfo
    ,[Loss_Event_normalized] = @Loss_Event_normalized
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Claim_Closed_Date] = @Claim_Closed_Date
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_bdx_copy]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Company_History_save:
        # table
        TableName = 'Company_History_save'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_History_save]'
        # columns
        id = 'id'
        Company_ID = 'Company_ID'
        Company_Name_Clean = 'Company_Name_Clean'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Industry_ID = 'Industry_ID'
        Company_Name = 'Company_Name'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Parent_Company_ID = 'Parent_Company_ID'
        Ultimate_Company_ID = 'Ultimate_Company_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(2048) = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_History_save] (
    [Company_ID]
    ,[Company_Name_Clean]
    ,[Country_ISO2_ID]
    ,[City_Unified_Name_ID]
    ,[Industry_ID]
    ,[Company_Name]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Parent_Company_ID]
    ,[Ultimate_Company_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_ID
    ,@Company_Name_Clean
    ,@Country_ISO2_ID
    ,@City_Unified_Name_ID
    ,@Industry_ID
    ,@Company_Name
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Parent_Company_ID
    ,@Ultimate_Company_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(2048) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(2048) = ?
    ,@Street nvarchar(1000) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(1000) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_History_save] SET 
    [id] = @id
    ,[Company_ID] = @Company_ID
    ,[Company_Name_Clean] = @Company_Name_Clean
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_Unified_Name_ID] = @City_Unified_Name_ID
    ,[Industry_ID] = @Industry_ID
    ,[Company_Name] = @Company_Name
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Parent_Company_ID] = @Parent_Company_ID
    ,[Ultimate_Company_ID] = @Ultimate_Company_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Company_History_save]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class file_level_meta_information_tbl:
        # table
        TableName = 'file_level_meta_information_tbl'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_meta_information_tbl]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        Subsystem_ID = 'Subsystem_ID'
        Treaty_Programm_ID = 'Treaty_Programm_ID'
        FSRI_ID = 'FSRI_ID'
        UW_Year = 'UW_Year'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        Priority = 'Priority'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[file_level_meta_information_tbl] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[Subsystem_ID]
    ,[Treaty_Programm_ID]
    ,[FSRI_ID]
    ,[UW_Year]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[Priority]
    ,[Deadline]
    ,[Exclude_from_dashboards]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@Subsystem_ID
    ,@Treaty_Programm_ID
    ,@FSRI_ID
    ,@UW_Year
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@Priority
    ,@Deadline
    ,@Exclude_from_dashboards
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[file_level_meta_information_tbl] SET 
    [BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[Subsystem_ID] = @Subsystem_ID
    ,[Treaty_Programm_ID] = @Treaty_Programm_ID
    ,[FSRI_ID] = @FSRI_ID
    ,[UW_Year] = @UW_Year
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[Priority] = @Priority
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, ID_Arbeitsvorrat: str) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
;

DELETE [dbo].[file_level_meta_information_tbl]
WHERE
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat ]).exec_df()

    class dnb_matching_save:
        # table
        TableName = 'dnb_matching_save'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[dnb_matching_save]'
        # columns
        company_id = 'company_id'
        dnb_id = 'dnb_id'
        api_score = 'api_score'
        match_settings = 'match_settings'
        matching_api = 'matching_api'
        version_of_match_algorithm = 'version_of_match_algorithm'
        string_match_score_internal_dnb_name = 'string_match_score_internal_dnb_name'
        string_match_score_internal_matchedvalue = 'string_match_score_internal_matchedvalue'
        matched_value = 'matched_value'
        matched_value_source = 'matched_value_source'
        CDT_score = 'CDT_score'
        best_match = 'best_match'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        manual_match = 'manual_match'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, company_id: int, dnb_id: str, manual_match: int, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@dnb_id nvarchar(100) = ?
    ,@api_score float = ?
    ,@match_settings nvarchar(100) = ?
    ,@matching_api nvarchar(100) = ?
    ,@version_of_match_algorithm nvarchar(400) = ?
    ,@string_match_score_internal_dnb_name float = ?
    ,@string_match_score_internal_matchedvalue float = ?
    ,@matched_value nvarchar(2048) = ?
    ,@matched_value_source nvarchar(128) = ?
    ,@CDT_score float = ?
    ,@best_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@manual_match bit = ?
;

INSERT INTO [dbo].[dnb_matching_save] (
    [company_id]
    ,[dnb_id]
    ,[api_score]
    ,[match_settings]
    ,[matching_api]
    ,[version_of_match_algorithm]
    ,[string_match_score_internal_dnb_name]
    ,[string_match_score_internal_matchedvalue]
    ,[matched_value]
    ,[matched_value_source]
    ,[CDT_score]
    ,[best_match]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[manual_match]
)
VALUES (
    @company_id
    ,@dnb_id
    ,@api_score
    ,@match_settings
    ,@matching_api
    ,@version_of_match_algorithm
    ,@string_match_score_internal_dnb_name
    ,@string_match_score_internal_matchedvalue
    ,@matched_value
    ,@matched_value_source
    ,@CDT_score
    ,@best_match
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@manual_match
);
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, Create_Time, Change_Time, Changed_By, manual_match ]).exec_df()

        def update(self, company_id: int, dnb_id: str, manual_match: int, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@dnb_id nvarchar(100) = ?
    ,@api_score float = ?
    ,@match_settings nvarchar(100) = ?
    ,@matching_api nvarchar(100) = ?
    ,@version_of_match_algorithm nvarchar(400) = ?
    ,@string_match_score_internal_dnb_name float = ?
    ,@string_match_score_internal_matchedvalue float = ?
    ,@matched_value nvarchar(2048) = ?
    ,@matched_value_source nvarchar(128) = ?
    ,@CDT_score float = ?
    ,@best_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@manual_match bit = ?
;

UPDATE [dbo].[dnb_matching_save] SET 
    [company_id] = @company_id
    ,[dnb_id] = @dnb_id
    ,[api_score] = @api_score
    ,[match_settings] = @match_settings
    ,[matching_api] = @matching_api
    ,[version_of_match_algorithm] = @version_of_match_algorithm
    ,[string_match_score_internal_dnb_name] = @string_match_score_internal_dnb_name
    ,[string_match_score_internal_matchedvalue] = @string_match_score_internal_matchedvalue
    ,[matched_value] = @matched_value
    ,[matched_value_source] = @matched_value_source
    ,[CDT_score] = @CDT_score
    ,[best_match] = @best_match
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[manual_match] = @manual_match
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, Create_Time, Change_Time, Changed_By, manual_match ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[dnb_matching_save]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_bdx_claims_linking:
        # table
        TableName = 'claims_bdx_claims_linking'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx_claims_linking]'
        # columns
        id = 'id'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[claims_bdx_claims_linking] (
    [id]
    ,[Policy_ID_Cleaned]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@Policy_ID_Cleaned
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[claims_bdx_claims_linking] SET 
    [id] = @id
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_bdx_claims_linking]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Company_Name_Clean:
        # table
        TableName = 'Dim_Company_Name_Clean'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Company_Name_Clean]'
        # columns
        Company_Name_Clean_ID = 'Company_Name_Clean_ID'
        Company_Name_ClientInfo = 'Company_Name_ClientInfo'
        Company_Name_Clean = 'Company_Name_Clean'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_Name_ClientInfo: str, Company_Name_Clean: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Name_ClientInfo nvarchar(600) = ?
    ,@Company_Name_Clean nvarchar(600) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Company_Name_Clean] (
    [Company_Name_ClientInfo]
    ,[Company_Name_Clean]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_Name_ClientInfo
    ,@Company_Name_Clean
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Name_ClientInfo, Company_Name_Clean, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Company_Name_Clean_ID: int, Company_Name_ClientInfo: str, Company_Name_Clean: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Name_Clean_ID bigint = ?
    ,@Company_Name_ClientInfo nvarchar(600) = ?
    ,@Company_Name_Clean nvarchar(600) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Company_Name_Clean] SET 
    [Company_Name_ClientInfo] = @Company_Name_ClientInfo
    ,[Company_Name_Clean] = @Company_Name_Clean
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Company_Name_Clean_ID] = @Company_Name_Clean_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Name_Clean_ID, Company_Name_ClientInfo, Company_Name_Clean, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Company_Name_Clean_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Company_Name_Clean_ID bigint = ?
;

DELETE [dbo].[Dim_Company_Name_Clean]
WHERE
    [Company_Name_Clean_ID] = @Company_Name_Clean_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Name_Clean_ID ]).exec_df()

    class Process_Client_Name:
        # table
        TableName = 'Process_Client_Name'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Process_Client_Name]'
        # columns
        Process_ID = 'Process_ID'
        Business_Client_ID = 'Business_Client_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Process_ID: int, Business_Client_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Process_ID bigint = ?
    ,@Business_Client_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Process_Client_Name] (
    [Process_ID]
    ,[Business_Client_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Process_ID
    ,@Business_Client_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ Process_ID, Business_Client_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Process_ID: int, Business_Client_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Process_ID bigint = ?
    ,@Business_Client_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Process_Client_Name] SET 
    [Business_Client_ID] = @Business_Client_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Process_ID] = @Process_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Process_ID, Business_Client_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Process_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Process_ID bigint = ?
;

DELETE [dbo].[Process_Client_Name]
WHERE
    [Process_ID] = @Process_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Process_ID ]).exec_df()

    class naics_codes:
        # table
        TableName = 'naics_codes'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[naics_codes]'
        # columns
        code = 'code'
        title = 'title'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, code: str, title: str) -> DataFrame:
            sql = """
DECLARE
    @code nvarchar(20) = ?
    ,@title nvarchar(400) = ?
;

INSERT INTO [dbo].[naics_codes] (
    [code]
    ,[title]
)
VALUES (
    @code
    ,@title
);
"""
            return DbCmd(self.cnOrStr, sql, [ code, title ]).exec_df()

        def update(self, code: str, title: str) -> DataFrame:
            sql = """
DECLARE
    @code nvarchar(20) = ?
    ,@title nvarchar(400) = ?
;

UPDATE [dbo].[naics_codes] SET 
    [code] = @code
    ,[title] = @title
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ code, title ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[naics_codes]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Client_Name_Hierarchy_Level:
        # table
        TableName = 'Dim_Client_Name_Hierarchy_Level'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Client_Name_Hierarchy_Level]'
        # columns
        Hierarchy_Level_ID = 'Hierarchy_Level_ID'
        Purpose = 'Purpose'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Purpose: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Purpose nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Client_Name_Hierarchy_Level] (
    [Purpose]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Purpose
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Purpose, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Hierarchy_Level_ID: int, Purpose: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_ID bigint = ?
    ,@Purpose nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Client_Name_Hierarchy_Level] SET 
    [Purpose] = @Purpose
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Hierarchy_Level_ID] = @Hierarchy_Level_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_ID, Purpose, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Hierarchy_Level_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_ID bigint = ?
;

DELETE [dbo].[Dim_Client_Name_Hierarchy_Level]
WHERE
    [Hierarchy_Level_ID] = @Hierarchy_Level_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_ID ]).exec_df()

    class exposure_bdx_claims_linking:
        # table
        TableName = 'exposure_bdx_claims_linking'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx_claims_linking]'
        # columns
        id = 'id'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[exposure_bdx_claims_linking] (
    [id]
    ,[Policy_ID_Cleaned]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@Policy_ID_Cleaned
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[exposure_bdx_claims_linking] SET 
    [id] = @id
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_bdx_claims_linking]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_FXRates:
        # table
        TableName = 'Dim_FXRates'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_FXRates]'
        # columns
        fx_ID = 'fx_ID'
        from_currency_ID = 'from_currency_ID'
        to_currency_ID = 'to_currency_ID'
        valid_from_year = 'valid_from_year'
        from_currency = 'from_currency'
        to_currency = 'to_currency'
        exch_rate = 'exch_rate'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, from_currency_ID: int, to_currency_ID: int, valid_from_year: int, from_currency: str, to_currency: str, exch_rate: float, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @from_currency_ID bigint = ?
    ,@to_currency_ID bigint = ?
    ,@valid_from_year int = ?
    ,@from_currency nvarchar(6) = ?
    ,@to_currency nvarchar(6) = ?
    ,@exch_rate float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_FXRates] (
    [from_currency_ID]
    ,[to_currency_ID]
    ,[valid_from_year]
    ,[from_currency]
    ,[to_currency]
    ,[exch_rate]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @from_currency_ID
    ,@to_currency_ID
    ,@valid_from_year
    ,@from_currency
    ,@to_currency
    ,@exch_rate
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ from_currency_ID, to_currency_ID, valid_from_year, from_currency, to_currency, exch_rate, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, fx_ID: int, from_currency_ID: int, to_currency_ID: int, valid_from_year: int, from_currency: str, to_currency: str, exch_rate: float, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @fx_ID bigint = ?
    ,@from_currency_ID bigint = ?
    ,@to_currency_ID bigint = ?
    ,@valid_from_year int = ?
    ,@from_currency nvarchar(6) = ?
    ,@to_currency nvarchar(6) = ?
    ,@exch_rate float = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_FXRates] SET 
    [from_currency_ID] = @from_currency_ID
    ,[to_currency_ID] = @to_currency_ID
    ,[valid_from_year] = @valid_from_year
    ,[from_currency] = @from_currency
    ,[to_currency] = @to_currency
    ,[exch_rate] = @exch_rate
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [fx_ID] = @fx_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ fx_ID, from_currency_ID, to_currency_ID, valid_from_year, from_currency, to_currency, exch_rate, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, fx_ID: int) -> DataFrame:
            sql = """
DECLARE
    @fx_ID bigint = ?
;

DELETE [dbo].[Dim_FXRates]
WHERE
    [fx_ID] = @fx_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ fx_ID ]).exec_df()

    class validationResults_exposure:
        # table
        TableName = 'validationResults_exposure'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[validationResults_exposure]'
        # columns
        id = 'id'
        ref_id = 'ref_id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        RuleName = 'RuleName'
        Issue = 'Issue'
        Column = 'Column'
        Column2 = 'Column2'
        Column3 = 'Column3'
        rowNr = 'rowNr'
        File = 'File'
        Value = 'Value'
        Value2 = 'Value2'
        Value3 = 'Value3'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ref_id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@RuleName nvarchar(2048) = ?
    ,@Issue nvarchar(2048) = ?
    ,@Column nvarchar(400) = ?
    ,@Column2 nvarchar(400) = ?
    ,@Column3 nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@File nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
    ,@Value2 nvarchar(2048) = ?
    ,@Value3 nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[validationResults_exposure] (
    [ref_id]
    ,[ID_Arbeitsvorrat]
    ,[RuleName]
    ,[Issue]
    ,[Column]
    ,[Column2]
    ,[Column3]
    ,[rowNr]
    ,[File]
    ,[Value]
    ,[Value2]
    ,[Value3]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ref_id
    ,@ID_Arbeitsvorrat
    ,@RuleName
    ,@Issue
    ,@Column
    ,@Column2
    ,@Column3
    ,@rowNr
    ,@File
    ,@Value
    ,@Value2
    ,@Value3
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ref_id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@RuleName nvarchar(2048) = ?
    ,@Issue nvarchar(2048) = ?
    ,@Column nvarchar(400) = ?
    ,@Column2 nvarchar(400) = ?
    ,@Column3 nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@File nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
    ,@Value2 nvarchar(2048) = ?
    ,@Value3 nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[validationResults_exposure] SET 
    [ref_id] = @ref_id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[RuleName] = @RuleName
    ,[Issue] = @Issue
    ,[Column] = @Column
    ,[Column2] = @Column2
    ,[Column3] = @Column3
    ,[rowNr] = @rowNr
    ,[File] = @File
    ,[Value] = @Value
    ,[Value2] = @Value2
    ,[Value3] = @Value3
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[validationResults_exposure]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class claims_linking_info:
        # table
        TableName = 'claims_linking_info'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_linking_info]'
        # columns
        id = 'id'
        score = 'score'
        type_of_match = 'type_of_match'
        comment = 'comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, type_of_match: str, score: float = None, comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@score float = ?
    ,@type_of_match nvarchar(400) = ?
    ,@comment nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[claims_linking_info] (
    [id]
    ,[score]
    ,[type_of_match]
    ,[comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@score
    ,@type_of_match
    ,@comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, score, type_of_match, comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, type_of_match: str, score: float = None, comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@score float = ?
    ,@type_of_match nvarchar(400) = ?
    ,@comment nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[claims_linking_info] SET 
    [id] = @id
    ,[score] = @score
    ,[type_of_match] = @type_of_match
    ,[comment] = @comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, score, type_of_match, comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_linking_info]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class tFactClaims:
        # table
        TableName = 'tFactClaims'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tFactClaims]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        PolicyInceptionDateKey = 'PolicyInceptionDateKey'
        Policy_Inception_Date = 'Policy_Inception_Date'
        PolicyExpiryDateKey = 'PolicyExpiryDateKey'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        FSRI_Treaty = 'FSRI_Treaty'
        FSRI_Section = 'FSRI_Section'
        FSRI_Period = 'FSRI_Period'
        Claim_ID_ClientInfo = 'Claim_ID_ClientInfo'
        Is_Claim_Closed = 'Is_Claim_Closed'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        LossDateKey = 'LossDateKey'
        Date_of_Loss = 'Date_of_Loss'
        Claims_Description = 'Claims_Description'
        Type_of_Loss = 'Type_of_Loss'
        Country_of_Loss_Settlement = 'Country_of_Loss_Settlement'
        Nr_Affected_Records = 'Nr_Affected_Records'
        Duration_of_Outage_hours = 'Duration_of_Outage_hours'
        Value_as_Of_Date = 'Value_as_Of_Date'
        Loss_Currency = 'Loss_Currency'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        Threshold_Orig_Curr_unind = 'Threshold_Orig_Curr_unind'
        Observation_Period_Start = 'Observation_Period_Start'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        IsCensored = 'IsCensored'
        ClientLimitBandKey = 'ClientLimitBandKey'
        Client_Limit_USD = 'Client_Limit_USD'
        FullLimitBandKey = 'FullLimitBandKey'
        Full_Limit_USD = 'Full_Limit_USD'
        AttachmentBandKey = 'AttachmentBandKey'
        Attachment_USD = 'Attachment_USD'
        SIRBandsKey = 'SIRBandsKey'
        SIR_USD = 'SIR_USD'
        Incurred_Insured_FGU_USD = 'Incurred_Insured_FGU_USD'
        Paid_Client_Share_USD = 'Paid_Client_Share_USD'
        Paid_Client_Share_USD_Combined = 'Paid_Client_Share_USD_Combined'
        ClaimsIncurredBandsKey = 'ClaimsIncurredBandsKey'
        Incurred_Client_Share_USD = 'Incurred_Client_Share_USD'
        ClaimsIncurredCombinedBandsKey = 'ClaimsIncurredCombinedBandsKey'
        Incurred_Client_Share_USD_Combined = 'Incurred_Client_Share_USD_Combined'
        ClaimsThresholdCategoriesKey = 'ClaimsThresholdCategoriesKey'
        Threshold_unind_USD = 'Threshold_unind_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        CountryKey = 'CountryKey'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Country_of_Loss_Settlement_ISO2 = 'Country_of_Loss_Settlement_ISO2'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        Internal_Claim_ID = 'Internal_Claim_ID'
        Country_Combined = 'Country_Combined'
        FileMetaInfoKey = 'FileMetaInfoKey'
        Insured_Name_BvD = 'Insured_Name_BvD'
        name_alias_bvd = 'name_alias_bvd'
        name_alias_source_bvd = 'name_alias_source_bvd'
        addr_internat_bvd = 'addr_internat_bvd'
        ambest_id_bvd = 'ambest_id_bvd'
        branch_count_bvd = 'branch_count_bvd'
        Trade_Level_CodeNumber_Mapped_Cambridge_BvD = 'Trade_Level_CodeNumber_Mapped_Cambridge_BvD'
        Trade_Level_Name_Mapped_Cambridge_BvD = 'Trade_Level_Name_Mapped_Cambridge_BvD'
        category_of_company_bvd = 'category_of_company_bvd'
        city_internat_bvd = 'city_internat_bvd'
        corporate_group_size_bvd = 'corporate_group_size_bvd'
        country_bvd = 'country_bvd'
        county_bvd = 'county_bvd'
        Country_ISO2_BvD = 'Country_ISO2_BvD'
        direct_parent_bvdid_bvd = 'direct_parent_bvdid_bvd'
        direct_parent_name_internat_bvd = 'direct_parent_name_internat_bvd'
        ein_bvd = 'ein_bvd'
        email_bvd = 'email_bvd'
        employees_bvd = 'employees_bvd'
        eurovat_bvd = 'eurovat_bvd'
        hierarchy_level_bvd = 'hierarchy_level_bvd'
        inactive_bvd = 'inactive_bvd'
        incorporation_date_bvd = 'incorporation_date_bvd'
        legalfrm_bvd = 'legalfrm_bvd'
        lei_lei_bvd = 'lei_lei_bvd'
        listed_bvd = 'listed_bvd'
        mainexch_bvd = 'mainexch_bvd'
        naicsccod2017_bvd = 'naicsccod2017_bvd'
        phone_bvd = 'phone_bvd'
        postcode_bvd = 'postcode_bvd'
        previous_names_set_array_bvd = 'previous_names_set_array_bvd'
        sd_isin_bvd = 'sd_isin_bvd'
        sd_ticker_bvd = 'sd_ticker_bvd'
        slegalf_bvd = 'slegalf_bvd'
        state_us_bvd = 'state_us_bvd'
        subs_count_bvd = 'subs_count_bvd'
        traderegisternr_bvd = 'traderegisternr_bvd'
        Turnover_EUR_BvD = 'Turnover_EUR_BvD'
        CompanySegmentKey = 'CompanySegmentKey'
        Turnover_USD_BvD = 'Turnover_USD_BvD'
        type_of_entity_bvd = 'type_of_entity_bvd'
        ultimate_parent_bvdid_bvd = 'ultimate_parent_bvdid_bvd'
        ultimate_parent_ctryiso_bvd = 'ultimate_parent_ctryiso_bvd'
        ultimate_parent_name_bvd = 'ultimate_parent_name_bvd'
        ussicccod_bvd = 'ussicccod_bvd'
        vatnumber_bvd = 'vatnumber_bvd'
        website_bvd = 'website_bvd'
        Bvd_Country_Check_claims = 'Bvd_Country_Check_claims'
        Bvd_Industry_Check_claims = 'Bvd_Industry_Check_claims'
        Combined_NotificationDate_IncidentDate = 'Combined_NotificationDate_IncidentDate'
        Claims_Time_btw_UWDate_IncNot_inQuarter = 'Claims_Time_btw_UWDate_IncNot_inQuarter'
        Claims_Time_btw_UWDate_IncNot_inMonth = 'Claims_Time_btw_UWDate_IncNot_inMonth'
        Claims_Time_btw_UWDate_IncNot_inDay = 'Claims_Time_btw_UWDate_IncNot_inDay'
        Reportingtime_inMonths = 'Reportingtime_inMonths'
        Reportingtime_inDays = 'Reportingtime_inDays'
        Trade_Level_CodeNumber_Mapped_Cambridge = 'Trade_Level_CodeNumber_Mapped_Cambridge'
        final_flag = 'final_flag'
        CambridgeKey = 'CambridgeKey'
        Loss_Development_Quarter = 'Loss_Development_Quarter'
        Trade_Level_Name_Mapped_Cambridge = 'Trade_Level_Name_Mapped_Cambridge'
        Is_Deleted = 'Is_Deleted'
        Is_Signal_Reserve = 'Is_Signal_Reserve'
        claim_cause = 'claim_cause'
        QualityKey = 'QualityKey'
        classification_score = 'classification_score'
        claim_cause_alternatives = 'claim_cause_alternatives'
        internal_external = 'internal_external'
        intention = 'intention'
        risk_id = 'risk_id'
        is_in_overlap_time_period = 'is_in_overlap_time_period'
        DateOfInceptionCombinedKey = 'DateOfInceptionCombinedKey'
        ID_Arbeitsvorrat_Exposure = 'ID_Arbeitsvorrat_Exposure'
        Client_Exposure = 'Client_Exposure'
        Insured_Name_ClientInfo_Exposure = 'Insured_Name_ClientInfo_Exposure'
        Policy_ID_ClientInfo_Exposure = 'Policy_ID_ClientInfo_Exposure'
        Policy_Inception_Date_Exposure = 'Policy_Inception_Date_Exposure'
        Policy_Expiry_Date_Exposure = 'Policy_Expiry_Date_Exposure'
        Coverage = 'Coverage'
        Currency_Exposure = 'Currency_Exposure'
        Client_Limit_USD_Exposure = 'Client_Limit_USD_Exposure'
        Full_Limit_USD_Exposure = 'Full_Limit_USD_Exposure'
        Attachment_USD_Exposure = 'Attachment_USD_Exposure'
        Client_Premium_USD = 'Client_Premium_USD'
        Combined_Premium_USD = 'Combined_Premium_USD'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Combined_Premium_Orig_Curr = 'Combined_Premium_Orig_Curr'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit_Exposure = 'Client_Share_of_Limit_Exposure'
        SIR_USD_Exposure = 'SIR_USD_Exposure'
        CountryExposureKey = 'CountryExposureKey'
        CambridgeExposureKey = 'CambridgeExposureKey'
        CompanySegmentExposureKey = 'CompanySegmentExposureKey'
        Turnover_USD_BvD_Exposure = 'Turnover_USD_BvD_Exposure'
        Duplicate_ID_Exposure = 'Duplicate_ID_Exposure'
        CountryCombinedKey = 'CountryCombinedKey'
        CambridgeCombinedKey = 'CambridgeCombinedKey'
        CompanySegmentCombinedKey = 'CompanySegmentCombinedKey'
        Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined = 'Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined'
        Calculated_or_ClientInfo_FGU_USD = 'Calculated_or_ClientInfo_FGU_USD'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str, Client: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, LossDateKey: int, rowNr: int, CountryKey: int, QualityKey: int, DateOfInceptionCombinedKey: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Date_of_Loss: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: int = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: date = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, IsCensored: int = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Paid_Client_Share_USD_Combined: float = None, ClaimsIncurredBandsKey: int = None, Incurred_Client_Share_USD: float = None, ClaimsIncurredCombinedBandsKey: int = None, Incurred_Client_Share_USD_Combined: float = None, ClaimsThresholdCategoriesKey: int = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Country_Combined: str = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, CompanySegmentKey: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Bvd_Country_Check_claims: str = None, Bvd_Industry_Check_claims: str = None, Combined_NotificationDate_IncidentDate: date = None, Claims_Time_btw_UWDate_IncNot_inQuarter: int = None, Claims_Time_btw_UWDate_IncNot_inMonth: int = None, Claims_Time_btw_UWDate_IncNot_inDay: int = None, Reportingtime_inMonths: int = None, Reportingtime_inDays: int = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, final_flag: int = None, CambridgeKey: int = None, Loss_Development_Quarter: int = None, Trade_Level_Name_Mapped_Cambridge: str = None, Is_Deleted: str = None, Is_Signal_Reserve: int = None, claim_cause: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, risk_id: int = None, is_in_overlap_time_period: int = None, ID_Arbeitsvorrat_Exposure: str = None, Client_Exposure: str = None, Insured_Name_ClientInfo_Exposure: str = None, Policy_ID_ClientInfo_Exposure: str = None, Policy_Inception_Date_Exposure: date = None, Policy_Expiry_Date_Exposure: date = None, Coverage: str = None, Currency_Exposure: str = None, Client_Limit_USD_Exposure: float = None, Full_Limit_USD_Exposure: float = None, Attachment_USD_Exposure: float = None, Client_Premium_USD: float = None, Combined_Premium_USD: float = None, Client_Premium_Orig_Curr: float = None, Combined_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_USD: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit_Exposure: float = None, SIR_USD_Exposure: float = None, CountryExposureKey: int = None, CambridgeExposureKey: int = None, CompanySegmentExposureKey: int = None, Turnover_USD_BvD_Exposure: float = None, Duplicate_ID_Exposure: str = None, CountryCombinedKey: int = None, CambridgeCombinedKey: int = None, CompanySegmentCombinedKey: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, Calculated_or_ClientInfo_FGU_USD: float = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@FSRI_Treaty nvarchar(400) = ?
    ,@FSRI_Section nvarchar(400) = ?
    ,@FSRI_Period nvarchar(400) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@LossDateKey int = ?
    ,@Date_of_Loss date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Nr_Affected_Records bigint = ?
    ,@Duration_of_Outage_hours float = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@Observation_Period_Start date(0) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@IsCensored bit = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Paid_Client_Share_USD_Combined float = ?
    ,@ClaimsIncurredBandsKey int = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@ClaimsIncurredCombinedBandsKey int = ?
    ,@Incurred_Client_Share_USD_Combined float = ?
    ,@ClaimsThresholdCategoriesKey int = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Country_Combined nvarchar(100) = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@Bvd_Country_Check_claims nvarchar(510) = ?
    ,@Bvd_Industry_Check_claims nvarchar(510) = ?
    ,@Combined_NotificationDate_IncidentDate date(0) = ?
    ,@Claims_Time_btw_UWDate_IncNot_inQuarter int = ?
    ,@Claims_Time_btw_UWDate_IncNot_inMonth int = ?
    ,@Claims_Time_btw_UWDate_IncNot_inDay int = ?
    ,@Reportingtime_inMonths int = ?
    ,@Reportingtime_inDays int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@final_flag bit = ?
    ,@CambridgeKey int = ?
    ,@Loss_Development_Quarter int = ?
    ,@Trade_Level_Name_Mapped_Cambridge nvarchar(2048) = ?
    ,@Is_Deleted nvarchar(100) = ?
    ,@Is_Signal_Reserve bit = ?
    ,@claim_cause nvarchar(100) = ?
    ,@QualityKey int = ?
    ,@classification_score float = ?
    ,@claim_cause_alternatives nvarchar(1000) = ?
    ,@internal_external nvarchar(20) = ?
    ,@intention nvarchar(20) = ?
    ,@risk_id bigint = ?
    ,@is_in_overlap_time_period bit = ?
    ,@DateOfInceptionCombinedKey int = ?
    ,@ID_Arbeitsvorrat_Exposure nvarchar(100) = ?
    ,@Client_Exposure nvarchar(400) = ?
    ,@Insured_Name_ClientInfo_Exposure nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo_Exposure nvarchar(1000) = ?
    ,@Policy_Inception_Date_Exposure date(0) = ?
    ,@Policy_Expiry_Date_Exposure date(0) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency_Exposure nvarchar(100) = ?
    ,@Client_Limit_USD_Exposure float = ?
    ,@Full_Limit_USD_Exposure float = ?
    ,@Attachment_USD_Exposure float = ?
    ,@Client_Premium_USD float = ?
    ,@Combined_Premium_USD float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Combined_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit_Exposure float = ?
    ,@SIR_USD_Exposure float = ?
    ,@CountryExposureKey int = ?
    ,@CambridgeExposureKey int = ?
    ,@CompanySegmentExposureKey int = ?
    ,@Turnover_USD_BvD_Exposure float = ?
    ,@Duplicate_ID_Exposure nvarchar(100) = ?
    ,@CountryCombinedKey int = ?
    ,@CambridgeCombinedKey int = ?
    ,@CompanySegmentCombinedKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined nvarchar(100) = ?
    ,@Calculated_or_ClientInfo_FGU_USD float = ?
;

INSERT INTO [dbo].[tFactClaims] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[PolicyInceptionDateKey]
    ,[Policy_Inception_Date]
    ,[PolicyExpiryDateKey]
    ,[Policy_Expiry_Date]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[FSRI_Treaty]
    ,[FSRI_Section]
    ,[FSRI_Period]
    ,[Claim_ID_ClientInfo]
    ,[Is_Claim_Closed]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[LossDateKey]
    ,[Date_of_Loss]
    ,[Claims_Description]
    ,[Type_of_Loss]
    ,[Country_of_Loss_Settlement]
    ,[Nr_Affected_Records]
    ,[Duration_of_Outage_hours]
    ,[Value_as_Of_Date]
    ,[Loss_Currency]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[Threshold_Orig_Curr_unind]
    ,[Observation_Period_Start]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[IsCensored]
    ,[ClientLimitBandKey]
    ,[Client_Limit_USD]
    ,[FullLimitBandKey]
    ,[Full_Limit_USD]
    ,[AttachmentBandKey]
    ,[Attachment_USD]
    ,[SIRBandsKey]
    ,[SIR_USD]
    ,[Incurred_Insured_FGU_USD]
    ,[Paid_Client_Share_USD]
    ,[Paid_Client_Share_USD_Combined]
    ,[ClaimsIncurredBandsKey]
    ,[Incurred_Client_Share_USD]
    ,[ClaimsIncurredCombinedBandsKey]
    ,[Incurred_Client_Share_USD_Combined]
    ,[ClaimsThresholdCategoriesKey]
    ,[Threshold_unind_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[CountryKey]
    ,[Insured_Country_ISO2]
    ,[Country_of_Loss_Settlement_ISO2]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[Internal_Claim_ID]
    ,[Country_Combined]
    ,[FileMetaInfoKey]
    ,[Insured_Name_BvD]
    ,[name_alias_bvd]
    ,[name_alias_source_bvd]
    ,[addr_internat_bvd]
    ,[ambest_id_bvd]
    ,[branch_count_bvd]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD]
    ,[Trade_Level_Name_Mapped_Cambridge_BvD]
    ,[category_of_company_bvd]
    ,[city_internat_bvd]
    ,[corporate_group_size_bvd]
    ,[country_bvd]
    ,[county_bvd]
    ,[Country_ISO2_BvD]
    ,[direct_parent_bvdid_bvd]
    ,[direct_parent_name_internat_bvd]
    ,[ein_bvd]
    ,[email_bvd]
    ,[employees_bvd]
    ,[eurovat_bvd]
    ,[hierarchy_level_bvd]
    ,[inactive_bvd]
    ,[incorporation_date_bvd]
    ,[legalfrm_bvd]
    ,[lei_lei_bvd]
    ,[listed_bvd]
    ,[mainexch_bvd]
    ,[naicsccod2017_bvd]
    ,[phone_bvd]
    ,[postcode_bvd]
    ,[previous_names_set_array_bvd]
    ,[sd_isin_bvd]
    ,[sd_ticker_bvd]
    ,[slegalf_bvd]
    ,[state_us_bvd]
    ,[subs_count_bvd]
    ,[traderegisternr_bvd]
    ,[Turnover_EUR_BvD]
    ,[CompanySegmentKey]
    ,[Turnover_USD_BvD]
    ,[type_of_entity_bvd]
    ,[ultimate_parent_bvdid_bvd]
    ,[ultimate_parent_ctryiso_bvd]
    ,[ultimate_parent_name_bvd]
    ,[ussicccod_bvd]
    ,[vatnumber_bvd]
    ,[website_bvd]
    ,[Bvd_Country_Check_claims]
    ,[Bvd_Industry_Check_claims]
    ,[Combined_NotificationDate_IncidentDate]
    ,[Claims_Time_btw_UWDate_IncNot_inQuarter]
    ,[Claims_Time_btw_UWDate_IncNot_inMonth]
    ,[Claims_Time_btw_UWDate_IncNot_inDay]
    ,[Reportingtime_inMonths]
    ,[Reportingtime_inDays]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge]
    ,[final_flag]
    ,[CambridgeKey]
    ,[Loss_Development_Quarter]
    ,[Trade_Level_Name_Mapped_Cambridge]
    ,[Is_Deleted]
    ,[Is_Signal_Reserve]
    ,[claim_cause]
    ,[QualityKey]
    ,[classification_score]
    ,[claim_cause_alternatives]
    ,[internal_external]
    ,[intention]
    ,[risk_id]
    ,[is_in_overlap_time_period]
    ,[DateOfInceptionCombinedKey]
    ,[ID_Arbeitsvorrat_Exposure]
    ,[Client_Exposure]
    ,[Insured_Name_ClientInfo_Exposure]
    ,[Policy_ID_ClientInfo_Exposure]
    ,[Policy_Inception_Date_Exposure]
    ,[Policy_Expiry_Date_Exposure]
    ,[Coverage]
    ,[Currency_Exposure]
    ,[Client_Limit_USD_Exposure]
    ,[Full_Limit_USD_Exposure]
    ,[Attachment_USD_Exposure]
    ,[Client_Premium_USD]
    ,[Combined_Premium_USD]
    ,[Client_Premium_Orig_Curr]
    ,[Combined_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_USD]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit_Exposure]
    ,[SIR_USD_Exposure]
    ,[CountryExposureKey]
    ,[CambridgeExposureKey]
    ,[CompanySegmentExposureKey]
    ,[Turnover_USD_BvD_Exposure]
    ,[Duplicate_ID_Exposure]
    ,[CountryCombinedKey]
    ,[CambridgeCombinedKey]
    ,[CompanySegmentCombinedKey]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined]
    ,[Calculated_or_ClientInfo_FGU_USD]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@PolicyInceptionDateKey
    ,@Policy_Inception_Date
    ,@PolicyExpiryDateKey
    ,@Policy_Expiry_Date
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@FSRI_Treaty
    ,@FSRI_Section
    ,@FSRI_Period
    ,@Claim_ID_ClientInfo
    ,@Is_Claim_Closed
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@LossDateKey
    ,@Date_of_Loss
    ,@Claims_Description
    ,@Type_of_Loss
    ,@Country_of_Loss_Settlement
    ,@Nr_Affected_Records
    ,@Duration_of_Outage_hours
    ,@Value_as_Of_Date
    ,@Loss_Currency
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@Threshold_Orig_Curr_unind
    ,@Observation_Period_Start
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@IsCensored
    ,@ClientLimitBandKey
    ,@Client_Limit_USD
    ,@FullLimitBandKey
    ,@Full_Limit_USD
    ,@AttachmentBandKey
    ,@Attachment_USD
    ,@SIRBandsKey
    ,@SIR_USD
    ,@Incurred_Insured_FGU_USD
    ,@Paid_Client_Share_USD
    ,@Paid_Client_Share_USD_Combined
    ,@ClaimsIncurredBandsKey
    ,@Incurred_Client_Share_USD
    ,@ClaimsIncurredCombinedBandsKey
    ,@Incurred_Client_Share_USD_Combined
    ,@ClaimsThresholdCategoriesKey
    ,@Threshold_unind_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@CountryKey
    ,@Insured_Country_ISO2
    ,@Country_of_Loss_Settlement_ISO2
    ,@BvD_ID
    ,@Duplicate_ID
    ,@Internal_Claim_ID
    ,@Country_Combined
    ,@FileMetaInfoKey
    ,@Insured_Name_BvD
    ,@name_alias_bvd
    ,@name_alias_source_bvd
    ,@addr_internat_bvd
    ,@ambest_id_bvd
    ,@branch_count_bvd
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,@Trade_Level_Name_Mapped_Cambridge_BvD
    ,@category_of_company_bvd
    ,@city_internat_bvd
    ,@corporate_group_size_bvd
    ,@country_bvd
    ,@county_bvd
    ,@Country_ISO2_BvD
    ,@direct_parent_bvdid_bvd
    ,@direct_parent_name_internat_bvd
    ,@ein_bvd
    ,@email_bvd
    ,@employees_bvd
    ,@eurovat_bvd
    ,@hierarchy_level_bvd
    ,@inactive_bvd
    ,@incorporation_date_bvd
    ,@legalfrm_bvd
    ,@lei_lei_bvd
    ,@listed_bvd
    ,@mainexch_bvd
    ,@naicsccod2017_bvd
    ,@phone_bvd
    ,@postcode_bvd
    ,@previous_names_set_array_bvd
    ,@sd_isin_bvd
    ,@sd_ticker_bvd
    ,@slegalf_bvd
    ,@state_us_bvd
    ,@subs_count_bvd
    ,@traderegisternr_bvd
    ,@Turnover_EUR_BvD
    ,@CompanySegmentKey
    ,@Turnover_USD_BvD
    ,@type_of_entity_bvd
    ,@ultimate_parent_bvdid_bvd
    ,@ultimate_parent_ctryiso_bvd
    ,@ultimate_parent_name_bvd
    ,@ussicccod_bvd
    ,@vatnumber_bvd
    ,@website_bvd
    ,@Bvd_Country_Check_claims
    ,@Bvd_Industry_Check_claims
    ,@Combined_NotificationDate_IncidentDate
    ,@Claims_Time_btw_UWDate_IncNot_inQuarter
    ,@Claims_Time_btw_UWDate_IncNot_inMonth
    ,@Claims_Time_btw_UWDate_IncNot_inDay
    ,@Reportingtime_inMonths
    ,@Reportingtime_inDays
    ,@Trade_Level_CodeNumber_Mapped_Cambridge
    ,@final_flag
    ,@CambridgeKey
    ,@Loss_Development_Quarter
    ,@Trade_Level_Name_Mapped_Cambridge
    ,@Is_Deleted
    ,@Is_Signal_Reserve
    ,@claim_cause
    ,@QualityKey
    ,@classification_score
    ,@claim_cause_alternatives
    ,@internal_external
    ,@intention
    ,@risk_id
    ,@is_in_overlap_time_period
    ,@DateOfInceptionCombinedKey
    ,@ID_Arbeitsvorrat_Exposure
    ,@Client_Exposure
    ,@Insured_Name_ClientInfo_Exposure
    ,@Policy_ID_ClientInfo_Exposure
    ,@Policy_Inception_Date_Exposure
    ,@Policy_Expiry_Date_Exposure
    ,@Coverage
    ,@Currency_Exposure
    ,@Client_Limit_USD_Exposure
    ,@Full_Limit_USD_Exposure
    ,@Attachment_USD_Exposure
    ,@Client_Premium_USD
    ,@Combined_Premium_USD
    ,@Client_Premium_Orig_Curr
    ,@Combined_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_USD
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit_Exposure
    ,@SIR_USD_Exposure
    ,@CountryExposureKey
    ,@CambridgeExposureKey
    ,@CompanySegmentExposureKey
    ,@Turnover_USD_BvD_Exposure
    ,@Duplicate_ID_Exposure
    ,@CountryCombinedKey
    ,@CambridgeCombinedKey
    ,@CompanySegmentCombinedKey
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined
    ,@Calculated_or_ClientInfo_FGU_USD
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, LossDateKey, Date_of_Loss, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, IsCensored, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Paid_Client_Share_USD_Combined, ClaimsIncurredBandsKey, Incurred_Client_Share_USD, ClaimsIncurredCombinedBandsKey, Incurred_Client_Share_USD_Combined, ClaimsThresholdCategoriesKey, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, CountryKey, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Country_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, CompanySegmentKey, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Bvd_Country_Check_claims, Bvd_Industry_Check_claims, Combined_NotificationDate_IncidentDate, Claims_Time_btw_UWDate_IncNot_inQuarter, Claims_Time_btw_UWDate_IncNot_inMonth, Claims_Time_btw_UWDate_IncNot_inDay, Reportingtime_inMonths, Reportingtime_inDays, Trade_Level_CodeNumber_Mapped_Cambridge, final_flag, CambridgeKey, Loss_Development_Quarter, Trade_Level_Name_Mapped_Cambridge, Is_Deleted, Is_Signal_Reserve, claim_cause, QualityKey, classification_score, claim_cause_alternatives, internal_external, intention, risk_id, is_in_overlap_time_period, DateOfInceptionCombinedKey, ID_Arbeitsvorrat_Exposure, Client_Exposure, Insured_Name_ClientInfo_Exposure, Policy_ID_ClientInfo_Exposure, Policy_Inception_Date_Exposure, Policy_Expiry_Date_Exposure, Coverage, Currency_Exposure, Client_Limit_USD_Exposure, Full_Limit_USD_Exposure, Attachment_USD_Exposure, Client_Premium_USD, Combined_Premium_USD, Client_Premium_Orig_Curr, Combined_Premium_Orig_Curr, Client_GrossNet_Premium_USD, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit_Exposure, SIR_USD_Exposure, CountryExposureKey, CambridgeExposureKey, CompanySegmentExposureKey, Turnover_USD_BvD_Exposure, Duplicate_ID_Exposure, CountryCombinedKey, CambridgeCombinedKey, CompanySegmentCombinedKey, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, Calculated_or_ClientInfo_FGU_USD ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, Client: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, LossDateKey: int, rowNr: int, CountryKey: int, QualityKey: int, DateOfInceptionCombinedKey: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Date_of_Loss: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: int = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: date = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, IsCensored: int = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Paid_Client_Share_USD_Combined: float = None, ClaimsIncurredBandsKey: int = None, Incurred_Client_Share_USD: float = None, ClaimsIncurredCombinedBandsKey: int = None, Incurred_Client_Share_USD_Combined: float = None, ClaimsThresholdCategoriesKey: int = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Country_Combined: str = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, CompanySegmentKey: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Bvd_Country_Check_claims: str = None, Bvd_Industry_Check_claims: str = None, Combined_NotificationDate_IncidentDate: date = None, Claims_Time_btw_UWDate_IncNot_inQuarter: int = None, Claims_Time_btw_UWDate_IncNot_inMonth: int = None, Claims_Time_btw_UWDate_IncNot_inDay: int = None, Reportingtime_inMonths: int = None, Reportingtime_inDays: int = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, final_flag: int = None, CambridgeKey: int = None, Loss_Development_Quarter: int = None, Trade_Level_Name_Mapped_Cambridge: str = None, Is_Deleted: str = None, Is_Signal_Reserve: int = None, claim_cause: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, risk_id: int = None, is_in_overlap_time_period: int = None, ID_Arbeitsvorrat_Exposure: str = None, Client_Exposure: str = None, Insured_Name_ClientInfo_Exposure: str = None, Policy_ID_ClientInfo_Exposure: str = None, Policy_Inception_Date_Exposure: date = None, Policy_Expiry_Date_Exposure: date = None, Coverage: str = None, Currency_Exposure: str = None, Client_Limit_USD_Exposure: float = None, Full_Limit_USD_Exposure: float = None, Attachment_USD_Exposure: float = None, Client_Premium_USD: float = None, Combined_Premium_USD: float = None, Client_Premium_Orig_Curr: float = None, Combined_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_USD: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit_Exposure: float = None, SIR_USD_Exposure: float = None, CountryExposureKey: int = None, CambridgeExposureKey: int = None, CompanySegmentExposureKey: int = None, Turnover_USD_BvD_Exposure: float = None, Duplicate_ID_Exposure: str = None, CountryCombinedKey: int = None, CambridgeCombinedKey: int = None, CompanySegmentCombinedKey: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, Calculated_or_ClientInfo_FGU_USD: float = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@FSRI_Treaty nvarchar(400) = ?
    ,@FSRI_Section nvarchar(400) = ?
    ,@FSRI_Period nvarchar(400) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@LossDateKey int = ?
    ,@Date_of_Loss date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Nr_Affected_Records bigint = ?
    ,@Duration_of_Outage_hours float = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@Observation_Period_Start date(0) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@IsCensored bit = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Paid_Client_Share_USD_Combined float = ?
    ,@ClaimsIncurredBandsKey int = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@ClaimsIncurredCombinedBandsKey int = ?
    ,@Incurred_Client_Share_USD_Combined float = ?
    ,@ClaimsThresholdCategoriesKey int = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Country_Combined nvarchar(100) = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@Bvd_Country_Check_claims nvarchar(510) = ?
    ,@Bvd_Industry_Check_claims nvarchar(510) = ?
    ,@Combined_NotificationDate_IncidentDate date(0) = ?
    ,@Claims_Time_btw_UWDate_IncNot_inQuarter int = ?
    ,@Claims_Time_btw_UWDate_IncNot_inMonth int = ?
    ,@Claims_Time_btw_UWDate_IncNot_inDay int = ?
    ,@Reportingtime_inMonths int = ?
    ,@Reportingtime_inDays int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@final_flag bit = ?
    ,@CambridgeKey int = ?
    ,@Loss_Development_Quarter int = ?
    ,@Trade_Level_Name_Mapped_Cambridge nvarchar(2048) = ?
    ,@Is_Deleted nvarchar(100) = ?
    ,@Is_Signal_Reserve bit = ?
    ,@claim_cause nvarchar(100) = ?
    ,@QualityKey int = ?
    ,@classification_score float = ?
    ,@claim_cause_alternatives nvarchar(1000) = ?
    ,@internal_external nvarchar(20) = ?
    ,@intention nvarchar(20) = ?
    ,@risk_id bigint = ?
    ,@is_in_overlap_time_period bit = ?
    ,@DateOfInceptionCombinedKey int = ?
    ,@ID_Arbeitsvorrat_Exposure nvarchar(100) = ?
    ,@Client_Exposure nvarchar(400) = ?
    ,@Insured_Name_ClientInfo_Exposure nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo_Exposure nvarchar(1000) = ?
    ,@Policy_Inception_Date_Exposure date(0) = ?
    ,@Policy_Expiry_Date_Exposure date(0) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency_Exposure nvarchar(100) = ?
    ,@Client_Limit_USD_Exposure float = ?
    ,@Full_Limit_USD_Exposure float = ?
    ,@Attachment_USD_Exposure float = ?
    ,@Client_Premium_USD float = ?
    ,@Combined_Premium_USD float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Combined_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit_Exposure float = ?
    ,@SIR_USD_Exposure float = ?
    ,@CountryExposureKey int = ?
    ,@CambridgeExposureKey int = ?
    ,@CompanySegmentExposureKey int = ?
    ,@Turnover_USD_BvD_Exposure float = ?
    ,@Duplicate_ID_Exposure nvarchar(100) = ?
    ,@CountryCombinedKey int = ?
    ,@CambridgeCombinedKey int = ?
    ,@CompanySegmentCombinedKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined nvarchar(100) = ?
    ,@Calculated_or_ClientInfo_FGU_USD float = ?
;

UPDATE [dbo].[tFactClaims] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[PolicyInceptionDateKey] = @PolicyInceptionDateKey
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[PolicyExpiryDateKey] = @PolicyExpiryDateKey
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[FSRI_Treaty] = @FSRI_Treaty
    ,[FSRI_Section] = @FSRI_Section
    ,[FSRI_Period] = @FSRI_Period
    ,[Claim_ID_ClientInfo] = @Claim_ID_ClientInfo
    ,[Is_Claim_Closed] = @Is_Claim_Closed
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[LossDateKey] = @LossDateKey
    ,[Date_of_Loss] = @Date_of_Loss
    ,[Claims_Description] = @Claims_Description
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Country_of_Loss_Settlement] = @Country_of_Loss_Settlement
    ,[Nr_Affected_Records] = @Nr_Affected_Records
    ,[Duration_of_Outage_hours] = @Duration_of_Outage_hours
    ,[Value_as_Of_Date] = @Value_as_Of_Date
    ,[Loss_Currency] = @Loss_Currency
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[Threshold_Orig_Curr_unind] = @Threshold_Orig_Curr_unind
    ,[Observation_Period_Start] = @Observation_Period_Start
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[IsCensored] = @IsCensored
    ,[ClientLimitBandKey] = @ClientLimitBandKey
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[FullLimitBandKey] = @FullLimitBandKey
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[AttachmentBandKey] = @AttachmentBandKey
    ,[Attachment_USD] = @Attachment_USD
    ,[SIRBandsKey] = @SIRBandsKey
    ,[SIR_USD] = @SIR_USD
    ,[Incurred_Insured_FGU_USD] = @Incurred_Insured_FGU_USD
    ,[Paid_Client_Share_USD] = @Paid_Client_Share_USD
    ,[Paid_Client_Share_USD_Combined] = @Paid_Client_Share_USD_Combined
    ,[ClaimsIncurredBandsKey] = @ClaimsIncurredBandsKey
    ,[Incurred_Client_Share_USD] = @Incurred_Client_Share_USD
    ,[ClaimsIncurredCombinedBandsKey] = @ClaimsIncurredCombinedBandsKey
    ,[Incurred_Client_Share_USD_Combined] = @Incurred_Client_Share_USD_Combined
    ,[ClaimsThresholdCategoriesKey] = @ClaimsThresholdCategoriesKey
    ,[Threshold_unind_USD] = @Threshold_unind_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[CountryKey] = @CountryKey
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Country_of_Loss_Settlement_ISO2] = @Country_of_Loss_Settlement_ISO2
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Internal_Claim_ID] = @Internal_Claim_ID
    ,[Country_Combined] = @Country_Combined
    ,[FileMetaInfoKey] = @FileMetaInfoKey
    ,[Insured_Name_BvD] = @Insured_Name_BvD
    ,[name_alias_bvd] = @name_alias_bvd
    ,[name_alias_source_bvd] = @name_alias_source_bvd
    ,[addr_internat_bvd] = @addr_internat_bvd
    ,[ambest_id_bvd] = @ambest_id_bvd
    ,[branch_count_bvd] = @branch_count_bvd
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD] = @Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,[Trade_Level_Name_Mapped_Cambridge_BvD] = @Trade_Level_Name_Mapped_Cambridge_BvD
    ,[category_of_company_bvd] = @category_of_company_bvd
    ,[city_internat_bvd] = @city_internat_bvd
    ,[corporate_group_size_bvd] = @corporate_group_size_bvd
    ,[country_bvd] = @country_bvd
    ,[county_bvd] = @county_bvd
    ,[Country_ISO2_BvD] = @Country_ISO2_BvD
    ,[direct_parent_bvdid_bvd] = @direct_parent_bvdid_bvd
    ,[direct_parent_name_internat_bvd] = @direct_parent_name_internat_bvd
    ,[ein_bvd] = @ein_bvd
    ,[email_bvd] = @email_bvd
    ,[employees_bvd] = @employees_bvd
    ,[eurovat_bvd] = @eurovat_bvd
    ,[hierarchy_level_bvd] = @hierarchy_level_bvd
    ,[inactive_bvd] = @inactive_bvd
    ,[incorporation_date_bvd] = @incorporation_date_bvd
    ,[legalfrm_bvd] = @legalfrm_bvd
    ,[lei_lei_bvd] = @lei_lei_bvd
    ,[listed_bvd] = @listed_bvd
    ,[mainexch_bvd] = @mainexch_bvd
    ,[naicsccod2017_bvd] = @naicsccod2017_bvd
    ,[phone_bvd] = @phone_bvd
    ,[postcode_bvd] = @postcode_bvd
    ,[previous_names_set_array_bvd] = @previous_names_set_array_bvd
    ,[sd_isin_bvd] = @sd_isin_bvd
    ,[sd_ticker_bvd] = @sd_ticker_bvd
    ,[slegalf_bvd] = @slegalf_bvd
    ,[state_us_bvd] = @state_us_bvd
    ,[subs_count_bvd] = @subs_count_bvd
    ,[traderegisternr_bvd] = @traderegisternr_bvd
    ,[Turnover_EUR_BvD] = @Turnover_EUR_BvD
    ,[CompanySegmentKey] = @CompanySegmentKey
    ,[Turnover_USD_BvD] = @Turnover_USD_BvD
    ,[type_of_entity_bvd] = @type_of_entity_bvd
    ,[ultimate_parent_bvdid_bvd] = @ultimate_parent_bvdid_bvd
    ,[ultimate_parent_ctryiso_bvd] = @ultimate_parent_ctryiso_bvd
    ,[ultimate_parent_name_bvd] = @ultimate_parent_name_bvd
    ,[ussicccod_bvd] = @ussicccod_bvd
    ,[vatnumber_bvd] = @vatnumber_bvd
    ,[website_bvd] = @website_bvd
    ,[Bvd_Country_Check_claims] = @Bvd_Country_Check_claims
    ,[Bvd_Industry_Check_claims] = @Bvd_Industry_Check_claims
    ,[Combined_NotificationDate_IncidentDate] = @Combined_NotificationDate_IncidentDate
    ,[Claims_Time_btw_UWDate_IncNot_inQuarter] = @Claims_Time_btw_UWDate_IncNot_inQuarter
    ,[Claims_Time_btw_UWDate_IncNot_inMonth] = @Claims_Time_btw_UWDate_IncNot_inMonth
    ,[Claims_Time_btw_UWDate_IncNot_inDay] = @Claims_Time_btw_UWDate_IncNot_inDay
    ,[Reportingtime_inMonths] = @Reportingtime_inMonths
    ,[Reportingtime_inDays] = @Reportingtime_inDays
    ,[Trade_Level_CodeNumber_Mapped_Cambridge] = @Trade_Level_CodeNumber_Mapped_Cambridge
    ,[final_flag] = @final_flag
    ,[CambridgeKey] = @CambridgeKey
    ,[Loss_Development_Quarter] = @Loss_Development_Quarter
    ,[Trade_Level_Name_Mapped_Cambridge] = @Trade_Level_Name_Mapped_Cambridge
    ,[Is_Deleted] = @Is_Deleted
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
    ,[claim_cause] = @claim_cause
    ,[QualityKey] = @QualityKey
    ,[classification_score] = @classification_score
    ,[claim_cause_alternatives] = @claim_cause_alternatives
    ,[internal_external] = @internal_external
    ,[intention] = @intention
    ,[risk_id] = @risk_id
    ,[is_in_overlap_time_period] = @is_in_overlap_time_period
    ,[DateOfInceptionCombinedKey] = @DateOfInceptionCombinedKey
    ,[ID_Arbeitsvorrat_Exposure] = @ID_Arbeitsvorrat_Exposure
    ,[Client_Exposure] = @Client_Exposure
    ,[Insured_Name_ClientInfo_Exposure] = @Insured_Name_ClientInfo_Exposure
    ,[Policy_ID_ClientInfo_Exposure] = @Policy_ID_ClientInfo_Exposure
    ,[Policy_Inception_Date_Exposure] = @Policy_Inception_Date_Exposure
    ,[Policy_Expiry_Date_Exposure] = @Policy_Expiry_Date_Exposure
    ,[Coverage] = @Coverage
    ,[Currency_Exposure] = @Currency_Exposure
    ,[Client_Limit_USD_Exposure] = @Client_Limit_USD_Exposure
    ,[Full_Limit_USD_Exposure] = @Full_Limit_USD_Exposure
    ,[Attachment_USD_Exposure] = @Attachment_USD_Exposure
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Combined_Premium_USD] = @Combined_Premium_USD
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Combined_Premium_Orig_Curr] = @Combined_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit_Exposure] = @Client_Share_of_Limit_Exposure
    ,[SIR_USD_Exposure] = @SIR_USD_Exposure
    ,[CountryExposureKey] = @CountryExposureKey
    ,[CambridgeExposureKey] = @CambridgeExposureKey
    ,[CompanySegmentExposureKey] = @CompanySegmentExposureKey
    ,[Turnover_USD_BvD_Exposure] = @Turnover_USD_BvD_Exposure
    ,[Duplicate_ID_Exposure] = @Duplicate_ID_Exposure
    ,[CountryCombinedKey] = @CountryCombinedKey
    ,[CambridgeCombinedKey] = @CambridgeCombinedKey
    ,[CompanySegmentCombinedKey] = @CompanySegmentCombinedKey
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined] = @Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined
    ,[Calculated_or_ClientInfo_FGU_USD] = @Calculated_or_ClientInfo_FGU_USD
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, LossDateKey, Date_of_Loss, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, IsCensored, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Paid_Client_Share_USD_Combined, ClaimsIncurredBandsKey, Incurred_Client_Share_USD, ClaimsIncurredCombinedBandsKey, Incurred_Client_Share_USD_Combined, ClaimsThresholdCategoriesKey, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, CountryKey, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Country_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, CompanySegmentKey, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Bvd_Country_Check_claims, Bvd_Industry_Check_claims, Combined_NotificationDate_IncidentDate, Claims_Time_btw_UWDate_IncNot_inQuarter, Claims_Time_btw_UWDate_IncNot_inMonth, Claims_Time_btw_UWDate_IncNot_inDay, Reportingtime_inMonths, Reportingtime_inDays, Trade_Level_CodeNumber_Mapped_Cambridge, final_flag, CambridgeKey, Loss_Development_Quarter, Trade_Level_Name_Mapped_Cambridge, Is_Deleted, Is_Signal_Reserve, claim_cause, QualityKey, classification_score, claim_cause_alternatives, internal_external, intention, risk_id, is_in_overlap_time_period, DateOfInceptionCombinedKey, ID_Arbeitsvorrat_Exposure, Client_Exposure, Insured_Name_ClientInfo_Exposure, Policy_ID_ClientInfo_Exposure, Policy_Inception_Date_Exposure, Policy_Expiry_Date_Exposure, Coverage, Currency_Exposure, Client_Limit_USD_Exposure, Full_Limit_USD_Exposure, Attachment_USD_Exposure, Client_Premium_USD, Combined_Premium_USD, Client_Premium_Orig_Curr, Combined_Premium_Orig_Curr, Client_GrossNet_Premium_USD, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit_Exposure, SIR_USD_Exposure, CountryExposureKey, CambridgeExposureKey, CompanySegmentExposureKey, Turnover_USD_BvD_Exposure, Duplicate_ID_Exposure, CountryCombinedKey, CambridgeCombinedKey, CompanySegmentCombinedKey, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, Calculated_or_ClientInfo_FGU_USD ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tFactClaims]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class bvd_matching:
        # table
        TableName = 'bvd_matching'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[bvd_matching]'
        # columns
        company_id = 'company_id'
        bvd_id = 'bvd_id'
        plausibility_class = 'plausibility_class'
        plausibility_score = 'plausibility_score'
        config_setting = 'config_setting'
        string_match_score_internal = 'string_match_score_internal'
        name_alias = 'name_alias'
        name_alias_source = 'name_alias_source'
        modified_overall_score = 'modified_overall_score'
        best_match = 'best_match'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, company_id: int, bvd_id: str, plausibility_class: int = None, plausibility_score: float = None, config_setting: str = None, string_match_score_internal: float = None, name_alias: str = None, name_alias_source: str = None, modified_overall_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@bvd_id nvarchar(100) = ?
    ,@plausibility_class bit = ?
    ,@plausibility_score float = ?
    ,@config_setting nvarchar(100) = ?
    ,@string_match_score_internal float = ?
    ,@name_alias nvarchar(2048) = ?
    ,@name_alias_source nvarchar(128) = ?
    ,@modified_overall_score float = ?
    ,@best_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[bvd_matching] (
    [company_id]
    ,[bvd_id]
    ,[plausibility_class]
    ,[plausibility_score]
    ,[config_setting]
    ,[string_match_score_internal]
    ,[name_alias]
    ,[name_alias_source]
    ,[modified_overall_score]
    ,[best_match]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @company_id
    ,@bvd_id
    ,@plausibility_class
    ,@plausibility_score
    ,@config_setting
    ,@string_match_score_internal
    ,@name_alias
    ,@name_alias_source
    ,@modified_overall_score
    ,@best_match
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, bvd_id, plausibility_class, plausibility_score, config_setting, string_match_score_internal, name_alias, name_alias_source, modified_overall_score, best_match, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, company_id: int, bvd_id: str, plausibility_class: int = None, plausibility_score: float = None, config_setting: str = None, string_match_score_internal: float = None, name_alias: str = None, name_alias_source: str = None, modified_overall_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@bvd_id nvarchar(100) = ?
    ,@plausibility_class bit = ?
    ,@plausibility_score float = ?
    ,@config_setting nvarchar(100) = ?
    ,@string_match_score_internal float = ?
    ,@name_alias nvarchar(2048) = ?
    ,@name_alias_source nvarchar(128) = ?
    ,@modified_overall_score float = ?
    ,@best_match bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[bvd_matching] SET 
    [plausibility_class] = @plausibility_class
    ,[plausibility_score] = @plausibility_score
    ,[config_setting] = @config_setting
    ,[string_match_score_internal] = @string_match_score_internal
    ,[name_alias] = @name_alias
    ,[name_alias_source] = @name_alias_source
    ,[modified_overall_score] = @modified_overall_score
    ,[best_match] = @best_match
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [company_id] = @company_id
    ,[bvd_id] = @bvd_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, bvd_id, plausibility_class, plausibility_score, config_setting, string_match_score_internal, name_alias, name_alias_source, modified_overall_score, best_match, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, company_id: int, bvd_id: str) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@bvd_id nvarchar(100) = ?
;

DELETE [dbo].[bvd_matching]
WHERE
    [company_id] = @company_id
    ,[bvd_id] = @bvd_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, bvd_id ]).exec_df()

    class validationResults_claims:
        # table
        TableName = 'validationResults_claims'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[validationResults_claims]'
        # columns
        id = 'id'
        ref_id = 'ref_id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        RuleName = 'RuleName'
        Issue = 'Issue'
        Column = 'Column'
        Column2 = 'Column2'
        Column3 = 'Column3'
        rowNr = 'rowNr'
        File = 'File'
        Value = 'Value'
        Value2 = 'Value2'
        Value3 = 'Value3'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ref_id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@RuleName nvarchar(2048) = ?
    ,@Issue nvarchar(2048) = ?
    ,@Column nvarchar(400) = ?
    ,@Column2 nvarchar(400) = ?
    ,@Column3 nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@File nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
    ,@Value2 nvarchar(2048) = ?
    ,@Value3 nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[validationResults_claims] (
    [ref_id]
    ,[ID_Arbeitsvorrat]
    ,[RuleName]
    ,[Issue]
    ,[Column]
    ,[Column2]
    ,[Column3]
    ,[rowNr]
    ,[File]
    ,[Value]
    ,[Value2]
    ,[Value3]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ref_id
    ,@ID_Arbeitsvorrat
    ,@RuleName
    ,@Issue
    ,@Column
    ,@Column2
    ,@Column3
    ,@rowNr
    ,@File
    ,@Value
    ,@Value2
    ,@Value3
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ref_id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@RuleName nvarchar(2048) = ?
    ,@Issue nvarchar(2048) = ?
    ,@Column nvarchar(400) = ?
    ,@Column2 nvarchar(400) = ?
    ,@Column3 nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@File nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
    ,@Value2 nvarchar(2048) = ?
    ,@Value3 nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[validationResults_claims] SET 
    [ref_id] = @ref_id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[RuleName] = @RuleName
    ,[Issue] = @Issue
    ,[Column] = @Column
    ,[Column2] = @Column2
    ,[Column3] = @Column3
    ,[rowNr] = @rowNr
    ,[File] = @File
    ,[Value] = @Value
    ,[Value2] = @Value2
    ,[Value3] = @Value3
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[validationResults_claims]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class bvd_old:
        # table
        TableName = 'bvd_old'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[bvd_old]'
        # columns
        id = 'id'
        bvd_id = 'bvd_id'
        name = 'name'
        name_alias = 'name_alias'
        name_alias_source = 'name_alias_source'
        addr_internat = 'addr_internat'
        ambest_id = 'ambest_id'
        branch_count = 'branch_count'
        category_of_company = 'category_of_company'
        city_internat = 'city_internat'
        corporate_group_size = 'corporate_group_size'
        country = 'country'
        county = 'county'
        ctryiso = 'ctryiso'
        direct_parent_bvdid = 'direct_parent_bvdid'
        direct_parent_name_internat = 'direct_parent_name_internat'
        ein = 'ein'
        email = 'email'
        employees = 'employees'
        eurovat = 'eurovat'
        hierarchy_level = 'hierarchy_level'
        inactive = 'inactive'
        incorporation_date = 'incorporation_date'
        legalfrm = 'legalfrm'
        lei_lei = 'lei_lei'
        listed = 'listed'
        mainexch = 'mainexch'
        naicsccod2017 = 'naicsccod2017'
        phone = 'phone'
        postcode = 'postcode'
        previous_names_set_array = 'previous_names_set_array'
        sd_isin = 'sd_isin'
        sd_ticker = 'sd_ticker'
        slegalf = 'slegalf'
        state_us = 'state_us'
        subs_count = 'subs_count'
        traderegisternr = 'traderegisternr'
        turnover_eur = 'turnover_eur'
        type_of_entity = 'type_of_entity'
        ultimate_parent_bvdid = 'ultimate_parent_bvdid'
        ultimate_parent_ctryiso = 'ultimate_parent_ctryiso'
        ultimate_parent_name = 'ultimate_parent_name'
        ussicccod = 'ussicccod'
        vatnumber = 'vatnumber'
        website = 'website'
        create_time = 'create_time'
        change_time = 'change_time'
        changed_by = 'changed_by'
        cambridgecod = 'cambridgecod'
        cambridgename = 'cambridgename'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, bvd_id: str, name: str = None, name_alias: str = None, name_alias_source: str = None, addr_internat: str = None, ambest_id: str = None, branch_count: int = None, category_of_company: str = None, city_internat: str = None, corporate_group_size: int = None, country: str = None, county: str = None, ctryiso: str = None, direct_parent_bvdid: str = None, direct_parent_name_internat: str = None, ein: str = None, email: str = None, employees: int = None, eurovat: str = None, hierarchy_level: int = None, inactive: str = None, incorporation_date: str = None, legalfrm: str = None, lei_lei: str = None, listed: str = None, mainexch: str = None, naicsccod2017: str = None, phone: str = None, postcode: str = None, previous_names_set_array: str = None, sd_isin: str = None, sd_ticker: str = None, slegalf: str = None, state_us: str = None, subs_count: int = None, traderegisternr: str = None, turnover_eur: int = None, type_of_entity: str = None, ultimate_parent_bvdid: str = None, ultimate_parent_ctryiso: str = None, ultimate_parent_name: str = None, ussicccod: str = None, vatnumber: str = None, website: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None, cambridgecod: str = None, cambridgename: str = None) -> DataFrame:
            sql = """
DECLARE
    @bvd_id nvarchar(100) = ?
    ,@name nvarchar(2048) = ?
    ,@name_alias nvarchar(2048) = ?
    ,@name_alias_source nvarchar(128) = ?
    ,@addr_internat nvarchar(2048) = ?
    ,@ambest_id nvarchar(100) = ?
    ,@branch_count int = ?
    ,@category_of_company nvarchar(100) = ?
    ,@city_internat nvarchar(400) = ?
    ,@corporate_group_size int = ?
    ,@country nvarchar(200) = ?
    ,@county nvarchar(100) = ?
    ,@ctryiso nchar(4) = ?
    ,@direct_parent_bvdid nvarchar(100) = ?
    ,@direct_parent_name_internat nvarchar(2048) = ?
    ,@ein nvarchar(100) = ?
    ,@email nvarchar(200) = ?
    ,@employees int = ?
    ,@eurovat nvarchar(100) = ?
    ,@hierarchy_level int = ?
    ,@inactive nvarchar(16) = ?
    ,@incorporation_date nvarchar(100) = ?
    ,@legalfrm nvarchar(400) = ?
    ,@lei_lei nvarchar(100) = ?
    ,@listed nvarchar(100) = ?
    ,@mainexch nvarchar(400) = ?
    ,@naicsccod2017 nvarchar(2048) = ?
    ,@phone nvarchar(100) = ?
    ,@postcode nvarchar(100) = ?
    ,@previous_names_set_array nvarchar(4096) = ?
    ,@sd_isin nvarchar(100) = ?
    ,@sd_ticker nvarchar(100) = ?
    ,@slegalf nvarchar(200) = ?
    ,@state_us nchar(4) = ?
    ,@subs_count int = ?
    ,@traderegisternr nvarchar(100) = ?
    ,@turnover_eur bigint = ?
    ,@type_of_entity nvarchar(200) = ?
    ,@ultimate_parent_bvdid nvarchar(100) = ?
    ,@ultimate_parent_ctryiso nchar(4) = ?
    ,@ultimate_parent_name nvarchar(2048) = ?
    ,@ussicccod nvarchar(2048) = ?
    ,@vatnumber nvarchar(100) = ?
    ,@website nvarchar(400) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
    ,@cambridgecod nvarchar(100) = ?
    ,@cambridgename nvarchar(2048) = ?
;

INSERT INTO [dbo].[bvd_old] (
    [bvd_id]
    ,[name]
    ,[name_alias]
    ,[name_alias_source]
    ,[addr_internat]
    ,[ambest_id]
    ,[branch_count]
    ,[category_of_company]
    ,[city_internat]
    ,[corporate_group_size]
    ,[country]
    ,[county]
    ,[ctryiso]
    ,[direct_parent_bvdid]
    ,[direct_parent_name_internat]
    ,[ein]
    ,[email]
    ,[employees]
    ,[eurovat]
    ,[hierarchy_level]
    ,[inactive]
    ,[incorporation_date]
    ,[legalfrm]
    ,[lei_lei]
    ,[listed]
    ,[mainexch]
    ,[naicsccod2017]
    ,[phone]
    ,[postcode]
    ,[previous_names_set_array]
    ,[sd_isin]
    ,[sd_ticker]
    ,[slegalf]
    ,[state_us]
    ,[subs_count]
    ,[traderegisternr]
    ,[turnover_eur]
    ,[type_of_entity]
    ,[ultimate_parent_bvdid]
    ,[ultimate_parent_ctryiso]
    ,[ultimate_parent_name]
    ,[ussicccod]
    ,[vatnumber]
    ,[website]
    ,[create_time]
    ,[change_time]
    ,[changed_by]
    ,[cambridgecod]
    ,[cambridgename]
)
VALUES (
    @bvd_id
    ,@name
    ,@name_alias
    ,@name_alias_source
    ,@addr_internat
    ,@ambest_id
    ,@branch_count
    ,@category_of_company
    ,@city_internat
    ,@corporate_group_size
    ,@country
    ,@county
    ,@ctryiso
    ,@direct_parent_bvdid
    ,@direct_parent_name_internat
    ,@ein
    ,@email
    ,@employees
    ,@eurovat
    ,@hierarchy_level
    ,@inactive
    ,@incorporation_date
    ,@legalfrm
    ,@lei_lei
    ,@listed
    ,@mainexch
    ,@naicsccod2017
    ,@phone
    ,@postcode
    ,@previous_names_set_array
    ,@sd_isin
    ,@sd_ticker
    ,@slegalf
    ,@state_us
    ,@subs_count
    ,@traderegisternr
    ,@turnover_eur
    ,@type_of_entity
    ,@ultimate_parent_bvdid
    ,@ultimate_parent_ctryiso
    ,@ultimate_parent_name
    ,@ussicccod
    ,@vatnumber
    ,@website
    ,@create_time
    ,@change_time
    ,@changed_by
    ,@cambridgecod
    ,@cambridgename
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ bvd_id, name, name_alias, name_alias_source, addr_internat, ambest_id, branch_count, category_of_company, city_internat, corporate_group_size, country, county, ctryiso, direct_parent_bvdid, direct_parent_name_internat, ein, email, employees, eurovat, hierarchy_level, inactive, incorporation_date, legalfrm, lei_lei, listed, mainexch, naicsccod2017, phone, postcode, previous_names_set_array, sd_isin, sd_ticker, slegalf, state_us, subs_count, traderegisternr, turnover_eur, type_of_entity, ultimate_parent_bvdid, ultimate_parent_ctryiso, ultimate_parent_name, ussicccod, vatnumber, website, create_time, change_time, changed_by, cambridgecod, cambridgename ]).exec_df()

        def update(self, id: int, bvd_id: str, name: str = None, name_alias: str = None, name_alias_source: str = None, addr_internat: str = None, ambest_id: str = None, branch_count: int = None, category_of_company: str = None, city_internat: str = None, corporate_group_size: int = None, country: str = None, county: str = None, ctryiso: str = None, direct_parent_bvdid: str = None, direct_parent_name_internat: str = None, ein: str = None, email: str = None, employees: int = None, eurovat: str = None, hierarchy_level: int = None, inactive: str = None, incorporation_date: str = None, legalfrm: str = None, lei_lei: str = None, listed: str = None, mainexch: str = None, naicsccod2017: str = None, phone: str = None, postcode: str = None, previous_names_set_array: str = None, sd_isin: str = None, sd_ticker: str = None, slegalf: str = None, state_us: str = None, subs_count: int = None, traderegisternr: str = None, turnover_eur: int = None, type_of_entity: str = None, ultimate_parent_bvdid: str = None, ultimate_parent_ctryiso: str = None, ultimate_parent_name: str = None, ussicccod: str = None, vatnumber: str = None, website: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None, cambridgecod: str = None, cambridgename: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@bvd_id nvarchar(100) = ?
    ,@name nvarchar(2048) = ?
    ,@name_alias nvarchar(2048) = ?
    ,@name_alias_source nvarchar(128) = ?
    ,@addr_internat nvarchar(2048) = ?
    ,@ambest_id nvarchar(100) = ?
    ,@branch_count int = ?
    ,@category_of_company nvarchar(100) = ?
    ,@city_internat nvarchar(400) = ?
    ,@corporate_group_size int = ?
    ,@country nvarchar(200) = ?
    ,@county nvarchar(100) = ?
    ,@ctryiso nchar(4) = ?
    ,@direct_parent_bvdid nvarchar(100) = ?
    ,@direct_parent_name_internat nvarchar(2048) = ?
    ,@ein nvarchar(100) = ?
    ,@email nvarchar(200) = ?
    ,@employees int = ?
    ,@eurovat nvarchar(100) = ?
    ,@hierarchy_level int = ?
    ,@inactive nvarchar(16) = ?
    ,@incorporation_date nvarchar(100) = ?
    ,@legalfrm nvarchar(400) = ?
    ,@lei_lei nvarchar(100) = ?
    ,@listed nvarchar(100) = ?
    ,@mainexch nvarchar(400) = ?
    ,@naicsccod2017 nvarchar(2048) = ?
    ,@phone nvarchar(100) = ?
    ,@postcode nvarchar(100) = ?
    ,@previous_names_set_array nvarchar(4096) = ?
    ,@sd_isin nvarchar(100) = ?
    ,@sd_ticker nvarchar(100) = ?
    ,@slegalf nvarchar(200) = ?
    ,@state_us nchar(4) = ?
    ,@subs_count int = ?
    ,@traderegisternr nvarchar(100) = ?
    ,@turnover_eur bigint = ?
    ,@type_of_entity nvarchar(200) = ?
    ,@ultimate_parent_bvdid nvarchar(100) = ?
    ,@ultimate_parent_ctryiso nchar(4) = ?
    ,@ultimate_parent_name nvarchar(2048) = ?
    ,@ussicccod nvarchar(2048) = ?
    ,@vatnumber nvarchar(100) = ?
    ,@website nvarchar(400) = ?
    ,@create_time datetime = ?
    ,@change_time datetime = ?
    ,@changed_by nvarchar(400) = ?
    ,@cambridgecod nvarchar(100) = ?
    ,@cambridgename nvarchar(2048) = ?
;

UPDATE [dbo].[bvd_old] SET 
    [bvd_id] = @bvd_id
    ,[name] = @name
    ,[name_alias] = @name_alias
    ,[name_alias_source] = @name_alias_source
    ,[addr_internat] = @addr_internat
    ,[ambest_id] = @ambest_id
    ,[branch_count] = @branch_count
    ,[category_of_company] = @category_of_company
    ,[city_internat] = @city_internat
    ,[corporate_group_size] = @corporate_group_size
    ,[country] = @country
    ,[county] = @county
    ,[ctryiso] = @ctryiso
    ,[direct_parent_bvdid] = @direct_parent_bvdid
    ,[direct_parent_name_internat] = @direct_parent_name_internat
    ,[ein] = @ein
    ,[email] = @email
    ,[employees] = @employees
    ,[eurovat] = @eurovat
    ,[hierarchy_level] = @hierarchy_level
    ,[inactive] = @inactive
    ,[incorporation_date] = @incorporation_date
    ,[legalfrm] = @legalfrm
    ,[lei_lei] = @lei_lei
    ,[listed] = @listed
    ,[mainexch] = @mainexch
    ,[naicsccod2017] = @naicsccod2017
    ,[phone] = @phone
    ,[postcode] = @postcode
    ,[previous_names_set_array] = @previous_names_set_array
    ,[sd_isin] = @sd_isin
    ,[sd_ticker] = @sd_ticker
    ,[slegalf] = @slegalf
    ,[state_us] = @state_us
    ,[subs_count] = @subs_count
    ,[traderegisternr] = @traderegisternr
    ,[turnover_eur] = @turnover_eur
    ,[type_of_entity] = @type_of_entity
    ,[ultimate_parent_bvdid] = @ultimate_parent_bvdid
    ,[ultimate_parent_ctryiso] = @ultimate_parent_ctryiso
    ,[ultimate_parent_name] = @ultimate_parent_name
    ,[ussicccod] = @ussicccod
    ,[vatnumber] = @vatnumber
    ,[website] = @website
    ,[create_time] = @create_time
    ,[change_time] = @change_time
    ,[changed_by] = @changed_by
    ,[cambridgecod] = @cambridgecod
    ,[cambridgename] = @cambridgename
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, bvd_id, name, name_alias, name_alias_source, addr_internat, ambest_id, branch_count, category_of_company, city_internat, corporate_group_size, country, county, ctryiso, direct_parent_bvdid, direct_parent_name_internat, ein, email, employees, eurovat, hierarchy_level, inactive, incorporation_date, legalfrm, lei_lei, listed, mainexch, naicsccod2017, phone, postcode, previous_names_set_array, sd_isin, sd_ticker, slegalf, state_us, subs_count, traderegisternr, turnover_eur, type_of_entity, ultimate_parent_bvdid, ultimate_parent_ctryiso, ultimate_parent_name, ussicccod, vatnumber, website, create_time, change_time, changed_by, cambridgecod, cambridgename ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[bvd_old]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Company:
        # table
        TableName = 'Company'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company]'
        # columns
        Company_ID = 'Company_ID'
        Company_Name_Clean = 'Company_Name_Clean'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Industry_ID = 'Industry_ID'
        Company_Name = 'Company_Name'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Parent_Company_ID = 'Parent_Company_ID'
        Ultimate_Company_ID = 'Ultimate_Company_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Name_Clean nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(600) = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company] (
    [Company_Name_Clean]
    ,[Country_ISO2_ID]
    ,[City_Unified_Name_ID]
    ,[Industry_ID]
    ,[Company_Name]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Parent_Company_ID]
    ,[Ultimate_Company_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_Name_Clean
    ,@Country_ISO2_ID
    ,@City_Unified_Name_ID
    ,@Industry_ID
    ,@Company_Name
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Parent_Company_ID
    ,@Ultimate_Company_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_Unified_Name_ID bigint = ?
    ,@Industry_ID bigint = ?
    ,@Company_Name nvarchar(600) = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Parent_Company_ID bigint = ?
    ,@Ultimate_Company_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company] SET 
    [Company_Name_Clean] = @Company_Name_Clean
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_Unified_Name_ID] = @City_Unified_Name_ID
    ,[Industry_ID] = @Industry_ID
    ,[Company_Name] = @Company_Name
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Parent_Company_ID] = @Parent_Company_ID
    ,[Ultimate_Company_ID] = @Ultimate_Company_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Company_ID] = @Company_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Company_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
;

DELETE [dbo].[Company]
WHERE
    [Company_ID] = @Company_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID ]).exec_df()

    class claims_bdx_test:
        # table
        TableName = 'claims_bdx_test'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx_test]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Claim_ID_ClientInfo = 'Claim_ID_ClientInfo'
        Is_Claim_Closed = 'Is_Claim_Closed'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Claims_Description = 'Claims_Description'
        Type_of_Loss = 'Type_of_Loss'
        Country_of_Loss_Settlement = 'Country_of_Loss_Settlement'
        Value_as_Of_Date = 'Value_as_Of_Date'
        Loss_Currency = 'Loss_Currency'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        Threshold_Orig_Curr_unind = 'Threshold_Orig_Curr_unind'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Is_Signal_Reserve = 'Is_Signal_Reserve'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Policy_ID_Cleaned: str = None, Is_Signal_Reserve: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Claim_ID_ClientInfo varchar(255) = ?
    ,@Is_Claim_Closed varchar(255) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description varchar(255) = ?
    ,@Type_of_Loss varchar(255) = ?
    ,@Country_of_Loss_Settlement varchar(255) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency varchar(255) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId int = ?
    ,@fileName varchar(255) = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr int = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@Policy_ID_Cleaned varchar(255) = ?
    ,@Is_Signal_Reserve bit = ?
;

INSERT INTO [dbo].[claims_bdx_test] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Claim_ID_ClientInfo]
    ,[Is_Claim_Closed]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Claims_Description]
    ,[Type_of_Loss]
    ,[Country_of_Loss_Settlement]
    ,[Value_as_Of_Date]
    ,[Loss_Currency]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[Threshold_Orig_Curr_unind]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Policy_ID_Cleaned]
    ,[Is_Signal_Reserve]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Claim_ID_ClientInfo
    ,@Is_Claim_Closed
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Claims_Description
    ,@Type_of_Loss
    ,@Country_of_Loss_Settlement
    ,@Value_as_Of_Date
    ,@Loss_Currency
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@Threshold_Orig_Curr_unind
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Policy_ID_Cleaned
    ,@Is_Signal_Reserve
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Policy_ID_Cleaned, Is_Signal_Reserve ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Policy_ID_Cleaned: str = None, Is_Signal_Reserve: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Claim_ID_ClientInfo varchar(255) = ?
    ,@Is_Claim_Closed varchar(255) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description varchar(255) = ?
    ,@Type_of_Loss varchar(255) = ?
    ,@Country_of_Loss_Settlement varchar(255) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency varchar(255) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId int = ?
    ,@fileName varchar(255) = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr int = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@Policy_ID_Cleaned varchar(255) = ?
    ,@Is_Signal_Reserve bit = ?
;

UPDATE [dbo].[claims_bdx_test] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Claim_ID_ClientInfo] = @Claim_ID_ClientInfo
    ,[Is_Claim_Closed] = @Is_Claim_Closed
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Claims_Description] = @Claims_Description
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Country_of_Loss_Settlement] = @Country_of_Loss_Settlement
    ,[Value_as_Of_Date] = @Value_as_Of_Date
    ,[Loss_Currency] = @Loss_Currency
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[Threshold_Orig_Curr_unind] = @Threshold_Orig_Curr_unind
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Policy_ID_Cleaned, Is_Signal_Reserve ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_bdx_test]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Company_V1:
        # table
        TableName = 'Company_V1'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_V1]'
        # columns
        id = 'id'
        duplicate_of_id = 'duplicate_of_id'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Homepage = 'Insured_Homepage'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        manual_curated_entry = 'manual_curated_entry'
        combined_entry = 'combined_entry'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, manual_curated_entry: int = None, combined_entry: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @duplicate_of_id bigint = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@manual_curated_entry bit = ?
    ,@combined_entry bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_V1] (
    [duplicate_of_id]
    ,[Insured_Name_Clean]
    ,[Insured_Name_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ISO2]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Homepage]
    ,[Turnover_ClientInfo_USD]
    ,[manual_curated_entry]
    ,[combined_entry]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @duplicate_of_id
    ,@Insured_Name_Clean
    ,@Insured_Name_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ISO2
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Homepage
    ,@Turnover_ClientInfo_USD
    ,@manual_curated_entry
    ,@combined_entry
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, manual_curated_entry, combined_entry, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, manual_curated_entry: int = None, combined_entry: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@duplicate_of_id bigint = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@manual_curated_entry bit = ?
    ,@combined_entry bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_V1] SET 
    [duplicate_of_id] = @duplicate_of_id
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[manual_curated_entry] = @manual_curated_entry
    ,[combined_entry] = @combined_entry
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, manual_curated_entry, combined_entry, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[Company_V1]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class _06_Countries:
        # table
        TableName = '06_Countries'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_06_Countries]'
        # columns
        Country = 'Country'
        Currency = 'Currency'
        ISO2 = 'ISO2'
        ISO3 = 'ISO3'
        Latitude = 'Latitude'
        Longitude = 'Longitude'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ISO2: str, ISO3: str, Country: str = None, Currency: str = None, Latitude: float = None, Longitude: float = None) -> DataFrame:
            sql = """
DECLARE
    @Country varchar(50) = ?
    ,@Currency varchar(10) = ?
    ,@ISO2 nchar(4) = ?
    ,@ISO3 nchar(6) = ?
    ,@Latitude float = ?
    ,@Longitude float = ?
;

INSERT INTO [dbo].[06_Countries] (
    [Country]
    ,[Currency]
    ,[ISO2]
    ,[ISO3]
    ,[Latitude]
    ,[Longitude]
)
VALUES (
    @Country
    ,@Currency
    ,@ISO2
    ,@ISO3
    ,@Latitude
    ,@Longitude
);
"""
            return DbCmd(self.cnOrStr, sql, [ Country, Currency, ISO2, ISO3, Latitude, Longitude ]).exec_df()

        def update(self, ISO2: str, ISO3: str, Country: str = None, Currency: str = None, Latitude: float = None, Longitude: float = None) -> DataFrame:
            sql = """
DECLARE
    @Country varchar(50) = ?
    ,@Currency varchar(10) = ?
    ,@ISO2 nchar(4) = ?
    ,@ISO3 nchar(6) = ?
    ,@Latitude float = ?
    ,@Longitude float = ?
;

UPDATE [dbo].[06_Countries] SET 
    [Country] = @Country
    ,[Currency] = @Currency
    ,[ISO2] = @ISO2
    ,[ISO3] = @ISO3
    ,[Latitude] = @Latitude
    ,[Longitude] = @Longitude
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Country, Currency, ISO2, ISO3, Latitude, Longitude ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[06_Countries]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_bdx:
        # table
        TableName = 'exposure_bdx'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_RestorationData = 'Coverage_3_Sublimit_RestorationData'
        Coverage_4_Sublimit_Reputation = 'Coverage_4_Sublimit_Reputation'
        Coverage_5_Sublimit_Business_Interruption = 'Coverage_5_Sublimit_Business_Interruption'
        Coverage_6_Sublimit_CBI_IT_Service_Provider = 'Coverage_6_Sublimit_CBI_IT_Service_Provider'
        Coverage_7_Sublimit_CBI_Supply_Chain = 'Coverage_7_Sublimit_CBI_Supply_Chain'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        Coverage_15_Sublimit_System_Failure = 'Coverage_15_Sublimit_System_Failure'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Duplicate_ID = 'Duplicate_ID'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Is_Special_Acceptance = 'Is_Special_Acceptance'
        Client_Limit_Occ_Orig_Curr = 'Client_Limit_Occ_Orig_Curr'
        Full_Limit_Occ_Orig_Curr = 'Full_Limit_Occ_Orig_Curr'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Client_Limit_Occ_USD = 'Client_Limit_Occ_USD'
        Full_Limit_Occ_USD = 'Full_Limit_Occ_USD'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'
        Portfolio_Tag = 'Portfolio_Tag'
        rowNr_deleted_count = 'rowNr_deleted_count'
        Process_ID = 'Process_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Process_ID: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, Portfolio_Tag: str = None, rowNr_deleted_count: int = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@Portfolio_Tag nvarchar(100) = ?
    ,@rowNr_deleted_count int = ?
    ,@Process_ID bigint = ?
;

INSERT INTO [dbo].[exposure_bdx] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_RestorationData]
    ,[Coverage_4_Sublimit_Reputation]
    ,[Coverage_5_Sublimit_Business_Interruption]
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider]
    ,[Coverage_7_Sublimit_CBI_Supply_Chain]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[Coverage_15_Sublimit_System_Failure]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Duplicate_ID]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Is_Special_Acceptance]
    ,[Client_Limit_Occ_Orig_Curr]
    ,[Full_Limit_Occ_Orig_Curr]
    ,[Policy_ID_Cleaned]
    ,[Client_Limit_Occ_USD]
    ,[Full_Limit_Occ_USD]
    ,[Insured_Name_Clean]
    ,[Company_ClientInfo_ID]
    ,[Portfolio_Tag]
    ,[rowNr_deleted_count]
    ,[Process_ID]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_RestorationData
    ,@Coverage_4_Sublimit_Reputation
    ,@Coverage_5_Sublimit_Business_Interruption
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,@Coverage_7_Sublimit_CBI_Supply_Chain
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@Coverage_15_Sublimit_System_Failure
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Duplicate_ID
    ,@ID_Arbeitsvorrat_MR_share
    ,@Is_Special_Acceptance
    ,@Client_Limit_Occ_Orig_Curr
    ,@Full_Limit_Occ_Orig_Curr
    ,@Policy_ID_Cleaned
    ,@Client_Limit_Occ_USD
    ,@Full_Limit_Occ_USD
    ,@Insured_Name_Clean
    ,@Company_ClientInfo_ID
    ,@Portfolio_Tag
    ,@rowNr_deleted_count
    ,@Process_ID
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID, Portfolio_Tag, rowNr_deleted_count, Process_ID ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, rowNr: int, Process_ID: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, Portfolio_Tag: str = None, rowNr_deleted_count: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_RestorationData float = ?
    ,@Coverage_4_Sublimit_Reputation float = ?
    ,@Coverage_5_Sublimit_Business_Interruption float = ?
    ,@Coverage_6_Sublimit_CBI_IT_Service_Provider float = ?
    ,@Coverage_7_Sublimit_CBI_Supply_Chain float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@Coverage_15_Sublimit_System_Failure float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Is_Special_Acceptance bit = ?
    ,@Client_Limit_Occ_Orig_Curr float = ?
    ,@Full_Limit_Occ_Orig_Curr float = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Client_Limit_Occ_USD float = ?
    ,@Full_Limit_Occ_USD float = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@Portfolio_Tag nvarchar(100) = ?
    ,@rowNr_deleted_count int = ?
    ,@Process_ID bigint = ?
;

UPDATE [dbo].[exposure_bdx] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_RestorationData] = @Coverage_3_Sublimit_RestorationData
    ,[Coverage_4_Sublimit_Reputation] = @Coverage_4_Sublimit_Reputation
    ,[Coverage_5_Sublimit_Business_Interruption] = @Coverage_5_Sublimit_Business_Interruption
    ,[Coverage_6_Sublimit_CBI_IT_Service_Provider] = @Coverage_6_Sublimit_CBI_IT_Service_Provider
    ,[Coverage_7_Sublimit_CBI_Supply_Chain] = @Coverage_7_Sublimit_CBI_Supply_Chain
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[Coverage_15_Sublimit_System_Failure] = @Coverage_15_Sublimit_System_Failure
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Duplicate_ID] = @Duplicate_ID
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Is_Special_Acceptance] = @Is_Special_Acceptance
    ,[Client_Limit_Occ_Orig_Curr] = @Client_Limit_Occ_Orig_Curr
    ,[Full_Limit_Occ_Orig_Curr] = @Full_Limit_Occ_Orig_Curr
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Client_Limit_Occ_USD] = @Client_Limit_Occ_USD
    ,[Full_Limit_Occ_USD] = @Full_Limit_Occ_USD
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Company_ClientInfo_ID] = @Company_ClientInfo_ID
    ,[Portfolio_Tag] = @Portfolio_Tag
    ,[rowNr_deleted_count] = @rowNr_deleted_count
    ,[Process_ID] = @Process_ID
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID, Portfolio_Tag, rowNr_deleted_count, Process_ID ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[exposure_bdx]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class _06_Month:
        # table
        TableName = '06_Month'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_06_Month]'
        # columns
        MM = 'MM'
        Month = 'Month'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, MM: int, Month: str) -> DataFrame:
            sql = """
DECLARE
    @MM tinyint = ?
    ,@Month varchar(9) = ?
;

INSERT INTO [dbo].[06_Month] (
    [MM]
    ,[Month]
)
VALUES (
    @MM
    ,@Month
);
"""
            return DbCmd(self.cnOrStr, sql, [ MM, Month ]).exec_df()

        def update(self, MM: int, Month: str) -> DataFrame:
            sql = """
DECLARE
    @MM tinyint = ?
    ,@Month varchar(9) = ?
;

UPDATE [dbo].[06_Month] SET 
    [Month] = @Month
 WHERE
    [MM] = @MM
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ MM, Month ]).exec_df()

        def delete(self, MM: int) -> DataFrame:
            sql = """
DECLARE
    @MM tinyint = ?
;

DELETE [dbo].[06_Month]
WHERE
    [MM] = @MM
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ MM ]).exec_df()

    class re_contracts:
        # table
        TableName = 're_contracts'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[re_contracts]'
        # columns
        re_contract_id = 're_contract_id'
        BuPa = 'BuPa'
        client_name = 'client_name'
        program_id = 'program_id'
        program_name = 'program_name'
        coverage_id = 'coverage_id'
        subsystem_id = 'subsystem_id'
        begin_date = 'begin_date'
        end_date = 'end_date'
        contract_type = 'contract_type'
        max_retention_USD = 'max_retention_USD'
        mr_share = 'mr_share'
        limit_100_percent_USD = 'limit_100_percent_USD'
        isStacked = 'isStacked'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        treaty_name = 'treaty_name'
        add_id = 'add_id'
        protected_share = 'protected_share'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, re_contract_id: int, BuPa: str, program_id: str, coverage_id: str, subsystem_id: str, begin_date: date, end_date: date, contract_type: str, client_name: str = None, program_name: str = None, max_retention_USD: float = None, mr_share: float = None, limit_100_percent_USD: float = None, isStacked: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, treaty_name: str = None, add_id: str = None, protected_share: float = None) -> DataFrame:
            sql = """
DECLARE
    @re_contract_id bigint = ?
    ,@BuPa nvarchar(480) = ?
    ,@client_name nvarchar(1000) = ?
    ,@program_id nvarchar(100) = ?
    ,@program_name nvarchar(2048) = ?
    ,@coverage_id nvarchar(100) = ?
    ,@subsystem_id nvarchar(100) = ?
    ,@begin_date date(0) = ?
    ,@end_date date(0) = ?
    ,@contract_type nvarchar(400) = ?
    ,@max_retention_USD float = ?
    ,@mr_share float = ?
    ,@limit_100_percent_USD float = ?
    ,@isStacked bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@treaty_name nvarchar(2048) = ?
    ,@add_id nvarchar(100) = ?
    ,@protected_share float = ?
;

INSERT INTO [dbo].[re_contracts] (
    [re_contract_id]
    ,[BuPa]
    ,[client_name]
    ,[program_id]
    ,[program_name]
    ,[coverage_id]
    ,[subsystem_id]
    ,[begin_date]
    ,[end_date]
    ,[contract_type]
    ,[max_retention_USD]
    ,[mr_share]
    ,[limit_100_percent_USD]
    ,[isStacked]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[treaty_name]
    ,[add_id]
    ,[protected_share]
)
VALUES (
    @re_contract_id
    ,@BuPa
    ,@client_name
    ,@program_id
    ,@program_name
    ,@coverage_id
    ,@subsystem_id
    ,@begin_date
    ,@end_date
    ,@contract_type
    ,@max_retention_USD
    ,@mr_share
    ,@limit_100_percent_USD
    ,@isStacked
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@treaty_name
    ,@add_id
    ,@protected_share
);
"""
            return DbCmd(self.cnOrStr, sql, [ re_contract_id, BuPa, client_name, program_id, program_name, coverage_id, subsystem_id, begin_date, end_date, contract_type, max_retention_USD, mr_share, limit_100_percent_USD, isStacked, Create_Time, Change_Time, Changed_By, treaty_name, add_id, protected_share ]).exec_df()

        def update(self, re_contract_id: int, BuPa: str, program_id: str, coverage_id: str, subsystem_id: str, begin_date: date, end_date: date, contract_type: str, client_name: str = None, program_name: str = None, max_retention_USD: float = None, mr_share: float = None, limit_100_percent_USD: float = None, isStacked: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, treaty_name: str = None, add_id: str = None, protected_share: float = None) -> DataFrame:
            sql = """
DECLARE
    @re_contract_id bigint = ?
    ,@BuPa nvarchar(480) = ?
    ,@client_name nvarchar(1000) = ?
    ,@program_id nvarchar(100) = ?
    ,@program_name nvarchar(2048) = ?
    ,@coverage_id nvarchar(100) = ?
    ,@subsystem_id nvarchar(100) = ?
    ,@begin_date date(0) = ?
    ,@end_date date(0) = ?
    ,@contract_type nvarchar(400) = ?
    ,@max_retention_USD float = ?
    ,@mr_share float = ?
    ,@limit_100_percent_USD float = ?
    ,@isStacked bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@treaty_name nvarchar(2048) = ?
    ,@add_id nvarchar(100) = ?
    ,@protected_share float = ?
;

UPDATE [dbo].[re_contracts] SET 
    [BuPa] = @BuPa
    ,[client_name] = @client_name
    ,[program_id] = @program_id
    ,[program_name] = @program_name
    ,[coverage_id] = @coverage_id
    ,[subsystem_id] = @subsystem_id
    ,[begin_date] = @begin_date
    ,[end_date] = @end_date
    ,[contract_type] = @contract_type
    ,[max_retention_USD] = @max_retention_USD
    ,[mr_share] = @mr_share
    ,[limit_100_percent_USD] = @limit_100_percent_USD
    ,[isStacked] = @isStacked
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[treaty_name] = @treaty_name
    ,[add_id] = @add_id
    ,[protected_share] = @protected_share
 WHERE
    [re_contract_id] = @re_contract_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ re_contract_id, BuPa, client_name, program_id, program_name, coverage_id, subsystem_id, begin_date, end_date, contract_type, max_retention_USD, mr_share, limit_100_percent_USD, isStacked, Create_Time, Change_Time, Changed_By, treaty_name, add_id, protected_share ]).exec_df()

        def delete(self, re_contract_id: int) -> DataFrame:
            sql = """
DECLARE
    @re_contract_id bigint = ?
;

DELETE [dbo].[re_contracts]
WHERE
    [re_contract_id] = @re_contract_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ re_contract_id ]).exec_df()

    class Client_Name_Hierarchy_Level:
        # table
        TableName = 'Client_Name_Hierarchy_Level'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Client_Name_Hierarchy_Level]'
        # columns
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Purpose = 'Purpose'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Hierarchy_Level_Id: int, Purpose: str) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_Id int = ?
    ,@Purpose nvarchar(200) = ?
;

INSERT INTO [dbo].[Client_Name_Hierarchy_Level] (
    [Hierarchy_Level_Id]
    ,[Purpose]
)
VALUES (
    @Hierarchy_Level_Id
    ,@Purpose
);
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_Id, Purpose ]).exec_df()

        def update(self, Hierarchy_Level_Id: int, Purpose: str) -> DataFrame:
            sql = """
DECLARE
    @Hierarchy_Level_Id int = ?
    ,@Purpose nvarchar(200) = ?
;

UPDATE [dbo].[Client_Name_Hierarchy_Level] SET 
    [Hierarchy_Level_Id] = @Hierarchy_Level_Id
    ,[Purpose] = @Purpose
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Hierarchy_Level_Id, Purpose ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Client_Name_Hierarchy_Level]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class file_level_status:
        # table
        TableName = 'file_level_status'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_status]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        runScript = 'runScript'
        runNorm = 'runNorm'
        runBvD = 'runBvD'
        runMRShares = 'runMRShares'
        runValidation = 'runValidation'
        runClaimsLinking = 'runClaimsLinking'
        runGenerateOutput = 'runGenerateOutput'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Process_ID = 'Process_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, Process_ID: int, runScript: int = None, runNorm: int = None, runBvD: int = None, runMRShares: int = None, runValidation: int = None, runClaimsLinking: int = None, runGenerateOutput: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@runScript bit = ?
    ,@runNorm bit = ?
    ,@runBvD bit = ?
    ,@runMRShares bit = ?
    ,@runValidation bit = ?
    ,@runClaimsLinking bit = ?
    ,@runGenerateOutput bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Process_ID bigint = ?
;

INSERT INTO [dbo].[file_level_status] (
    [ID_Arbeitsvorrat]
    ,[runScript]
    ,[runNorm]
    ,[runBvD]
    ,[runMRShares]
    ,[runValidation]
    ,[runClaimsLinking]
    ,[runGenerateOutput]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Process_ID]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@runScript
    ,@runNorm
    ,@runBvD
    ,@runMRShares
    ,@runValidation
    ,@runClaimsLinking
    ,@runGenerateOutput
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Process_ID
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, runGenerateOutput, Create_Time, Change_Time, Changed_By, Process_ID ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, Process_ID: int, runScript: int = None, runNorm: int = None, runBvD: int = None, runMRShares: int = None, runValidation: int = None, runClaimsLinking: int = None, runGenerateOutput: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@runScript bit = ?
    ,@runNorm bit = ?
    ,@runBvD bit = ?
    ,@runMRShares bit = ?
    ,@runValidation bit = ?
    ,@runClaimsLinking bit = ?
    ,@runGenerateOutput bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Process_ID bigint = ?
;

UPDATE [dbo].[file_level_status] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[runScript] = @runScript
    ,[runNorm] = @runNorm
    ,[runBvD] = @runBvD
    ,[runMRShares] = @runMRShares
    ,[runValidation] = @runValidation
    ,[runClaimsLinking] = @runClaimsLinking
    ,[runGenerateOutput] = @runGenerateOutput
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Process_ID] = @Process_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, runGenerateOutput, Create_Time, Change_Time, Changed_By, Process_ID ]).exec_df()

        def delete(self, Process_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Process_ID bigint = ?
;

DELETE [dbo].[file_level_status]
WHERE
    [Process_ID] = @Process_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Process_ID ]).exec_df()

    class fileID2re_contracts:
        # table
        TableName = 'fileID2re_contracts'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[fileID2re_contracts]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        re_contract_id = 're_contract_id'
        uw_selection = 'uw_selection'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, uw_selection: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@re_contract_id bigint = ?
    ,@uw_selection bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[fileID2re_contracts] (
    [ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[re_contract_id]
    ,[uw_selection]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@re_contract_id
    ,@uw_selection
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, re_contract_id, uw_selection, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, uw_selection: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@re_contract_id bigint = ?
    ,@uw_selection bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[fileID2re_contracts] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[re_contract_id] = @re_contract_id
    ,[uw_selection] = @uw_selection
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, re_contract_id, uw_selection, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[fileID2re_contracts]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class z_Attachment_Bands:
        # table
        TableName = 'z_Attachment_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Attachment_Bands]'
        # columns
        Aggr__Bands = 'Aggr. Bands'
        Band_Name = 'Band Name'
        Rank = 'Rank'
        Rank2 = 'Rank2'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Aggr__Bands: str, Band_Name: str, Rank: int, Rank2: int) -> DataFrame:
            sql = """
DECLARE
    @Aggr__Bands varchar(15) = ?
    ,@Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
    ,@Rank2 tinyint = ?
;

INSERT INTO [dbo].[z_Attachment_Bands] (
    [Aggr. Bands]
    ,[Band Name]
    ,[Rank]
    ,[Rank2]
)
VALUES (
    @Aggr__Bands
    ,@Band_Name
    ,@Rank
    ,@Rank2
);
"""
            return DbCmd(self.cnOrStr, sql, [ Aggr__Bands, Band_Name, Rank, Rank2 ]).exec_df()

        def update(self, Aggr__Bands: str, Band_Name: str, Rank: int, Rank2: int) -> DataFrame:
            sql = """
DECLARE
    @Aggr__Bands varchar(15) = ?
    ,@Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
    ,@Rank2 tinyint = ?
;

UPDATE [dbo].[z_Attachment_Bands] SET 
    [Aggr. Bands] = @Aggr__Bands
    ,[Band Name] = @Band_Name
    ,[Rank2] = @Rank2
 WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Aggr__Bands, Band_Name, Rank, Rank2 ]).exec_df()

        def delete(self, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Rank tinyint = ?
;

DELETE [dbo].[z_Attachment_Bands]
WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rank ]).exec_df()

    class Dim_Industry:
        # table
        TableName = 'Dim_Industry'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Industry]'
        # columns
        Industry_ID = 'Industry_ID'
        Industry_Code_Standard = 'Industry_Code_Standard'
        Industry_Code_Number = 'Industry_Code_Number'
        Industry_Description = 'Industry_Description'
        Year = 'Year'
        Is_Custom = 'Is_Custom'
        SIC_ID = 'SIC_ID'
        NAICS_ID = 'NAICS_ID'
        NACE_ID = 'NACE_ID'
        Cambridge_ID = 'Cambridge_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Industry_Code_Standard: str, Industry_Description: str, Industry_Code_Number: str = None, Year: int = None, Is_Custom: int = None, SIC_ID: int = None, NAICS_ID: int = None, NACE_ID: int = None, Cambridge_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Industry_Code_Standard nvarchar(400) = ?
    ,@Industry_Code_Number nvarchar(100) = ?
    ,@Industry_Description nvarchar(2048) = ?
    ,@Year int = ?
    ,@Is_Custom bit = ?
    ,@SIC_ID bigint = ?
    ,@NAICS_ID bigint = ?
    ,@NACE_ID bigint = ?
    ,@Cambridge_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Industry] (
    [Industry_Code_Standard]
    ,[Industry_Code_Number]
    ,[Industry_Description]
    ,[Year]
    ,[Is_Custom]
    ,[SIC_ID]
    ,[NAICS_ID]
    ,[NACE_ID]
    ,[Cambridge_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Industry_Code_Standard
    ,@Industry_Code_Number
    ,@Industry_Description
    ,@Year
    ,@Is_Custom
    ,@SIC_ID
    ,@NAICS_ID
    ,@NACE_ID
    ,@Cambridge_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Industry_Code_Standard, Industry_Code_Number, Industry_Description, Year, Is_Custom, SIC_ID, NAICS_ID, NACE_ID, Cambridge_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Industry_ID: int, Industry_Code_Standard: str, Industry_Description: str, Industry_Code_Number: str = None, Year: int = None, Is_Custom: int = None, SIC_ID: int = None, NAICS_ID: int = None, NACE_ID: int = None, Cambridge_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Industry_ID bigint = ?
    ,@Industry_Code_Standard nvarchar(400) = ?
    ,@Industry_Code_Number nvarchar(100) = ?
    ,@Industry_Description nvarchar(2048) = ?
    ,@Year int = ?
    ,@Is_Custom bit = ?
    ,@SIC_ID bigint = ?
    ,@NAICS_ID bigint = ?
    ,@NACE_ID bigint = ?
    ,@Cambridge_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Industry] SET 
    [Industry_Code_Standard] = @Industry_Code_Standard
    ,[Industry_Code_Number] = @Industry_Code_Number
    ,[Industry_Description] = @Industry_Description
    ,[Year] = @Year
    ,[Is_Custom] = @Is_Custom
    ,[SIC_ID] = @SIC_ID
    ,[NAICS_ID] = @NAICS_ID
    ,[NACE_ID] = @NACE_ID
    ,[Cambridge_ID] = @Cambridge_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Industry_ID] = @Industry_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Industry_ID, Industry_Code_Standard, Industry_Code_Number, Industry_Description, Year, Is_Custom, SIC_ID, NAICS_ID, NACE_ID, Cambridge_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Industry_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Industry_ID bigint = ?
;

DELETE [dbo].[Dim_Industry]
WHERE
    [Industry_ID] = @Industry_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Industry_ID ]).exec_df()

    class z_Client_Limit_Bands:
        # table
        TableName = 'z_Client_Limit_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Client_Limit_Bands]'
        # columns
        Band_Name = 'Band_Name'
        Rank = 'Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

INSERT INTO [dbo].[z_Client_Limit_Bands] (
    [Band_Name]
    ,[Rank]
)
VALUES (
    @Band_Name
    ,@Rank
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def update(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

UPDATE [dbo].[z_Client_Limit_Bands] SET 
    [Band_Name] = @Band_Name
 WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def delete(self, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Rank tinyint = ?
;

DELETE [dbo].[z_Client_Limit_Bands]
WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rank ]).exec_df()

    class z_Full_Limit_Bands:
        # table
        TableName = 'z_Full_Limit_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Full_Limit_Bands]'
        # columns
        Band_Name = 'Band Name'
        Rank = 'Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

INSERT INTO [dbo].[z_Full_Limit_Bands] (
    [Band Name]
    ,[Rank]
)
VALUES (
    @Band_Name
    ,@Rank
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def update(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

UPDATE [dbo].[z_Full_Limit_Bands] SET 
    [Band Name] = @Band_Name
 WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def delete(self, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Rank tinyint = ?
;

DELETE [dbo].[z_Full_Limit_Bands]
WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rank ]).exec_df()

    class z_Percentage_Band:
        # table
        TableName = 'z_Percentage_Band'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Percentage_Band]'
        # columns
        BandName = 'BandName'
        PercentValue = 'PercentValue'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, BandName: str, PercentValue: float) -> DataFrame:
            sql = """
DECLARE
    @BandName varchar(15) = ?
    ,@PercentValue decimal(2, 1) = ?
;

INSERT INTO [dbo].[z_Percentage_Band] (
    [BandName]
    ,[PercentValue]
)
VALUES (
    @BandName
    ,@PercentValue
);
"""
            return DbCmd(self.cnOrStr, sql, [ BandName, PercentValue ]).exec_df()

        def update(self, BandName: str, PercentValue: float) -> DataFrame:
            sql = """
DECLARE
    @BandName varchar(15) = ?
    ,@PercentValue decimal(2, 1) = ?
;

UPDATE [dbo].[z_Percentage_Band] SET 
    [BandName] = @BandName
    ,[PercentValue] = @PercentValue
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ BandName, PercentValue ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[z_Percentage_Band]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Claim:
        # table
        TableName = 'Claim'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Claim]'
        # columns
        Claim_ID = 'Claim_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Company_ID = 'Company_ID'
        Claim_Ref_Clean = 'Claim_Ref_Clean'
        Claim_Ref = 'Claim_Ref'
        Date_of_Loss = 'Date_of_Loss'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Exposure_ID = 'Exposure_ID'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Type_of_Loss = 'Type_of_Loss'
        Loss_Event_ID = 'Loss_Event_ID'
        Claims_Description = 'Claims_Description'
        Policy_Ref = 'Policy_Ref'
        Policy_Ref_Clean = 'Policy_Ref_Clean'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Currency_ID = 'Policy_Currency_ID'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Parent_ID = 'Parent_ID'
        Ultimate_Parent_ID = 'Ultimate_Parent_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @Technical_Client_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Claim_Ref_Clean nvarchar(1000) = ?
    ,@Claim_Ref nvarchar(1000) = ?
    ,@Date_of_Loss date(0) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Exposure_ID bigint = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Parent_ID bigint = ?
    ,@Ultimate_Parent_ID bigint = ?
;

INSERT INTO [dbo].[Claim] (
    [Technical_Client_ID]
    ,[Company_ID]
    ,[Claim_Ref_Clean]
    ,[Claim_Ref]
    ,[Date_of_Loss]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Exposure_ID]
    ,[Country_ISO2_ID]
    ,[Type_of_Loss]
    ,[Loss_Event_ID]
    ,[Claims_Description]
    ,[Policy_Ref]
    ,[Policy_Ref_Clean]
    ,[Policy_Inception_Date]
    ,[Policy_Currency_ID]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Parent_ID]
    ,[Ultimate_Parent_ID]
)
VALUES (
    @Technical_Client_ID
    ,@Company_ID
    ,@Claim_Ref_Clean
    ,@Claim_Ref
    ,@Date_of_Loss
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Exposure_ID
    ,@Country_ISO2_ID
    ,@Type_of_Loss
    ,@Loss_Event_ID
    ,@Claims_Description
    ,@Policy_Ref
    ,@Policy_Ref_Clean
    ,@Policy_Inception_Date
    ,@Policy_Currency_ID
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Parent_ID
    ,@Ultimate_Parent_ID
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ]).exec_df()

        def update(self, Claim_ID: int, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Claim_Ref_Clean nvarchar(1000) = ?
    ,@Claim_Ref nvarchar(1000) = ?
    ,@Date_of_Loss date(0) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Exposure_ID bigint = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Parent_ID bigint = ?
    ,@Ultimate_Parent_ID bigint = ?
;

UPDATE [dbo].[Claim] SET 
    [Technical_Client_ID] = @Technical_Client_ID
    ,[Company_ID] = @Company_ID
    ,[Claim_Ref_Clean] = @Claim_Ref_Clean
    ,[Claim_Ref] = @Claim_Ref
    ,[Date_of_Loss] = @Date_of_Loss
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Exposure_ID] = @Exposure_ID
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Loss_Event_ID] = @Loss_Event_ID
    ,[Claims_Description] = @Claims_Description
    ,[Policy_Ref] = @Policy_Ref
    ,[Policy_Ref_Clean] = @Policy_Ref_Clean
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Currency_ID] = @Policy_Currency_ID
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Parent_ID] = @Parent_ID
    ,[Ultimate_Parent_ID] = @Ultimate_Parent_ID
 WHERE
    [Claim_ID] = @Claim_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ID, Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ]).exec_df()

        def delete(self, Claim_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Claim_ID bigint = ?
;

DELETE [dbo].[Claim]
WHERE
    [Claim_ID] = @Claim_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ID ]).exec_df()

    class z_Percentage_Band_Dev:
        # table
        TableName = 'z_Percentage_Band_Dev'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Percentage_Band_Dev]'
        # columns
        BandName = 'BandName'
        PercentValue = 'PercentValue'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, BandName: str, PercentValue: float) -> DataFrame:
            sql = """
DECLARE
    @BandName varchar(10) = ?
    ,@PercentValue decimal(6, 1) = ?
;

INSERT INTO [dbo].[z_Percentage_Band_Dev] (
    [BandName]
    ,[PercentValue]
)
VALUES (
    @BandName
    ,@PercentValue
);
"""
            return DbCmd(self.cnOrStr, sql, [ BandName, PercentValue ]).exec_df()

        def update(self, BandName: str, PercentValue: float) -> DataFrame:
            sql = """
DECLARE
    @BandName varchar(10) = ?
    ,@PercentValue decimal(6, 1) = ?
;

UPDATE [dbo].[z_Percentage_Band_Dev] SET 
    [BandName] = @BandName
    ,[PercentValue] = @PercentValue
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ BandName, PercentValue ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[z_Percentage_Band_Dev]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class file_level_meta_information_20220601:
        # table
        TableName = 'file_level_meta_information_20220601'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[file_level_meta_information_20220601]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        BU = 'BU'
        Client_Name = 'Client_Name'
        Orig_File_Name = 'Orig_File_Name'
        bdx_type = 'bdx_type'
        Subsystem_ID = 'Subsystem_ID'
        Treaty_Programm_ID = 'Treaty_Programm_ID'
        FSRI_ID = 'FSRI_ID'
        UW_Year = 'UW_Year'
        Smart_matching_done = 'Smart_matching_done'
        R_code_done = 'R_code_done'
        Q_A_done = 'Q_A_done'
        Function_ran = 'Function_ran'
        Validation_issues_done = 'Validation_issues_done'
        Four_Eye_Check_done = 'Four_Eye_Check_done'
        Signoff_done = 'Signoff_done'
        Contact_Data_Team = 'Contact_Data_Team'
        Responsible_Four_Eye_Check = 'Responsible_Four_Eye_Check'
        UW = 'UW'
        PML_needs_to_be_done = 'PML_needs_to_be_done'
        PML_has_been_done = 'PML_has_been_done'
        SRAC_needs_to_be_done = 'SRAC_needs_to_be_done'
        SRAC_has_been_done = 'SRAC_has_been_done'
        Priority = 'Priority'
        Deadline = 'Deadline'
        Exclude_from_dashboards = 'Exclude_from_dashboards'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[file_level_meta_information_20220601] (
    [ID_Arbeitsvorrat]
    ,[BU]
    ,[Client_Name]
    ,[Orig_File_Name]
    ,[bdx_type]
    ,[Subsystem_ID]
    ,[Treaty_Programm_ID]
    ,[FSRI_ID]
    ,[UW_Year]
    ,[Smart_matching_done]
    ,[R_code_done]
    ,[Q_A_done]
    ,[Function_ran]
    ,[Validation_issues_done]
    ,[Four_Eye_Check_done]
    ,[Signoff_done]
    ,[Contact_Data_Team]
    ,[Responsible_Four_Eye_Check]
    ,[UW]
    ,[PML_needs_to_be_done]
    ,[PML_has_been_done]
    ,[SRAC_needs_to_be_done]
    ,[SRAC_has_been_done]
    ,[Priority]
    ,[Deadline]
    ,[Exclude_from_dashboards]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@BU
    ,@Client_Name
    ,@Orig_File_Name
    ,@bdx_type
    ,@Subsystem_ID
    ,@Treaty_Programm_ID
    ,@FSRI_ID
    ,@UW_Year
    ,@Smart_matching_done
    ,@R_code_done
    ,@Q_A_done
    ,@Function_ran
    ,@Validation_issues_done
    ,@Four_Eye_Check_done
    ,@Signoff_done
    ,@Contact_Data_Team
    ,@Responsible_Four_Eye_Check
    ,@UW
    ,@PML_needs_to_be_done
    ,@PML_has_been_done
    ,@SRAC_needs_to_be_done
    ,@SRAC_has_been_done
    ,@Priority
    ,@Deadline
    ,@Exclude_from_dashboards
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@BU nvarchar(100) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@Orig_File_Name nvarchar(2048) = ?
    ,@bdx_type nvarchar(100) = ?
    ,@Subsystem_ID nvarchar(100) = ?
    ,@Treaty_Programm_ID nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@UW_Year bigint = ?
    ,@Smart_matching_done nvarchar(100) = ?
    ,@R_code_done nvarchar(100) = ?
    ,@Q_A_done nvarchar(100) = ?
    ,@Function_ran nvarchar(100) = ?
    ,@Validation_issues_done nvarchar(100) = ?
    ,@Four_Eye_Check_done nvarchar(100) = ?
    ,@Signoff_done nvarchar(100) = ?
    ,@Contact_Data_Team nvarchar(100) = ?
    ,@Responsible_Four_Eye_Check nvarchar(100) = ?
    ,@UW nvarchar(400) = ?
    ,@PML_needs_to_be_done nvarchar(100) = ?
    ,@PML_has_been_done nvarchar(100) = ?
    ,@SRAC_needs_to_be_done nvarchar(100) = ?
    ,@SRAC_has_been_done nvarchar(100) = ?
    ,@Priority nvarchar(100) = ?
    ,@Deadline datetime = ?
    ,@Exclude_from_dashboards bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[file_level_meta_information_20220601] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[BU] = @BU
    ,[Client_Name] = @Client_Name
    ,[Orig_File_Name] = @Orig_File_Name
    ,[bdx_type] = @bdx_type
    ,[Subsystem_ID] = @Subsystem_ID
    ,[Treaty_Programm_ID] = @Treaty_Programm_ID
    ,[FSRI_ID] = @FSRI_ID
    ,[UW_Year] = @UW_Year
    ,[Smart_matching_done] = @Smart_matching_done
    ,[R_code_done] = @R_code_done
    ,[Q_A_done] = @Q_A_done
    ,[Function_ran] = @Function_ran
    ,[Validation_issues_done] = @Validation_issues_done
    ,[Four_Eye_Check_done] = @Four_Eye_Check_done
    ,[Signoff_done] = @Signoff_done
    ,[Contact_Data_Team] = @Contact_Data_Team
    ,[Responsible_Four_Eye_Check] = @Responsible_Four_Eye_Check
    ,[UW] = @UW
    ,[PML_needs_to_be_done] = @PML_needs_to_be_done
    ,[PML_has_been_done] = @PML_has_been_done
    ,[SRAC_needs_to_be_done] = @SRAC_needs_to_be_done
    ,[SRAC_has_been_done] = @SRAC_has_been_done
    ,[Priority] = @Priority
    ,[Deadline] = @Deadline
    ,[Exclude_from_dashboards] = @Exclude_from_dashboards
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[file_level_meta_information_20220601]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_linking:
        # table
        TableName = 'claims_linking'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_linking]'
        # columns
        id = 'id'
        claim_id = 'claim_id'
        risk_id = 'risk_id'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, claim_id: int, risk_id: int = None) -> DataFrame:
            sql = """
DECLARE
    @claim_id bigint = ?
    ,@risk_id bigint = ?
;

INSERT INTO [dbo].[claims_linking] (
    [claim_id]
    ,[risk_id]
)
VALUES (
    @claim_id
    ,@risk_id
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ claim_id, risk_id ]).exec_df()

        def update(self, id: int, claim_id: int, risk_id: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@claim_id bigint = ?
    ,@risk_id bigint = ?
;

UPDATE [dbo].[claims_linking] SET 
    [claim_id] = @claim_id
    ,[risk_id] = @risk_id
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, claim_id, risk_id ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[claims_linking]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class z_Premium_Bands:
        # table
        TableName = 'z_Premium_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Premium_Bands]'
        # columns
        Band_Name = 'Band_Name'
        Rank = 'Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

INSERT INTO [dbo].[z_Premium_Bands] (
    [Band_Name]
    ,[Rank]
)
VALUES (
    @Band_Name
    ,@Rank
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def update(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

UPDATE [dbo].[z_Premium_Bands] SET 
    [Band_Name] = @Band_Name
 WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def delete(self, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Rank tinyint = ?
;

DELETE [dbo].[z_Premium_Bands]
WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rank ]).exec_df()

    class data_entry_meta_information_20220601:
        # table
        TableName = 'data_entry_meta_information_20220601'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[data_entry_meta_information_20220601]'
        # columns
        id = 'id'
        File_Name = 'File_Name'
        UW = 'UW'
        BU = 'BU'
        Number_Risks = 'Number_Risks'
        Number_Claims = 'Number_Claims'
        bdx_type = 'bdx_type'
        As_at_Date = 'As_at_Date'
        Subsystem = 'Subsystem'
        Program_IDs = 'Program_IDs'
        Additional_Program_IDs = 'Additional_Program_IDs'
        FSRI_ID = 'FSRI_ID'
        Client_Name = 'Client_Name'
        BuPa = 'BuPa'
        Treaty_Program_Name = 'Treaty_Program_Name'
        Begin_Date = 'Begin_Date'
        End_Date = 'End_Date'
        UW_Year = 'UW_Year'
        Is_From_CU_Collection = 'Is_From_CU_Collection'
        Is_Ignored = 'Is_Ignored'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[data_entry_meta_information_20220601] (
    [id]
    ,[File_Name]
    ,[UW]
    ,[BU]
    ,[Number_Risks]
    ,[Number_Claims]
    ,[bdx_type]
    ,[As_at_Date]
    ,[Subsystem]
    ,[Program_IDs]
    ,[Additional_Program_IDs]
    ,[FSRI_ID]
    ,[Client_Name]
    ,[BuPa]
    ,[Treaty_Program_Name]
    ,[Begin_Date]
    ,[End_Date]
    ,[UW_Year]
    ,[Is_From_CU_Collection]
    ,[Is_Ignored]
    ,[ID_Arbeitsvorrat]
    ,[Comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@File_Name
    ,@UW
    ,@BU
    ,@Number_Risks
    ,@Number_Claims
    ,@bdx_type
    ,@As_at_Date
    ,@Subsystem
    ,@Program_IDs
    ,@Additional_Program_IDs
    ,@FSRI_ID
    ,@Client_Name
    ,@BuPa
    ,@Treaty_Program_Name
    ,@Begin_Date
    ,@End_Date
    ,@UW_Year
    ,@Is_From_CU_Collection
    ,@Is_Ignored
    ,@ID_Arbeitsvorrat
    ,@Comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@File_Name nvarchar(2048) = ?
    ,@UW nvarchar(400) = ?
    ,@BU nvarchar(100) = ?
    ,@Number_Risks bigint = ?
    ,@Number_Claims bigint = ?
    ,@bdx_type nvarchar(100) = ?
    ,@As_at_Date date(0) = ?
    ,@Subsystem nvarchar(100) = ?
    ,@Program_IDs nvarchar(400) = ?
    ,@Additional_Program_IDs nvarchar(400) = ?
    ,@FSRI_ID nvarchar(400) = ?
    ,@Client_Name nvarchar(2048) = ?
    ,@BuPa nvarchar(100) = ?
    ,@Treaty_Program_Name nvarchar(2048) = ?
    ,@Begin_Date date(0) = ?
    ,@End_Date date(0) = ?
    ,@UW_Year bigint = ?
    ,@Is_From_CU_Collection bit = ?
    ,@Is_Ignored bit = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Comment nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[data_entry_meta_information_20220601] SET 
    [id] = @id
    ,[File_Name] = @File_Name
    ,[UW] = @UW
    ,[BU] = @BU
    ,[Number_Risks] = @Number_Risks
    ,[Number_Claims] = @Number_Claims
    ,[bdx_type] = @bdx_type
    ,[As_at_Date] = @As_at_Date
    ,[Subsystem] = @Subsystem
    ,[Program_IDs] = @Program_IDs
    ,[Additional_Program_IDs] = @Additional_Program_IDs
    ,[FSRI_ID] = @FSRI_ID
    ,[Client_Name] = @Client_Name
    ,[BuPa] = @BuPa
    ,[Treaty_Program_Name] = @Treaty_Program_Name
    ,[Begin_Date] = @Begin_Date
    ,[End_Date] = @End_Date
    ,[UW_Year] = @UW_Year
    ,[Is_From_CU_Collection] = @Is_From_CU_Collection
    ,[Is_Ignored] = @Is_Ignored
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Comment] = @Comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[data_entry_meta_information_20220601]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_company_linking:
        # table
        TableName = 'claims_company_linking'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_company_linking]'
        # columns
        id = 'id'
        company_id = 'company_id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@company_id bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[claims_company_linking] (
    [id]
    ,[company_id]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@company_id
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, company_id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@company_id bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[claims_company_linking] SET 
    [company_id] = @company_id
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, company_id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[claims_company_linking]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class claims_bdx:
        # table
        TableName = 'claims_bdx'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Claim_ID_ClientInfo = 'Claim_ID_ClientInfo'
        Is_Claim_Closed = 'Is_Claim_Closed'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Claims_Description = 'Claims_Description'
        Type_of_Loss = 'Type_of_Loss'
        Country_of_Loss_Settlement = 'Country_of_Loss_Settlement'
        Value_as_Of_Date = 'Value_as_Of_Date'
        Loss_Currency = 'Loss_Currency'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        Threshold_Orig_Curr_unind = 'Threshold_Orig_Curr_unind'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        IsCensored = 'IsCensored'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Incurred_Insured_FGU_USD = 'Incurred_Insured_FGU_USD'
        Paid_Client_Share_USD = 'Paid_Client_Share_USD'
        Incurred_Client_Share_USD = 'Incurred_Client_Share_USD'
        Threshold_unind_USD = 'Threshold_unind_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Country_of_Loss_Settlement_ISO2 = 'Country_of_Loss_Settlement_ISO2'
        Duplicate_ID = 'Duplicate_ID'
        Internal_Claim_ID = 'Internal_Claim_ID'
        Is_Signal_Reserve = 'Is_Signal_Reserve'
        Loss_Event_ClientInfo = 'Loss_Event_ClientInfo'
        Loss_Event_normalized = 'Loss_Event_normalized'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Claim_Closed_Date = 'Claim_Closed_Date'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'
        rowNr_deleted_count = 'rowNr_deleted_count'
        Process_ID = 'Process_ID'
        Claim_ClientInfo_ID = 'Claim_ClientInfo_ID'
        Claim_ID_Cleaned = 'Claim_ID_Cleaned'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Process_ID: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, rowNr_deleted_count: int = None, Claim_ClientInfo_ID: int = None, Claim_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event_normalized nvarchar(2048) = ?
    ,@Product_Name_ClientInfo nvarchar(2048) = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@rowNr_deleted_count int = ?
    ,@Process_ID bigint = ?
    ,@Claim_ClientInfo_ID bigint = ?
    ,@Claim_ID_Cleaned nvarchar(1000) = ?
;

INSERT INTO [dbo].[claims_bdx] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Claim_ID_ClientInfo]
    ,[Is_Claim_Closed]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Claims_Description]
    ,[Type_of_Loss]
    ,[Country_of_Loss_Settlement]
    ,[Value_as_Of_Date]
    ,[Loss_Currency]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[Threshold_Orig_Curr_unind]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[IsCensored]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Incurred_Insured_FGU_USD]
    ,[Paid_Client_Share_USD]
    ,[Incurred_Client_Share_USD]
    ,[Threshold_unind_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Country_of_Loss_Settlement_ISO2]
    ,[Duplicate_ID]
    ,[Internal_Claim_ID]
    ,[Is_Signal_Reserve]
    ,[Loss_Event_ClientInfo]
    ,[Loss_Event_normalized]
    ,[Product_Name_ClientInfo]
    ,[Claim_Closed_Date]
    ,[Policy_ID_Cleaned]
    ,[Insured_Name_Clean]
    ,[Company_ClientInfo_ID]
    ,[rowNr_deleted_count]
    ,[Process_ID]
    ,[Claim_ClientInfo_ID]
    ,[Claim_ID_Cleaned]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Claim_ID_ClientInfo
    ,@Is_Claim_Closed
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Claims_Description
    ,@Type_of_Loss
    ,@Country_of_Loss_Settlement
    ,@Value_as_Of_Date
    ,@Loss_Currency
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@Threshold_Orig_Curr_unind
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@IsCensored
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Incurred_Insured_FGU_USD
    ,@Paid_Client_Share_USD
    ,@Incurred_Client_Share_USD
    ,@Threshold_unind_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Country_of_Loss_Settlement_ISO2
    ,@Duplicate_ID
    ,@Internal_Claim_ID
    ,@Is_Signal_Reserve
    ,@Loss_Event_ClientInfo
    ,@Loss_Event_normalized
    ,@Product_Name_ClientInfo
    ,@Claim_Closed_Date
    ,@Policy_ID_Cleaned
    ,@Insured_Name_Clean
    ,@Company_ClientInfo_ID
    ,@rowNr_deleted_count
    ,@Process_ID
    ,@Claim_ClientInfo_ID
    ,@Claim_ID_Cleaned
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned, Insured_Name_Clean, Company_ClientInfo_ID, rowNr_deleted_count, Process_ID, Claim_ClientInfo_ID, Claim_ID_Cleaned ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Process_ID: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, rowNr_deleted_count: int = None, Claim_ClientInfo_ID: int = None, Claim_ID_Cleaned: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Claim_ID_ClientInfo nvarchar(1000) = ?
    ,@Is_Claim_Closed nvarchar(100) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Country_of_Loss_Settlement nvarchar(100) = ?
    ,@Value_as_Of_Date date(0) = ?
    ,@Loss_Currency nvarchar(100) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nvarchar(100) = ?
    ,@Country_of_Loss_Settlement_ISO2 nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Internal_Claim_ID bigint = ?
    ,@Is_Signal_Reserve bit = ?
    ,@Loss_Event_ClientInfo nvarchar(2048) = ?
    ,@Loss_Event_normalized nvarchar(2048) = ?
    ,@Product_Name_ClientInfo nvarchar(2048) = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Policy_ID_Cleaned nvarchar(1000) = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@rowNr_deleted_count int = ?
    ,@Process_ID bigint = ?
    ,@Claim_ClientInfo_ID bigint = ?
    ,@Claim_ID_Cleaned nvarchar(1000) = ?
;

UPDATE [dbo].[claims_bdx] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Claim_ID_ClientInfo] = @Claim_ID_ClientInfo
    ,[Is_Claim_Closed] = @Is_Claim_Closed
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Claims_Description] = @Claims_Description
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Country_of_Loss_Settlement] = @Country_of_Loss_Settlement
    ,[Value_as_Of_Date] = @Value_as_Of_Date
    ,[Loss_Currency] = @Loss_Currency
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[Threshold_Orig_Curr_unind] = @Threshold_Orig_Curr_unind
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[IsCensored] = @IsCensored
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Incurred_Insured_FGU_USD] = @Incurred_Insured_FGU_USD
    ,[Paid_Client_Share_USD] = @Paid_Client_Share_USD
    ,[Incurred_Client_Share_USD] = @Incurred_Client_Share_USD
    ,[Threshold_unind_USD] = @Threshold_unind_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Country_of_Loss_Settlement_ISO2] = @Country_of_Loss_Settlement_ISO2
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Internal_Claim_ID] = @Internal_Claim_ID
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
    ,[Loss_Event_ClientInfo] = @Loss_Event_ClientInfo
    ,[Loss_Event_normalized] = @Loss_Event_normalized
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Claim_Closed_Date] = @Claim_Closed_Date
    ,[Policy_ID_Cleaned] = @Policy_ID_Cleaned
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Company_ClientInfo_ID] = @Company_ClientInfo_ID
    ,[rowNr_deleted_count] = @rowNr_deleted_count
    ,[Process_ID] = @Process_ID
    ,[Claim_ClientInfo_ID] = @Claim_ClientInfo_ID
    ,[Claim_ID_Cleaned] = @Claim_ID_Cleaned
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned, Insured_Name_Clean, Company_ClientInfo_ID, rowNr_deleted_count, Process_ID, Claim_ClientInfo_ID, Claim_ID_Cleaned ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[claims_bdx]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class z_SIR_Bands:
        # table
        TableName = 'z_SIR_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_SIR_Bands]'
        # columns
        Band_Name = 'Band Name'
        Rank = 'Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

INSERT INTO [dbo].[z_SIR_Bands] (
    [Band Name]
    ,[Rank]
)
VALUES (
    @Band_Name
    ,@Rank
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def update(self, Band_Name: str, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Name varchar(15) = ?
    ,@Rank tinyint = ?
;

UPDATE [dbo].[z_SIR_Bands] SET 
    [Band Name] = @Band_Name
 WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Name, Rank ]).exec_df()

        def delete(self, Rank: int) -> DataFrame:
            sql = """
DECLARE
    @Rank tinyint = ?
;

DELETE [dbo].[z_SIR_Bands]
WHERE
    [Rank] = @Rank
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Rank ]).exec_df()

    class issues_excel_input:
        # table
        TableName = 'issues_excel_input'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[issues_excel_input]'
        # columns
        excel_file = 'excel_file'
        row_number = 'row_number'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Column = 'Column'
        Value = 'Value'
        Criticality = 'Criticality'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, excel_file: str, row_number: int = None, ID_Arbeitsvorrat: str = None, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @excel_file nvarchar(200) = ?
    ,@row_number bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(100) = ?
    ,@Value nvarchar(100) = ?
    ,@Criticality bigint = ?
    ,@Comment nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[issues_excel_input] (
    [excel_file]
    ,[row_number]
    ,[ID_Arbeitsvorrat]
    ,[Column]
    ,[Value]
    ,[Criticality]
    ,[Comment]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @excel_file
    ,@row_number
    ,@ID_Arbeitsvorrat
    ,@Column
    ,@Value
    ,@Criticality
    ,@Comment
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ excel_file, row_number, ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, excel_file: str, row_number: int = None, ID_Arbeitsvorrat: str = None, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @excel_file nvarchar(200) = ?
    ,@row_number bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(100) = ?
    ,@Value nvarchar(100) = ?
    ,@Criticality bigint = ?
    ,@Comment nvarchar(1000) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[issues_excel_input] SET 
    [excel_file] = @excel_file
    ,[row_number] = @row_number
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Column] = @Column
    ,[Value] = @Value
    ,[Criticality] = @Criticality
    ,[Comment] = @Comment
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ excel_file, row_number, ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[issues_excel_input]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Turnover:
        # table
        TableName = 'Turnover'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Turnover]'
        # columns
        Turnover_ID = 'Turnover_ID'
        Company_ID = 'Company_ID'
        Turnover_Year = 'Turnover_Year'
        Turnover = 'Turnover'
        Currency_ID = 'Currency_ID'
        Source_of_Change = 'Source_of_Change'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Turnover_Year int = ?
    ,@Turnover float = ?
    ,@Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Turnover] (
    [Company_ID]
    ,[Turnover_Year]
    ,[Turnover]
    ,[Currency_ID]
    ,[Source_of_Change]
    ,[Is_Manually_Curated]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_ID
    ,@Turnover_Year
    ,@Turnover
    ,@Currency_ID
    ,@Source_of_Change
    ,@Is_Manually_Curated
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Turnover_ID: int, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Turnover_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Turnover_Year int = ?
    ,@Turnover float = ?
    ,@Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Turnover] SET 
    [Company_ID] = @Company_ID
    ,[Turnover_Year] = @Turnover_Year
    ,[Turnover] = @Turnover
    ,[Currency_ID] = @Currency_ID
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Turnover_ID] = @Turnover_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Turnover_ID, Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Turnover_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Turnover_ID bigint = ?
;

DELETE [dbo].[Turnover]
WHERE
    [Turnover_ID] = @Turnover_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Turnover_ID ]).exec_df()

    class Dim_Band_Hierarchy_1L:
        # table
        TableName = 'Dim_Band_Hierarchy_1L'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Band_Hierarchy_1L]'
        # columns
        Band_Hierarchy_ID = 'Band_Hierarchy_ID'
        Band_Group = 'Band_Group'
        Band_Type = 'Band_Type'
        Band_ID_From = 'Band_ID_From'
        Band_ID_To = 'Band_ID_To'
        Level_1 = 'Level_1'
        Level_1_Sortindex = 'Level_1_Sortindex'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Band_Group nvarchar(40) = ?
    ,@Band_Type nvarchar(40) = ?
    ,@Band_ID_From bigint = ?
    ,@Band_ID_To bigint = ?
    ,@Level_1 nvarchar(100) = ?
    ,@Level_1_Sortindex smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Band_Hierarchy_1L] (
    [Band_Group]
    ,[Band_Type]
    ,[Band_ID_From]
    ,[Band_ID_To]
    ,[Level_1]
    ,[Level_1_Sortindex]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Band_Group
    ,@Band_Type
    ,@Band_ID_From
    ,@Band_ID_To
    ,@Level_1
    ,@Level_1_Sortindex
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Band_Hierarchy_ID: int, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Band_Hierarchy_ID bigint = ?
    ,@Band_Group nvarchar(40) = ?
    ,@Band_Type nvarchar(40) = ?
    ,@Band_ID_From bigint = ?
    ,@Band_ID_To bigint = ?
    ,@Level_1 nvarchar(100) = ?
    ,@Level_1_Sortindex smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Band_Hierarchy_1L] SET 
    [Band_Group] = @Band_Group
    ,[Band_Type] = @Band_Type
    ,[Band_ID_From] = @Band_ID_From
    ,[Band_ID_To] = @Band_ID_To
    ,[Level_1] = @Level_1
    ,[Level_1_Sortindex] = @Level_1_Sortindex
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Band_Hierarchy_ID] = @Band_Hierarchy_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Hierarchy_ID, Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Band_Hierarchy_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Hierarchy_ID bigint = ?
;

DELETE [dbo].[Dim_Band_Hierarchy_1L]
WHERE
    [Band_Hierarchy_ID] = @Band_Hierarchy_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Hierarchy_ID ]).exec_df()

    class tDimCambridge:
        # table
        TableName = 'tDimCambridge'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimCambridge]'
        # columns
        CambridgeKey = 'CambridgeKey'
        Cambridge_Code_Number = 'Cambridge_Code_Number'
        Cambridge_Code_Number_Agg = 'Cambridge_Code_Number_Agg'
        Cambridge_Full_Agg_Name = 'Cambridge_Full_Agg_Name'
        Cambridge_Full_Name = 'Cambridge_Full_Name'
        Cambridge_Name = 'Cambridge_Name'
        Cambridge_Name_Agg = 'Cambridge_Name_Agg'
        Cambridge_Rank = 'Cambridge_Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Cambridge_Code_Number: str, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None) -> DataFrame:
            sql = """
DECLARE
    @Cambridge_Code_Number nvarchar(200) = ?
    ,@Cambridge_Code_Number_Agg int = ?
    ,@Cambridge_Full_Agg_Name nvarchar(200) = ?
    ,@Cambridge_Full_Name nvarchar(200) = ?
    ,@Cambridge_Name nvarchar(200) = ?
    ,@Cambridge_Name_Agg nvarchar(200) = ?
    ,@Cambridge_Rank int = ?
;

INSERT INTO [dbo].[tDimCambridge] (
    [Cambridge_Code_Number]
    ,[Cambridge_Code_Number_Agg]
    ,[Cambridge_Full_Agg_Name]
    ,[Cambridge_Full_Name]
    ,[Cambridge_Name]
    ,[Cambridge_Name_Agg]
    ,[Cambridge_Rank]
)
VALUES (
    @Cambridge_Code_Number
    ,@Cambridge_Code_Number_Agg
    ,@Cambridge_Full_Agg_Name
    ,@Cambridge_Full_Name
    ,@Cambridge_Name
    ,@Cambridge_Name_Agg
    ,@Cambridge_Rank
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ]).exec_df()

        def update(self, CambridgeKey: int, Cambridge_Code_Number: str, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None) -> DataFrame:
            sql = """
DECLARE
    @CambridgeKey int = ?
    ,@Cambridge_Code_Number nvarchar(200) = ?
    ,@Cambridge_Code_Number_Agg int = ?
    ,@Cambridge_Full_Agg_Name nvarchar(200) = ?
    ,@Cambridge_Full_Name nvarchar(200) = ?
    ,@Cambridge_Name nvarchar(200) = ?
    ,@Cambridge_Name_Agg nvarchar(200) = ?
    ,@Cambridge_Rank int = ?
;

UPDATE [dbo].[tDimCambridge] SET 
    [CambridgeKey] = @CambridgeKey
    ,[Cambridge_Code_Number] = @Cambridge_Code_Number
    ,[Cambridge_Code_Number_Agg] = @Cambridge_Code_Number_Agg
    ,[Cambridge_Full_Agg_Name] = @Cambridge_Full_Agg_Name
    ,[Cambridge_Full_Name] = @Cambridge_Full_Name
    ,[Cambridge_Name] = @Cambridge_Name
    ,[Cambridge_Name_Agg] = @Cambridge_Name_Agg
    ,[Cambridge_Rank] = @Cambridge_Rank
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CambridgeKey, Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tDimCambridge]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_company_linking:
        # table
        TableName = 'exposure_company_linking'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_company_linking]'
        # columns
        id = 'id'
        company_id = 'company_id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@company_id bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[exposure_company_linking] (
    [id]
    ,[company_id]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @id
    ,@company_id
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, company_id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@company_id bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[exposure_company_linking] SET 
    [company_id] = @company_id
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, company_id, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[exposure_company_linking]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class Processes_Yuru:
        # table
        TableName = 'Processes_Yuru'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Processes_Yuru]'
        # columns
        Id = 'Id'
        OperationFlag = 'OperationFlag'
        InsertionDate = 'InsertionDate'
        LastModifiedDate = 'LastModifiedDate'
        Deadline = 'Deadline'
        BusinessContact = 'BusinessContact'
        BusinessUnit = 'BusinessUnit'
        Priority = 'Priority'
        SmartMatchingContact = 'SmartMatchingContact'
        ProjectTeamContact = 'ProjectTeamContact'
        FourEyeCheckContact = 'FourEyeCheckContact'
        Comment = 'Comment'
        SignOffToken = 'SignOffToken'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None) -> DataFrame:
            sql = """
DECLARE
    @OperationFlag int = ?
    ,@InsertionDate datetime2(7) = ?
    ,@LastModifiedDate datetime2(7) = ?
    ,@Deadline datetime2(7) = ?
    ,@BusinessContact nvarchar(max) = ?
    ,@BusinessUnit int = ?
    ,@Priority int = ?
    ,@SmartMatchingContact nvarchar(max) = ?
    ,@ProjectTeamContact nvarchar(max) = ?
    ,@FourEyeCheckContact nvarchar(max) = ?
    ,@Comment nvarchar(max) = ?
    ,@SignOffToken nvarchar(max) = ?
;

INSERT INTO [dbo].[Processes_Yuru] (
    [OperationFlag]
    ,[InsertionDate]
    ,[LastModifiedDate]
    ,[Deadline]
    ,[BusinessContact]
    ,[BusinessUnit]
    ,[Priority]
    ,[SmartMatchingContact]
    ,[ProjectTeamContact]
    ,[FourEyeCheckContact]
    ,[Comment]
    ,[SignOffToken]
)
VALUES (
    @OperationFlag
    ,@InsertionDate
    ,@LastModifiedDate
    ,@Deadline
    ,@BusinessContact
    ,@BusinessUnit
    ,@Priority
    ,@SmartMatchingContact
    ,@ProjectTeamContact
    ,@FourEyeCheckContact
    ,@Comment
    ,@SignOffToken
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken ]).exec_df()

        def update(self, Id: int, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None) -> DataFrame:
            sql = """
DECLARE
    @Id bigint = ?
    ,@OperationFlag int = ?
    ,@InsertionDate datetime2(7) = ?
    ,@LastModifiedDate datetime2(7) = ?
    ,@Deadline datetime2(7) = ?
    ,@BusinessContact nvarchar(max) = ?
    ,@BusinessUnit int = ?
    ,@Priority int = ?
    ,@SmartMatchingContact nvarchar(max) = ?
    ,@ProjectTeamContact nvarchar(max) = ?
    ,@FourEyeCheckContact nvarchar(max) = ?
    ,@Comment nvarchar(max) = ?
    ,@SignOffToken nvarchar(max) = ?
;

UPDATE [dbo].[Processes_Yuru] SET 
    [Id] = @Id
    ,[OperationFlag] = @OperationFlag
    ,[InsertionDate] = @InsertionDate
    ,[LastModifiedDate] = @LastModifiedDate
    ,[Deadline] = @Deadline
    ,[BusinessContact] = @BusinessContact
    ,[BusinessUnit] = @BusinessUnit
    ,[Priority] = @Priority
    ,[SmartMatchingContact] = @SmartMatchingContact
    ,[ProjectTeamContact] = @ProjectTeamContact
    ,[FourEyeCheckContact] = @FourEyeCheckContact
    ,[Comment] = @Comment
    ,[SignOffToken] = @SignOffToken
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Id, OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Processes_Yuru]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Band_Hierarchy_2L:
        # table
        TableName = 'Dim_Band_Hierarchy_2L'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Band_Hierarchy_2L]'
        # columns
        Band_Hierarchy_ID = 'Band_Hierarchy_ID'
        Band_Group = 'Band_Group'
        Band_Type = 'Band_Type'
        Band_ID_From = 'Band_ID_From'
        Band_ID_To = 'Band_ID_To'
        Level_1 = 'Level_1'
        Level_1_Sortindex = 'Level_1_Sortindex'
        Level_2 = 'Level_2'
        Level_2_Sortindex = 'Level_2_Sortindex'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Level_2: str, Level_2_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Band_Group nvarchar(40) = ?
    ,@Band_Type nvarchar(40) = ?
    ,@Band_ID_From bigint = ?
    ,@Band_ID_To bigint = ?
    ,@Level_1 nvarchar(100) = ?
    ,@Level_1_Sortindex smallint = ?
    ,@Level_2 nvarchar(100) = ?
    ,@Level_2_Sortindex smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Band_Hierarchy_2L] (
    [Band_Group]
    ,[Band_Type]
    ,[Band_ID_From]
    ,[Band_ID_To]
    ,[Level_1]
    ,[Level_1_Sortindex]
    ,[Level_2]
    ,[Level_2_Sortindex]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Band_Group
    ,@Band_Type
    ,@Band_ID_From
    ,@Band_ID_To
    ,@Level_1
    ,@Level_1_Sortindex
    ,@Level_2
    ,@Level_2_Sortindex
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Level_2, Level_2_Sortindex, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Band_Hierarchy_ID: int, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Level_2: str, Level_2_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Band_Hierarchy_ID bigint = ?
    ,@Band_Group nvarchar(40) = ?
    ,@Band_Type nvarchar(40) = ?
    ,@Band_ID_From bigint = ?
    ,@Band_ID_To bigint = ?
    ,@Level_1 nvarchar(100) = ?
    ,@Level_1_Sortindex smallint = ?
    ,@Level_2 nvarchar(100) = ?
    ,@Level_2_Sortindex smallint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Band_Hierarchy_2L] SET 
    [Band_Group] = @Band_Group
    ,[Band_Type] = @Band_Type
    ,[Band_ID_From] = @Band_ID_From
    ,[Band_ID_To] = @Band_ID_To
    ,[Level_1] = @Level_1
    ,[Level_1_Sortindex] = @Level_1_Sortindex
    ,[Level_2] = @Level_2
    ,[Level_2_Sortindex] = @Level_2_Sortindex
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Band_Hierarchy_ID] = @Band_Hierarchy_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Hierarchy_ID, Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Level_2, Level_2_Sortindex, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Band_Hierarchy_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Band_Hierarchy_ID bigint = ?
;

DELETE [dbo].[Dim_Band_Hierarchy_2L]
WHERE
    [Band_Hierarchy_ID] = @Band_Hierarchy_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Hierarchy_ID ]).exec_df()

    class tDimCountries:
        # table
        TableName = 'tDimCountries'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimCountries]'
        # columns
        CountryKey = 'CountryKey'
        Country = 'Country'
        Currency = 'Currency'
        ISO2 = 'ISO2'
        ISO3 = 'ISO3'
        Latitude = 'Latitude'
        Longitude = 'Longitude'
        Continent_Name = 'Continent_Name'
        Region_Name = 'Region_Name'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Country: str, ISO2: str, ISO3: str, Currency: str = None, Latitude: float = None, Longitude: float = None, Continent_Name: str = None, Region_Name: str = None) -> DataFrame:
            sql = """
DECLARE
    @Country nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@ISO2 nvarchar(10) = ?
    ,@ISO3 nvarchar(10) = ?
    ,@Latitude float = ?
    ,@Longitude float = ?
    ,@Continent_Name nvarchar(100) = ?
    ,@Region_Name nvarchar(100) = ?
;

INSERT INTO [dbo].[tDimCountries] (
    [Country]
    ,[Currency]
    ,[ISO2]
    ,[ISO3]
    ,[Latitude]
    ,[Longitude]
    ,[Continent_Name]
    ,[Region_Name]
)
VALUES (
    @Country
    ,@Currency
    ,@ISO2
    ,@ISO3
    ,@Latitude
    ,@Longitude
    ,@Continent_Name
    ,@Region_Name
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Country, Currency, ISO2, ISO3, Latitude, Longitude, Continent_Name, Region_Name ]).exec_df()

        def update(self, CountryKey: int, Country: str, ISO2: str, ISO3: str, Currency: str = None, Latitude: float = None, Longitude: float = None, Continent_Name: str = None, Region_Name: str = None) -> DataFrame:
            sql = """
DECLARE
    @CountryKey int = ?
    ,@Country nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@ISO2 nvarchar(10) = ?
    ,@ISO3 nvarchar(10) = ?
    ,@Latitude float = ?
    ,@Longitude float = ?
    ,@Continent_Name nvarchar(100) = ?
    ,@Region_Name nvarchar(100) = ?
;

UPDATE [dbo].[tDimCountries] SET 
    [Country] = @Country
    ,[Currency] = @Currency
    ,[ISO2] = @ISO2
    ,[ISO3] = @ISO3
    ,[Latitude] = @Latitude
    ,[Longitude] = @Longitude
    ,[Continent_Name] = @Continent_Name
    ,[Region_Name] = @Region_Name
 WHERE
    [CountryKey] = @CountryKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CountryKey, Country, Currency, ISO2, ISO3, Latitude, Longitude, Continent_Name, Region_Name ]).exec_df()

        def delete(self, CountryKey: int) -> DataFrame:
            sql = """
DECLARE
    @CountryKey int = ?
;

DELETE [dbo].[tDimCountries]
WHERE
    [CountryKey] = @CountryKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CountryKey ]).exec_df()

    class Claim_History:
        # table
        TableName = 'Claim_History'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Claim_History]'
        # columns
        id = 'id'
        Claim_ID = 'Claim_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Company_ID = 'Company_ID'
        Claim_Ref_Clean = 'Claim_Ref_Clean'
        Claim_Ref = 'Claim_Ref'
        Date_of_Loss = 'Date_of_Loss'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Exposure_ID = 'Exposure_ID'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Type_of_Loss = 'Type_of_Loss'
        Loss_Event_ID = 'Loss_Event_ID'
        Claims_Description = 'Claims_Description'
        Policy_Ref = 'Policy_Ref'
        Policy_Ref_Clean = 'Policy_Ref_Clean'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Currency_ID = 'Policy_Currency_ID'
        Source_of_Change = 'Source_of_Change'
        Is_Combined = 'Is_Combined'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Parent_ID = 'Parent_ID'
        Ultimate_Parent_ID = 'Ultimate_Parent_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Claim_ID: int, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Claim_Ref_Clean nvarchar(1000) = ?
    ,@Claim_Ref nvarchar(1000) = ?
    ,@Date_of_Loss date(0) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Exposure_ID bigint = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Parent_ID bigint = ?
    ,@Ultimate_Parent_ID bigint = ?
;

INSERT INTO [dbo].[Claim_History] (
    [Claim_ID]
    ,[Technical_Client_ID]
    ,[Company_ID]
    ,[Claim_Ref_Clean]
    ,[Claim_Ref]
    ,[Date_of_Loss]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Exposure_ID]
    ,[Country_ISO2_ID]
    ,[Type_of_Loss]
    ,[Loss_Event_ID]
    ,[Claims_Description]
    ,[Policy_Ref]
    ,[Policy_Ref_Clean]
    ,[Policy_Inception_Date]
    ,[Policy_Currency_ID]
    ,[Source_of_Change]
    ,[Is_Combined]
    ,[Is_Manually_Curated]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Parent_ID]
    ,[Ultimate_Parent_ID]
)
VALUES (
    @Claim_ID
    ,@Technical_Client_ID
    ,@Company_ID
    ,@Claim_Ref_Clean
    ,@Claim_Ref
    ,@Date_of_Loss
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Exposure_ID
    ,@Country_ISO2_ID
    ,@Type_of_Loss
    ,@Loss_Event_ID
    ,@Claims_Description
    ,@Policy_Ref
    ,@Policy_Ref_Clean
    ,@Policy_Inception_Date
    ,@Policy_Currency_ID
    ,@Source_of_Change
    ,@Is_Combined
    ,@Is_Manually_Curated
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Parent_ID
    ,@Ultimate_Parent_ID
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ID, Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ]).exec_df()

        def update(self, id: int, Claim_ID: int, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Claim_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Claim_Ref_Clean nvarchar(1000) = ?
    ,@Claim_Ref nvarchar(1000) = ?
    ,@Date_of_Loss date(0) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Exposure_ID bigint = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Combined bit = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Parent_ID bigint = ?
    ,@Ultimate_Parent_ID bigint = ?
;

UPDATE [dbo].[Claim_History] SET 
    [Claim_ID] = @Claim_ID
    ,[Technical_Client_ID] = @Technical_Client_ID
    ,[Company_ID] = @Company_ID
    ,[Claim_Ref_Clean] = @Claim_Ref_Clean
    ,[Claim_Ref] = @Claim_Ref
    ,[Date_of_Loss] = @Date_of_Loss
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Exposure_ID] = @Exposure_ID
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Loss_Event_ID] = @Loss_Event_ID
    ,[Claims_Description] = @Claims_Description
    ,[Policy_Ref] = @Policy_Ref
    ,[Policy_Ref_Clean] = @Policy_Ref_Clean
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Currency_ID] = @Policy_Currency_ID
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Combined] = @Is_Combined
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Parent_ID] = @Parent_ID
    ,[Ultimate_Parent_ID] = @Ultimate_Parent_ID
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Claim_ID, Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[Claim_History]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class tFactExposure_test:
        # table
        TableName = 'tFactExposure_test'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tFactExposure_test]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        PolicyInceptionDateKey = 'PolicyInceptionDateKey'
        Policy_Inception_Date = 'Policy_Inception_Date'
        PolicyExpiryDateKey = 'PolicyExpiryDateKey'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Turnover_Year_ClientInfo = 'Turnover_Year_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        PII_Records_Stored = 'PII_Records_Stored'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        CountryKey = 'CountryKey'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd = 'Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd'
        Coverage_4_Sublimit_RestorationData = 'Coverage_4_Sublimit_RestorationData'
        Coverage_5_Sublimit_Reputation = 'Coverage_5_Sublimit_Reputation'
        Coverage_6_Sublimit_Business_Interruption = 'Coverage_6_Sublimit_Business_Interruption'
        Coverage_7_Sublimit_Contingent_Business_Interruption = 'Coverage_7_Sublimit_Contingent_Business_Interruption'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        DatasourceID = 'DatasourceID'
        rowNr = 'rowNr'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        ClientLimitBandKey = 'ClientLimitBandKey'
        Client_Limit_USD = 'Client_Limit_USD'
        FullLimitBandKey = 'FullLimitBandKey'
        Full_Limit_USD = 'Full_Limit_USD'
        AttachmentBandKey = 'AttachmentBandKey'
        Attachment_USD = 'Attachment_USD'
        SIRBandsKey = 'SIRBandsKey'
        SIR_USD = 'SIR_USD'
        PremiumBandsKey = 'PremiumBandsKey'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        CambridgeKey = 'CambridgeKey'
        Trade_Level_CodeNumber_Mapped_Cambridge = 'Trade_Level_CodeNumber_Mapped_Cambridge'
        MR_Limit_USD = 'MR_Limit_USD'
        MR_Premium_USD = 'MR_Premium_USD'
        MR_GrossNet_Premium_USD = 'MR_GrossNet_Premium_USD'
        mr_share = 'mr_share'
        CompanySegmentKey = 'CompanySegmentKey'
        Turnover_USD_Combined = 'Turnover_USD_Combined'
        FileMetaInfoKey = 'FileMetaInfoKey'
        Insured_Name_BvD = 'Insured_Name_BvD'
        name_alias_bvd = 'name_alias_bvd'
        name_alias_source_bvd = 'name_alias_source_bvd'
        addr_internat_bvd = 'addr_internat_bvd'
        ambest_id_bvd = 'ambest_id_bvd'
        branch_count_bvd = 'branch_count_bvd'
        Trade_Level_CodeNumber_Mapped_Cambridge_BvD = 'Trade_Level_CodeNumber_Mapped_Cambridge_BvD'
        Trade_Level_Name_Mapped_Cambridge_BvD = 'Trade_Level_Name_Mapped_Cambridge_BvD'
        category_of_company_bvd = 'category_of_company_bvd'
        city_internat_bvd = 'city_internat_bvd'
        corporate_group_size_bvd = 'corporate_group_size_bvd'
        country_bvd = 'country_bvd'
        county_bvd = 'county_bvd'
        Country_ISO2_BvD = 'Country_ISO2_BvD'
        direct_parent_bvdid_bvd = 'direct_parent_bvdid_bvd'
        direct_parent_name_internat_bvd = 'direct_parent_name_internat_bvd'
        ein_bvd = 'ein_bvd'
        email_bvd = 'email_bvd'
        employees_bvd = 'employees_bvd'
        eurovat_bvd = 'eurovat_bvd'
        hierarchy_level_bvd = 'hierarchy_level_bvd'
        inactive_bvd = 'inactive_bvd'
        incorporation_date_bvd = 'incorporation_date_bvd'
        legalfrm_bvd = 'legalfrm_bvd'
        lei_lei_bvd = 'lei_lei_bvd'
        listed_bvd = 'listed_bvd'
        mainexch_bvd = 'mainexch_bvd'
        naicsccod2017_bvd = 'naicsccod2017_bvd'
        phone_bvd = 'phone_bvd'
        postcode_bvd = 'postcode_bvd'
        previous_names_set_array_bvd = 'previous_names_set_array_bvd'
        sd_isin_bvd = 'sd_isin_bvd'
        sd_ticker_bvd = 'sd_ticker_bvd'
        slegalf_bvd = 'slegalf_bvd'
        state_us_bvd = 'state_us_bvd'
        subs_count_bvd = 'subs_count_bvd'
        traderegisternr_bvd = 'traderegisternr_bvd'
        Turnover_EUR_BvD = 'Turnover_EUR_BvD'
        Turnover_USD_BvD = 'Turnover_USD_BvD'
        type_of_entity_bvd = 'type_of_entity_bvd'
        ultimate_parent_bvdid_bvd = 'ultimate_parent_bvdid_bvd'
        ultimate_parent_ctryiso_bvd = 'ultimate_parent_ctryiso_bvd'
        ultimate_parent_name_bvd = 'ultimate_parent_name_bvd'
        ussicccod_bvd = 'ussicccod_bvd'
        vatnumber_bvd = 'vatnumber_bvd'
        website_bvd = 'website_bvd'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        DELETE_indicator = 'DELETE_indicator'
        Bvd_Country_Check = 'Bvd_Country_Check'
        Bvd_Industry_Check = 'Bvd_Industry_Check'
        Turnover_Deviation_BvD = 'Turnover_Deviation_BvD'
        Turnover_Dev_as_perc_of_Client_Turnover = 'Turnover_Dev_as_perc_of_Client_Turnover'
        Abs_Dev_perc = 'Abs_Dev_perc'
        Bvd_Turnover_Check = 'Bvd_Turnover_Check'
        Policy_Duration_Day = 'Policy_Duration_Day'
        Policy_Duration_Month = 'Policy_Duration_Month'
        Is_MR_Share_calculated = 'Is_MR_Share_calculated'
        Is_Deleted = 'Is_Deleted'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@DatasourceID nvarchar(466) = ?
    ,@rowNr bigint = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@PremiumBandsKey int = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@CambridgeKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@mr_share float = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_Combined float = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Bvd_Country_Check nvarchar(510) = ?
    ,@Bvd_Industry_Check nvarchar(510) = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Bvd_Turnover_Check nvarchar(510) = ?
    ,@Policy_Duration_Day int = ?
    ,@Policy_Duration_Month int = ?
    ,@Is_MR_Share_calculated nvarchar(60) = ?
    ,@Is_Deleted nvarchar(100) = ?
;

INSERT INTO [dbo].[tFactExposure_test] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[PolicyInceptionDateKey]
    ,[Policy_Inception_Date]
    ,[PolicyExpiryDateKey]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Turnover_Year_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[PII_Records_Stored]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[CountryKey]
    ,[Insured_Country_ISO2]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd]
    ,[Coverage_4_Sublimit_RestorationData]
    ,[Coverage_5_Sublimit_Reputation]
    ,[Coverage_6_Sublimit_Business_Interruption]
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[DatasourceID]
    ,[rowNr]
    ,[Turnover_ClientInfo_USD]
    ,[ClientLimitBandKey]
    ,[Client_Limit_USD]
    ,[FullLimitBandKey]
    ,[Full_Limit_USD]
    ,[AttachmentBandKey]
    ,[Attachment_USD]
    ,[SIRBandsKey]
    ,[SIR_USD]
    ,[PremiumBandsKey]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[CambridgeKey]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge]
    ,[MR_Limit_USD]
    ,[MR_Premium_USD]
    ,[MR_GrossNet_Premium_USD]
    ,[mr_share]
    ,[CompanySegmentKey]
    ,[Turnover_USD_Combined]
    ,[FileMetaInfoKey]
    ,[Insured_Name_BvD]
    ,[name_alias_bvd]
    ,[name_alias_source_bvd]
    ,[addr_internat_bvd]
    ,[ambest_id_bvd]
    ,[branch_count_bvd]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD]
    ,[Trade_Level_Name_Mapped_Cambridge_BvD]
    ,[category_of_company_bvd]
    ,[city_internat_bvd]
    ,[corporate_group_size_bvd]
    ,[country_bvd]
    ,[county_bvd]
    ,[Country_ISO2_BvD]
    ,[direct_parent_bvdid_bvd]
    ,[direct_parent_name_internat_bvd]
    ,[ein_bvd]
    ,[email_bvd]
    ,[employees_bvd]
    ,[eurovat_bvd]
    ,[hierarchy_level_bvd]
    ,[inactive_bvd]
    ,[incorporation_date_bvd]
    ,[legalfrm_bvd]
    ,[lei_lei_bvd]
    ,[listed_bvd]
    ,[mainexch_bvd]
    ,[naicsccod2017_bvd]
    ,[phone_bvd]
    ,[postcode_bvd]
    ,[previous_names_set_array_bvd]
    ,[sd_isin_bvd]
    ,[sd_ticker_bvd]
    ,[slegalf_bvd]
    ,[state_us_bvd]
    ,[subs_count_bvd]
    ,[traderegisternr_bvd]
    ,[Turnover_EUR_BvD]
    ,[Turnover_USD_BvD]
    ,[type_of_entity_bvd]
    ,[ultimate_parent_bvdid_bvd]
    ,[ultimate_parent_ctryiso_bvd]
    ,[ultimate_parent_name_bvd]
    ,[ussicccod_bvd]
    ,[vatnumber_bvd]
    ,[website_bvd]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[DELETE_indicator]
    ,[Bvd_Country_Check]
    ,[Bvd_Industry_Check]
    ,[Turnover_Deviation_BvD]
    ,[Turnover_Dev_as_perc_of_Client_Turnover]
    ,[Abs_Dev_perc]
    ,[Bvd_Turnover_Check]
    ,[Policy_Duration_Day]
    ,[Policy_Duration_Month]
    ,[Is_MR_Share_calculated]
    ,[Is_Deleted]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@PolicyInceptionDateKey
    ,@Policy_Inception_Date
    ,@PolicyExpiryDateKey
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Turnover_Year_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Insured_No_of_Employees
    ,@PII_Records_Stored
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@CountryKey
    ,@Insured_Country_ISO2
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,@Coverage_4_Sublimit_RestorationData
    ,@Coverage_5_Sublimit_Reputation
    ,@Coverage_6_Sublimit_Business_Interruption
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@DatasourceID
    ,@rowNr
    ,@Turnover_ClientInfo_USD
    ,@ClientLimitBandKey
    ,@Client_Limit_USD
    ,@FullLimitBandKey
    ,@Full_Limit_USD
    ,@AttachmentBandKey
    ,@Attachment_USD
    ,@SIRBandsKey
    ,@SIR_USD
    ,@PremiumBandsKey
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@BvD_ID
    ,@Duplicate_ID
    ,@CambridgeKey
    ,@Trade_Level_CodeNumber_Mapped_Cambridge
    ,@MR_Limit_USD
    ,@MR_Premium_USD
    ,@MR_GrossNet_Premium_USD
    ,@mr_share
    ,@CompanySegmentKey
    ,@Turnover_USD_Combined
    ,@FileMetaInfoKey
    ,@Insured_Name_BvD
    ,@name_alias_bvd
    ,@name_alias_source_bvd
    ,@addr_internat_bvd
    ,@ambest_id_bvd
    ,@branch_count_bvd
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,@Trade_Level_Name_Mapped_Cambridge_BvD
    ,@category_of_company_bvd
    ,@city_internat_bvd
    ,@corporate_group_size_bvd
    ,@country_bvd
    ,@county_bvd
    ,@Country_ISO2_BvD
    ,@direct_parent_bvdid_bvd
    ,@direct_parent_name_internat_bvd
    ,@ein_bvd
    ,@email_bvd
    ,@employees_bvd
    ,@eurovat_bvd
    ,@hierarchy_level_bvd
    ,@inactive_bvd
    ,@incorporation_date_bvd
    ,@legalfrm_bvd
    ,@lei_lei_bvd
    ,@listed_bvd
    ,@mainexch_bvd
    ,@naicsccod2017_bvd
    ,@phone_bvd
    ,@postcode_bvd
    ,@previous_names_set_array_bvd
    ,@sd_isin_bvd
    ,@sd_ticker_bvd
    ,@slegalf_bvd
    ,@state_us_bvd
    ,@subs_count_bvd
    ,@traderegisternr_bvd
    ,@Turnover_EUR_BvD
    ,@Turnover_USD_BvD
    ,@type_of_entity_bvd
    ,@ultimate_parent_bvdid_bvd
    ,@ultimate_parent_ctryiso_bvd
    ,@ultimate_parent_name_bvd
    ,@ussicccod_bvd
    ,@vatnumber_bvd
    ,@website_bvd
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@DELETE_indicator
    ,@Bvd_Country_Check
    ,@Bvd_Industry_Check
    ,@Turnover_Deviation_BvD
    ,@Turnover_Dev_as_perc_of_Client_Turnover
    ,@Abs_Dev_perc
    ,@Bvd_Turnover_Check
    ,@Policy_Duration_Day
    ,@Policy_Duration_Month
    ,@Is_MR_Share_calculated
    ,@Is_Deleted
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@DatasourceID nvarchar(466) = ?
    ,@rowNr bigint = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@PremiumBandsKey int = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@CambridgeKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@mr_share float = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_Combined float = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Bvd_Country_Check nvarchar(510) = ?
    ,@Bvd_Industry_Check nvarchar(510) = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Bvd_Turnover_Check nvarchar(510) = ?
    ,@Policy_Duration_Day int = ?
    ,@Policy_Duration_Month int = ?
    ,@Is_MR_Share_calculated nvarchar(60) = ?
    ,@Is_Deleted nvarchar(100) = ?
;

UPDATE [dbo].[tFactExposure_test] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[PolicyInceptionDateKey] = @PolicyInceptionDateKey
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[PolicyExpiryDateKey] = @PolicyExpiryDateKey
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Turnover_Year_ClientInfo] = @Turnover_Year_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[PII_Records_Stored] = @PII_Records_Stored
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[CountryKey] = @CountryKey
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd] = @Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,[Coverage_4_Sublimit_RestorationData] = @Coverage_4_Sublimit_RestorationData
    ,[Coverage_5_Sublimit_Reputation] = @Coverage_5_Sublimit_Reputation
    ,[Coverage_6_Sublimit_Business_Interruption] = @Coverage_6_Sublimit_Business_Interruption
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption] = @Coverage_7_Sublimit_Contingent_Business_Interruption
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[DatasourceID] = @DatasourceID
    ,[rowNr] = @rowNr
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[ClientLimitBandKey] = @ClientLimitBandKey
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[FullLimitBandKey] = @FullLimitBandKey
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[AttachmentBandKey] = @AttachmentBandKey
    ,[Attachment_USD] = @Attachment_USD
    ,[SIRBandsKey] = @SIRBandsKey
    ,[SIR_USD] = @SIR_USD
    ,[PremiumBandsKey] = @PremiumBandsKey
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[CambridgeKey] = @CambridgeKey
    ,[Trade_Level_CodeNumber_Mapped_Cambridge] = @Trade_Level_CodeNumber_Mapped_Cambridge
    ,[MR_Limit_USD] = @MR_Limit_USD
    ,[MR_Premium_USD] = @MR_Premium_USD
    ,[MR_GrossNet_Premium_USD] = @MR_GrossNet_Premium_USD
    ,[mr_share] = @mr_share
    ,[CompanySegmentKey] = @CompanySegmentKey
    ,[Turnover_USD_Combined] = @Turnover_USD_Combined
    ,[FileMetaInfoKey] = @FileMetaInfoKey
    ,[Insured_Name_BvD] = @Insured_Name_BvD
    ,[name_alias_bvd] = @name_alias_bvd
    ,[name_alias_source_bvd] = @name_alias_source_bvd
    ,[addr_internat_bvd] = @addr_internat_bvd
    ,[ambest_id_bvd] = @ambest_id_bvd
    ,[branch_count_bvd] = @branch_count_bvd
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD] = @Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,[Trade_Level_Name_Mapped_Cambridge_BvD] = @Trade_Level_Name_Mapped_Cambridge_BvD
    ,[category_of_company_bvd] = @category_of_company_bvd
    ,[city_internat_bvd] = @city_internat_bvd
    ,[corporate_group_size_bvd] = @corporate_group_size_bvd
    ,[country_bvd] = @country_bvd
    ,[county_bvd] = @county_bvd
    ,[Country_ISO2_BvD] = @Country_ISO2_BvD
    ,[direct_parent_bvdid_bvd] = @direct_parent_bvdid_bvd
    ,[direct_parent_name_internat_bvd] = @direct_parent_name_internat_bvd
    ,[ein_bvd] = @ein_bvd
    ,[email_bvd] = @email_bvd
    ,[employees_bvd] = @employees_bvd
    ,[eurovat_bvd] = @eurovat_bvd
    ,[hierarchy_level_bvd] = @hierarchy_level_bvd
    ,[inactive_bvd] = @inactive_bvd
    ,[incorporation_date_bvd] = @incorporation_date_bvd
    ,[legalfrm_bvd] = @legalfrm_bvd
    ,[lei_lei_bvd] = @lei_lei_bvd
    ,[listed_bvd] = @listed_bvd
    ,[mainexch_bvd] = @mainexch_bvd
    ,[naicsccod2017_bvd] = @naicsccod2017_bvd
    ,[phone_bvd] = @phone_bvd
    ,[postcode_bvd] = @postcode_bvd
    ,[previous_names_set_array_bvd] = @previous_names_set_array_bvd
    ,[sd_isin_bvd] = @sd_isin_bvd
    ,[sd_ticker_bvd] = @sd_ticker_bvd
    ,[slegalf_bvd] = @slegalf_bvd
    ,[state_us_bvd] = @state_us_bvd
    ,[subs_count_bvd] = @subs_count_bvd
    ,[traderegisternr_bvd] = @traderegisternr_bvd
    ,[Turnover_EUR_BvD] = @Turnover_EUR_BvD
    ,[Turnover_USD_BvD] = @Turnover_USD_BvD
    ,[type_of_entity_bvd] = @type_of_entity_bvd
    ,[ultimate_parent_bvdid_bvd] = @ultimate_parent_bvdid_bvd
    ,[ultimate_parent_ctryiso_bvd] = @ultimate_parent_ctryiso_bvd
    ,[ultimate_parent_name_bvd] = @ultimate_parent_name_bvd
    ,[ussicccod_bvd] = @ussicccod_bvd
    ,[vatnumber_bvd] = @vatnumber_bvd
    ,[website_bvd] = @website_bvd
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Bvd_Country_Check] = @Bvd_Country_Check
    ,[Bvd_Industry_Check] = @Bvd_Industry_Check
    ,[Turnover_Deviation_BvD] = @Turnover_Deviation_BvD
    ,[Turnover_Dev_as_perc_of_Client_Turnover] = @Turnover_Dev_as_perc_of_Client_Turnover
    ,[Abs_Dev_perc] = @Abs_Dev_perc
    ,[Bvd_Turnover_Check] = @Bvd_Turnover_Check
    ,[Policy_Duration_Day] = @Policy_Duration_Day
    ,[Policy_Duration_Month] = @Policy_Duration_Month
    ,[Is_MR_Share_calculated] = @Is_MR_Share_calculated
    ,[Is_Deleted] = @Is_Deleted
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tFactExposure_test]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_bdx_jb:
        # table
        TableName = 'exposure_bdx_jb'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_bdx_jb]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Turnover_Year_ClientInfo = 'Turnover_Year_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        PII_Records_Stored = 'PII_Records_Stored'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd = 'Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd'
        Coverage_4_Sublimit_RestorationData = 'Coverage_4_Sublimit_RestorationData'
        Coverage_5_Sublimit_Reputation = 'Coverage_5_Sublimit_Reputation'
        Coverage_6_Sublimit_Business_Interruption = 'Coverage_6_Sublimit_Business_Interruption'
        Coverage_7_Sublimit_Contingent_Business_Interruption = 'Coverage_7_Sublimit_Contingent_Business_Interruption'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, ID_Arbeitsvorrat_MR_share: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo varchar(255) = ?
    ,@Coverage varchar(255) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo int = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_No_of_Employees int = ?
    ,@PII_Records_Stored int = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Insured_Homepage varchar(255) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId int = ?
    ,@folderName varchar(255) = ?
    ,@folderPath varchar(255) = ?
    ,@fileId int = ?
    ,@fileName varchar(255) = ?
    ,@sheetInFileIdx int = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr int = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@ID_Arbeitsvorrat_MR_share varchar(255) = ?
;

INSERT INTO [dbo].[exposure_bdx_jb] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Turnover_Year_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[PII_Records_Stored]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd]
    ,[Coverage_4_Sublimit_RestorationData]
    ,[Coverage_5_Sublimit_Reputation]
    ,[Coverage_6_Sublimit_Business_Interruption]
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[ID_Arbeitsvorrat_MR_share]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Turnover_Year_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@PII_Records_Stored
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,@Coverage_4_Sublimit_RestorationData
    ,@Coverage_5_Sublimit_Reputation
    ,@Coverage_6_Sublimit_Business_Interruption
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@ID_Arbeitsvorrat_MR_share
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, ID_Arbeitsvorrat_MR_share ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, ID_Arbeitsvorrat_MR_share: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo varchar(255) = ?
    ,@Coverage varchar(255) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo int = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_No_of_Employees int = ?
    ,@PII_Records_Stored int = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Insured_Homepage varchar(255) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId int = ?
    ,@folderName varchar(255) = ?
    ,@folderPath varchar(255) = ?
    ,@fileId int = ?
    ,@fileName varchar(255) = ?
    ,@sheetInFileIdx int = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr int = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@ID_Arbeitsvorrat_MR_share varchar(255) = ?
;

UPDATE [dbo].[exposure_bdx_jb] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Turnover_Year_ClientInfo] = @Turnover_Year_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[PII_Records_Stored] = @PII_Records_Stored
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd] = @Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,[Coverage_4_Sublimit_RestorationData] = @Coverage_4_Sublimit_RestorationData
    ,[Coverage_5_Sublimit_Reputation] = @Coverage_5_Sublimit_Reputation
    ,[Coverage_6_Sublimit_Business_Interruption] = @Coverage_6_Sublimit_Business_Interruption
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption] = @Coverage_7_Sublimit_Contingent_Business_Interruption
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, ID_Arbeitsvorrat_MR_share ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_bdx_jb]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class tFactExposure:
        # table
        TableName = 'tFactExposure'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tFactExposure]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        PolicyInceptionDateKey = 'PolicyInceptionDateKey'
        Policy_Inception_Date = 'Policy_Inception_Date'
        PolicyExpiryDateKey = 'PolicyExpiryDateKey'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Turnover_Year_ClientInfo = 'Turnover_Year_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        PII_Records_Stored = 'PII_Records_Stored'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        CountryKey = 'CountryKey'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd = 'Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd'
        Coverage_4_Sublimit_RestorationData = 'Coverage_4_Sublimit_RestorationData'
        Coverage_5_Sublimit_Reputation = 'Coverage_5_Sublimit_Reputation'
        Coverage_6_Sublimit_Business_Interruption = 'Coverage_6_Sublimit_Business_Interruption'
        Coverage_7_Sublimit_Contingent_Business_Interruption = 'Coverage_7_Sublimit_Contingent_Business_Interruption'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        DatasourceID = 'DatasourceID'
        rowNr = 'rowNr'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        ClientLimitBandKey = 'ClientLimitBandKey'
        Client_Limit_USD = 'Client_Limit_USD'
        FullLimitBandKey = 'FullLimitBandKey'
        Full_Limit_USD = 'Full_Limit_USD'
        AttachmentBandKey = 'AttachmentBandKey'
        Attachment_USD = 'Attachment_USD'
        SIRBandsKey = 'SIRBandsKey'
        SIR_USD = 'SIR_USD'
        PremiumBandsKey = 'PremiumBandsKey'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        CambridgeKey = 'CambridgeKey'
        Trade_Level_CodeNumber_Mapped_Cambridge = 'Trade_Level_CodeNumber_Mapped_Cambridge'
        MR_Limit_USD = 'MR_Limit_USD'
        MR_Premium_USD = 'MR_Premium_USD'
        MR_GrossNet_Premium_USD = 'MR_GrossNet_Premium_USD'
        mr_share = 'mr_share'
        CompanySegmentKey = 'CompanySegmentKey'
        Turnover_USD_Combined = 'Turnover_USD_Combined'
        FileMetaInfoKey = 'FileMetaInfoKey'
        Insured_Name_BvD = 'Insured_Name_BvD'
        name_alias_bvd = 'name_alias_bvd'
        name_alias_source_bvd = 'name_alias_source_bvd'
        addr_internat_bvd = 'addr_internat_bvd'
        ambest_id_bvd = 'ambest_id_bvd'
        branch_count_bvd = 'branch_count_bvd'
        Trade_Level_CodeNumber_Mapped_Cambridge_BvD = 'Trade_Level_CodeNumber_Mapped_Cambridge_BvD'
        Trade_Level_Name_Mapped_Cambridge_BvD = 'Trade_Level_Name_Mapped_Cambridge_BvD'
        category_of_company_bvd = 'category_of_company_bvd'
        city_internat_bvd = 'city_internat_bvd'
        corporate_group_size_bvd = 'corporate_group_size_bvd'
        country_bvd = 'country_bvd'
        county_bvd = 'county_bvd'
        Country_ISO2_BvD = 'Country_ISO2_BvD'
        direct_parent_bvdid_bvd = 'direct_parent_bvdid_bvd'
        direct_parent_name_internat_bvd = 'direct_parent_name_internat_bvd'
        ein_bvd = 'ein_bvd'
        email_bvd = 'email_bvd'
        employees_bvd = 'employees_bvd'
        eurovat_bvd = 'eurovat_bvd'
        hierarchy_level_bvd = 'hierarchy_level_bvd'
        inactive_bvd = 'inactive_bvd'
        incorporation_date_bvd = 'incorporation_date_bvd'
        legalfrm_bvd = 'legalfrm_bvd'
        lei_lei_bvd = 'lei_lei_bvd'
        listed_bvd = 'listed_bvd'
        mainexch_bvd = 'mainexch_bvd'
        naicsccod2017_bvd = 'naicsccod2017_bvd'
        phone_bvd = 'phone_bvd'
        postcode_bvd = 'postcode_bvd'
        previous_names_set_array_bvd = 'previous_names_set_array_bvd'
        sd_isin_bvd = 'sd_isin_bvd'
        sd_ticker_bvd = 'sd_ticker_bvd'
        slegalf_bvd = 'slegalf_bvd'
        state_us_bvd = 'state_us_bvd'
        subs_count_bvd = 'subs_count_bvd'
        traderegisternr_bvd = 'traderegisternr_bvd'
        Turnover_EUR_BvD = 'Turnover_EUR_BvD'
        Turnover_USD_BvD = 'Turnover_USD_BvD'
        type_of_entity_bvd = 'type_of_entity_bvd'
        ultimate_parent_bvdid_bvd = 'ultimate_parent_bvdid_bvd'
        ultimate_parent_ctryiso_bvd = 'ultimate_parent_ctryiso_bvd'
        ultimate_parent_name_bvd = 'ultimate_parent_name_bvd'
        ussicccod_bvd = 'ussicccod_bvd'
        vatnumber_bvd = 'vatnumber_bvd'
        website_bvd = 'website_bvd'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        DELETE_indicator = 'DELETE_indicator'
        Bvd_Country_Check = 'Bvd_Country_Check'
        Bvd_Industry_Check = 'Bvd_Industry_Check'
        Turnover_Deviation_BvD = 'Turnover_Deviation_BvD'
        Turnover_Dev_as_perc_of_Client_Turnover = 'Turnover_Dev_as_perc_of_Client_Turnover'
        Abs_Dev_perc = 'Abs_Dev_perc'
        Bvd_Turnover_Check = 'Bvd_Turnover_Check'
        Policy_Duration_Day = 'Policy_Duration_Day'
        Policy_Duration_Month = 'Policy_Duration_Month'
        Is_MR_Share_calculated = 'Is_MR_Share_calculated'
        Is_Deleted = 'Is_Deleted'
        Combined_Premium_USD = 'Combined_Premium_USD'
        Combined_Premium_Orig_Curr = 'Combined_Premium_Orig_Curr'
        is_in_overlap_time_period = 'is_in_overlap_time_period'
        Client_Claims = 'Client_Claims'
        Trade_Level_CodeNumber_Mapped_Cambridge_Claims = 'Trade_Level_CodeNumber_Mapped_Cambridge_Claims'
        sum_of_Incurred_Client_Share_USD = 'sum_of_Incurred_Client_Share_USD'
        sum_of_Paid_Client_Share_USD = 'sum_of_Paid_Client_Share_USD'
        sum_of_Incurred_Insured_FGU_USD = 'sum_of_Incurred_Insured_FGU_USD'
        max_Threshold_unind_USD = 'max_Threshold_unind_USD'
        Is_Signal_Reserve = 'Is_Signal_Reserve'
        sum_of_Calculated_or_ClientInfo_FGU_USD = 'sum_of_Calculated_or_ClientInfo_FGU_USD'
        is_Incurred_greaterthan_ClientLimit = 'is_Incurred_greaterthan_ClientLimit'
        is_Client_limit_depleted = 'is_Client_limit_depleted'
        Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined = 'Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined'
        matched_claims_per_policy = 'matched_claims_per_policy'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None, Combined_Premium_USD: float = None, Combined_Premium_Orig_Curr: float = None, is_in_overlap_time_period: int = None, Client_Claims: str = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims: str = None, sum_of_Incurred_Client_Share_USD: float = None, sum_of_Paid_Client_Share_USD: float = None, sum_of_Incurred_Insured_FGU_USD: float = None, max_Threshold_unind_USD: float = None, Is_Signal_Reserve: int = None, sum_of_Calculated_or_ClientInfo_FGU_USD: float = None, is_Incurred_greaterthan_ClientLimit: int = None, is_Client_limit_depleted: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, matched_claims_per_policy: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@DatasourceID nvarchar(466) = ?
    ,@rowNr bigint = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@PremiumBandsKey int = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@CambridgeKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@mr_share float = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_Combined float = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Bvd_Country_Check nvarchar(510) = ?
    ,@Bvd_Industry_Check nvarchar(510) = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Bvd_Turnover_Check nvarchar(510) = ?
    ,@Policy_Duration_Day int = ?
    ,@Policy_Duration_Month int = ?
    ,@Is_MR_Share_calculated nvarchar(60) = ?
    ,@Is_Deleted nvarchar(100) = ?
    ,@Combined_Premium_USD float = ?
    ,@Combined_Premium_Orig_Curr float = ?
    ,@is_in_overlap_time_period bit = ?
    ,@Client_Claims nvarchar(400) = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims nvarchar(100) = ?
    ,@sum_of_Incurred_Client_Share_USD float = ?
    ,@sum_of_Paid_Client_Share_USD float = ?
    ,@sum_of_Incurred_Insured_FGU_USD float = ?
    ,@max_Threshold_unind_USD float = ?
    ,@Is_Signal_Reserve bit = ?
    ,@sum_of_Calculated_or_ClientInfo_FGU_USD float = ?
    ,@is_Incurred_greaterthan_ClientLimit bit = ?
    ,@is_Client_limit_depleted bit = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined nvarchar(100) = ?
    ,@matched_claims_per_policy int = ?
;

INSERT INTO [dbo].[tFactExposure] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[ID_Arbeitsvorrat_MR_share]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[PolicyInceptionDateKey]
    ,[Policy_Inception_Date]
    ,[PolicyExpiryDateKey]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Turnover_Year_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[PII_Records_Stored]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[CountryKey]
    ,[Insured_Country_ISO2]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd]
    ,[Coverage_4_Sublimit_RestorationData]
    ,[Coverage_5_Sublimit_Reputation]
    ,[Coverage_6_Sublimit_Business_Interruption]
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[DatasourceID]
    ,[rowNr]
    ,[Turnover_ClientInfo_USD]
    ,[ClientLimitBandKey]
    ,[Client_Limit_USD]
    ,[FullLimitBandKey]
    ,[Full_Limit_USD]
    ,[AttachmentBandKey]
    ,[Attachment_USD]
    ,[SIRBandsKey]
    ,[SIR_USD]
    ,[PremiumBandsKey]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[CambridgeKey]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge]
    ,[MR_Limit_USD]
    ,[MR_Premium_USD]
    ,[MR_GrossNet_Premium_USD]
    ,[mr_share]
    ,[CompanySegmentKey]
    ,[Turnover_USD_Combined]
    ,[FileMetaInfoKey]
    ,[Insured_Name_BvD]
    ,[name_alias_bvd]
    ,[name_alias_source_bvd]
    ,[addr_internat_bvd]
    ,[ambest_id_bvd]
    ,[branch_count_bvd]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD]
    ,[Trade_Level_Name_Mapped_Cambridge_BvD]
    ,[category_of_company_bvd]
    ,[city_internat_bvd]
    ,[corporate_group_size_bvd]
    ,[country_bvd]
    ,[county_bvd]
    ,[Country_ISO2_BvD]
    ,[direct_parent_bvdid_bvd]
    ,[direct_parent_name_internat_bvd]
    ,[ein_bvd]
    ,[email_bvd]
    ,[employees_bvd]
    ,[eurovat_bvd]
    ,[hierarchy_level_bvd]
    ,[inactive_bvd]
    ,[incorporation_date_bvd]
    ,[legalfrm_bvd]
    ,[lei_lei_bvd]
    ,[listed_bvd]
    ,[mainexch_bvd]
    ,[naicsccod2017_bvd]
    ,[phone_bvd]
    ,[postcode_bvd]
    ,[previous_names_set_array_bvd]
    ,[sd_isin_bvd]
    ,[sd_ticker_bvd]
    ,[slegalf_bvd]
    ,[state_us_bvd]
    ,[subs_count_bvd]
    ,[traderegisternr_bvd]
    ,[Turnover_EUR_BvD]
    ,[Turnover_USD_BvD]
    ,[type_of_entity_bvd]
    ,[ultimate_parent_bvdid_bvd]
    ,[ultimate_parent_ctryiso_bvd]
    ,[ultimate_parent_name_bvd]
    ,[ussicccod_bvd]
    ,[vatnumber_bvd]
    ,[website_bvd]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[DELETE_indicator]
    ,[Bvd_Country_Check]
    ,[Bvd_Industry_Check]
    ,[Turnover_Deviation_BvD]
    ,[Turnover_Dev_as_perc_of_Client_Turnover]
    ,[Abs_Dev_perc]
    ,[Bvd_Turnover_Check]
    ,[Policy_Duration_Day]
    ,[Policy_Duration_Month]
    ,[Is_MR_Share_calculated]
    ,[Is_Deleted]
    ,[Combined_Premium_USD]
    ,[Combined_Premium_Orig_Curr]
    ,[is_in_overlap_time_period]
    ,[Client_Claims]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims]
    ,[sum_of_Incurred_Client_Share_USD]
    ,[sum_of_Paid_Client_Share_USD]
    ,[sum_of_Incurred_Insured_FGU_USD]
    ,[max_Threshold_unind_USD]
    ,[Is_Signal_Reserve]
    ,[sum_of_Calculated_or_ClientInfo_FGU_USD]
    ,[is_Incurred_greaterthan_ClientLimit]
    ,[is_Client_limit_depleted]
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined]
    ,[matched_claims_per_policy]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@ID_Arbeitsvorrat_MR_share
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@PolicyInceptionDateKey
    ,@Policy_Inception_Date
    ,@PolicyExpiryDateKey
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Turnover_Year_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Insured_No_of_Employees
    ,@PII_Records_Stored
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@CountryKey
    ,@Insured_Country_ISO2
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,@Coverage_4_Sublimit_RestorationData
    ,@Coverage_5_Sublimit_Reputation
    ,@Coverage_6_Sublimit_Business_Interruption
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@DatasourceID
    ,@rowNr
    ,@Turnover_ClientInfo_USD
    ,@ClientLimitBandKey
    ,@Client_Limit_USD
    ,@FullLimitBandKey
    ,@Full_Limit_USD
    ,@AttachmentBandKey
    ,@Attachment_USD
    ,@SIRBandsKey
    ,@SIR_USD
    ,@PremiumBandsKey
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@BvD_ID
    ,@Duplicate_ID
    ,@CambridgeKey
    ,@Trade_Level_CodeNumber_Mapped_Cambridge
    ,@MR_Limit_USD
    ,@MR_Premium_USD
    ,@MR_GrossNet_Premium_USD
    ,@mr_share
    ,@CompanySegmentKey
    ,@Turnover_USD_Combined
    ,@FileMetaInfoKey
    ,@Insured_Name_BvD
    ,@name_alias_bvd
    ,@name_alias_source_bvd
    ,@addr_internat_bvd
    ,@ambest_id_bvd
    ,@branch_count_bvd
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,@Trade_Level_Name_Mapped_Cambridge_BvD
    ,@category_of_company_bvd
    ,@city_internat_bvd
    ,@corporate_group_size_bvd
    ,@country_bvd
    ,@county_bvd
    ,@Country_ISO2_BvD
    ,@direct_parent_bvdid_bvd
    ,@direct_parent_name_internat_bvd
    ,@ein_bvd
    ,@email_bvd
    ,@employees_bvd
    ,@eurovat_bvd
    ,@hierarchy_level_bvd
    ,@inactive_bvd
    ,@incorporation_date_bvd
    ,@legalfrm_bvd
    ,@lei_lei_bvd
    ,@listed_bvd
    ,@mainexch_bvd
    ,@naicsccod2017_bvd
    ,@phone_bvd
    ,@postcode_bvd
    ,@previous_names_set_array_bvd
    ,@sd_isin_bvd
    ,@sd_ticker_bvd
    ,@slegalf_bvd
    ,@state_us_bvd
    ,@subs_count_bvd
    ,@traderegisternr_bvd
    ,@Turnover_EUR_BvD
    ,@Turnover_USD_BvD
    ,@type_of_entity_bvd
    ,@ultimate_parent_bvdid_bvd
    ,@ultimate_parent_ctryiso_bvd
    ,@ultimate_parent_name_bvd
    ,@ussicccod_bvd
    ,@vatnumber_bvd
    ,@website_bvd
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@DELETE_indicator
    ,@Bvd_Country_Check
    ,@Bvd_Industry_Check
    ,@Turnover_Deviation_BvD
    ,@Turnover_Dev_as_perc_of_Client_Turnover
    ,@Abs_Dev_perc
    ,@Bvd_Turnover_Check
    ,@Policy_Duration_Day
    ,@Policy_Duration_Month
    ,@Is_MR_Share_calculated
    ,@Is_Deleted
    ,@Combined_Premium_USD
    ,@Combined_Premium_Orig_Curr
    ,@is_in_overlap_time_period
    ,@Client_Claims
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims
    ,@sum_of_Incurred_Client_Share_USD
    ,@sum_of_Paid_Client_Share_USD
    ,@sum_of_Incurred_Insured_FGU_USD
    ,@max_Threshold_unind_USD
    ,@Is_Signal_Reserve
    ,@sum_of_Calculated_or_ClientInfo_FGU_USD
    ,@is_Incurred_greaterthan_ClientLimit
    ,@is_Client_limit_depleted
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined
    ,@matched_claims_per_policy
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted, Combined_Premium_USD, Combined_Premium_Orig_Curr, is_in_overlap_time_period, Client_Claims, Trade_Level_CodeNumber_Mapped_Cambridge_Claims, sum_of_Incurred_Client_Share_USD, sum_of_Paid_Client_Share_USD, sum_of_Incurred_Insured_FGU_USD, max_Threshold_unind_USD, Is_Signal_Reserve, sum_of_Calculated_or_ClientInfo_FGU_USD, is_Incurred_greaterthan_ClientLimit, is_Client_limit_depleted, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, matched_claims_per_policy ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None, Combined_Premium_USD: float = None, Combined_Premium_Orig_Curr: float = None, is_in_overlap_time_period: int = None, Client_Claims: str = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims: str = None, sum_of_Incurred_Client_Share_USD: float = None, sum_of_Paid_Client_Share_USD: float = None, sum_of_Incurred_Insured_FGU_USD: float = None, max_Threshold_unind_USD: float = None, Is_Signal_Reserve: int = None, sum_of_Calculated_or_ClientInfo_FGU_USD: float = None, is_Incurred_greaterthan_ClientLimit: int = None, is_Client_limit_depleted: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, matched_claims_per_policy: int = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@PolicyInceptionDateKey int = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@PolicyExpiryDateKey int = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@CountryKey int = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@DatasourceID nvarchar(466) = ?
    ,@rowNr bigint = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@ClientLimitBandKey int = ?
    ,@Client_Limit_USD float = ?
    ,@FullLimitBandKey int = ?
    ,@Full_Limit_USD float = ?
    ,@AttachmentBandKey int = ?
    ,@Attachment_USD float = ?
    ,@SIRBandsKey int = ?
    ,@SIR_USD float = ?
    ,@PremiumBandsKey int = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@CambridgeKey int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge nvarchar(100) = ?
    ,@MR_Limit_USD float = ?
    ,@MR_Premium_USD float = ?
    ,@MR_GrossNet_Premium_USD float = ?
    ,@mr_share float = ?
    ,@CompanySegmentKey int = ?
    ,@Turnover_USD_Combined float = ?
    ,@FileMetaInfoKey int = ?
    ,@Insured_Name_BvD nvarchar(2048) = ?
    ,@name_alias_bvd nvarchar(2048) = ?
    ,@name_alias_source_bvd nvarchar(128) = ?
    ,@addr_internat_bvd nvarchar(2048) = ?
    ,@ambest_id_bvd nvarchar(100) = ?
    ,@branch_count_bvd int = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_BvD nvarchar(100) = ?
    ,@Trade_Level_Name_Mapped_Cambridge_BvD nvarchar(2048) = ?
    ,@category_of_company_bvd nvarchar(100) = ?
    ,@city_internat_bvd nvarchar(400) = ?
    ,@corporate_group_size_bvd int = ?
    ,@country_bvd nvarchar(200) = ?
    ,@county_bvd nvarchar(100) = ?
    ,@Country_ISO2_BvD nvarchar(10) = ?
    ,@direct_parent_bvdid_bvd nvarchar(100) = ?
    ,@direct_parent_name_internat_bvd nvarchar(2048) = ?
    ,@ein_bvd nvarchar(100) = ?
    ,@email_bvd nvarchar(200) = ?
    ,@employees_bvd int = ?
    ,@eurovat_bvd nvarchar(100) = ?
    ,@hierarchy_level_bvd int = ?
    ,@inactive_bvd nvarchar(16) = ?
    ,@incorporation_date_bvd nvarchar(100) = ?
    ,@legalfrm_bvd nvarchar(400) = ?
    ,@lei_lei_bvd nvarchar(100) = ?
    ,@listed_bvd nvarchar(100) = ?
    ,@mainexch_bvd nvarchar(400) = ?
    ,@naicsccod2017_bvd nvarchar(2048) = ?
    ,@phone_bvd nvarchar(100) = ?
    ,@postcode_bvd nvarchar(100) = ?
    ,@previous_names_set_array_bvd nvarchar(4096) = ?
    ,@sd_isin_bvd nvarchar(100) = ?
    ,@sd_ticker_bvd nvarchar(100) = ?
    ,@slegalf_bvd nvarchar(200) = ?
    ,@state_us_bvd nvarchar(10) = ?
    ,@subs_count_bvd int = ?
    ,@traderegisternr_bvd nvarchar(100) = ?
    ,@Turnover_EUR_BvD bigint = ?
    ,@Turnover_USD_BvD bigint = ?
    ,@type_of_entity_bvd nvarchar(200) = ?
    ,@ultimate_parent_bvdid_bvd nvarchar(100) = ?
    ,@ultimate_parent_ctryiso_bvd nvarchar(10) = ?
    ,@ultimate_parent_name_bvd nvarchar(2048) = ?
    ,@ussicccod_bvd nvarchar(2048) = ?
    ,@vatnumber_bvd nvarchar(100) = ?
    ,@website_bvd nvarchar(400) = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Bvd_Country_Check nvarchar(510) = ?
    ,@Bvd_Industry_Check nvarchar(510) = ?
    ,@Turnover_Deviation_BvD float = ?
    ,@Turnover_Dev_as_perc_of_Client_Turnover float = ?
    ,@Abs_Dev_perc float = ?
    ,@Bvd_Turnover_Check nvarchar(510) = ?
    ,@Policy_Duration_Day int = ?
    ,@Policy_Duration_Month int = ?
    ,@Is_MR_Share_calculated nvarchar(60) = ?
    ,@Is_Deleted nvarchar(100) = ?
    ,@Combined_Premium_USD float = ?
    ,@Combined_Premium_Orig_Curr float = ?
    ,@is_in_overlap_time_period bit = ?
    ,@Client_Claims nvarchar(400) = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims nvarchar(100) = ?
    ,@sum_of_Incurred_Client_Share_USD float = ?
    ,@sum_of_Paid_Client_Share_USD float = ?
    ,@sum_of_Incurred_Insured_FGU_USD float = ?
    ,@max_Threshold_unind_USD float = ?
    ,@Is_Signal_Reserve bit = ?
    ,@sum_of_Calculated_or_ClientInfo_FGU_USD float = ?
    ,@is_Incurred_greaterthan_ClientLimit bit = ?
    ,@is_Client_limit_depleted bit = ?
    ,@Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined nvarchar(100) = ?
    ,@matched_claims_per_policy int = ?
;

UPDATE [dbo].[tFactExposure] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[PolicyInceptionDateKey] = @PolicyInceptionDateKey
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[PolicyExpiryDateKey] = @PolicyExpiryDateKey
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Turnover_Year_ClientInfo] = @Turnover_Year_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[PII_Records_Stored] = @PII_Records_Stored
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[CountryKey] = @CountryKey
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd] = @Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,[Coverage_4_Sublimit_RestorationData] = @Coverage_4_Sublimit_RestorationData
    ,[Coverage_5_Sublimit_Reputation] = @Coverage_5_Sublimit_Reputation
    ,[Coverage_6_Sublimit_Business_Interruption] = @Coverage_6_Sublimit_Business_Interruption
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption] = @Coverage_7_Sublimit_Contingent_Business_Interruption
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[DatasourceID] = @DatasourceID
    ,[rowNr] = @rowNr
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[ClientLimitBandKey] = @ClientLimitBandKey
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[FullLimitBandKey] = @FullLimitBandKey
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[AttachmentBandKey] = @AttachmentBandKey
    ,[Attachment_USD] = @Attachment_USD
    ,[SIRBandsKey] = @SIRBandsKey
    ,[SIR_USD] = @SIR_USD
    ,[PremiumBandsKey] = @PremiumBandsKey
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[CambridgeKey] = @CambridgeKey
    ,[Trade_Level_CodeNumber_Mapped_Cambridge] = @Trade_Level_CodeNumber_Mapped_Cambridge
    ,[MR_Limit_USD] = @MR_Limit_USD
    ,[MR_Premium_USD] = @MR_Premium_USD
    ,[MR_GrossNet_Premium_USD] = @MR_GrossNet_Premium_USD
    ,[mr_share] = @mr_share
    ,[CompanySegmentKey] = @CompanySegmentKey
    ,[Turnover_USD_Combined] = @Turnover_USD_Combined
    ,[FileMetaInfoKey] = @FileMetaInfoKey
    ,[Insured_Name_BvD] = @Insured_Name_BvD
    ,[name_alias_bvd] = @name_alias_bvd
    ,[name_alias_source_bvd] = @name_alias_source_bvd
    ,[addr_internat_bvd] = @addr_internat_bvd
    ,[ambest_id_bvd] = @ambest_id_bvd
    ,[branch_count_bvd] = @branch_count_bvd
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_BvD] = @Trade_Level_CodeNumber_Mapped_Cambridge_BvD
    ,[Trade_Level_Name_Mapped_Cambridge_BvD] = @Trade_Level_Name_Mapped_Cambridge_BvD
    ,[category_of_company_bvd] = @category_of_company_bvd
    ,[city_internat_bvd] = @city_internat_bvd
    ,[corporate_group_size_bvd] = @corporate_group_size_bvd
    ,[country_bvd] = @country_bvd
    ,[county_bvd] = @county_bvd
    ,[Country_ISO2_BvD] = @Country_ISO2_BvD
    ,[direct_parent_bvdid_bvd] = @direct_parent_bvdid_bvd
    ,[direct_parent_name_internat_bvd] = @direct_parent_name_internat_bvd
    ,[ein_bvd] = @ein_bvd
    ,[email_bvd] = @email_bvd
    ,[employees_bvd] = @employees_bvd
    ,[eurovat_bvd] = @eurovat_bvd
    ,[hierarchy_level_bvd] = @hierarchy_level_bvd
    ,[inactive_bvd] = @inactive_bvd
    ,[incorporation_date_bvd] = @incorporation_date_bvd
    ,[legalfrm_bvd] = @legalfrm_bvd
    ,[lei_lei_bvd] = @lei_lei_bvd
    ,[listed_bvd] = @listed_bvd
    ,[mainexch_bvd] = @mainexch_bvd
    ,[naicsccod2017_bvd] = @naicsccod2017_bvd
    ,[phone_bvd] = @phone_bvd
    ,[postcode_bvd] = @postcode_bvd
    ,[previous_names_set_array_bvd] = @previous_names_set_array_bvd
    ,[sd_isin_bvd] = @sd_isin_bvd
    ,[sd_ticker_bvd] = @sd_ticker_bvd
    ,[slegalf_bvd] = @slegalf_bvd
    ,[state_us_bvd] = @state_us_bvd
    ,[subs_count_bvd] = @subs_count_bvd
    ,[traderegisternr_bvd] = @traderegisternr_bvd
    ,[Turnover_EUR_BvD] = @Turnover_EUR_BvD
    ,[Turnover_USD_BvD] = @Turnover_USD_BvD
    ,[type_of_entity_bvd] = @type_of_entity_bvd
    ,[ultimate_parent_bvdid_bvd] = @ultimate_parent_bvdid_bvd
    ,[ultimate_parent_ctryiso_bvd] = @ultimate_parent_ctryiso_bvd
    ,[ultimate_parent_name_bvd] = @ultimate_parent_name_bvd
    ,[ussicccod_bvd] = @ussicccod_bvd
    ,[vatnumber_bvd] = @vatnumber_bvd
    ,[website_bvd] = @website_bvd
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Bvd_Country_Check] = @Bvd_Country_Check
    ,[Bvd_Industry_Check] = @Bvd_Industry_Check
    ,[Turnover_Deviation_BvD] = @Turnover_Deviation_BvD
    ,[Turnover_Dev_as_perc_of_Client_Turnover] = @Turnover_Dev_as_perc_of_Client_Turnover
    ,[Abs_Dev_perc] = @Abs_Dev_perc
    ,[Bvd_Turnover_Check] = @Bvd_Turnover_Check
    ,[Policy_Duration_Day] = @Policy_Duration_Day
    ,[Policy_Duration_Month] = @Policy_Duration_Month
    ,[Is_MR_Share_calculated] = @Is_MR_Share_calculated
    ,[Is_Deleted] = @Is_Deleted
    ,[Combined_Premium_USD] = @Combined_Premium_USD
    ,[Combined_Premium_Orig_Curr] = @Combined_Premium_Orig_Curr
    ,[is_in_overlap_time_period] = @is_in_overlap_time_period
    ,[Client_Claims] = @Client_Claims
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims] = @Trade_Level_CodeNumber_Mapped_Cambridge_Claims
    ,[sum_of_Incurred_Client_Share_USD] = @sum_of_Incurred_Client_Share_USD
    ,[sum_of_Paid_Client_Share_USD] = @sum_of_Paid_Client_Share_USD
    ,[sum_of_Incurred_Insured_FGU_USD] = @sum_of_Incurred_Insured_FGU_USD
    ,[max_Threshold_unind_USD] = @max_Threshold_unind_USD
    ,[Is_Signal_Reserve] = @Is_Signal_Reserve
    ,[sum_of_Calculated_or_ClientInfo_FGU_USD] = @sum_of_Calculated_or_ClientInfo_FGU_USD
    ,[is_Incurred_greaterthan_ClientLimit] = @is_Incurred_greaterthan_ClientLimit
    ,[is_Client_limit_depleted] = @is_Client_limit_depleted
    ,[Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined] = @Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined
    ,[matched_claims_per_policy] = @matched_claims_per_policy
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted, Combined_Premium_USD, Combined_Premium_Orig_Curr, is_in_overlap_time_period, Client_Claims, Trade_Level_CodeNumber_Mapped_Cambridge_Claims, sum_of_Incurred_Client_Share_USD, sum_of_Paid_Client_Share_USD, sum_of_Incurred_Insured_FGU_USD, max_Threshold_unind_USD, Is_Signal_Reserve, sum_of_Calculated_or_ClientInfo_FGU_USD, is_Incurred_greaterthan_ClientLimit, is_Client_limit_depleted, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, matched_claims_per_policy ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tFactExposure]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class z_Policy_Bands:
        # table
        TableName = 'z_Policy_Bands'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[z_Policy_Bands]'
        # columns
        Band_Index = 'Band Index'
        Band_Name = 'Band_Name'
        Band_With_Type = 'Band_With_Type'
        Indexed_Band_Name = 'Indexed_Band_Name'
        Count_of_PolicyBand_Rank = 'Count of PolicyBand_Rank'
        Rank = 'Rank'
        Type = 'Type'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_Index: str, Band_Name: str, Band_With_Type: str, Indexed_Band_Name: str, Count_of_PolicyBand_Rank: int, Rank: int, Type: str) -> DataFrame:
            sql = """
DECLARE
    @Band_Index varchar(7) = ?
    ,@Band_Name varchar(15) = ?
    ,@Band_With_Type varchar(20) = ?
    ,@Indexed_Band_Name varchar(21) = ?
    ,@Count_of_PolicyBand_Rank int = ?
    ,@Rank tinyint = ?
    ,@Type varchar(3) = ?
;

INSERT INTO [dbo].[z_Policy_Bands] (
    [Band Index]
    ,[Band_Name]
    ,[Band_With_Type]
    ,[Indexed_Band_Name]
    ,[Count of PolicyBand_Rank]
    ,[Rank]
    ,[Type]
)
VALUES (
    @Band_Index
    ,@Band_Name
    ,@Band_With_Type
    ,@Indexed_Band_Name
    ,@Count_of_PolicyBand_Rank
    ,@Rank
    ,@Type
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Index, Band_Name, Band_With_Type, Indexed_Band_Name, Count_of_PolicyBand_Rank, Rank, Type ]).exec_df()

        def update(self, Band_Index: str, Band_Name: str, Band_With_Type: str, Indexed_Band_Name: str, Count_of_PolicyBand_Rank: int, Rank: int, Type: str) -> DataFrame:
            sql = """
DECLARE
    @Band_Index varchar(7) = ?
    ,@Band_Name varchar(15) = ?
    ,@Band_With_Type varchar(20) = ?
    ,@Indexed_Band_Name varchar(21) = ?
    ,@Count_of_PolicyBand_Rank int = ?
    ,@Rank tinyint = ?
    ,@Type varchar(3) = ?
;

UPDATE [dbo].[z_Policy_Bands] SET 
    [Band Index] = @Band_Index
    ,[Band_Name] = @Band_Name
    ,[Band_With_Type] = @Band_With_Type
    ,[Indexed_Band_Name] = @Indexed_Band_Name
    ,[Count of PolicyBand_Rank] = @Count_of_PolicyBand_Rank
    ,[Rank] = @Rank
    ,[Type] = @Type
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_Index, Band_Name, Band_With_Type, Indexed_Band_Name, Count_of_PolicyBand_Rank, Rank, Type ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[z_Policy_Bands]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class random_place_JB:
        # table
        TableName = 'random_place_JB'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[random_place_JB]'
        # columns
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None) -> DataFrame:
            sql = """
DECLARE
    @Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
;

INSERT INTO [dbo].[random_place_JB] (
    [Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
)
VALUES (
    @Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
);
"""
            return DbCmd(self.cnOrStr, sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ]).exec_df()

        def update(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None) -> DataFrame:
            sql = """
DECLARE
    @Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
;

UPDATE [dbo].[random_place_JB] SET 
    [Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[random_place_JB]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Amount_Band:
        # table
        TableName = 'Dim_Amount_Band'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Amount_Band]'
        # columns
        Band_ID = 'Band_ID'
        Interval_Start = 'Interval_Start'
        Interval_End = 'Interval_End'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Band_ID: int, Interval_Start: int, Interval_End: int) -> DataFrame:
            sql = """
DECLARE
    @Band_ID bigint = ?
    ,@Interval_Start bigint = ?
    ,@Interval_End bigint = ?
;

INSERT INTO [dbo].[Dim_Amount_Band] (
    [Band_ID]
    ,[Interval_Start]
    ,[Interval_End]
)
VALUES (
    @Band_ID
    ,@Interval_Start
    ,@Interval_End
);
"""
            return DbCmd(self.cnOrStr, sql, [ Band_ID, Interval_Start, Interval_End ]).exec_df()

        def update(self, Band_ID: int, Interval_Start: int, Interval_End: int) -> DataFrame:
            sql = """
DECLARE
    @Band_ID bigint = ?
    ,@Interval_Start bigint = ?
    ,@Interval_End bigint = ?
;

UPDATE [dbo].[Dim_Amount_Band] SET 
    [Interval_Start] = @Interval_Start
    ,[Interval_End] = @Interval_End
 WHERE
    [Band_ID] = @Band_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_ID, Interval_Start, Interval_End ]).exec_df()

        def delete(self, Band_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Band_ID bigint = ?
;

DELETE [dbo].[Dim_Amount_Band]
WHERE
    [Band_ID] = @Band_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Band_ID ]).exec_df()

    class Claim_ClientInfo:
        # table
        TableName = 'Claim_ClientInfo'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Claim_ClientInfo]'
        # columns
        Claim_ClientInfo_ID = 'Claim_ClientInfo_ID'
        Claim_ID = 'Claim_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Insured_Name_Clean_ID = 'Insured_Name_Clean_ID'
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'
        Claim_Ref_ClientInfo = 'Claim_Ref_ClientInfo'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Type_of_Loss = 'Type_of_Loss'
        Loss_Event_ID = 'Loss_Event_ID'
        Claims_Description = 'Claims_Description'
        Policy_Ref_ClientInfo = 'Policy_Ref_ClientInfo'
        Policy_Ref_Clean = 'Policy_Ref_Clean'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Currency_ID = 'Policy_Currency_ID'
        Source = 'Source'
        Insured_Name_ClientInfo_Uncut = 'Insured_Name_ClientInfo_Uncut'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Source: str, Claim_ID: int = None, Technical_Client_ID: int = None, Insured_Name_Clean_ID: int = None, Company_ClientInfo_ID: int = None, Claim_Ref_ClientInfo: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref_ClientInfo: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Insured_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Insured_Name_Clean_ID bigint = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@Claim_Ref_ClientInfo nvarchar(1000) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref_ClientInfo nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo_Uncut nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Claim_ClientInfo] (
    [Claim_ID]
    ,[Technical_Client_ID]
    ,[Insured_Name_Clean_ID]
    ,[Company_ClientInfo_ID]
    ,[Claim_Ref_ClientInfo]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Country_ISO2_ID]
    ,[Type_of_Loss]
    ,[Loss_Event_ID]
    ,[Claims_Description]
    ,[Policy_Ref_ClientInfo]
    ,[Policy_Ref_Clean]
    ,[Policy_Inception_Date]
    ,[Policy_Currency_ID]
    ,[Source]
    ,[Insured_Name_ClientInfo_Uncut]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Claim_ID
    ,@Technical_Client_ID
    ,@Insured_Name_Clean_ID
    ,@Company_ClientInfo_ID
    ,@Claim_Ref_ClientInfo
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Country_ISO2_ID
    ,@Type_of_Loss
    ,@Loss_Event_ID
    ,@Claims_Description
    ,@Policy_Ref_ClientInfo
    ,@Policy_Ref_Clean
    ,@Policy_Inception_Date
    ,@Policy_Currency_ID
    ,@Source
    ,@Insured_Name_ClientInfo_Uncut
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ID, Technical_Client_ID, Insured_Name_Clean_ID, Company_ClientInfo_ID, Claim_Ref_ClientInfo, Date_of_Incident, Date_of_Notification, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref_ClientInfo, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source, Insured_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Claim_ClientInfo_ID: int, Source: str, Claim_ID: int = None, Technical_Client_ID: int = None, Insured_Name_Clean_ID: int = None, Company_ClientInfo_ID: int = None, Claim_Ref_ClientInfo: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref_ClientInfo: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Insured_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_ClientInfo_ID bigint = ?
    ,@Claim_ID bigint = ?
    ,@Technical_Client_ID bigint = ?
    ,@Insured_Name_Clean_ID bigint = ?
    ,@Company_ClientInfo_ID bigint = ?
    ,@Claim_Ref_ClientInfo nvarchar(1000) = ?
    ,@Date_of_Incident date(0) = ?
    ,@Date_of_Notification date(0) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@Type_of_Loss nvarchar(400) = ?
    ,@Loss_Event_ID bigint = ?
    ,@Claims_Description nvarchar(8000) = ?
    ,@Policy_Ref_ClientInfo nvarchar(1000) = ?
    ,@Policy_Ref_Clean nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Currency_ID bigint = ?
    ,@Source nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo_Uncut nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Claim_ClientInfo] SET 
    [Claim_ID] = @Claim_ID
    ,[Technical_Client_ID] = @Technical_Client_ID
    ,[Insured_Name_Clean_ID] = @Insured_Name_Clean_ID
    ,[Company_ClientInfo_ID] = @Company_ClientInfo_ID
    ,[Claim_Ref_ClientInfo] = @Claim_Ref_ClientInfo
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Loss_Event_ID] = @Loss_Event_ID
    ,[Claims_Description] = @Claims_Description
    ,[Policy_Ref_ClientInfo] = @Policy_Ref_ClientInfo
    ,[Policy_Ref_Clean] = @Policy_Ref_Clean
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Currency_ID] = @Policy_Currency_ID
    ,[Source] = @Source
    ,[Insured_Name_ClientInfo_Uncut] = @Insured_Name_ClientInfo_Uncut
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Claim_ClientInfo_ID] = @Claim_ClientInfo_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ClientInfo_ID, Claim_ID, Technical_Client_ID, Insured_Name_Clean_ID, Company_ClientInfo_ID, Claim_Ref_ClientInfo, Date_of_Incident, Date_of_Notification, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref_ClientInfo, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source, Insured_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Claim_ClientInfo_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Claim_ClientInfo_ID bigint = ?
;

DELETE [dbo].[Claim_ClientInfo]
WHERE
    [Claim_ClientInfo_ID] = @Claim_ClientInfo_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_ClientInfo_ID ]).exec_df()

    class claims_bdx_jb:
        # table
        TableName = 'claims_bdx_jb'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx_jb]'
        # columns
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None) -> DataFrame:
            sql = """
DECLARE
    @Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
;

INSERT INTO [dbo].[claims_bdx_jb] (
    [Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
)
VALUES (
    @Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
);
"""
            return DbCmd(self.cnOrStr, sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ]).exec_df()

        def update(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None) -> DataFrame:
            sql = """
DECLARE
    @Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
;

UPDATE [dbo].[claims_bdx_jb] SET 
    [Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_bdx_jb]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class exposure_unpivoted_extended:
        # table
        TableName = 'exposure_unpivoted_extended'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[exposure_unpivoted_extended]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        PolicyColumn = 'PolicyColumn'
        value = 'value'
        Band_Index = 'Band_Index'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, id: int, ID_Arbeitsvorrat: str = None, PolicyColumn: str = None, value: float = None, Band_Index: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@PolicyColumn nvarchar(8000) = ?
    ,@value float = ?
    ,@Band_Index nvarchar(20) = ?
;

INSERT INTO [dbo].[exposure_unpivoted_extended] (
    [id]
    ,[ID_Arbeitsvorrat]
    ,[PolicyColumn]
    ,[value]
    ,[Band_Index]
)
VALUES (
    @id
    ,@ID_Arbeitsvorrat
    ,@PolicyColumn
    ,@value
    ,@Band_Index
);
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, PolicyColumn, value, Band_Index ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str = None, PolicyColumn: str = None, value: float = None, Band_Index: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@PolicyColumn nvarchar(8000) = ?
    ,@value float = ?
    ,@Band_Index nvarchar(20) = ?
;

UPDATE [dbo].[exposure_unpivoted_extended] SET 
    [id] = @id
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[PolicyColumn] = @PolicyColumn
    ,[value] = @value
    ,[Band_Index] = @Band_Index
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, PolicyColumn, value, Band_Index ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[exposure_unpivoted_extended]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Turnover_History:
        # table
        TableName = 'Turnover_History'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Turnover_History]'
        # columns
        id = 'id'
        Turnover_ID = 'Turnover_ID'
        Company_ID = 'Company_ID'
        Turnover_Year = 'Turnover_Year'
        Turnover = 'Turnover'
        Currency_ID = 'Currency_ID'
        Source_of_Change = 'Source_of_Change'
        Is_Manually_Curated = 'Is_Manually_Curated'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Turnover_ID: int, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Turnover_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Turnover_Year int = ?
    ,@Turnover float = ?
    ,@Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Turnover_History] (
    [Turnover_ID]
    ,[Company_ID]
    ,[Turnover_Year]
    ,[Turnover]
    ,[Currency_ID]
    ,[Source_of_Change]
    ,[Is_Manually_Curated]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Turnover_ID
    ,@Company_ID
    ,@Turnover_Year
    ,@Turnover
    ,@Currency_ID
    ,@Source_of_Change
    ,@Is_Manually_Curated
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Turnover_ID, Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, id: int, Turnover_ID: int, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@Turnover_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Turnover_Year int = ?
    ,@Turnover float = ?
    ,@Currency_ID bigint = ?
    ,@Source_of_Change nvarchar(2048) = ?
    ,@Is_Manually_Curated bit = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Turnover_History] SET 
    [Turnover_ID] = @Turnover_ID
    ,[Company_ID] = @Company_ID
    ,[Turnover_Year] = @Turnover_Year
    ,[Turnover] = @Turnover
    ,[Currency_ID] = @Currency_ID
    ,[Source_of_Change] = @Source_of_Change
    ,[Is_Manually_Curated] = @Is_Manually_Curated
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, Turnover_ID, Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[Turnover_History]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class tDimCompany_Segment:
        # table
        TableName = 'tDimCompany_Segment'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimCompany_Segment]'
        # columns
        CompanySegmentKey = 'CompanySegmentKey'
        Company_Segment = 'Company_Segment'
        Index = 'Index'
        Indexed_Company_Seg_Name = 'Indexed_Company_Seg_Name'
        Company_Sub_Segment = 'Company_Sub_Segment'
        Index_Sub_Segment = 'Index_Sub_Segment'
        Indexed_Company_Sub_Seg_Name = 'Indexed_Company_Sub_Seg_Name'
        RangeStart = 'RangeStart'
        RangeEnd = 'RangeEnd'
        IntervalStart = 'IntervalStart'
        IntervalEnd = 'IntervalEnd'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_Segment: str, Index: int, Indexed_Company_Seg_Name: str, Company_Sub_Segment: str, Index_Sub_Segment: int, Indexed_Company_Sub_Seg_Name: str, RangeStart: str = None, RangeEnd: str = None, IntervalStart: int = None, IntervalEnd: int = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Segment nvarchar(100) = ?
    ,@Index int = ?
    ,@Indexed_Company_Seg_Name nvarchar(100) = ?
    ,@Company_Sub_Segment nvarchar(100) = ?
    ,@Index_Sub_Segment int = ?
    ,@Indexed_Company_Sub_Seg_Name nvarchar(100) = ?
    ,@RangeStart nvarchar(100) = ?
    ,@RangeEnd nvarchar(100) = ?
    ,@IntervalStart bigint = ?
    ,@IntervalEnd bigint = ?
;

INSERT INTO [dbo].[tDimCompany_Segment] (
    [Company_Segment]
    ,[Index]
    ,[Indexed_Company_Seg_Name]
    ,[Company_Sub_Segment]
    ,[Index_Sub_Segment]
    ,[Indexed_Company_Sub_Seg_Name]
    ,[RangeStart]
    ,[RangeEnd]
    ,[IntervalStart]
    ,[IntervalEnd]
)
VALUES (
    @Company_Segment
    ,@Index
    ,@Indexed_Company_Seg_Name
    ,@Company_Sub_Segment
    ,@Index_Sub_Segment
    ,@Indexed_Company_Sub_Seg_Name
    ,@RangeStart
    ,@RangeEnd
    ,@IntervalStart
    ,@IntervalEnd
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Segment, Index, Indexed_Company_Seg_Name, Company_Sub_Segment, Index_Sub_Segment, Indexed_Company_Sub_Seg_Name, RangeStart, RangeEnd, IntervalStart, IntervalEnd ]).exec_df()

        def update(self, CompanySegmentKey: int, Company_Segment: str, Index: int, Indexed_Company_Seg_Name: str, Company_Sub_Segment: str, Index_Sub_Segment: int, Indexed_Company_Sub_Seg_Name: str, RangeStart: str = None, RangeEnd: str = None, IntervalStart: int = None, IntervalEnd: int = None) -> DataFrame:
            sql = """
DECLARE
    @CompanySegmentKey int = ?
    ,@Company_Segment nvarchar(100) = ?
    ,@Index int = ?
    ,@Indexed_Company_Seg_Name nvarchar(100) = ?
    ,@Company_Sub_Segment nvarchar(100) = ?
    ,@Index_Sub_Segment int = ?
    ,@Indexed_Company_Sub_Seg_Name nvarchar(100) = ?
    ,@RangeStart nvarchar(100) = ?
    ,@RangeEnd nvarchar(100) = ?
    ,@IntervalStart bigint = ?
    ,@IntervalEnd bigint = ?
;

UPDATE [dbo].[tDimCompany_Segment] SET 
    [Company_Segment] = @Company_Segment
    ,[Index] = @Index
    ,[Indexed_Company_Seg_Name] = @Indexed_Company_Seg_Name
    ,[Company_Sub_Segment] = @Company_Sub_Segment
    ,[Index_Sub_Segment] = @Index_Sub_Segment
    ,[Indexed_Company_Sub_Seg_Name] = @Indexed_Company_Sub_Seg_Name
    ,[RangeStart] = @RangeStart
    ,[RangeEnd] = @RangeEnd
    ,[IntervalStart] = @IntervalStart
    ,[IntervalEnd] = @IntervalEnd
 WHERE
    [CompanySegmentKey] = @CompanySegmentKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CompanySegmentKey, Company_Segment, Index, Indexed_Company_Seg_Name, Company_Sub_Segment, Index_Sub_Segment, Indexed_Company_Sub_Seg_Name, RangeStart, RangeEnd, IntervalStart, IntervalEnd ]).exec_df()

        def delete(self, CompanySegmentKey: int) -> DataFrame:
            sql = """
DECLARE
    @CompanySegmentKey int = ?
;

DELETE [dbo].[tDimCompany_Segment]
WHERE
    [CompanySegmentKey] = @CompanySegmentKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CompanySegmentKey ]).exec_df()

    class Dim_Tag_Role:
        # table
        TableName = 'Dim_Tag_Role'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Tag_Role]'
        # columns
        Tag_Role_ID = 'Tag_Role_ID'
        Tag_Role = 'Tag_Role'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Tag_Role: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Tag_Role nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Tag_Role] (
    [Tag_Role]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Tag_Role
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Tag_Role, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Tag_Role_ID: int, Tag_Role: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Tag_Role_ID bigint = ?
    ,@Tag_Role nvarchar(40) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Tag_Role] SET 
    [Tag_Role] = @Tag_Role
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Tag_Role_ID] = @Tag_Role_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Tag_Role_ID, Tag_Role, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Tag_Role_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Tag_Role_ID bigint = ?
;

DELETE [dbo].[Dim_Tag_Role]
WHERE
    [Tag_Role_ID] = @Tag_Role_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Tag_Role_ID ]).exec_df()

    class _04_Status_Data_Preperation:
        # table
        TableName = '04_Status_Data_Preperation'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_04_Status_Data_Preperation]'
        # columns
        _4_eye_check_done_ = '4-eye check done?'
        BU = 'BU'
        BuPa = 'BuPa'
        Business_Contact = 'Business Contact'
        Client_Name = 'Client_Name'
        Comment = 'Comment'
        Contact_Project_Team = 'Contact Project Team'
        Cyber_Expo_sure__Claims = 'Cyber Expo-sure/ Claims'
        Data_as_of = 'Data as of'
        Deadline = 'Deadline'
        First_Check_Claim__Expo_sure_Linkable = 'First Check Claim/ Expo-sure Linkable'
        FSRI_Treaty_Nr = 'FSRI Treaty Nr'
        ID = 'ID'
        Insured_Name_available = 'Insured Name available'
        Kind_of_data_delivery__in_force__from_to__quarterly__etc__ = 'Kind of data delivery (in-force, from-to, quarterly, etc.)'
        NDA_Status = 'NDA Status'
        Original_file_name = 'Original file name'
        PML_has_been_sent_ = 'PML has been sent?'
        PML_needs_to_be_done_ = 'PML needs to be done?'
        Policy_ID = 'Policy ID'
        Priority = 'Priority'
        Q_A_with_UW_done_ = 'Q&A with UW done?'
        R_Code_manipulation_done_ = 'R Code manipulation done?'
        Responsible_for_4_eye_check = 'Responsible for 4-eye check'
        Run_function_executed_ = 'Run function executed?'
        Smart_matching_done_ = 'Smart matching done?'
        Specify_reporting_threshhold = 'Specify reporting threshhold'
        SRAC_has_been_sent_ = 'SRAC has been sent?'
        SRAC_needs_to_be_done_ = 'SRAC needs to be done?'
        Tab_name = 'Tab name'
        UW_sign_off_done_ = 'UW sign-off done?'
        UW_Year = 'UW Year'
        UWPF_Nr = 'UWPF Nr'
        Validation_issues_resolved_ = 'Validation issues resolved?'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID: int, _4_eye_check_done_: str = None, BU: str = None, BuPa: int = None, Business_Contact: str = None, Client_Name: str = None, Comment: str = None, Contact_Project_Team: str = None, Cyber_Expo_sure__Claims: str = None, Data_as_of: date = None, Deadline: date = None, First_Check_Claim__Expo_sure_Linkable: str = None, FSRI_Treaty_Nr: int = None, Insured_Name_available: str = None, Kind_of_data_delivery__in_force__from_to__quarterly__etc__: str = None, NDA_Status: str = None, Original_file_name: str = None, PML_has_been_sent_: str = None, PML_needs_to_be_done_: str = None, Policy_ID: str = None, Priority: str = None, Q_A_with_UW_done_: str = None, R_Code_manipulation_done_: str = None, Responsible_for_4_eye_check: str = None, Run_function_executed_: str = None, Smart_matching_done_: str = None, Specify_reporting_threshhold: str = None, SRAC_has_been_sent_: str = None, SRAC_needs_to_be_done_: str = None, Tab_name: str = None, UW_sign_off_done_: str = None, UW_Year: int = None, UWPF_Nr: int = None, Validation_issues_resolved_: str = None) -> DataFrame:
            sql = """
DECLARE
    @_4_eye_check_done_ varchar(4) = ?
    ,@BU varchar(10) = ?
    ,@BuPa int = ?
    ,@Business_Contact varchar(50) = ?
    ,@Client_Name varchar(100) = ?
    ,@Comment varchar(200) = ?
    ,@Contact_Project_Team varchar(30) = ?
    ,@Cyber_Expo_sure__Claims varchar(20) = ?
    ,@Data_as_of date(0) = ?
    ,@Deadline date(0) = ?
    ,@First_Check_Claim__Expo_sure_Linkable varchar(50) = ?
    ,@FSRI_Treaty_Nr int = ?
    ,@ID smallint = ?
    ,@Insured_Name_available varchar(6) = ?
    ,@Kind_of_data_delivery__in_force__from_to__quarterly__etc__ varchar(120) = ?
    ,@NDA_Status varchar(30) = ?
    ,@Original_file_name varchar(330) = ?
    ,@PML_has_been_sent_ varchar(3) = ?
    ,@PML_needs_to_be_done_ varchar(3) = ?
    ,@Policy_ID varchar(30) = ?
    ,@Priority varchar(10) = ?
    ,@Q_A_with_UW_done_ varchar(4) = ?
    ,@R_Code_manipulation_done_ varchar(4) = ?
    ,@Responsible_for_4_eye_check varchar(30) = ?
    ,@Run_function_executed_ varchar(4) = ?
    ,@Smart_matching_done_ varchar(4) = ?
    ,@Specify_reporting_threshhold varchar(120) = ?
    ,@SRAC_has_been_sent_ varchar(3) = ?
    ,@SRAC_needs_to_be_done_ varchar(3) = ?
    ,@Tab_name varchar(80) = ?
    ,@UW_sign_off_done_ varchar(4) = ?
    ,@UW_Year int = ?
    ,@UWPF_Nr int = ?
    ,@Validation_issues_resolved_ varchar(4) = ?
;

INSERT INTO [dbo].[04_Status_Data_Preperation] (
    [4-eye check done?]
    ,[BU]
    ,[BuPa]
    ,[Business Contact]
    ,[Client_Name]
    ,[Comment]
    ,[Contact Project Team]
    ,[Cyber Expo-sure/ Claims]
    ,[Data as of]
    ,[Deadline]
    ,[First Check Claim/ Expo-sure Linkable]
    ,[FSRI Treaty Nr]
    ,[ID]
    ,[Insured Name available]
    ,[Kind of data delivery (in-force, from-to, quarterly, etc.)]
    ,[NDA Status]
    ,[Original file name]
    ,[PML has been sent?]
    ,[PML needs to be done?]
    ,[Policy ID]
    ,[Priority]
    ,[Q&A with UW done?]
    ,[R Code manipulation done?]
    ,[Responsible for 4-eye check]
    ,[Run function executed?]
    ,[Smart matching done?]
    ,[Specify reporting threshhold]
    ,[SRAC has been sent?]
    ,[SRAC needs to be done?]
    ,[Tab name]
    ,[UW sign-off done?]
    ,[UW Year]
    ,[UWPF Nr]
    ,[Validation issues resolved?]
)
VALUES (
    @_4_eye_check_done_
    ,@BU
    ,@BuPa
    ,@Business_Contact
    ,@Client_Name
    ,@Comment
    ,@Contact_Project_Team
    ,@Cyber_Expo_sure__Claims
    ,@Data_as_of
    ,@Deadline
    ,@First_Check_Claim__Expo_sure_Linkable
    ,@FSRI_Treaty_Nr
    ,@ID
    ,@Insured_Name_available
    ,@Kind_of_data_delivery__in_force__from_to__quarterly__etc__
    ,@NDA_Status
    ,@Original_file_name
    ,@PML_has_been_sent_
    ,@PML_needs_to_be_done_
    ,@Policy_ID
    ,@Priority
    ,@Q_A_with_UW_done_
    ,@R_Code_manipulation_done_
    ,@Responsible_for_4_eye_check
    ,@Run_function_executed_
    ,@Smart_matching_done_
    ,@Specify_reporting_threshhold
    ,@SRAC_has_been_sent_
    ,@SRAC_needs_to_be_done_
    ,@Tab_name
    ,@UW_sign_off_done_
    ,@UW_Year
    ,@UWPF_Nr
    ,@Validation_issues_resolved_
);
"""
            return DbCmd(self.cnOrStr, sql, [ _4_eye_check_done_, BU, BuPa, Business_Contact, Client_Name, Comment, Contact_Project_Team, Cyber_Expo_sure__Claims, Data_as_of, Deadline, First_Check_Claim__Expo_sure_Linkable, FSRI_Treaty_Nr, ID, Insured_Name_available, Kind_of_data_delivery__in_force__from_to__quarterly__etc__, NDA_Status, Original_file_name, PML_has_been_sent_, PML_needs_to_be_done_, Policy_ID, Priority, Q_A_with_UW_done_, R_Code_manipulation_done_, Responsible_for_4_eye_check, Run_function_executed_, Smart_matching_done_, Specify_reporting_threshhold, SRAC_has_been_sent_, SRAC_needs_to_be_done_, Tab_name, UW_sign_off_done_, UW_Year, UWPF_Nr, Validation_issues_resolved_ ]).exec_df()

        def update(self, ID: int, _4_eye_check_done_: str = None, BU: str = None, BuPa: int = None, Business_Contact: str = None, Client_Name: str = None, Comment: str = None, Contact_Project_Team: str = None, Cyber_Expo_sure__Claims: str = None, Data_as_of: date = None, Deadline: date = None, First_Check_Claim__Expo_sure_Linkable: str = None, FSRI_Treaty_Nr: int = None, Insured_Name_available: str = None, Kind_of_data_delivery__in_force__from_to__quarterly__etc__: str = None, NDA_Status: str = None, Original_file_name: str = None, PML_has_been_sent_: str = None, PML_needs_to_be_done_: str = None, Policy_ID: str = None, Priority: str = None, Q_A_with_UW_done_: str = None, R_Code_manipulation_done_: str = None, Responsible_for_4_eye_check: str = None, Run_function_executed_: str = None, Smart_matching_done_: str = None, Specify_reporting_threshhold: str = None, SRAC_has_been_sent_: str = None, SRAC_needs_to_be_done_: str = None, Tab_name: str = None, UW_sign_off_done_: str = None, UW_Year: int = None, UWPF_Nr: int = None, Validation_issues_resolved_: str = None) -> DataFrame:
            sql = """
DECLARE
    @_4_eye_check_done_ varchar(4) = ?
    ,@BU varchar(10) = ?
    ,@BuPa int = ?
    ,@Business_Contact varchar(50) = ?
    ,@Client_Name varchar(100) = ?
    ,@Comment varchar(200) = ?
    ,@Contact_Project_Team varchar(30) = ?
    ,@Cyber_Expo_sure__Claims varchar(20) = ?
    ,@Data_as_of date(0) = ?
    ,@Deadline date(0) = ?
    ,@First_Check_Claim__Expo_sure_Linkable varchar(50) = ?
    ,@FSRI_Treaty_Nr int = ?
    ,@ID smallint = ?
    ,@Insured_Name_available varchar(6) = ?
    ,@Kind_of_data_delivery__in_force__from_to__quarterly__etc__ varchar(120) = ?
    ,@NDA_Status varchar(30) = ?
    ,@Original_file_name varchar(330) = ?
    ,@PML_has_been_sent_ varchar(3) = ?
    ,@PML_needs_to_be_done_ varchar(3) = ?
    ,@Policy_ID varchar(30) = ?
    ,@Priority varchar(10) = ?
    ,@Q_A_with_UW_done_ varchar(4) = ?
    ,@R_Code_manipulation_done_ varchar(4) = ?
    ,@Responsible_for_4_eye_check varchar(30) = ?
    ,@Run_function_executed_ varchar(4) = ?
    ,@Smart_matching_done_ varchar(4) = ?
    ,@Specify_reporting_threshhold varchar(120) = ?
    ,@SRAC_has_been_sent_ varchar(3) = ?
    ,@SRAC_needs_to_be_done_ varchar(3) = ?
    ,@Tab_name varchar(80) = ?
    ,@UW_sign_off_done_ varchar(4) = ?
    ,@UW_Year int = ?
    ,@UWPF_Nr int = ?
    ,@Validation_issues_resolved_ varchar(4) = ?
;

UPDATE [dbo].[04_Status_Data_Preperation] SET 
    [4-eye check done?] = @_4_eye_check_done_
    ,[BU] = @BU
    ,[BuPa] = @BuPa
    ,[Business Contact] = @Business_Contact
    ,[Client_Name] = @Client_Name
    ,[Comment] = @Comment
    ,[Contact Project Team] = @Contact_Project_Team
    ,[Cyber Expo-sure/ Claims] = @Cyber_Expo_sure__Claims
    ,[Data as of] = @Data_as_of
    ,[Deadline] = @Deadline
    ,[First Check Claim/ Expo-sure Linkable] = @First_Check_Claim__Expo_sure_Linkable
    ,[FSRI Treaty Nr] = @FSRI_Treaty_Nr
    ,[Insured Name available] = @Insured_Name_available
    ,[Kind of data delivery (in-force, from-to, quarterly, etc.)] = @Kind_of_data_delivery__in_force__from_to__quarterly__etc__
    ,[NDA Status] = @NDA_Status
    ,[Original file name] = @Original_file_name
    ,[PML has been sent?] = @PML_has_been_sent_
    ,[PML needs to be done?] = @PML_needs_to_be_done_
    ,[Policy ID] = @Policy_ID
    ,[Priority] = @Priority
    ,[Q&A with UW done?] = @Q_A_with_UW_done_
    ,[R Code manipulation done?] = @R_Code_manipulation_done_
    ,[Responsible for 4-eye check] = @Responsible_for_4_eye_check
    ,[Run function executed?] = @Run_function_executed_
    ,[Smart matching done?] = @Smart_matching_done_
    ,[Specify reporting threshhold] = @Specify_reporting_threshhold
    ,[SRAC has been sent?] = @SRAC_has_been_sent_
    ,[SRAC needs to be done?] = @SRAC_needs_to_be_done_
    ,[Tab name] = @Tab_name
    ,[UW sign-off done?] = @UW_sign_off_done_
    ,[UW Year] = @UW_Year
    ,[UWPF Nr] = @UWPF_Nr
    ,[Validation issues resolved?] = @Validation_issues_resolved_
 WHERE
    [ID] = @ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ _4_eye_check_done_, BU, BuPa, Business_Contact, Client_Name, Comment, Contact_Project_Team, Cyber_Expo_sure__Claims, Data_as_of, Deadline, First_Check_Claim__Expo_sure_Linkable, FSRI_Treaty_Nr, ID, Insured_Name_available, Kind_of_data_delivery__in_force__from_to__quarterly__etc__, NDA_Status, Original_file_name, PML_has_been_sent_, PML_needs_to_be_done_, Policy_ID, Priority, Q_A_with_UW_done_, R_Code_manipulation_done_, Responsible_for_4_eye_check, Run_function_executed_, Smart_matching_done_, Specify_reporting_threshhold, SRAC_has_been_sent_, SRAC_needs_to_be_done_, Tab_name, UW_sign_off_done_, UW_Year, UWPF_Nr, Validation_issues_resolved_ ]).exec_df()

        def delete(self, ID: int) -> DataFrame:
            sql = """
DECLARE
    @ID smallint = ?
;

DELETE [dbo].[04_Status_Data_Preperation]
WHERE
    [ID] = @ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID ]).exec_df()

    class _03_Policy_Columns:
        # table
        TableName = '03_Policy_Columns'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_03_Policy_Columns]'
        # columns
        Column_Name = 'Column_Name'
        Column_Short = 'Column_Short'
        Column_Order = 'Column_Order'
        Order = 'Order'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Column_Name: str = None, Column_Short: str = None, Column_Order: str = None, Order: int = None) -> DataFrame:
            sql = """
DECLARE
    @Column_Name varchar(30) = ?
    ,@Column_Short varchar(15) = ?
    ,@Column_Order varchar(35) = ?
    ,@Order tinyint = ?
;

INSERT INTO [dbo].[03_Policy_Columns] (
    [Column_Name]
    ,[Column_Short]
    ,[Column_Order]
    ,[Order]
)
VALUES (
    @Column_Name
    ,@Column_Short
    ,@Column_Order
    ,@Order
);
"""
            return DbCmd(self.cnOrStr, sql, [ Column_Name, Column_Short, Column_Order, Order ]).exec_df()

        def update(self, Column_Name: str = None, Column_Short: str = None, Column_Order: str = None, Order: int = None) -> DataFrame:
            sql = """
DECLARE
    @Column_Name varchar(30) = ?
    ,@Column_Short varchar(15) = ?
    ,@Column_Order varchar(35) = ?
    ,@Order tinyint = ?
;

UPDATE [dbo].[03_Policy_Columns] SET 
    [Column_Name] = @Column_Name
    ,[Column_Short] = @Column_Short
    ,[Column_Order] = @Column_Order
    ,[Order] = @Order
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Column_Name, Column_Short, Column_Order, Order ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[03_Policy_Columns]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class tDimClaims_Cause_Quality:
        # table
        TableName = 'tDimClaims_Cause_Quality'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimClaims_Cause_Quality]'
        # columns
        QualityKey = 'QualityKey'
        Quality = 'Quality'
        Index = 'Index'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Quality: str, Index: int) -> DataFrame:
            sql = """
DECLARE
    @Quality nvarchar(100) = ?
    ,@Index int = ?
;

INSERT INTO [dbo].[tDimClaims_Cause_Quality] (
    [Quality]
    ,[Index]
)
VALUES (
    @Quality
    ,@Index
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Quality, Index ]).exec_df()

        def update(self, QualityKey: int, Quality: str, Index: int) -> DataFrame:
            sql = """
DECLARE
    @QualityKey int = ?
    ,@Quality nvarchar(100) = ?
    ,@Index int = ?
;

UPDATE [dbo].[tDimClaims_Cause_Quality] SET 
    [Quality] = @Quality
    ,[Index] = @Index
 WHERE
    [QualityKey] = @QualityKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ QualityKey, Quality, Index ]).exec_df()

        def delete(self, QualityKey: int) -> DataFrame:
            sql = """
DECLARE
    @QualityKey int = ?
;

DELETE [dbo].[tDimClaims_Cause_Quality]
WHERE
    [QualityKey] = @QualityKey
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ QualityKey ]).exec_df()

    class _06_Cambridge:
        # table
        TableName = '06_Cambridge'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_06_Cambridge]'
        # columns
        Cambridge_Code_Number = 'Cambridge_Code_Number'
        Cambridge_Code_Number_Agg = 'Cambridge_Code_Number_Agg'
        Cambridge_Full_Agg_Name = 'Cambridge_Full_Agg_Name'
        Cambridge_Full_Name = 'Cambridge_Full_Name'
        Cambridge_Name = 'Cambridge_Name'
        Cambridge_Name_Agg = 'Cambridge_Name_Agg'
        Cambridge_Rank = 'Cambridge_Rank'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Cambridge_Code_Number: float, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None) -> DataFrame:
            sql = """
DECLARE
    @Cambridge_Code_Number numeric(3, 1) = ?
    ,@Cambridge_Code_Number_Agg tinyint = ?
    ,@Cambridge_Full_Agg_Name varchar(60) = ?
    ,@Cambridge_Full_Name varchar(60) = ?
    ,@Cambridge_Name varchar(50) = ?
    ,@Cambridge_Name_Agg varchar(50) = ?
    ,@Cambridge_Rank tinyint = ?
;

INSERT INTO [dbo].[06_Cambridge] (
    [Cambridge_Code_Number]
    ,[Cambridge_Code_Number_Agg]
    ,[Cambridge_Full_Agg_Name]
    ,[Cambridge_Full_Name]
    ,[Cambridge_Name]
    ,[Cambridge_Name_Agg]
    ,[Cambridge_Rank]
)
VALUES (
    @Cambridge_Code_Number
    ,@Cambridge_Code_Number_Agg
    ,@Cambridge_Full_Agg_Name
    ,@Cambridge_Full_Name
    ,@Cambridge_Name
    ,@Cambridge_Name_Agg
    ,@Cambridge_Rank
);
"""
            return DbCmd(self.cnOrStr, sql, [ Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ]).exec_df()

        def update(self, Cambridge_Code_Number: float, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None) -> DataFrame:
            sql = """
DECLARE
    @Cambridge_Code_Number numeric(3, 1) = ?
    ,@Cambridge_Code_Number_Agg tinyint = ?
    ,@Cambridge_Full_Agg_Name varchar(60) = ?
    ,@Cambridge_Full_Name varchar(60) = ?
    ,@Cambridge_Name varchar(50) = ?
    ,@Cambridge_Name_Agg varchar(50) = ?
    ,@Cambridge_Rank tinyint = ?
;

UPDATE [dbo].[06_Cambridge] SET 
    [Cambridge_Code_Number_Agg] = @Cambridge_Code_Number_Agg
    ,[Cambridge_Full_Agg_Name] = @Cambridge_Full_Agg_Name
    ,[Cambridge_Full_Name] = @Cambridge_Full_Name
    ,[Cambridge_Name] = @Cambridge_Name
    ,[Cambridge_Name_Agg] = @Cambridge_Name_Agg
    ,[Cambridge_Rank] = @Cambridge_Rank
 WHERE
    [Cambridge_Code_Number] = @Cambridge_Code_Number
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ]).exec_df()

        def delete(self, Cambridge_Code_Number: float) -> DataFrame:
            sql = """
DECLARE
    @Cambridge_Code_Number numeric(3, 1) = ?
;

DELETE [dbo].[06_Cambridge]
WHERE
    [Cambridge_Code_Number] = @Cambridge_Code_Number
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Cambridge_Code_Number ]).exec_df()

    class test_bdx:
        # table
        TableName = 'test_bdx'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[test_bdx]'
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Product_Name_ClientInfo = 'Product_Name_ClientInfo'
        Coverage = 'Coverage'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'
        Client_GrossNet_Premium_Orig_Curr = 'Client_GrossNet_Premium_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Turnover_ClientInfo = 'Turnover_ClientInfo'
        Turnover_Year_ClientInfo = 'Turnover_Year_ClientInfo'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_No_of_Employees = 'Insured_No_of_Employees'
        PII_Records_Stored = 'PII_Records_Stored'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        Coverage_1_Sublimit_Data_Breach_1st = 'Coverage_1_Sublimit_Data_Breach_1st'
        Coverage_2_Sublimit_Data_Breach_privacy_event_3rd = 'Coverage_2_Sublimit_Data_Breach_privacy_event_3rd'
        Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd = 'Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd'
        Coverage_4_Sublimit_RestorationData = 'Coverage_4_Sublimit_RestorationData'
        Coverage_5_Sublimit_Reputation = 'Coverage_5_Sublimit_Reputation'
        Coverage_6_Sublimit_Business_Interruption = 'Coverage_6_Sublimit_Business_Interruption'
        Coverage_7_Sublimit_Contingent_Business_Interruption = 'Coverage_7_Sublimit_Contingent_Business_Interruption'
        Coverage_8_Sublimit_Extortion = 'Coverage_8_Sublimit_Extortion'
        Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud = 'Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud'
        Coverage_10_Sublimit_PCI_DSS = 'Coverage_10_Sublimit_PCI_DSS'
        Coverage_11_Sublimit_Network_Security = 'Coverage_11_Sublimit_Network_Security'
        Coverage_12_Sublimit_Media_Liability = 'Coverage_12_Sublimit_Media_Liability'
        Coverage_13_Sublimit_Tech_PI_E_and_O = 'Coverage_13_Sublimit_Tech_PI_E_and_O'
        Coverage_14_Sublimit_D_and_O = 'Coverage_14_Sublimit_D_and_O'
        folderId = 'folderId'
        folderName = 'folderName'
        folderPath = 'folderPath'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetInFileIdx = 'sheetInFileIdx'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Client_Premium_USD = 'Client_Premium_USD'
        Client_GrossNet_Premium_USD = 'Client_GrossNet_Premium_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        BvD_ID = 'BvD_ID'
        Risk_ID = 'Risk_ID'
        Tower_ID = 'Tower_ID'
        Duplicate_ID = 'Duplicate_ID'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Risk_ID nvarchar(100) = ?
    ,@Tower_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
;

INSERT INTO [dbo].[test_bdx] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Product_Name_ClientInfo]
    ,[Coverage]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Premium_Orig_Curr]
    ,[Client_GrossNet_Premium_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Turnover_ClientInfo]
    ,[Turnover_Year_ClientInfo]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_No_of_Employees]
    ,[PII_Records_Stored]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[Coverage_1_Sublimit_Data_Breach_1st]
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd]
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd]
    ,[Coverage_4_Sublimit_RestorationData]
    ,[Coverage_5_Sublimit_Reputation]
    ,[Coverage_6_Sublimit_Business_Interruption]
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption]
    ,[Coverage_8_Sublimit_Extortion]
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud]
    ,[Coverage_10_Sublimit_PCI_DSS]
    ,[Coverage_11_Sublimit_Network_Security]
    ,[Coverage_12_Sublimit_Media_Liability]
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O]
    ,[Coverage_14_Sublimit_D_and_O]
    ,[folderId]
    ,[folderName]
    ,[folderPath]
    ,[fileId]
    ,[fileName]
    ,[sheetInFileIdx]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[Turnover_ClientInfo_USD]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Client_Premium_USD]
    ,[Client_GrossNet_Premium_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[BvD_ID]
    ,[Risk_ID]
    ,[Tower_ID]
    ,[Duplicate_ID]
    ,[ID_Arbeitsvorrat_MR_share]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Product_Name_ClientInfo
    ,@Coverage
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Premium_Orig_Curr
    ,@Client_GrossNet_Premium_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Turnover_ClientInfo
    ,@Turnover_Year_ClientInfo
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_No_of_Employees
    ,@PII_Records_Stored
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@Coverage_1_Sublimit_Data_Breach_1st
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,@Coverage_4_Sublimit_RestorationData
    ,@Coverage_5_Sublimit_Reputation
    ,@Coverage_6_Sublimit_Business_Interruption
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption
    ,@Coverage_8_Sublimit_Extortion
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,@Coverage_10_Sublimit_PCI_DSS
    ,@Coverage_11_Sublimit_Network_Security
    ,@Coverage_12_Sublimit_Media_Liability
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O
    ,@Coverage_14_Sublimit_D_and_O
    ,@folderId
    ,@folderName
    ,@folderPath
    ,@fileId
    ,@fileName
    ,@sheetInFileIdx
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@Turnover_ClientInfo_USD
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Client_Premium_USD
    ,@Client_GrossNet_Premium_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@BvD_ID
    ,@Risk_ID
    ,@Tower_ID
    ,@Duplicate_ID
    ,@ID_Arbeitsvorrat_MR_share
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share ]).exec_df()

        def update(self, id: int, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Client nvarchar(400) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Policy_ID_ClientInfo nvarchar(1000) = ?
    ,@Policy_Inception_Date date(0) = ?
    ,@Policy_Expiry_Date date(0) = ?
    ,@Product_Name_ClientInfo nvarchar(1000) = ?
    ,@Coverage nvarchar(100) = ?
    ,@Currency nvarchar(100) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Premium_Orig_Curr float = ?
    ,@Client_GrossNet_Premium_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Turnover_ClientInfo float = ?
    ,@Turnover_Year_ClientInfo bigint = ?
    ,@Trade_Level_ClientInfo nvarchar(2048) = ?
    ,@Trade_Level_Code_Number_ClientInfo nvarchar(100) = ?
    ,@Trade_Level_Code_Standard_ClientInfo nvarchar(400) = ?
    ,@Insured_No_of_Employees bigint = ?
    ,@PII_Records_Stored bigint = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ClientInfo nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Coverage_1_Sublimit_Data_Breach_1st float = ?
    ,@Coverage_2_Sublimit_Data_Breach_privacy_event_3rd float = ?
    ,@Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd float = ?
    ,@Coverage_4_Sublimit_RestorationData float = ?
    ,@Coverage_5_Sublimit_Reputation float = ?
    ,@Coverage_6_Sublimit_Business_Interruption float = ?
    ,@Coverage_7_Sublimit_Contingent_Business_Interruption float = ?
    ,@Coverage_8_Sublimit_Extortion float = ?
    ,@Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud float = ?
    ,@Coverage_10_Sublimit_PCI_DSS float = ?
    ,@Coverage_11_Sublimit_Network_Security float = ?
    ,@Coverage_12_Sublimit_Media_Liability float = ?
    ,@Coverage_13_Sublimit_Tech_PI_E_and_O float = ?
    ,@Coverage_14_Sublimit_D_and_O float = ?
    ,@folderId bigint = ?
    ,@folderName nvarchar(1000) = ?
    ,@folderPath nvarchar(1000) = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetInFileIdx bigint = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Client_Premium_USD float = ?
    ,@Client_GrossNet_Premium_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@BvD_ID nvarchar(100) = ?
    ,@Risk_ID nvarchar(100) = ?
    ,@Tower_ID nvarchar(100) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@ID_Arbeitsvorrat_MR_share nvarchar(200) = ?
;

UPDATE [dbo].[test_bdx] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Product_Name_ClientInfo] = @Product_Name_ClientInfo
    ,[Coverage] = @Coverage
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Premium_Orig_Curr] = @Client_Premium_Orig_Curr
    ,[Client_GrossNet_Premium_Orig_Curr] = @Client_GrossNet_Premium_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Turnover_ClientInfo] = @Turnover_ClientInfo
    ,[Turnover_Year_ClientInfo] = @Turnover_Year_ClientInfo
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_No_of_Employees] = @Insured_No_of_Employees
    ,[PII_Records_Stored] = @PII_Records_Stored
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Coverage_1_Sublimit_Data_Breach_1st] = @Coverage_1_Sublimit_Data_Breach_1st
    ,[Coverage_2_Sublimit_Data_Breach_privacy_event_3rd] = @Coverage_2_Sublimit_Data_Breach_privacy_event_3rd
    ,[Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd] = @Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd
    ,[Coverage_4_Sublimit_RestorationData] = @Coverage_4_Sublimit_RestorationData
    ,[Coverage_5_Sublimit_Reputation] = @Coverage_5_Sublimit_Reputation
    ,[Coverage_6_Sublimit_Business_Interruption] = @Coverage_6_Sublimit_Business_Interruption
    ,[Coverage_7_Sublimit_Contingent_Business_Interruption] = @Coverage_7_Sublimit_Contingent_Business_Interruption
    ,[Coverage_8_Sublimit_Extortion] = @Coverage_8_Sublimit_Extortion
    ,[Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud] = @Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud
    ,[Coverage_10_Sublimit_PCI_DSS] = @Coverage_10_Sublimit_PCI_DSS
    ,[Coverage_11_Sublimit_Network_Security] = @Coverage_11_Sublimit_Network_Security
    ,[Coverage_12_Sublimit_Media_Liability] = @Coverage_12_Sublimit_Media_Liability
    ,[Coverage_13_Sublimit_Tech_PI_E_and_O] = @Coverage_13_Sublimit_Tech_PI_E_and_O
    ,[Coverage_14_Sublimit_D_and_O] = @Coverage_14_Sublimit_D_and_O
    ,[folderId] = @folderId
    ,[folderName] = @folderName
    ,[folderPath] = @folderPath
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetInFileIdx] = @sheetInFileIdx
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Client_Premium_USD] = @Client_Premium_USD
    ,[Client_GrossNet_Premium_USD] = @Client_GrossNet_Premium_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[BvD_ID] = @BvD_ID
    ,[Risk_ID] = @Risk_ID
    ,[Tower_ID] = @Tower_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[ID_Arbeitsvorrat_MR_share] = @ID_Arbeitsvorrat_MR_share
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[test_bdx]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class _10_Company_Segment:
        # table
        TableName = '10_Company_Segment'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_10_Company_Segment]'
        # columns
        Company_Segment_Name = 'Company_Segment_Name'
        Index = 'Index'
        Indexed_Company_Seg_Name = 'Indexed_Company_Seg_Name'
        Range_End = 'Range End'
        Range_Start = 'Range Start'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Company_Segment_Name: str, Index: int, Indexed_Company_Seg_Name: str = None, Range_End: str = None, Range_Start: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Segment_Name varchar(20) = ?
    ,@Index tinyint = ?
    ,@Indexed_Company_Seg_Name varchar(25) = ?
    ,@Range_End varchar(10) = ?
    ,@Range_Start varchar(10) = ?
;

INSERT INTO [dbo].[10_Company_Segment] (
    [Company_Segment_Name]
    ,[Index]
    ,[Indexed_Company_Seg_Name]
    ,[Range End]
    ,[Range Start]
)
VALUES (
    @Company_Segment_Name
    ,@Index
    ,@Indexed_Company_Seg_Name
    ,@Range_End
    ,@Range_Start
);
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Segment_Name, Index, Indexed_Company_Seg_Name, Range_End, Range_Start ]).exec_df()

        def update(self, Company_Segment_Name: str, Index: int, Indexed_Company_Seg_Name: str = None, Range_End: str = None, Range_Start: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_Segment_Name varchar(20) = ?
    ,@Index tinyint = ?
    ,@Indexed_Company_Seg_Name varchar(25) = ?
    ,@Range_End varchar(10) = ?
    ,@Range_Start varchar(10) = ?
;

UPDATE [dbo].[10_Company_Segment] SET 
    [Company_Segment_Name] = @Company_Segment_Name
    ,[Indexed_Company_Seg_Name] = @Indexed_Company_Seg_Name
    ,[Range End] = @Range_End
    ,[Range Start] = @Range_Start
 WHERE
    [Index] = @Index
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_Segment_Name, Index, Indexed_Company_Seg_Name, Range_End, Range_Start ]).exec_df()

        def delete(self, Index: int) -> DataFrame:
            sql = """
DECLARE
    @Index tinyint = ?
;

DELETE [dbo].[10_Company_Segment]
WHERE
    [Index] = @Index
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Index ]).exec_df()

    class unread_values:
        # table
        TableName = 'unread_values'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[unread_values]'
        # columns
        id = 'id'
        row = 'row'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Column = 'Column'
        Value = 'Value'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, row: int, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None) -> DataFrame:
            sql = """
DECLARE
    @row bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
;

INSERT INTO [dbo].[unread_values] (
    [row]
    ,[ID_Arbeitsvorrat]
    ,[Column]
    ,[Value]
)
VALUES (
    @row
    ,@ID_Arbeitsvorrat
    ,@Column
    ,@Value
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ row, ID_Arbeitsvorrat, Column, Value ]).exec_df()

        def update(self, id: int, row: int, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
    ,@row bigint = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@Column nvarchar(400) = ?
    ,@Value nvarchar(2048) = ?
;

UPDATE [dbo].[unread_values] SET 
    [row] = @row
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Column] = @Column
    ,[Value] = @Value
 WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id, row, ID_Arbeitsvorrat, Column, Value ]).exec_df()

        def delete(self, id: int) -> DataFrame:
            sql = """
DECLARE
    @id bigint = ?
;

DELETE [dbo].[unread_values]
WHERE
    [id] = @id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ id ]).exec_df()

    class tDimDate:
        # table
        TableName = 'tDimDate'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[tDimDate]'
        # columns
        DateKey = 'DateKey'
        Date = 'Date'
        Year = 'Year'
        Qtr = 'Qtr'
        Qtr_sort = 'Qtr_sort'
        Month = 'Month'
        Month_sort = 'Month_sort'
        MonthText_long = 'MonthText_long'
        MonthText_short = 'MonthText_short'
        Week = 'Week'
        ISO_Year = 'ISO Year'
        ISO_Week = 'ISO Week'
        Day_of_Week = 'Day_of_Week'
        Day_of_Week_Name = 'Day_of_Week_Name'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, DateKey: int, Date: date, Year: str = None, Qtr: str = None, Qtr_sort: int = None, Month: str = None, Month_sort: int = None, MonthText_long: str = None, MonthText_short: str = None, Week: str = None, ISO_Year: str = None, ISO_Week: str = None, Day_of_Week: int = None, Day_of_Week_Name: str = None) -> DataFrame:
            sql = """
DECLARE
    @DateKey int = ?
    ,@Date date(0) = ?
    ,@Year nvarchar(8) = ?
    ,@Qtr nvarchar(24) = ?
    ,@Qtr_sort int = ?
    ,@Month nvarchar(40) = ?
    ,@Month_sort int = ?
    ,@MonthText_long nvarchar(50) = ?
    ,@MonthText_short nvarchar(20) = ?
    ,@Week nvarchar(40) = ?
    ,@ISO_Year nvarchar(8) = ?
    ,@ISO_Week nvarchar(40) = ?
    ,@Day_of_Week int = ?
    ,@Day_of_Week_Name nvarchar(40) = ?
;

INSERT INTO [dbo].[tDimDate] (
    [DateKey]
    ,[Date]
    ,[Year]
    ,[Qtr]
    ,[Qtr_sort]
    ,[Month]
    ,[Month_sort]
    ,[MonthText_long]
    ,[MonthText_short]
    ,[Week]
    ,[ISO Year]
    ,[ISO Week]
    ,[Day_of_Week]
    ,[Day_of_Week_Name]
)
VALUES (
    @DateKey
    ,@Date
    ,@Year
    ,@Qtr
    ,@Qtr_sort
    ,@Month
    ,@Month_sort
    ,@MonthText_long
    ,@MonthText_short
    ,@Week
    ,@ISO_Year
    ,@ISO_Week
    ,@Day_of_Week
    ,@Day_of_Week_Name
);
"""
            return DbCmd(self.cnOrStr, sql, [ DateKey, Date, Year, Qtr, Qtr_sort, Month, Month_sort, MonthText_long, MonthText_short, Week, ISO_Year, ISO_Week, Day_of_Week, Day_of_Week_Name ]).exec_df()

        def update(self, DateKey: int, Date: date, Year: str = None, Qtr: str = None, Qtr_sort: int = None, Month: str = None, Month_sort: int = None, MonthText_long: str = None, MonthText_short: str = None, Week: str = None, ISO_Year: str = None, ISO_Week: str = None, Day_of_Week: int = None, Day_of_Week_Name: str = None) -> DataFrame:
            sql = """
DECLARE
    @DateKey int = ?
    ,@Date date(0) = ?
    ,@Year nvarchar(8) = ?
    ,@Qtr nvarchar(24) = ?
    ,@Qtr_sort int = ?
    ,@Month nvarchar(40) = ?
    ,@Month_sort int = ?
    ,@MonthText_long nvarchar(50) = ?
    ,@MonthText_short nvarchar(20) = ?
    ,@Week nvarchar(40) = ?
    ,@ISO_Year nvarchar(8) = ?
    ,@ISO_Week nvarchar(40) = ?
    ,@Day_of_Week int = ?
    ,@Day_of_Week_Name nvarchar(40) = ?
;

UPDATE [dbo].[tDimDate] SET 
    [DateKey] = @DateKey
    ,[Date] = @Date
    ,[Year] = @Year
    ,[Qtr] = @Qtr
    ,[Qtr_sort] = @Qtr_sort
    ,[Month] = @Month
    ,[Month_sort] = @Month_sort
    ,[MonthText_long] = @MonthText_long
    ,[MonthText_short] = @MonthText_short
    ,[Week] = @Week
    ,[ISO Year] = @ISO_Year
    ,[ISO Week] = @ISO_Week
    ,[Day_of_Week] = @Day_of_Week
    ,[Day_of_Week_Name] = @Day_of_Week_Name
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ DateKey, Date, Year, Qtr, Qtr_sort, Month, Month_sort, MonthText_long, MonthText_short, Week, ISO_Year, ISO_Week, Day_of_Week, Day_of_Week_Name ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[tDimDate]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Client_Name_Exceptions:
        # table
        TableName = 'Client_Name_Exceptions'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Client_Name_Exceptions]'
        # columns
        ProcessId = 'ProcessId'
        Business_Client_Name = 'Business_Client_Name'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ProcessId: int, Business_Client_Name: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ProcessId bigint = ?
    ,@Business_Client_Name nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Client_Name_Exceptions] (
    [ProcessId]
    ,[Business_Client_Name]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @ProcessId
    ,@Business_Client_Name
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ ProcessId, Business_Client_Name, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, ProcessId: int, Business_Client_Name: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @ProcessId bigint = ?
    ,@Business_Client_Name nvarchar(400) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Client_Name_Exceptions] SET 
    [Business_Client_Name] = @Business_Client_Name
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [ProcessId] = @ProcessId
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ProcessId, Business_Client_Name, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, ProcessId: int) -> DataFrame:
            sql = """
DECLARE
    @ProcessId bigint = ?
;

DELETE [dbo].[Client_Name_Exceptions]
WHERE
    [ProcessId] = @ProcessId
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ProcessId ]).exec_df()

    class Processes:
        # table
        TableName = 'Processes'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Processes]'
        # columns
        Id = 'Id'
        OperationFlag = 'OperationFlag'
        InsertionDate = 'InsertionDate'
        LastModifiedDate = 'LastModifiedDate'
        Deadline = 'Deadline'
        BusinessContact = 'BusinessContact'
        BusinessUnit = 'BusinessUnit'
        Priority = 'Priority'
        SmartMatchingContact = 'SmartMatchingContact'
        ProjectTeamContact = 'ProjectTeamContact'
        FourEyeCheckContact = 'FourEyeCheckContact'
        Comment = 'Comment'
        SignOffToken = 'SignOffToken'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        CreatedBy = 'CreatedBy'
        LastModifiedBy = 'LastModifiedBy'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Id: int, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, ID_Arbeitsvorrat: str, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None, CreatedBy: str = None, LastModifiedBy: str = None) -> DataFrame:
            sql = """
DECLARE
    @Id bigint = ?
    ,@OperationFlag int = ?
    ,@InsertionDate datetime2(7) = ?
    ,@LastModifiedDate datetime2(7) = ?
    ,@Deadline datetime2(7) = ?
    ,@BusinessContact nvarchar(1000) = ?
    ,@BusinessUnit int = ?
    ,@Priority int = ?
    ,@SmartMatchingContact nvarchar(1000) = ?
    ,@ProjectTeamContact nvarchar(1000) = ?
    ,@FourEyeCheckContact nvarchar(1000) = ?
    ,@Comment nvarchar(8000) = ?
    ,@SignOffToken nvarchar(8000) = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@CreatedBy nvarchar(1000) = ?
    ,@LastModifiedBy nvarchar(1000) = ?
;

INSERT INTO [dbo].[Processes] (
    [Id]
    ,[OperationFlag]
    ,[InsertionDate]
    ,[LastModifiedDate]
    ,[Deadline]
    ,[BusinessContact]
    ,[BusinessUnit]
    ,[Priority]
    ,[SmartMatchingContact]
    ,[ProjectTeamContact]
    ,[FourEyeCheckContact]
    ,[Comment]
    ,[SignOffToken]
    ,[ID_Arbeitsvorrat]
    ,[CreatedBy]
    ,[LastModifiedBy]
)
VALUES (
    @Id
    ,@OperationFlag
    ,@InsertionDate
    ,@LastModifiedDate
    ,@Deadline
    ,@BusinessContact
    ,@BusinessUnit
    ,@Priority
    ,@SmartMatchingContact
    ,@ProjectTeamContact
    ,@FourEyeCheckContact
    ,@Comment
    ,@SignOffToken
    ,@ID_Arbeitsvorrat
    ,@CreatedBy
    ,@LastModifiedBy
);
"""
            return DbCmd(self.cnOrStr, sql, [ Id, OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken, ID_Arbeitsvorrat, CreatedBy, LastModifiedBy ]).exec_df()

        def update(self, Id: int, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, ID_Arbeitsvorrat: str, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None, CreatedBy: str = None, LastModifiedBy: str = None) -> DataFrame:
            sql = """
DECLARE
    @Id bigint = ?
    ,@OperationFlag int = ?
    ,@InsertionDate datetime2(7) = ?
    ,@LastModifiedDate datetime2(7) = ?
    ,@Deadline datetime2(7) = ?
    ,@BusinessContact nvarchar(1000) = ?
    ,@BusinessUnit int = ?
    ,@Priority int = ?
    ,@SmartMatchingContact nvarchar(1000) = ?
    ,@ProjectTeamContact nvarchar(1000) = ?
    ,@FourEyeCheckContact nvarchar(1000) = ?
    ,@Comment nvarchar(8000) = ?
    ,@SignOffToken nvarchar(8000) = ?
    ,@ID_Arbeitsvorrat nvarchar(100) = ?
    ,@CreatedBy nvarchar(1000) = ?
    ,@LastModifiedBy nvarchar(1000) = ?
;

UPDATE [dbo].[Processes] SET 
    [Id] = @Id
    ,[OperationFlag] = @OperationFlag
    ,[InsertionDate] = @InsertionDate
    ,[LastModifiedDate] = @LastModifiedDate
    ,[Deadline] = @Deadline
    ,[BusinessContact] = @BusinessContact
    ,[BusinessUnit] = @BusinessUnit
    ,[Priority] = @Priority
    ,[SmartMatchingContact] = @SmartMatchingContact
    ,[ProjectTeamContact] = @ProjectTeamContact
    ,[FourEyeCheckContact] = @FourEyeCheckContact
    ,[Comment] = @Comment
    ,[SignOffToken] = @SignOffToken
    ,[ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[CreatedBy] = @CreatedBy
    ,[LastModifiedBy] = @LastModifiedBy
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Id, OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken, ID_Arbeitsvorrat, CreatedBy, LastModifiedBy ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[Processes]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class claims_bdx_temporary_jb:
        # table
        TableName = 'claims_bdx_temporary_jb'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[claims_bdx_temporary_jb]'
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Client = 'Client'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Policy_ID_ClientInfo = 'Policy_ID_ClientInfo'
        Policy_Inception_Date = 'Policy_Inception_Date'
        Policy_Expiry_Date = 'Policy_Expiry_Date'
        Currency = 'Currency'
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Share_of_Limit = 'Client_Share_of_Limit'
        SIR_Orig_Curr = 'SIR_Orig_Curr'
        BI_Waiting_Period_in_Hours = 'BI_Waiting_Period_in_Hours'
        Trade_Level_ClientInfo = 'Trade_Level_ClientInfo'
        Trade_Level_Code_Number_ClientInfo = 'Trade_Level_Code_Number_ClientInfo'
        Trade_Level_Code_Standard_ClientInfo = 'Trade_Level_Code_Standard_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ClientInfo = 'Insured_Country_ClientInfo'
        Insured_Homepage = 'Insured_Homepage'
        FSRI_Treaty = 'FSRI_Treaty'
        FSRI_Section = 'FSRI_Section'
        FSRI_Period = 'FSRI_Period'
        Claim_ID_ClientInfo = 'Claim_ID_ClientInfo'
        Is_Claim_Closed = 'Is_Claim_Closed'
        Date_of_Incident = 'Date_of_Incident'
        Date_of_Notification = 'Date_of_Notification'
        Claims_Description = 'Claims_Description'
        Type_of_Loss = 'Type_of_Loss'
        Country_of_Loss_Settlement = 'Country_of_Loss_Settlement'
        Nr_Affected_Records = 'Nr_Affected_Records'
        Duration_of_Outage_hours = 'Duration_of_Outage_hours'
        Value_as_Of_Date = 'Value_as_Of_Date'
        Loss_Currency = 'Loss_Currency'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        Threshold_Orig_Curr_unind = 'Threshold_Orig_Curr_unind'
        Observation_Period_Start = 'Observation_Period_Start'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'
        IsCensored = 'IsCensored'
        Client_Limit_USD = 'Client_Limit_USD'
        Full_Limit_USD = 'Full_Limit_USD'
        Attachment_USD = 'Attachment_USD'
        SIR_USD = 'SIR_USD'
        Incurred_Insured_FGU_USD = 'Incurred_Insured_FGU_USD'
        Paid_Client_Share_USD = 'Paid_Client_Share_USD'
        Incurred_Client_Share_USD = 'Incurred_Client_Share_USD'
        Threshold_unind_USD = 'Threshold_unind_USD'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Country_of_Loss_Settlement_ISO2 = 'Country_of_Loss_Settlement_ISO2'
        BvD_ID = 'BvD_ID'
        Duplicate_ID = 'Duplicate_ID'
        Internal_Claim_ID = 'Internal_Claim_ID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: str = None, Policy_Expiry_Date: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: str = None, Date_of_Notification: str = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: str = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: str = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: str = None, fileId: str = None, fileName: str = None, sheetName: str = None, rowNr: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date varchar(255) = ?
    ,@Policy_Expiry_Date varchar(255) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Insured_Homepage varchar(255) = ?
    ,@FSRI_Treaty varchar(255) = ?
    ,@FSRI_Section varchar(255) = ?
    ,@FSRI_Period varchar(255) = ?
    ,@Claim_ID_ClientInfo varchar(255) = ?
    ,@Is_Claim_Closed varchar(255) = ?
    ,@Date_of_Incident varchar(255) = ?
    ,@Date_of_Notification varchar(255) = ?
    ,@Claims_Description varchar(1151) = ?
    ,@Type_of_Loss varchar(255) = ?
    ,@Country_of_Loss_Settlement varchar(255) = ?
    ,@Nr_Affected_Records varchar(255) = ?
    ,@Duration_of_Outage_hours float = ?
    ,@Value_as_Of_Date varchar(255) = ?
    ,@Loss_Currency varchar(255) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@Observation_Period_Start varchar(255) = ?
    ,@fileId varchar(255) = ?
    ,@fileName varchar(255) = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr varchar(255) = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By varchar(255) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge varchar(255) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge varchar(255) = ?
    ,@Insured_Country_ISO2 varchar(255) = ?
    ,@Country_of_Loss_Settlement_ISO2 varchar(255) = ?
    ,@BvD_ID varchar(255) = ?
    ,@Duplicate_ID varchar(255) = ?
    ,@Internal_Claim_ID varchar(255) = ?
;

INSERT INTO [dbo].[claims_bdx_temporary_jb] (
    [ID_Arbeitsvorrat]
    ,[Client]
    ,[Insured_Name_ClientInfo]
    ,[Policy_ID_ClientInfo]
    ,[Policy_Inception_Date]
    ,[Policy_Expiry_Date]
    ,[Currency]
    ,[Client_Limit_Orig_Curr]
    ,[Full_Limit_Orig_Curr]
    ,[Attachment_Orig_Curr]
    ,[Client_Share_of_Limit]
    ,[SIR_Orig_Curr]
    ,[BI_Waiting_Period_in_Hours]
    ,[Trade_Level_ClientInfo]
    ,[Trade_Level_Code_Number_ClientInfo]
    ,[Trade_Level_Code_Standard_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ClientInfo]
    ,[Insured_Homepage]
    ,[FSRI_Treaty]
    ,[FSRI_Section]
    ,[FSRI_Period]
    ,[Claim_ID_ClientInfo]
    ,[Is_Claim_Closed]
    ,[Date_of_Incident]
    ,[Date_of_Notification]
    ,[Claims_Description]
    ,[Type_of_Loss]
    ,[Country_of_Loss_Settlement]
    ,[Nr_Affected_Records]
    ,[Duration_of_Outage_hours]
    ,[Value_as_Of_Date]
    ,[Loss_Currency]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[Threshold_Orig_Curr_unind]
    ,[Observation_Period_Start]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
    ,[IsCensored]
    ,[Client_Limit_USD]
    ,[Full_Limit_USD]
    ,[Attachment_USD]
    ,[SIR_USD]
    ,[Incurred_Insured_FGU_USD]
    ,[Paid_Client_Share_USD]
    ,[Incurred_Client_Share_USD]
    ,[Threshold_unind_USD]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Country_ISO2]
    ,[Country_of_Loss_Settlement_ISO2]
    ,[BvD_ID]
    ,[Duplicate_ID]
    ,[Internal_Claim_ID]
)
VALUES (
    @ID_Arbeitsvorrat
    ,@Client
    ,@Insured_Name_ClientInfo
    ,@Policy_ID_ClientInfo
    ,@Policy_Inception_Date
    ,@Policy_Expiry_Date
    ,@Currency
    ,@Client_Limit_Orig_Curr
    ,@Full_Limit_Orig_Curr
    ,@Attachment_Orig_Curr
    ,@Client_Share_of_Limit
    ,@SIR_Orig_Curr
    ,@BI_Waiting_Period_in_Hours
    ,@Trade_Level_ClientInfo
    ,@Trade_Level_Code_Number_ClientInfo
    ,@Trade_Level_Code_Standard_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ClientInfo
    ,@Insured_Homepage
    ,@FSRI_Treaty
    ,@FSRI_Section
    ,@FSRI_Period
    ,@Claim_ID_ClientInfo
    ,@Is_Claim_Closed
    ,@Date_of_Incident
    ,@Date_of_Notification
    ,@Claims_Description
    ,@Type_of_Loss
    ,@Country_of_Loss_Settlement
    ,@Nr_Affected_Records
    ,@Duration_of_Outage_hours
    ,@Value_as_Of_Date
    ,@Loss_Currency
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@Threshold_Orig_Curr_unind
    ,@Observation_Period_Start
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
    ,@IsCensored
    ,@Client_Limit_USD
    ,@Full_Limit_USD
    ,@Attachment_USD
    ,@SIR_USD
    ,@Incurred_Insured_FGU_USD
    ,@Paid_Client_Share_USD
    ,@Incurred_Client_Share_USD
    ,@Threshold_unind_USD
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Country_ISO2
    ,@Country_of_Loss_Settlement_ISO2
    ,@BvD_ID
    ,@Duplicate_ID
    ,@Internal_Claim_ID
);
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID ]).exec_df()

        def update(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: str = None, Policy_Expiry_Date: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: str = None, Date_of_Notification: str = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: str = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: str = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: str = None, fileId: str = None, fileName: str = None, sheetName: str = None, rowNr: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: str = None) -> DataFrame:
            sql = """
DECLARE
    @ID_Arbeitsvorrat varchar(255) = ?
    ,@Client varchar(255) = ?
    ,@Insured_Name_ClientInfo varchar(255) = ?
    ,@Policy_ID_ClientInfo varchar(255) = ?
    ,@Policy_Inception_Date varchar(255) = ?
    ,@Policy_Expiry_Date varchar(255) = ?
    ,@Currency varchar(255) = ?
    ,@Client_Limit_Orig_Curr float = ?
    ,@Full_Limit_Orig_Curr float = ?
    ,@Attachment_Orig_Curr float = ?
    ,@Client_Share_of_Limit float = ?
    ,@SIR_Orig_Curr float = ?
    ,@BI_Waiting_Period_in_Hours float = ?
    ,@Trade_Level_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Number_ClientInfo varchar(255) = ?
    ,@Trade_Level_Code_Standard_ClientInfo varchar(255) = ?
    ,@Insured_Street varchar(255) = ?
    ,@Insured_City varchar(255) = ?
    ,@Insured_ZIP_Code varchar(255) = ?
    ,@Insured_State varchar(255) = ?
    ,@Insured_Country_ClientInfo varchar(255) = ?
    ,@Insured_Homepage varchar(255) = ?
    ,@FSRI_Treaty varchar(255) = ?
    ,@FSRI_Section varchar(255) = ?
    ,@FSRI_Period varchar(255) = ?
    ,@Claim_ID_ClientInfo varchar(255) = ?
    ,@Is_Claim_Closed varchar(255) = ?
    ,@Date_of_Incident varchar(255) = ?
    ,@Date_of_Notification varchar(255) = ?
    ,@Claims_Description varchar(1151) = ?
    ,@Type_of_Loss varchar(255) = ?
    ,@Country_of_Loss_Settlement varchar(255) = ?
    ,@Nr_Affected_Records varchar(255) = ?
    ,@Duration_of_Outage_hours float = ?
    ,@Value_as_Of_Date varchar(255) = ?
    ,@Loss_Currency varchar(255) = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@Threshold_Orig_Curr_unind float = ?
    ,@Observation_Period_Start varchar(255) = ?
    ,@fileId varchar(255) = ?
    ,@fileName varchar(255) = ?
    ,@sheetName varchar(255) = ?
    ,@rowNr varchar(255) = ?
    ,@DELETE_indicator varchar(255) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By varchar(255) = ?
    ,@IsCensored bit = ?
    ,@Client_Limit_USD float = ?
    ,@Full_Limit_USD float = ?
    ,@Attachment_USD float = ?
    ,@SIR_USD float = ?
    ,@Incurred_Insured_FGU_USD float = ?
    ,@Paid_Client_Share_USD float = ?
    ,@Incurred_Client_Share_USD float = ?
    ,@Threshold_unind_USD float = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge varchar(255) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge varchar(255) = ?
    ,@Insured_Country_ISO2 varchar(255) = ?
    ,@Country_of_Loss_Settlement_ISO2 varchar(255) = ?
    ,@BvD_ID varchar(255) = ?
    ,@Duplicate_ID varchar(255) = ?
    ,@Internal_Claim_ID varchar(255) = ?
;

UPDATE [dbo].[claims_bdx_temporary_jb] SET 
    [ID_Arbeitsvorrat] = @ID_Arbeitsvorrat
    ,[Client] = @Client
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Policy_ID_ClientInfo] = @Policy_ID_ClientInfo
    ,[Policy_Inception_Date] = @Policy_Inception_Date
    ,[Policy_Expiry_Date] = @Policy_Expiry_Date
    ,[Currency] = @Currency
    ,[Client_Limit_Orig_Curr] = @Client_Limit_Orig_Curr
    ,[Full_Limit_Orig_Curr] = @Full_Limit_Orig_Curr
    ,[Attachment_Orig_Curr] = @Attachment_Orig_Curr
    ,[Client_Share_of_Limit] = @Client_Share_of_Limit
    ,[SIR_Orig_Curr] = @SIR_Orig_Curr
    ,[BI_Waiting_Period_in_Hours] = @BI_Waiting_Period_in_Hours
    ,[Trade_Level_ClientInfo] = @Trade_Level_ClientInfo
    ,[Trade_Level_Code_Number_ClientInfo] = @Trade_Level_Code_Number_ClientInfo
    ,[Trade_Level_Code_Standard_ClientInfo] = @Trade_Level_Code_Standard_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ClientInfo] = @Insured_Country_ClientInfo
    ,[Insured_Homepage] = @Insured_Homepage
    ,[FSRI_Treaty] = @FSRI_Treaty
    ,[FSRI_Section] = @FSRI_Section
    ,[FSRI_Period] = @FSRI_Period
    ,[Claim_ID_ClientInfo] = @Claim_ID_ClientInfo
    ,[Is_Claim_Closed] = @Is_Claim_Closed
    ,[Date_of_Incident] = @Date_of_Incident
    ,[Date_of_Notification] = @Date_of_Notification
    ,[Claims_Description] = @Claims_Description
    ,[Type_of_Loss] = @Type_of_Loss
    ,[Country_of_Loss_Settlement] = @Country_of_Loss_Settlement
    ,[Nr_Affected_Records] = @Nr_Affected_Records
    ,[Duration_of_Outage_hours] = @Duration_of_Outage_hours
    ,[Value_as_Of_Date] = @Value_as_Of_Date
    ,[Loss_Currency] = @Loss_Currency
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[Threshold_Orig_Curr_unind] = @Threshold_Orig_Curr_unind
    ,[Observation_Period_Start] = @Observation_Period_Start
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
    ,[IsCensored] = @IsCensored
    ,[Client_Limit_USD] = @Client_Limit_USD
    ,[Full_Limit_USD] = @Full_Limit_USD
    ,[Attachment_USD] = @Attachment_USD
    ,[SIR_USD] = @SIR_USD
    ,[Incurred_Insured_FGU_USD] = @Incurred_Insured_FGU_USD
    ,[Paid_Client_Share_USD] = @Paid_Client_Share_USD
    ,[Incurred_Client_Share_USD] = @Incurred_Client_Share_USD
    ,[Threshold_unind_USD] = @Threshold_unind_USD
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Country_of_Loss_Settlement_ISO2] = @Country_of_Loss_Settlement_ISO2
    ,[BvD_ID] = @BvD_ID
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Internal_Claim_ID] = @Internal_Claim_ID
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[claims_bdx_temporary_jb]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class pgo_data:
        # table
        TableName = 'pgo_data'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[pgo_data]'
        # columns
        PROGRAM_SUBSYSTEM_TREATY_ID = 'PROGRAM_SUBSYSTEM_TREATY_ID'
        PRICING_TOOL = 'PRICING_TOOL'
        DWH_RI_CONTRACT_PERIOD_ID = 'DWH_RI_CONTRACT_PERIOD_ID'
        PROGRAM_ID_OR_RIPP_ID = 'PROGRAM_ID_OR_RIPP_ID'
        ADMIN_SYSTEM_RPP_ID = 'ADMIN_SYSTEM_RPP_ID'
        ISPROPERTYBUYBACK = 'ISPROPERTYBUYBACK'
        PROGRAM_NAME = 'PROGRAM_NAME'
        DWH_RI_COVERAGE_ID = 'DWH_RI_COVERAGE_ID'
        TREATY_ID_OR_COVERAGE_ID = 'TREATY_ID_OR_COVERAGE_ID'
        TREATY_NAME = 'TREATY_NAME'
        FSRI_LINKAGES_FOR_TTY_BUSINESS = 'FSRI_LINKAGES_FOR_TTY_BUSINESS'
        FMT_ID_FOR_FAB_BUSINESS = 'FMT_ID_FOR_FAB_BUSINESS'
        CLIENT_ID = 'CLIENT_ID'
        CLIENT_NAME = 'CLIENT_NAME'
        BROKER_ID = 'BROKER_ID'
        BROKER_NAME = 'BROKER_NAME'
        INSURED_ID = 'INSURED_ID'
        INSURED_NAME = 'INSURED_NAME'
        RESULT_RESPONSIBILITY_ID = 'RESULT_RESPONSIBILITY_ID'
        RESULT_RESPONSIBILITY_NAME = 'RESULT_RESPONSIBILITY_NAME'
        RESULT_RESPONSIBILITY_LVL_3 = 'RESULT_RESPONSIBILITY_LVL_3'
        RESULT_RESPONSIBILITY_LVL_4 = 'RESULT_RESPONSIBILITY_LVL_4'
        RESULT_RESPONSIBILITY_LVL_5 = 'RESULT_RESPONSIBILITY_LVL_5'
        RESULT_RESPONSIBILITY_LVL_6 = 'RESULT_RESPONSIBILITY_LVL_6'
        RESULT_RESP_PRESENTATION = 'RESULT_RESP_PRESENTATION'
        UW_YEAR = 'UW_YEAR'
        BEGIN_DATE = 'BEGIN_DATE'
        END_DATE = 'END_DATE'
        UW_STATUS = 'UW_STATUS'
        NATURE_OF_TREATY = 'NATURE_OF_TREATY'
        FAC_OR_TREATY = 'FAC_OR_TREATY'
        VIRTUAL_TREATY_FLAG = 'VIRTUAL_TREATY_FLAG'
        MR_SHARE = 'MR_SHARE'
        PROTECTED_SHARE = 'PROTECTED_SHARE'
        TOTAL_CYBER_COB_SHARE = 'TOTAL_CYBER_COB_SHARE'
        ORIG_CURRENCY_ID = 'ORIG_CURRENCY_ID'
        EXCHANGE_RATE = 'EXCHANGE_RATE'
        LIMIT_100_PCT = 'LIMIT_100_PCT'
        ATTACHMENT_POINT_100_PCT = 'ATTACHMENT_POINT_100_PCT'
        EXP_TOTAL_PREMIUM_100_PCT = 'EXP_TOTAL_PREMIUM_100_PCT'
        EXP_TOTAL_DEDUCTIONS_100_PCT = 'EXP_TOTAL_DEDUCTIONS_100_PCT'
        EXP_ULTIMATE_LOSS_100_PCT = 'EXP_ULTIMATE_LOSS_100_PCT'
        EXP_TOTAL_PREMIUM_100P_CYB = 'EXP_TOTAL_PREMIUM_100P_CYB'
        EXP_TOTAL_DEDUCTIONS_100P_CYB = 'EXP_TOTAL_DEDUCTIONS_100P_CYB'
        EXP_ULT_LOSS_100P_CYB = 'EXP_ULT_LOSS_100P_CYB'
        EXP_TOTAL_PREMIUM_MR_SHARE = 'EXP_TOTAL_PREMIUM_MR_SHARE'
        EXP_TOTAL_DEDUCTIONS_MR_SHARE = 'EXP_TOTAL_DEDUCTIONS_MR_SHARE'
        EXP_ULT_LOSS_MR_SHARE = 'EXP_ULT_LOSS_MR_SHARE'
        EXP_TOTAL_PREMIUM_MR_SHARE_CYB = 'EXP_TOTAL_PREMIUM_MR_SHARE_CYB'
        EXP_TOTAL_DEDUCTS_MR_SHARE_CYB = 'EXP_TOTAL_DEDUCTS_MR_SHARE_CYB'
        EXP_ULT_LOSS_MR_SHARE_CYB = 'EXP_ULT_LOSS_MR_SHARE_CYB'
        ULTIMATE_LOSS_RATIO = 'ULTIMATE_LOSS_RATIO'
        TECHNICAL_COMBINED_RATIO = 'TECHNICAL_COMBINED_RATIO'
        COMBINED_RATIO = 'COMBINED_RATIO'
        EXP_PV_UW_RESLT_MR_SHARE = 'EXP_PV_UW_RESLT_MR_SHARE'
        RORAC = 'RORAC'
        IDENTITY_THEFT_SHARE = 'IDENTITY_THEFT_SHARE'
        DATA_COMPROMISE_SHARE = 'DATA_COMPROMISE_SHARE'
        CYBER_THIRD_PARTY_SHARE = 'CYBER_THIRD_PARTY_SHARE'
        CYBER_THIRD_PARTY_FI_SHARE = 'CYBER_THIRD_PARTY_FI_SHARE'
        CYBER_THIRD_PARTY_COMM_SHARE = 'CYBER_THIRD_PARTY_COMM_SHARE'
        CYBER_FIRST_PARTY_SHARE = 'CYBER_FIRST_PARTY_SHARE'
        CYBER_FIRST_PARTY_PERS_SHARE = 'CYBER_FIRST_PARTY_PERS_SHARE'
        CYBER_INDUCED_PROP_DMG_SHARE = 'CYBER_INDUCED_PROP_DMG_SHARE'
        CYBER_MULTIPERIL_TPL_SHARE = 'CYBER_MULTIPERIL_TPL_SHARE'
        MARINE_CYBER_THIRD_PARTY_SHARE = 'MARINE_CYBER_THIRD_PARTY_SHARE'
        MARINE_CYBER_FIRST_PARTY_SHARE = 'MARINE_CYBER_FIRST_PARTY_SHARE'
        PRICING_BUDGET_ITV = 'PRICING_BUDGET_ITV'
        PRICING_BUDGET_DB = 'PRICING_BUDGET_DB'
        PRICING_BUDGET_IF = 'PRICING_BUDGET_IF'
        PRICING_BUDGET_CLOUD = 'PRICING_BUDGET_CLOUD'
        TOTAL_PREMIUM_ASAT_100_PCT = 'TOTAL_PREMIUM_ASAT_100_PCT'
        TOTAL_DEDUCTIONS_ASAT_100_PCT = 'TOTAL_DEDUCTIONS_ASAT_100_PCT'
        ULT_LOSS_ASAT_100_PCT = 'ULT_LOSS_ASAT_100_PCT'
        TOTAL_PREMIUM_ASAT_100P_CYB = 'TOTAL_PREMIUM_ASAT_100P_CYB'
        TOTAL_DEDUCTIONS_ASAT_100P_CYB = 'TOTAL_DEDUCTIONS_ASAT_100P_CYB'
        ULT_LOSS_ASAT_100P_CYB = 'ULT_LOSS_ASAT_100P_CYB'
        TOTAL_PREMIUM_ASAT_MR_SHARE = 'TOTAL_PREMIUM_ASAT_MR_SHARE'
        TOTAL_DEDUCTIONS_ASAT_MR_SHARE = 'TOTAL_DEDUCTIONS_ASAT_MR_SHARE'
        ULT_LOSS_ASAT_MR_SHARE = 'ULT_LOSS_ASAT_MR_SHARE'
        TOTAL_PREM_ASAT_MR_SHARE_CYB = 'TOTAL_PREM_ASAT_MR_SHARE_CYB'
        TOTAL_DEDUCT_ASAT_MR_SHARE_CYB = 'TOTAL_DEDUCT_ASAT_MR_SHARE_CYB'
        ULT_LOSS_ASAT_MR_SHARE_CYB = 'ULT_LOSS_ASAT_MR_SHARE_CYB'
        TOTAL_PREMIUM_PROJ_100_PCT = 'TOTAL_PREMIUM_PROJ_100_PCT'
        TOTAL_DEDUCTIONS_PROJ_100_PCT = 'TOTAL_DEDUCTIONS_PROJ_100_PCT'
        ULT_LOSS_PROJ_100_PCT = 'ULT_LOSS_PROJ_100_PCT'
        TOTAL_PREMIUM_PROJ_100P_CYB = 'TOTAL_PREMIUM_PROJ_100P_CYB'
        TOTAL_DEDUCTIONS_PROJ_100P_CYB = 'TOTAL_DEDUCTIONS_PROJ_100P_CYB'
        ULT_LOSS_PROJ_100P_CYB = 'ULT_LOSS_PROJ_100P_CYB'
        TOTAL_PREMIUM_PROJ_MR_SHARE = 'TOTAL_PREMIUM_PROJ_MR_SHARE'
        TOTAL_DEDUCTIONS_PROJ_MR_SHARE = 'TOTAL_DEDUCTIONS_PROJ_MR_SHARE'
        ULT_LOSS_PROJ_MR_SHARE = 'ULT_LOSS_PROJ_MR_SHARE'
        TOTAL_PREM_PROJ_MR_SHARE_CYB = 'TOTAL_PREM_PROJ_MR_SHARE_CYB'
        TOTAL_DEDUCT_PROJ_MR_SHARE_CYB = 'TOTAL_DEDUCT_PROJ_MR_SHARE_CYB'
        ULT_LOSS_PROJ_MR_SHARE_CYB = 'ULT_LOSS_PROJ_MR_SHARE_CYB'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, PROGRAM_SUBSYSTEM_TREATY_ID: str = None, PRICING_TOOL: str = None, DWH_RI_CONTRACT_PERIOD_ID: float = None, PROGRAM_ID_OR_RIPP_ID: float = None, ADMIN_SYSTEM_RPP_ID: str = None, ISPROPERTYBUYBACK: str = None, PROGRAM_NAME: str = None, DWH_RI_COVERAGE_ID: float = None, TREATY_ID_OR_COVERAGE_ID: float = None, TREATY_NAME: str = None, FSRI_LINKAGES_FOR_TTY_BUSINESS: str = None, FMT_ID_FOR_FAB_BUSINESS: str = None, CLIENT_ID: str = None, CLIENT_NAME: str = None, BROKER_ID: str = None, BROKER_NAME: str = None, INSURED_ID: str = None, INSURED_NAME: str = None, RESULT_RESPONSIBILITY_ID: str = None, RESULT_RESPONSIBILITY_NAME: str = None, RESULT_RESPONSIBILITY_LVL_3: str = None, RESULT_RESPONSIBILITY_LVL_4: str = None, RESULT_RESPONSIBILITY_LVL_5: str = None, RESULT_RESPONSIBILITY_LVL_6: str = None, RESULT_RESP_PRESENTATION: str = None, UW_YEAR: float = None, BEGIN_DATE: datetime = None, END_DATE: datetime = None, UW_STATUS: str = None, NATURE_OF_TREATY: str = None, FAC_OR_TREATY: str = None, VIRTUAL_TREATY_FLAG: float = None, MR_SHARE: float = None, PROTECTED_SHARE: float = None, TOTAL_CYBER_COB_SHARE: float = None, ORIG_CURRENCY_ID: str = None, EXCHANGE_RATE: float = None, LIMIT_100_PCT: float = None, ATTACHMENT_POINT_100_PCT: float = None, EXP_TOTAL_PREMIUM_100_PCT: float = None, EXP_TOTAL_DEDUCTIONS_100_PCT: float = None, EXP_ULTIMATE_LOSS_100_PCT: float = None, EXP_TOTAL_PREMIUM_100P_CYB: float = None, EXP_TOTAL_DEDUCTIONS_100P_CYB: float = None, EXP_ULT_LOSS_100P_CYB: float = None, EXP_TOTAL_PREMIUM_MR_SHARE: float = None, EXP_TOTAL_DEDUCTIONS_MR_SHARE: float = None, EXP_ULT_LOSS_MR_SHARE: float = None, EXP_TOTAL_PREMIUM_MR_SHARE_CYB: float = None, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB: float = None, EXP_ULT_LOSS_MR_SHARE_CYB: float = None, ULTIMATE_LOSS_RATIO: float = None, TECHNICAL_COMBINED_RATIO: float = None, COMBINED_RATIO: float = None, EXP_PV_UW_RESLT_MR_SHARE: float = None, RORAC: float = None, IDENTITY_THEFT_SHARE: float = None, DATA_COMPROMISE_SHARE: float = None, CYBER_THIRD_PARTY_SHARE: float = None, CYBER_THIRD_PARTY_FI_SHARE: float = None, CYBER_THIRD_PARTY_COMM_SHARE: float = None, CYBER_FIRST_PARTY_SHARE: float = None, CYBER_FIRST_PARTY_PERS_SHARE: float = None, CYBER_INDUCED_PROP_DMG_SHARE: float = None, CYBER_MULTIPERIL_TPL_SHARE: float = None, MARINE_CYBER_THIRD_PARTY_SHARE: float = None, MARINE_CYBER_FIRST_PARTY_SHARE: float = None, PRICING_BUDGET_ITV: float = None, PRICING_BUDGET_DB: float = None, PRICING_BUDGET_IF: float = None, PRICING_BUDGET_CLOUD: float = None, TOTAL_PREMIUM_ASAT_100_PCT: float = None, TOTAL_DEDUCTIONS_ASAT_100_PCT: float = None, ULT_LOSS_ASAT_100_PCT: float = None, TOTAL_PREMIUM_ASAT_100P_CYB: float = None, TOTAL_DEDUCTIONS_ASAT_100P_CYB: float = None, ULT_LOSS_ASAT_100P_CYB: float = None, TOTAL_PREMIUM_ASAT_MR_SHARE: float = None, TOTAL_DEDUCTIONS_ASAT_MR_SHARE: float = None, ULT_LOSS_ASAT_MR_SHARE: float = None, TOTAL_PREM_ASAT_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB: float = None, ULT_LOSS_ASAT_MR_SHARE_CYB: float = None, TOTAL_PREMIUM_PROJ_100_PCT: float = None, TOTAL_DEDUCTIONS_PROJ_100_PCT: float = None, ULT_LOSS_PROJ_100_PCT: float = None, TOTAL_PREMIUM_PROJ_100P_CYB: float = None, TOTAL_DEDUCTIONS_PROJ_100P_CYB: float = None, ULT_LOSS_PROJ_100P_CYB: float = None, TOTAL_PREMIUM_PROJ_MR_SHARE: float = None, TOTAL_DEDUCTIONS_PROJ_MR_SHARE: float = None, ULT_LOSS_PROJ_MR_SHARE: float = None, TOTAL_PREM_PROJ_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB: float = None, ULT_LOSS_PROJ_MR_SHARE_CYB: float = None) -> DataFrame:
            sql = """
DECLARE
    @PROGRAM_SUBSYSTEM_TREATY_ID varchar(255) = ?
    ,@PRICING_TOOL varchar(255) = ?
    ,@DWH_RI_CONTRACT_PERIOD_ID float = ?
    ,@PROGRAM_ID_OR_RIPP_ID float = ?
    ,@ADMIN_SYSTEM_RPP_ID varchar(255) = ?
    ,@ISPROPERTYBUYBACK varchar(255) = ?
    ,@PROGRAM_NAME varchar(255) = ?
    ,@DWH_RI_COVERAGE_ID float = ?
    ,@TREATY_ID_OR_COVERAGE_ID float = ?
    ,@TREATY_NAME varchar(255) = ?
    ,@FSRI_LINKAGES_FOR_TTY_BUSINESS varchar(261) = ?
    ,@FMT_ID_FOR_FAB_BUSINESS varchar(255) = ?
    ,@CLIENT_ID varchar(255) = ?
    ,@CLIENT_NAME varchar(255) = ?
    ,@BROKER_ID varchar(255) = ?
    ,@BROKER_NAME varchar(255) = ?
    ,@INSURED_ID varchar(255) = ?
    ,@INSURED_NAME varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_ID varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_NAME varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_3 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_4 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_5 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_6 varchar(255) = ?
    ,@RESULT_RESP_PRESENTATION varchar(255) = ?
    ,@UW_YEAR float = ?
    ,@BEGIN_DATE datetime = ?
    ,@END_DATE datetime = ?
    ,@UW_STATUS varchar(255) = ?
    ,@NATURE_OF_TREATY varchar(255) = ?
    ,@FAC_OR_TREATY varchar(255) = ?
    ,@VIRTUAL_TREATY_FLAG float = ?
    ,@MR_SHARE float = ?
    ,@PROTECTED_SHARE float = ?
    ,@TOTAL_CYBER_COB_SHARE float = ?
    ,@ORIG_CURRENCY_ID varchar(255) = ?
    ,@EXCHANGE_RATE float = ?
    ,@LIMIT_100_PCT float = ?
    ,@ATTACHMENT_POINT_100_PCT float = ?
    ,@EXP_TOTAL_PREMIUM_100_PCT float = ?
    ,@EXP_TOTAL_DEDUCTIONS_100_PCT float = ?
    ,@EXP_ULTIMATE_LOSS_100_PCT float = ?
    ,@EXP_TOTAL_PREMIUM_100P_CYB float = ?
    ,@EXP_TOTAL_DEDUCTIONS_100P_CYB float = ?
    ,@EXP_ULT_LOSS_100P_CYB float = ?
    ,@EXP_TOTAL_PREMIUM_MR_SHARE float = ?
    ,@EXP_TOTAL_DEDUCTIONS_MR_SHARE float = ?
    ,@EXP_ULT_LOSS_MR_SHARE float = ?
    ,@EXP_TOTAL_PREMIUM_MR_SHARE_CYB float = ?
    ,@EXP_TOTAL_DEDUCTS_MR_SHARE_CYB float = ?
    ,@EXP_ULT_LOSS_MR_SHARE_CYB float = ?
    ,@ULTIMATE_LOSS_RATIO float = ?
    ,@TECHNICAL_COMBINED_RATIO float = ?
    ,@COMBINED_RATIO float = ?
    ,@EXP_PV_UW_RESLT_MR_SHARE float = ?
    ,@RORAC float = ?
    ,@IDENTITY_THEFT_SHARE float = ?
    ,@DATA_COMPROMISE_SHARE float = ?
    ,@CYBER_THIRD_PARTY_SHARE float = ?
    ,@CYBER_THIRD_PARTY_FI_SHARE float = ?
    ,@CYBER_THIRD_PARTY_COMM_SHARE float = ?
    ,@CYBER_FIRST_PARTY_SHARE float = ?
    ,@CYBER_FIRST_PARTY_PERS_SHARE float = ?
    ,@CYBER_INDUCED_PROP_DMG_SHARE float = ?
    ,@CYBER_MULTIPERIL_TPL_SHARE float = ?
    ,@MARINE_CYBER_THIRD_PARTY_SHARE float = ?
    ,@MARINE_CYBER_FIRST_PARTY_SHARE float = ?
    ,@PRICING_BUDGET_ITV float = ?
    ,@PRICING_BUDGET_DB float = ?
    ,@PRICING_BUDGET_IF float = ?
    ,@PRICING_BUDGET_CLOUD float = ?
    ,@TOTAL_PREMIUM_ASAT_100_PCT float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_100_PCT float = ?
    ,@ULT_LOSS_ASAT_100_PCT float = ?
    ,@TOTAL_PREMIUM_ASAT_100P_CYB float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_100P_CYB float = ?
    ,@ULT_LOSS_ASAT_100P_CYB float = ?
    ,@TOTAL_PREMIUM_ASAT_MR_SHARE float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_MR_SHARE float = ?
    ,@ULT_LOSS_ASAT_MR_SHARE float = ?
    ,@TOTAL_PREM_ASAT_MR_SHARE_CYB float = ?
    ,@TOTAL_DEDUCT_ASAT_MR_SHARE_CYB float = ?
    ,@ULT_LOSS_ASAT_MR_SHARE_CYB float = ?
    ,@TOTAL_PREMIUM_PROJ_100_PCT float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_100_PCT float = ?
    ,@ULT_LOSS_PROJ_100_PCT float = ?
    ,@TOTAL_PREMIUM_PROJ_100P_CYB float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_100P_CYB float = ?
    ,@ULT_LOSS_PROJ_100P_CYB float = ?
    ,@TOTAL_PREMIUM_PROJ_MR_SHARE float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_MR_SHARE float = ?
    ,@ULT_LOSS_PROJ_MR_SHARE float = ?
    ,@TOTAL_PREM_PROJ_MR_SHARE_CYB float = ?
    ,@TOTAL_DEDUCT_PROJ_MR_SHARE_CYB float = ?
    ,@ULT_LOSS_PROJ_MR_SHARE_CYB float = ?
;

INSERT INTO [dbo].[pgo_data] (
    [PROGRAM_SUBSYSTEM_TREATY_ID]
    ,[PRICING_TOOL]
    ,[DWH_RI_CONTRACT_PERIOD_ID]
    ,[PROGRAM_ID_OR_RIPP_ID]
    ,[ADMIN_SYSTEM_RPP_ID]
    ,[ISPROPERTYBUYBACK]
    ,[PROGRAM_NAME]
    ,[DWH_RI_COVERAGE_ID]
    ,[TREATY_ID_OR_COVERAGE_ID]
    ,[TREATY_NAME]
    ,[FSRI_LINKAGES_FOR_TTY_BUSINESS]
    ,[FMT_ID_FOR_FAB_BUSINESS]
    ,[CLIENT_ID]
    ,[CLIENT_NAME]
    ,[BROKER_ID]
    ,[BROKER_NAME]
    ,[INSURED_ID]
    ,[INSURED_NAME]
    ,[RESULT_RESPONSIBILITY_ID]
    ,[RESULT_RESPONSIBILITY_NAME]
    ,[RESULT_RESPONSIBILITY_LVL_3]
    ,[RESULT_RESPONSIBILITY_LVL_4]
    ,[RESULT_RESPONSIBILITY_LVL_5]
    ,[RESULT_RESPONSIBILITY_LVL_6]
    ,[RESULT_RESP_PRESENTATION]
    ,[UW_YEAR]
    ,[BEGIN_DATE]
    ,[END_DATE]
    ,[UW_STATUS]
    ,[NATURE_OF_TREATY]
    ,[FAC_OR_TREATY]
    ,[VIRTUAL_TREATY_FLAG]
    ,[MR_SHARE]
    ,[PROTECTED_SHARE]
    ,[TOTAL_CYBER_COB_SHARE]
    ,[ORIG_CURRENCY_ID]
    ,[EXCHANGE_RATE]
    ,[LIMIT_100_PCT]
    ,[ATTACHMENT_POINT_100_PCT]
    ,[EXP_TOTAL_PREMIUM_100_PCT]
    ,[EXP_TOTAL_DEDUCTIONS_100_PCT]
    ,[EXP_ULTIMATE_LOSS_100_PCT]
    ,[EXP_TOTAL_PREMIUM_100P_CYB]
    ,[EXP_TOTAL_DEDUCTIONS_100P_CYB]
    ,[EXP_ULT_LOSS_100P_CYB]
    ,[EXP_TOTAL_PREMIUM_MR_SHARE]
    ,[EXP_TOTAL_DEDUCTIONS_MR_SHARE]
    ,[EXP_ULT_LOSS_MR_SHARE]
    ,[EXP_TOTAL_PREMIUM_MR_SHARE_CYB]
    ,[EXP_TOTAL_DEDUCTS_MR_SHARE_CYB]
    ,[EXP_ULT_LOSS_MR_SHARE_CYB]
    ,[ULTIMATE_LOSS_RATIO]
    ,[TECHNICAL_COMBINED_RATIO]
    ,[COMBINED_RATIO]
    ,[EXP_PV_UW_RESLT_MR_SHARE]
    ,[RORAC]
    ,[IDENTITY_THEFT_SHARE]
    ,[DATA_COMPROMISE_SHARE]
    ,[CYBER_THIRD_PARTY_SHARE]
    ,[CYBER_THIRD_PARTY_FI_SHARE]
    ,[CYBER_THIRD_PARTY_COMM_SHARE]
    ,[CYBER_FIRST_PARTY_SHARE]
    ,[CYBER_FIRST_PARTY_PERS_SHARE]
    ,[CYBER_INDUCED_PROP_DMG_SHARE]
    ,[CYBER_MULTIPERIL_TPL_SHARE]
    ,[MARINE_CYBER_THIRD_PARTY_SHARE]
    ,[MARINE_CYBER_FIRST_PARTY_SHARE]
    ,[PRICING_BUDGET_ITV]
    ,[PRICING_BUDGET_DB]
    ,[PRICING_BUDGET_IF]
    ,[PRICING_BUDGET_CLOUD]
    ,[TOTAL_PREMIUM_ASAT_100_PCT]
    ,[TOTAL_DEDUCTIONS_ASAT_100_PCT]
    ,[ULT_LOSS_ASAT_100_PCT]
    ,[TOTAL_PREMIUM_ASAT_100P_CYB]
    ,[TOTAL_DEDUCTIONS_ASAT_100P_CYB]
    ,[ULT_LOSS_ASAT_100P_CYB]
    ,[TOTAL_PREMIUM_ASAT_MR_SHARE]
    ,[TOTAL_DEDUCTIONS_ASAT_MR_SHARE]
    ,[ULT_LOSS_ASAT_MR_SHARE]
    ,[TOTAL_PREM_ASAT_MR_SHARE_CYB]
    ,[TOTAL_DEDUCT_ASAT_MR_SHARE_CYB]
    ,[ULT_LOSS_ASAT_MR_SHARE_CYB]
    ,[TOTAL_PREMIUM_PROJ_100_PCT]
    ,[TOTAL_DEDUCTIONS_PROJ_100_PCT]
    ,[ULT_LOSS_PROJ_100_PCT]
    ,[TOTAL_PREMIUM_PROJ_100P_CYB]
    ,[TOTAL_DEDUCTIONS_PROJ_100P_CYB]
    ,[ULT_LOSS_PROJ_100P_CYB]
    ,[TOTAL_PREMIUM_PROJ_MR_SHARE]
    ,[TOTAL_DEDUCTIONS_PROJ_MR_SHARE]
    ,[ULT_LOSS_PROJ_MR_SHARE]
    ,[TOTAL_PREM_PROJ_MR_SHARE_CYB]
    ,[TOTAL_DEDUCT_PROJ_MR_SHARE_CYB]
    ,[ULT_LOSS_PROJ_MR_SHARE_CYB]
)
VALUES (
    @PROGRAM_SUBSYSTEM_TREATY_ID
    ,@PRICING_TOOL
    ,@DWH_RI_CONTRACT_PERIOD_ID
    ,@PROGRAM_ID_OR_RIPP_ID
    ,@ADMIN_SYSTEM_RPP_ID
    ,@ISPROPERTYBUYBACK
    ,@PROGRAM_NAME
    ,@DWH_RI_COVERAGE_ID
    ,@TREATY_ID_OR_COVERAGE_ID
    ,@TREATY_NAME
    ,@FSRI_LINKAGES_FOR_TTY_BUSINESS
    ,@FMT_ID_FOR_FAB_BUSINESS
    ,@CLIENT_ID
    ,@CLIENT_NAME
    ,@BROKER_ID
    ,@BROKER_NAME
    ,@INSURED_ID
    ,@INSURED_NAME
    ,@RESULT_RESPONSIBILITY_ID
    ,@RESULT_RESPONSIBILITY_NAME
    ,@RESULT_RESPONSIBILITY_LVL_3
    ,@RESULT_RESPONSIBILITY_LVL_4
    ,@RESULT_RESPONSIBILITY_LVL_5
    ,@RESULT_RESPONSIBILITY_LVL_6
    ,@RESULT_RESP_PRESENTATION
    ,@UW_YEAR
    ,@BEGIN_DATE
    ,@END_DATE
    ,@UW_STATUS
    ,@NATURE_OF_TREATY
    ,@FAC_OR_TREATY
    ,@VIRTUAL_TREATY_FLAG
    ,@MR_SHARE
    ,@PROTECTED_SHARE
    ,@TOTAL_CYBER_COB_SHARE
    ,@ORIG_CURRENCY_ID
    ,@EXCHANGE_RATE
    ,@LIMIT_100_PCT
    ,@ATTACHMENT_POINT_100_PCT
    ,@EXP_TOTAL_PREMIUM_100_PCT
    ,@EXP_TOTAL_DEDUCTIONS_100_PCT
    ,@EXP_ULTIMATE_LOSS_100_PCT
    ,@EXP_TOTAL_PREMIUM_100P_CYB
    ,@EXP_TOTAL_DEDUCTIONS_100P_CYB
    ,@EXP_ULT_LOSS_100P_CYB
    ,@EXP_TOTAL_PREMIUM_MR_SHARE
    ,@EXP_TOTAL_DEDUCTIONS_MR_SHARE
    ,@EXP_ULT_LOSS_MR_SHARE
    ,@EXP_TOTAL_PREMIUM_MR_SHARE_CYB
    ,@EXP_TOTAL_DEDUCTS_MR_SHARE_CYB
    ,@EXP_ULT_LOSS_MR_SHARE_CYB
    ,@ULTIMATE_LOSS_RATIO
    ,@TECHNICAL_COMBINED_RATIO
    ,@COMBINED_RATIO
    ,@EXP_PV_UW_RESLT_MR_SHARE
    ,@RORAC
    ,@IDENTITY_THEFT_SHARE
    ,@DATA_COMPROMISE_SHARE
    ,@CYBER_THIRD_PARTY_SHARE
    ,@CYBER_THIRD_PARTY_FI_SHARE
    ,@CYBER_THIRD_PARTY_COMM_SHARE
    ,@CYBER_FIRST_PARTY_SHARE
    ,@CYBER_FIRST_PARTY_PERS_SHARE
    ,@CYBER_INDUCED_PROP_DMG_SHARE
    ,@CYBER_MULTIPERIL_TPL_SHARE
    ,@MARINE_CYBER_THIRD_PARTY_SHARE
    ,@MARINE_CYBER_FIRST_PARTY_SHARE
    ,@PRICING_BUDGET_ITV
    ,@PRICING_BUDGET_DB
    ,@PRICING_BUDGET_IF
    ,@PRICING_BUDGET_CLOUD
    ,@TOTAL_PREMIUM_ASAT_100_PCT
    ,@TOTAL_DEDUCTIONS_ASAT_100_PCT
    ,@ULT_LOSS_ASAT_100_PCT
    ,@TOTAL_PREMIUM_ASAT_100P_CYB
    ,@TOTAL_DEDUCTIONS_ASAT_100P_CYB
    ,@ULT_LOSS_ASAT_100P_CYB
    ,@TOTAL_PREMIUM_ASAT_MR_SHARE
    ,@TOTAL_DEDUCTIONS_ASAT_MR_SHARE
    ,@ULT_LOSS_ASAT_MR_SHARE
    ,@TOTAL_PREM_ASAT_MR_SHARE_CYB
    ,@TOTAL_DEDUCT_ASAT_MR_SHARE_CYB
    ,@ULT_LOSS_ASAT_MR_SHARE_CYB
    ,@TOTAL_PREMIUM_PROJ_100_PCT
    ,@TOTAL_DEDUCTIONS_PROJ_100_PCT
    ,@ULT_LOSS_PROJ_100_PCT
    ,@TOTAL_PREMIUM_PROJ_100P_CYB
    ,@TOTAL_DEDUCTIONS_PROJ_100P_CYB
    ,@ULT_LOSS_PROJ_100P_CYB
    ,@TOTAL_PREMIUM_PROJ_MR_SHARE
    ,@TOTAL_DEDUCTIONS_PROJ_MR_SHARE
    ,@ULT_LOSS_PROJ_MR_SHARE
    ,@TOTAL_PREM_PROJ_MR_SHARE_CYB
    ,@TOTAL_DEDUCT_PROJ_MR_SHARE_CYB
    ,@ULT_LOSS_PROJ_MR_SHARE_CYB
);
"""
            return DbCmd(self.cnOrStr, sql, [ PROGRAM_SUBSYSTEM_TREATY_ID, PRICING_TOOL, DWH_RI_CONTRACT_PERIOD_ID, PROGRAM_ID_OR_RIPP_ID, ADMIN_SYSTEM_RPP_ID, ISPROPERTYBUYBACK, PROGRAM_NAME, DWH_RI_COVERAGE_ID, TREATY_ID_OR_COVERAGE_ID, TREATY_NAME, FSRI_LINKAGES_FOR_TTY_BUSINESS, FMT_ID_FOR_FAB_BUSINESS, CLIENT_ID, CLIENT_NAME, BROKER_ID, BROKER_NAME, INSURED_ID, INSURED_NAME, RESULT_RESPONSIBILITY_ID, RESULT_RESPONSIBILITY_NAME, RESULT_RESPONSIBILITY_LVL_3, RESULT_RESPONSIBILITY_LVL_4, RESULT_RESPONSIBILITY_LVL_5, RESULT_RESPONSIBILITY_LVL_6, RESULT_RESP_PRESENTATION, UW_YEAR, BEGIN_DATE, END_DATE, UW_STATUS, NATURE_OF_TREATY, FAC_OR_TREATY, VIRTUAL_TREATY_FLAG, MR_SHARE, PROTECTED_SHARE, TOTAL_CYBER_COB_SHARE, ORIG_CURRENCY_ID, EXCHANGE_RATE, LIMIT_100_PCT, ATTACHMENT_POINT_100_PCT, EXP_TOTAL_PREMIUM_100_PCT, EXP_TOTAL_DEDUCTIONS_100_PCT, EXP_ULTIMATE_LOSS_100_PCT, EXP_TOTAL_PREMIUM_100P_CYB, EXP_TOTAL_DEDUCTIONS_100P_CYB, EXP_ULT_LOSS_100P_CYB, EXP_TOTAL_PREMIUM_MR_SHARE, EXP_TOTAL_DEDUCTIONS_MR_SHARE, EXP_ULT_LOSS_MR_SHARE, EXP_TOTAL_PREMIUM_MR_SHARE_CYB, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB, EXP_ULT_LOSS_MR_SHARE_CYB, ULTIMATE_LOSS_RATIO, TECHNICAL_COMBINED_RATIO, COMBINED_RATIO, EXP_PV_UW_RESLT_MR_SHARE, RORAC, IDENTITY_THEFT_SHARE, DATA_COMPROMISE_SHARE, CYBER_THIRD_PARTY_SHARE, CYBER_THIRD_PARTY_FI_SHARE, CYBER_THIRD_PARTY_COMM_SHARE, CYBER_FIRST_PARTY_SHARE, CYBER_FIRST_PARTY_PERS_SHARE, CYBER_INDUCED_PROP_DMG_SHARE, CYBER_MULTIPERIL_TPL_SHARE, MARINE_CYBER_THIRD_PARTY_SHARE, MARINE_CYBER_FIRST_PARTY_SHARE, PRICING_BUDGET_ITV, PRICING_BUDGET_DB, PRICING_BUDGET_IF, PRICING_BUDGET_CLOUD, TOTAL_PREMIUM_ASAT_100_PCT, TOTAL_DEDUCTIONS_ASAT_100_PCT, ULT_LOSS_ASAT_100_PCT, TOTAL_PREMIUM_ASAT_100P_CYB, TOTAL_DEDUCTIONS_ASAT_100P_CYB, ULT_LOSS_ASAT_100P_CYB, TOTAL_PREMIUM_ASAT_MR_SHARE, TOTAL_DEDUCTIONS_ASAT_MR_SHARE, ULT_LOSS_ASAT_MR_SHARE, TOTAL_PREM_ASAT_MR_SHARE_CYB, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB, ULT_LOSS_ASAT_MR_SHARE_CYB, TOTAL_PREMIUM_PROJ_100_PCT, TOTAL_DEDUCTIONS_PROJ_100_PCT, ULT_LOSS_PROJ_100_PCT, TOTAL_PREMIUM_PROJ_100P_CYB, TOTAL_DEDUCTIONS_PROJ_100P_CYB, ULT_LOSS_PROJ_100P_CYB, TOTAL_PREMIUM_PROJ_MR_SHARE, TOTAL_DEDUCTIONS_PROJ_MR_SHARE, ULT_LOSS_PROJ_MR_SHARE, TOTAL_PREM_PROJ_MR_SHARE_CYB, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB, ULT_LOSS_PROJ_MR_SHARE_CYB ]).exec_df()

        def update(self, PROGRAM_SUBSYSTEM_TREATY_ID: str = None, PRICING_TOOL: str = None, DWH_RI_CONTRACT_PERIOD_ID: float = None, PROGRAM_ID_OR_RIPP_ID: float = None, ADMIN_SYSTEM_RPP_ID: str = None, ISPROPERTYBUYBACK: str = None, PROGRAM_NAME: str = None, DWH_RI_COVERAGE_ID: float = None, TREATY_ID_OR_COVERAGE_ID: float = None, TREATY_NAME: str = None, FSRI_LINKAGES_FOR_TTY_BUSINESS: str = None, FMT_ID_FOR_FAB_BUSINESS: str = None, CLIENT_ID: str = None, CLIENT_NAME: str = None, BROKER_ID: str = None, BROKER_NAME: str = None, INSURED_ID: str = None, INSURED_NAME: str = None, RESULT_RESPONSIBILITY_ID: str = None, RESULT_RESPONSIBILITY_NAME: str = None, RESULT_RESPONSIBILITY_LVL_3: str = None, RESULT_RESPONSIBILITY_LVL_4: str = None, RESULT_RESPONSIBILITY_LVL_5: str = None, RESULT_RESPONSIBILITY_LVL_6: str = None, RESULT_RESP_PRESENTATION: str = None, UW_YEAR: float = None, BEGIN_DATE: datetime = None, END_DATE: datetime = None, UW_STATUS: str = None, NATURE_OF_TREATY: str = None, FAC_OR_TREATY: str = None, VIRTUAL_TREATY_FLAG: float = None, MR_SHARE: float = None, PROTECTED_SHARE: float = None, TOTAL_CYBER_COB_SHARE: float = None, ORIG_CURRENCY_ID: str = None, EXCHANGE_RATE: float = None, LIMIT_100_PCT: float = None, ATTACHMENT_POINT_100_PCT: float = None, EXP_TOTAL_PREMIUM_100_PCT: float = None, EXP_TOTAL_DEDUCTIONS_100_PCT: float = None, EXP_ULTIMATE_LOSS_100_PCT: float = None, EXP_TOTAL_PREMIUM_100P_CYB: float = None, EXP_TOTAL_DEDUCTIONS_100P_CYB: float = None, EXP_ULT_LOSS_100P_CYB: float = None, EXP_TOTAL_PREMIUM_MR_SHARE: float = None, EXP_TOTAL_DEDUCTIONS_MR_SHARE: float = None, EXP_ULT_LOSS_MR_SHARE: float = None, EXP_TOTAL_PREMIUM_MR_SHARE_CYB: float = None, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB: float = None, EXP_ULT_LOSS_MR_SHARE_CYB: float = None, ULTIMATE_LOSS_RATIO: float = None, TECHNICAL_COMBINED_RATIO: float = None, COMBINED_RATIO: float = None, EXP_PV_UW_RESLT_MR_SHARE: float = None, RORAC: float = None, IDENTITY_THEFT_SHARE: float = None, DATA_COMPROMISE_SHARE: float = None, CYBER_THIRD_PARTY_SHARE: float = None, CYBER_THIRD_PARTY_FI_SHARE: float = None, CYBER_THIRD_PARTY_COMM_SHARE: float = None, CYBER_FIRST_PARTY_SHARE: float = None, CYBER_FIRST_PARTY_PERS_SHARE: float = None, CYBER_INDUCED_PROP_DMG_SHARE: float = None, CYBER_MULTIPERIL_TPL_SHARE: float = None, MARINE_CYBER_THIRD_PARTY_SHARE: float = None, MARINE_CYBER_FIRST_PARTY_SHARE: float = None, PRICING_BUDGET_ITV: float = None, PRICING_BUDGET_DB: float = None, PRICING_BUDGET_IF: float = None, PRICING_BUDGET_CLOUD: float = None, TOTAL_PREMIUM_ASAT_100_PCT: float = None, TOTAL_DEDUCTIONS_ASAT_100_PCT: float = None, ULT_LOSS_ASAT_100_PCT: float = None, TOTAL_PREMIUM_ASAT_100P_CYB: float = None, TOTAL_DEDUCTIONS_ASAT_100P_CYB: float = None, ULT_LOSS_ASAT_100P_CYB: float = None, TOTAL_PREMIUM_ASAT_MR_SHARE: float = None, TOTAL_DEDUCTIONS_ASAT_MR_SHARE: float = None, ULT_LOSS_ASAT_MR_SHARE: float = None, TOTAL_PREM_ASAT_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB: float = None, ULT_LOSS_ASAT_MR_SHARE_CYB: float = None, TOTAL_PREMIUM_PROJ_100_PCT: float = None, TOTAL_DEDUCTIONS_PROJ_100_PCT: float = None, ULT_LOSS_PROJ_100_PCT: float = None, TOTAL_PREMIUM_PROJ_100P_CYB: float = None, TOTAL_DEDUCTIONS_PROJ_100P_CYB: float = None, ULT_LOSS_PROJ_100P_CYB: float = None, TOTAL_PREMIUM_PROJ_MR_SHARE: float = None, TOTAL_DEDUCTIONS_PROJ_MR_SHARE: float = None, ULT_LOSS_PROJ_MR_SHARE: float = None, TOTAL_PREM_PROJ_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB: float = None, ULT_LOSS_PROJ_MR_SHARE_CYB: float = None) -> DataFrame:
            sql = """
DECLARE
    @PROGRAM_SUBSYSTEM_TREATY_ID varchar(255) = ?
    ,@PRICING_TOOL varchar(255) = ?
    ,@DWH_RI_CONTRACT_PERIOD_ID float = ?
    ,@PROGRAM_ID_OR_RIPP_ID float = ?
    ,@ADMIN_SYSTEM_RPP_ID varchar(255) = ?
    ,@ISPROPERTYBUYBACK varchar(255) = ?
    ,@PROGRAM_NAME varchar(255) = ?
    ,@DWH_RI_COVERAGE_ID float = ?
    ,@TREATY_ID_OR_COVERAGE_ID float = ?
    ,@TREATY_NAME varchar(255) = ?
    ,@FSRI_LINKAGES_FOR_TTY_BUSINESS varchar(261) = ?
    ,@FMT_ID_FOR_FAB_BUSINESS varchar(255) = ?
    ,@CLIENT_ID varchar(255) = ?
    ,@CLIENT_NAME varchar(255) = ?
    ,@BROKER_ID varchar(255) = ?
    ,@BROKER_NAME varchar(255) = ?
    ,@INSURED_ID varchar(255) = ?
    ,@INSURED_NAME varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_ID varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_NAME varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_3 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_4 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_5 varchar(255) = ?
    ,@RESULT_RESPONSIBILITY_LVL_6 varchar(255) = ?
    ,@RESULT_RESP_PRESENTATION varchar(255) = ?
    ,@UW_YEAR float = ?
    ,@BEGIN_DATE datetime = ?
    ,@END_DATE datetime = ?
    ,@UW_STATUS varchar(255) = ?
    ,@NATURE_OF_TREATY varchar(255) = ?
    ,@FAC_OR_TREATY varchar(255) = ?
    ,@VIRTUAL_TREATY_FLAG float = ?
    ,@MR_SHARE float = ?
    ,@PROTECTED_SHARE float = ?
    ,@TOTAL_CYBER_COB_SHARE float = ?
    ,@ORIG_CURRENCY_ID varchar(255) = ?
    ,@EXCHANGE_RATE float = ?
    ,@LIMIT_100_PCT float = ?
    ,@ATTACHMENT_POINT_100_PCT float = ?
    ,@EXP_TOTAL_PREMIUM_100_PCT float = ?
    ,@EXP_TOTAL_DEDUCTIONS_100_PCT float = ?
    ,@EXP_ULTIMATE_LOSS_100_PCT float = ?
    ,@EXP_TOTAL_PREMIUM_100P_CYB float = ?
    ,@EXP_TOTAL_DEDUCTIONS_100P_CYB float = ?
    ,@EXP_ULT_LOSS_100P_CYB float = ?
    ,@EXP_TOTAL_PREMIUM_MR_SHARE float = ?
    ,@EXP_TOTAL_DEDUCTIONS_MR_SHARE float = ?
    ,@EXP_ULT_LOSS_MR_SHARE float = ?
    ,@EXP_TOTAL_PREMIUM_MR_SHARE_CYB float = ?
    ,@EXP_TOTAL_DEDUCTS_MR_SHARE_CYB float = ?
    ,@EXP_ULT_LOSS_MR_SHARE_CYB float = ?
    ,@ULTIMATE_LOSS_RATIO float = ?
    ,@TECHNICAL_COMBINED_RATIO float = ?
    ,@COMBINED_RATIO float = ?
    ,@EXP_PV_UW_RESLT_MR_SHARE float = ?
    ,@RORAC float = ?
    ,@IDENTITY_THEFT_SHARE float = ?
    ,@DATA_COMPROMISE_SHARE float = ?
    ,@CYBER_THIRD_PARTY_SHARE float = ?
    ,@CYBER_THIRD_PARTY_FI_SHARE float = ?
    ,@CYBER_THIRD_PARTY_COMM_SHARE float = ?
    ,@CYBER_FIRST_PARTY_SHARE float = ?
    ,@CYBER_FIRST_PARTY_PERS_SHARE float = ?
    ,@CYBER_INDUCED_PROP_DMG_SHARE float = ?
    ,@CYBER_MULTIPERIL_TPL_SHARE float = ?
    ,@MARINE_CYBER_THIRD_PARTY_SHARE float = ?
    ,@MARINE_CYBER_FIRST_PARTY_SHARE float = ?
    ,@PRICING_BUDGET_ITV float = ?
    ,@PRICING_BUDGET_DB float = ?
    ,@PRICING_BUDGET_IF float = ?
    ,@PRICING_BUDGET_CLOUD float = ?
    ,@TOTAL_PREMIUM_ASAT_100_PCT float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_100_PCT float = ?
    ,@ULT_LOSS_ASAT_100_PCT float = ?
    ,@TOTAL_PREMIUM_ASAT_100P_CYB float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_100P_CYB float = ?
    ,@ULT_LOSS_ASAT_100P_CYB float = ?
    ,@TOTAL_PREMIUM_ASAT_MR_SHARE float = ?
    ,@TOTAL_DEDUCTIONS_ASAT_MR_SHARE float = ?
    ,@ULT_LOSS_ASAT_MR_SHARE float = ?
    ,@TOTAL_PREM_ASAT_MR_SHARE_CYB float = ?
    ,@TOTAL_DEDUCT_ASAT_MR_SHARE_CYB float = ?
    ,@ULT_LOSS_ASAT_MR_SHARE_CYB float = ?
    ,@TOTAL_PREMIUM_PROJ_100_PCT float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_100_PCT float = ?
    ,@ULT_LOSS_PROJ_100_PCT float = ?
    ,@TOTAL_PREMIUM_PROJ_100P_CYB float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_100P_CYB float = ?
    ,@ULT_LOSS_PROJ_100P_CYB float = ?
    ,@TOTAL_PREMIUM_PROJ_MR_SHARE float = ?
    ,@TOTAL_DEDUCTIONS_PROJ_MR_SHARE float = ?
    ,@ULT_LOSS_PROJ_MR_SHARE float = ?
    ,@TOTAL_PREM_PROJ_MR_SHARE_CYB float = ?
    ,@TOTAL_DEDUCT_PROJ_MR_SHARE_CYB float = ?
    ,@ULT_LOSS_PROJ_MR_SHARE_CYB float = ?
;

UPDATE [dbo].[pgo_data] SET 
    [PROGRAM_SUBSYSTEM_TREATY_ID] = @PROGRAM_SUBSYSTEM_TREATY_ID
    ,[PRICING_TOOL] = @PRICING_TOOL
    ,[DWH_RI_CONTRACT_PERIOD_ID] = @DWH_RI_CONTRACT_PERIOD_ID
    ,[PROGRAM_ID_OR_RIPP_ID] = @PROGRAM_ID_OR_RIPP_ID
    ,[ADMIN_SYSTEM_RPP_ID] = @ADMIN_SYSTEM_RPP_ID
    ,[ISPROPERTYBUYBACK] = @ISPROPERTYBUYBACK
    ,[PROGRAM_NAME] = @PROGRAM_NAME
    ,[DWH_RI_COVERAGE_ID] = @DWH_RI_COVERAGE_ID
    ,[TREATY_ID_OR_COVERAGE_ID] = @TREATY_ID_OR_COVERAGE_ID
    ,[TREATY_NAME] = @TREATY_NAME
    ,[FSRI_LINKAGES_FOR_TTY_BUSINESS] = @FSRI_LINKAGES_FOR_TTY_BUSINESS
    ,[FMT_ID_FOR_FAB_BUSINESS] = @FMT_ID_FOR_FAB_BUSINESS
    ,[CLIENT_ID] = @CLIENT_ID
    ,[CLIENT_NAME] = @CLIENT_NAME
    ,[BROKER_ID] = @BROKER_ID
    ,[BROKER_NAME] = @BROKER_NAME
    ,[INSURED_ID] = @INSURED_ID
    ,[INSURED_NAME] = @INSURED_NAME
    ,[RESULT_RESPONSIBILITY_ID] = @RESULT_RESPONSIBILITY_ID
    ,[RESULT_RESPONSIBILITY_NAME] = @RESULT_RESPONSIBILITY_NAME
    ,[RESULT_RESPONSIBILITY_LVL_3] = @RESULT_RESPONSIBILITY_LVL_3
    ,[RESULT_RESPONSIBILITY_LVL_4] = @RESULT_RESPONSIBILITY_LVL_4
    ,[RESULT_RESPONSIBILITY_LVL_5] = @RESULT_RESPONSIBILITY_LVL_5
    ,[RESULT_RESPONSIBILITY_LVL_6] = @RESULT_RESPONSIBILITY_LVL_6
    ,[RESULT_RESP_PRESENTATION] = @RESULT_RESP_PRESENTATION
    ,[UW_YEAR] = @UW_YEAR
    ,[BEGIN_DATE] = @BEGIN_DATE
    ,[END_DATE] = @END_DATE
    ,[UW_STATUS] = @UW_STATUS
    ,[NATURE_OF_TREATY] = @NATURE_OF_TREATY
    ,[FAC_OR_TREATY] = @FAC_OR_TREATY
    ,[VIRTUAL_TREATY_FLAG] = @VIRTUAL_TREATY_FLAG
    ,[MR_SHARE] = @MR_SHARE
    ,[PROTECTED_SHARE] = @PROTECTED_SHARE
    ,[TOTAL_CYBER_COB_SHARE] = @TOTAL_CYBER_COB_SHARE
    ,[ORIG_CURRENCY_ID] = @ORIG_CURRENCY_ID
    ,[EXCHANGE_RATE] = @EXCHANGE_RATE
    ,[LIMIT_100_PCT] = @LIMIT_100_PCT
    ,[ATTACHMENT_POINT_100_PCT] = @ATTACHMENT_POINT_100_PCT
    ,[EXP_TOTAL_PREMIUM_100_PCT] = @EXP_TOTAL_PREMIUM_100_PCT
    ,[EXP_TOTAL_DEDUCTIONS_100_PCT] = @EXP_TOTAL_DEDUCTIONS_100_PCT
    ,[EXP_ULTIMATE_LOSS_100_PCT] = @EXP_ULTIMATE_LOSS_100_PCT
    ,[EXP_TOTAL_PREMIUM_100P_CYB] = @EXP_TOTAL_PREMIUM_100P_CYB
    ,[EXP_TOTAL_DEDUCTIONS_100P_CYB] = @EXP_TOTAL_DEDUCTIONS_100P_CYB
    ,[EXP_ULT_LOSS_100P_CYB] = @EXP_ULT_LOSS_100P_CYB
    ,[EXP_TOTAL_PREMIUM_MR_SHARE] = @EXP_TOTAL_PREMIUM_MR_SHARE
    ,[EXP_TOTAL_DEDUCTIONS_MR_SHARE] = @EXP_TOTAL_DEDUCTIONS_MR_SHARE
    ,[EXP_ULT_LOSS_MR_SHARE] = @EXP_ULT_LOSS_MR_SHARE
    ,[EXP_TOTAL_PREMIUM_MR_SHARE_CYB] = @EXP_TOTAL_PREMIUM_MR_SHARE_CYB
    ,[EXP_TOTAL_DEDUCTS_MR_SHARE_CYB] = @EXP_TOTAL_DEDUCTS_MR_SHARE_CYB
    ,[EXP_ULT_LOSS_MR_SHARE_CYB] = @EXP_ULT_LOSS_MR_SHARE_CYB
    ,[ULTIMATE_LOSS_RATIO] = @ULTIMATE_LOSS_RATIO
    ,[TECHNICAL_COMBINED_RATIO] = @TECHNICAL_COMBINED_RATIO
    ,[COMBINED_RATIO] = @COMBINED_RATIO
    ,[EXP_PV_UW_RESLT_MR_SHARE] = @EXP_PV_UW_RESLT_MR_SHARE
    ,[RORAC] = @RORAC
    ,[IDENTITY_THEFT_SHARE] = @IDENTITY_THEFT_SHARE
    ,[DATA_COMPROMISE_SHARE] = @DATA_COMPROMISE_SHARE
    ,[CYBER_THIRD_PARTY_SHARE] = @CYBER_THIRD_PARTY_SHARE
    ,[CYBER_THIRD_PARTY_FI_SHARE] = @CYBER_THIRD_PARTY_FI_SHARE
    ,[CYBER_THIRD_PARTY_COMM_SHARE] = @CYBER_THIRD_PARTY_COMM_SHARE
    ,[CYBER_FIRST_PARTY_SHARE] = @CYBER_FIRST_PARTY_SHARE
    ,[CYBER_FIRST_PARTY_PERS_SHARE] = @CYBER_FIRST_PARTY_PERS_SHARE
    ,[CYBER_INDUCED_PROP_DMG_SHARE] = @CYBER_INDUCED_PROP_DMG_SHARE
    ,[CYBER_MULTIPERIL_TPL_SHARE] = @CYBER_MULTIPERIL_TPL_SHARE
    ,[MARINE_CYBER_THIRD_PARTY_SHARE] = @MARINE_CYBER_THIRD_PARTY_SHARE
    ,[MARINE_CYBER_FIRST_PARTY_SHARE] = @MARINE_CYBER_FIRST_PARTY_SHARE
    ,[PRICING_BUDGET_ITV] = @PRICING_BUDGET_ITV
    ,[PRICING_BUDGET_DB] = @PRICING_BUDGET_DB
    ,[PRICING_BUDGET_IF] = @PRICING_BUDGET_IF
    ,[PRICING_BUDGET_CLOUD] = @PRICING_BUDGET_CLOUD
    ,[TOTAL_PREMIUM_ASAT_100_PCT] = @TOTAL_PREMIUM_ASAT_100_PCT
    ,[TOTAL_DEDUCTIONS_ASAT_100_PCT] = @TOTAL_DEDUCTIONS_ASAT_100_PCT
    ,[ULT_LOSS_ASAT_100_PCT] = @ULT_LOSS_ASAT_100_PCT
    ,[TOTAL_PREMIUM_ASAT_100P_CYB] = @TOTAL_PREMIUM_ASAT_100P_CYB
    ,[TOTAL_DEDUCTIONS_ASAT_100P_CYB] = @TOTAL_DEDUCTIONS_ASAT_100P_CYB
    ,[ULT_LOSS_ASAT_100P_CYB] = @ULT_LOSS_ASAT_100P_CYB
    ,[TOTAL_PREMIUM_ASAT_MR_SHARE] = @TOTAL_PREMIUM_ASAT_MR_SHARE
    ,[TOTAL_DEDUCTIONS_ASAT_MR_SHARE] = @TOTAL_DEDUCTIONS_ASAT_MR_SHARE
    ,[ULT_LOSS_ASAT_MR_SHARE] = @ULT_LOSS_ASAT_MR_SHARE
    ,[TOTAL_PREM_ASAT_MR_SHARE_CYB] = @TOTAL_PREM_ASAT_MR_SHARE_CYB
    ,[TOTAL_DEDUCT_ASAT_MR_SHARE_CYB] = @TOTAL_DEDUCT_ASAT_MR_SHARE_CYB
    ,[ULT_LOSS_ASAT_MR_SHARE_CYB] = @ULT_LOSS_ASAT_MR_SHARE_CYB
    ,[TOTAL_PREMIUM_PROJ_100_PCT] = @TOTAL_PREMIUM_PROJ_100_PCT
    ,[TOTAL_DEDUCTIONS_PROJ_100_PCT] = @TOTAL_DEDUCTIONS_PROJ_100_PCT
    ,[ULT_LOSS_PROJ_100_PCT] = @ULT_LOSS_PROJ_100_PCT
    ,[TOTAL_PREMIUM_PROJ_100P_CYB] = @TOTAL_PREMIUM_PROJ_100P_CYB
    ,[TOTAL_DEDUCTIONS_PROJ_100P_CYB] = @TOTAL_DEDUCTIONS_PROJ_100P_CYB
    ,[ULT_LOSS_PROJ_100P_CYB] = @ULT_LOSS_PROJ_100P_CYB
    ,[TOTAL_PREMIUM_PROJ_MR_SHARE] = @TOTAL_PREMIUM_PROJ_MR_SHARE
    ,[TOTAL_DEDUCTIONS_PROJ_MR_SHARE] = @TOTAL_DEDUCTIONS_PROJ_MR_SHARE
    ,[ULT_LOSS_PROJ_MR_SHARE] = @ULT_LOSS_PROJ_MR_SHARE
    ,[TOTAL_PREM_PROJ_MR_SHARE_CYB] = @TOTAL_PREM_PROJ_MR_SHARE_CYB
    ,[TOTAL_DEDUCT_PROJ_MR_SHARE_CYB] = @TOTAL_DEDUCT_PROJ_MR_SHARE_CYB
    ,[ULT_LOSS_PROJ_MR_SHARE_CYB] = @ULT_LOSS_PROJ_MR_SHARE_CYB
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ PROGRAM_SUBSYSTEM_TREATY_ID, PRICING_TOOL, DWH_RI_CONTRACT_PERIOD_ID, PROGRAM_ID_OR_RIPP_ID, ADMIN_SYSTEM_RPP_ID, ISPROPERTYBUYBACK, PROGRAM_NAME, DWH_RI_COVERAGE_ID, TREATY_ID_OR_COVERAGE_ID, TREATY_NAME, FSRI_LINKAGES_FOR_TTY_BUSINESS, FMT_ID_FOR_FAB_BUSINESS, CLIENT_ID, CLIENT_NAME, BROKER_ID, BROKER_NAME, INSURED_ID, INSURED_NAME, RESULT_RESPONSIBILITY_ID, RESULT_RESPONSIBILITY_NAME, RESULT_RESPONSIBILITY_LVL_3, RESULT_RESPONSIBILITY_LVL_4, RESULT_RESPONSIBILITY_LVL_5, RESULT_RESPONSIBILITY_LVL_6, RESULT_RESP_PRESENTATION, UW_YEAR, BEGIN_DATE, END_DATE, UW_STATUS, NATURE_OF_TREATY, FAC_OR_TREATY, VIRTUAL_TREATY_FLAG, MR_SHARE, PROTECTED_SHARE, TOTAL_CYBER_COB_SHARE, ORIG_CURRENCY_ID, EXCHANGE_RATE, LIMIT_100_PCT, ATTACHMENT_POINT_100_PCT, EXP_TOTAL_PREMIUM_100_PCT, EXP_TOTAL_DEDUCTIONS_100_PCT, EXP_ULTIMATE_LOSS_100_PCT, EXP_TOTAL_PREMIUM_100P_CYB, EXP_TOTAL_DEDUCTIONS_100P_CYB, EXP_ULT_LOSS_100P_CYB, EXP_TOTAL_PREMIUM_MR_SHARE, EXP_TOTAL_DEDUCTIONS_MR_SHARE, EXP_ULT_LOSS_MR_SHARE, EXP_TOTAL_PREMIUM_MR_SHARE_CYB, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB, EXP_ULT_LOSS_MR_SHARE_CYB, ULTIMATE_LOSS_RATIO, TECHNICAL_COMBINED_RATIO, COMBINED_RATIO, EXP_PV_UW_RESLT_MR_SHARE, RORAC, IDENTITY_THEFT_SHARE, DATA_COMPROMISE_SHARE, CYBER_THIRD_PARTY_SHARE, CYBER_THIRD_PARTY_FI_SHARE, CYBER_THIRD_PARTY_COMM_SHARE, CYBER_FIRST_PARTY_SHARE, CYBER_FIRST_PARTY_PERS_SHARE, CYBER_INDUCED_PROP_DMG_SHARE, CYBER_MULTIPERIL_TPL_SHARE, MARINE_CYBER_THIRD_PARTY_SHARE, MARINE_CYBER_FIRST_PARTY_SHARE, PRICING_BUDGET_ITV, PRICING_BUDGET_DB, PRICING_BUDGET_IF, PRICING_BUDGET_CLOUD, TOTAL_PREMIUM_ASAT_100_PCT, TOTAL_DEDUCTIONS_ASAT_100_PCT, ULT_LOSS_ASAT_100_PCT, TOTAL_PREMIUM_ASAT_100P_CYB, TOTAL_DEDUCTIONS_ASAT_100P_CYB, ULT_LOSS_ASAT_100P_CYB, TOTAL_PREMIUM_ASAT_MR_SHARE, TOTAL_DEDUCTIONS_ASAT_MR_SHARE, ULT_LOSS_ASAT_MR_SHARE, TOTAL_PREM_ASAT_MR_SHARE_CYB, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB, ULT_LOSS_ASAT_MR_SHARE_CYB, TOTAL_PREMIUM_PROJ_100_PCT, TOTAL_DEDUCTIONS_PROJ_100_PCT, ULT_LOSS_PROJ_100_PCT, TOTAL_PREMIUM_PROJ_100P_CYB, TOTAL_DEDUCTIONS_PROJ_100P_CYB, ULT_LOSS_PROJ_100P_CYB, TOTAL_PREMIUM_PROJ_MR_SHARE, TOTAL_DEDUCTIONS_PROJ_MR_SHARE, ULT_LOSS_PROJ_MR_SHARE, TOTAL_PREM_PROJ_MR_SHARE_CYB, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB, ULT_LOSS_PROJ_MR_SHARE_CYB ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[pgo_data]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Dim_Portfolio_Tag:
        # table
        TableName = 'Dim_Portfolio_Tag'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Portfolio_Tag]'
        # columns
        Portfolio_Tag_ID = 'Portfolio_Tag_ID'
        Portfolio_Tag = 'Portfolio_Tag'
        Tag_Role_ID = 'Tag_Role_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Portfolio_Tag: str, Tag_Role_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag nvarchar(100) = ?
    ,@Tag_Role_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Portfolio_Tag] (
    [Portfolio_Tag]
    ,[Tag_Role_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Portfolio_Tag
    ,@Tag_Role_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag, Tag_Role_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Portfolio_Tag_ID: int, Portfolio_Tag: str, Tag_Role_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag_ID bigint = ?
    ,@Portfolio_Tag nvarchar(100) = ?
    ,@Tag_Role_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Portfolio_Tag] SET 
    [Portfolio_Tag] = @Portfolio_Tag
    ,[Tag_Role_ID] = @Tag_Role_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Portfolio_Tag_ID] = @Portfolio_Tag_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag_ID, Portfolio_Tag, Tag_Role_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Portfolio_Tag_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag_ID bigint = ?
;

DELETE [dbo].[Dim_Portfolio_Tag]
WHERE
    [Portfolio_Tag_ID] = @Portfolio_Tag_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag_ID ]).exec_df()

    class _13_Claims_Incurred_Band:
        # table
        TableName = '13_Claims_Incurred_Band'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[_13_Claims_Incurred_Band]'
        # columns
        Segmentation_Name = 'Segmentation_Name'
        Aggregated_Segmentation = 'Aggregated_Segmentation'
        Sort_Aggr = 'Sort_Aggr'
        Index = 'Index'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Index: int, Segmentation_Name: str = None, Aggregated_Segmentation: str = None, Sort_Aggr: int = None) -> DataFrame:
            sql = """
DECLARE
    @Segmentation_Name varchar(14) = ?
    ,@Aggregated_Segmentation varchar(14) = ?
    ,@Sort_Aggr tinyint = ?
    ,@Index tinyint = ?
;

INSERT INTO [dbo].[13_Claims_Incurred_Band] (
    [Segmentation_Name]
    ,[Aggregated_Segmentation]
    ,[Sort_Aggr]
    ,[Index]
)
VALUES (
    @Segmentation_Name
    ,@Aggregated_Segmentation
    ,@Sort_Aggr
    ,@Index
);
"""
            return DbCmd(self.cnOrStr, sql, [ Segmentation_Name, Aggregated_Segmentation, Sort_Aggr, Index ]).exec_df()

        def update(self, Index: int, Segmentation_Name: str = None, Aggregated_Segmentation: str = None, Sort_Aggr: int = None) -> DataFrame:
            sql = """
DECLARE
    @Segmentation_Name varchar(14) = ?
    ,@Aggregated_Segmentation varchar(14) = ?
    ,@Sort_Aggr tinyint = ?
    ,@Index tinyint = ?
;

UPDATE [dbo].[13_Claims_Incurred_Band] SET 
    [Segmentation_Name] = @Segmentation_Name
    ,[Aggregated_Segmentation] = @Aggregated_Segmentation
    ,[Sort_Aggr] = @Sort_Aggr
 WHERE
    [Index] = @Index
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Segmentation_Name, Aggregated_Segmentation, Sort_Aggr, Index ]).exec_df()

        def delete(self, Index: int) -> DataFrame:
            sql = """
DECLARE
    @Index tinyint = ?
;

DELETE [dbo].[13_Claims_Incurred_Band]
WHERE
    [Index] = @Index
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Index ]).exec_df()

    class Claim_Development:
        # table
        TableName = 'Claim_Development'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Claim_Development]'
        # columns
        Claim_Development_ID = 'Claim_Development_ID'
        Process_ID = 'Process_ID'
        Claim_ClientInfo_ID = 'Claim_ClientInfo_ID'
        Value_as_of_Date = 'Value_as_of_Date'
        Claim_Unified_Status_ID = 'Claim_Unified_Status_ID'
        Claim_Closed_Date = 'Claim_Closed_Date'
        Signal_Reserve_ID = 'Signal_Reserve_ID'
        Loss_Currency_ID = 'Loss_Currency_ID'
        Threshold_unindexed_Orig_Curr = 'Threshold_unindexed_Orig_Curr'
        Incurred_Insured_FGU_Orig_Curr = 'Incurred_Insured_FGU_Orig_Curr'
        Paid_Client_Share_Orig_Curr = 'Paid_Client_Share_Orig_Curr'
        Incurred_Client_Share_Orig_Curr = 'Incurred_Client_Share_Orig_Curr'
        fileId = 'fileId'
        fileName = 'fileName'
        sheetName = 'sheetName'
        rowNr = 'rowNr'
        DELETE_indicator = 'DELETE_indicator'
        Duplicate_ID = 'Duplicate_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Process_ID: int, rowNr: int, Claim_ClientInfo_ID: int = None, Value_as_of_Date: date = None, Claim_Unified_Status_ID: int = None, Claim_Closed_Date: date = None, Signal_Reserve_ID: int = None, Loss_Currency_ID: int = None, Threshold_unindexed_Orig_Curr: float = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Duplicate_ID: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Process_ID bigint = ?
    ,@Claim_ClientInfo_ID bigint = ?
    ,@Value_as_of_Date date(0) = ?
    ,@Claim_Unified_Status_ID bigint = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Signal_Reserve_ID bigint = ?
    ,@Loss_Currency_ID bigint = ?
    ,@Threshold_unindexed_Orig_Curr float = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Claim_Development] (
    [Process_ID]
    ,[Claim_ClientInfo_ID]
    ,[Value_as_of_Date]
    ,[Claim_Unified_Status_ID]
    ,[Claim_Closed_Date]
    ,[Signal_Reserve_ID]
    ,[Loss_Currency_ID]
    ,[Threshold_unindexed_Orig_Curr]
    ,[Incurred_Insured_FGU_Orig_Curr]
    ,[Paid_Client_Share_Orig_Curr]
    ,[Incurred_Client_Share_Orig_Curr]
    ,[fileId]
    ,[fileName]
    ,[sheetName]
    ,[rowNr]
    ,[DELETE_indicator]
    ,[Duplicate_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Process_ID
    ,@Claim_ClientInfo_ID
    ,@Value_as_of_Date
    ,@Claim_Unified_Status_ID
    ,@Claim_Closed_Date
    ,@Signal_Reserve_ID
    ,@Loss_Currency_ID
    ,@Threshold_unindexed_Orig_Curr
    ,@Incurred_Insured_FGU_Orig_Curr
    ,@Paid_Client_Share_Orig_Curr
    ,@Incurred_Client_Share_Orig_Curr
    ,@fileId
    ,@fileName
    ,@sheetName
    ,@rowNr
    ,@DELETE_indicator
    ,@Duplicate_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Process_ID, Claim_ClientInfo_ID, Value_as_of_Date, Claim_Unified_Status_ID, Claim_Closed_Date, Signal_Reserve_ID, Loss_Currency_ID, Threshold_unindexed_Orig_Curr, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, fileId, fileName, sheetName, rowNr, DELETE_indicator, Duplicate_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Claim_Development_ID: int, Process_ID: int, rowNr: int, Claim_ClientInfo_ID: int = None, Value_as_of_Date: date = None, Claim_Unified_Status_ID: int = None, Claim_Closed_Date: date = None, Signal_Reserve_ID: int = None, Loss_Currency_ID: int = None, Threshold_unindexed_Orig_Curr: float = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Duplicate_ID: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Claim_Development_ID bigint = ?
    ,@Process_ID bigint = ?
    ,@Claim_ClientInfo_ID bigint = ?
    ,@Value_as_of_Date date(0) = ?
    ,@Claim_Unified_Status_ID bigint = ?
    ,@Claim_Closed_Date date(0) = ?
    ,@Signal_Reserve_ID bigint = ?
    ,@Loss_Currency_ID bigint = ?
    ,@Threshold_unindexed_Orig_Curr float = ?
    ,@Incurred_Insured_FGU_Orig_Curr float = ?
    ,@Paid_Client_Share_Orig_Curr float = ?
    ,@Incurred_Client_Share_Orig_Curr float = ?
    ,@fileId bigint = ?
    ,@fileName nvarchar(1000) = ?
    ,@sheetName nvarchar(400) = ?
    ,@rowNr bigint = ?
    ,@DELETE_indicator nvarchar(2048) = ?
    ,@Duplicate_ID nvarchar(100) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Claim_Development] SET 
    [Process_ID] = @Process_ID
    ,[Claim_ClientInfo_ID] = @Claim_ClientInfo_ID
    ,[Value_as_of_Date] = @Value_as_of_Date
    ,[Claim_Unified_Status_ID] = @Claim_Unified_Status_ID
    ,[Claim_Closed_Date] = @Claim_Closed_Date
    ,[Signal_Reserve_ID] = @Signal_Reserve_ID
    ,[Loss_Currency_ID] = @Loss_Currency_ID
    ,[Threshold_unindexed_Orig_Curr] = @Threshold_unindexed_Orig_Curr
    ,[Incurred_Insured_FGU_Orig_Curr] = @Incurred_Insured_FGU_Orig_Curr
    ,[Paid_Client_Share_Orig_Curr] = @Paid_Client_Share_Orig_Curr
    ,[Incurred_Client_Share_Orig_Curr] = @Incurred_Client_Share_Orig_Curr
    ,[fileId] = @fileId
    ,[fileName] = @fileName
    ,[sheetName] = @sheetName
    ,[rowNr] = @rowNr
    ,[DELETE_indicator] = @DELETE_indicator
    ,[Duplicate_ID] = @Duplicate_ID
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Claim_Development_ID] = @Claim_Development_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_Development_ID, Process_ID, Claim_ClientInfo_ID, Value_as_of_Date, Claim_Unified_Status_ID, Claim_Closed_Date, Signal_Reserve_ID, Loss_Currency_ID, Threshold_unindexed_Orig_Curr, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, fileId, fileName, sheetName, rowNr, DELETE_indicator, Duplicate_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Claim_Development_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Claim_Development_ID bigint = ?
;

DELETE [dbo].[Claim_Development]
WHERE
    [Claim_Development_ID] = @Claim_Development_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Claim_Development_ID ]).exec_df()

    class Dim_Policy_Type:
        # table
        TableName = 'Dim_Policy_Type'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Dim_Policy_Type]'
        # columns
        Policy_Type_ID = 'Policy_Type_ID'
        Policy_Type = 'Policy_Type'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Policy_Type: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Policy_Type nvarchar(100) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Dim_Policy_Type] (
    [Policy_Type]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Policy_Type
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Policy_Type, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Policy_Type_ID: int, Policy_Type: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Policy_Type_ID bigint = ?
    ,@Policy_Type nvarchar(100) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Dim_Policy_Type] SET 
    [Policy_Type] = @Policy_Type
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Policy_Type_ID] = @Policy_Type_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Policy_Type_ID, Policy_Type, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Policy_Type_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Policy_Type_ID bigint = ?
;

DELETE [dbo].[Dim_Policy_Type]
WHERE
    [Policy_Type_ID] = @Policy_Type_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Policy_Type_ID ]).exec_df()

    class sysdiagrams:
        # table
        TableName = 'sysdiagrams'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[sysdiagrams]'
        # columns
        name = 'name'
        principal_id = 'principal_id'
        diagram_id = 'diagram_id'
        version = 'version'
        definition = 'definition'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, name: str, principal_id: int, version: int = None, definition: bytes = BinaryNull) -> DataFrame:
            sql = """
DECLARE
    @name sysname(256) = ?
    ,@principal_id int = ?
    ,@version int = ?
    ,@definition varbinary = ?
;

INSERT INTO [dbo].[sysdiagrams] (
    [name]
    ,[principal_id]
    ,[version]
    ,[definition]
)
VALUES (
    @name
    ,@principal_id
    ,@version
    ,@definition
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ name, principal_id, version, definition ]).exec_df()

        def update(self, name: str, principal_id: int, diagram_id: int, version: int = None, definition: bytes = BinaryNull) -> DataFrame:
            sql = """
DECLARE
    @name sysname(256) = ?
    ,@principal_id int = ?
    ,@diagram_id int = ?
    ,@version int = ?
    ,@definition varbinary = ?
;

UPDATE [dbo].[sysdiagrams] SET 
    [name] = @name
    ,[principal_id] = @principal_id
    ,[version] = @version
    ,[definition] = @definition
 WHERE
    [diagram_id] = @diagram_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ name, principal_id, diagram_id, version, definition ]).exec_df()

        def delete(self, diagram_id: int) -> DataFrame:
            sql = """
DECLARE
    @diagram_id int = ?
;

DELETE [dbo].[sysdiagrams]
WHERE
    [diagram_id] = @diagram_id
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ diagram_id ]).exec_df()

    class company_lookup_temp:
        # table
        TableName = 'company_lookup_temp'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[company_lookup_temp]'
        # columns
        company_id = 'company_id'
        duplicate_of_id = 'duplicate_of_id'
        Insured_Name_Clean = 'Insured_Name_Clean'
        Insured_Name_ClientInfo = 'Insured_Name_ClientInfo'
        Insured_Street = 'Insured_Street'
        Insured_City = 'Insured_City'
        Insured_ZIP_Code = 'Insured_ZIP_Code'
        Insured_State = 'Insured_State'
        Insured_Country_ISO2 = 'Insured_Country_ISO2'
        Trade_Level_Name_ClientInfo_Mapped_Cambridge = 'Trade_Level_Name_ClientInfo_Mapped_Cambridge'
        Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge = 'Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge'
        Insured_Homepage = 'Insured_Homepage'
        Turnover_ClientInfo_USD = 'Turnover_ClientInfo_USD'
        bvd_id = 'bvd_id'
        manual_curated_entry = 'manual_curated_entry'
        combined_entry = 'combined_entry'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, company_id: int = None, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, bvd_id: str = None, manual_curated_entry: int = None, combined_entry: int = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@duplicate_of_id bigint = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@bvd_id nvarchar(100) = ?
    ,@manual_curated_entry bit = ?
    ,@combined_entry bit = ?
;

INSERT INTO [dbo].[company_lookup_temp] (
    [company_id]
    ,[duplicate_of_id]
    ,[Insured_Name_Clean]
    ,[Insured_Name_ClientInfo]
    ,[Insured_Street]
    ,[Insured_City]
    ,[Insured_ZIP_Code]
    ,[Insured_State]
    ,[Insured_Country_ISO2]
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge]
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge]
    ,[Insured_Homepage]
    ,[Turnover_ClientInfo_USD]
    ,[bvd_id]
    ,[manual_curated_entry]
    ,[combined_entry]
)
VALUES (
    @company_id
    ,@duplicate_of_id
    ,@Insured_Name_Clean
    ,@Insured_Name_ClientInfo
    ,@Insured_Street
    ,@Insured_City
    ,@Insured_ZIP_Code
    ,@Insured_State
    ,@Insured_Country_ISO2
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,@Insured_Homepage
    ,@Turnover_ClientInfo_USD
    ,@bvd_id
    ,@manual_curated_entry
    ,@combined_entry
);
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, bvd_id, manual_curated_entry, combined_entry ]).exec_df()

        def update(self, company_id: int = None, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, bvd_id: str = None, manual_curated_entry: int = None, combined_entry: int = None) -> DataFrame:
            sql = """
DECLARE
    @company_id bigint = ?
    ,@duplicate_of_id bigint = ?
    ,@Insured_Name_Clean nvarchar(2048) = ?
    ,@Insured_Name_ClientInfo nvarchar(2048) = ?
    ,@Insured_Street nvarchar(1000) = ?
    ,@Insured_City nvarchar(400) = ?
    ,@Insured_ZIP_Code nvarchar(100) = ?
    ,@Insured_State nvarchar(400) = ?
    ,@Insured_Country_ISO2 nchar(4) = ?
    ,@Trade_Level_Name_ClientInfo_Mapped_Cambridge nvarchar(2048) = ?
    ,@Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge nvarchar(100) = ?
    ,@Insured_Homepage nvarchar(1000) = ?
    ,@Turnover_ClientInfo_USD float = ?
    ,@bvd_id nvarchar(100) = ?
    ,@manual_curated_entry bit = ?
    ,@combined_entry bit = ?
;

UPDATE [dbo].[company_lookup_temp] SET 
    [company_id] = @company_id
    ,[duplicate_of_id] = @duplicate_of_id
    ,[Insured_Name_Clean] = @Insured_Name_Clean
    ,[Insured_Name_ClientInfo] = @Insured_Name_ClientInfo
    ,[Insured_Street] = @Insured_Street
    ,[Insured_City] = @Insured_City
    ,[Insured_ZIP_Code] = @Insured_ZIP_Code
    ,[Insured_State] = @Insured_State
    ,[Insured_Country_ISO2] = @Insured_Country_ISO2
    ,[Trade_Level_Name_ClientInfo_Mapped_Cambridge] = @Trade_Level_Name_ClientInfo_Mapped_Cambridge
    ,[Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge] = @Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge
    ,[Insured_Homepage] = @Insured_Homepage
    ,[Turnover_ClientInfo_USD] = @Turnover_ClientInfo_USD
    ,[bvd_id] = @bvd_id
    ,[manual_curated_entry] = @manual_curated_entry
    ,[combined_entry] = @combined_entry
 WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ company_id, duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, bvd_id, manual_curated_entry, combined_entry ]).exec_df()

        def delete(self) -> DataFrame:
            sql = """
DECLARE
;

DELETE [dbo].[company_lookup_temp]
WHERE
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [  ]).exec_df()

    class Company_ClientInfo:
        # table
        TableName = 'Company_ClientInfo'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Company_ClientInfo]'
        # columns
        Company_ClientInfo_ID = 'Company_ClientInfo_ID'
        Company_ID = 'Company_ID'
        Company_Name_Clean_ID = 'Company_Name_Clean_ID'
        Company_Name_ClientInfo = 'Company_Name_ClientInfo'
        Country_ISO2_ID = 'Country_ISO2_ID'
        City_ClientInfo_ID = 'City_ClientInfo_ID'
        Industry_ClientInfo_ID = 'Industry_ClientInfo_ID'
        Street = 'Street'
        ZIP_Code = 'ZIP_Code'
        State_ID = 'State_ID'
        Domain_Name = 'Domain_Name'
        Source = 'Source'
        Company_Name_ClientInfo_Uncut = 'Company_Name_ClientInfo_Uncut'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Company_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ID bigint = ?
    ,@Company_Name_Clean_ID bigint = ?
    ,@Company_Name_ClientInfo nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_ClientInfo_ID bigint = ?
    ,@Industry_ClientInfo_ID bigint = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source nvarchar(2048) = ?
    ,@Company_Name_ClientInfo_Uncut nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Company_ClientInfo] (
    [Company_ID]
    ,[Company_Name_Clean_ID]
    ,[Company_Name_ClientInfo]
    ,[Country_ISO2_ID]
    ,[City_ClientInfo_ID]
    ,[Industry_ClientInfo_ID]
    ,[Street]
    ,[ZIP_Code]
    ,[State_ID]
    ,[Domain_Name]
    ,[Source]
    ,[Company_Name_ClientInfo_Uncut]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Company_ID
    ,@Company_Name_Clean_ID
    ,@Company_Name_ClientInfo
    ,@Country_ISO2_ID
    ,@City_ClientInfo_ID
    ,@Industry_ClientInfo_ID
    ,@Street
    ,@ZIP_Code
    ,@State_ID
    ,@Domain_Name
    ,@Source
    ,@Company_Name_ClientInfo_Uncut
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Company_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Company_ClientInfo_ID: int, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Company_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Company_ClientInfo_ID bigint = ?
    ,@Company_ID bigint = ?
    ,@Company_Name_Clean_ID bigint = ?
    ,@Company_Name_ClientInfo nvarchar(600) = ?
    ,@Country_ISO2_ID bigint = ?
    ,@City_ClientInfo_ID bigint = ?
    ,@Industry_ClientInfo_ID bigint = ?
    ,@Street nvarchar(400) = ?
    ,@ZIP_Code nvarchar(100) = ?
    ,@State_ID bigint = ?
    ,@Domain_Name nvarchar(400) = ?
    ,@Source nvarchar(2048) = ?
    ,@Company_Name_ClientInfo_Uncut nvarchar(2048) = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Company_ClientInfo] SET 
    [Company_ID] = @Company_ID
    ,[Company_Name_Clean_ID] = @Company_Name_Clean_ID
    ,[Company_Name_ClientInfo] = @Company_Name_ClientInfo
    ,[Country_ISO2_ID] = @Country_ISO2_ID
    ,[City_ClientInfo_ID] = @City_ClientInfo_ID
    ,[Industry_ClientInfo_ID] = @Industry_ClientInfo_ID
    ,[Street] = @Street
    ,[ZIP_Code] = @ZIP_Code
    ,[State_ID] = @State_ID
    ,[Domain_Name] = @Domain_Name
    ,[Source] = @Source
    ,[Company_Name_ClientInfo_Uncut] = @Company_Name_ClientInfo_Uncut
    ,[Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Company_ClientInfo_ID] = @Company_ClientInfo_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ClientInfo_ID, Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Company_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Company_ClientInfo_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Company_ClientInfo_ID bigint = ?
;

DELETE [dbo].[Company_ClientInfo]
WHERE
    [Company_ClientInfo_ID] = @Company_ClientInfo_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Company_ClientInfo_ID ]).exec_df()

    class Bridge_Portfolio_Tags:
        # table
        TableName = 'Bridge_Portfolio_Tags'
        SchemaName = 'dbo'
        QualifiedName = '[dbo].[Bridge_Portfolio_Tags]'
        # columns
        Portfolio_Tag_ID = 'Portfolio_Tag_ID'
        Exposure_ID = 'Exposure_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, Portfolio_Tag_ID: int, Exposure_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag_ID bigint = ?
    ,@Exposure_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

INSERT INTO [dbo].[Bridge_Portfolio_Tags] (
    [Portfolio_Tag_ID]
    ,[Exposure_ID]
    ,[Create_Time]
    ,[Change_Time]
    ,[Changed_By]
)
VALUES (
    @Portfolio_Tag_ID
    ,@Exposure_ID
    ,@Create_Time
    ,@Change_Time
    ,@Changed_By
);
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag_ID, Exposure_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def update(self, Portfolio_Tag_ID: int, Exposure_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag_ID bigint = ?
    ,@Exposure_ID bigint = ?
    ,@Create_Time datetime = ?
    ,@Change_Time datetime = ?
    ,@Changed_By nvarchar(400) = ?
;

UPDATE [dbo].[Bridge_Portfolio_Tags] SET 
    [Create_Time] = @Create_Time
    ,[Change_Time] = @Change_Time
    ,[Changed_By] = @Changed_By
 WHERE
    [Portfolio_Tag_ID] = @Portfolio_Tag_ID
    ,[Exposure_ID] = @Exposure_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag_ID, Exposure_ID, Create_Time, Change_Time, Changed_By ]).exec_df()

        def delete(self, Portfolio_Tag_ID: int, Exposure_ID: int) -> DataFrame:
            sql = """
DECLARE
    @Portfolio_Tag_ID bigint = ?
    ,@Exposure_ID bigint = ?
;

DELETE [dbo].[Bridge_Portfolio_Tags]
WHERE
    [Portfolio_Tag_ID] = @Portfolio_Tag_ID
    ,[Exposure_ID] = @Exposure_ID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ Portfolio_Tag_ID, Exposure_ID ]).exec_df()

