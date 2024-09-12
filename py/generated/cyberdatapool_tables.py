from mrtest import DbExec, Result
import pyodbc
from datetime import datetime, date, time
from uuid import UUID

class CyberDataPoolDboTables:
    class Dim_Claim_Status:
        # columns
        Claim_Status_ID = 'Claim_Status_ID'
        Claim_Status = 'Claim_Status'
        Claim_Unified_Status_ID = 'Claim_Unified_Status_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Claim_Status'
        def __repr__(self): return '[dbo].[Dim_Claim_Status]'

        def insert(self, Claim_Status: str, Claim_Unified_Status_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Claim_Status] ([Claim_Status], [Claim_Unified_Status_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Claim_Status, Claim_Unified_Status_ID, Create_Time, Change_Time, Changed_By ])


    class dnb:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'dnb'
        def __repr__(self): return '[dbo].[dnb]'

        def insert(self, dnb_id: str, name: str = None, previous_names: str = None, primary_address: str = None, primary_postcode: str = None, primary_city: str = None, continental_region: str = None, region: str = None, country: str = None, country_iso2code: str = None, phone: str = None, email_address: str = None, website: str = None, group_size: int = None, group_hierarchy_level: int = None, subsidiary_count: int = None, legal_identifiers: str = None, legal_identifier_types: str = None, trade_register_number: str = None, operating_status: str = None, operating_status_start_date: str = None, legal_form: str = None, legal_form_type: str = None, company_size: str = None, company_incorporation_date: str = None, company_start_date: str = None, naics2022_primary_code: str = None, ussic_primary_code: str = None, employees: int = None, employees_as_of_date: str = None, employees_data_reliability: str = None, revenue_usd: int = None, revenue_reporting_date: str = None, revenue_statement_duration_months: int = None, revenue_data_reliability: str = None, revenue_type: str = None, revenue_original_currency: str = None, revenue_original: int = None, ultimate_parent_company_mr_company_id: str = None, ultimate_parent_company_mr_company_name: str = None, ultimate_parent_company_country_iso2code: str = None, ultimate_parent_company_employees: int = None, ultimate_parent_company_revenue_usd: int = None, ultimate_parent_company_revenue_reporting_date: str = None, ultimate_parent_company_revenue_statement_duration_months: int = None, ultimate_parent_company_revenue_original_currency: str = None, ultimate_parent_company_revenue_original: int = None, direct_parent_mr_company_id: str = None, direct_parent_mr_company_name: str = None, direct_parent_country_iso2code: str = None, direct_parent_employees: int = None, direct_parent_revenue_usd: int = None, direct_parent_revenue_reporting_date: str = None, direct_parent_revenue_statement_duration_months: int = None, direct_parent_revenue_original_currency: str = None, direct_parent_revenue_original: int = None, aka_names: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None):
            sql = """INSERT INTO [dbo].[dnb] ([dnb_id], [name], [previous_names], [primary_address], [primary_postcode], [primary_city], [continental_region], [region], [country], [country_iso2code], [phone], [email_address], [website], [group_size], [group_hierarchy_level], [subsidiary_count], [legal_identifiers], [legal_identifier_types], [trade_register_number], [operating_status], [operating_status_start_date], [legal_form], [legal_form_type], [company_size], [company_incorporation_date], [company_start_date], [naics2022_primary_code], [ussic_primary_code], [employees], [employees_as_of_date], [employees_data_reliability], [revenue_usd], [revenue_reporting_date], [revenue_statement_duration_months], [revenue_data_reliability], [revenue_type], [revenue_original_currency], [revenue_original], [ultimate_parent_company_mr_company_id], [ultimate_parent_company_mr_company_name], [ultimate_parent_company_country_iso2code], [ultimate_parent_company_employees], [ultimate_parent_company_revenue_usd], [ultimate_parent_company_revenue_reporting_date], [ultimate_parent_company_revenue_statement_duration_months], [ultimate_parent_company_revenue_original_currency], [ultimate_parent_company_revenue_original], [direct_parent_mr_company_id], [direct_parent_mr_company_name], [direct_parent_country_iso2code], [direct_parent_employees], [direct_parent_revenue_usd], [direct_parent_revenue_reporting_date], [direct_parent_revenue_statement_duration_months], [direct_parent_revenue_original_currency], [direct_parent_revenue_original], [aka_names], [create_time], [change_time], [changed_by]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ dnb_id, name, previous_names, primary_address, primary_postcode, primary_city, continental_region, region, country, country_iso2code, phone, email_address, website, group_size, group_hierarchy_level, subsidiary_count, legal_identifiers, legal_identifier_types, trade_register_number, operating_status, operating_status_start_date, legal_form, legal_form_type, company_size, company_incorporation_date, company_start_date, naics2022_primary_code, ussic_primary_code, employees, employees_as_of_date, employees_data_reliability, revenue_usd, revenue_reporting_date, revenue_statement_duration_months, revenue_data_reliability, revenue_type, revenue_original_currency, revenue_original, ultimate_parent_company_mr_company_id, ultimate_parent_company_mr_company_name, ultimate_parent_company_country_iso2code, ultimate_parent_company_employees, ultimate_parent_company_revenue_usd, ultimate_parent_company_revenue_reporting_date, ultimate_parent_company_revenue_statement_duration_months, ultimate_parent_company_revenue_original_currency, ultimate_parent_company_revenue_original, direct_parent_mr_company_id, direct_parent_mr_company_name, direct_parent_country_iso2code, direct_parent_employees, direct_parent_revenue_usd, direct_parent_revenue_reporting_date, direct_parent_revenue_statement_duration_months, direct_parent_revenue_original_currency, direct_parent_revenue_original, aka_names, create_time, change_time, changed_by ])


    class Dim_BI_Waiting_Period:
        # columns
        BI_Waiting_Period_ID = 'BI_Waiting_Period_ID'
        BI_Waiting_Period = 'BI_Waiting_Period'
        Unit = 'Unit'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_BI_Waiting_Period'
        def __repr__(self): return '[dbo].[Dim_BI_Waiting_Period]'

        def insert(self, BI_Waiting_Period_ID: int, BI_Waiting_Period: str, Unit: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_BI_Waiting_Period] ([BI_Waiting_Period_ID], [BI_Waiting_Period], [Unit], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ BI_Waiting_Period_ID, BI_Waiting_Period, Unit, Create_Time, Change_Time, Changed_By ])


    class validation_criticality:
        # columns
        Rule_Name = 'Rule_Name'
        Column_Name = 'Column_Name'
        Criticality = 'Criticality'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'validation_criticality'
        def __repr__(self): return '[dbo].[validation_criticality]'

        def insert(self, Rule_Name: str, Column_Name: str, Criticality: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[validation_criticality] ([Rule_Name], [Column_Name], [Criticality], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Rule_Name, Column_Name, Criticality, Create_Time, Change_Time, Changed_By ])


    class FXRates:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'FXRates'
        def __repr__(self): return '[dbo].[FXRates]'

        def insert(self, rate_type: str, from_currency: str, to_currency: str, valid_from: date, exch_rate: float, valid_from_year: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[FXRates] ([rate_type], [from_currency], [to_currency], [valid_from], [exch_rate], [valid_from_year], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year, Create_Time, Change_Time, Changed_By ])


    class exposure2not_reinsured:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure2not_reinsured'
        def __repr__(self): return '[dbo].[exposure2not_reinsured]'

        def insert(self, ID_Arbeitsvorrat: str, exposure_id: int, reason_id: int, reason: str, ID_Arbeitsvorrat_MR_share: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[exposure2not_reinsured] ([ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [exposure_id], [reason_id], [reason], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, reason_id, reason, Create_Time, Change_Time, Changed_By ])


    class Dim_Loss_Event:
        # columns
        Loss_Event_ID = 'Loss_Event_ID'
        Loss_Event_ClientInfo = 'Loss_Event_ClientInfo'
        Loss_Event = 'Loss_Event'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Loss_Event'
        def __repr__(self): return '[dbo].[Dim_Loss_Event]'

        def insert(self, Loss_Event_ClientInfo: str, Loss_Event: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Loss_Event] ([Loss_Event_ClientInfo], [Loss_Event], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Loss_Event_ClientInfo, Loss_Event, Create_Time, Change_Time, Changed_By ])


    class data_requirements:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'data_requirements'
        def __repr__(self): return '[dbo].[data_requirements]'

        def insert(self, Table_Name: str, Column_Name: str, Version: str, Origin: str, Column_Type: str, Is_Mandatory: str, Dim_Table_Column: str = None, Default_Aggregation: str = None, Sort_Index: int = None, Column_Name_Normalized: str = None, Priority: int = None, Group_Id: int = None, Group_Name: str = None, Dashboard_Column_Label: str = None, Claims_Policy_Related: str = None):
            sql = """INSERT INTO [dbo].[data_requirements] ([Table_Name], [Column_Name], [Version], [Origin], [Column_Type], [Is_Mandatory], [Dim_Table_Column], [Default_Aggregation], [Sort_Index], [Column_Name_Normalized], [Priority], [Group_Id], [Group_Name], [Dashboard_Column_Label], [Claims_Policy_Related]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Table_Name, Column_Name, Version, Origin, Column_Type, Is_Mandatory, Dim_Table_Column, Default_Aggregation, Sort_Index, Column_Name_Normalized, Priority, Group_Id, Group_Name, Dashboard_Column_Label, Claims_Policy_Related ])


    class Company_History:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_History'
        def __repr__(self): return '[dbo].[Company_History]'

        def insert(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_History] ([Company_ID], [Company_Name_Clean], [Country_ISO2_ID], [City_Unified_Name_ID], [Industry_ID], [Company_Name], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Parent_Company_ID], [Ultimate_Company_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ])


    class Dim_City:
        # columns
        City_ID = 'City_ID'
        City = 'City'
        City_Unified_Name_ID = 'City_Unified_Name_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_City'
        def __repr__(self): return '[dbo].[Dim_City]'

        def insert(self, City: str, City_Unified_Name_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_City] ([City], [City_Unified_Name_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ City, City_Unified_Name_ID, Create_Time, Change_Time, Changed_By ])


    class exposure_bdx_test:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx_test'
        def __repr__(self): return '[dbo].[exposure_bdx_test]'

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None):
            sql = """INSERT INTO [dbo].[exposure_bdx_test] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_RestorationData], [Coverage_4_Sublimit_Reputation], [Coverage_5_Sublimit_Business_Interruption], [Coverage_6_Sublimit_CBI_IT_Service_Provider], [Coverage_7_Sublimit_CBI_Supply_Chain], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [Coverage_15_Sublimit_System_Failure], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Duplicate_ID], [ID_Arbeitsvorrat_MR_share], [Is_Special_Acceptance], [Client_Limit_Occ_Orig_Curr], [Full_Limit_Occ_Orig_Curr], [Policy_ID_Cleaned], [Client_Limit_Occ_USD], [Full_Limit_Occ_USD], [Insured_Name_Clean], [Company_ClientInfo_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ])


    class claims_cause:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_cause'
        def __repr__(self): return '[dbo].[claims_cause]'

        def insert(self, claim_id: int, claim_cause: str = None, classification_quality: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[claims_cause] ([claim_id], [claim_cause], [classification_quality], [classification_score], [claim_cause_alternatives], [internal_external], [intention], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ claim_id, claim_cause, classification_quality, classification_score, claim_cause_alternatives, internal_external, intention, Create_Time, Change_Time, Changed_By ])


    class FXRates_20210302:
        # columns
        rate_type = 'rate_type'
        from_currency = 'from_currency'
        to_currency = 'to_currency'
        valid_from = 'valid_from'
        exch_rate = 'exch_rate'
        valid_from_year = 'valid_from_year'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'FXRates_20210302'
        def __repr__(self): return '[dbo].[FXRates_20210302]'

        def insert(self, rate_type: str = None, from_currency: str = None, to_currency: str = None, valid_from: date = None, exch_rate: float = None, valid_from_year: float = None):
            sql = """INSERT INTO [dbo].[FXRates_20210302] ([rate_type], [from_currency], [to_currency], [valid_from], [exch_rate], [valid_from_year]) VALUES (?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ rate_type, from_currency, to_currency, valid_from, exch_rate, valid_from_year ])


    class exposure2mr_share:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure2mr_share'
        def __repr__(self): return '[dbo].[exposure2mr_share]'

        def insert(self, ID_Arbeitsvorrat: str, exposure_id: int, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, mr_share: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[exposure2mr_share] ([ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [exposure_id], [re_contract_id], [mr_share], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, exposure_id, re_contract_id, mr_share, Create_Time, Change_Time, Changed_By ])


    class data_entry_meta_information_20220530:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'data_entry_meta_information_20220530'
        def __repr__(self): return '[dbo].[data_entry_meta_information_20220530]'

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[data_entry_meta_information_20220530] ([id], [File_Name], [UW], [BU], [Number_Risks], [Number_Claims], [bdx_type], [As_at_Date], [Subsystem], [Program_IDs], [Additional_Program_IDs], [FSRI_ID], [Client_Name], [BuPa], [Treaty_Program_Name], [Begin_Date], [End_Date], [UW_Year], [Is_From_CU_Collection], [Is_Ignored], [ID_Arbeitsvorrat], [Comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ])


    class Dim_Product:
        # columns
        Product_ID = 'Product_ID'
        Technical_Client_ID = 'Technical_Client_ID'
        Product_Name = 'Product_Name'
        Product_Unified_Name = 'Product_Unified_Name'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Product'
        def __repr__(self): return '[dbo].[Dim_Product]'

        def insert(self, Technical_Client_ID: int, Product_Name: str, Product_Unified_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Product] ([Technical_Client_ID], [Product_Name], [Product_Unified_Name], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Technical_Client_ID, Product_Name, Product_Unified_Name, Create_Time, Change_Time, Changed_By ])


    class file_level_meta_information_20220530:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_meta_information_20220530'
        def __repr__(self): return '[dbo].[file_level_meta_information_20220530]'

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[file_level_meta_information_20220530] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [Subsystem_ID], [Treaty_Programm_ID], [FSRI_ID], [UW_Year], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [Priority], [Deadline], [Exclude_from_dashboards], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ])


    class Dim_Country:
        # columns
        Country_ID = 'Country_ID'
        Country = 'Country'
        Country_ISO2_Code = 'Country_ISO2_Code'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Country'
        def __repr__(self): return '[dbo].[Dim_Country]'

        def insert(self, Country: str, Country_ISO2_Code: str, Country_ISO2_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Country] ([Country], [Country_ISO2_Code], [Country_ISO2_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Country, Country_ISO2_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ])


    class Dim_Signal_Reserve:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Signal_Reserve'
        def __repr__(self): return '[dbo].[Dim_Signal_Reserve]'

        def insert(self, Technical_Client_ID: int, Reserve_Amount: float, Reserve_Meaning: str = None, Is_Signal_Reserve: int = None, Type_of_Reserve: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Signal_Reserve] ([Technical_Client_ID], [Reserve_Amount], [Reserve_Meaning], [Is_Signal_Reserve], [Type_of_Reserve], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Technical_Client_ID, Reserve_Amount, Reserve_Meaning, Is_Signal_Reserve, Type_of_Reserve, Create_Time, Change_Time, Changed_By ])


    class Client_Name:
        # columns
        Id = 'Id'
        Parent_Id = 'Parent_Id'
        Client = 'Client'
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Client_Name'
        def __repr__(self): return '[dbo].[Client_Name]'

        def insert(self, Parent_Id: int, Client: str, Hierarchy_Level_Id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Client_Name] ([Parent_Id], [Client], [Hierarchy_Level_Id], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Parent_Id, Client, Hierarchy_Level_Id, Create_Time, Change_Time, Changed_By ])


    class claims_cause_keywords:
        # columns
        id = 'id'
        cause_id = 'cause_id'
        claim_id = 'claim_id'
        key_word = 'key_word'
        score = 'score'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_cause_keywords'
        def __repr__(self): return '[dbo].[claims_cause_keywords]'

        def insert(self, cause_id: int, claim_id: int, key_word: str = None, score: float = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[claims_cause_keywords] ([cause_id], [claim_id], [key_word], [score], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ cause_id, claim_id, key_word, score, Create_Time, Change_Time, Changed_By ])


    class naics_cambridge:
        # columns
        id = 'id'
        naics_code = 'naics_code'
        cambridge_code = 'cambridge_code'
        cambridge_name = 'cambridge_name'
        create_time = 'create_time'
        change_time = 'change_time'
        changed_by = 'changed_by'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'naics_cambridge'
        def __repr__(self): return '[dbo].[naics_cambridge]'

        def insert(self, naics_code: str = None, cambridge_code: str = None, cambridge_name: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None):
            sql = """INSERT INTO [dbo].[naics_cambridge] ([naics_code], [cambridge_code], [cambridge_name], [create_time], [change_time], [changed_by]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ naics_code, cambridge_code, cambridge_name, create_time, change_time, changed_by ])


    class tDimFileMetaInfo:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimFileMetaInfo'
        def __repr__(self): return '[dbo].[tDimFileMetaInfo]'

        def insert(self, ID_Arbeitsvorrat: str, BU: str, Client_Name: str, Orig_File_Name: str, bdx_type: str, Smart_matching_done: int, R_code_done: int, Q_A_done: int, Function_ran: int, Validation_issues_done: int, Four_Eye_Check_done: int, Signoff_done: int, Contact_Data_Team: str, Responsible_Four_Eye_Check: str, UW: str, PML_needs_to_be_done: int, PML_has_been_done: int, SRAC_needs_to_be_done: int, SRAC_has_been_done: int, runScript: int, runNorm: int, runBvD: int, runMRShares: int, runValidation: int, runClaimsLinking: int, Priority: str, Deadline: date, Exclude_from_dashboards: int, UW_Year: int, Count_Missing_Priority_1: int, Count_Missing_Priority_2: int, Count_Missing_Priority_3: int, Count_OK_Priority_3: int, completeness_traffic_light: str = None):
            sql = """INSERT INTO [dbo].[tDimFileMetaInfo] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [runScript], [runNorm], [runBvD], [runMRShares], [runValidation], [runClaimsLinking], [Priority], [Deadline], [Exclude_from_dashboards], [UW_Year], [Count_Missing_Priority_1], [Count_Missing_Priority_2], [Count_Missing_Priority_3], [Count_OK_Priority_3], [completeness_traffic_light]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, Priority, Deadline, Exclude_from_dashboards, UW_Year, Count_Missing_Priority_1, Count_Missing_Priority_2, Count_Missing_Priority_3, Count_OK_Priority_3, completeness_traffic_light ])


    class file_level_test:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_test'
        def __repr__(self): return '[dbo].[file_level_test]'

        def insert(self, ID_Arbeitsvorrat: str, Smart_matching_done: str, R_code_done: str, Q_A_done: str, Function_ran: str, Validation_issues_done: str, Four_Eye_Check_done: str, Signoff_done: str, PML_needs_to_be_done: str, PML_has_been_done: str, SRAC_needs_to_be_done: str, SRAC_has_been_done: str, BU: str = None, Client_Name: str = None, Orig_File_Name: str = None, bdx_type: str = None, FSRI_ID: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, Deadline: datetime = None, Exclude_from_dashboards: int = None):
            sql = """INSERT INTO [dbo].[file_level_test] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [FSRI_ID], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [Deadline], [Exclude_from_dashboards]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ])


    class exposure_bdx_1607:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx_1607'
        def __repr__(self): return '[dbo].[exposure_bdx_1607]'

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None):
            sql = """INSERT INTO [dbo].[exposure_bdx_1607] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_RestorationData], [Coverage_4_Sublimit_Reputation], [Coverage_5_Sublimit_Business_Interruption], [Coverage_6_Sublimit_CBI_IT_Service_Provider], [Coverage_7_Sublimit_CBI_Supply_Chain], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [Coverage_15_Sublimit_System_Failure], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Duplicate_ID], [ID_Arbeitsvorrat_MR_share], [Is_Special_Acceptance], [Client_Limit_Occ_Orig_Curr], [Full_Limit_Occ_Orig_Curr], [Policy_ID_Cleaned], [Client_Limit_Occ_USD], [Full_Limit_Occ_USD], [Insured_Name_Clean], [Company_ClientInfo_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID ])


    class file_level_test_table:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_test_table'
        def __repr__(self): return '[dbo].[file_level_test_table]'

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, FSRI_ID: str = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Deadline: datetime = None):
            sql = """INSERT INTO [dbo].[file_level_test_table] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [FSRI_ID], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [Deadline], [Exclude_from_dashboards]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, FSRI_ID, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Deadline, Exclude_from_dashboards ])


    class Dim_Currency:
        # columns
        Currency_ID = 'Currency_ID'
        Currency = 'Currency'
        Is_Reporting_Currency = 'Is_Reporting_Currency'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Currency'
        def __repr__(self): return '[dbo].[Dim_Currency]'

        def insert(self, Currency: str, Is_Reporting_Currency: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Currency] ([Currency], [Is_Reporting_Currency], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Currency, Is_Reporting_Currency, Create_Time, Change_Time, Changed_By ])


    class re_modifications:
        # columns
        id = 'id'
        re_contract_id = 're_contract_id'
        modified_field = 'modified_field'
        previous_value = 'previous_value'
        modified_value = 'modified_value'
        modification_time = 'modification_time'
        changed_by = 'changed_by'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 're_modifications'
        def __repr__(self): return '[dbo].[re_modifications]'

        def insert(self, re_contract_id: int, modified_field: str, previous_value: str = None, modified_value: str = None, modification_time: datetime = None, changed_by: str = None):
            sql = """INSERT INTO [dbo].[re_modifications] ([re_contract_id], [modified_field], [previous_value], [modified_value], [modification_time], [changed_by]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ re_contract_id, modified_field, previous_value, modified_value, modification_time, changed_by ])


    class logging_statements:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'logging_statements'
        def __repr__(self): return '[dbo].[logging_statements]'

        def insert(self, ID_Arbeitsvorrat: str, bdx_type: str, log_type: str, processing_step: str, ID_Arbeitsvorrat_MR_share: str = None, issue: str = None, log_message: str = None, comment: str = None, number_of_issues: int = None, reference_value: int = None, warning_time: datetime = None, warned_user: str = None):
            sql = """INSERT INTO [dbo].[logging_statements] ([ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [bdx_type], [log_type], [processing_step], [issue], [log_message], [comment], [number_of_issues], [reference_value], [warning_time], [warned_user]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, bdx_type, log_type, processing_step, issue, log_message, comment, number_of_issues, reference_value, warning_time, warned_user ])


    class dnb_matching:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'dnb_matching'
        def __repr__(self): return '[dbo].[dnb_matching]'

        def insert(self, company_id: int, dnb_id: str, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, manual_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[dnb_matching] ([company_id], [dnb_id], [api_score], [match_settings], [matching_api], [version_of_match_algorithm], [string_match_score_internal_dnb_name], [string_match_score_internal_matchedvalue], [matched_value], [matched_value_source], [CDT_score], [best_match], [manual_match], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, manual_match, Create_Time, Change_Time, Changed_By ])


    class exposure_columns:
        # columns
        id = 'id'
        name = 'name'
        type = 'type'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_columns'
        def __repr__(self): return '[dbo].[exposure_columns]'

        def insert(self, id: int, name: str, type: str = None):
            sql = """INSERT INTO [dbo].[exposure_columns] ([id], [name], [type]) VALUES (?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, name, type ])


    class internal_policy_ids:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'internal_policy_ids'
        def __repr__(self): return '[dbo].[internal_policy_ids]'

        def insert(self, id: int, ID_Arbeitsvorrat: str, Internal_Policy_ID_1: int = None, Internal_Policy_ID_2: int = None, renewed_flag1: int = None, renewed_flag2: int = None, cancelled_flag1: int = None, cancelled_flag2: int = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None):
            sql = """INSERT INTO [dbo].[internal_policy_ids] ([id], [ID_Arbeitsvorrat], [Internal_Policy_ID_1], [Internal_Policy_ID_2], [renewed_flag1], [renewed_flag2], [cancelled_flag1], [cancelled_flag2], [create_time], [change_time], [changed_by]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, Internal_Policy_ID_1, Internal_Policy_ID_2, renewed_flag1, renewed_flag2, cancelled_flag1, cancelled_flag2, create_time, change_time, changed_by ])


    class exposure_bdx_copy:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx_copy'
        def __repr__(self): return '[dbo].[exposure_bdx_copy]'

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None):
            sql = """INSERT INTO [dbo].[exposure_bdx_copy] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_RestorationData], [Coverage_4_Sublimit_Reputation], [Coverage_5_Sublimit_Business_Interruption], [Coverage_6_Sublimit_CBI_IT_Service_Provider], [Coverage_7_Sublimit_CBI_Supply_Chain], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [Coverage_15_Sublimit_System_Failure], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [BvD_ID], [Duplicate_ID], [ID_Arbeitsvorrat_MR_share], [Is_Special_Acceptance], [Client_Limit_Occ_Orig_Curr], [Full_Limit_Occ_Orig_Curr], [Policy_ID_Cleaned]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned ])


    class exposure_extended:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_extended'
        def __repr__(self): return '[dbo].[exposure_extended]'

        def insert(self, id: int, ID_Arbeitsvorrat: str = None, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, Trade_Level_Name_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, MR_Share: float = None, Attachment_Band: int = None, Client_Limit_Band: int = None, Full_Limit_Band: int = None, Premium_Band: int = None, SIR_Band: int = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Abs_Dev_Perc_Bands: float = None, Turnover_USD_Combined: float = None, Company_Segment_Index: int = None, Country_Combined: str = None, MR_Limit_Band: int = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Policy_Expiry_Year: int = None, Policy_Inception_Month: int = None, Policy_Inception_Year: int = None, Policy_Inception_Year_Month: str = None, PrimaryOrExcess: str = None):
            sql = """INSERT INTO [dbo].[exposure_extended] ([id], [ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Turnover_Year_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [PII_Records_Stored], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Country_ISO2], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd], [Coverage_4_Sublimit_RestorationData], [Coverage_5_Sublimit_Reputation], [Coverage_6_Sublimit_Business_Interruption], [Coverage_7_Sublimit_Contingent_Business_Interruption], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [BvD_ID], [Risk_ID], [Tower_ID], [Duplicate_ID], [Insured_Name_BvD], [name_alias_bvd], [name_alias_source_bvd], [addr_internat_bvd], [ambest_id_bvd], [branch_count_bvd], [Trade_Level_CodeNumber_Mapped_Cambridge_BvD], [Trade_Level_Name_Mapped_Cambridge_BvD], [category_of_company_bvd], [city_internat_bvd], [corporate_group_size_bvd], [country_bvd], [county_bvd], [Country_ISO2_BvD], [direct_parent_bvdid_bvd], [direct_parent_name_internat_bvd], [ein_bvd], [email_bvd], [employees_bvd], [eurovat_bvd], [hierarchy_level_bvd], [inactive_bvd], [incorporation_date_bvd], [legalfrm_bvd], [lei_lei_bvd], [listed_bvd], [mainexch_bvd], [naicsccod2017_bvd], [phone_bvd], [postcode_bvd], [previous_names_set_array_bvd], [sd_isin_bvd], [sd_ticker_bvd], [slegalf_bvd], [state_us_bvd], [subs_count_bvd], [traderegisternr_bvd], [Turnover_EUR_BvD], [Turnover_USD_BvD], [type_of_entity_bvd], [ultimate_parent_bvdid_bvd], [ultimate_parent_ctryiso_bvd], [ultimate_parent_name_bvd], [ussicccod_bvd], [vatnumber_bvd], [website_bvd], [Trade_Level_CodeNumber_Mapped_Cambridge], [Trade_Level_Name_Mapped_Cambridge], [MR_Limit_USD], [MR_Premium_USD], [MR_GrossNet_Premium_USD], [MR_Share], [Attachment_Band], [Client_Limit_Band], [Full_Limit_Band], [Premium_Band], [SIR_Band], [Turnover_Deviation_BvD], [Turnover_Dev_as_perc_of_Client_Turnover], [Abs_Dev_perc], [Abs_Dev_Perc_Bands], [Turnover_USD_Combined], [Company_Segment_Index], [Country_Combined], [MR_Limit_Band], [Policy_Duration_Day], [Policy_Duration_Month], [Policy_Expiry_Year], [Policy_Inception_Month], [Policy_Inception_Year], [Policy_Inception_Year_Month], [PrimaryOrExcess]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Trade_Level_CodeNumber_Mapped_Cambridge, Trade_Level_Name_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, MR_Share, Attachment_Band, Client_Limit_Band, Full_Limit_Band, Premium_Band, SIR_Band, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Abs_Dev_Perc_Bands, Turnover_USD_Combined, Company_Segment_Index, Country_Combined, MR_Limit_Band, Policy_Duration_Day, Policy_Duration_Month, Policy_Expiry_Year, Policy_Inception_Month, Policy_Inception_Year, Policy_Inception_Year_Month, PrimaryOrExcess ])


    class Client_Name_Level:
        # columns
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Purpose = 'Purpose'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Client_Name_Level'
        def __repr__(self): return '[dbo].[Client_Name_Level]'

        def insert(self, Hierarchy_Level_Id: int, Purpose: str):
            sql = """INSERT INTO [dbo].[Client_Name_Level] ([Hierarchy_Level_Id], [Purpose]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Hierarchy_Level_Id, Purpose ])


    class issues_file_level_meta_information:
        # columns
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Column = 'Column'
        Value = 'Value'
        Criticality = 'Criticality'
        Comment = 'Comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'issues_file_level_meta_information'
        def __repr__(self): return '[dbo].[issues_file_level_meta_information]'

        def insert(self, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[issues_file_level_meta_information] ([ID_Arbeitsvorrat], [Column], [Value], [Criticality], [Comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ])


    class Dim_Client_Name:
        # columns
        Client_ID = 'Client_ID'
        Client_Name = 'Client_Name'
        Hierarchy_Level_ID = 'Hierarchy_Level_ID'
        Parent_ID = 'Parent_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Client_Name'
        def __repr__(self): return '[dbo].[Dim_Client_Name]'

        def insert(self, Client_Name: str, Hierarchy_Level_ID: int, Parent_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Client_Name] ([Client_Name], [Hierarchy_Level_ID], [Parent_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Client_Name, Hierarchy_Level_ID, Parent_ID, Create_Time, Change_Time, Changed_By ])


    class data_entry_meta_information_tbl:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'data_entry_meta_information_tbl'
        def __repr__(self): return '[dbo].[data_entry_meta_information_tbl]'

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[data_entry_meta_information_tbl] ([id], [File_Name], [UW], [BU], [Number_Risks], [Number_Claims], [bdx_type], [As_at_Date], [Subsystem], [Program_IDs], [Additional_Program_IDs], [FSRI_ID], [Client_Name], [BuPa], [Treaty_Program_Name], [Begin_Date], [End_Date], [UW_Year], [Is_From_CU_Collection], [Is_Ignored], [ID_Arbeitsvorrat], [Comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ])


    class Company_ClientInfo_save:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_ClientInfo_save'
        def __repr__(self): return '[dbo].[Company_ClientInfo_save]'

        def insert(self, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_ClientInfo_save] ([Company_ID], [Company_Name_Clean_ID], [Company_Name_ClientInfo], [Country_ISO2_ID], [City_ClientInfo_ID], [Industry_ClientInfo_ID], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Create_Time, Change_Time, Changed_By ])


    class Dim_State:
        # columns
        State_ID = 'State_ID'
        State = 'State'
        State_Code = 'State_Code'
        Country_ISO2_ID = 'Country_ISO2_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_State'
        def __repr__(self): return '[dbo].[Dim_State]'

        def insert(self, State: str, State_Code: str, Country_ISO2_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_State] ([State], [State_Code], [Country_ISO2_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ State, State_Code, Country_ISO2_ID, Create_Time, Change_Time, Changed_By ])


    class Company_save:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_save'
        def __repr__(self): return '[dbo].[Company_save]'

        def insert(self, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_save] ([Company_Name_Clean], [Country_ISO2_ID], [City_Unified_Name_ID], [Industry_ID], [Company_Name], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Parent_Company_ID], [Ultimate_Company_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ])


    class claims_bdx_copy:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx_copy'
        def __repr__(self): return '[dbo].[claims_bdx_copy]'

        def insert(self, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None):
            sql = """INSERT INTO [dbo].[claims_bdx_copy] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Claim_ID_ClientInfo], [Is_Claim_Closed], [Date_of_Incident], [Date_of_Notification], [Claims_Description], [Type_of_Loss], [Country_of_Loss_Settlement], [Value_as_Of_Date], [Loss_Currency], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [Threshold_Orig_Curr_unind], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [IsCensored], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Incurred_Insured_FGU_USD], [Paid_Client_Share_USD], [Incurred_Client_Share_USD], [Threshold_unind_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Country_of_Loss_Settlement_ISO2], [BvD_ID], [Duplicate_ID], [Internal_Claim_ID], [Is_Signal_Reserve], [Loss_Event_ClientInfo], [Loss_Event_normalized], [Product_Name_ClientInfo], [Claim_Closed_Date], [Policy_ID_Cleaned]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned ])


    class Company_History_save:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_History_save'
        def __repr__(self): return '[dbo].[Company_History_save]'

        def insert(self, Company_ID: int, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_History_save] ([Company_ID], [Company_Name_Clean], [Country_ISO2_ID], [City_Unified_Name_ID], [Industry_ID], [Company_Name], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Parent_Company_ID], [Ultimate_Company_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_ID, Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ])


    class file_level_meta_information_tbl:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_meta_information_tbl'
        def __repr__(self): return '[dbo].[file_level_meta_information_tbl]'

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[file_level_meta_information_tbl] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [Subsystem_ID], [Treaty_Programm_ID], [FSRI_ID], [UW_Year], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [Priority], [Deadline], [Exclude_from_dashboards], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ])


    class dnb_matching_save:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'dnb_matching_save'
        def __repr__(self): return '[dbo].[dnb_matching_save]'

        def insert(self, company_id: int, dnb_id: str, manual_match: int, api_score: float = None, match_settings: str = None, matching_api: str = None, version_of_match_algorithm: str = None, string_match_score_internal_dnb_name: float = None, string_match_score_internal_matchedvalue: float = None, matched_value: str = None, matched_value_source: str = None, CDT_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[dnb_matching_save] ([company_id], [dnb_id], [api_score], [match_settings], [matching_api], [version_of_match_algorithm], [string_match_score_internal_dnb_name], [string_match_score_internal_matchedvalue], [matched_value], [matched_value_source], [CDT_score], [best_match], [Create_Time], [Change_Time], [Changed_By], [manual_match]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ company_id, dnb_id, api_score, match_settings, matching_api, version_of_match_algorithm, string_match_score_internal_dnb_name, string_match_score_internal_matchedvalue, matched_value, matched_value_source, CDT_score, best_match, Create_Time, Change_Time, Changed_By, manual_match ])


    class claims_bdx_claims_linking:
        # columns
        id = 'id'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx_claims_linking'
        def __repr__(self): return '[dbo].[claims_bdx_claims_linking]'

        def insert(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[claims_bdx_claims_linking] ([id], [Policy_ID_Cleaned], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ])


    class Dim_Company_Name_Clean:
        # columns
        Company_Name_Clean_ID = 'Company_Name_Clean_ID'
        Company_Name_ClientInfo = 'Company_Name_ClientInfo'
        Company_Name_Clean = 'Company_Name_Clean'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Company_Name_Clean'
        def __repr__(self): return '[dbo].[Dim_Company_Name_Clean]'

        def insert(self, Company_Name_ClientInfo: str, Company_Name_Clean: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Company_Name_Clean] ([Company_Name_ClientInfo], [Company_Name_Clean], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_Name_ClientInfo, Company_Name_Clean, Create_Time, Change_Time, Changed_By ])


    class Process_Client_Name:
        # columns
        Process_ID = 'Process_ID'
        Business_Client_ID = 'Business_Client_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Process_Client_Name'
        def __repr__(self): return '[dbo].[Process_Client_Name]'

        def insert(self, Process_ID: int, Business_Client_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Process_Client_Name] ([Process_ID], [Business_Client_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Process_ID, Business_Client_ID, Create_Time, Change_Time, Changed_By ])


    class naics_codes:
        # columns
        code = 'code'
        title = 'title'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'naics_codes'
        def __repr__(self): return '[dbo].[naics_codes]'

        def insert(self, code: str, title: str):
            sql = """INSERT INTO [dbo].[naics_codes] ([code], [title]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ code, title ])


    class Dim_Client_Name_Hierarchy_Level:
        # columns
        Hierarchy_Level_ID = 'Hierarchy_Level_ID'
        Purpose = 'Purpose'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Client_Name_Hierarchy_Level'
        def __repr__(self): return '[dbo].[Dim_Client_Name_Hierarchy_Level]'

        def insert(self, Purpose: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Client_Name_Hierarchy_Level] ([Purpose], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Purpose, Create_Time, Change_Time, Changed_By ])


    class exposure_bdx_claims_linking:
        # columns
        id = 'id'
        Policy_ID_Cleaned = 'Policy_ID_Cleaned'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx_claims_linking'
        def __repr__(self): return '[dbo].[exposure_bdx_claims_linking]'

        def insert(self, id: int, Policy_ID_Cleaned: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[exposure_bdx_claims_linking] ([id], [Policy_ID_Cleaned], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, Policy_ID_Cleaned, Create_Time, Change_Time, Changed_By ])


    class Dim_FXRates:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_FXRates'
        def __repr__(self): return '[dbo].[Dim_FXRates]'

        def insert(self, from_currency_ID: int, to_currency_ID: int, valid_from_year: int, from_currency: str, to_currency: str, exch_rate: float, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_FXRates] ([from_currency_ID], [to_currency_ID], [valid_from_year], [from_currency], [to_currency], [exch_rate], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ from_currency_ID, to_currency_ID, valid_from_year, from_currency, to_currency, exch_rate, Create_Time, Change_Time, Changed_By ])


    class validationResults_exposure:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'validationResults_exposure'
        def __repr__(self): return '[dbo].[validationResults_exposure]'

        def insert(self, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[validationResults_exposure] ([ref_id], [ID_Arbeitsvorrat], [RuleName], [Issue], [Column], [Column2], [Column3], [rowNr], [File], [Value], [Value2], [Value3], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ])


    class claims_linking_info:
        # columns
        id = 'id'
        score = 'score'
        type_of_match = 'type_of_match'
        comment = 'comment'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_linking_info'
        def __repr__(self): return '[dbo].[claims_linking_info]'

        def insert(self, id: int, type_of_match: str, score: float = None, comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[claims_linking_info] ([id], [score], [type_of_match], [comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, score, type_of_match, comment, Create_Time, Change_Time, Changed_By ])


    class tFactClaims:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tFactClaims'
        def __repr__(self): return '[dbo].[tFactClaims]'

        def insert(self, id: int, ID_Arbeitsvorrat: str, Client: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, LossDateKey: int, rowNr: int, CountryKey: int, QualityKey: int, DateOfInceptionCombinedKey: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Date_of_Loss: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: int = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: date = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, IsCensored: int = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Paid_Client_Share_USD_Combined: float = None, ClaimsIncurredBandsKey: int = None, Incurred_Client_Share_USD: float = None, ClaimsIncurredCombinedBandsKey: int = None, Incurred_Client_Share_USD_Combined: float = None, ClaimsThresholdCategoriesKey: int = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Country_Combined: str = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, CompanySegmentKey: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, Bvd_Country_Check_claims: str = None, Bvd_Industry_Check_claims: str = None, Combined_NotificationDate_IncidentDate: date = None, Claims_Time_btw_UWDate_IncNot_inQuarter: int = None, Claims_Time_btw_UWDate_IncNot_inMonth: int = None, Claims_Time_btw_UWDate_IncNot_inDay: int = None, Reportingtime_inMonths: int = None, Reportingtime_inDays: int = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, final_flag: int = None, CambridgeKey: int = None, Loss_Development_Quarter: int = None, Trade_Level_Name_Mapped_Cambridge: str = None, Is_Deleted: str = None, Is_Signal_Reserve: int = None, claim_cause: str = None, classification_score: float = None, claim_cause_alternatives: str = None, internal_external: str = None, intention: str = None, risk_id: int = None, is_in_overlap_time_period: int = None, ID_Arbeitsvorrat_Exposure: str = None, Client_Exposure: str = None, Insured_Name_ClientInfo_Exposure: str = None, Policy_ID_ClientInfo_Exposure: str = None, Policy_Inception_Date_Exposure: date = None, Policy_Expiry_Date_Exposure: date = None, Coverage: str = None, Currency_Exposure: str = None, Client_Limit_USD_Exposure: float = None, Full_Limit_USD_Exposure: float = None, Attachment_USD_Exposure: float = None, Client_Premium_USD: float = None, Combined_Premium_USD: float = None, Client_Premium_Orig_Curr: float = None, Combined_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_USD: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit_Exposure: float = None, SIR_USD_Exposure: float = None, CountryExposureKey: int = None, CambridgeExposureKey: int = None, CompanySegmentExposureKey: int = None, Turnover_USD_BvD_Exposure: float = None, Duplicate_ID_Exposure: str = None, CountryCombinedKey: int = None, CambridgeCombinedKey: int = None, CompanySegmentCombinedKey: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, Calculated_or_ClientInfo_FGU_USD: float = None):
            sql = """INSERT INTO [dbo].[tFactClaims] ([id], [ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [PolicyInceptionDateKey], [Policy_Inception_Date], [PolicyExpiryDateKey], [Policy_Expiry_Date], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [FSRI_Treaty], [FSRI_Section], [FSRI_Period], [Claim_ID_ClientInfo], [Is_Claim_Closed], [Date_of_Incident], [Date_of_Notification], [LossDateKey], [Date_of_Loss], [Claims_Description], [Type_of_Loss], [Country_of_Loss_Settlement], [Nr_Affected_Records], [Duration_of_Outage_hours], [Value_as_Of_Date], [Loss_Currency], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [Threshold_Orig_Curr_unind], [Observation_Period_Start], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [IsCensored], [ClientLimitBandKey], [Client_Limit_USD], [FullLimitBandKey], [Full_Limit_USD], [AttachmentBandKey], [Attachment_USD], [SIRBandsKey], [SIR_USD], [Incurred_Insured_FGU_USD], [Paid_Client_Share_USD], [Paid_Client_Share_USD_Combined], [ClaimsIncurredBandsKey], [Incurred_Client_Share_USD], [ClaimsIncurredCombinedBandsKey], [Incurred_Client_Share_USD_Combined], [ClaimsThresholdCategoriesKey], [Threshold_unind_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [CountryKey], [Insured_Country_ISO2], [Country_of_Loss_Settlement_ISO2], [BvD_ID], [Duplicate_ID], [Internal_Claim_ID], [Country_Combined], [FileMetaInfoKey], [Insured_Name_BvD], [name_alias_bvd], [name_alias_source_bvd], [addr_internat_bvd], [ambest_id_bvd], [branch_count_bvd], [Trade_Level_CodeNumber_Mapped_Cambridge_BvD], [Trade_Level_Name_Mapped_Cambridge_BvD], [category_of_company_bvd], [city_internat_bvd], [corporate_group_size_bvd], [country_bvd], [county_bvd], [Country_ISO2_BvD], [direct_parent_bvdid_bvd], [direct_parent_name_internat_bvd], [ein_bvd], [email_bvd], [employees_bvd], [eurovat_bvd], [hierarchy_level_bvd], [inactive_bvd], [incorporation_date_bvd], [legalfrm_bvd], [lei_lei_bvd], [listed_bvd], [mainexch_bvd], [naicsccod2017_bvd], [phone_bvd], [postcode_bvd], [previous_names_set_array_bvd], [sd_isin_bvd], [sd_ticker_bvd], [slegalf_bvd], [state_us_bvd], [subs_count_bvd], [traderegisternr_bvd], [Turnover_EUR_BvD], [CompanySegmentKey], [Turnover_USD_BvD], [type_of_entity_bvd], [ultimate_parent_bvdid_bvd], [ultimate_parent_ctryiso_bvd], [ultimate_parent_name_bvd], [ussicccod_bvd], [vatnumber_bvd], [website_bvd], [Bvd_Country_Check_claims], [Bvd_Industry_Check_claims], [Combined_NotificationDate_IncidentDate], [Claims_Time_btw_UWDate_IncNot_inQuarter], [Claims_Time_btw_UWDate_IncNot_inMonth], [Claims_Time_btw_UWDate_IncNot_inDay], [Reportingtime_inMonths], [Reportingtime_inDays], [Trade_Level_CodeNumber_Mapped_Cambridge], [final_flag], [CambridgeKey], [Loss_Development_Quarter], [Trade_Level_Name_Mapped_Cambridge], [Is_Deleted], [Is_Signal_Reserve], [claim_cause], [QualityKey], [classification_score], [claim_cause_alternatives], [internal_external], [intention], [risk_id], [is_in_overlap_time_period], [DateOfInceptionCombinedKey], [ID_Arbeitsvorrat_Exposure], [Client_Exposure], [Insured_Name_ClientInfo_Exposure], [Policy_ID_ClientInfo_Exposure], [Policy_Inception_Date_Exposure], [Policy_Expiry_Date_Exposure], [Coverage], [Currency_Exposure], [Client_Limit_USD_Exposure], [Full_Limit_USD_Exposure], [Attachment_USD_Exposure], [Client_Premium_USD], [Combined_Premium_USD], [Client_Premium_Orig_Curr], [Combined_Premium_Orig_Curr], [Client_GrossNet_Premium_USD], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit_Exposure], [SIR_USD_Exposure], [CountryExposureKey], [CambridgeExposureKey], [CompanySegmentExposureKey], [Turnover_USD_BvD_Exposure], [Duplicate_ID_Exposure], [CountryCombinedKey], [CambridgeCombinedKey], [CompanySegmentCombinedKey], [Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined], [Calculated_or_ClientInfo_FGU_USD]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, LossDateKey, Date_of_Loss, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, IsCensored, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Paid_Client_Share_USD_Combined, ClaimsIncurredBandsKey, Incurred_Client_Share_USD, ClaimsIncurredCombinedBandsKey, Incurred_Client_Share_USD_Combined, ClaimsThresholdCategoriesKey, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, CountryKey, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID, Country_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, CompanySegmentKey, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, Bvd_Country_Check_claims, Bvd_Industry_Check_claims, Combined_NotificationDate_IncidentDate, Claims_Time_btw_UWDate_IncNot_inQuarter, Claims_Time_btw_UWDate_IncNot_inMonth, Claims_Time_btw_UWDate_IncNot_inDay, Reportingtime_inMonths, Reportingtime_inDays, Trade_Level_CodeNumber_Mapped_Cambridge, final_flag, CambridgeKey, Loss_Development_Quarter, Trade_Level_Name_Mapped_Cambridge, Is_Deleted, Is_Signal_Reserve, claim_cause, QualityKey, classification_score, claim_cause_alternatives, internal_external, intention, risk_id, is_in_overlap_time_period, DateOfInceptionCombinedKey, ID_Arbeitsvorrat_Exposure, Client_Exposure, Insured_Name_ClientInfo_Exposure, Policy_ID_ClientInfo_Exposure, Policy_Inception_Date_Exposure, Policy_Expiry_Date_Exposure, Coverage, Currency_Exposure, Client_Limit_USD_Exposure, Full_Limit_USD_Exposure, Attachment_USD_Exposure, Client_Premium_USD, Combined_Premium_USD, Client_Premium_Orig_Curr, Combined_Premium_Orig_Curr, Client_GrossNet_Premium_USD, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit_Exposure, SIR_USD_Exposure, CountryExposureKey, CambridgeExposureKey, CompanySegmentExposureKey, Turnover_USD_BvD_Exposure, Duplicate_ID_Exposure, CountryCombinedKey, CambridgeCombinedKey, CompanySegmentCombinedKey, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, Calculated_or_ClientInfo_FGU_USD ])


    class bvd_matching:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'bvd_matching'
        def __repr__(self): return '[dbo].[bvd_matching]'

        def insert(self, company_id: int, bvd_id: str, plausibility_class: int = None, plausibility_score: float = None, config_setting: str = None, string_match_score_internal: float = None, name_alias: str = None, name_alias_source: str = None, modified_overall_score: float = None, best_match: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[bvd_matching] ([company_id], [bvd_id], [plausibility_class], [plausibility_score], [config_setting], [string_match_score_internal], [name_alias], [name_alias_source], [modified_overall_score], [best_match], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ company_id, bvd_id, plausibility_class, plausibility_score, config_setting, string_match_score_internal, name_alias, name_alias_source, modified_overall_score, best_match, Create_Time, Change_Time, Changed_By ])


    class validationResults_claims:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'validationResults_claims'
        def __repr__(self): return '[dbo].[validationResults_claims]'

        def insert(self, ref_id: int, ID_Arbeitsvorrat: str, RuleName: str, Issue: str, Column: str = None, Column2: str = None, Column3: str = None, rowNr: int = None, File: str = None, Value: str = None, Value2: str = None, Value3: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[validationResults_claims] ([ref_id], [ID_Arbeitsvorrat], [RuleName], [Issue], [Column], [Column2], [Column3], [rowNr], [File], [Value], [Value2], [Value3], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ref_id, ID_Arbeitsvorrat, RuleName, Issue, Column, Column2, Column3, rowNr, File, Value, Value2, Value3, Create_Time, Change_Time, Changed_By ])


    class bvd_old:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'bvd_old'
        def __repr__(self): return '[dbo].[bvd_old]'

        def insert(self, bvd_id: str, name: str = None, name_alias: str = None, name_alias_source: str = None, addr_internat: str = None, ambest_id: str = None, branch_count: int = None, category_of_company: str = None, city_internat: str = None, corporate_group_size: int = None, country: str = None, county: str = None, ctryiso: str = None, direct_parent_bvdid: str = None, direct_parent_name_internat: str = None, ein: str = None, email: str = None, employees: int = None, eurovat: str = None, hierarchy_level: int = None, inactive: str = None, incorporation_date: str = None, legalfrm: str = None, lei_lei: str = None, listed: str = None, mainexch: str = None, naicsccod2017: str = None, phone: str = None, postcode: str = None, previous_names_set_array: str = None, sd_isin: str = None, sd_ticker: str = None, slegalf: str = None, state_us: str = None, subs_count: int = None, traderegisternr: str = None, turnover_eur: int = None, type_of_entity: str = None, ultimate_parent_bvdid: str = None, ultimate_parent_ctryiso: str = None, ultimate_parent_name: str = None, ussicccod: str = None, vatnumber: str = None, website: str = None, create_time: datetime = None, change_time: datetime = None, changed_by: str = None, cambridgecod: str = None, cambridgename: str = None):
            sql = """INSERT INTO [dbo].[bvd_old] ([bvd_id], [name], [name_alias], [name_alias_source], [addr_internat], [ambest_id], [branch_count], [category_of_company], [city_internat], [corporate_group_size], [country], [county], [ctryiso], [direct_parent_bvdid], [direct_parent_name_internat], [ein], [email], [employees], [eurovat], [hierarchy_level], [inactive], [incorporation_date], [legalfrm], [lei_lei], [listed], [mainexch], [naicsccod2017], [phone], [postcode], [previous_names_set_array], [sd_isin], [sd_ticker], [slegalf], [state_us], [subs_count], [traderegisternr], [turnover_eur], [type_of_entity], [ultimate_parent_bvdid], [ultimate_parent_ctryiso], [ultimate_parent_name], [ussicccod], [vatnumber], [website], [create_time], [change_time], [changed_by], [cambridgecod], [cambridgename]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ bvd_id, name, name_alias, name_alias_source, addr_internat, ambest_id, branch_count, category_of_company, city_internat, corporate_group_size, country, county, ctryiso, direct_parent_bvdid, direct_parent_name_internat, ein, email, employees, eurovat, hierarchy_level, inactive, incorporation_date, legalfrm, lei_lei, listed, mainexch, naicsccod2017, phone, postcode, previous_names_set_array, sd_isin, sd_ticker, slegalf, state_us, subs_count, traderegisternr, turnover_eur, type_of_entity, ultimate_parent_bvdid, ultimate_parent_ctryiso, ultimate_parent_name, ussicccod, vatnumber, website, create_time, change_time, changed_by, cambridgecod, cambridgename ])


    class Company:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company'
        def __repr__(self): return '[dbo].[Company]'

        def insert(self, Company_Name_Clean: str, Company_Name: str, Source_of_Change: str, Country_ISO2_ID: int = None, City_Unified_Name_ID: int = None, Industry_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Parent_Company_ID: int = None, Ultimate_Company_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company] ([Company_Name_Clean], [Country_ISO2_ID], [City_Unified_Name_ID], [Industry_ID], [Company_Name], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Parent_Company_ID], [Ultimate_Company_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_Name_Clean, Country_ISO2_ID, City_Unified_Name_ID, Industry_ID, Company_Name, Street, ZIP_Code, State_ID, Domain_Name, Source_of_Change, Is_Combined, Is_Manually_Curated, Parent_Company_ID, Ultimate_Company_ID, Create_Time, Change_Time, Changed_By ])


    class claims_bdx_test:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx_test'
        def __repr__(self): return '[dbo].[claims_bdx_test]'

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, Policy_ID_Cleaned: str = None, Is_Signal_Reserve: int = None):
            sql = """INSERT INTO [dbo].[claims_bdx_test] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Claim_ID_ClientInfo], [Is_Claim_Closed], [Date_of_Incident], [Date_of_Notification], [Claims_Description], [Type_of_Loss], [Country_of_Loss_Settlement], [Value_as_Of_Date], [Loss_Currency], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [Threshold_Orig_Curr_unind], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [Policy_ID_Cleaned], [Is_Signal_Reserve]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Policy_ID_Cleaned, Is_Signal_Reserve ])


    class Company_V1:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_V1'
        def __repr__(self): return '[dbo].[Company_V1]'

        def insert(self, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, manual_curated_entry: int = None, combined_entry: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_V1] ([duplicate_of_id], [Insured_Name_Clean], [Insured_Name_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ISO2], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Homepage], [Turnover_ClientInfo_USD], [manual_curated_entry], [combined_entry], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, manual_curated_entry, combined_entry, Create_Time, Change_Time, Changed_By ])


    class _06_Countries:
        # columns
        Country = 'Country'
        Currency = 'Currency'
        ISO2 = 'ISO2'
        ISO3 = 'ISO3'
        Latitude = 'Latitude'
        Longitude = 'Longitude'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '06_Countries'
        def __repr__(self): return '[dbo].[06_Countries]'

        def insert(self, ISO2: str, ISO3: str, Country: str = None, Currency: str = None, Latitude: float = None, Longitude: float = None):
            sql = """INSERT INTO [dbo].[06_Countries] ([Country], [Currency], [ISO2], [ISO3], [Latitude], [Longitude]) VALUES (?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Country, Currency, ISO2, ISO3, Latitude, Longitude ])


    class exposure_bdx:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx'
        def __repr__(self): return '[dbo].[exposure_bdx]'

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Process_ID: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_RestorationData: float = None, Coverage_4_Sublimit_Reputation: float = None, Coverage_5_Sublimit_Business_Interruption: float = None, Coverage_6_Sublimit_CBI_IT_Service_Provider: float = None, Coverage_7_Sublimit_CBI_Supply_Chain: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, Coverage_15_Sublimit_System_Failure: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None, Is_Special_Acceptance: int = None, Client_Limit_Occ_Orig_Curr: float = None, Full_Limit_Occ_Orig_Curr: float = None, Policy_ID_Cleaned: str = None, Client_Limit_Occ_USD: float = None, Full_Limit_Occ_USD: float = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, Portfolio_Tag: str = None, rowNr_deleted_count: int = None):
            sql = """INSERT INTO [dbo].[exposure_bdx] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_RestorationData], [Coverage_4_Sublimit_Reputation], [Coverage_5_Sublimit_Business_Interruption], [Coverage_6_Sublimit_CBI_IT_Service_Provider], [Coverage_7_Sublimit_CBI_Supply_Chain], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [Coverage_15_Sublimit_System_Failure], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Duplicate_ID], [ID_Arbeitsvorrat_MR_share], [Is_Special_Acceptance], [Client_Limit_Occ_Orig_Curr], [Full_Limit_Occ_Orig_Curr], [Policy_ID_Cleaned], [Client_Limit_Occ_USD], [Full_Limit_Occ_USD], [Insured_Name_Clean], [Company_ClientInfo_ID], [Portfolio_Tag], [rowNr_deleted_count], [Process_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_RestorationData, Coverage_4_Sublimit_Reputation, Coverage_5_Sublimit_Business_Interruption, Coverage_6_Sublimit_CBI_IT_Service_Provider, Coverage_7_Sublimit_CBI_Supply_Chain, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, Coverage_15_Sublimit_System_Failure, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Duplicate_ID, ID_Arbeitsvorrat_MR_share, Is_Special_Acceptance, Client_Limit_Occ_Orig_Curr, Full_Limit_Occ_Orig_Curr, Policy_ID_Cleaned, Client_Limit_Occ_USD, Full_Limit_Occ_USD, Insured_Name_Clean, Company_ClientInfo_ID, Portfolio_Tag, rowNr_deleted_count, Process_ID ])


    class _06_Month:
        # columns
        MM = 'MM'
        Month = 'Month'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '06_Month'
        def __repr__(self): return '[dbo].[06_Month]'

        def insert(self, MM: int, Month: str):
            sql = """INSERT INTO [dbo].[06_Month] ([MM], [Month]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ MM, Month ])


    class re_contracts:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 're_contracts'
        def __repr__(self): return '[dbo].[re_contracts]'

        def insert(self, re_contract_id: int, BuPa: str, program_id: str, coverage_id: str, subsystem_id: str, begin_date: date, end_date: date, contract_type: str, client_name: str = None, program_name: str = None, max_retention_USD: float = None, mr_share: float = None, limit_100_percent_USD: float = None, isStacked: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, treaty_name: str = None, add_id: str = None, protected_share: float = None):
            sql = """INSERT INTO [dbo].[re_contracts] ([re_contract_id], [BuPa], [client_name], [program_id], [program_name], [coverage_id], [subsystem_id], [begin_date], [end_date], [contract_type], [max_retention_USD], [mr_share], [limit_100_percent_USD], [isStacked], [Create_Time], [Change_Time], [Changed_By], [treaty_name], [add_id], [protected_share]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ re_contract_id, BuPa, client_name, program_id, program_name, coverage_id, subsystem_id, begin_date, end_date, contract_type, max_retention_USD, mr_share, limit_100_percent_USD, isStacked, Create_Time, Change_Time, Changed_By, treaty_name, add_id, protected_share ])


    class Client_Name_Hierarchy_Level:
        # columns
        Hierarchy_Level_Id = 'Hierarchy_Level_Id'
        Purpose = 'Purpose'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Client_Name_Hierarchy_Level'
        def __repr__(self): return '[dbo].[Client_Name_Hierarchy_Level]'

        def insert(self, Hierarchy_Level_Id: int, Purpose: str):
            sql = """INSERT INTO [dbo].[Client_Name_Hierarchy_Level] ([Hierarchy_Level_Id], [Purpose]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Hierarchy_Level_Id, Purpose ])


    class file_level_status:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_status'
        def __repr__(self): return '[dbo].[file_level_status]'

        def insert(self, ID_Arbeitsvorrat: str, Process_ID: int, runScript: int = None, runNorm: int = None, runBvD: int = None, runMRShares: int = None, runValidation: int = None, runClaimsLinking: int = None, runGenerateOutput: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[file_level_status] ([ID_Arbeitsvorrat], [runScript], [runNorm], [runBvD], [runMRShares], [runValidation], [runClaimsLinking], [runGenerateOutput], [Create_Time], [Change_Time], [Changed_By], [Process_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, runScript, runNorm, runBvD, runMRShares, runValidation, runClaimsLinking, runGenerateOutput, Create_Time, Change_Time, Changed_By, Process_ID ])


    class fileID2re_contracts:
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        ID_Arbeitsvorrat_MR_share = 'ID_Arbeitsvorrat_MR_share'
        re_contract_id = 're_contract_id'
        uw_selection = 'uw_selection'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'fileID2re_contracts'
        def __repr__(self): return '[dbo].[fileID2re_contracts]'

        def insert(self, ID_Arbeitsvorrat: str, re_contract_id: int, ID_Arbeitsvorrat_MR_share: str = None, uw_selection: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[fileID2re_contracts] ([ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [re_contract_id], [uw_selection], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, re_contract_id, uw_selection, Create_Time, Change_Time, Changed_By ])


    class z_Attachment_Bands:
        # columns
        Aggr__Bands = 'Aggr. Bands'
        Band_Name = 'Band Name'
        Rank = 'Rank'
        Rank2 = 'Rank2'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Attachment_Bands'
        def __repr__(self): return '[dbo].[z_Attachment_Bands]'

        def insert(self, Aggr__Bands: str, Band_Name: str, Rank: int, Rank2: int):
            sql = """INSERT INTO [dbo].[z_Attachment_Bands] ([Aggr. Bands], [Band Name], [Rank], [Rank2]) VALUES (?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Aggr__Bands, Band_Name, Rank, Rank2 ])


    class Dim_Industry:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Industry'
        def __repr__(self): return '[dbo].[Dim_Industry]'

        def insert(self, Industry_Code_Standard: str, Industry_Description: str, Industry_Code_Number: str = None, Year: int = None, Is_Custom: int = None, SIC_ID: int = None, NAICS_ID: int = None, NACE_ID: int = None, Cambridge_ID: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Industry] ([Industry_Code_Standard], [Industry_Code_Number], [Industry_Description], [Year], [Is_Custom], [SIC_ID], [NAICS_ID], [NACE_ID], [Cambridge_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Industry_Code_Standard, Industry_Code_Number, Industry_Description, Year, Is_Custom, SIC_ID, NAICS_ID, NACE_ID, Cambridge_ID, Create_Time, Change_Time, Changed_By ])


    class z_Client_Limit_Bands:
        # columns
        Band_Name = 'Band_Name'
        Rank = 'Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Client_Limit_Bands'
        def __repr__(self): return '[dbo].[z_Client_Limit_Bands]'

        def insert(self, Band_Name: str, Rank: int):
            sql = """INSERT INTO [dbo].[z_Client_Limit_Bands] ([Band_Name], [Rank]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Band_Name, Rank ])


    class z_Full_Limit_Bands:
        # columns
        Band_Name = 'Band Name'
        Rank = 'Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Full_Limit_Bands'
        def __repr__(self): return '[dbo].[z_Full_Limit_Bands]'

        def insert(self, Band_Name: str, Rank: int):
            sql = """INSERT INTO [dbo].[z_Full_Limit_Bands] ([Band Name], [Rank]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Band_Name, Rank ])


    class z_Percentage_Band:
        # columns
        BandName = 'BandName'
        PercentValue = 'PercentValue'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Percentage_Band'
        def __repr__(self): return '[dbo].[z_Percentage_Band]'

        def insert(self, BandName: str, PercentValue: float):
            sql = """INSERT INTO [dbo].[z_Percentage_Band] ([BandName], [PercentValue]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ BandName, PercentValue ])


    class Claim:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Claim'
        def __repr__(self): return '[dbo].[Claim]'

        def insert(self, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None):
            sql = """INSERT INTO [dbo].[Claim] ([Technical_Client_ID], [Company_ID], [Claim_Ref_Clean], [Claim_Ref], [Date_of_Loss], [Date_of_Incident], [Date_of_Notification], [Exposure_ID], [Country_ISO2_ID], [Type_of_Loss], [Loss_Event_ID], [Claims_Description], [Policy_Ref], [Policy_Ref_Clean], [Policy_Inception_Date], [Policy_Currency_ID], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Create_Time], [Change_Time], [Changed_By], [Parent_ID], [Ultimate_Parent_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ])


    class z_Percentage_Band_Dev:
        # columns
        BandName = 'BandName'
        PercentValue = 'PercentValue'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Percentage_Band_Dev'
        def __repr__(self): return '[dbo].[z_Percentage_Band_Dev]'

        def insert(self, BandName: str, PercentValue: float):
            sql = """INSERT INTO [dbo].[z_Percentage_Band_Dev] ([BandName], [PercentValue]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ BandName, PercentValue ])


    class file_level_meta_information_20220601:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'file_level_meta_information_20220601'
        def __repr__(self): return '[dbo].[file_level_meta_information_20220601]'

        def insert(self, ID_Arbeitsvorrat: str, BU: str, bdx_type: str, Exclude_from_dashboards: int, Client_Name: str = None, Orig_File_Name: str = None, Subsystem_ID: str = None, Treaty_Programm_ID: str = None, FSRI_ID: str = None, UW_Year: int = None, Smart_matching_done: str = None, R_code_done: str = None, Q_A_done: str = None, Function_ran: str = None, Validation_issues_done: str = None, Four_Eye_Check_done: str = None, Signoff_done: str = None, Contact_Data_Team: str = None, Responsible_Four_Eye_Check: str = None, UW: str = None, PML_needs_to_be_done: str = None, PML_has_been_done: str = None, SRAC_needs_to_be_done: str = None, SRAC_has_been_done: str = None, Priority: str = None, Deadline: datetime = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[file_level_meta_information_20220601] ([ID_Arbeitsvorrat], [BU], [Client_Name], [Orig_File_Name], [bdx_type], [Subsystem_ID], [Treaty_Programm_ID], [FSRI_ID], [UW_Year], [Smart_matching_done], [R_code_done], [Q_A_done], [Function_ran], [Validation_issues_done], [Four_Eye_Check_done], [Signoff_done], [Contact_Data_Team], [Responsible_Four_Eye_Check], [UW], [PML_needs_to_be_done], [PML_has_been_done], [SRAC_needs_to_be_done], [SRAC_has_been_done], [Priority], [Deadline], [Exclude_from_dashboards], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, BU, Client_Name, Orig_File_Name, bdx_type, Subsystem_ID, Treaty_Programm_ID, FSRI_ID, UW_Year, Smart_matching_done, R_code_done, Q_A_done, Function_ran, Validation_issues_done, Four_Eye_Check_done, Signoff_done, Contact_Data_Team, Responsible_Four_Eye_Check, UW, PML_needs_to_be_done, PML_has_been_done, SRAC_needs_to_be_done, SRAC_has_been_done, Priority, Deadline, Exclude_from_dashboards, Create_Time, Change_Time, Changed_By ])


    class claims_linking:
        # columns
        id = 'id'
        claim_id = 'claim_id'
        risk_id = 'risk_id'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_linking'
        def __repr__(self): return '[dbo].[claims_linking]'

        def insert(self, claim_id: int, risk_id: int = None):
            sql = """INSERT INTO [dbo].[claims_linking] ([claim_id], [risk_id]) VALUES (?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ claim_id, risk_id ])


    class z_Premium_Bands:
        # columns
        Band_Name = 'Band_Name'
        Rank = 'Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Premium_Bands'
        def __repr__(self): return '[dbo].[z_Premium_Bands]'

        def insert(self, Band_Name: str, Rank: int):
            sql = """INSERT INTO [dbo].[z_Premium_Bands] ([Band_Name], [Rank]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Band_Name, Rank ])


    class data_entry_meta_information_20220601:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'data_entry_meta_information_20220601'
        def __repr__(self): return '[dbo].[data_entry_meta_information_20220601]'

        def insert(self, id: int, UW: str, BU: str, bdx_type: str, Subsystem: str, File_Name: str = None, Number_Risks: int = None, Number_Claims: int = None, As_at_Date: date = None, Program_IDs: str = None, Additional_Program_IDs: str = None, FSRI_ID: str = None, Client_Name: str = None, BuPa: str = None, Treaty_Program_Name: str = None, Begin_Date: date = None, End_Date: date = None, UW_Year: int = None, Is_From_CU_Collection: int = None, Is_Ignored: int = None, ID_Arbeitsvorrat: str = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[data_entry_meta_information_20220601] ([id], [File_Name], [UW], [BU], [Number_Risks], [Number_Claims], [bdx_type], [As_at_Date], [Subsystem], [Program_IDs], [Additional_Program_IDs], [FSRI_ID], [Client_Name], [BuPa], [Treaty_Program_Name], [Begin_Date], [End_Date], [UW_Year], [Is_From_CU_Collection], [Is_Ignored], [ID_Arbeitsvorrat], [Comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, File_Name, UW, BU, Number_Risks, Number_Claims, bdx_type, As_at_Date, Subsystem, Program_IDs, Additional_Program_IDs, FSRI_ID, Client_Name, BuPa, Treaty_Program_Name, Begin_Date, End_Date, UW_Year, Is_From_CU_Collection, Is_Ignored, ID_Arbeitsvorrat, Comment, Create_Time, Change_Time, Changed_By ])


    class claims_company_linking:
        # columns
        id = 'id'
        company_id = 'company_id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_company_linking'
        def __repr__(self): return '[dbo].[claims_company_linking]'

        def insert(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[claims_company_linking] ([id], [company_id], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, company_id, Create_Time, Change_Time, Changed_By ])


    class claims_bdx:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx'
        def __repr__(self): return '[dbo].[claims_bdx]'

        def insert(self, ID_Arbeitsvorrat: str, Client: str, rowNr: int, Process_ID: int, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Value_as_Of_Date: date = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, Duplicate_ID: str = None, Internal_Claim_ID: int = None, Is_Signal_Reserve: int = None, Loss_Event_ClientInfo: str = None, Loss_Event_normalized: str = None, Product_Name_ClientInfo: str = None, Claim_Closed_Date: date = None, Policy_ID_Cleaned: str = None, Insured_Name_Clean: str = None, Company_ClientInfo_ID: int = None, rowNr_deleted_count: int = None, Claim_ClientInfo_ID: int = None, Claim_ID_Cleaned: str = None):
            sql = """INSERT INTO [dbo].[claims_bdx] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Claim_ID_ClientInfo], [Is_Claim_Closed], [Date_of_Incident], [Date_of_Notification], [Claims_Description], [Type_of_Loss], [Country_of_Loss_Settlement], [Value_as_Of_Date], [Loss_Currency], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [Threshold_Orig_Curr_unind], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [IsCensored], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Incurred_Insured_FGU_USD], [Paid_Client_Share_USD], [Incurred_Client_Share_USD], [Threshold_unind_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Country_of_Loss_Settlement_ISO2], [Duplicate_ID], [Internal_Claim_ID], [Is_Signal_Reserve], [Loss_Event_ClientInfo], [Loss_Event_normalized], [Product_Name_ClientInfo], [Claim_Closed_Date], [Policy_ID_Cleaned], [Insured_Name_Clean], [Company_ClientInfo_ID], [rowNr_deleted_count], [Process_ID], [Claim_ClientInfo_ID], [Claim_ID_Cleaned]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, Duplicate_ID, Internal_Claim_ID, Is_Signal_Reserve, Loss_Event_ClientInfo, Loss_Event_normalized, Product_Name_ClientInfo, Claim_Closed_Date, Policy_ID_Cleaned, Insured_Name_Clean, Company_ClientInfo_ID, rowNr_deleted_count, Process_ID, Claim_ClientInfo_ID, Claim_ID_Cleaned ])


    class z_SIR_Bands:
        # columns
        Band_Name = 'Band Name'
        Rank = 'Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_SIR_Bands'
        def __repr__(self): return '[dbo].[z_SIR_Bands]'

        def insert(self, Band_Name: str, Rank: int):
            sql = """INSERT INTO [dbo].[z_SIR_Bands] ([Band Name], [Rank]) VALUES (?, ?);"""
            return self.dbx.get_result(sql, [ Band_Name, Rank ])


    class issues_excel_input:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'issues_excel_input'
        def __repr__(self): return '[dbo].[issues_excel_input]'

        def insert(self, excel_file: str, row_number: int = None, ID_Arbeitsvorrat: str = None, Column: str = None, Value: str = None, Criticality: int = None, Comment: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[issues_excel_input] ([excel_file], [row_number], [ID_Arbeitsvorrat], [Column], [Value], [Criticality], [Comment], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ excel_file, row_number, ID_Arbeitsvorrat, Column, Value, Criticality, Comment, Create_Time, Change_Time, Changed_By ])


    class Turnover:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Turnover'
        def __repr__(self): return '[dbo].[Turnover]'

        def insert(self, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Turnover] ([Company_ID], [Turnover_Year], [Turnover], [Currency_ID], [Source_of_Change], [Is_Manually_Curated], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ])


    class Dim_Band_Hierarchy_1L:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Band_Hierarchy_1L'
        def __repr__(self): return '[dbo].[Dim_Band_Hierarchy_1L]'

        def insert(self, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Band_Hierarchy_1L] ([Band_Group], [Band_Type], [Band_ID_From], [Band_ID_To], [Level_1], [Level_1_Sortindex], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Create_Time, Change_Time, Changed_By ])


    class tDimCambridge:
        # columns
        CambridgeKey = 'CambridgeKey'
        Cambridge_Code_Number = 'Cambridge_Code_Number'
        Cambridge_Code_Number_Agg = 'Cambridge_Code_Number_Agg'
        Cambridge_Full_Agg_Name = 'Cambridge_Full_Agg_Name'
        Cambridge_Full_Name = 'Cambridge_Full_Name'
        Cambridge_Name = 'Cambridge_Name'
        Cambridge_Name_Agg = 'Cambridge_Name_Agg'
        Cambridge_Rank = 'Cambridge_Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimCambridge'
        def __repr__(self): return '[dbo].[tDimCambridge]'

        def insert(self, Cambridge_Code_Number: str, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None):
            sql = """INSERT INTO [dbo].[tDimCambridge] ([Cambridge_Code_Number], [Cambridge_Code_Number_Agg], [Cambridge_Full_Agg_Name], [Cambridge_Full_Name], [Cambridge_Name], [Cambridge_Name_Agg], [Cambridge_Rank]) VALUES (?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ])


    class exposure_company_linking:
        # columns
        id = 'id'
        company_id = 'company_id'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_company_linking'
        def __repr__(self): return '[dbo].[exposure_company_linking]'

        def insert(self, id: int, company_id: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[exposure_company_linking] ([id], [company_id], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, company_id, Create_Time, Change_Time, Changed_By ])


    class Processes_Yuru:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Processes_Yuru'
        def __repr__(self): return '[dbo].[Processes_Yuru]'

        def insert(self, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None):
            sql = """INSERT INTO [dbo].[Processes_Yuru] ([OperationFlag], [InsertionDate], [LastModifiedDate], [Deadline], [BusinessContact], [BusinessUnit], [Priority], [SmartMatchingContact], [ProjectTeamContact], [FourEyeCheckContact], [Comment], [SignOffToken]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken ])


    class Dim_Band_Hierarchy_2L:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Band_Hierarchy_2L'
        def __repr__(self): return '[dbo].[Dim_Band_Hierarchy_2L]'

        def insert(self, Band_Group: str, Band_Type: str, Level_1: str, Level_1_Sortindex: int, Level_2: str, Level_2_Sortindex: int, Band_ID_From: int = None, Band_ID_To: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Band_Hierarchy_2L] ([Band_Group], [Band_Type], [Band_ID_From], [Band_ID_To], [Level_1], [Level_1_Sortindex], [Level_2], [Level_2_Sortindex], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Band_Group, Band_Type, Band_ID_From, Band_ID_To, Level_1, Level_1_Sortindex, Level_2, Level_2_Sortindex, Create_Time, Change_Time, Changed_By ])


    class tDimCountries:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimCountries'
        def __repr__(self): return '[dbo].[tDimCountries]'

        def insert(self, Country: str, ISO2: str, ISO3: str, Currency: str = None, Latitude: float = None, Longitude: float = None, Continent_Name: str = None, Region_Name: str = None):
            sql = """INSERT INTO [dbo].[tDimCountries] ([Country], [Currency], [ISO2], [ISO3], [Latitude], [Longitude], [Continent_Name], [Region_Name]) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Country, Currency, ISO2, ISO3, Latitude, Longitude, Continent_Name, Region_Name ])


    class Claim_History:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Claim_History'
        def __repr__(self): return '[dbo].[Claim_History]'

        def insert(self, Claim_ID: int, Source_of_Change: str, Technical_Client_ID: int = None, Company_ID: int = None, Claim_Ref_Clean: str = None, Claim_Ref: str = None, Date_of_Loss: date = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Exposure_ID: int = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Is_Combined: int = None, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Parent_ID: int = None, Ultimate_Parent_ID: int = None):
            sql = """INSERT INTO [dbo].[Claim_History] ([Claim_ID], [Technical_Client_ID], [Company_ID], [Claim_Ref_Clean], [Claim_Ref], [Date_of_Loss], [Date_of_Incident], [Date_of_Notification], [Exposure_ID], [Country_ISO2_ID], [Type_of_Loss], [Loss_Event_ID], [Claims_Description], [Policy_Ref], [Policy_Ref_Clean], [Policy_Inception_Date], [Policy_Currency_ID], [Source_of_Change], [Is_Combined], [Is_Manually_Curated], [Create_Time], [Change_Time], [Changed_By], [Parent_ID], [Ultimate_Parent_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Claim_ID, Technical_Client_ID, Company_ID, Claim_Ref_Clean, Claim_Ref, Date_of_Loss, Date_of_Incident, Date_of_Notification, Exposure_ID, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source_of_Change, Is_Combined, Is_Manually_Curated, Create_Time, Change_Time, Changed_By, Parent_ID, Ultimate_Parent_ID ])


    class tFactExposure_test:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tFactExposure_test'
        def __repr__(self): return '[dbo].[tFactExposure_test]'

        def insert(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None):
            sql = """INSERT INTO [dbo].[tFactExposure_test] ([id], [ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [PolicyInceptionDateKey], [Policy_Inception_Date], [PolicyExpiryDateKey], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Turnover_Year_ClientInfo], [Trade_Level_ClientInfo], [Insured_No_of_Employees], [PII_Records_Stored], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [CountryKey], [Insured_Country_ISO2], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd], [Coverage_4_Sublimit_RestorationData], [Coverage_5_Sublimit_Reputation], [Coverage_6_Sublimit_Business_Interruption], [Coverage_7_Sublimit_Contingent_Business_Interruption], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [DatasourceID], [rowNr], [Turnover_ClientInfo_USD], [ClientLimitBandKey], [Client_Limit_USD], [FullLimitBandKey], [Full_Limit_USD], [AttachmentBandKey], [Attachment_USD], [SIRBandsKey], [SIR_USD], [PremiumBandsKey], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [BvD_ID], [Duplicate_ID], [CambridgeKey], [Trade_Level_CodeNumber_Mapped_Cambridge], [MR_Limit_USD], [MR_Premium_USD], [MR_GrossNet_Premium_USD], [mr_share], [CompanySegmentKey], [Turnover_USD_Combined], [FileMetaInfoKey], [Insured_Name_BvD], [name_alias_bvd], [name_alias_source_bvd], [addr_internat_bvd], [ambest_id_bvd], [branch_count_bvd], [Trade_Level_CodeNumber_Mapped_Cambridge_BvD], [Trade_Level_Name_Mapped_Cambridge_BvD], [category_of_company_bvd], [city_internat_bvd], [corporate_group_size_bvd], [country_bvd], [county_bvd], [Country_ISO2_BvD], [direct_parent_bvdid_bvd], [direct_parent_name_internat_bvd], [ein_bvd], [email_bvd], [employees_bvd], [eurovat_bvd], [hierarchy_level_bvd], [inactive_bvd], [incorporation_date_bvd], [legalfrm_bvd], [lei_lei_bvd], [listed_bvd], [mainexch_bvd], [naicsccod2017_bvd], [phone_bvd], [postcode_bvd], [previous_names_set_array_bvd], [sd_isin_bvd], [sd_ticker_bvd], [slegalf_bvd], [state_us_bvd], [subs_count_bvd], [traderegisternr_bvd], [Turnover_EUR_BvD], [Turnover_USD_BvD], [type_of_entity_bvd], [ultimate_parent_bvdid_bvd], [ultimate_parent_ctryiso_bvd], [ultimate_parent_name_bvd], [ussicccod_bvd], [vatnumber_bvd], [website_bvd], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [DELETE_indicator], [Bvd_Country_Check], [Bvd_Industry_Check], [Turnover_Deviation_BvD], [Turnover_Dev_as_perc_of_Client_Turnover], [Abs_Dev_perc], [Bvd_Turnover_Check], [Policy_Duration_Day], [Policy_Duration_Month], [Is_MR_Share_calculated], [Is_Deleted]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted ])


    class exposure_bdx_jb:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_bdx_jb'
        def __repr__(self): return '[dbo].[exposure_bdx_jb]'

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, rowNr: int = None, DELETE_indicator: str = None, ID_Arbeitsvorrat_MR_share: str = None):
            sql = """INSERT INTO [dbo].[exposure_bdx_jb] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Turnover_Year_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [PII_Records_Stored], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd], [Coverage_4_Sublimit_RestorationData], [Coverage_5_Sublimit_Reputation], [Coverage_6_Sublimit_Business_Interruption], [Coverage_7_Sublimit_Contingent_Business_Interruption], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [ID_Arbeitsvorrat_MR_share]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, ID_Arbeitsvorrat_MR_share ])


    class tFactExposure:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tFactExposure'
        def __repr__(self): return '[dbo].[tFactExposure]'

        def insert(self, id: int, ID_Arbeitsvorrat: str, PolicyInceptionDateKey: int, PolicyExpiryDateKey: int, CountryKey: int, rowNr: int, CambridgeKey: int, CompanySegmentKey: int, ID_Arbeitsvorrat_MR_share: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Country_ISO2: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, DatasourceID: str = None, Turnover_ClientInfo_USD: float = None, ClientLimitBandKey: int = None, Client_Limit_USD: float = None, FullLimitBandKey: int = None, Full_Limit_USD: float = None, AttachmentBandKey: int = None, Attachment_USD: float = None, SIRBandsKey: int = None, SIR_USD: float = None, PremiumBandsKey: int = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Trade_Level_CodeNumber_Mapped_Cambridge: str = None, MR_Limit_USD: float = None, MR_Premium_USD: float = None, MR_GrossNet_Premium_USD: float = None, mr_share: float = None, Turnover_USD_Combined: float = None, FileMetaInfoKey: int = None, Insured_Name_BvD: str = None, name_alias_bvd: str = None, name_alias_source_bvd: str = None, addr_internat_bvd: str = None, ambest_id_bvd: str = None, branch_count_bvd: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_BvD: str = None, Trade_Level_Name_Mapped_Cambridge_BvD: str = None, category_of_company_bvd: str = None, city_internat_bvd: str = None, corporate_group_size_bvd: int = None, country_bvd: str = None, county_bvd: str = None, Country_ISO2_BvD: str = None, direct_parent_bvdid_bvd: str = None, direct_parent_name_internat_bvd: str = None, ein_bvd: str = None, email_bvd: str = None, employees_bvd: int = None, eurovat_bvd: str = None, hierarchy_level_bvd: int = None, inactive_bvd: str = None, incorporation_date_bvd: str = None, legalfrm_bvd: str = None, lei_lei_bvd: str = None, listed_bvd: str = None, mainexch_bvd: str = None, naicsccod2017_bvd: str = None, phone_bvd: str = None, postcode_bvd: str = None, previous_names_set_array_bvd: str = None, sd_isin_bvd: str = None, sd_ticker_bvd: str = None, slegalf_bvd: str = None, state_us_bvd: str = None, subs_count_bvd: int = None, traderegisternr_bvd: str = None, Turnover_EUR_BvD: int = None, Turnover_USD_BvD: int = None, type_of_entity_bvd: str = None, ultimate_parent_bvdid_bvd: str = None, ultimate_parent_ctryiso_bvd: str = None, ultimate_parent_name_bvd: str = None, ussicccod_bvd: str = None, vatnumber_bvd: str = None, website_bvd: str = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Bvd_Country_Check: str = None, Bvd_Industry_Check: str = None, Turnover_Deviation_BvD: float = None, Turnover_Dev_as_perc_of_Client_Turnover: float = None, Abs_Dev_perc: float = None, Bvd_Turnover_Check: str = None, Policy_Duration_Day: int = None, Policy_Duration_Month: int = None, Is_MR_Share_calculated: str = None, Is_Deleted: str = None, Combined_Premium_USD: float = None, Combined_Premium_Orig_Curr: float = None, is_in_overlap_time_period: int = None, Client_Claims: str = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims: str = None, sum_of_Incurred_Client_Share_USD: float = None, sum_of_Paid_Client_Share_USD: float = None, sum_of_Incurred_Insured_FGU_USD: float = None, max_Threshold_unind_USD: float = None, Is_Signal_Reserve: int = None, sum_of_Calculated_or_ClientInfo_FGU_USD: float = None, is_Incurred_greaterthan_ClientLimit: int = None, is_Client_limit_depleted: int = None, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined: str = None, matched_claims_per_policy: int = None):
            sql = """INSERT INTO [dbo].[tFactExposure] ([id], [ID_Arbeitsvorrat], [ID_Arbeitsvorrat_MR_share], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [PolicyInceptionDateKey], [Policy_Inception_Date], [PolicyExpiryDateKey], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Turnover_Year_ClientInfo], [Trade_Level_ClientInfo], [Insured_No_of_Employees], [PII_Records_Stored], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [CountryKey], [Insured_Country_ISO2], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd], [Coverage_4_Sublimit_RestorationData], [Coverage_5_Sublimit_Reputation], [Coverage_6_Sublimit_Business_Interruption], [Coverage_7_Sublimit_Contingent_Business_Interruption], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [DatasourceID], [rowNr], [Turnover_ClientInfo_USD], [ClientLimitBandKey], [Client_Limit_USD], [FullLimitBandKey], [Full_Limit_USD], [AttachmentBandKey], [Attachment_USD], [SIRBandsKey], [SIR_USD], [PremiumBandsKey], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [BvD_ID], [Duplicate_ID], [CambridgeKey], [Trade_Level_CodeNumber_Mapped_Cambridge], [MR_Limit_USD], [MR_Premium_USD], [MR_GrossNet_Premium_USD], [mr_share], [CompanySegmentKey], [Turnover_USD_Combined], [FileMetaInfoKey], [Insured_Name_BvD], [name_alias_bvd], [name_alias_source_bvd], [addr_internat_bvd], [ambest_id_bvd], [branch_count_bvd], [Trade_Level_CodeNumber_Mapped_Cambridge_BvD], [Trade_Level_Name_Mapped_Cambridge_BvD], [category_of_company_bvd], [city_internat_bvd], [corporate_group_size_bvd], [country_bvd], [county_bvd], [Country_ISO2_BvD], [direct_parent_bvdid_bvd], [direct_parent_name_internat_bvd], [ein_bvd], [email_bvd], [employees_bvd], [eurovat_bvd], [hierarchy_level_bvd], [inactive_bvd], [incorporation_date_bvd], [legalfrm_bvd], [lei_lei_bvd], [listed_bvd], [mainexch_bvd], [naicsccod2017_bvd], [phone_bvd], [postcode_bvd], [previous_names_set_array_bvd], [sd_isin_bvd], [sd_ticker_bvd], [slegalf_bvd], [state_us_bvd], [subs_count_bvd], [traderegisternr_bvd], [Turnover_EUR_BvD], [Turnover_USD_BvD], [type_of_entity_bvd], [ultimate_parent_bvdid_bvd], [ultimate_parent_ctryiso_bvd], [ultimate_parent_name_bvd], [ussicccod_bvd], [vatnumber_bvd], [website_bvd], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [DELETE_indicator], [Bvd_Country_Check], [Bvd_Industry_Check], [Turnover_Deviation_BvD], [Turnover_Dev_as_perc_of_Client_Turnover], [Abs_Dev_perc], [Bvd_Turnover_Check], [Policy_Duration_Day], [Policy_Duration_Month], [Is_MR_Share_calculated], [Is_Deleted], [Combined_Premium_USD], [Combined_Premium_Orig_Curr], [is_in_overlap_time_period], [Client_Claims], [Trade_Level_CodeNumber_Mapped_Cambridge_Claims], [sum_of_Incurred_Client_Share_USD], [sum_of_Paid_Client_Share_USD], [sum_of_Incurred_Insured_FGU_USD], [max_Threshold_unind_USD], [Is_Signal_Reserve], [sum_of_Calculated_or_ClientInfo_FGU_USD], [is_Incurred_greaterthan_ClientLimit], [is_Client_limit_depleted], [Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined], [matched_claims_per_policy]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, ID_Arbeitsvorrat_MR_share, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, PolicyInceptionDateKey, Policy_Inception_Date, PolicyExpiryDateKey, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, CountryKey, Insured_Country_ISO2, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, DatasourceID, rowNr, Turnover_ClientInfo_USD, ClientLimitBandKey, Client_Limit_USD, FullLimitBandKey, Full_Limit_USD, AttachmentBandKey, Attachment_USD, SIRBandsKey, SIR_USD, PremiumBandsKey, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, BvD_ID, Duplicate_ID, CambridgeKey, Trade_Level_CodeNumber_Mapped_Cambridge, MR_Limit_USD, MR_Premium_USD, MR_GrossNet_Premium_USD, mr_share, CompanySegmentKey, Turnover_USD_Combined, FileMetaInfoKey, Insured_Name_BvD, name_alias_bvd, name_alias_source_bvd, addr_internat_bvd, ambest_id_bvd, branch_count_bvd, Trade_Level_CodeNumber_Mapped_Cambridge_BvD, Trade_Level_Name_Mapped_Cambridge_BvD, category_of_company_bvd, city_internat_bvd, corporate_group_size_bvd, country_bvd, county_bvd, Country_ISO2_BvD, direct_parent_bvdid_bvd, direct_parent_name_internat_bvd, ein_bvd, email_bvd, employees_bvd, eurovat_bvd, hierarchy_level_bvd, inactive_bvd, incorporation_date_bvd, legalfrm_bvd, lei_lei_bvd, listed_bvd, mainexch_bvd, naicsccod2017_bvd, phone_bvd, postcode_bvd, previous_names_set_array_bvd, sd_isin_bvd, sd_ticker_bvd, slegalf_bvd, state_us_bvd, subs_count_bvd, traderegisternr_bvd, Turnover_EUR_BvD, Turnover_USD_BvD, type_of_entity_bvd, ultimate_parent_bvdid_bvd, ultimate_parent_ctryiso_bvd, ultimate_parent_name_bvd, ussicccod_bvd, vatnumber_bvd, website_bvd, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, DELETE_indicator, Bvd_Country_Check, Bvd_Industry_Check, Turnover_Deviation_BvD, Turnover_Dev_as_perc_of_Client_Turnover, Abs_Dev_perc, Bvd_Turnover_Check, Policy_Duration_Day, Policy_Duration_Month, Is_MR_Share_calculated, Is_Deleted, Combined_Premium_USD, Combined_Premium_Orig_Curr, is_in_overlap_time_period, Client_Claims, Trade_Level_CodeNumber_Mapped_Cambridge_Claims, sum_of_Incurred_Client_Share_USD, sum_of_Paid_Client_Share_USD, sum_of_Incurred_Insured_FGU_USD, max_Threshold_unind_USD, Is_Signal_Reserve, sum_of_Calculated_or_ClientInfo_FGU_USD, is_Incurred_greaterthan_ClientLimit, is_Client_limit_depleted, Trade_Level_CodeNumber_Mapped_Cambridge_Claims_Exp_Combined, matched_claims_per_policy ])


    class z_Policy_Bands:
        # columns
        Band_Index = 'Band Index'
        Band_Name = 'Band_Name'
        Band_With_Type = 'Band_With_Type'
        Indexed_Band_Name = 'Indexed_Band_Name'
        Count_of_PolicyBand_Rank = 'Count of PolicyBand_Rank'
        Rank = 'Rank'
        Type = 'Type'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'z_Policy_Bands'
        def __repr__(self): return '[dbo].[z_Policy_Bands]'

        def insert(self, Band_Index: str, Band_Name: str, Band_With_Type: str, Indexed_Band_Name: str, Count_of_PolicyBand_Rank: int, Rank: int, Type: str):
            sql = """INSERT INTO [dbo].[z_Policy_Bands] ([Band Index], [Band_Name], [Band_With_Type], [Indexed_Band_Name], [Count of PolicyBand_Rank], [Rank], [Type]) VALUES (?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Band_Index, Band_Name, Band_With_Type, Indexed_Band_Name, Count_of_PolicyBand_Rank, Rank, Type ])


    class random_place_JB:
        # columns
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'random_place_JB'
        def __repr__(self): return '[dbo].[random_place_JB]'

        def insert(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None):
            sql = """INSERT INTO [dbo].[random_place_JB] ([Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr]) VALUES (?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ])


    class Dim_Amount_Band:
        # columns
        Band_ID = 'Band_ID'
        Interval_Start = 'Interval_Start'
        Interval_End = 'Interval_End'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Amount_Band'
        def __repr__(self): return '[dbo].[Dim_Amount_Band]'

        def insert(self, Band_ID: int, Interval_Start: int, Interval_End: int):
            sql = """INSERT INTO [dbo].[Dim_Amount_Band] ([Band_ID], [Interval_Start], [Interval_End]) VALUES (?, ?, ?);"""
            return self.dbx.get_result(sql, [ Band_ID, Interval_Start, Interval_End ])


    class Claim_ClientInfo:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Claim_ClientInfo'
        def __repr__(self): return '[dbo].[Claim_ClientInfo]'

        def insert(self, Source: str, Claim_ID: int = None, Technical_Client_ID: int = None, Insured_Name_Clean_ID: int = None, Company_ClientInfo_ID: int = None, Claim_Ref_ClientInfo: str = None, Date_of_Incident: date = None, Date_of_Notification: date = None, Country_ISO2_ID: int = None, Type_of_Loss: str = None, Loss_Event_ID: int = None, Claims_Description: str = None, Policy_Ref_ClientInfo: str = None, Policy_Ref_Clean: str = None, Policy_Inception_Date: date = None, Policy_Currency_ID: int = None, Insured_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Claim_ClientInfo] ([Claim_ID], [Technical_Client_ID], [Insured_Name_Clean_ID], [Company_ClientInfo_ID], [Claim_Ref_ClientInfo], [Date_of_Incident], [Date_of_Notification], [Country_ISO2_ID], [Type_of_Loss], [Loss_Event_ID], [Claims_Description], [Policy_Ref_ClientInfo], [Policy_Ref_Clean], [Policy_Inception_Date], [Policy_Currency_ID], [Source], [Insured_Name_ClientInfo_Uncut], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Claim_ID, Technical_Client_ID, Insured_Name_Clean_ID, Company_ClientInfo_ID, Claim_Ref_ClientInfo, Date_of_Incident, Date_of_Notification, Country_ISO2_ID, Type_of_Loss, Loss_Event_ID, Claims_Description, Policy_Ref_ClientInfo, Policy_Ref_Clean, Policy_Inception_Date, Policy_Currency_ID, Source, Insured_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ])


    class claims_bdx_jb:
        # columns
        Client_Limit_Orig_Curr = 'Client_Limit_Orig_Curr'
        Full_Limit_Orig_Curr = 'Full_Limit_Orig_Curr'
        Attachment_Orig_Curr = 'Attachment_Orig_Curr'
        Client_Premium_Orig_Curr = 'Client_Premium_Orig_Curr'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx_jb'
        def __repr__(self): return '[dbo].[claims_bdx_jb]'

        def insert(self, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None):
            sql = """INSERT INTO [dbo].[claims_bdx_jb] ([Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr]) VALUES (?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr ])


    class exposure_unpivoted_extended:
        # columns
        id = 'id'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        PolicyColumn = 'PolicyColumn'
        value = 'value'
        Band_Index = 'Band_Index'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'exposure_unpivoted_extended'
        def __repr__(self): return '[dbo].[exposure_unpivoted_extended]'

        def insert(self, id: int, ID_Arbeitsvorrat: str = None, PolicyColumn: str = None, value: float = None, Band_Index: str = None):
            sql = """INSERT INTO [dbo].[exposure_unpivoted_extended] ([id], [ID_Arbeitsvorrat], [PolicyColumn], [value], [Band_Index]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ id, ID_Arbeitsvorrat, PolicyColumn, value, Band_Index ])


    class Turnover_History:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Turnover_History'
        def __repr__(self): return '[dbo].[Turnover_History]'

        def insert(self, Turnover_ID: int, Company_ID: int, Turnover_Year: int, Turnover: float, Currency_ID: int, Source_of_Change: str, Is_Manually_Curated: int = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Turnover_History] ([Turnover_ID], [Company_ID], [Turnover_Year], [Turnover], [Currency_ID], [Source_of_Change], [Is_Manually_Curated], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Turnover_ID, Company_ID, Turnover_Year, Turnover, Currency_ID, Source_of_Change, Is_Manually_Curated, Create_Time, Change_Time, Changed_By ])


    class tDimCompany_Segment:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimCompany_Segment'
        def __repr__(self): return '[dbo].[tDimCompany_Segment]'

        def insert(self, Company_Segment: str, Index: int, Indexed_Company_Seg_Name: str, Company_Sub_Segment: str, Index_Sub_Segment: int, Indexed_Company_Sub_Seg_Name: str, RangeStart: str = None, RangeEnd: str = None, IntervalStart: int = None, IntervalEnd: int = None):
            sql = """INSERT INTO [dbo].[tDimCompany_Segment] ([Company_Segment], [Index], [Indexed_Company_Seg_Name], [Company_Sub_Segment], [Index_Sub_Segment], [Indexed_Company_Sub_Seg_Name], [RangeStart], [RangeEnd], [IntervalStart], [IntervalEnd]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_Segment, Index, Indexed_Company_Seg_Name, Company_Sub_Segment, Index_Sub_Segment, Indexed_Company_Sub_Seg_Name, RangeStart, RangeEnd, IntervalStart, IntervalEnd ])


    class Dim_Tag_Role:
        # columns
        Tag_Role_ID = 'Tag_Role_ID'
        Tag_Role = 'Tag_Role'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Tag_Role'
        def __repr__(self): return '[dbo].[Dim_Tag_Role]'

        def insert(self, Tag_Role: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Tag_Role] ([Tag_Role], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Tag_Role, Create_Time, Change_Time, Changed_By ])


    class _04_Status_Data_Preperation:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '04_Status_Data_Preperation'
        def __repr__(self): return '[dbo].[04_Status_Data_Preperation]'

        def insert(self, ID: int, _4_eye_check_done_: str = None, BU: str = None, BuPa: int = None, Business_Contact: str = None, Client_Name: str = None, Comment: str = None, Contact_Project_Team: str = None, Cyber_Expo_sure__Claims: str = None, Data_as_of: date = None, Deadline: date = None, First_Check_Claim__Expo_sure_Linkable: str = None, FSRI_Treaty_Nr: int = None, Insured_Name_available: str = None, Kind_of_data_delivery__in_force__from_to__quarterly__etc__: str = None, NDA_Status: str = None, Original_file_name: str = None, PML_has_been_sent_: str = None, PML_needs_to_be_done_: str = None, Policy_ID: str = None, Priority: str = None, Q_A_with_UW_done_: str = None, R_Code_manipulation_done_: str = None, Responsible_for_4_eye_check: str = None, Run_function_executed_: str = None, Smart_matching_done_: str = None, Specify_reporting_threshhold: str = None, SRAC_has_been_sent_: str = None, SRAC_needs_to_be_done_: str = None, Tab_name: str = None, UW_sign_off_done_: str = None, UW_Year: int = None, UWPF_Nr: int = None, Validation_issues_resolved_: str = None):
            sql = """INSERT INTO [dbo].[04_Status_Data_Preperation] ([4-eye check done?], [BU], [BuPa], [Business Contact], [Client_Name], [Comment], [Contact Project Team], [Cyber Expo-sure/ Claims], [Data as of], [Deadline], [First Check Claim/ Expo-sure Linkable], [FSRI Treaty Nr], [ID], [Insured Name available], [Kind of data delivery (in-force, from-to, quarterly, etc.)], [NDA Status], [Original file name], [PML has been sent?], [PML needs to be done?], [Policy ID], [Priority], [Q&A with UW done?], [R Code manipulation done?], [Responsible for 4-eye check], [Run function executed?], [Smart matching done?], [Specify reporting threshhold], [SRAC has been sent?], [SRAC needs to be done?], [Tab name], [UW sign-off done?], [UW Year], [UWPF Nr], [Validation issues resolved?]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ _4_eye_check_done_, BU, BuPa, Business_Contact, Client_Name, Comment, Contact_Project_Team, Cyber_Expo_sure__Claims, Data_as_of, Deadline, First_Check_Claim__Expo_sure_Linkable, FSRI_Treaty_Nr, ID, Insured_Name_available, Kind_of_data_delivery__in_force__from_to__quarterly__etc__, NDA_Status, Original_file_name, PML_has_been_sent_, PML_needs_to_be_done_, Policy_ID, Priority, Q_A_with_UW_done_, R_Code_manipulation_done_, Responsible_for_4_eye_check, Run_function_executed_, Smart_matching_done_, Specify_reporting_threshhold, SRAC_has_been_sent_, SRAC_needs_to_be_done_, Tab_name, UW_sign_off_done_, UW_Year, UWPF_Nr, Validation_issues_resolved_ ])


    class _03_Policy_Columns:
        # columns
        Column_Name = 'Column_Name'
        Column_Short = 'Column_Short'
        Column_Order = 'Column_Order'
        Order = 'Order'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '03_Policy_Columns'
        def __repr__(self): return '[dbo].[03_Policy_Columns]'

        def insert(self, Column_Name: str = None, Column_Short: str = None, Column_Order: str = None, Order: int = None):
            sql = """INSERT INTO [dbo].[03_Policy_Columns] ([Column_Name], [Column_Short], [Column_Order], [Order]) VALUES (?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Column_Name, Column_Short, Column_Order, Order ])


    class tDimClaims_Cause_Quality:
        # columns
        QualityKey = 'QualityKey'
        Quality = 'Quality'
        Index = 'Index'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimClaims_Cause_Quality'
        def __repr__(self): return '[dbo].[tDimClaims_Cause_Quality]'

        def insert(self, Quality: str, Index: int):
            sql = """INSERT INTO [dbo].[tDimClaims_Cause_Quality] ([Quality], [Index]) VALUES (?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Quality, Index ])


    class _06_Cambridge:
        # columns
        Cambridge_Code_Number = 'Cambridge_Code_Number'
        Cambridge_Code_Number_Agg = 'Cambridge_Code_Number_Agg'
        Cambridge_Full_Agg_Name = 'Cambridge_Full_Agg_Name'
        Cambridge_Full_Name = 'Cambridge_Full_Name'
        Cambridge_Name = 'Cambridge_Name'
        Cambridge_Name_Agg = 'Cambridge_Name_Agg'
        Cambridge_Rank = 'Cambridge_Rank'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '06_Cambridge'
        def __repr__(self): return '[dbo].[06_Cambridge]'

        def insert(self, Cambridge_Code_Number: float, Cambridge_Code_Number_Agg: int = None, Cambridge_Full_Agg_Name: str = None, Cambridge_Full_Name: str = None, Cambridge_Name: str = None, Cambridge_Name_Agg: str = None, Cambridge_Rank: int = None):
            sql = """INSERT INTO [dbo].[06_Cambridge] ([Cambridge_Code_Number], [Cambridge_Code_Number_Agg], [Cambridge_Full_Agg_Name], [Cambridge_Full_Name], [Cambridge_Name], [Cambridge_Name_Agg], [Cambridge_Rank]) VALUES (?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Cambridge_Code_Number, Cambridge_Code_Number_Agg, Cambridge_Full_Agg_Name, Cambridge_Full_Name, Cambridge_Name, Cambridge_Name_Agg, Cambridge_Rank ])


    class test_bdx:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'test_bdx'
        def __repr__(self): return '[dbo].[test_bdx]'

        def insert(self, ID_Arbeitsvorrat: str, rowNr: int, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: date = None, Policy_Expiry_Date: date = None, Product_Name_ClientInfo: str = None, Coverage: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Premium_Orig_Curr: float = None, Client_GrossNet_Premium_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Turnover_ClientInfo: float = None, Turnover_Year_ClientInfo: int = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_No_of_Employees: int = None, PII_Records_Stored: int = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, Coverage_1_Sublimit_Data_Breach_1st: float = None, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd: float = None, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd: float = None, Coverage_4_Sublimit_RestorationData: float = None, Coverage_5_Sublimit_Reputation: float = None, Coverage_6_Sublimit_Business_Interruption: float = None, Coverage_7_Sublimit_Contingent_Business_Interruption: float = None, Coverage_8_Sublimit_Extortion: float = None, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud: float = None, Coverage_10_Sublimit_PCI_DSS: float = None, Coverage_11_Sublimit_Network_Security: float = None, Coverage_12_Sublimit_Media_Liability: float = None, Coverage_13_Sublimit_Tech_PI_E_and_O: float = None, Coverage_14_Sublimit_D_and_O: float = None, folderId: int = None, folderName: str = None, folderPath: str = None, fileId: int = None, fileName: str = None, sheetInFileIdx: int = None, sheetName: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, Turnover_ClientInfo_USD: float = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Client_Premium_USD: float = None, Client_GrossNet_Premium_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, BvD_ID: str = None, Risk_ID: str = None, Tower_ID: str = None, Duplicate_ID: str = None, ID_Arbeitsvorrat_MR_share: str = None):
            sql = """INSERT INTO [dbo].[test_bdx] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Product_Name_ClientInfo], [Coverage], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Premium_Orig_Curr], [Client_GrossNet_Premium_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Turnover_ClientInfo], [Turnover_Year_ClientInfo], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_No_of_Employees], [PII_Records_Stored], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [Coverage_1_Sublimit_Data_Breach_1st], [Coverage_2_Sublimit_Data_Breach_privacy_event_3rd], [Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd], [Coverage_4_Sublimit_RestorationData], [Coverage_5_Sublimit_Reputation], [Coverage_6_Sublimit_Business_Interruption], [Coverage_7_Sublimit_Contingent_Business_Interruption], [Coverage_8_Sublimit_Extortion], [Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud], [Coverage_10_Sublimit_PCI_DSS], [Coverage_11_Sublimit_Network_Security], [Coverage_12_Sublimit_Media_Liability], [Coverage_13_Sublimit_Tech_PI_E_and_O], [Coverage_14_Sublimit_D_and_O], [folderId], [folderName], [folderPath], [fileId], [fileName], [sheetInFileIdx], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [Turnover_ClientInfo_USD], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Client_Premium_USD], [Client_GrossNet_Premium_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [BvD_ID], [Risk_ID], [Tower_ID], [Duplicate_ID], [ID_Arbeitsvorrat_MR_share]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Product_Name_ClientInfo, Coverage, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Premium_Orig_Curr, Client_GrossNet_Premium_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Turnover_ClientInfo, Turnover_Year_ClientInfo, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_No_of_Employees, PII_Records_Stored, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, Coverage_1_Sublimit_Data_Breach_1st, Coverage_2_Sublimit_Data_Breach_privacy_event_3rd, Coverage_3_Sublimit_Data_Breach_Combined_1st_and_3rd, Coverage_4_Sublimit_RestorationData, Coverage_5_Sublimit_Reputation, Coverage_6_Sublimit_Business_Interruption, Coverage_7_Sublimit_Contingent_Business_Interruption, Coverage_8_Sublimit_Extortion, Coverage_9_Sublimit_Cyber_Crime_Financial_theft_and_fraud, Coverage_10_Sublimit_PCI_DSS, Coverage_11_Sublimit_Network_Security, Coverage_12_Sublimit_Media_Liability, Coverage_13_Sublimit_Tech_PI_E_and_O, Coverage_14_Sublimit_D_and_O, folderId, folderName, folderPath, fileId, fileName, sheetInFileIdx, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, Turnover_ClientInfo_USD, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Client_Premium_USD, Client_GrossNet_Premium_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, BvD_ID, Risk_ID, Tower_ID, Duplicate_ID, ID_Arbeitsvorrat_MR_share ])


    class _10_Company_Segment:
        # columns
        Company_Segment_Name = 'Company_Segment_Name'
        Index = 'Index'
        Indexed_Company_Seg_Name = 'Indexed_Company_Seg_Name'
        Range_End = 'Range End'
        Range_Start = 'Range Start'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '10_Company_Segment'
        def __repr__(self): return '[dbo].[10_Company_Segment]'

        def insert(self, Company_Segment_Name: str, Index: int, Indexed_Company_Seg_Name: str = None, Range_End: str = None, Range_Start: str = None):
            sql = """INSERT INTO [dbo].[10_Company_Segment] ([Company_Segment_Name], [Index], [Indexed_Company_Seg_Name], [Range End], [Range Start]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Company_Segment_Name, Index, Indexed_Company_Seg_Name, Range_End, Range_Start ])


    class unread_values:
        # columns
        id = 'id'
        row = 'row'
        ID_Arbeitsvorrat = 'ID_Arbeitsvorrat'
        Column = 'Column'
        Value = 'Value'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'unread_values'
        def __repr__(self): return '[dbo].[unread_values]'

        def insert(self, row: int, ID_Arbeitsvorrat: str, Column: str = None, Value: str = None):
            sql = """INSERT INTO [dbo].[unread_values] ([row], [ID_Arbeitsvorrat], [Column], [Value]) VALUES (?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ row, ID_Arbeitsvorrat, Column, Value ])


    class tDimDate:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'tDimDate'
        def __repr__(self): return '[dbo].[tDimDate]'

        def insert(self, DateKey: int, Date: date, Year: str = None, Qtr: str = None, Qtr_sort: int = None, Month: str = None, Month_sort: int = None, MonthText_long: str = None, MonthText_short: str = None, Week: str = None, ISO_Year: str = None, ISO_Week: str = None, Day_of_Week: int = None, Day_of_Week_Name: str = None):
            sql = """INSERT INTO [dbo].[tDimDate] ([DateKey], [Date], [Year], [Qtr], [Qtr_sort], [Month], [Month_sort], [MonthText_long], [MonthText_short], [Week], [ISO Year], [ISO Week], [Day_of_Week], [Day_of_Week_Name]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ DateKey, Date, Year, Qtr, Qtr_sort, Month, Month_sort, MonthText_long, MonthText_short, Week, ISO_Year, ISO_Week, Day_of_Week, Day_of_Week_Name ])


    class Client_Name_Exceptions:
        # columns
        ProcessId = 'ProcessId'
        Business_Client_Name = 'Business_Client_Name'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Client_Name_Exceptions'
        def __repr__(self): return '[dbo].[Client_Name_Exceptions]'

        def insert(self, ProcessId: int, Business_Client_Name: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Client_Name_Exceptions] ([ProcessId], [Business_Client_Name], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ProcessId, Business_Client_Name, Create_Time, Change_Time, Changed_By ])


    class Processes:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Processes'
        def __repr__(self): return '[dbo].[Processes]'

        def insert(self, Id: int, OperationFlag: int, InsertionDate: datetime, LastModifiedDate: datetime, BusinessUnit: int, Priority: int, ID_Arbeitsvorrat: str, Deadline: datetime = None, BusinessContact: str = None, SmartMatchingContact: str = None, ProjectTeamContact: str = None, FourEyeCheckContact: str = None, Comment: str = None, SignOffToken: str = None, CreatedBy: str = None, LastModifiedBy: str = None):
            sql = """INSERT INTO [dbo].[Processes] ([Id], [OperationFlag], [InsertionDate], [LastModifiedDate], [Deadline], [BusinessContact], [BusinessUnit], [Priority], [SmartMatchingContact], [ProjectTeamContact], [FourEyeCheckContact], [Comment], [SignOffToken], [ID_Arbeitsvorrat], [CreatedBy], [LastModifiedBy]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Id, OperationFlag, InsertionDate, LastModifiedDate, Deadline, BusinessContact, BusinessUnit, Priority, SmartMatchingContact, ProjectTeamContact, FourEyeCheckContact, Comment, SignOffToken, ID_Arbeitsvorrat, CreatedBy, LastModifiedBy ])


    class claims_bdx_temporary_jb:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'claims_bdx_temporary_jb'
        def __repr__(self): return '[dbo].[claims_bdx_temporary_jb]'

        def insert(self, ID_Arbeitsvorrat: str = None, Client: str = None, Insured_Name_ClientInfo: str = None, Policy_ID_ClientInfo: str = None, Policy_Inception_Date: str = None, Policy_Expiry_Date: str = None, Currency: str = None, Client_Limit_Orig_Curr: float = None, Full_Limit_Orig_Curr: float = None, Attachment_Orig_Curr: float = None, Client_Share_of_Limit: float = None, SIR_Orig_Curr: float = None, BI_Waiting_Period_in_Hours: float = None, Trade_Level_ClientInfo: str = None, Trade_Level_Code_Number_ClientInfo: str = None, Trade_Level_Code_Standard_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ClientInfo: str = None, Insured_Homepage: str = None, FSRI_Treaty: str = None, FSRI_Section: str = None, FSRI_Period: str = None, Claim_ID_ClientInfo: str = None, Is_Claim_Closed: str = None, Date_of_Incident: str = None, Date_of_Notification: str = None, Claims_Description: str = None, Type_of_Loss: str = None, Country_of_Loss_Settlement: str = None, Nr_Affected_Records: str = None, Duration_of_Outage_hours: float = None, Value_as_Of_Date: str = None, Loss_Currency: str = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, Threshold_Orig_Curr_unind: float = None, Observation_Period_Start: str = None, fileId: str = None, fileName: str = None, sheetName: str = None, rowNr: str = None, DELETE_indicator: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None, IsCensored: int = None, Client_Limit_USD: float = None, Full_Limit_USD: float = None, Attachment_USD: float = None, SIR_USD: float = None, Incurred_Insured_FGU_USD: float = None, Paid_Client_Share_USD: float = None, Incurred_Client_Share_USD: float = None, Threshold_unind_USD: float = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Country_ISO2: str = None, Country_of_Loss_Settlement_ISO2: str = None, BvD_ID: str = None, Duplicate_ID: str = None, Internal_Claim_ID: str = None):
            sql = """INSERT INTO [dbo].[claims_bdx_temporary_jb] ([ID_Arbeitsvorrat], [Client], [Insured_Name_ClientInfo], [Policy_ID_ClientInfo], [Policy_Inception_Date], [Policy_Expiry_Date], [Currency], [Client_Limit_Orig_Curr], [Full_Limit_Orig_Curr], [Attachment_Orig_Curr], [Client_Share_of_Limit], [SIR_Orig_Curr], [BI_Waiting_Period_in_Hours], [Trade_Level_ClientInfo], [Trade_Level_Code_Number_ClientInfo], [Trade_Level_Code_Standard_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ClientInfo], [Insured_Homepage], [FSRI_Treaty], [FSRI_Section], [FSRI_Period], [Claim_ID_ClientInfo], [Is_Claim_Closed], [Date_of_Incident], [Date_of_Notification], [Claims_Description], [Type_of_Loss], [Country_of_Loss_Settlement], [Nr_Affected_Records], [Duration_of_Outage_hours], [Value_as_Of_Date], [Loss_Currency], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [Threshold_Orig_Curr_unind], [Observation_Period_Start], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [Create_Time], [Change_Time], [Changed_By], [IsCensored], [Client_Limit_USD], [Full_Limit_USD], [Attachment_USD], [SIR_USD], [Incurred_Insured_FGU_USD], [Paid_Client_Share_USD], [Incurred_Client_Share_USD], [Threshold_unind_USD], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Country_ISO2], [Country_of_Loss_Settlement_ISO2], [BvD_ID], [Duplicate_ID], [Internal_Claim_ID]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ ID_Arbeitsvorrat, Client, Insured_Name_ClientInfo, Policy_ID_ClientInfo, Policy_Inception_Date, Policy_Expiry_Date, Currency, Client_Limit_Orig_Curr, Full_Limit_Orig_Curr, Attachment_Orig_Curr, Client_Share_of_Limit, SIR_Orig_Curr, BI_Waiting_Period_in_Hours, Trade_Level_ClientInfo, Trade_Level_Code_Number_ClientInfo, Trade_Level_Code_Standard_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ClientInfo, Insured_Homepage, FSRI_Treaty, FSRI_Section, FSRI_Period, Claim_ID_ClientInfo, Is_Claim_Closed, Date_of_Incident, Date_of_Notification, Claims_Description, Type_of_Loss, Country_of_Loss_Settlement, Nr_Affected_Records, Duration_of_Outage_hours, Value_as_Of_Date, Loss_Currency, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, Threshold_Orig_Curr_unind, Observation_Period_Start, fileId, fileName, sheetName, rowNr, DELETE_indicator, Create_Time, Change_Time, Changed_By, IsCensored, Client_Limit_USD, Full_Limit_USD, Attachment_USD, SIR_USD, Incurred_Insured_FGU_USD, Paid_Client_Share_USD, Incurred_Client_Share_USD, Threshold_unind_USD, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Country_ISO2, Country_of_Loss_Settlement_ISO2, BvD_ID, Duplicate_ID, Internal_Claim_ID ])


    class pgo_data:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'pgo_data'
        def __repr__(self): return '[dbo].[pgo_data]'

        def insert(self, PROGRAM_SUBSYSTEM_TREATY_ID: str = None, PRICING_TOOL: str = None, DWH_RI_CONTRACT_PERIOD_ID: float = None, PROGRAM_ID_OR_RIPP_ID: float = None, ADMIN_SYSTEM_RPP_ID: str = None, ISPROPERTYBUYBACK: str = None, PROGRAM_NAME: str = None, DWH_RI_COVERAGE_ID: float = None, TREATY_ID_OR_COVERAGE_ID: float = None, TREATY_NAME: str = None, FSRI_LINKAGES_FOR_TTY_BUSINESS: str = None, FMT_ID_FOR_FAB_BUSINESS: str = None, CLIENT_ID: str = None, CLIENT_NAME: str = None, BROKER_ID: str = None, BROKER_NAME: str = None, INSURED_ID: str = None, INSURED_NAME: str = None, RESULT_RESPONSIBILITY_ID: str = None, RESULT_RESPONSIBILITY_NAME: str = None, RESULT_RESPONSIBILITY_LVL_3: str = None, RESULT_RESPONSIBILITY_LVL_4: str = None, RESULT_RESPONSIBILITY_LVL_5: str = None, RESULT_RESPONSIBILITY_LVL_6: str = None, RESULT_RESP_PRESENTATION: str = None, UW_YEAR: float = None, BEGIN_DATE: datetime = None, END_DATE: datetime = None, UW_STATUS: str = None, NATURE_OF_TREATY: str = None, FAC_OR_TREATY: str = None, VIRTUAL_TREATY_FLAG: float = None, MR_SHARE: float = None, PROTECTED_SHARE: float = None, TOTAL_CYBER_COB_SHARE: float = None, ORIG_CURRENCY_ID: str = None, EXCHANGE_RATE: float = None, LIMIT_100_PCT: float = None, ATTACHMENT_POINT_100_PCT: float = None, EXP_TOTAL_PREMIUM_100_PCT: float = None, EXP_TOTAL_DEDUCTIONS_100_PCT: float = None, EXP_ULTIMATE_LOSS_100_PCT: float = None, EXP_TOTAL_PREMIUM_100P_CYB: float = None, EXP_TOTAL_DEDUCTIONS_100P_CYB: float = None, EXP_ULT_LOSS_100P_CYB: float = None, EXP_TOTAL_PREMIUM_MR_SHARE: float = None, EXP_TOTAL_DEDUCTIONS_MR_SHARE: float = None, EXP_ULT_LOSS_MR_SHARE: float = None, EXP_TOTAL_PREMIUM_MR_SHARE_CYB: float = None, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB: float = None, EXP_ULT_LOSS_MR_SHARE_CYB: float = None, ULTIMATE_LOSS_RATIO: float = None, TECHNICAL_COMBINED_RATIO: float = None, COMBINED_RATIO: float = None, EXP_PV_UW_RESLT_MR_SHARE: float = None, RORAC: float = None, IDENTITY_THEFT_SHARE: float = None, DATA_COMPROMISE_SHARE: float = None, CYBER_THIRD_PARTY_SHARE: float = None, CYBER_THIRD_PARTY_FI_SHARE: float = None, CYBER_THIRD_PARTY_COMM_SHARE: float = None, CYBER_FIRST_PARTY_SHARE: float = None, CYBER_FIRST_PARTY_PERS_SHARE: float = None, CYBER_INDUCED_PROP_DMG_SHARE: float = None, CYBER_MULTIPERIL_TPL_SHARE: float = None, MARINE_CYBER_THIRD_PARTY_SHARE: float = None, MARINE_CYBER_FIRST_PARTY_SHARE: float = None, PRICING_BUDGET_ITV: float = None, PRICING_BUDGET_DB: float = None, PRICING_BUDGET_IF: float = None, PRICING_BUDGET_CLOUD: float = None, TOTAL_PREMIUM_ASAT_100_PCT: float = None, TOTAL_DEDUCTIONS_ASAT_100_PCT: float = None, ULT_LOSS_ASAT_100_PCT: float = None, TOTAL_PREMIUM_ASAT_100P_CYB: float = None, TOTAL_DEDUCTIONS_ASAT_100P_CYB: float = None, ULT_LOSS_ASAT_100P_CYB: float = None, TOTAL_PREMIUM_ASAT_MR_SHARE: float = None, TOTAL_DEDUCTIONS_ASAT_MR_SHARE: float = None, ULT_LOSS_ASAT_MR_SHARE: float = None, TOTAL_PREM_ASAT_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB: float = None, ULT_LOSS_ASAT_MR_SHARE_CYB: float = None, TOTAL_PREMIUM_PROJ_100_PCT: float = None, TOTAL_DEDUCTIONS_PROJ_100_PCT: float = None, ULT_LOSS_PROJ_100_PCT: float = None, TOTAL_PREMIUM_PROJ_100P_CYB: float = None, TOTAL_DEDUCTIONS_PROJ_100P_CYB: float = None, ULT_LOSS_PROJ_100P_CYB: float = None, TOTAL_PREMIUM_PROJ_MR_SHARE: float = None, TOTAL_DEDUCTIONS_PROJ_MR_SHARE: float = None, ULT_LOSS_PROJ_MR_SHARE: float = None, TOTAL_PREM_PROJ_MR_SHARE_CYB: float = None, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB: float = None, ULT_LOSS_PROJ_MR_SHARE_CYB: float = None):
            sql = """INSERT INTO [dbo].[pgo_data] ([PROGRAM_SUBSYSTEM_TREATY_ID], [PRICING_TOOL], [DWH_RI_CONTRACT_PERIOD_ID], [PROGRAM_ID_OR_RIPP_ID], [ADMIN_SYSTEM_RPP_ID], [ISPROPERTYBUYBACK], [PROGRAM_NAME], [DWH_RI_COVERAGE_ID], [TREATY_ID_OR_COVERAGE_ID], [TREATY_NAME], [FSRI_LINKAGES_FOR_TTY_BUSINESS], [FMT_ID_FOR_FAB_BUSINESS], [CLIENT_ID], [CLIENT_NAME], [BROKER_ID], [BROKER_NAME], [INSURED_ID], [INSURED_NAME], [RESULT_RESPONSIBILITY_ID], [RESULT_RESPONSIBILITY_NAME], [RESULT_RESPONSIBILITY_LVL_3], [RESULT_RESPONSIBILITY_LVL_4], [RESULT_RESPONSIBILITY_LVL_5], [RESULT_RESPONSIBILITY_LVL_6], [RESULT_RESP_PRESENTATION], [UW_YEAR], [BEGIN_DATE], [END_DATE], [UW_STATUS], [NATURE_OF_TREATY], [FAC_OR_TREATY], [VIRTUAL_TREATY_FLAG], [MR_SHARE], [PROTECTED_SHARE], [TOTAL_CYBER_COB_SHARE], [ORIG_CURRENCY_ID], [EXCHANGE_RATE], [LIMIT_100_PCT], [ATTACHMENT_POINT_100_PCT], [EXP_TOTAL_PREMIUM_100_PCT], [EXP_TOTAL_DEDUCTIONS_100_PCT], [EXP_ULTIMATE_LOSS_100_PCT], [EXP_TOTAL_PREMIUM_100P_CYB], [EXP_TOTAL_DEDUCTIONS_100P_CYB], [EXP_ULT_LOSS_100P_CYB], [EXP_TOTAL_PREMIUM_MR_SHARE], [EXP_TOTAL_DEDUCTIONS_MR_SHARE], [EXP_ULT_LOSS_MR_SHARE], [EXP_TOTAL_PREMIUM_MR_SHARE_CYB], [EXP_TOTAL_DEDUCTS_MR_SHARE_CYB], [EXP_ULT_LOSS_MR_SHARE_CYB], [ULTIMATE_LOSS_RATIO], [TECHNICAL_COMBINED_RATIO], [COMBINED_RATIO], [EXP_PV_UW_RESLT_MR_SHARE], [RORAC], [IDENTITY_THEFT_SHARE], [DATA_COMPROMISE_SHARE], [CYBER_THIRD_PARTY_SHARE], [CYBER_THIRD_PARTY_FI_SHARE], [CYBER_THIRD_PARTY_COMM_SHARE], [CYBER_FIRST_PARTY_SHARE], [CYBER_FIRST_PARTY_PERS_SHARE], [CYBER_INDUCED_PROP_DMG_SHARE], [CYBER_MULTIPERIL_TPL_SHARE], [MARINE_CYBER_THIRD_PARTY_SHARE], [MARINE_CYBER_FIRST_PARTY_SHARE], [PRICING_BUDGET_ITV], [PRICING_BUDGET_DB], [PRICING_BUDGET_IF], [PRICING_BUDGET_CLOUD], [TOTAL_PREMIUM_ASAT_100_PCT], [TOTAL_DEDUCTIONS_ASAT_100_PCT], [ULT_LOSS_ASAT_100_PCT], [TOTAL_PREMIUM_ASAT_100P_CYB], [TOTAL_DEDUCTIONS_ASAT_100P_CYB], [ULT_LOSS_ASAT_100P_CYB], [TOTAL_PREMIUM_ASAT_MR_SHARE], [TOTAL_DEDUCTIONS_ASAT_MR_SHARE], [ULT_LOSS_ASAT_MR_SHARE], [TOTAL_PREM_ASAT_MR_SHARE_CYB], [TOTAL_DEDUCT_ASAT_MR_SHARE_CYB], [ULT_LOSS_ASAT_MR_SHARE_CYB], [TOTAL_PREMIUM_PROJ_100_PCT], [TOTAL_DEDUCTIONS_PROJ_100_PCT], [ULT_LOSS_PROJ_100_PCT], [TOTAL_PREMIUM_PROJ_100P_CYB], [TOTAL_DEDUCTIONS_PROJ_100P_CYB], [ULT_LOSS_PROJ_100P_CYB], [TOTAL_PREMIUM_PROJ_MR_SHARE], [TOTAL_DEDUCTIONS_PROJ_MR_SHARE], [ULT_LOSS_PROJ_MR_SHARE], [TOTAL_PREM_PROJ_MR_SHARE_CYB], [TOTAL_DEDUCT_PROJ_MR_SHARE_CYB], [ULT_LOSS_PROJ_MR_SHARE_CYB]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ PROGRAM_SUBSYSTEM_TREATY_ID, PRICING_TOOL, DWH_RI_CONTRACT_PERIOD_ID, PROGRAM_ID_OR_RIPP_ID, ADMIN_SYSTEM_RPP_ID, ISPROPERTYBUYBACK, PROGRAM_NAME, DWH_RI_COVERAGE_ID, TREATY_ID_OR_COVERAGE_ID, TREATY_NAME, FSRI_LINKAGES_FOR_TTY_BUSINESS, FMT_ID_FOR_FAB_BUSINESS, CLIENT_ID, CLIENT_NAME, BROKER_ID, BROKER_NAME, INSURED_ID, INSURED_NAME, RESULT_RESPONSIBILITY_ID, RESULT_RESPONSIBILITY_NAME, RESULT_RESPONSIBILITY_LVL_3, RESULT_RESPONSIBILITY_LVL_4, RESULT_RESPONSIBILITY_LVL_5, RESULT_RESPONSIBILITY_LVL_6, RESULT_RESP_PRESENTATION, UW_YEAR, BEGIN_DATE, END_DATE, UW_STATUS, NATURE_OF_TREATY, FAC_OR_TREATY, VIRTUAL_TREATY_FLAG, MR_SHARE, PROTECTED_SHARE, TOTAL_CYBER_COB_SHARE, ORIG_CURRENCY_ID, EXCHANGE_RATE, LIMIT_100_PCT, ATTACHMENT_POINT_100_PCT, EXP_TOTAL_PREMIUM_100_PCT, EXP_TOTAL_DEDUCTIONS_100_PCT, EXP_ULTIMATE_LOSS_100_PCT, EXP_TOTAL_PREMIUM_100P_CYB, EXP_TOTAL_DEDUCTIONS_100P_CYB, EXP_ULT_LOSS_100P_CYB, EXP_TOTAL_PREMIUM_MR_SHARE, EXP_TOTAL_DEDUCTIONS_MR_SHARE, EXP_ULT_LOSS_MR_SHARE, EXP_TOTAL_PREMIUM_MR_SHARE_CYB, EXP_TOTAL_DEDUCTS_MR_SHARE_CYB, EXP_ULT_LOSS_MR_SHARE_CYB, ULTIMATE_LOSS_RATIO, TECHNICAL_COMBINED_RATIO, COMBINED_RATIO, EXP_PV_UW_RESLT_MR_SHARE, RORAC, IDENTITY_THEFT_SHARE, DATA_COMPROMISE_SHARE, CYBER_THIRD_PARTY_SHARE, CYBER_THIRD_PARTY_FI_SHARE, CYBER_THIRD_PARTY_COMM_SHARE, CYBER_FIRST_PARTY_SHARE, CYBER_FIRST_PARTY_PERS_SHARE, CYBER_INDUCED_PROP_DMG_SHARE, CYBER_MULTIPERIL_TPL_SHARE, MARINE_CYBER_THIRD_PARTY_SHARE, MARINE_CYBER_FIRST_PARTY_SHARE, PRICING_BUDGET_ITV, PRICING_BUDGET_DB, PRICING_BUDGET_IF, PRICING_BUDGET_CLOUD, TOTAL_PREMIUM_ASAT_100_PCT, TOTAL_DEDUCTIONS_ASAT_100_PCT, ULT_LOSS_ASAT_100_PCT, TOTAL_PREMIUM_ASAT_100P_CYB, TOTAL_DEDUCTIONS_ASAT_100P_CYB, ULT_LOSS_ASAT_100P_CYB, TOTAL_PREMIUM_ASAT_MR_SHARE, TOTAL_DEDUCTIONS_ASAT_MR_SHARE, ULT_LOSS_ASAT_MR_SHARE, TOTAL_PREM_ASAT_MR_SHARE_CYB, TOTAL_DEDUCT_ASAT_MR_SHARE_CYB, ULT_LOSS_ASAT_MR_SHARE_CYB, TOTAL_PREMIUM_PROJ_100_PCT, TOTAL_DEDUCTIONS_PROJ_100_PCT, ULT_LOSS_PROJ_100_PCT, TOTAL_PREMIUM_PROJ_100P_CYB, TOTAL_DEDUCTIONS_PROJ_100P_CYB, ULT_LOSS_PROJ_100P_CYB, TOTAL_PREMIUM_PROJ_MR_SHARE, TOTAL_DEDUCTIONS_PROJ_MR_SHARE, ULT_LOSS_PROJ_MR_SHARE, TOTAL_PREM_PROJ_MR_SHARE_CYB, TOTAL_DEDUCT_PROJ_MR_SHARE_CYB, ULT_LOSS_PROJ_MR_SHARE_CYB ])


    class Dim_Portfolio_Tag:
        # columns
        Portfolio_Tag_ID = 'Portfolio_Tag_ID'
        Portfolio_Tag = 'Portfolio_Tag'
        Tag_Role_ID = 'Tag_Role_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Portfolio_Tag'
        def __repr__(self): return '[dbo].[Dim_Portfolio_Tag]'

        def insert(self, Portfolio_Tag: str, Tag_Role_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Portfolio_Tag] ([Portfolio_Tag], [Tag_Role_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Portfolio_Tag, Tag_Role_ID, Create_Time, Change_Time, Changed_By ])


    class _13_Claims_Incurred_Band:
        # columns
        Segmentation_Name = 'Segmentation_Name'
        Aggregated_Segmentation = 'Aggregated_Segmentation'
        Sort_Aggr = 'Sort_Aggr'
        Index = 'Index'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return '13_Claims_Incurred_Band'
        def __repr__(self): return '[dbo].[13_Claims_Incurred_Band]'

        def insert(self, Index: int, Segmentation_Name: str = None, Aggregated_Segmentation: str = None, Sort_Aggr: int = None):
            sql = """INSERT INTO [dbo].[13_Claims_Incurred_Band] ([Segmentation_Name], [Aggregated_Segmentation], [Sort_Aggr], [Index]) VALUES (?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Segmentation_Name, Aggregated_Segmentation, Sort_Aggr, Index ])


    class Claim_Development:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Claim_Development'
        def __repr__(self): return '[dbo].[Claim_Development]'

        def insert(self, Process_ID: int, rowNr: int, Claim_ClientInfo_ID: int = None, Value_as_of_Date: date = None, Claim_Unified_Status_ID: int = None, Claim_Closed_Date: date = None, Signal_Reserve_ID: int = None, Loss_Currency_ID: int = None, Threshold_unindexed_Orig_Curr: float = None, Incurred_Insured_FGU_Orig_Curr: float = None, Paid_Client_Share_Orig_Curr: float = None, Incurred_Client_Share_Orig_Curr: float = None, fileId: int = None, fileName: str = None, sheetName: str = None, DELETE_indicator: str = None, Duplicate_ID: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Claim_Development] ([Process_ID], [Claim_ClientInfo_ID], [Value_as_of_Date], [Claim_Unified_Status_ID], [Claim_Closed_Date], [Signal_Reserve_ID], [Loss_Currency_ID], [Threshold_unindexed_Orig_Curr], [Incurred_Insured_FGU_Orig_Curr], [Paid_Client_Share_Orig_Curr], [Incurred_Client_Share_Orig_Curr], [fileId], [fileName], [sheetName], [rowNr], [DELETE_indicator], [Duplicate_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Process_ID, Claim_ClientInfo_ID, Value_as_of_Date, Claim_Unified_Status_ID, Claim_Closed_Date, Signal_Reserve_ID, Loss_Currency_ID, Threshold_unindexed_Orig_Curr, Incurred_Insured_FGU_Orig_Curr, Paid_Client_Share_Orig_Curr, Incurred_Client_Share_Orig_Curr, fileId, fileName, sheetName, rowNr, DELETE_indicator, Duplicate_ID, Create_Time, Change_Time, Changed_By ])


    class Dim_Policy_Type:
        # columns
        Policy_Type_ID = 'Policy_Type_ID'
        Policy_Type = 'Policy_Type'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Dim_Policy_Type'
        def __repr__(self): return '[dbo].[Dim_Policy_Type]'

        def insert(self, Policy_Type: str, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Dim_Policy_Type] ([Policy_Type], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Policy_Type, Create_Time, Change_Time, Changed_By ])


    class sysdiagrams:
        # columns
        name = 'name'
        principal_id = 'principal_id'
        diagram_id = 'diagram_id'
        version = 'version'
        definition = 'definition'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'sysdiagrams'
        def __repr__(self): return '[dbo].[sysdiagrams]'

        def insert(self, name: str, principal_id: int, version: int = None, definition: bytes = None):
            sql = """INSERT INTO [dbo].[sysdiagrams] ([name], [principal_id], [version], [definition]) VALUES (?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ name, principal_id, version, definition ])


    class company_lookup_temp:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'company_lookup_temp'
        def __repr__(self): return '[dbo].[company_lookup_temp]'

        def insert(self, company_id: int = None, duplicate_of_id: int = None, Insured_Name_Clean: str = None, Insured_Name_ClientInfo: str = None, Insured_Street: str = None, Insured_City: str = None, Insured_ZIP_Code: str = None, Insured_State: str = None, Insured_Country_ISO2: str = None, Trade_Level_Name_ClientInfo_Mapped_Cambridge: str = None, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge: str = None, Insured_Homepage: str = None, Turnover_ClientInfo_USD: float = None, bvd_id: str = None, manual_curated_entry: int = None, combined_entry: int = None):
            sql = """INSERT INTO [dbo].[company_lookup_temp] ([company_id], [duplicate_of_id], [Insured_Name_Clean], [Insured_Name_ClientInfo], [Insured_Street], [Insured_City], [Insured_ZIP_Code], [Insured_State], [Insured_Country_ISO2], [Trade_Level_Name_ClientInfo_Mapped_Cambridge], [Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge], [Insured_Homepage], [Turnover_ClientInfo_USD], [bvd_id], [manual_curated_entry], [combined_entry]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ company_id, duplicate_of_id, Insured_Name_Clean, Insured_Name_ClientInfo, Insured_Street, Insured_City, Insured_ZIP_Code, Insured_State, Insured_Country_ISO2, Trade_Level_Name_ClientInfo_Mapped_Cambridge, Trade_Level_CodeNumber_ClientInfo_Mapped_Cambridge, Insured_Homepage, Turnover_ClientInfo_USD, bvd_id, manual_curated_entry, combined_entry ])


    class Company_ClientInfo:
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

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Company_ClientInfo'
        def __repr__(self): return '[dbo].[Company_ClientInfo]'

        def insert(self, Source: str, Company_ID: int = None, Company_Name_Clean_ID: int = None, Company_Name_ClientInfo: str = None, Country_ISO2_ID: int = None, City_ClientInfo_ID: int = None, Industry_ClientInfo_ID: int = None, Street: str = None, ZIP_Code: str = None, State_ID: int = None, Domain_Name: str = None, Company_Name_ClientInfo_Uncut: str = None, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Company_ClientInfo] ([Company_ID], [Company_Name_Clean_ID], [Company_Name_ClientInfo], [Country_ISO2_ID], [City_ClientInfo_ID], [Industry_ClientInfo_ID], [Street], [ZIP_Code], [State_ID], [Domain_Name], [Source], [Company_Name_ClientInfo_Uncut], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

SELECT SCOPE_IDENTITY() as [scope_identity];"""
            return self.dbx.get_result(sql, [ Company_ID, Company_Name_Clean_ID, Company_Name_ClientInfo, Country_ISO2_ID, City_ClientInfo_ID, Industry_ClientInfo_ID, Street, ZIP_Code, State_ID, Domain_Name, Source, Company_Name_ClientInfo_Uncut, Create_Time, Change_Time, Changed_By ])


    class Bridge_Portfolio_Tags:
        # columns
        Portfolio_Tag_ID = 'Portfolio_Tag_ID'
        Exposure_ID = 'Exposure_ID'
        Create_Time = 'Create_Time'
        Change_Time = 'Change_Time'
        Changed_By = 'Changed_By'

        def __init__(self, cn: (pyodbc.Connection | str)):
            self.dbx = DbExec(cn)

        def __str__(self): return 'Bridge_Portfolio_Tags'
        def __repr__(self): return '[dbo].[Bridge_Portfolio_Tags]'

        def insert(self, Portfolio_Tag_ID: int, Exposure_ID: int, Create_Time: datetime = None, Change_Time: datetime = None, Changed_By: str = None):
            sql = """INSERT INTO [dbo].[Bridge_Portfolio_Tags] ([Portfolio_Tag_ID], [Exposure_ID], [Create_Time], [Change_Time], [Changed_By]) VALUES (?, ?, ?, ?, ?);"""
            return self.dbx.get_result(sql, [ Portfolio_Tag_ID, Exposure_ID, Create_Time, Change_Time, Changed_By ])


