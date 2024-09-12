from mrtest import DbExec, Result
import pyodbc
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class CyberDataPoolDboRoutines:
    def __init__(self, cn: (pyodbc.Connection | str)):
        self.dbx = DbExec(cn)

    def add_Claim_BDX(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[add_Claim_BDX]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def trunc_exposure_bdx(self) -> Result:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[trunc_exposure_bdx];
"""
        return self.dbx.get_result(sql, [  ])

    def chk_Dim_Policy_Type(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Policy_Type]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_BI_Waiting_Period(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_BI_Waiting_Period]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def add_DnB(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_DnB]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_DnB(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_DnB]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def del_DnB(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[del_DnB]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def add_DnB_Matching(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @checkForExisting bit = ?;
DECLARE @suppressOutput bit = ?;

EXECUTE @_return_value = [dbo].[add_DnB_Matching]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@checkForExisting = @checkForExisting
    ,@suppressOutput = @suppressOutput;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, checkForExisting, suppressOutput ])

    def upd_Company_ClientInfo(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Company_ClientInfo]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def del_DnB_Matching(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[del_DnB_Matching]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def add_Dim_Company_Name_Clean(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Company_Name_Clean]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Dim_Company_Name_Clean(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Company_Name_Clean]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_Dim_State(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_State]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Dim_State(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_State]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def usp_ensure_temp_to_target_table_columns(self, target_table: str, temp_table: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @target_table sysname(256) = ?;
DECLARE @temp_table sysname(256) = ?;

EXECUTE @_return_value = [dbo].[usp_ensure_temp_to_target_table_columns]
    @target_table = @target_table
    ,@temp_table = @temp_table;
"""
        return self.dbx.get_result(sql, [ target_table, temp_table ])

    def chk_Dim_Claim_Status(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Claim_Status]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_Loss_Event(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Loss_Event]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_Signal_Reserve(self, InputTableName: str, ColProcessID: str, ColReserveAmount: str, ColSignalReserve: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;
DECLARE @ColReserveAmount nvarchar(100) = ?;
DECLARE @ColSignalReserve nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Signal_Reserve]
    @InputTableName = @InputTableName
    ,@ColProcessID = @ColProcessID
    ,@ColReserveAmount = @ColReserveAmount
    ,@ColSignalReserve = @ColSignalReserve;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColProcessID, ColReserveAmount, ColSignalReserve ])

    def add_Dim_Country(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Country]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Dim_Country(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Country]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def chk_Dim_City(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_City]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_Country(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Country]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def usp_merge_Dim_City(self, Session_ID: UUID, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @Session_ID uniqueidentifier = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[usp_merge_Dim_City]
    @Session_ID = @Session_ID
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ Session_ID, InputVariables ])

    def chk_Dim_Currency(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Currency]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_Industry(self, InputTableName: str, ColCodeStandard: str, ColCodeNumber: str, ColDescription: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColCodeStandard nvarchar(100) = ?;
DECLARE @ColCodeNumber nvarchar(100) = ?;
DECLARE @ColDescription nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Industry]
    @InputTableName = @InputTableName
    ,@ColCodeStandard = @ColCodeStandard
    ,@ColCodeNumber = @ColCodeNumber
    ,@ColDescription = @ColDescription;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColCodeStandard, ColCodeNumber, ColDescription ])

    def chk_Dim_Industry_Other(self, InputTableName: str, ColCodeStandard: str, ColCodeNumber: str, ColDescription: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColCodeStandard nvarchar(100) = ?;
DECLARE @ColCodeNumber nvarchar(100) = ?;
DECLARE @ColDescription nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Industry_Other]
    @InputTableName = @InputTableName
    ,@ColCodeStandard = @ColCodeStandard
    ,@ColCodeNumber = @ColCodeNumber
    ,@ColDescription = @ColDescription;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColCodeStandard, ColCodeNumber, ColDescription ])

    def chk_Dim_State(self, InputTableName: str, ColState: str, ColCountry: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColState nvarchar(100) = ?;
DECLARE @ColCountry nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_State]
    @InputTableName = @InputTableName
    ,@ColState = @ColState
    ,@ColCountry = @ColCountry;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColState, ColCountry ])

    def upd_Dim_Industry(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Industry]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_Dim_Industry(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Industry]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_File_Level_Status(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @checkForExisting bit = ?;
DECLARE @suppressOutput bit = ?;

EXECUTE @_return_value = [dbo].[add_File_Level_Status]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@checkForExisting = @checkForExisting
    ,@suppressOutput = @suppressOutput;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, checkForExisting, suppressOutput ])

    def chk_DnB_Matching(self, InputTableName: str, getCompanyID: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @getCompanyID bit = ?;

EXECUTE @_return_value = [dbo].[chk_DnB_Matching]
    @InputTableName = @InputTableName
    ,@getCompanyID = @getCompanyID;
"""
        return self.dbx.get_result(sql, [ InputTableName, getCompanyID ])

    def upd_File_Level_Status(self, InputTableName: str, InputVariables: str, autoAdd: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @autoAdd bit = ?;

EXECUTE @_return_value = [dbo].[upd_File_Level_Status]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@autoAdd = @autoAdd;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, autoAdd ])

    def del_File_Level_Status(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[del_File_Level_Status]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def chk_Claim(self, InputTableName: str, ColID: str, ColClient: str, ColClaimClientInfoID: str, ColInsured: str, ColCountry: str, ColLossEvent: str, ColCurrency: str, ColLossType: str, ColCompClientInfoID: str, ColClaimID: str, ColClaimCleanID: str, ColIncidentDate: str, ColNotificationDate: str, ColClaimDescription: str, ColPolicyID: str, ColPolicyCleanID: str, ColInceptionDate: str, ColProcessID: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColID nvarchar(100) = ?;
DECLARE @ColClient nvarchar(100) = ?;
DECLARE @ColClaimClientInfoID nvarchar(100) = ?;
DECLARE @ColInsured nvarchar(100) = ?;
DECLARE @ColCountry nvarchar(100) = ?;
DECLARE @ColLossEvent nvarchar(100) = ?;
DECLARE @ColCurrency nvarchar(100) = ?;
DECLARE @ColLossType nvarchar(100) = ?;
DECLARE @ColCompClientInfoID nvarchar(100) = ?;
DECLARE @ColClaimID nvarchar(100) = ?;
DECLARE @ColClaimCleanID nvarchar(100) = ?;
DECLARE @ColIncidentDate nvarchar(100) = ?;
DECLARE @ColNotificationDate nvarchar(100) = ?;
DECLARE @ColClaimDescription nvarchar(100) = ?;
DECLARE @ColPolicyID nvarchar(100) = ?;
DECLARE @ColPolicyCleanID nvarchar(100) = ?;
DECLARE @ColInceptionDate nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Claim]
    @InputTableName = @InputTableName
    ,@ColID = @ColID
    ,@ColClient = @ColClient
    ,@ColClaimClientInfoID = @ColClaimClientInfoID
    ,@ColInsured = @ColInsured
    ,@ColCountry = @ColCountry
    ,@ColLossEvent = @ColLossEvent
    ,@ColCurrency = @ColCurrency
    ,@ColLossType = @ColLossType
    ,@ColCompClientInfoID = @ColCompClientInfoID
    ,@ColClaimID = @ColClaimID
    ,@ColClaimCleanID = @ColClaimCleanID
    ,@ColIncidentDate = @ColIncidentDate
    ,@ColNotificationDate = @ColNotificationDate
    ,@ColClaimDescription = @ColClaimDescription
    ,@ColPolicyID = @ColPolicyID
    ,@ColPolicyCleanID = @ColPolicyCleanID
    ,@ColInceptionDate = @ColInceptionDate
    ,@ColProcessID = @ColProcessID;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColID, ColClient, ColClaimClientInfoID, ColInsured, ColCountry, ColLossEvent, ColCurrency, ColLossType, ColCompClientInfoID, ColClaimID, ColClaimCleanID, ColIncidentDate, ColNotificationDate, ColClaimDescription, ColPolicyID, ColPolicyCleanID, ColInceptionDate, ColProcessID ])

    def upd_DnB_Matching(self, InputTableName: str, InputVariables: str, autoAdd: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @autoAdd bit = ?;

EXECUTE @_return_value = [dbo].[upd_DnB_Matching]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@autoAdd = @autoAdd;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, autoAdd ])

    def add_Dim_Client_Name(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Client_Name]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Dim_Client_Name(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Client_Name]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def del_Dim_Client_Name(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[del_Dim_Client_Name]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def chk_Company(self, InputTableName: str, ColID: str, ColCompanyName: str, ColCompanyNameClean: str, ColCountry: str, ColCity: str, ColState: str, ColStreet: str, ColZIP: str, ColIndustryCodeStandard: str, ColIndustryCodeNumber: str, ColIndustryDescription: str, ColDomain: str, ColProcessID: str, ColCompClientInfoID: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColID nvarchar(100) = ?;
DECLARE @ColCompanyName nvarchar(100) = ?;
DECLARE @ColCompanyNameClean nvarchar(100) = ?;
DECLARE @ColCountry nvarchar(100) = ?;
DECLARE @ColCity nvarchar(100) = ?;
DECLARE @ColState nvarchar(100) = ?;
DECLARE @ColStreet nvarchar(100) = ?;
DECLARE @ColZIP nvarchar(100) = ?;
DECLARE @ColIndustryCodeStandard nvarchar(100) = ?;
DECLARE @ColIndustryCodeNumber nvarchar(100) = ?;
DECLARE @ColIndustryDescription nvarchar(100) = ?;
DECLARE @ColDomain nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;
DECLARE @ColCompClientInfoID nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Company]
    @InputTableName = @InputTableName
    ,@ColID = @ColID
    ,@ColCompanyName = @ColCompanyName
    ,@ColCompanyNameClean = @ColCompanyNameClean
    ,@ColCountry = @ColCountry
    ,@ColCity = @ColCity
    ,@ColState = @ColState
    ,@ColStreet = @ColStreet
    ,@ColZIP = @ColZIP
    ,@ColIndustryCodeStandard = @ColIndustryCodeStandard
    ,@ColIndustryCodeNumber = @ColIndustryCodeNumber
    ,@ColIndustryDescription = @ColIndustryDescription
    ,@ColDomain = @ColDomain
    ,@ColProcessID = @ColProcessID
    ,@ColCompClientInfoID = @ColCompClientInfoID;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColID, ColCompanyName, ColCompanyNameClean, ColCountry, ColCity, ColState, ColStreet, ColZIP, ColIndustryCodeStandard, ColIndustryCodeNumber, ColIndustryDescription, ColDomain, ColProcessID, ColCompClientInfoID ])

    def chk_Turnover(self, InputTableName: str, ColCurrency: str, ColTurnover: str, ColInceptionDate: str, ColCompClientInfoID: str, ColProcessID: str, ColDelete: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColCurrency nvarchar(100) = ?;
DECLARE @ColTurnover nvarchar(100) = ?;
DECLARE @ColInceptionDate nvarchar(100) = ?;
DECLARE @ColCompClientInfoID nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;
DECLARE @ColDelete nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Turnover]
    @InputTableName = @InputTableName
    ,@ColCurrency = @ColCurrency
    ,@ColTurnover = @ColTurnover
    ,@ColInceptionDate = @ColInceptionDate
    ,@ColCompClientInfoID = @ColCompClientInfoID
    ,@ColProcessID = @ColProcessID
    ,@ColDelete = @ColDelete;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColCurrency, ColTurnover, ColInceptionDate, ColCompClientInfoID, ColProcessID, ColDelete ])

    def add_Dim_Tag_Role(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Tag_Role]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def chk_Bridge_Portfolio_Tags(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Bridge_Portfolio_Tags]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def upd_Dim_Tag_Role(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Tag_Role]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_Company(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Company]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def chk_Dim_Tag_Role(self, InputTableName: str, ColNameToCheck: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColNameToCheck nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Tag_Role]
    @InputTableName = @InputTableName
    ,@ColNameToCheck = @ColNameToCheck;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColNameToCheck ])

    def chk_Dim_Client_Name(self, InputTableName: str, ColProcessID: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Client_Name]
    @InputTableName = @InputTableName
    ,@ColProcessID = @ColProcessID;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColProcessID ])

    def add_Turnover(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @checkForExisting bit = ?;
DECLARE @suppressOutput bit = ?;

EXECUTE @_return_value = [dbo].[add_Turnover]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@checkForExisting = @checkForExisting
    ,@suppressOutput = @suppressOutput;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, checkForExisting, suppressOutput ])

    def upd_Company(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Company]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Turnover(self, InputTableName: str, InputVariables: str, autoAdd: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;
DECLARE @autoAdd bit = ?;

EXECUTE @_return_value = [dbo].[upd_Turnover]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables
    ,@autoAdd = @autoAdd;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables, autoAdd ])

    def chk_Dim_Product(self, InputTableName: str, ColProcessID: str, ColProduct: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColProcessID nvarchar(100) = ?;
DECLARE @ColProduct nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Product]
    @InputTableName = @InputTableName
    ,@ColProcessID = @ColProcessID
    ,@ColProduct = @ColProduct;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColProcessID, ColProduct ])

    def add_Exposure_BDX(self, InputTableName: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[add_Exposure_BDX]
    @InputTableName = @InputTableName;
"""
        return self.dbx.get_result(sql, [ InputTableName ])

    def add_Dim_City(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_City]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_Dim_Portfolio_Tag(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Dim_Portfolio_Tag]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def chk_Dim_Portfolio_Tag(self, InputTableName: str, ColTag: str, ColTagRole: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @ColTag nvarchar(100) = ?;
DECLARE @ColTagRole nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[chk_Dim_Portfolio_Tag]
    @InputTableName = @InputTableName
    ,@ColTag = @ColTag
    ,@ColTagRole = @ColTagRole;
"""
        return self.dbx.get_result(sql, [ InputTableName, ColTag, ColTagRole ])

    def upd_Dim_City(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_City]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def upd_Dim_Portfolio_Tag(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[upd_Dim_Portfolio_Tag]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def add_Bridge_Portfolio_Tags(self, InputTableName: str, InputVariables: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @InputVariables nvarchar(max) = ?;

EXECUTE @_return_value = [dbo].[add_Bridge_Portfolio_Tags]
    @InputTableName = @InputTableName
    ,@InputVariables = @InputVariables;
"""
        return self.dbx.get_result(sql, [ InputTableName, InputVariables ])

    def sp_upgraddiagrams(self) -> Result:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[sp_upgraddiagrams];
"""
        return self.dbx.get_result(sql, [  ])

    def add_BDX_data(self, InputTableName: str, OutputTableName: str, ColID: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = ?;
DECLARE @OutputTableName nvarchar(100) = ?;
DECLARE @ColID nvarchar(100) = ?;

EXECUTE @_return_value = [dbo].[add_BDX_data]
    @InputTableName = @InputTableName
    ,@OutputTableName = @OutputTableName
    ,@ColID = @ColID;
"""
        return self.dbx.get_result(sql, [ InputTableName, OutputTableName, ColID ])

    def sp_helpdiagrams(self, diagramname: str, owner_id: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @diagramname sysname(256) = ?;
DECLARE @owner_id int = ?;

EXECUTE @_return_value = [dbo].[sp_helpdiagrams]
    @diagramname = @diagramname
    ,@owner_id = @owner_id;
"""
        return self.dbx.get_result(sql, [ diagramname, owner_id ])

