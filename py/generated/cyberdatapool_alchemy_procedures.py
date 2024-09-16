from sqlalchemy import Connection, CursorResult, text
from mrtest import get_alchemy_connection
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class CyberDataPoolDboAlchemyProcedures:
    def __init__(self, cnOrStr: (Connection | str)):
        self.con = get_alchemy_connection(cnOrStr)


    def add_Claim_BDX(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[add_Claim_BDX]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def trunc_exposure_bdx(self) -> CursorResult:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[trunc_exposure_bdx];

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), {  })

    def chk_Dim_Policy_Type(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Policy_Type]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_BI_Waiting_Period(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_BI_Waiting_Period]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def add_DnB(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_DnB]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_DnB(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_DnB]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def del_DnB(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[del_DnB]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def add_DnB_Matching(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @checkForExisting bit = :checkForExisting;
DECLARE @suppressOutput bit = :suppressOutput;

EXECUTE @_return_value = [dbo].[add_DnB_Matching]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@checkForExisting = :checkForExisting
    ,@suppressOutput = :suppressOutput;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'checkForExisting': checkForExisting, 'suppressOutput': suppressOutput })

    def upd_Company_ClientInfo(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Company_ClientInfo]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def del_DnB_Matching(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[del_DnB_Matching]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def add_Dim_Company_Name_Clean(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Company_Name_Clean]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Dim_Company_Name_Clean(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Company_Name_Clean]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def add_Dim_State(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_State]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Dim_State(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_State]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def usp_ensure_temp_to_target_table_columns(self, target_table: str, temp_table: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @target_table sysname(256) = :target_table;
DECLARE @temp_table sysname(256) = :temp_table;

EXECUTE @_return_value = [dbo].[usp_ensure_temp_to_target_table_columns]
    @target_table = :target_table
    ,@temp_table = :temp_table;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'target_table': target_table, 'temp_table': temp_table })

    def chk_Dim_Claim_Status(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Claim_Status]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_Loss_Event(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Loss_Event]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_Signal_Reserve(self, InputTableName: str, ColProcessID: str, ColReserveAmount: str, ColSignalReserve: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;
DECLARE @ColReserveAmount nvarchar(100) = :ColReserveAmount;
DECLARE @ColSignalReserve nvarchar(100) = :ColSignalReserve;

EXECUTE @_return_value = [dbo].[chk_Dim_Signal_Reserve]
    @InputTableName = :InputTableName
    ,@ColProcessID = :ColProcessID
    ,@ColReserveAmount = :ColReserveAmount
    ,@ColSignalReserve = :ColSignalReserve;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColProcessID': ColProcessID, 'ColReserveAmount': ColReserveAmount, 'ColSignalReserve': ColSignalReserve })

    def add_Dim_Country(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Country]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Dim_Country(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Country]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def chk_Dim_City(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_City]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_Country(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Country]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def usp_merge_Dim_City(self, Session_ID: UUID, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @Session_ID uniqueidentifier = :Session_ID;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[usp_merge_Dim_City]
    @Session_ID = :Session_ID
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'Session_ID': Session_ID, 'InputVariables': InputVariables })

    def chk_Dim_Currency(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Currency]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_Industry(self, InputTableName: str, ColCodeStandard: str, ColCodeNumber: str, ColDescription: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColCodeStandard nvarchar(100) = :ColCodeStandard;
DECLARE @ColCodeNumber nvarchar(100) = :ColCodeNumber;
DECLARE @ColDescription nvarchar(100) = :ColDescription;

EXECUTE @_return_value = [dbo].[chk_Dim_Industry]
    @InputTableName = :InputTableName
    ,@ColCodeStandard = :ColCodeStandard
    ,@ColCodeNumber = :ColCodeNumber
    ,@ColDescription = :ColDescription;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColCodeStandard': ColCodeStandard, 'ColCodeNumber': ColCodeNumber, 'ColDescription': ColDescription })

    def chk_Dim_Industry_Other(self, InputTableName: str, ColCodeStandard: str, ColCodeNumber: str, ColDescription: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColCodeStandard nvarchar(100) = :ColCodeStandard;
DECLARE @ColCodeNumber nvarchar(100) = :ColCodeNumber;
DECLARE @ColDescription nvarchar(100) = :ColDescription;

EXECUTE @_return_value = [dbo].[chk_Dim_Industry_Other]
    @InputTableName = :InputTableName
    ,@ColCodeStandard = :ColCodeStandard
    ,@ColCodeNumber = :ColCodeNumber
    ,@ColDescription = :ColDescription;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColCodeStandard': ColCodeStandard, 'ColCodeNumber': ColCodeNumber, 'ColDescription': ColDescription })

    def chk_Dim_State(self, InputTableName: str, ColState: str, ColCountry: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColState nvarchar(100) = :ColState;
DECLARE @ColCountry nvarchar(100) = :ColCountry;

EXECUTE @_return_value = [dbo].[chk_Dim_State]
    @InputTableName = :InputTableName
    ,@ColState = :ColState
    ,@ColCountry = :ColCountry;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColState': ColState, 'ColCountry': ColCountry })

    def usp_merge_Dim_Client_Name(self, Session_ID: UUID, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @Session_ID uniqueidentifier = :Session_ID;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[usp_merge_Dim_Client_Name]
    @Session_ID = :Session_ID
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'Session_ID': Session_ID, 'InputVariables': InputVariables })

    def usp_merge_Dim_Company_Name_Clean(self, Session_ID: UUID, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @Session_ID uniqueidentifier = :Session_ID;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[usp_merge_Dim_Company_Name_Clean]
    @Session_ID = :Session_ID
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'Session_ID': Session_ID, 'InputVariables': InputVariables })

    def upd_Dim_Industry(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Industry]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def usp_merge_Dim_Country(self, Session_ID: UUID, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @Session_ID uniqueidentifier = :Session_ID;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[usp_merge_Dim_Country]
    @Session_ID = :Session_ID
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'Session_ID': Session_ID, 'InputVariables': InputVariables })

    def add_Dim_Industry(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Industry]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def add_File_Level_Status(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @checkForExisting bit = :checkForExisting;
DECLARE @suppressOutput bit = :suppressOutput;

EXECUTE @_return_value = [dbo].[add_File_Level_Status]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@checkForExisting = :checkForExisting
    ,@suppressOutput = :suppressOutput;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'checkForExisting': checkForExisting, 'suppressOutput': suppressOutput })

    def chk_DnB_Matching(self, InputTableName: str, getCompanyID: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @getCompanyID bit = :getCompanyID;

EXECUTE @_return_value = [dbo].[chk_DnB_Matching]
    @InputTableName = :InputTableName
    ,@getCompanyID = :getCompanyID;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'getCompanyID': getCompanyID })

    def upd_File_Level_Status(self, InputTableName: str, InputVariables: str, autoAdd: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @autoAdd bit = :autoAdd;

EXECUTE @_return_value = [dbo].[upd_File_Level_Status]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@autoAdd = :autoAdd;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'autoAdd': autoAdd })

    def del_File_Level_Status(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[del_File_Level_Status]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def chk_Claim(self, InputTableName: str, ColID: str, ColClient: str, ColClaimClientInfoID: str, ColInsured: str, ColCountry: str, ColLossEvent: str, ColCurrency: str, ColLossType: str, ColCompClientInfoID: str, ColClaimID: str, ColClaimCleanID: str, ColIncidentDate: str, ColNotificationDate: str, ColClaimDescription: str, ColPolicyID: str, ColPolicyCleanID: str, ColInceptionDate: str, ColProcessID: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColID nvarchar(100) = :ColID;
DECLARE @ColClient nvarchar(100) = :ColClient;
DECLARE @ColClaimClientInfoID nvarchar(100) = :ColClaimClientInfoID;
DECLARE @ColInsured nvarchar(100) = :ColInsured;
DECLARE @ColCountry nvarchar(100) = :ColCountry;
DECLARE @ColLossEvent nvarchar(100) = :ColLossEvent;
DECLARE @ColCurrency nvarchar(100) = :ColCurrency;
DECLARE @ColLossType nvarchar(100) = :ColLossType;
DECLARE @ColCompClientInfoID nvarchar(100) = :ColCompClientInfoID;
DECLARE @ColClaimID nvarchar(100) = :ColClaimID;
DECLARE @ColClaimCleanID nvarchar(100) = :ColClaimCleanID;
DECLARE @ColIncidentDate nvarchar(100) = :ColIncidentDate;
DECLARE @ColNotificationDate nvarchar(100) = :ColNotificationDate;
DECLARE @ColClaimDescription nvarchar(100) = :ColClaimDescription;
DECLARE @ColPolicyID nvarchar(100) = :ColPolicyID;
DECLARE @ColPolicyCleanID nvarchar(100) = :ColPolicyCleanID;
DECLARE @ColInceptionDate nvarchar(100) = :ColInceptionDate;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;

EXECUTE @_return_value = [dbo].[chk_Claim]
    @InputTableName = :InputTableName
    ,@ColID = :ColID
    ,@ColClient = :ColClient
    ,@ColClaimClientInfoID = :ColClaimClientInfoID
    ,@ColInsured = :ColInsured
    ,@ColCountry = :ColCountry
    ,@ColLossEvent = :ColLossEvent
    ,@ColCurrency = :ColCurrency
    ,@ColLossType = :ColLossType
    ,@ColCompClientInfoID = :ColCompClientInfoID
    ,@ColClaimID = :ColClaimID
    ,@ColClaimCleanID = :ColClaimCleanID
    ,@ColIncidentDate = :ColIncidentDate
    ,@ColNotificationDate = :ColNotificationDate
    ,@ColClaimDescription = :ColClaimDescription
    ,@ColPolicyID = :ColPolicyID
    ,@ColPolicyCleanID = :ColPolicyCleanID
    ,@ColInceptionDate = :ColInceptionDate
    ,@ColProcessID = :ColProcessID;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColID': ColID, 'ColClient': ColClient, 'ColClaimClientInfoID': ColClaimClientInfoID, 'ColInsured': ColInsured, 'ColCountry': ColCountry, 'ColLossEvent': ColLossEvent, 'ColCurrency': ColCurrency, 'ColLossType': ColLossType, 'ColCompClientInfoID': ColCompClientInfoID, 'ColClaimID': ColClaimID, 'ColClaimCleanID': ColClaimCleanID, 'ColIncidentDate': ColIncidentDate, 'ColNotificationDate': ColNotificationDate, 'ColClaimDescription': ColClaimDescription, 'ColPolicyID': ColPolicyID, 'ColPolicyCleanID': ColPolicyCleanID, 'ColInceptionDate': ColInceptionDate, 'ColProcessID': ColProcessID })

    def upd_DnB_Matching(self, InputTableName: str, InputVariables: str, autoAdd: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @autoAdd bit = :autoAdd;

EXECUTE @_return_value = [dbo].[upd_DnB_Matching]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@autoAdd = :autoAdd;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'autoAdd': autoAdd })

    def add_Dim_Client_Name(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Client_Name]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Dim_Client_Name(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Client_Name]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def del_Dim_Client_Name(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[del_Dim_Client_Name]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def chk_Company(self, InputTableName: str, ColID: str, ColCompanyName: str, ColCompanyNameClean: str, ColCountry: str, ColCity: str, ColState: str, ColStreet: str, ColZIP: str, ColIndustryCodeStandard: str, ColIndustryCodeNumber: str, ColIndustryDescription: str, ColDomain: str, ColProcessID: str, ColCompClientInfoID: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColID nvarchar(100) = :ColID;
DECLARE @ColCompanyName nvarchar(100) = :ColCompanyName;
DECLARE @ColCompanyNameClean nvarchar(100) = :ColCompanyNameClean;
DECLARE @ColCountry nvarchar(100) = :ColCountry;
DECLARE @ColCity nvarchar(100) = :ColCity;
DECLARE @ColState nvarchar(100) = :ColState;
DECLARE @ColStreet nvarchar(100) = :ColStreet;
DECLARE @ColZIP nvarchar(100) = :ColZIP;
DECLARE @ColIndustryCodeStandard nvarchar(100) = :ColIndustryCodeStandard;
DECLARE @ColIndustryCodeNumber nvarchar(100) = :ColIndustryCodeNumber;
DECLARE @ColIndustryDescription nvarchar(100) = :ColIndustryDescription;
DECLARE @ColDomain nvarchar(100) = :ColDomain;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;
DECLARE @ColCompClientInfoID nvarchar(100) = :ColCompClientInfoID;

EXECUTE @_return_value = [dbo].[chk_Company]
    @InputTableName = :InputTableName
    ,@ColID = :ColID
    ,@ColCompanyName = :ColCompanyName
    ,@ColCompanyNameClean = :ColCompanyNameClean
    ,@ColCountry = :ColCountry
    ,@ColCity = :ColCity
    ,@ColState = :ColState
    ,@ColStreet = :ColStreet
    ,@ColZIP = :ColZIP
    ,@ColIndustryCodeStandard = :ColIndustryCodeStandard
    ,@ColIndustryCodeNumber = :ColIndustryCodeNumber
    ,@ColIndustryDescription = :ColIndustryDescription
    ,@ColDomain = :ColDomain
    ,@ColProcessID = :ColProcessID
    ,@ColCompClientInfoID = :ColCompClientInfoID;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColID': ColID, 'ColCompanyName': ColCompanyName, 'ColCompanyNameClean': ColCompanyNameClean, 'ColCountry': ColCountry, 'ColCity': ColCity, 'ColState': ColState, 'ColStreet': ColStreet, 'ColZIP': ColZIP, 'ColIndustryCodeStandard': ColIndustryCodeStandard, 'ColIndustryCodeNumber': ColIndustryCodeNumber, 'ColIndustryDescription': ColIndustryDescription, 'ColDomain': ColDomain, 'ColProcessID': ColProcessID, 'ColCompClientInfoID': ColCompClientInfoID })

    def chk_Turnover(self, InputTableName: str, ColCurrency: str, ColTurnover: str, ColInceptionDate: str, ColCompClientInfoID: str, ColProcessID: str, ColDelete: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColCurrency nvarchar(100) = :ColCurrency;
DECLARE @ColTurnover nvarchar(100) = :ColTurnover;
DECLARE @ColInceptionDate nvarchar(100) = :ColInceptionDate;
DECLARE @ColCompClientInfoID nvarchar(100) = :ColCompClientInfoID;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;
DECLARE @ColDelete nvarchar(100) = :ColDelete;

EXECUTE @_return_value = [dbo].[chk_Turnover]
    @InputTableName = :InputTableName
    ,@ColCurrency = :ColCurrency
    ,@ColTurnover = :ColTurnover
    ,@ColInceptionDate = :ColInceptionDate
    ,@ColCompClientInfoID = :ColCompClientInfoID
    ,@ColProcessID = :ColProcessID
    ,@ColDelete = :ColDelete;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColCurrency': ColCurrency, 'ColTurnover': ColTurnover, 'ColInceptionDate': ColInceptionDate, 'ColCompClientInfoID': ColCompClientInfoID, 'ColProcessID': ColProcessID, 'ColDelete': ColDelete })

    def add_Dim_Tag_Role(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Tag_Role]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def chk_Bridge_Portfolio_Tags(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[chk_Bridge_Portfolio_Tags]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def upd_Dim_Tag_Role(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Tag_Role]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def add_Company(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Company]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def chk_Dim_Tag_Role(self, InputTableName: str, ColNameToCheck: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColNameToCheck nvarchar(100) = :ColNameToCheck;

EXECUTE @_return_value = [dbo].[chk_Dim_Tag_Role]
    @InputTableName = :InputTableName
    ,@ColNameToCheck = :ColNameToCheck;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColNameToCheck': ColNameToCheck })

    def chk_Dim_Client_Name(self, InputTableName: str, ColProcessID: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;

EXECUTE @_return_value = [dbo].[chk_Dim_Client_Name]
    @InputTableName = :InputTableName
    ,@ColProcessID = :ColProcessID;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColProcessID': ColProcessID })

    def add_Turnover(self, InputTableName: str, InputVariables: str, checkForExisting: int, suppressOutput: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @checkForExisting bit = :checkForExisting;
DECLARE @suppressOutput bit = :suppressOutput;

EXECUTE @_return_value = [dbo].[add_Turnover]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@checkForExisting = :checkForExisting
    ,@suppressOutput = :suppressOutput;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'checkForExisting': checkForExisting, 'suppressOutput': suppressOutput })

    def upd_Company(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Company]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Turnover(self, InputTableName: str, InputVariables: str, autoAdd: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;
DECLARE @autoAdd bit = :autoAdd;

EXECUTE @_return_value = [dbo].[upd_Turnover]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables
    ,@autoAdd = :autoAdd;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables, 'autoAdd': autoAdd })

    def chk_Dim_Product(self, InputTableName: str, ColProcessID: str, ColProduct: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColProcessID nvarchar(100) = :ColProcessID;
DECLARE @ColProduct nvarchar(100) = :ColProduct;

EXECUTE @_return_value = [dbo].[chk_Dim_Product]
    @InputTableName = :InputTableName
    ,@ColProcessID = :ColProcessID
    ,@ColProduct = :ColProduct;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColProcessID': ColProcessID, 'ColProduct': ColProduct })

    def add_Exposure_BDX(self, InputTableName: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;

EXECUTE @_return_value = [dbo].[add_Exposure_BDX]
    @InputTableName = :InputTableName;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName })

    def add_Dim_City(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_City]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def add_Dim_Portfolio_Tag(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Dim_Portfolio_Tag]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def chk_Dim_Portfolio_Tag(self, InputTableName: str, ColTag: str, ColTagRole: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @ColTag nvarchar(100) = :ColTag;
DECLARE @ColTagRole nvarchar(100) = :ColTagRole;

EXECUTE @_return_value = [dbo].[chk_Dim_Portfolio_Tag]
    @InputTableName = :InputTableName
    ,@ColTag = :ColTag
    ,@ColTagRole = :ColTagRole;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'ColTag': ColTag, 'ColTagRole': ColTagRole })

    def upd_Dim_City(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_City]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def upd_Dim_Portfolio_Tag(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[upd_Dim_Portfolio_Tag]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def add_Bridge_Portfolio_Tags(self, InputTableName: str, InputVariables: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @InputVariables nvarchar(max) = :InputVariables;

EXECUTE @_return_value = [dbo].[add_Bridge_Portfolio_Tags]
    @InputTableName = :InputTableName
    ,@InputVariables = :InputVariables;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'InputVariables': InputVariables })

    def sp_upgraddiagrams(self) -> CursorResult:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[sp_upgraddiagrams];

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), {  })

    def add_BDX_data(self, InputTableName: str, OutputTableName: str, ColID: str) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @InputTableName nvarchar(100) = :InputTableName;
DECLARE @OutputTableName nvarchar(100) = :OutputTableName;
DECLARE @ColID nvarchar(100) = :ColID;

EXECUTE @_return_value = [dbo].[add_BDX_data]
    @InputTableName = :InputTableName
    ,@OutputTableName = :OutputTableName
    ,@ColID = :ColID;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'InputTableName': InputTableName, 'OutputTableName': OutputTableName, 'ColID': ColID })

    def sp_helpdiagrams(self, diagramname: str, owner_id: int) -> CursorResult:
        sql = """
DECLARE @_return_value INT;
DECLARE @diagramname sysname(256) = :diagramname;
DECLARE @owner_id int = :owner_id;

EXECUTE @_return_value = [dbo].[sp_helpdiagrams]
    @diagramname = :diagramname
    ,@owner_id = :owner_id;

SELECT @_return_value [return_value];
"""
        return self.con.execute(text(sql), { 'diagramname': diagramname, 'owner_id': owner_id })

