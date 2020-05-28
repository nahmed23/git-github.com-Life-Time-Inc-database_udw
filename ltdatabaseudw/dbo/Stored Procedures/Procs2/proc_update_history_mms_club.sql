CREATE PROC [dbo].[proc_update_history_mms_club] AS
begin

set nocount on
set xact_abort on

if object_id('tempdb.dbo.#stage_mms_Club_history_update') is not null drop table #stage_mms_Club_history_update
create table dbo.#stage_mms_Club_history_update with (location=user_db, distribution = hash(ClubID)) as
select 
STG.MMSClubKey,
STG.ClubID,
STG.ValRegionID,
STG.StatementMessageID,
STG.ValClubTypeID,
STG.DomainNamePrefix,
STG.ClubName,
STG.ReceiptFooter,
STG.DisplayUIFlag,
STG.CheckInGroupLevel,
STG.ValStatementTypeID,
STG.ChargeToAccountFlag,
STG.ValPreSaleID,
STG.ClubActivationDate,
STG.ValTimeZoneID,
STG.MMSInsertedDateTime,
STG.ValCWRegionID,
STG.EFTGroupID,
STG.GLTaxID,
STG.GLClubID,
STG.CRMDivisionCode,
STG.AssessJrMemberDuesFlag,
STG.SellJrMemberDuesFlag,
STG.MMSUpdatedDateTime,
STG.ClubCode,
STG.SiteID,
STG.NewMemberCardFlag,
STG.InsertedDateTime,
STG.InsertUser,
STG.BatchID,
STG.ETLSourceSystemKey,
STG.ValMemberActivityRegionID,
STG.IGStoreID,
STG.ChildCenterWeeklyLimit,
STG.ValSalesAreaID,
STG.ValPTRCLAreaID,
STG.KronosForecastMapPath,
STG.GLCashEntryCompanyName,
STG.GLCashEntryAccount,
STG.GLReceivablesEntryAccount,
STG.GLCashEntryCashSubAccount,
STG.GLCashEntryCreditCardSubAccount,
STG.GLReceivablesEntrySubAccount,
STG.GLReceivablesEntryCompanyName,
STG.FormalClubName,
STG.ClubDeactivationDate,
STG.ValCurrencyCodeID,
STG.AllowMultipleCurrencyFlag,
STG.WorkdayRegion,
STG.UpdatedDateTime,
STG.UpdateUser,
STG.AllowJuniorCheckInFlag,
STG.MarketingMapRegion,
STG.MarketingMapXmlStateName,
STG.MarketingClubLevel,
STG.LTFResourceID,
SAT.health_mms_club_identifier,
SAT.Charge_Next_Month_Date,
SAT.Max_Junior_Age,
SAT.Max_Secondary_Age,
SAT.Min_Front_Desk_Checkin_Age,
SAT.Max_Child_Center_Checkin_Age,
row_number() over(partition by STG.MMSClubKey order by STG.MMSClubKey ) as row_num
FROM stage_mms_Club_history STG 
LEFT JOIN
s_mms_club SAT 
ON STG.clubid=SAT.club_id
and isnull(STG.MMSUpdatedDateTime,getdate()) = isnull(SAT.Updated_Date_Time,getdate())
and isnull(STG.MMSinsertedDateTime,getdate()) = isnull(SAT.inserted_Date_Time,getdate())
and
(
SAT.health_mms_club_identifier is not null or 
SAT.Charge_Next_Month_Date is not null or 
SAT.Max_Junior_Age is not null or 
SAT.Max_Secondary_Age is not null or 
SAT.Min_Front_Desk_Checkin_Age is not null or 
SAT.Max_Child_Center_Checkin_Age  is not null
) 


begin tran
 
   delete dbo.stage_mms_Club_history


insert into stage_mms_Club_history
(
MMSClubKey,
ClubID,
ValRegionID,
StatementMessageID,
ValClubTypeID,
DomainNamePrefix,
ClubName,
ReceiptFooter,
DisplayUIFlag,
CheckInGroupLevel,
ValStatementTypeID,
ChargeToAccountFlag,
ValPreSaleID,
ClubActivationDate,
ValTimeZoneID,
MMSInsertedDateTime,
ValCWRegionID,
EFTGroupID,
GLTaxID,
GLClubID,
CRMDivisionCode,
AssessJrMemberDuesFlag,
SellJrMemberDuesFlag,
MMSUpdatedDateTime,
ClubCode,
SiteID,
NewMemberCardFlag,
InsertedDateTime,
InsertUser,
BatchID,
ETLSourceSystemKey,
ValMemberActivityRegionID,
IGStoreID,
ChildCenterWeeklyLimit,
ValSalesAreaID,
ValPTRCLAreaID,
KronosForecastMapPath,
GLCashEntryCompanyName,
GLCashEntryAccount,
GLReceivablesEntryAccount,
GLCashEntryCashSubAccount,
GLCashEntryCreditCardSubAccount,
GLReceivablesEntrySubAccount,
GLReceivablesEntryCompanyName,
FormalClubName,
ClubDeactivationDate,
ValCurrencyCodeID,
AllowMultipleCurrencyFlag,
WorkdayRegion,
UpdatedDateTime,
UpdateUser,
AllowJuniorCheckInFlag,
MarketingMapRegion,
MarketingMapXmlStateName,
MarketingClubLevel,
LTFResourceID,
HealthClubIdentifier,
ChargeNextMonthDate,
MaxJuniorAge,
MaxSecondaryAge,
MinFrontDeskCheckinAge,
MaxChildCenterCheckinAge
)
  select 
  MMSClubKey,
ClubID,
ValRegionID,
StatementMessageID,
ValClubTypeID,
DomainNamePrefix,
ClubName,
ReceiptFooter,
DisplayUIFlag,
CheckInGroupLevel,
ValStatementTypeID,
ChargeToAccountFlag,
ValPreSaleID,
ClubActivationDate,
ValTimeZoneID,
MMSInsertedDateTime,
ValCWRegionID,
EFTGroupID,
GLTaxID,
GLClubID,
CRMDivisionCode,
AssessJrMemberDuesFlag,
SellJrMemberDuesFlag,
MMSUpdatedDateTime,
ClubCode,
SiteID,
NewMemberCardFlag,
InsertedDateTime,
InsertUser,
BatchID,
ETLSourceSystemKey,
ValMemberActivityRegionID,
IGStoreID,
ChildCenterWeeklyLimit,
ValSalesAreaID,
ValPTRCLAreaID,
KronosForecastMapPath,
GLCashEntryCompanyName,
GLCashEntryAccount,
GLReceivablesEntryAccount,
GLCashEntryCashSubAccount,
GLCashEntryCreditCardSubAccount,
GLReceivablesEntrySubAccount,
GLReceivablesEntryCompanyName,
FormalClubName,
ClubDeactivationDate,
ValCurrencyCodeID,
AllowMultipleCurrencyFlag,
WorkdayRegion,
UpdatedDateTime,
UpdateUser,
AllowJuniorCheckInFlag,
MarketingMapRegion,
MarketingMapXmlStateName,
MarketingClubLevel,
LTFResourceID,
health_mms_club_identifier,
Charge_Next_Month_Date,
Max_Junior_Age,
Max_Secondary_Age,
Min_Front_Desk_Checkin_Age,
Max_Child_Center_Checkin_Age
from #stage_mms_Club_history_update
where row_num=1
 
 commit tran

end

----End of Proc proc_update_history_mms_club*****************





SET ANSI_NULLS ON
