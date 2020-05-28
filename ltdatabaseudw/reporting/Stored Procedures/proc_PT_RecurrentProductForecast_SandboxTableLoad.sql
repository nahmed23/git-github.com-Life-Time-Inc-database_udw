CREATE PROC [reporting].[proc_PT_RecurrentProductForecast_SandboxTableLoad] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 

 ----- Sample Execution
 ---   Exec [reporting].[proc_PT_RecurrentProductForecast_SandboxTableLoad] 
 ----


-- NOTE: This stored procedure is run by Informatica and populates the MNCODB24.Sandbox table "[rep].[PT_RecurrentProductForecast]"
--  the data from this Sandbox table is used by the LTFDM_Operations stored procedures:
--   "proc_PTTrainer_DSSR_RevenueandServiceEmployeeSummary"
--   "proc_PTTrainer_DSSR_OldAndNewBusiness"
--  Both of these LTFDM_Operations stored procedures are executed daily by the Informatica task flow "DSSR_TF_PT_Trainer" 
--    To populate the Sandbox tables [rep].[OldAndNewBusiness] and [rep].[RevenueandServiceEmployeeSummary]




DECLARE @ReportDate DateTime
DECLARE @ReportDateDimDateKey VARCHAR(8)
DECLARE @ReportMonthEndDimDateKey Varchar(8)
DECLARE @FiveYearsFutureDimDateKey Varchar(8)
DECLARE @StartDate DateTime
SET @ReportDate = getdate()-1    ------ always running for "yesterday"
SET @ReportDateDimDateKey = (SELECT dim_date_key  FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate AS Date))
SET @ReportMonthEndDimDateKey = (SELECT month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)
SET @FiveYearsFutureDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DateAdd(year,5,Cast(@ReportDate AS Date)))
SET @StartDate = (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)



IF OBJECT_ID('tempdb.dbo.#DimClubList', 'U') IS NOT NULL
DROP TABLE #DimClubList; 

SELECT DISTINCT DimClub.dim_club_key AS DimClubKey,
                DimClub.club_id AS MMSClubID,
                DimClub.club_name AS ClubName,
                DimClub.local_currency_code AS LocalCurrencyCode,
                DimClub.club_code AS ClubCode,
				DimClub.club_open_dim_date_key AS ClubOpenDateDimDateKey,
				PTRCLArea.description AS RegionName,
				CASE WHEN ClubOpenDate.calendar_date >= DATEADD(Month,-1,@StartDate)
				     THEN 'Y'
					 ELSE 'N'
					 END NewBusinessOnlyClub --- All transactions for clubs opening since the 1st of the prior month are considered New Member-New Business
INTO #DimClubList  
  FROM [marketing].[v_dim_club] DimClub
  JOIN [marketing].[v_dim_description] PTRCLArea
    ON DimClub.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
  JOIN [marketing].[v_dim_date] ClubOpenDate
    ON DimClub.club_open_dim_date_key = ClubOpenDate.dim_date_key
  Where DimClub.club_close_dim_date_key in(-997,-998,-999)
  OR DimClub.club_close_dim_date_key > @ReportMonthEndDimDateKey


 

  -------------- Gathering forecasted amount

DECLARE @ReportDateFourDigitYearTwoDigitMonth Varchar(6)

SET @ReportDateFourDigitYearTwoDigitMonth = (Select Substring(four_digit_year_two_digit_month_two_digit_day,1,6)
                                               From [marketing].[v_dim_date]
                                               Where dim_date_key = @ReportMonthEndDimDateKey)

IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate;

----- To Determine next assessment date on subscriptions based on subscription periods
-----  Assumption - payment is 1 month in advance of subscriptionPeriodTo
----- These are the subscriptions with a PeriodTo falling within a month after the report date
----- This will eliminate subscriptions which have already ended 
----- This also elimiates subscriptions which have already assessed in the current report month, based on the "to" date being < the report date
SELECT sp.dim_exerp_subscription_key,
       MAX(sp.from_dim_date_key) AS from_dim_date_key,
       MAX(sp.to_dim_date_key) AS to_dim_date_key,
	   MAX(DateAdd(day,1,ToDimDate.calendar_date)) AS NextAssessment
INTO #NextAssessmentDate  
From [marketing].[v_dim_exerp_subscription_period] sp 
  JOIN [marketing].[v_dim_date] ToDimDate
    ON sp.to_dim_date_key = ToDimDate.dim_date_key
Where sp.to_dim_date_key >= @ReportDateDimDateKey
AND ToDimDate.calendar_date < DateAdd(month,1,@ReportDate)
GROUP BY sp.dim_exerp_subscription_key



 ---- to find the last change log record for the subscriptions yet to assess
  IF OBJECT_ID('tempdb.dbo.#LastChangeLog', 'U') IS NOT NULL
DROP TABLE #LastChangeLog;

 SELECT ChangeLog.dim_exerp_subscription_key,
        MAX(ChangeLog.subscription_change_log_id) subscription_change_log_id
 INTO #LastChangeLog
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
   JOIN #NextAssessmentDate  NextAssessment
     ON ChangeLog.dim_exerp_subscription_key = NextAssessment.dim_exerp_subscription_key
GROUP BY ChangeLog.dim_exerp_subscription_key
--Order by ChangeLog.dim_exerp_subscription_key

--- Get the Commissionable Employee from the latest Subscription_change_log record
  IF OBJECT_ID('tempdb.dbo.#LatestCommissionableEmployee', 'U') IS NOT NULL
DROP TABLE #LatestCommissionableEmployee;

 SELECT 
    ChangeLog.dim_exerp_subscription_key,
	E.dim_employee_key,
	E.employee_id,
	E.last_name,
	E.first_name,
	E.middle_name,
	CASE WHEN (ISNULL(ChangeLog.dim_employee_key,'-998') <> '-998' ) 
	        THEN E.last_name +', '+ E.first_name 
			ELSE 'None Designated' 
			END CommisionedEmployee 
 INTO #LatestCommissionableEmployee
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
   JOIN #LastChangeLog LastChangeLog 
     ON ChangeLog.subscription_change_log_id = LastChangeLog .subscription_change_log_id
   JOIN [marketing].[v_dim_employee] E 
     ON ChangeLog.dim_employee_key = E.dim_employee_key



IF OBJECT_ID('tempdb.dbo.#MembershipRecurrentProductIDList', 'U') IS NOT NULL
DROP TABLE #MembershipRecurrentProductIDList;

  ---- to remove $0, terminated, pre-paid, on-hold and already assessed recurrent products from the query
  ---- also eliminates any forecast for a non-active member or a suspended membership
SELECT RecurrentProductSubscription.subscription_id AS FactMembershipRecurrentProductKey,
RecurrentProductSubscription.start_dim_date_key AS ActivationDimDateKey,
RecurrentProductSubscription.end_dim_date_key,
CASE WHEN RecurrentProductSubscription.end_dim_date_key = '-998'
	            THEN @FiveYearsFutureDimDateKey
				ELSE RecurrentProductSubscription.end_dim_date_key
				END TerminationDimDateKey,
RecurrentProductSubscription.billed_until_dim_date_key,
Member.membership_id AS MembershipID,
DimMMSProduct.product_id AS MembershipRecurrentProductID,
Freeze.start_dim_date_key AS HoldStartDimDateKey, 
Freeze.end_dim_date_key AS HoldEndDimDateKey, 
NextAssessment.NextAssessment AS NextAssessmentDate,
NextAssessmentDate.dim_date_key AS NextAssessmentDateDimDateKey,
NextAssessmentDate.day_number_in_month AS AssessmentDayOfMonth,    ------- replace this date once we know the real date value
Membership.membership_status,
DimMMSProduct.dim_reporting_hierarchy_key,
DimClub.MMSClubID,
DimClub.NewBusinessOnlyClub,
RecurrentProductSubscription.price,
Member.member_active_flag,
IsNull(CommissionEmployee.employee_id,'-998') AS employee_id
INTO #MembershipRecurrentProductIDList       

FROM [marketing].[v_dim_exerp_subscription] RecurrentProductSubscription
JOIN #DimClubList DimClub
 ON RecurrentProductSubscription.dim_club_key = DimClub.DimClubKey
JOIN [marketing].[v_dim_exerp_product] DimExerpProduct
 ON RecurrentProductSubscription.dim_exerp_product_key = DimExerpProduct.dim_exerp_product_key
JOIN [marketing].[v_dim_mms_product] DimMMSProduct
  ON DimExerpProduct.dim_mms_product_key = DimMMSProduct.dim_mms_product_key
JOIN [marketing].[v_dim_mms_member] Member
  ON RecurrentProductSubscription.dim_mms_member_key = Member.dim_mms_member_key
LEFT JOIN [marketing].[v_dim_mms_membership] Membership                                 
  ON Member.membership_id = Membership.membership_id
LEFT JOIN [marketing].[v_dim_exerp_freeze_period] Freeze
    ON RecurrentProductSubscription.dim_exerp_subscription_key = Freeze.dim_exerp_subscription_key
	  AND Freeze.cancel_dim_date_key in('-997','-998','-999')
JOIN #NextAssessmentDate NextAssessment
    ON RecurrentProductSubscription.dim_exerp_subscription_key = NextAssessment.dim_exerp_subscription_key
JOIN [marketing].[v_dim_date] NextAssessmentDate
    ON Cast(NextAssessment.NextAssessment AS Date) = NextAssessmentDate.calendar_date
LEFT JOIN #LatestCommissionableEmployee  CommissionEmployee
    ON RecurrentProductSubscription.dim_exerp_subscription_key = CommissionEmployee.dim_exerp_subscription_key
WHERE RecurrentProductSubscription.Price > 0       ---- price is greater than $0
AND (RecurrentProductSubscription.end_dim_date_key = '-998' 
     or RecurrentProductSubscription.end_dim_date_key >= @ReportMonthEndDimDateKey)   ----not terminated or terminated in future month
AND DimMMSProduct.reporting_division = 'Personal Training'      
AND NextAssessmentDate.dim_date_key > RecurrentProductSubscription.billed_until_dim_date_key           ---- not pre-paid for this period
AND NextAssessmentDate.dim_date_key <= @ReportMonthEndDimDateKey                                       ---- Next assessment not in future month
AND ((Freeze.start_dim_date_key is Null  and  Freeze.end_dim_date_key is Null)                         ---- Subscription with no hold period
      OR NextAssessmentDate.dim_date_key < Freeze.start_dim_date_key                                   ---- Next assessment is before hold period
	  OR NextAssessmentDate.dim_date_key > Freeze.end_dim_date_key )                                   ---- Next assessment is after hold period
AND (Member.member_active_flag = 'Y' or  Member.member_active_flag is Null)  ----- member is currently active - assuming null is active
AND Membership.membership_status <> 'Suspended'   ----- membership is currently not suspended    


 


  ----  Then to return the Total price by commission employee from the most recently staged record for each recurrent product
Select @ReportDateDimDateKey AS ReportDateDimDateKey,
     RecurrentProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
	 PTDSSRCategory.reporting_division AS ReportingDivision,
	 PTDSSRCategory.reporting_sub_division AS ReportingSubdivision,
     PTDSSRCategory.reporting_department AS ReportingDepartment,
	 PTDSSRCategory.reporting_product_group AS ReportingProductGroup,
     RecurrentProduct.MMSClubID,
	 RecurrentProduct.employee_id as CommissionEmployeeID,    
     PTDSSRCategory.PTDSSRCategory AS ProductCategory,
	 CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Business'
			ELSE 'Old Business'
			END BusinessType,
	 CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Member'
			ELSE 'EFT Amount'
			END  BusinessSubType,
	 Sum(RecurrentProduct.price) AS EmployeeRecurrentProductForecast


From #MembershipRecurrentProductIDList  RecurrentProduct
  JOIN [reporting].[v_PTDSSR_MoveIt_KnowIt_NourishIt] PTDSSRCategory                  
    ON RecurrentProduct.dim_reporting_hierarchy_key = PTDSSRCategory.dim_reporting_hierarchy_key
 WHERE PTDSSRCategory.ActiveFlag = 'Y'    

	
GROUP BY RecurrentProduct.MMSClubID,
     RecurrentProduct.dim_reporting_hierarchy_key,
	 PTDSSRCategory.reporting_division,
	 PTDSSRCategory.reporting_sub_division,
     PTDSSRCategory.reporting_department,
	 PTDSSRCategory.reporting_product_group,
	 RecurrentProduct.employee_id,    
     PTDSSRCategory.PTDSSRCategory,
	 RecurrentProduct.NewBusinessOnlyClub,
	 CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Business'
			ELSE 'Old Business'
			END,
	 CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Member'
			ELSE 'EFT Amount'
			END
Order by RecurrentProduct.MMSClubID


DROP TABLE #DimClubList
DROP TABLE #NextAssessmentDate
DROP TABLE #MembershipRecurrentProductIDList



END
