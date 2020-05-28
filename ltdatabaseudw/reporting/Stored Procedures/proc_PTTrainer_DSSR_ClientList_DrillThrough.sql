CREATE PROC [reporting].[proc_PTTrainer_DSSR_ClientList_DrillThrough] @ReportDate [DATETIME],@MMSClubID [INT],@EmployeeID [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 


---- sample executions:
---- Exec [reporting].[proc_PTTrainer_DSSR_ClientList_DrillThrough] '1/22/2020',238,144512
---- Exec [reporting].[proc_PTTrainer_DSSR_ClientList_DrillThrough] '4/19/2020',238,-1
----

							
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')
DECLARE @ReportRunDateLessOne DateTime
DECLARE @ReportRunDateLessOneDimDateKey VARCHAR(32)
SET @ReportRunDateLessOne = getdate()-1 
SET @ReportRunDateLessOneDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportRunDateLessOne as Date))

 ----- this report is currently set to look back 2 months prior plus the report month
DECLARE @RetentionPeriod VARCHAR(15)
SET @RetentionPeriod = '3 Months'


DECLARE @SessionPriceGreaterThan Decimal(12,2)
DECLARE @ReportDateDimDateKey VARCHAR(32)
DECLARE @StartFourDigitYearDashTwoDigitMonth VARCHAR(7)
DECLARE @ReportEndDimDateKey VARCHAR(32)
DECLARE @ReportDate_FirstOfMonthDate DateTime


SET @SessionPriceGreaterThan = 0	
SET @ReportDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date))
SET @StartFourDigitYearDashTwoDigitMonth = (SELECT four_digit_year_dash_two_digit_month FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)
SET @ReportEndDimDateKey = (SELECT month_ending_dim_date_key
                              FROM [marketing].[v_dim_date] 
							  WHERE calendar_date = Cast(@ReportDate as Date)
							  GROUP BY month_ending_dim_date_key)
SET @ReportDate_FirstOfMonthDate  = (SELECT month_starting_date
                              FROM [marketing].[v_dim_date] 
							  WHERE calendar_date = Cast(@ReportDate as Date))
							  


---- Declare and Set static variables
DECLARE @HeaderDivisionList VARCHAR(50)
DECLARE @HeaderSubdivisionList VARCHAR(8000)
DECLARE @HeaderDepartmentList  VARCHAR(8000)
DECLARE @HeaderProductGroupList VARCHAR(8000)


SET @HeaderDivisionList = 'Personal Training'
SET @HeaderSubdivisionList = 'All Subdivisions'
SET @HeaderDepartmentList = 'All Departments'                                  
SET @HeaderProductGroupList =  'All Product Groups' 
                                          

IF OBJECT_ID('tempdb.dbo.#DimLocation', 'U') IS NOT NULL
  DROP TABLE #DimLocation; 
 --- to get additional information on the employee's club  									
SELECT DISTINCT DimLocation.dim_club_key AS DimClubKey,
                DimLocation.club_name AS ClubName,
                DimLocation.local_currency_code AS LocalCurrencyCode,
				DimLocation.club_code AS ClubCode,
                DimDescription.description AS RegionName
  INTO #DimLocation     
  FROM [marketing].[v_dim_club] DimLocation
   JOIN [marketing].[v_dim_description] DimDescription
     ON DimLocation.pt_rcl_area_dim_description_key = DimDescription.dim_description_key
 WHERE DimLocation.club_id = @MMSClubID Or @MMSClubID = -1


 IF OBJECT_ID('tempdb.dbo.#SelectedTeamMemberID', 'U') IS NOT NULL
  DROP TABLE #SelectedTeamMemberID;  
  --- to get additional information on the Trainer   
 SELECT Employee.dim_employee_key AS DimEmployeeKey, 
        Employee.employee_id AS EmployeeID,
		Employee.dim_club_key AS DimClubKey,		  ----- name change										
        Employee.first_name AS FirstName,
		Employee.last_name AS LastName
  INTO #SelectedTeamMemberID    
 FROM [marketing].[v_dim_employee] Employee
  JOIN #DimLocation Club
    ON Employee.dim_club_key = Club.DimClubKey
 WHERE (Employee.employee_id = @EmployeeID	
        OR @EmployeeID = -1)
  AND (Employee.termination_date Is Null OR Employee.termination_date > @ReportDate_FirstOfMonthDate)
																		
										
  -----  We are always looking back 2 months from the report month.	
DECLARE @ReportMonthFirstOfMonthDimDateKey VARCHAR(32)									
DECLARE @ReportMonthPriorOneDimDateKey VARCHAR(32)									
DECLARE @ReportMonthPriorTwoDimDateKey VARCHAR(32)										

SET @ReportMonthFirstOfMonthDimDateKey	= (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date))																											
SET @ReportMonthPriorOneDimDateKey = (SELECT prior_month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date) GROUP BY prior_month_starting_dim_date_key )										
SET @ReportMonthPriorTwoDimDateKey = (SELECT prior_month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE month_starting_dim_date_key = @ReportMonthPriorOneDimDateKey GROUP BY prior_month_starting_dim_date_key )										
									
										
DECLARE @RetentionPeriodEnd_1stOfMonth DateTime										
DECLARE @RetentionPeriodStart_1stOfMonth_3Mo DateTime
DECLARE @RetentionPeriod_StartDimDateKey VARCHAR(32)											
																	
										
SET @RetentionPeriodEnd_1stOfMonth = (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date))										
SET @RetentionPeriodStart_1stOfMonth_3Mo = (Select calendar_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportMonthPriorTwoDimDateKey)	 ------ @RetentionPeriod = '3 Months'						
SET @RetentionPeriod_StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @RetentionPeriodStart_1stOfMonth_3Mo)


  ---- Pulls in data from the daily refreshed PT DSSR table if the report date is "yesterday"
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodAllSessionsAndParticipation', 'U') IS NOT NULL
  DROP TABLE #RetentionPeriodAllSessionsAndParticipation; 

 SELECT Detail.delivered_four_digit_year_dash_two_digit_month AS DeliveredFourDigitYearDashTwoDigitMonth,
    Detail.delivered_date_dim_date_key AS DeliveredDateDimDateKey,
	Detail.delivered_dim_club_key AS DeliveredDimClubKey,
	Detail.delivered_dim_employee_key AS DeliveredDimEmployeeKey,
	Employee.EmployeeID AS DeliveredEmployeeID,
	Employee.DimClubKey AS EmployeeHomeDimClubKey,
	Detail.dim_member_key AS DimMemberKey,
	Detail.delivered_price AS DeliveredPrice,
	Detail.dim_product_key AS DimProductKey,
	Detail.product_description AS ProductDescription,
	Detail.product_dim_reporting_hierarchy_key AS ProductDimReportingHierarchyKey,
    Detail.member_date_of_birth AS MemberDOB,
	Detail.dim_mms_membership_key AS DimMMSMembershipKey,
	'MMS AND Exerp' AS Source
INTO #RetentionPeriodAllSessionsAndParticipation	  
 FROM [dbo].[fact_ptdssr_client_retention_detail] Detail   
   JOIN #DimLocation Club
     ON Detail.employee_home_dim_club_key = Club.DimClubKey
   JOIN #SelectedTeamMemberID Employee
     ON Detail.delivered_dim_employee_key = Employee.DimEmployeeKey
 WHERE Detail.report_date_dim_date_key = @ReportDateDimDateKey 
 AND @ReportRunDateLessOneDimDateKey = @ReportDateDimDateKey

UNION ALL

  SELECT DeliveryDimDate.four_digit_year_dash_two_digit_month AS DeliveredFourDigitYearDashTwoDigitMonth,
    DeliveryDimDate.dim_date_key AS DeliveredDateDimDateKey,
	FactPackageSession.delivered_dim_club_key AS DeliveredDimClubKey,
	FactPackageSession.delivered_dim_employee_key AS DeliveredDimEmployeeKey,
	DeliveredEmployee.EmployeeID AS DeliveredEmployeeID,
	DeliveredEmployee.DimClubKey AS EmployeeHomeDimClubKey,
	FactPackageSession.dim_mms_member_key AS DimMemberKey,
	FactPackageSession.delivered_session_price AS DeliveredPrice,
	FactPackageSession.fact_mms_package_dim_product_key AS DimProductKey,
	DimProduct.product_description AS ProductDescription,
	DimProduct.dim_reporting_hierarchy_key AS ProductDimReportingHierarchyKey,
     DimCustomer.date_of_birth AS MemberDOB,
	 DimCustomer.dim_mms_membership_key AS DimMMSMembershipKey,
	 'MMS' AS Source								
	
FROM [marketing].[v_fact_mms_package_session] FactPackageSession
	JOIN [marketing].[v_dim_mms_product] DimProduct										
		ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
	JOIN [marketing].[v_dim_date]  DeliveryDimDate										
		ON FactPackageSession.delivered_dim_date_key = DeliveryDimDate.dim_date_key	
	JOIN #SelectedTeamMemberID  DeliveredEmployee
	    ON FactPackageSession.delivered_dim_employee_key = DeliveredEmployee.DimEmployeeKey
	JOIN [marketing].[v_dim_mms_member] DimCustomer										
		ON FactPackageSession.dim_mms_member_key = DimCustomer.dim_mms_member_key
WHERE FactPackageSession.delivered_dim_date_key >= @ReportMonthPriorTwoDimDateKey   ----- first of 2 months prior
  AND DimProduct.reporting_division = 'Personal Training'
  AND FactPackageSession.delivered_session_price > @SessionPriceGreaterThan
  AND @ReportRunDateLessOneDimDateKey > @ReportDateDimDateKey

UNION ALL

SELECT DeliveryDate.four_digit_year_dash_two_digit_month AS DeliveredFourDigitYearDashTwoDigitMonth,
       DeliveryDate.dim_date_key AS DeliveredDateDimDateKey,
	   Booking.dim_club_key AS DeliveredDimClubKey,
	   Instructors.dim_employee_key AS DeliveredDimEmployeeKey,
	   DeliveredEmployee.EmployeeID AS DeliveredEmployeeID,
	   DeliveredEmployee.DimClubKey AS EmployeeHomeDimClubKey,
	   Participation.dim_mms_member_key AS DimMemberKey,
	   SubscriptionPeriod.price_per_booking AS DeliveredPrice,
	   ExerpActivity.dim_mms_product_key AS DimProductKey,
	   MMSProduct.product_description AS ProductDesription,
	   MMSProduct.dim_reporting_hierarchy_key AS ProductDimReportingHierarchyKey,
       DimCustomer.date_of_birth AS MemberDOB,
	   DimCustomer.dim_mms_membership_key AS DimMMSMembershipKey,
	   'Exerp' AS Source
FROM   [marketing].[v_dim_exerp_booking] Booking
  JOIN [marketing].[v_fact_exerp_participation] Participation
    ON Booking.dim_exerp_booking_key = Participation.dim_exerp_booking_key
  JOIN [marketing].[v_dim_exerp_staff_usage] Instructors
    ON Booking.booking_id = Instructors.booking_id 
  JOIN [marketing].[v_dim_exerp_activity] ExerpActivity
    ON Booking.dim_exerp_activity_key = ExerpActivity.dim_exerp_activity_key
  JOIN [marketing].[v_dim_mms_product] MMSProduct
    ON ExerpActivity.dim_mms_product_key = MMSProduct.dim_mms_product_key
  JOIN #SelectedTeamMemberID  DeliveredEmployee
	ON Instructors.dim_employee_key = DeliveredEmployee.DimEmployeeKey
  JOIN [marketing].[v_dim_date] DeliveryDate
    ON Booking.start_dim_date_key = DeliveryDate.dim_date_key
  JOIN [marketing].[v_dim_exerp_activity] Activity 
    ON Booking.dim_exerp_activity_key = Activity.dim_exerp_activity_key
  JOIN [marketing].[v_dim_exerp_subscription_period] SubscriptionPeriod     
    ON Participation.dim_exerp_subscription_key = SubscriptionPeriod.dim_exerp_subscription_key
	AND Participation.dim_mms_member_key = SubscriptionPeriod.dim_mms_member_key
	AND Booking.start_dim_date_key >= SubscriptionPeriod.from_dim_date_key
	AND Booking.start_dim_date_key <= SubscriptionPeriod.to_dim_date_key
  JOIN [marketing].[v_dim_mms_member] DimCustomer										
	ON Participation.dim_mms_member_key = DimCustomer.dim_mms_member_key
WHERE MMSProduct.reporting_division = 'Personal Training'
AND Instructors.staff_usage_state = 'ACTIVE'
AND ExerpActivity.activity_group_name in ('Small Group Training','Pilates Class','Virtual Training')
AND Booking.start_dim_date_key >= @ReportMonthPriorTwoDimDateKey   ----- first of 2 months prior
AND Booking.start_dim_date_key <= @ReportDateDimDateKey
AND SubscriptionPeriod.price_per_booking > @SessionPriceGreaterThan
AND @ReportRunDateLessOneDimDateKey > @ReportDateDimDateKey


   							


IF OBJECT_ID('tempdb.dbo.#MaxDeliveredDateByEmployeeMember', 'U') IS NOT NULL
  DROP TABLE #MaxDeliveredDateByEmployeeMember; 

SELECT DeliveredDimEmployeeKey,
       DimMemberKey,
	   Max(DeliveredDateDimDateKey) AS MaxDeliveredDateDimDateKey
  INTO #MaxDeliveredDateByEmployeeMember
FROM #RetentionPeriodAllSessionsAndParticipation  
  	GROUP BY DeliveredDimEmployeeKey,
       DimMemberKey

IF OBJECT_ID('tempdb.dbo.#MemberHomePhone', 'U') IS NOT NULL
  DROP TABLE #MemberHomePhone; 

Select Participation.DimMemberKey,
       MAX(CASE WHEN DoNotPhone.val_communication_preference_id = 2
	     THEN '' 
	     WHEN IsNull(PhoneType.description,'Not Home Phone') = 'Home'  -------- Max() Removes multiple Member records  - Home and Null
	     THEN MembershipHomePhone.area_code
		 ELSE ''
		 END) MemberHomePhone_AreaCode,
       MAX(CASE WHEN DoNotPhone.val_communication_preference_id = 2
	     THEN '' 
	     WHEN IsNull(PhoneType.description,'Not Home Phone') = 'Home'
	     THEN MembershipHomePhone.number
		 ELSE ''
		 END) MemberHomePhone_Number
	INTO #MemberHomePhone
FROM #RetentionPeriodAllSessionsAndParticipation  Participation
  JOIN [marketing].[v_dim_mms_member] DimCustomer										
	ON Participation.DimMemberKey = DimCustomer.dim_mms_member_key
  LEFT JOIN [marketing].[v_dim_mms_membership_phone] MembershipHomePhone
	ON 	DimCustomer.dim_mms_membership_key = MembershipHomePhone.dim_mms_membership_key	
  LEFT JOIN [marketing].[v_dim_description] PhoneType
	    ON MembershipHomePhone.phone_type_dim_description_key = PhoneType.dim_description_key
		AND PhoneType.description = 'Home'
  LEFT JOIN [marketing].[v_dim_mms_membership_communication_preference] DoNotPhone
	    ON Participation.DimMMSMembershipKey = DoNotPhone.dim_mms_membership_key
	     AND DoNotPhone.val_communication_preference_id = 2    ------ "Do Not Phone"    ---- limiting this join to just those memberships which have requested "Do Not Phone" so the number can be removed in the result
	     AND DoNotPhone.active_flag = 'Y'
 GROUP BY Participation.DimMemberKey



 IF OBJECT_ID('tempdb.dbo.#MaxDeliveredProductByEmployeeMember', 'U') IS NOT NULL
  DROP TABLE #MaxDeliveredProductByEmployeeMember; 
---- to return one record per customer-employee pair, with the last date and one of the products delivered on that date
 
SELECT 
    MaxDelivery.MaxDeliveredDateDimDateKey,
	MaxDelivery.DeliveredDimEmployeeKey,
	MaxDelivery.DimMemberKey,
	MAX(SessionsAndParticipation.ProductDescription) AS ProductDescription
INTO #MaxDeliveredProductByEmployeeMember    
FROM #MaxDeliveredDateByEmployeeMember MaxDelivery
	JOIN #RetentionPeriodAllSessionsAndParticipation SessionsAndParticipation	
		ON MaxDelivery.MaxDeliveredDateDimDateKey = SessionsAndParticipation.DeliveredDateDimDateKey
		AND MaxDelivery.DimMemberKey = SessionsAndParticipation.DimMemberKey
		AND MaxDelivery.DeliveredDimEmployeeKey = SessionsAndParticipation.DeliveredDimEmployeeKey 
GROUP BY
	 MaxDelivery.MaxDeliveredDateDimDateKey,
	 MaxDelivery.DeliveredDimEmployeeKey,
	 MaxDelivery.DimMemberKey



   IF OBJECT_ID('tempdb.dbo.#RetentionPeriodEmployeeClient_MostRecentRecord', 'U') IS NOT NULL
  DROP TABLE #RetentionPeriodEmployeeClient_MostRecentRecord; 

 ------ a listing of all retention period employee/customer pairs and the most recent product delivery date  with product

 SELECT DeliveryDate.four_digit_year_dash_two_digit_month AS DeliveredFourDigitYearDashTwoDigitMonth,
    DeliveryDate.dim_date_key AS DeliveredDateDimDateKey,
	SessionsAndParticipation.DeliveredDimClubKey,
	SessionsAndParticipation.DeliveredDimEmployeeKey,
	SessionsAndParticipation.DeliveredEmployeeID,
	SessionsAndParticipation.EmployeeHomeDimClubKey,
	MaxDeliveredRecord.DimMemberKey,
	SessionsAndParticipation.DeliveredPrice,
	SessionsAndParticipation.DimProductKey,
	MaxDeliveredRecord.ProductDescription,
	SessionsAndParticipation.ProductDimReportingHierarchyKey,
    SessionsAndParticipation.MemberDOB
INTO #RetentionPeriodEmployeeClient_MostRecentRecord
FROM #RetentionPeriodAllSessionsAndParticipation  SessionsAndParticipation
 JOIN #MaxDeliveredProductByEmployeeMember MaxDeliveredRecord
   ON SessionsAndParticipation.DeliveredDateDimDateKey =  MaxDeliveredRecord.MaxDeliveredDateDimDateKey
    AND SessionsAndParticipation.DeliveredDimEmployeeKey = MaxDeliveredRecord.DeliveredDimEmployeeKey 
	AND SessionsAndParticipation.DimMemberKey = MaxDeliveredRecord.DimMemberKey 
	AND SessionsAndParticipation.ProductDescription = MaxDeliveredRecord.ProductDescription
 JOIN [marketing].[v_dim_date]  DeliveryDate										
	ON MaxDeliveredRecord.MaxDeliveredDateDimDateKey = DeliveryDate.dim_date_key
 

---- now finding customers with either a recurrent product schedule or a current subscription for a PT division product

 IF OBJECT_ID('tempdb.dbo.#CustomersWithRecurrentProduct_Prelim', 'U') IS NOT NULL
  DROP TABLE #CustomersWithRecurrentProduct_Prelim; 

 ----- query for customers with recurrent product schedule
 SELECT Customers.DimMemberKey, 
        'Y' AS HasRecurrentProduct
  INTO #CustomersWithRecurrentProduct_Prelim    
 FROM #MemberHomePhone Customers   ----- This temp table contains 1 record per member in the retention period of 3 months
  JOIN [marketing].[v_fact_mms_membership_recurrent_product] RecurrentProduct
    ON Customers.DimMemberKey = RecurrentProduct.dim_mms_member_key
  JOIN [marketing].[v_dim_mms_product] Product
    ON RecurrentProduct.dim_mms_product_key = Product.dim_mms_product_key
  WHERE RecurrentProduct.activation_dim_date_key <= @ReportDateDimDateKey
	AND (IsNull(RecurrentProduct.termination_dim_date_key,'-998') = '-998' 
	     OR IsNull(RecurrentProduct.termination_dim_date_key,'99991231') > @ReportDateDimDateKey)
	AND (RecurrentProduct.hold_start_dim_date_key = '-998' 
        OR @ReportDateDimDateKey < RecurrentProduct.hold_start_dim_date_key
		 OR @ReportDateDimDateKey > RecurrentProduct.hold_end_dim_date_key )
	AND Product.reporting_division = 'Personal Training'
 GROUP BY Customers.DimMemberKey

 UNION 
 
  SELECT Customers.DimMemberKey, 
        'Y' AS HasRecurrentProduct
  FROM #MemberHomePhone Customers
  JOIN [marketing].[v_dim_exerp_subscription]  RecurrentProduct
    ON Customers.DimMemberKey = RecurrentProduct.dim_mms_member_key
  JOIN [marketing].[v_dim_exerp_product] ExerpProduct
    ON RecurrentProduct.dim_exerp_product_key = ExerpProduct.dim_exerp_product_key
  JOIN [marketing].[v_dim_mms_product] MMSProduct
    ON ExerpProduct.dim_mms_product_key = MMSProduct.dim_mms_product_key
  LEFT JOIN [marketing].[v_dim_exerp_freeze_period] Freeze   
    ON RecurrentProduct.dim_exerp_subscription_key = Freeze.dim_exerp_subscription_key
     AND Freeze.cancel_dim_date_key in('-997','-998','-999')   ----- limiting the returned results to only freezes active on the report date
     AND @ReportDateDimDateKey >= Freeze.start_dim_date_key    ----- limiting the returned results to only freezes active on the report date 
     AND @ReportDateDimDateKey <= Freeze.end_dim_date_key      ----- limiting the returned results to only freezes active on the report date
  WHERE RecurrentProduct.start_dim_date_key <= @ReportDateDimDateKey
	AND (RecurrentProduct.end_dim_date_key = '-998' 
	     OR RecurrentProduct.end_dim_date_key > @ReportDateDimDateKey)
	AND Freeze.start_dim_date_key Is Null 
	AND MMSProduct.reporting_division = 'Personal Training'
 GROUP BY Customers.DimMemberKey



----- Did a separate grouping because I was concerned that members  
----- may return in both queries of the above union during the phased rollout of Exerp
  IF OBJECT_ID('tempdb.dbo.#CustomersWithRecurrentProduct', 'U') IS NOT NULL
  DROP TABLE #CustomersWithRecurrentProduct; 

 SELECT DimMemberKey, 
        MAX(HasRecurrentProduct) AS HasRecurrentProduct    ---- if "Y" from either of the above unioned queries, the member is a "Y"
  INTO #CustomersWithRecurrentProduct
  FROM #CustomersWithRecurrentProduct_Prelim
  GROUP BY DimMemberKey  

---- Return customers with outstanding packages and current subscription periods with future bookings.

 ------ query for customers with outstanding package sessions
  IF OBJECT_ID('tempdb.dbo.#CustomerOutstandingPackageSessions', 'U') IS NOT NULL
  DROP TABLE #CustomerOutstandingPackageSessions;

  SELECT Customers.DimMemberKey,
         Member.member_id,
         SUM(FactPackage.sessions_left) AS SessionsLeft,
		 SUM(FactPackage.balance_amount) AS BalanceAmount
  INTO #CustomerOutstandingPackageSessions  
  FROM [marketing].[v_fact_mms_package] FactPackage
   JOIN #MemberHomePhone Customers   ----- This temp table contains 1 record per member in the retention period of 3 months
     ON Customers.DimMemberKey = FactPackage.dim_mms_member_key
   JOIN [marketing].[v_dim_mms_product] Product
     ON FactPackage.dim_mms_product_key = Product.dim_mms_product_key
   JOIN [marketing].[v_dim_mms_member] Member
     ON Customers.DimMemberKey = Member.dim_mms_member_key
  WHERE FactPackage.sessions_left > 0
   AND Product.reporting_division = 'Personal Training'
   GROUP BY Customers.DimMemberKey,
   Member.member_id



   ----- In Exerp, it is a multi-step process
   ----- To not double count other PT Products which are being counted as "clipcards" (sessions) we need to hardcode the SGT groups
   ----- we need to only return bookings that are set to occur after the report date (those that are "outstanding")
IF OBJECT_ID('tempdb.dbo.#CustomerSubscriptions', 'U') IS NOT NULL
DROP TABLE #CustomerSubscriptions;

SELECT Customers.DimMemberKey,
       SubscriptionPeriod.dim_exerp_subscription_key,
       SubscriptionPeriod.price_per_booking,
	   SubscriptionPeriod.from_dim_date_key,
	   SubscriptionPeriod.to_dim_date_key,
	   SubscriptionPeriod.subscription_period_state
INTO #CustomerSubscriptions
FROM [marketing].[v_dim_exerp_subscription_period] SubscriptionPeriod
  JOIN #MemberHomePhone Customers   ----- This temp table contains 1 record per member in the retention period of 3 months
   ON SubscriptionPeriod.dim_mms_member_key = Customers.DimMemberKey
WHERE SubscriptionPeriod.to_dim_date_key > @ReportDateDimDateKey 
AND SubscriptionPeriod.subscription_period_state = 'ACTIVE'

IF OBJECT_ID('tempdb.dbo.#CustomerOutstandingBookings', 'U') IS NOT NULL
DROP TABLE #CustomerOutstandingBookings;

SELECT Participation.dim_mms_member_key AS DimMemberKey,
       COUNT(Booking.start_dim_date_key) AS SessionsLeft,
       SUM(CustomersSubscriptions.price_per_booking)  AS BalanceAmount
INTO #CustomerOutstandingBookings    
FROM [marketing].[v_fact_exerp_participation] Participation
 JOIN #CustomerSubscriptions CustomersSubscriptions
   ON Participation.dim_mms_member_key = CustomersSubscriptions.DimMemberKey
   AND Participation.dim_exerp_subscription_key = CustomersSubscriptions.dim_exerp_subscription_key
 JOIN [marketing].[v_dim_exerp_booking] Booking 
   ON Booking.dim_exerp_booking_key = Participation.dim_exerp_booking_key
 JOIN [marketing].[v_dim_exerp_activity] Activity 
   ON Booking.dim_exerp_activity_key = Activity.dim_exerp_activity_key

WHERE Activity.activity_group_name in ('Small Group Training','Pilates Class','Virtual Training')
AND Booking.start_dim_date_key >= @ReportDateDimDateKey 
AND Booking.start_dim_date_key >= CustomersSubscriptions.from_dim_date_key     ----- found that some customers had multiple active period records for the same subscription
AND Booking.start_dim_date_key <= CustomersSubscriptions.to_dim_date_key
AND Participation.participation_state = 'BOOKED' 
GROUP BY Participation.dim_mms_member_key


------ Bring it together
  IF OBJECT_ID('tempdb.dbo.#CustomerOutstandingSessionsAndBookings', 'U') IS NOT NULL
  DROP TABLE #CustomerOutstandingSessionsAndBookings;

SELECT NestedResult.DimMemberKey,
       SUM(NestedResult.SessionsLeft) AS SessionsLeft,
	   SUM(NestedResult.BalanceAmount) AS BalanceAmount
INTO #CustomerOutstandingSessionsAndBookings    
FROM (  SELECT DimMemberKey,
         SessionsLeft,
		 BalanceAmount
       FROM #CustomerOutstandingPackageSessions
    UNION ALL
       SELECT DimMemberKey,
         SessionsLeft,
         BalanceAmount
       FROM #CustomerOutstandingBookings) NestedResult
GROUP BY NestedResult.DimMemberKey
						
 ----- To gather current month delivered session/booking data for each employee	
  IF OBJECT_ID('tempdb.dbo.#ReportMonthClients', 'U') IS NOT NULL
  DROP TABLE #ReportMonthClients;
    									
SELECT SessionsAndParticipation.DeliveredDimEmployeeKey,
    SessionsAndParticipation.DimMemberKey,
	1 AS MonthTally, 
	1 AS FirstMonthTally,										
	Count(SessionsAndParticipation.DeliveredDateDimDateKey) AS SessionCount,
	Sum(SessionsAndParticipation.DeliveredPrice) AS TotalSessionsPrice,
	Max(SessionsAndParticipation.DeliveredDateDimDateKey) AS MaxDeliveryDateDimDateKey									
INTO #ReportMonthClients	 	  			
FROM #RetentionPeriodAllSessionsAndParticipation SessionsAndParticipation															
WHERE SessionsAndParticipation.DeliveredDateDimDateKey >= @ReportMonthFirstOfMonthDimDateKey																
	AND SessionsAndParticipation.DeliveredDateDimDateKey <= @ReportDateDimDateKey							
GROUP BY SessionsAndParticipation.DeliveredDimEmployeeKey,SessionsAndParticipation.DimMemberKey																		
										
														
										
  ---- gather session information for the report month customers back the selected number of months	
  
   IF OBJECT_ID('tempdb.dbo.#ReportMonthPriorOne_Clients', 'U') IS NOT NULL
  DROP TABLE #ReportMonthPriorOne_Clients;   
 									
SELECT SessionsAndParticipation.DeliveredDimEmployeeKey,
       SessionsAndParticipation.DimMemberKey AS DimMemberKey,
	   1 AS MonthTally, 
	   0 AS FirstMonthTally, 
	   '20000101' AS MaxDeliveryDateDimDateKey 									
INTO #ReportMonthPriorOne_Clients		  								
FROM #RetentionPeriodAllSessionsAndParticipation SessionsAndParticipation										
	JOIN #ReportMonthClients 										       
		ON SessionsAndParticipation.DeliveredDimEmployeeKey = #ReportMonthClients.DeliveredDimEmployeeKey		----- Only return employees/client pairs who were active in the report month									
		AND SessionsAndParticipation.DimMemberKey = #ReportMonthClients.DimMemberKey										
																				
WHERE  SessionsAndParticipation.DeliveredDateDimDateKey >= @ReportMonthPriorOneDimDateKey	
AND 	SessionsAndParticipation.DeliveredDateDimDateKey < @ReportMonthFirstOfMonthDimDateKey															
									
GROUP BY SessionsAndParticipation.DeliveredDimEmployeeKey,SessionsAndParticipation.DimMemberKey	

			


   IF OBJECT_ID('tempdb.dbo.#ReportMonthPriorTwo_Clients', 'U') IS NOT NULL
  DROP TABLE #ReportMonthPriorTwo_Clients;										
										
SELECT SessionsAndParticipation.DeliveredDimEmployeeKey,
       SessionsAndParticipation.DimMemberKey AS DimMemberKey,  
	   1 AS MonthTally, 
	   0 AS FirstMonthTally, 
	   '20000101' AS MaxDeliveryDateDimDateKey							
INTO #ReportMonthPriorTwo_Clients										
FROM #RetentionPeriodAllSessionsAndParticipation SessionsAndParticipation										
 JOIN #ReportMonthPriorOne_Clients 										       
   ON SessionsAndParticipation.DeliveredDimEmployeeKey = #ReportMonthPriorOne_Clients.DeliveredDimEmployeeKey		----- Only return employees/client pairs who were active in the report month and the prior month									
  AND SessionsAndParticipation.DimMemberKey = #ReportMonthPriorOne_Clients.DimMemberKey	
																					
WHERE SessionsAndParticipation.DeliveredDateDimDateKey >= @ReportMonthPriorTwoDimDateKey																		
  AND SessionsAndParticipation.DeliveredDateDimDateKey < @ReportMonthPriorOneDimDateKey						
GROUP BY SessionsAndParticipation.DeliveredDimEmployeeKey,SessionsAndParticipation.DimMemberKey

								
																		
  --- consolidate the pertinant data into a single temp table for the number of months selected										
  --- 3 month retention	
   IF OBJECT_ID('tempdb.dbo.#3MonthDetail', 'U') IS NOT NULL
  DROP TABLE #3MonthDetail;	  

									
SELECT DeliveredDimEmployeeKey,DimMemberKey,MonthTally,FirstMonthTally,MaxDeliveryDateDimDateKey										
INTO #3MonthDetail						 				
FROM #ReportMonthClients										
----WHERE @RetentionPeriod ='3 Months'										
										
UNION ALL
										
SELECT DeliveredDimEmployeeKey,DimMemberKey, MonthTally, FirstMonthTally,MaxDeliveryDateDimDateKey									
FROM #ReportMonthPriorOne_Clients										
----WHERE @RetentionPeriod ='3 Months'										
										
UNION ALL
									
SELECT DeliveredDimEmployeeKey,DimMemberKey, MonthTally, FirstMonthTally,MaxDeliveryDateDimDateKey										
FROM #ReportMonthPriorTwo_Clients										
----WHERE @RetentionPeriod ='3 Months'																			
																		
									
  ---- sum the resulting table	

   IF OBJECT_ID('tempdb.dbo.#TenureData', 'U') IS NOT NULL
  DROP TABLE #TenureData;	 
									
SELECT Detail.DeliveredDimEmployeeKey, 
       Detail.DimMemberKey,
	   DimCustomer.member_id AS MemberID,
	   DimCustomer.first_name AS FirstName,
	   DimCustomer.last_name AS LastName,
	   DimCustomer.dim_mms_membership_key,
	   DimCustomer.date_of_birth AS BirthDate,
	   Sum(Detail.MonthTally) as CustomerTenure, 
	   Sum(Detail.FirstMonthTally) as CustomerCount, 
	   MAX(Detail.MaxDeliveryDateDimDateKey) as MaxDeliveryDimDateKey,
	   MAX(MaxDeliveryDate.calendar_date) AS MaxDeliveryDimDate										
INTO #TenureData    							 
FROM #3MonthDetail	Detail
  	JOIN [marketing].[v_dim_mms_member] DimCustomer										
		ON Detail.DimMemberKey = DimCustomer.dim_mms_member_key
	JOIN [marketing].[v_dim_date] MaxDeliveryDate
	    ON Detail.MaxDeliveryDateDimDateKey = MaxDeliveryDate.dim_date_key									
GROUP BY Detail.DeliveredDimEmployeeKey, 
         Detail.DimMemberKey,
		 DimCustomer.member_id,
		 DimCustomer.first_name,
	     DimCustomer.last_name,
		 DimCustomer.dim_mms_membership_key,
		 DimCustomer.date_of_birth
		 
		 															
										
 ---- joins total session price and count for the report month for the employee and pulls in employee info										
SELECT 	DimLocation.RegionName, 										
	DimLocation.ClubName,										
	DimLocation.ClubCode, 										
	SelectedEmployee.EmployeeID, 										
	SelectedEmployee.FirstName AS EmployeeFirstName, 										
	SelectedEmployee.LastName AS EmployeeLastName,										
	#TenureData.MemberID AS RetentionPeriod_ClientMemberID,										
	#TenureData.FirstName AS RetentionPeriod_ClientFirstName,										
	#TenureData.LastName AS RetentionPeriod_ClientLastName,
	#TenureData.BirthDate,							
	#TenureData.MaxDeliveryDimDate AS LastDeliveryDate,										
	#TenureData.CustomerTenure AS ReportMonth_ClientTenure_Months,										
	#ReportMonthClients.SessionCount AS ReportMonth_Client_TotalSessionsCount,										
	#ReportMonthClients.TotalSessionsPrice AS ReportMonth_Client_TotalSessionsPrice,
	@ReportRunDateTime AS ReportRunDateTime,
	@ReportDate AS ReportDate,
	@HeaderDivisionList AS HeaderDivisionList,
	@HeaderSubdivisionList AS HeaderSubdivisionList,
	@HeaderDepartmentList AS HeaderDepartmentList,
	@HeaderProductGroupList	AS HeaderProductGroupList,
	@StartFourDigitYearDashTwoDigitMonth AS HeaderReportMonth,
	@RetentionPeriod AS HeaderRetentionPeriod,										
	EmployeeClientDetail.ProductDescription AS LastSessionProduct,
	CASE WHEN IsNull(MemberHomePhone.MemberHomePhone_AreaCode,'') = ''
	     THEN ''
		 ELSE Cast(MemberHomePhone.MemberHomePhone_AreaCode as varchar)+'-'+ Substring(Cast(MemberHomePhone.MemberHomePhone_Number as varchar),1,3)+'-'+Substring(Cast(MemberHomePhone.MemberHomePhone_Number as varchar),4,7) 
		 END MemberHomePhone,
	IsNull(OutstandingPackageSessions.SessionsLeft,0) AS SessionsLeft,
	IsNull(OutstandingPackageSessions.BalanceAmount,0) AS BalanceAmount,
	IsNull(RecurrentProduct.HasRecurrentProduct,'N') AS HasRecurrentProduct,
	@ReportRunDateLessOne AS ReportRunDateLessOne
									
FROM #TenureData									
	JOIN #ReportMonthClients 						
		ON #TenureData.DeliveredDimEmployeeKey = #ReportMonthClients.DeliveredDimEmployeeKey										
		AND #TenureData.DimMemberKey = #ReportMonthClients.DimMemberKey										
	JOIN #SelectedTeamMemberID SelectedEmployee
		ON #TenureData.DeliveredDimEmployeeKey = SelectedEmployee.DimEmployeeKey										
	JOIN #DimLocation DimLocation										
		ON SelectedEmployee.DimClubKey = DimLocation.DimClubKey										
	JOIN [marketing].[v_dim_mms_member] DimCustomer										
		ON #TenureData.DimMemberKey = DimCustomer.dim_mms_member_key									
	JOIN #RetentionPeriodEmployeeClient_MostRecentRecord EmployeeClientDetail
		ON #TenureData.DeliveredDimEmployeeKey = EmployeeClientDetail.DeliveredDimEmployeeKey										
		AND #TenureData.DimMemberKey = EmployeeClientDetail.DimMemberKey
		AND #TenureData.MaxDeliveryDimDateKey = EmployeeClientDetail.DeliveredDateDimDateKey	
	JOIN [marketing].[v_dim_date] DimDate 
		ON DimDate.calendar_date = DimCustomer.date_of_birth	
	LEFT JOIN #CustomerOutstandingSessionsAndBookings OutstandingPackageSessions
		ON #TenureData.DimMemberKey = OutstandingPackageSessions.DimMemberKey	
	LEFT JOIN #CustomersWithRecurrentProduct	RecurrentProduct
		ON #TenureData.DimMemberKey = RecurrentProduct.DimMemberKey	
    LEFT JOIN #MemberHomePhone MemberHomePhone
	    ON EmployeeClientDetail.DimMemberKey = MemberHomePhone.DimMemberKey
	
										
UNION 										
										
										
  ------ appends info on all clients for the retention period										
SELECT	DimLocation.RegionName, 										
	DimLocation.ClubName,										
	DimLocation.ClubCode, 										
	SelectedEmployee.EmployeeID, 										
	SelectedEmployee.FirstName AS EmployeeFirstName, 										
	SelectedEmployee.LastName AS EmployeeLastName,											
	DimCustomer.member_id AS RetentionPeriod_ClientMemberID,										
	DimCustomer.first_name AS RetentionPeriod_ClientFirstName,										
	DimCustomer.last_name AS RetentionPeriod_ClientLastName,
	DimCustomer.date_of_birth AS BirthDate,										
	DeliveryDimDate.calendar_date AS LastDeliveryDate,										
	NULL AS ReportMonth_ClientTenure_Months,										
	NULL AS ReportMonth_Client_TotalSessionsCount,										
	NULL AS ReportMonth_Client_TotalSessionsPrice,
	@ReportRunDateTime AS ReportRunDateTime,
	@ReportDate AS ReportDate,
	@HeaderDivisionList AS HeaderDivisionList,
	@HeaderSubdivisionList AS HeaderSubdivisionList,
	@HeaderDepartmentList AS HeaderDepartmentList,
	@HeaderProductGroupList	AS HeaderProductGroupList,
	@StartFourDigitYearDashTwoDigitMonth AS HeaderReportMonth,
	@RetentionPeriod AS HeaderRetentionPeriod,
	Detail.ProductDescription AS LastSessionProduct,
    CASE WHEN IsNull(MemberHomePhone.MemberHomePhone_AreaCode,'') = ''
	     THEN ''
		 ELSE Cast(MemberHomePhone.MemberHomePhone_AreaCode as varchar)+'-'+ Substring(Cast(MemberHomePhone.MemberHomePhone_Number as varchar),1,3)+'-'+Substring(Cast(MemberHomePhone.MemberHomePhone_Number as varchar),4,7) 
		 END MemberHomePhone,
	IsNull(OutstandingPackageSessions.SessionsLeft,0) AS SessionsLeft,
	IsNull(OutstandingPackageSessions.BalanceAmount,0) AS BalanceAmount,
	IsNull(RecurrentProduct.HasRecurrentProduct,'N') AS HasRecurrentProduct,
	@ReportRunDateLessOne AS ReportRunDateLessOne		
FROM  #RetentionPeriodEmployeeClient_MostRecentRecord Detail   
JOIN [marketing].[v_dim_date] DeliveryDimDate
  ON Detail.DeliveredDateDimDateKey = DeliveryDimDate.dim_date_key
JOIN #SelectedTeamMemberID SelectedEmployee
  ON Detail.DeliveredDimEmployeeKey = SelectedEmployee.DimEmployeeKey										
JOIN #DimLocation DimLocation										
  ON SelectedEmployee.DimClubKey = DimLocation.DimClubKey
JOIN [marketing].[v_dim_mms_member] DimCustomer										
  ON Detail.DimMemberKey = DimCustomer.dim_mms_member_key	
LEFT JOIN #CustomerOutstandingSessionsAndBookings OutstandingPackageSessions
  ON Detail.DimMemberKey = OutstandingPackageSessions.DimMemberKey	
LEFT JOIN #CustomersWithRecurrentProduct	RecurrentProduct
  ON Detail.DimMemberKey = RecurrentProduct.DimMemberKey	
LEFT JOIN #MemberHomePhone MemberHomePhone
  ON Detail.DimMemberKey = MemberHomePhone.DimMemberKey									
WHERE DeliveryDimDate.calendar_date < @RetentionPeriodEnd_1stOfMonth	



END
