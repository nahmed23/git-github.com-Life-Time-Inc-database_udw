CREATE PROC [reporting].[proc_UDW_FactPTDSSRClientRetentionDetail] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
    END

 -------
 ------- This proc is executed by an Informatica process to populate the UDW table "fact_ptdssr_client_retention_detail"
 -------
DECLARE @ReportDate [DATETIME] = '1/1/1900'

SET @ReportDate = CASE WHEN @ReportDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END						

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


DECLARE @SessionPriceGreaterThan Decimal(12,2)
DECLARE @ReportDateDimDateKey VARCHAR(32)

SET @SessionPriceGreaterThan = 0	
SET @ReportDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date))
						  															
										
  -----  We are always looking back 2 months from the report month.									
DECLARE @ReportMonthPriorOneDimDateKey VARCHAR(32)										
DECLARE @ReportMonthPriorTwoDimDateKey VARCHAR(32)										
																										
SET @ReportMonthPriorOneDimDateKey = (SELECT prior_month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = Cast(@ReportDate as Date) GROUP BY prior_month_starting_dim_date_key )										
SET @ReportMonthPriorTwoDimDateKey = (SELECT prior_month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE month_starting_dim_date_key = @ReportMonthPriorOneDimDateKey GROUP BY prior_month_starting_dim_date_key )										




 ------   Delete and repopulate records if the query is run again for the same 3 month period
 ------   Also delete any older delivery record which is not for a 1:1 product
  DELETE fact_ptdssr_client_retention_detail
  WHERE delivered_month_starting_dim_date_key >= @ReportMonthPriorTwoDimDateKey
        OR one_on_one_pt_product_flag = 'N'

 ------  Populate table with new records

  INSERT INTO fact_ptdssr_client_retention_detail (
  delivered_date_dim_date_key,
  delivered_dim_club_key,
  delivered_dim_employee_key,
  delivered_employee_id,
  delivered_four_digit_year_dash_two_digit_month,
  delivered_month_starting_dim_date_key,
  delivered_price,
  dim_member_key,
  dim_mms_membership_key,
  dim_product_key,
  employee_home_dim_club_key,
  member_date_of_birth, 
  member_first_name,
  member_last_name,
  one_on_one_pt_product_flag,
  product_description,
  product_dim_reporting_hierarchy_key,
  report_date_dim_date_key,
  source_system,
  dv_load_date_time,		-- need to include all dv_columns in stored procedure
  dv_load_end_date_time,	-- need to include all dv_columns in stored procedure
  dv_batch_id,				-- need to include all dv_columns in stored procedure
  dv_inserted_date_time,	-- need to include all dv_columns in stored procedure
  dv_insert_user			-- need to include all dv_columns in stored procedure
  )

									
 SELECT 
	DeliveryDimDate.dim_date_key AS delivered_date_dim_date_key,
	FactPackageSession.delivered_dim_club_key,
	FactPackageSession.delivered_dim_employee_key,
	DeliveredEmployee.employee_id AS delivered_employee_id,
	DeliveryDimDate.four_digit_year_dash_two_digit_month AS delivered_four_digit_year_dash_two_digit_month,
	DeliveryDimDate.month_starting_dim_date_key AS delivered_month_starting_dim_date_key,   
	FactPackageSession.delivered_session_price AS delivered_price,
	FactPackageSession.dim_mms_member_key AS dim_member_key,	
	DimCustomer.dim_mms_membership_key,	
	FactPackageSession.fact_mms_package_dim_product_key AS dim_product_key,
	DeliveredEmployee.dim_club_key AS employee_home_dim_club_key,
	DimCustomer.date_of_birth AS member_date_of_birth,
	DimCustomer.first_name AS member_first_name,
	DimCustomer.last_name AS member_last_name,
	CASE WHEN OneOnOneProducts.MMSProductID Is Null
      THEN 'N'
	  ELSE 'Y'
	  END one_on_one_pt_product_flag,
	DimProduct.product_description,
	DimProduct.dim_reporting_hierarchy_key AS product_dim_reporting_hierarchy_key,
	@ReportDateDimDateKey AS report_date_dim_date_key,
	'MMS' AS source_system,
	getdate(),												--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	convert(datetime, '99991231', 112),						--this value would be same for all the stored procedure
	'-1',													--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	getdate(),												--this value would be same for all the stored procedure
    suser_sname()											--this value would be same for all the stored procedure								
	
FROM [marketing].[v_fact_mms_package_session] FactPackageSession
	JOIN [marketing].[v_dim_mms_product] DimProduct										
		ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
	JOIN [marketing].[v_dim_date]  DeliveryDimDate										
		ON FactPackageSession.delivered_dim_date_key = DeliveryDimDate.dim_date_key	
	JOIN [marketing].[v_dim_employee]  DeliveredEmployee
	    ON FactPackageSession.delivered_dim_employee_key = DeliveredEmployee.dim_employee_key
	JOIN [marketing].[v_dim_mms_member] DimCustomer										
		ON FactPackageSession.dim_mms_member_key = DimCustomer.dim_mms_member_key
	LEFT JOIN [reporting].[v_PTDSSR_OneOnOneProduct] OneOnOneProducts
        ON FactPackageSession.fact_mms_package_dim_product_key = OneOnOneProducts.DimProductKey
WHERE FactPackageSession.delivered_dim_date_key >= @ReportMonthPriorTwoDimDateKey   ----- first of 2 months prior
  AND DimProduct.reporting_division = 'Personal Training'
  AND FactPackageSession.delivered_session_price > @SessionPriceGreaterThan

UNION ALL

SELECT 
	DeliveryDate.dim_date_key AS delivered_date_dim_date_key,
	Booking.dim_club_key AS delivered_dim_club_key,
	Instructors.dim_employee_key AS delivered_dim_employee_key,
	DeliveredEmployee.employee_id AS delivered_employee_id,
	DeliveryDate.four_digit_year_dash_two_digit_month AS delivered_four_digit_year_dash_two_digit_month,
	DeliveryDate.month_starting_dim_date_key AS delivered_month_starting_dim_date_key, 
	SubscriptionPeriod.price_per_booking AS delivered_price,       
	Participation.dim_mms_member_key AS dim_member_key,	   
	DimCustomer.dim_mms_membership_key,	   
	ExerpActivity.dim_mms_product_key AS dim_product_key,	   
	DeliveredEmployee.dim_club_key AS employee_home_dim_club_key,
	DimCustomer.date_of_birth AS member_date_of_birth,
	DimCustomer.first_name AS member_first_name,
	DimCustomer.last_name AS member_last_name,	   
	'N' AS	one_on_one_pt_product_flag,   
	MMSProduct.product_description,
	MMSProduct.dim_reporting_hierarchy_key AS product_dim_reporting_hierarchy_key,	   
	@ReportDateDimDateKey AS report_date_dim_date_key,
	'Exerp' AS source_system,
	getdate(),												--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	convert(datetime, '99991231', 112),						--this value would be same for all the stored procedure
	'-1',													--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	getdate(),												--this value would be same for all the stored procedure
    suser_sname()											--this value would be same for all the stored procedure	

FROM   [marketing].[v_dim_exerp_booking] Booking
  JOIN [marketing].[v_fact_exerp_participation] Participation
    ON Booking.dim_exerp_booking_key = Participation.dim_exerp_booking_key
  JOIN [marketing].[v_dim_exerp_staff_usage] Instructors
    ON Booking.booking_id = Instructors.booking_id 
  JOIN [marketing].[v_dim_exerp_activity] ExerpActivity
    ON Booking.dim_exerp_activity_key = ExerpActivity.dim_exerp_activity_key
  JOIN [marketing].[v_dim_mms_product] MMSProduct
    ON ExerpActivity.dim_mms_product_key = MMSProduct.dim_mms_product_key
  JOIN [marketing].[v_dim_employee]  DeliveredEmployee
	ON Instructors.dim_employee_key = DeliveredEmployee.dim_employee_key
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
END
