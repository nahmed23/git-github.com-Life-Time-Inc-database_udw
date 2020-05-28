CREATE PROC [sandbox].[proc_ops_PTrev_PCSD] AS 
begin

Declare @StartDate date
	 , @EndDate date
	 , @ReportRunDateTime datetime


SET @StartDate = DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1)-1,0)    ----- returns 1st of prior month
                     
SET @EndDate = DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1)+1,0) -------returns 1st of next month

DECLARE @StartDimDateKey INT,
        @StartMonthStartingDimDateKey INT,
        @ReportStartDate VARCHAR(12),
        @PriorYearStartDimDateKey INT,
        @StartMonthEndingDimDateKey varchar(8)

SELECT @StartDimDateKey = dd1.dim_date_key,
       @StartMonthStartingDimDateKey = dd1.month_starting_dim_date_key,
       @ReportStartDate = dd1.calendar_date,
       @PriorYearStartDimDateKey = dd2.dim_date_key,
       @StartMonthEndingDimDateKey = dd1.month_ending_dim_date_key
 FROM [marketing].[v_dim_date] dd1
 join marketing.v_dim_date dd2 on dd1.prior_year_date = dd2.calendar_date
WHERE dd1.calendar_date = @StartDate

DECLARE @EndDimDateKey INT,
        @EndMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
        @ReportEndDate VARCHAR(12),
        @EndMonthEndingDate DateTime

SELECT @EndDimDateKey = dim_date_key,
       @EndMonthEndingDimDateKey = month_ending_dim_date_key,
       @ReportEndDate = calendar_date,
       @EndMonthEndingDate = month_ending_date
 FROM [marketing].[v_dim_date]
WHERE calendar_date = @EndDate


IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchy; 
create table #DimReportingHierarchy with (distribution = round_robin, heap) as
with dept (reporting_department) as
(
    select distinct reporting_department 
      from marketing.v_dim_reporting_hierarchy_history 
      
),
drh (dim_reporting_hierarchy_key, reporting_division, reporting_sub_division, reporting_department, reporting_product_group, reporting_region_type) as
(
select drh.dim_reporting_hierarchy_key, drh.reporting_division, drh.reporting_sub_division, drh.reporting_department, drh.reporting_product_group, drh.reporting_region_type
from marketing.v_dim_reporting_hierarchy_history drh
), 
c (c) as 
(
    select count(distinct reporting_region_type) from drh
)
select drh.dim_reporting_hierarchy_key DimReportingHierarchyKey, 
       drh.reporting_division DivisionName, 
       drh.reporting_sub_division SubdivisionName, 
       drh.reporting_department DepartmentName, 
       drh.reporting_product_group ProductGroupName, 
       drh.reporting_region_type RegionType,
       case when c.c = 1 then drh.reporting_region_type else 'MMS Region' end ReportRegionType,
       case when drh.reporting_product_group in ('Weight Loss Challenges','90 Day Weight Loss')then 'Y' else 'N' end PTDeferredRevenueProductGroupFlag
  from drh
  cross join c

  DECLARE @StartDateMonthStartDimDateKey INT
DECLARE @EndDateMonthStartDimDateKey INT
DECLARE @StartDateCalendarMonthNumberInYear INT
DECLARE @EndDateCalendarMonthNumberInYear INT
DECLARE @EndDatePriorMonthEndDateDimDateKey INT

  SET @StartDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartDimDateKey) 
SET @EndDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key  = @EndDimDateKey) 
SET @StartDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key  = @StartDimDateKey)
SET @EndDateCalendarMonthNumberInYear = (SELECT month_number_in_year   FROM [marketing].[v_dim_date] WHERE dim_date_key  = @EndDimDateKey)
SET @EndDatePriorMonthEndDateDimDateKey = (SELECT prior_month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key  = @EndDimDateKey)



DECLARE @EComm60DayChallengeRevenueStartDimDateKey INT
  ---- When the start date is the 1st of the 2nd month of the quarter, set the start date to the 1st of the prior month
SET @EComm60DayChallengeRevenueStartDimDateKey = (SELECT CASE WHEN (@StartDimDateKey = @StartDateMonthStartDimDateKey)          ---- Date range begins on the 1st of a month
                                                              THEN (CASE WHEN @StartDateCalendarMonthNumberInYear in(2,5,8,11)
                                                                         THEN (Select prior_month_starting_dim_date_key
                                                                                 FROM [marketing].[v_dim_date]
                                                                                WHERE dim_date_key = @StartDimDateKey)
                                                                          WHEN @StartDateCalendarMonthNumberInYear in(1,4,7,10)
                                                                          THEN (Select month_starting_dim_date_key
                                                                                  FROM [marketing].[v_dim_date] 
                                                                                 WHERE dim_date_key = @StartDimDateKey) 
                                                                          ELSE @StartDimDateKey
                                                                                   END)
                                                
                                                              ELSE  @StartDimDateKey END
                                                  FROM [marketing].[v_dim_date]
                                                  WHERE dim_date_key = @StartDimDateKey ) ---- to limit result set to one record)

DECLARE @EComm60DayChallengeRevenueEndDimDateKey INT
  ---- When the End Date is in the 1st month of the quarter, set the end date to the end of the prior month
SET @EComm60DayChallengeRevenueEndDimDateKey = (SELECT CASE WHEN @EndDateCalendarMonthNumberInYear in(1,4,7,10)
                                                            THEN @EndDatePriorMonthEndDateDimDateKey 
                                                            ELSE @EndDimDateKey
                                                            END
                                                FROM [marketing].[v_dim_date]
                                                WHERE dim_date_key = @EndDimDateKey)  ---- to limit result set to one record



IF OBJECT_ID('tempdb.dbo.#base', 'U') IS NOT NULL DROP TABLE #base;
create table #base with (distribution = hash(source_fact_table_key)) as
    SELECT FactAllocatedTransaction.sales_source,
           FactAllocatedTransaction.source_fact_table_key,
            FactAllocatedTransaction.transaction_type,
              FactAllocatedTransaction.allocated_quantity,
              FactAllocatedTransaction.allocated_amount,
              FactAllocatedTransaction.transaction_quantity,
              FactAllocatedTransaction.transaction_amount,
              FactAllocatedTransaction.discount_amount,
              FactAllocatedTransaction.transaction_dim_date_key,
              FactAllocatedTransaction.payment_types,
              FactAllocatedTransaction.transaction_id,
              FactAllocatedTransaction.line_number,
              FactAllocatedTransaction.dim_mms_member_key,
              FactAllocatedTransaction.dim_mms_membership_key,
              FactAllocatedTransaction.dim_mms_transaction_reason_key,
              FactAllocatedTransaction.sales_channel_dim_description_key,
              FactAllocatedTransaction.allocated_dim_club_key,
              FactAllocatedTransaction.primary_sales_dim_employee_key,
              FactAllocatedTransaction.allocated_month_starting_dim_date_key,
              FactAllocatedTransaction.dim_product_key,
              FactAllocatedTransaction.source_product_id,
              FactAllocatedTransaction.product_description,
              FactAllocatedTransaction.dim_reporting_hierarchy_key,
              FactAllocatedTransaction.reporting_division,
              FactAllocatedTransaction.reporting_sub_division,
              FactAllocatedTransaction.reporting_department,
              FactAllocatedTransaction.reporting_product_group,
              FactAllocatedTransaction.allocation_rule,
              FactAllocatedTransaction.ecommerce_shipment_number,
              FactAllocatedTransaction.ecommerce_order_number,
              FactAllocatedTransaction.ecommerce_autoship_flag,
              FactAllocatedTransaction.ecommerce_shipping_and_handling_amount,
              FactAllocatedTransaction.ecommerce_product_cost,
              FactAllocatedTransaction.mms_tran_id,
              FactAllocatedTransaction.mms_tran_item_id,
              FactAllocatedTransaction.exerp_sale_employee_id,
              FactAllocatedTransaction.exerp_service_employee_id,
              FactAllocatedTransaction.membership_id,
              FactAllocatedTransaction.membership_type,
              FactAllocatedTransaction.member_id,
              FactAllocatedTransaction.member_name,
              FactAllocatedTransaction.member_first_name,
              FactAllocatedTransaction.member_last_name,
              DimLocation.Workday_Region,
              DimLocation.Club_Name,
              DimLocation.Club_ID,
              DimLocation.GL_Club_ID,
              DimLocation.Local_Currency_Code

    from dbo.fact_allocated_transaction_item FactAllocatedTransaction
    join marketing.v_dim_reporting_hierarchy drh on FactAllocatedTransaction.dim_reporting_hierarchy_key = drh.Dim_Reporting_Hierarchy_Key --filter
 --   JOIN #sales_sourceList SalesSourceList ON FactAllocatedTransaction.sales_source = SalesSourceList.sales_source --filter
    JOIN marketing.v_dim_club DimLocation ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.Dim_Club_Key --filter


    WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @StartDimDateKey --everything *sold* in range that isn't ecomm deferral
           AND FactAllocatedTransaction.transaction_dim_date_key < @EndDimDateKey
           and FactAllocatedTransaction.ecommerce_deferral_flag = 'N')
       OR (FactAllocatedTransaction.sales_source = 'MMS' --everything mms *deferring* to range, and isn't Sale Month Activity.  This prevents transactions allocating to current month but aren't between the dates.  Seems arbitrary, and only really impactful to historical reporting (testing on 2/26 for last 10/1-10/28)
           AND FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
           AND FactAllocatedTransaction.allocated_month_starting_dim_date_key < @EndMonthEndingDimDateKey
           AND FactAllocatedTransaction.allocation_rule <> 'Sale Month Activity')
       OR (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey --ecomm *deferrals* in range
              AND FactAllocatedTransaction.transaction_dim_date_key < @EComm60DayChallengeRevenueEndDimDateKey
              AND FactAllocatedTransaction.ecommerce_deferral_flag = 'Y')

IF OBJECT_ID('tempdb.dbo.#AllocatedTransactionDetail', 'U') IS NOT NULL  
  DROP TABLE #AllocatedTransactionDetail;

----- To create a temp table for the desired allocated transaction records for the stored procecure rather than
----- linking so many attribute views to the larger full view,  This can also be re-used for building the MMS discounts temp table
create table #AllocatedTransactionDetail with (distribution = hash(source_fact_table_key)) as
   SELECT FactAllocatedTransaction.sales_source AS SalesSource,
          FactAllocatedTransaction.source_fact_table_key,
          FactAllocatedTransaction.Workday_Region,
          FactAllocatedTransaction.Club_Name,
          FactAllocatedTransaction.Club_ID,
          FactAllocatedTransaction.GL_Club_ID,
          PostDimDate.calendar_date AS TranDate,   -------- Name Change  no time returned
          PostDimDate.calendar_date AS PostedDate,
          FactAllocatedTransaction.transaction_type AS TransactionType, 
          FactAllocatedTransaction.source_product_id SourceProductID, ----------BSD
          FactAllocatedTransaction.product_description ProductDescription,-------------BSD
          FactAllocatedTransaction.reporting_department RevenueReportingDepartmentName,        
          FactAllocatedTransaction.reporting_product_group RevenueProductGroup,

          CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997','-998','-999') 
                    THEN NULL
               ELSE PrimarySalesDimEmployee.employee_id 
               END PrimarySellingTeamMemberID,
          CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
                    THEN NULL
               ELSE PrimarySalesDimEmployee.employee_name_last_first
               END PrimarySellingTeamMember,
          NULL SecondarySellingTeamMemberID,
          NULL SecondarySellingTeamMember,
          FactAllocatedTransaction.membership_id AS MembershipID,
          FactAllocatedTransaction.membership_type AS MembershipTypeDescription,
          FactAllocatedTransaction.member_id AS MemberID,
          FactAllocatedTransaction.member_name AS MemberName,
          RevenueMonthDimDate.four_digit_year_dash_two_digit_month AS RevenueYearMonth,
          FactAllocatedTransaction.allocated_quantity AS RevenueQuantity,
          FactAllocatedTransaction.allocated_amount AS RevenueAmount,
          FactAllocatedTransaction.transaction_quantity AS SaleQuantity,
          FactAllocatedTransaction.transaction_amount AS SaleAmount,
          FactAllocatedTransaction.discount_amount AS TotalDiscountAmount,
          FactAllocatedTransaction.Local_Currency_Code AS CurrencyCode,
          FactAllocatedTransaction.transaction_dim_date_key AS TranDimDateKey,
          NULL SaleDimTimeKey,
          FactAllocatedTransaction.member_first_name AS MemberFirstName,
          FactAllocatedTransaction.member_last_name AS MemberLastName,
          NULL SoldNotServicedFlag,
          FactAllocatedTransaction.payment_types AS PaymentType,     
          TransactionReason.description AS TransactionReason,
          SalesChannel.description AS SalesChannel,
          0 CorporateTransferAmount,
          FactAllocatedTransaction.reporting_division DivisionName, ----BSD
          FactAllocatedTransaction.reporting_sub_division SubDivisionName, -----BSD
          FactAllocatedTransaction.ecommerce_shipment_number ECommerceShipmentNumber,
          FactAllocatedTransaction.ecommerce_order_number ECommerceOrderNumber, 
          FactAllocatedTransaction.ecommerce_autoship_flag ECommerceAutoshipFlag, 
          Null ECommerceOrderEntryTrackingNumber,
          FactAllocatedTransaction.ecommerce_shipping_and_handling_amount ECommerceShippingAndHandlingAmount, 
          FactAllocatedTransaction.ecommerce_product_cost ECommerceProductCost,
          FactAllocatedTransaction.mms_tran_item_id MMSTranItemID,
          FactAllocatedTransaction.mms_tran_id MMSTranID,
          FactAllocatedTransaction.allocated_month_starting_dim_date_key,
          FactAllocatedTransaction.exerp_service_employee_id,
          FactAllocatedTransaction.exerp_sale_employee_id,
		  FactAllocatedTransaction.dim_product_key

   FROM #base FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_mms_transaction_reason] TransactionReason
     ON FactAllocatedTransaction.dim_mms_transaction_reason_key = TransactionReason.dim_mms_transaction_reason_key
   LEFT JOIN [marketing].[v_dim_description] SalesChannel
     ON FactAllocatedTransaction.sales_channel_dim_description_key = SalesChannel.dim_description_key
   JOIN [marketing].[v_dim_date] PostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = PostDimDate.dim_date_key
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
     ON FactAllocatedTransaction.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key
   JOIN [marketing].[v_dim_date] RevenueMonthDimDate
     ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key



IF OBJECT_ID('tempdb.dbo.#TransactionDiscountDetail', 'U') IS NOT NULL
  DROP TABLE #TransactionDiscountDetail;

 SELECT DiscountRank.TranItemID,
          SUM(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.DiscountAmount ELSE 0 END) DiscountAmount1,
          SUM(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.DiscountAmount ELSE 0 END) DiscountAmount2,
          SUM(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.DiscountAmount ELSE 0 END) DiscountAmount3,
          SUM(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.DiscountAmount ELSE 0 END) DiscountAmount4,
          SUM(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.DiscountAmount ELSE 0 END) DiscountAmount5,
          CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) 
                  THEN NULL
               ELSE MAX(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END)
             END Discount1,
          CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) 
                  THEN NULL
               ELSE MAX(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END)
             END Discount2,
          CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) 
                  THEN NULL
               ELSE MAX(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END)
             END Discount3,
          CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) 
                  THEN NULL
               ELSE MAX(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END)
             END Discount4,
          CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) 
                  THEN NULL
               ELSE MAX(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END)
             END Discount5
			 		  , @StartDate as StartDate
   INTO #TransactionDiscountDetail    
   FROM (SELECT FactClubPOSAllocatedRevenueDiscount.TranItemID,     
                RANK() OVER (PARTITION BY FactClubPOSAllocatedRevenueDiscount.TranItemID 
                             ORDER BY FactClubPOSAllocatedRevenueDiscount.FactClubPOSAllocatedRevenueDiscountKey) Ranking,
                FactClubPOSAllocatedRevenueDiscount.DiscountAmount DiscountAmount,
                DimMMSPricingDiscount.sales_promotion_receipt_text AS SalesPromotionReceiptText                        
         FROM (SELECT MIN(FactMMSAllocatedTransactionItemDiscount.tran_item_id) TranItemID, 
                      MIN(FactMMSAllocatedTransactionItemDiscount.discount_amount) DiscountAmount, 
                      MIN(FactMMSAllocatedTransactionItemDiscount.dim_mms_pricing_discount_key) DimPricingDiscountKey, 
                      MIN(FactMMSAllocatedTransactionItemDiscount.fact_mms_allocated_transaction_item_discount_key) FactClubPOSallocatedRevenueDiscountKey
                 FROM #AllocatedTransactionDetail MMSAllocatedTransactionItem
                 JOIN [marketing].[v_fact_mms_allocated_transaction_item_discount]  FactMMSAllocatedTransactionItemDiscount       
                   ON MMSAllocatedTransactionItem.MMSTranItemID = FactMMSAllocatedTransactionItemDiscount.tran_item_id

             WHERE     MMSAllocatedTransactionItem.SalesSource = 'MMS'
                   AND MMSAllocatedTransactionItem.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
                   AND MMSAllocatedTransactionItem.allocated_month_starting_dim_date_key < @EndMonthEndingDimDateKey
             GROUP BY FactMMSAllocatedTransactionItemDiscount.tran_item_id,FactMMSAllocatedTransactionItemDiscount.dim_mms_pricing_discount_key
                 ) FactClubPOSAllocatedRevenueDiscount
         JOIN [marketing].[v_dim_mms_pricing_discount] DimMMSPricingDiscount     
           ON FactClubPOSAllocatedRevenueDiscount.DimPricingDiscountKey = DimMMSPricingDiscount.dim_mms_pricing_discount_key) DiscountRank
   WHERE DiscountRank.Ranking <= 5
   GROUP BY DiscountRank.TranItemID



--Result set
SELECT distinct TranDetail.SalesSource as SourceSystem,
       TranDetail.Workday_Region,
       TranDetail.Club_Name,
       TranDetail.Club_ID,
       TranDetail.TranDate,
       TranDetail.PostedDate,
       TranDetail.SourceProductID,
       TranDetail.ProductDescription,
       TranDetail.RevenueReportingDepartmentName,
       TranDetail.RevenueProductGroup,
       TranDetail.PrimarySellingTeamMemberID as EmployeeID,
       Emp.Employee_Name as EmployeeName,
       TranDetail.MembershipID,
       TranDetail.MemberID,
       TranDetail.MemberName,
       TranDetail.RevenueYearMonth,
       TranDetail.RevenueQuantity,
       TranDetail.RevenueAmount,
       TranDetail.SaleQuantity,
       TranDetail.SaleAmount,
       TranDetail.TotalDiscountAmount,
          TranDetail.CurrencyCode,
       TranDetail.TranDimDateKey,
       --TranDetail.SaleDimTimeKey,
       --TranDetail.MemberFirstName,
       --TranDetail.MemberLastName,
       TranDetail.DivisionName,
       TranDetail.SubdivisionName,
       TranDetail.ECommerceShipmentNumber,
       TranDetail.ECommerceOrderNumber,
       TranDetail.ECommerceAutoshipFlag,
       TranDetail.ECommerceOrderEntryTrackingNumber,
       TranDetail.ECommerceShippingAndHandlingAmount,
       TranDetail.ECommerceProductCost,
       isnull(TranDetail.MMSTranID,0) MMSTranID,
       isnull(TranDetail.MMSTranItemID,0) MMSTranItemID,
       TranDetail.source_fact_table_key,
	   case when trandetail.salessource = 'Magento' then mp.Workday_Offering_ID
	   else case when trandetail.salessource = 'Cafe' then 'Cafe'
	   else case when trandetail.salessource = 'MMS' then mmsp.Workday_Offering else null end end end as WorkdayOffering,
	   case when trandetail.salessource = 'Magento' then mp.Workday_Costcenter_ID
	   else case when trandetail.salessource = 'Cafe' then 'Cafe'
	   else case when trandetail.salessource = 'MMS' then mmsp.Workday_Cost_center else null end end end as WorkdayCostCenter
	
into #dataset
FROM #AllocatedTransactionDetail TranDetail   
 left join #TransactionDiscountDetail
   on trandetail.MMSTranItemID = #transactiondiscountdetail.tranitemid
   	left join marketing.v_dim_magento_product mp on convert(varchar(55),mp.sku) = convert(varchar(55),TranDetail.SourceProductID)
	left join marketing.v_dim_cafe_product cp on convert(varchar(55),cp.menu_item_id) = convert(varchar(55),trandetail.SourceProductID)
	left join marketing.v_dim_mms_product mmsp on convert(varchar(55),mmsp.product_ID) = convert(varchar(55),trandetail.SourceProductID)
	left join marketing.v_dim_employee emp on emp.employee_ID = TranDetail.PrimarySellingTeamMemberID
WHERE ((TranDetail.PrimarySellingTeamMemberID Is not Null  AND TranDetail.PrimarySellingTeamMemberID > -997 )
        OR ((TranDetail.PrimarySellingTeamMemberID Is Null OR TranDetail.PrimarySellingTeamMemberID < -997) ))
    AND TranDetail.RevenueAmount <> 0
	and trandetail.DivisionName = 'Personal Training'
--ORDER BY source_fact_table_key,TranDetail.Workday_Region, TranDetail.Club_Name, TranDetail.SaleDimDateKey, TranDetail.SaleDimTimeKey, TranDetail.MemberFirstname, TranDetail.MemberLastName, TranDetail.MemberID,TranDetail.RevenueYearMonth


--select *

--from #dataset



IF OBJECT_ID('tempdb.dbo.#DeliverySet', 'U') IS NOT NULL
  DROP TABLE #DeliverySet; 

SELECT 'MMS' as SourceSystem,
	   DeliveryDimLocation.Workday_Region,
	   DeliveryDimLocation.Club_Name,
	   DeliveryDimLocation.Club_ID,
	  d.calendar_date AS TranDate,
	d.calendar_date as PostedDate,
	convert(varchar(55),PackageProduct.product_ID) as SourceProductID,
	   PackageProduct.Product_Description as ProductDescription,
	   	   PackageProduct.Reporting_department as RevenueReportingDepartmentName,
		   PackageProduct.Reporting_Product_Group as RevenueProductGroup,
		   DeliveredEmployee.employee_id as EmployeeID,
		   DeliveredEmployee.employee_name as EmployeeName,
		   	m.membership_ID as MembershipID,
			m.member_id as MemberID,
			m.customer_name as MemberName,
			d.four_digit_year_dash_two_digit_month as RevenueYearMonth,
			1 as RevenueQuantity,
--       FactPackageSession.delivered_session_price AS DeliveredSessionPrice,
--	   FactPackageSession.delivered_session_lt_bucks_amount as LTBucksAmount,
	   FactPackageSession.delivered_session_price - isnull(FactPackageSession.Delivered_session_lt_bucks_amount, 0) as RevenueAmount,
	   Null as SaleQuantity,
	   Null as SaleAmount,
	   FactPackageSession.Delivered_Session_LT_Bucks_Amount as TotalDiscountAmount,
	   FactPackageSession.original_currency_code as CurrencyCode,
	   d.dim_date_key as TranDimDateKey,
	   PackageProduct.Reporting_Division as DivisionName,
	   PackageProduct.Reporting_Sub_Division as SubDivisionName,
	   Null as ECommerceShipmentNumber,
       Null as ECommerceOrderNumber,
       Null as ECommerceAutoshipFlag,
       Null as ECommerceOrderEntryTrackingNumber,
       Null as ECommerceShippingAndHandlingAmount,
       Null as ECommerceProductCost,
       Null as  MMSTranID,
       Null as  MMSTranItemID,
       'Null' as source_fact_table_key,
	   PackageProduct.Workday_offering as WorkdayOffering,
	   PackageProduct.Workday_cost_center as WorkdayCostCenter,
	   'Null' as participation_id,
	   'Null' as booking_id,
	   'Null' as subscription_id,
	   'Delivery' as 'RevType'

	INTO #DeliverySet
FROM [marketing].[v_fact_mms_package_session] FactPackageSession
	JOIN marketing.v_dim_club DeliveryDimLocation
	  ON FactPackageSession.[delivered_dim_club_key] = DeliveryDimLocation.dim_club_key
    JOIN marketing.v_dim_mms_product PackageProduct
      ON FactPackageSession.fact_mms_package_dim_product_key = PackageProduct.Dim_MMS_Product_Key
	left join marketing.v_dim_mms_department dep on dep.department_id = packageproduct.department_ID
	LEFT JOIN [marketing].[v_dim_employee] DeliveredEmployee  ON FactPackageSession.Delivered_dim_employee_key = DeliveredEmployee.dim_employee_key
	left join marketing.v_dim_mms_member m on FactPackageSession.dim_mms_member_key = m.dim_mms_member_key
    left join marketing.v_dim_date d on d.dim_date_key = FactPackageSession.Delivered_Dim_Date_Key
	 
WHERE FactPackageSession.delivered_dim_date_key >= @StartDimDateKey
      AND FactPackageSession.delivered_dim_date_key < @EndDimDateKey
	 and PackageProduct.Reporting_Division = 'Personal Training'
	 and PackageProduct.Workday_offering <> 'OF10117'



if object_id('tempdb..#clubs') is not null drop table #clubs
create table #clubs with (distribution = round_robin) as
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  FROM [marketing].[v_dim_club] DimClub
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key in('-997','-998','-999')  OR DimClub.club_close_dim_date_key > @StartDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description

if object_id('tempdb..#DimLocationInfo') is not null drop table #DimLocationInfo
create table #DimLocationInfo with (distribution = round_robin) as
SELECT DimClub.DimClubKey,      ------ name change
      DimClub.MMSRegion as Region,
       DimClub.ClubName AS MMSClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.LocalCurrencyCode
  INTO #DimLocationInfo    
  FROM #Clubs DimClub     
 GROUP BY DimClub.DimClubKey, 
      DimClub.MMSRegion,
       DimClub.ClubName,
	   DimClub.club_id,
	   DimClub.gl_club_id,
	   DimClub.LocalCurrencyCode


/* Find all Participation records related to Pilates/SGT , where booking date is after the Start Date of the Reporting Range till the date the subscription is Paid*/
IF OBJECT_ID('tempdb.dbo.#SubscriptionSet', 'U') IS NOT NULL
drop table #SubscriptionSet;

/* Number of Participations */
select * 

into #SubscriptionSet
from
(SELECT 'Exerp' as SourceSystem,
	c.Workday_Region,
       c.club_name,
       c.Club_ID,
       d.calendar_date as TranDate,
	   d.calendar_date as PostedDate,
	   convert(varchar(55),mmsp.product_id) as SourceProductID,
	   mmsp.Product_description as ProductDescription,
	   mmsp.Reporting_department as RevenueReportingDepartment,
	   mmsp.Reporting_Product_group as RevenueProductGroup,
       dim_employee.employee_ID as EmployeeID,
	   Dim_employee.employee_name as EmployeeName,
	   mem.membership_ID as MemberShipID,
	   mem.member_ID as MemberID,
	   mem.customer_name as MemberName,
	   d.four_digit_year_dash_two_digit_month as RevenueYearMonth,
	   1 as RevenueQuantity,
       case when fact_exerp_subscription_participation.delivered_dim_date_key between @StartDimDateKey and @EndDimDateKey
                 then dim_exerp_subscription_period.price_per_booking
            else 0
        end RevenueAmount,	  
		Null as SaleQuantity,
		Null as SaleAmount,
		Null as TotalDiscount,
		'USD' as CurrencyCode,
		d.dim_date_key as TranDimDateKey,
		mmsp.Reporting_Division as DivisionName,
	    mmsp.Reporting_Sub_division as SubDivisionName,
	   Null as ECommerceShipmentNumber,
       Null as ECommerceOrderNumber,
       Null as ECommerceAutoshipFlag,
       Null as ECommerceOrderEntryTrackingNumber,
       Null as ECommerceShippingAndHandlingAmount,
       Null as ECommerceProductCost,
       Null as  MMSTranID,
       Null as  MMSTranItemID,
       dim_exerp_subscription.dim_exerp_subscription_key as source_fact_table_key,
       mmsp.workday_cost_center,
       mmsp.workday_offering,
	   fact_exerp_subscription_participation.participation_id,
	   bo.booking_id,
       dim_exerp_subscription.subscription_id,
	   'Subscription' as RevType


 
  from marketing.v_fact_exerp_subscription_participation fact_exerp_subscription_participation
  join marketing.v_dim_exerp_subscription_period dim_exerp_subscription_period 
       on fact_exerp_subscription_participation.dim_exerp_subscription_period_key = dim_exerp_subscription_period.dim_exerp_subscription_period_key
  join marketing.v_dim_exerp_subscription dim_exerp_subscription 
       on dim_exerp_subscription_period.dim_exerp_subscription_key = dim_exerp_subscription.dim_exerp_subscription_key
  join marketing.v_dim_exerp_booking bo 
       on fact_exerp_subscription_participation.dim_exerp_booking_key = bo.dim_exerp_booking_key
  join marketing.v_dim_exerp_product p 
       on fact_exerp_subscription_participation.dim_exerp_product_key = p.dim_exerp_product_key
  left join marketing.v_dim_mms_Product mmsp 
       on fact_exerp_subscription_participation.dim_mms_product_key = mmsp.dim_mms_product_key
  inner join marketing.v_dim_club c 
       on fact_exerp_subscription_participation.dim_club_key = c.dim_club_key
  inner join marketing.v_dim_description mmsregion 
       on c.region_dim_description_key = mmsregion.dim_description_key
  join marketing.v_dim_employee dim_employee
    on fact_exerp_subscription_participation.dim_employee_key = dim_employee.dim_employee_key
	join marketing.v_dim_date d on d.dim_date_key = fact_exerp_subscription_participation.delivered_dim_date_key
	join marketing.v_dim_mms_member mem on mem.dim_mms_member_key = fact_exerp_subscription_participation.dim_mms_member_key
  where 1=1
    and (dim_exerp_subscription_period.from_dim_date_key between @StartDimDateKey and @EndDimDateKey 
         or dim_exerp_subscription_period.to_dim_date_key between @StartDimDateKey and @EndDimDateKey)
		 and dim_exerp_subscription_period.from_dim_date_key < 20200318
		 and dim_exerp_subscription_period.to_dim_date_key < 20200318

		 union all

	select 'Exerp' as SourceSystem,
	c.Workday_Region,
       c.club_name,
       c.Club_ID,
       dtran.calendar_date as TranDate,
	   d.calendar_date as PostedDate,
	   convert(varchar(55),pr.product_id) as SourceProductID,
	   pr.Product_description as ProductDescription,
	   pr.Reporting_department as RevenueReportingDepartment,
	   pr.Reporting_Product_group as RevenueProductGroup,
       e.employee_ID as EmployeeID,
	   e.employee_name as EmployeeName,
	   mem.membership_ID as MemberShipID,
	   mem.member_ID as MemberID,
	   mem.customer_name as MemberName,
	   d.four_digit_year_dash_two_digit_month as RevenueYearMonth,
	   1 as RevenueQuantity,
       ati.allocated_amount as RevenueAmount,	  
		Null as SaleQuantity,
		Null as SaleAmount,
		Null as TotalDiscount,
		c.local_currency_code as CurrencyCode,
		d.dim_date_key as TranDimDateKey,
		pr.Reporting_Division as DivisionName,
	    pr.Reporting_Sub_division as SubDivisionName,
	   Null as ECommerceShipmentNumber,
       Null as ECommerceOrderNumber,
       Null as ECommerceAutoshipFlag,
       Null as ECommerceOrderEntryTrackingNumber,
       Null as ECommerceShippingAndHandlingAmount,
       Null as ECommerceProductCost,
       Null as  MMSTranID,
       Null as  MMSTranItemID,
       sub.dim_exerp_subscription_key as source_fact_table_key,
       pr.workday_cost_center,
       pr.workday_offering,
	   	   p.participation_id,
	   bk.booking_id,
       sub.subscription_id,
	   'Subscription' as RevType


from marketing.v_dim_exerp_subscription sub 
join marketing.v_dim_exerp_subscription_period sp on sub.dim_exerp_subscription_key = sp.dim_exerp_subscription_key
join marketing.v_dim_club c on c.dim_club_key = sub.dim_club_key
join marketing.v_dim_exerp_product pro on pro.dim_exerp_product_key = sub.dim_exerp_product_key
join marketing.v_fact_exerp_transaction_log tlog on tlog.fact_exerp_transaction_log_key = sp.fact_exerp_transaction_log_key
left join marketing.v_fact_mms_transaction_item ti on tlog.external_ID = ti.external_item_id
left join marketing.v_dim_mms_product pr on pr.dim_mms_product_key = ti.dim_mms_product_key
left join marketing.v_fact_mms_allocated_transaction_item ati on ati.tran_item_id = ti.tran_item_id
left join marketing.v_dim_employee e on e.dim_employee_key = ati.primary_sales_dim_employee_key
left join marketing.v_fact_exerp_participation p on p.dim_exerp_subscription_period_key = sp.dim_exerp_subscription_period_key
left join marketing.v_dim_exerp_booking bk on bk.dim_exerp_booking_key = p.dim_exerp_booking_key
left join marketing.v_dim_date d on d.dim_date_key = sp.from_dim_date_key
left join marketing.v_dim_date dtran on dtran.dim_date_key = ti.post_dim_date_key
left join marketing.v_dim_mms_member mem on mem.dim_mms_member_key = ti.dim_mms_member_key

where sp.from_dim_date_key >= @StartDimDateKey
and sp.from_dim_date_key < @EndDimDateKey
and pr.reporting_product_group in ('Pilates Group', 'SGT Flex')
and pr.package_product_flag = 'N'
 ) u


select a.*,
	   'Null' as participation_id,
	   'Null' as booking_id,
	   'Null' as subscription_id
	, 'Products' as RevType


into #productset
from #dataset a 

where Workdayoffering in ('Cafe','OF54010', 'OF10104', 'OF10105', 'OF10202', 'OF10220', 'OF10122', 'OF10123', 'OF10115')



select a.*,
	   'Null' as participation_id,
	   'Null' as booking_id,
	   'Null' as subscription_id
	, 'Challenges' as RevType


into #challengeset
from #dataset a


where Workdayoffering = 'OF10117'

--if object_id('lt_udw.[sandbox].[ops_PTrev_PCSD]') is not null drop table [sandbox].[ops_PTrev_PCSD]
--create table [sandbox].[ops_PTrev_PCSD] with (distribution = round_robin) as

select * 


into #tempset
from (

select *

from #productset

union all

select *

from #Challengeset

union all

select *

from #DeliverySet

union all

select *

from #SubscriptionSet
where RevenueAmount <> 0
and convert(date,convert(varchar(8),TranDimDateKey)) < cast(getdate() as date)
) t

Delete from sandbox.ops_PTrev_PCSD where source_fact_table_key in

(select tmp.source_fact_table_key


from #tempset tmp
        join sandbox.ops_PTrev_PCSD fulldataset on tmp.source_fact_table_key = fulldataset.source_fact_table_key)

insert into sandbox.ops_PTrev_PCSD
select *

from #tempset

drop table #dataset
drop table #challengeset
drop table #productset
drop table #deliverySet
drop table #SubscriptionSet
drop table #tempset



end
