CREATE PROC [reporting].[proc_RevenueTransactionDetail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimLocationKeyList [VARCHAR](4000),@SalesSourceList [VARCHAR](4000),@CommissionTypeList [VARCHAR](4000),@DimReportingHierarchyKeyList [Varchar](8000),@TotalReportingHierarchyKeyCount [INT],@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@myLTBucksProductFilter [VARCHAR](50) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 
 ----- Sample Execution
---   Exec [reporting].[proc_RevenueTransactionDetail] '10/1/2019','10/28/2019','All Regions','151','MMS|Cafe|HealthCheckUSA|Magento','Commissioned|Non-Commissioned','All Product Groups',100,'Personal Training','All Subdivisions','All Departments','Not Limited by myLT Bucks'


 -----
 -----  This stored procedure is used for Report ID 107 "Revenue Transaction Detail"

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')



SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
                      WHEN @StartDate = 'Dec 30, 1899'
                      THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
                      ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
                    ELSE @EndDate END

DECLARE @StartDimDateKey INT,
        @StartMonthStartingDimDateKey INT,
        @ReportStartDate VARCHAR(12),
        @PriorYearStartDimDateKey INT,
        @StartMonthEndingDimDateKey varchar(8)

SELECT @StartDimDateKey = dd1.dim_date_key,
       @StartMonthStartingDimDateKey = dd1.month_starting_dim_date_key,
       @ReportStartDate = dd1.standard_date_name,
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
       @ReportEndDate = standard_date_name,
       @EndMonthEndingDate = month_ending_date
 FROM [marketing].[v_dim_date]
WHERE calendar_date = @EndDate


------- Create Hierarchy temp table to return selected group names      
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchy; 
create table #DimReportingHierarchy with (distribution = round_robin, heap) as
with dept (reporting_department) as
(
    select reporting_department 
      from marketing.v_dim_reporting_hierarchy_history 
      where ('|'+@DepartmentMinDimReportingHierarchyKeyList+'|' like '%|'+dim_reporting_hierarchy_key+'|%' 
             or @DepartmentMinDimReportingHierarchyKeyList like '%All Departments%')
      group by reporting_department
),
drh (dim_reporting_hierarchy_key, reporting_division, reporting_sub_division, reporting_department, reporting_product_group, reporting_region_type) as
(
select drh.dim_reporting_hierarchy_key, drh.reporting_division, drh.reporting_sub_division, drh.reporting_department, drh.reporting_product_group, drh.reporting_region_type
from marketing.v_dim_reporting_hierarchy_history drh
where ('|'+@DivisionList+'|' like '%|'+drh.reporting_division+'|%' or @DivisionList like '%All Divisions%')
  and ('|'+@SubDivisionList+'|' like '%|'+drh.reporting_sub_division+'|%' or @SubDivisionList like '%All Subdivisions%')
  and ('|'+@DimReportingHierarchyKeyList+'|' like '%|'+drh.dim_reporting_hierarchy_key+'|%' or @DimReportingHierarchyKeyList like '%All Product Groups%')
  and drh.effective_dim_date_key <= @EndMonthEndingDimDateKey
  and drh.expiration_dim_date_key >= @StartMonthEndingDimDateKey
  and drh.reporting_department in (select reporting_department from dept)
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

DECLARE @HeaderDivisionList VARCHAR(8000),
        @HeaderSubdivisionList VARCHAR(8000),
        @RegionType VARCHAR(50)
SELECT @HeaderDivisionList = CASE WHEN @DivisionList like '%All Divisions%'  
                                    THEN 'All Divisions'
                                  ELSE REPLACE(@DivisionList, '|', ', ') 
                                  END,
       @HeaderSubdivisionList = CASE WHEN @SubDivisionList like '%All Subdivisions%' 
                                       THEN 'All Subdivisions' 
                                     ELSE REPLACE(@SubDivisionList, '|', ', ') 
                                     END,                                         
       @RegionType = (SELECT MIN(ReportRegionType) FROM #DimReportingHierarchy)

 ----- Create Sales Source temp table   
IF OBJECT_ID('tempdb.dbo.#sales_sourceList', 'U') IS NOT NULL DROP TABLE #sales_sourceList;   
create table #sales_sourceList with (distribution = round_robin, heap) as
select source_system sales_source
from d_udwcloudsync_product_master 
where '|'+@SalesSourceList+'|' like '%|'+source_system+'|%'
group by source_system


 ----- When All Regions and All Clubs are selection options, and the Regions could be of different types
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL DROP TABLE #DimLocationInfo;   
create table #DimLocationInfo with (distribution = round_robin, heap) as
SELECT DimClub.dim_club_key AS DimClubKey, 
        DimClub.club_id MMSClubID, 
        DimClub.club_name AS MMSClubName,
        DimClub.gl_club_id GLClubID,
        DimClub.local_currency_code AS LocalCurrencyCode,
        CASE WHEN @RegionType = 'PT RCL Area' THEN PTRCLRegion.description
            WHEN @RegionType = 'Member Activities Region' THEN MemberActivitiesRegion.description
            WHEN @RegionTYpe = 'MMS Region' THEN MMSRegion.description 
        END Region
    FROM [marketing].[v_dim_club] DimClub
    --JOIN #club_list ClubKeyList ON ClubKeyList.Item = DimClub.club_id OR ClubKeyList.Item = -1
    JOIN [marketing].[v_dim_description]  MMSRegion ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
    JOIN [marketing].[v_dim_description]  PTRCLRegion ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
    JOIN [marketing].[v_dim_description]  MemberActivitiesRegion ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
   where ('|'+@DimLocationKeyList+'|' like '%|'+cast(DimClub.club_id as varchar)+'|%' or '|'+@DimLocationKeyList+'|' like '%|-1|%')
    and (   ('|'+@RegionList+'|' like '%|'+cast(MMSRegion.description as varchar)+'|%' and @RegionType = 'MMS Region')
         or ('|'+@RegionList+'|' like '%|'+cast(PTRCLRegion.description as varchar)+'|%' and @RegionType = 'PT RCL Area')
         or ('|'+@RegionList+'|' like '%|'+cast(MemberActivitiesRegion.description as varchar)+'|%' and @Regiontype = 'Member Activities Region')
         or @regionList like '%All Regions%')
    and DimClub.club_id Not In (-1,99,100)
    AND DimClub.club_id < 900
    AND DimClub.club_type = 'Club'
    AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @StartDimDateKey)  
GROUP BY DimClub.dim_club_key, 
        DimClub.club_id, 
        DimClub.club_name,
        DimClub.club_code,
        DimClub.gl_club_id,
        DimClub.local_currency_code,
        CASE WHEN @RegionType = 'PT RCL Area' THEN PTRCLRegion.description
            WHEN @RegionType = 'Member Activities Region' THEN MemberActivitiesRegion.description
            WHEN @RegionTYpe = 'MMS Region' THEN MMSRegion.description 
        END




  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month

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

--get underlying data, filter by above temp tables, attach relevent columns
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
              DimLocation.Region,
              DimLocation.MMSClubName,
              DimLocation.MMSClubID,
              DimLocation.GLClubID,
              DimLocation.LocalCurrencyCode
    from dbo.fact_allocated_transaction_item FactAllocatedTransaction
    join #DimReportingHierarchy drh on FactAllocatedTransaction.dim_reporting_hierarchy_key = drh.DimReportingHierarchyKey --filter
    JOIN #sales_sourceList SalesSourceList ON FactAllocatedTransaction.sales_source = SalesSourceList.sales_source --filter
    JOIN #DimLocationInfo  DimLocation ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey --filter
    WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @StartDimDateKey --everything *sold* in range that isn't ecomm deferral
           AND FactAllocatedTransaction.transaction_dim_date_key <= @EndDimDateKey
           and FactAllocatedTransaction.ecommerce_deferral_flag = 'N')
       OR (FactAllocatedTransaction.sales_source = 'MMS' --everything mms *deferring* to range, and isn't Sale Month Activity.  This prevents transactions allocating to current month but aren't between the dates.  Seems arbitrary, and only really impactful to historical reporting (testing on 2/26 for last 10/1-10/28)
           AND FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey 
           AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey
           AND FactAllocatedTransaction.allocation_rule <> 'Sale Month Activity')
       OR (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey --ecomm *deferrals* in range
              AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey
              AND FactAllocatedTransaction.ecommerce_deferral_flag = 'Y')

IF OBJECT_ID('tempdb.dbo.#AllocatedTransactionDetail', 'U') IS NOT NULL  
  DROP TABLE #AllocatedTransactionDetail;

----- To create a temp table for the desired allocated transaction records for the stored procecure rather than
----- linking so many attribute views to the larger full view,  This can also be re-used for building the MMS discounts temp table
create table #AllocatedTransactionDetail with (distribution = hash(source_fact_table_key)) as
   SELECT FactAllocatedTransaction.sales_source AS SalesSource,
          FactAllocatedTransaction.source_fact_table_key,
          FactAllocatedTransaction.Region,
          FactAllocatedTransaction.MMSClubName,
          FactAllocatedTransaction.MMSClubID,
          FactAllocatedTransaction.GLClubID,
          PostDimDate.standard_date_name AS SaleDate,   -------- Name Change  no time returned
          PostDimDate.standard_date_name AS PostedDate,
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
          FactAllocatedTransaction.LocalCurrencyCode AS CurrencyCode,
          FactAllocatedTransaction.transaction_dim_date_key AS SaleDimDateKey,
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
          FactAllocatedTransaction.exerp_sale_employee_id
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
                   AND MMSAllocatedTransactionItem.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey
             GROUP BY FactMMSAllocatedTransactionItemDiscount.tran_item_id,FactMMSAllocatedTransactionItemDiscount.dim_mms_pricing_discount_key
                 ) FactClubPOSAllocatedRevenueDiscount
         JOIN [marketing].[v_dim_mms_pricing_discount] DimMMSPricingDiscount     
           ON FactClubPOSAllocatedRevenueDiscount.DimPricingDiscountKey = DimMMSPricingDiscount.dim_mms_pricing_discount_key) DiscountRank
   WHERE DiscountRank.Ranking <= 5
   GROUP BY DiscountRank.TranItemID

--Result set
SELECT TranDetail.SalesSource,
       TranDetail.Region,
       TranDetail.MMSClubName,
       TranDetail.MMSClubID,
       TranDetail.GLClubID,
       TranDetail.SaleDate,
       TranDetail.PostedDate,
       TranDetail.TransactionType,
       TranDetail.SourceProductID,
       TranDetail.ProductDescription,
       TranDetail.RevenueReportingDepartmentName,
       TranDetail.RevenueProductGroup,
       TranDetail.PrimarySellingTeamMemberID,
       TranDetail.PrimarySellingTeamMember,
       TranDetail.SecondarySellingTeamMemberID,
       TranDetail.SecondarySellingTeamMember,
       TranDetail.MembershipID,
       TranDetail.MembershipTypeDescription,
       TranDetail.MemberID,
       TranDetail.MemberName,
       TranDetail.RevenueYearMonth,
       TranDetail.RevenueQuantity,
       TranDetail.RevenueAmount,
       TranDetail.SaleQuantity,
       TranDetail.SaleAmount,
       TranDetail.TotalDiscountAmount,
       #TransactionDiscountDetail.DiscountAmount1,
       #TransactionDiscountDetail.DiscountAmount2,
       #TransactionDiscountDetail.DiscountAmount3,
       #TransactionDiscountDetail.DiscountAmount4,
       #TransactionDiscountDetail.DiscountAmount5,
       #TransactionDiscountDetail.Discount1,
       #TransactionDiscountDetail.Discount2,
       #TransactionDiscountDetail.Discount3,
       #TransactionDiscountDetail.Discount4,
       #TransactionDiscountDetail.Discount5,
       TranDetail.CurrencyCode,
       TranDetail.SaleDimDateKey,
       TranDetail.SaleDimTimeKey,
       TranDetail.MemberFirstName,
       TranDetail.MemberLastName,
       CASE WHEN TranDetail.ProductDescription like '%SNS%'
            THEN 'Y'
            WHEN IsNull(TranDetail.PrimarySellingTeamMemberID,'-998') = '-998'
            THEN 'N'
            WHEN IsNull(TranDetail.exerp_service_employee_id,'-998') = '-998'
            THEN 'N'
            WHEN TranDetail.exerp_sale_employee_id = TranDetail.exerp_service_employee_id
            THEN 'N'
            ELSE 'Y'
            END SoldNotServicedFlag,
       TranDetail.PaymentType,
       TranDetail.TransactionReason,
       TranDetail.SalesChannel,
       TranDetail.CorporateTransferAmount,
       TranDetail.DivisionName,
       TranDetail.SubdivisionName,
       @ReportStartDate ReportStartDate,
       @ReportEndDate ReportEndDate,
       @ReportRunDateTime ReportRunDateTime,
       NULL RevenueReportingDepartmentNameCommaList,    --@RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,    ------ must build in Cognos
       NULL  RevenueProductGroupNameCommaList,   --@RevenueProductGroupNameCommaList RevenueProductGroupNameCommaList,                  ------ must build in Cognos
       @HeaderDivisionList HeaderDivisionList,
       @HeaderSubdivisionList HeaderSubdivisionList,
       TranDetail.ECommerceShipmentNumber,
       TranDetail.ECommerceOrderNumber,
       TranDetail.ECommerceAutoshipFlag,
       TranDetail.ECommerceOrderEntryTrackingNumber,
       TranDetail.ECommerceShippingAndHandlingAmount,
       TranDetail.ECommerceProductCost,
       CASE WHEN @myLTBucksProductFilter = 'myLT Bucks Only'
            THEN 'myLT Buck$ Only'
            WHEN @myLTBucksProductFilter = 'Exclude myLT Bucks'
            THEN 'Exclude myLT Buck$'
            WHEN @myLTBucksProductFilter = 'Not Limited by myLT Bucks'
            THEN 'Not Limited by myLT Buck$'
            END HeadermyLTBucksProductFilter,
       isnull(TranDetail.MMSTranID,0) MMSTranID,
       isnull(TranDetail.MMSTranItemID,0) MMSTranItemID,
       TranDetail.exerp_sale_employee_id AS Exerp_SalesEmployeeID,
       TranDetail.exerp_service_employee_id AS Exerp_ServiceEmployeeID,
       TranDetail.source_fact_table_key
FROM #AllocatedTransactionDetail TranDetail   
 left join #TransactionDiscountDetail
   on trandetail.MMSTranItemID = #transactiondiscountdetail.tranitemid
WHERE  ((TranDetail.SalesChannel = 'Loyalty Program' and @myLTBucksProductFilter = 'myLT Bucks Only')   ----- EmployeeID = -5 "Loyalty Program"  ---- This flags the "Bucks Store" transactions
            OR
           (TranDetail.SalesChannel is Null and @myLTBucksProductFilter ='Exclude myLT Bucks')
            OR
           (TranDetail.SalesChannel <> 'Loyalty Program' and @myLTBucksProductFilter ='Exclude myLT Bucks')
            OR
           (@myLTBucksProductFilter = 'Not Limited by myLT Bucks'))
  AND ((TranDetail.PrimarySellingTeamMemberID Is not Null  AND TranDetail.PrimarySellingTeamMemberID > -997 and @CommissionTypeList like 'Commissioned%')
        OR ((TranDetail.PrimarySellingTeamMemberID Is Null OR TranDetail.PrimarySellingTeamMemberID < -997) and @CommissionTypeList like '%Non-commissioned'))
    AND TranDetail.RevenueAmount <> 0
ORDER BY source_fact_table_key,TranDetail.Region, TranDetail.MMSClubName, TranDetail.SaleDimDateKey, TranDetail.SaleDimTimeKey, TranDetail.MemberFirstname, TranDetail.MemberLastName, TranDetail.MemberID,TranDetail.RevenueYearMonth



DROP TABLE #sales_sourceList
DROP TABLE #DimLocationInfo
DROP TABLE #DimReportingHierarchy  
DROP TABLE #AllocatedTransactionDetail





END
