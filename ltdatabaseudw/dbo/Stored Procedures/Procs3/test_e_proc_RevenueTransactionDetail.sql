CREATE PROC [dbo].[test_e_proc_RevenueTransactionDetail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimLocationKeyList [VARCHAR](4000),@SalesSourceList [VARCHAR](4000),@CommissionTypeList [VARCHAR](4000),@DimReportingHierarchyKeyList [Varchar](8000),@TotalReportingHierarchyKeyCount [INT],@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@myLTBucksProductFilter [VARCHAR](50) AS
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
		@PriorYearStartDimDateKey INT

SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @StartMonthStartingDimDateKey  =(SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @ReportStartDate = (SELECT standard_date_name FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @PriorYearStartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date =(SELECT prior_year_date FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate))


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

Exec [reporting].[proc_DimReportingHierarchy_history] @DivisionList,@SubDivisionList,@DepartmentMinDimReportingHierarchyKeyList,@DimReportingHierarchyKeyList,@StartDimDateKey,@EndDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 


 SELECT DimReportingHierarchyKey,  
       DivisionName,    
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType,
	   CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
		END PTDeferredRevenueProductGroupFlag
 INTO #DimReportingHierarchy   
 FROM #OuterOutputTable


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
IF OBJECT_ID('tempdb.dbo.#sales_sourceList', 'U') IS NOT NULL
  DROP TABLE #sales_sourceList;   

DECLARE @list_table VARCHAR(100)
SET @list_table = 'sales_source_list'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT DISTINCT sales_sourceList.Item sales_source
  INTO #sales_sourceList
  FROM #sales_source_list  sales_sourceList


 ----- When All Regions and All Clubs are selection options, and the Regions could be of different types
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

  ----- Create club temp table
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @DimLocationKeyList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id
	  OR ClubKeyList.Item = -1
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @StartDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description

IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
  DROP TABLE #DimLocationInfo;

  ----- Create Region temp table
SET @list_table = 'region_list'

  EXEC marketing.proc_parse_pipe_list @RegionList,@list_table
	
SELECT DimClub.DimClubKey,      ------ name change
      CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion 
		   END  Region,
       DimClub.ClubName AS MMSClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.LocalCurrencyCode
  INTO #DimLocationInfo    
  FROM #Clubs DimClub     
  JOIN #region_list RegionList 
   ON RegionList.Item = CASE WHEN @RegionType = 'PT RCL Area' 
                                   THEN DimClub.PTRCLRegion
                              WHEN @RegionType = 'Member Activities Region' 
                                   THEN DimClub.MemberActivitiesRegion
                              WHEN @RegionTYpe = 'MMS Region' 
                                   THEN DimClub.MMSRegion END
     OR RegionList.Item = 'All Regions'
 GROUP BY DimClub.DimClubKey, 
      CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion 
		   END,
       DimClub.ClubName,
	   DimClub.club_id,
	   DimClub.gl_club_id,
	   DimClub.LocalCurrencyCode





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


IF OBJECT_ID('tempdb.dbo.#AllocatedTransactionDetail', 'U') IS NOT NULL  
  DROP TABLE #AllocatedTransactionDetail;
  create table #AllocatedTransactionDetail with (distribution = hash(transaction_id)) as
----- To create a temp table for the desired allocated transaction records for the stored procecure rather than
----- linking so many attribute views to the larger full view,  This can also be re-used for building the MMS discounts temp table

SELECT FactAllocatedTransaction.sales_source,
FactAllocatedTransaction.transaction_type,
          FactAllocatedTransaction.allocated_quantity,
          FactAllocatedTransaction.allocated_amount,
          FactAllocatedTransaction.transaction_quantity,
          FactAllocatedTransaction.transaction_amount,
          FactAllocatedTransaction.discount_amount,
		  FactAllocatedTransaction.transaction_dim_date_key,
		  FactAllocatedTransaction.payment_types,
		  FactAllocatedTransaction.transaction_id,
		  FactAllocatedTransaction.autoship_flag,
		  FactAllocatedTransaction.shipping_and_handling_amount,
		  FactAllocatedTransaction.product_cost,
		  FactAllocatedTransaction.line_number,
		  FactAllocatedTransaction.dim_product_key,
		  FactAllocatedTransaction.dim_mms_member_key,
		  FactAllocatedTransaction.dim_mms_transaction_reason_key,
		  FactAllocatedTransaction.sales_channel_dim_description_key,
		  FactAllocatedTransaction.allocated_dim_club_key,
		  FactAllocatedTransaction.primary_sales_dim_employee_key,
		  FactAllocatedTransaction.allocated_month_starting_dim_date_key,
		  1 as FilteringId
	--INTO #AllocatedTransactionDetail   
FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @StartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EndDimDateKey)

UNION

SELECT FactAllocatedTransaction.sales_source,
FactAllocatedTransaction.transaction_type,
          FactAllocatedTransaction.allocated_quantity,
          FactAllocatedTransaction.allocated_amount,
          FactAllocatedTransaction.transaction_quantity,
          FactAllocatedTransaction.transaction_amount,
          FactAllocatedTransaction.discount_amount,
		  FactAllocatedTransaction.transaction_dim_date_key,
		  FactAllocatedTransaction.payment_types,
		  FactAllocatedTransaction.transaction_id,
		  FactAllocatedTransaction.autoship_flag,
		  FactAllocatedTransaction.shipping_and_handling_amount,
		  FactAllocatedTransaction.product_cost,
		  FactAllocatedTransaction.line_number,
		  FactAllocatedTransaction.dim_product_key,
		  FactAllocatedTransaction.dim_mms_member_key,
		  FactAllocatedTransaction.dim_mms_transaction_reason_key,
		  FactAllocatedTransaction.sales_channel_dim_description_key,
		  FactAllocatedTransaction.allocated_dim_club_key,
		  FactAllocatedTransaction.primary_sales_dim_employee_key,
		  FactAllocatedTransaction.allocated_month_starting_dim_date_key,
		  1 as FilteringId	
FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
     ON FactAllocatedTransaction.dim_product_key = DimMMSProduct.dim_mms_product_key
	   AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate
WHERE (FactAllocatedTransaction.sales_source = 'MMS'
		  AND DimMMSProduct.allocation_rule <> 'Sale Month Activity'
	      AND FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
	      AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey )

UNION

SELECT FactAllocatedTransaction.sales_source,
FactAllocatedTransaction.transaction_type,
          FactAllocatedTransaction.allocated_quantity,
          FactAllocatedTransaction.allocated_amount,
          FactAllocatedTransaction.transaction_quantity,
          FactAllocatedTransaction.transaction_amount,
          FactAllocatedTransaction.discount_amount,
		  FactAllocatedTransaction.transaction_dim_date_key,
		  FactAllocatedTransaction.payment_types,
		  FactAllocatedTransaction.transaction_id,
		  FactAllocatedTransaction.autoship_flag,
		  FactAllocatedTransaction.shipping_and_handling_amount,
		  FactAllocatedTransaction.product_cost,
		  FactAllocatedTransaction.line_number,
		  FactAllocatedTransaction.dim_product_key,
		  FactAllocatedTransaction.dim_mms_member_key,
		  FactAllocatedTransaction.dim_mms_transaction_reason_key,
		  FactAllocatedTransaction.sales_channel_dim_description_key,
		  FactAllocatedTransaction.allocated_dim_club_key,
		  FactAllocatedTransaction.primary_sales_dim_employee_key,
		  FactAllocatedTransaction.allocated_month_starting_dim_date_key,
		  2 as FilteringId	
FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')



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
                   ON MMSAllocatedTransactionItem.line_number = FactMMSAllocatedTransactionItemDiscount.tran_item_id

			 WHERE 	MMSAllocatedTransactionItem.sales_source = 'MMS'
				   AND MMSAllocatedTransactionItem.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
				   AND MMSAllocatedTransactionItem.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey
             GROUP BY FactMMSAllocatedTransactionItemDiscount.tran_item_id,FactMMSAllocatedTransactionItemDiscount.dim_mms_pricing_discount_key
			     ) FactClubPOSAllocatedRevenueDiscount
         JOIN [marketing].[v_dim_mms_pricing_discount] DimMMSPricingDiscount     
           ON FactClubPOSAllocatedRevenueDiscount.DimPricingDiscountKey = DimMMSPricingDiscount.dim_mms_pricing_discount_key) DiscountRank
   WHERE DiscountRank.Ranking <= 5
   GROUP BY DiscountRank.TranItemID





IF OBJECT_ID('tempdb.dbo.#TransactionDetail', 'U') IS NOT NULL
  DROP TABLE #TransactionDetail;

  create table #transactiondetail with (distribution = hash(transaction_id)) as
   SELECT FactAllocatedTransaction.transaction_id,
          FactAllocatedTransaction.sales_source AS SalesSource,
          DimLocation.Region,
          DimLocation.MMSClubName,
          DimLocation.MMSClubID,
          DimLocation.GLClubID,
          PostDimDate.standard_date_name AS SaleDate,   -------- Name Change  no time returned
          PostDimDate.standard_date_name AS PostedDate,
          FactAllocatedTransaction.transaction_type AS TransactionType, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN CONVERT(VARCHAR(50),DimCafeProduct.menu_item_id)
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.code
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN CONVERT(VARCHAR(50),DimHealthCheckUSAProduct.product_sku)
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN CONVERT(VARCHAR(50),DimMMSProduct.product_id)
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN CONVERT(VARCHAR(50),DimMagentoProduct.sku)
			   END SourceProductID,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimCafeProduct.menu_item_name
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimMMSProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  ProductDescription,
 		 CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DepartmentName 
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,        
 		 CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.ProductGroupName 
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  RevenueProductGroup, 
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
          Member.membership_id AS MembershipID,
          Membership.membership_type AS MembershipTypeDescription,
          Member.member_id AS MemberID,
          Member.customer_name_last_first AS MemberName,
          RevenueMonthDimDate.four_digit_year_dash_two_digit_month AS RevenueYearMonth,
          FactAllocatedTransaction.allocated_quantity AS RevenueQuantity,
          FactAllocatedTransaction.allocated_amount AS RevenueAmount,
          FactAllocatedTransaction.transaction_quantity AS SaleQuantity,
          FactAllocatedTransaction.transaction_amount AS SaleAmount,
          FactAllocatedTransaction.discount_amount AS TotalDiscountAmount,
          DiscountDetail.DiscountAmount1,                              
		  DiscountDetail.DiscountAmount2,
		  DiscountDetail.DiscountAmount3,
		  DiscountDetail.DiscountAmount4,
		  DiscountDetail.DiscountAmount5,
		  DiscountDetail.Discount1,
		  DiscountDetail.Discount2,
		  DiscountDetail.Discount3,
		  DiscountDetail.Discount4,
		  DiscountDetail.Discount5,
		  DimLocation.LocalCurrencyCode AS CurrencyCode,
		  FactAllocatedTransaction.transaction_dim_date_key AS SaleDimDateKey,
          NULL SaleDimTimeKey,
          Member.first_name AS MemberFirstName,
          Member.last_name AS MemberLastName,
          NULL SoldNotServicedFlag,
          FactAllocatedTransaction.payment_types AS PaymentType,     
          TransactionReason.description AS TransactionReason,
          SalesChannel.description AS SalesChannel,
          0 CorporateTransferAmount,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN DimReportingHierarchy_Magento.DivisionName
			   END  DivisionName,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
			   END  SubdivisionName,
          CASE WHEN FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		       THEN FactAllocatedTransaction.transaction_id
			   ELSE NULL
			   END ECommerceShipmentNumber,
          CASE WHEN FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		       THEN FactAllocatedTransaction.line_number
			   ELSE NULL
			   END ECommerceOrderNumber, 
          CASE WHEN FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		       THEN FactAllocatedTransaction.autoship_flag
			   ELSE NULL
			   END ECommerceAutoshipFlag, 
          Null ECommerceOrderEntryTrackingNumber,
          CASE WHEN FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		       THEN FactAllocatedTransaction.shipping_and_handling_amount
			   ELSE NULL
			   END ECommerceShippingAndHandlingAmount, 
          CASE WHEN FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		       THEN FactAllocatedTransaction.product_cost
			   ELSE NULL
			   END ECommerceProductCost,
          CASE WHEN FactAllocatedTransaction.sales_source in('MMS')
		       THEN FactAllocatedTransaction.line_number
			   ELSE 0
			   END MMSTranItemID,
          CASE WHEN FactAllocatedTransaction.sales_source in('MMS')
		       THEN CAST(FactAllocatedTransaction.transaction_id AS INT)
			   ELSE 0
			   END MMSTranID
	--INTO #TransactionDetail  
   FROM #AllocatedTransactionDetail FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct
     ON FactAllocatedTransaction.dim_product_key = DimCafeProduct.dim_cafe_product_key
	   AND FactAllocatedTransaction.sales_source = 'Cafe'
	   AND DimCafeProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimCafeProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
     ON FactAllocatedTransaction.dim_product_key = DimMMSProduct.dim_mms_product_key
	   AND FactAllocatedTransaction.sales_source = 'MMS'
	   AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe
     ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey
	 ------AND DimReportingHierarchy_Cafe.PTDeferredRevenueProductGroupFlag = 'N'       --- this was eliminating any 60 Day challenge products sold through the cafe
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
     ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey
	 ------AND DimReportingHierarchy_MMS.PTDeferredRevenueProductGroupFlag = 'N'   --- this was eliminating any 60 Day challenge products sold through MMS
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN [marketing].[v_dim_mms_member] Member
     ON FactAllocatedTransaction.dim_mms_member_key = Member.dim_mms_member_key
   LEFT JOIN [marketing].[v_dim_mms_membership] Membership
     ON Member.membership_id = Membership.membership_id
   LEFT JOIN [marketing].[v_dim_mms_transaction_reason] TransactionReason
     ON FactAllocatedTransaction.dim_mms_transaction_reason_key = TransactionReason.dim_mms_transaction_reason_key
   LEFT JOIN [marketing].[v_dim_description] SalesChannel
     ON FactAllocatedTransaction.sales_channel_dim_description_key = SalesChannel.dim_description_key
  -- LEFT JOIN #TransactionDiscountDetail MMSTranDiscount     
  --   ON FactAllocatedTransaction.line_number = MMSTranDiscount.TranItemID
	 --AND FactAllocatedTransaction.sales_source = 'MMS'
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] PostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = PostDimDate.dim_date_key
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
     ON FactAllocatedTransaction.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key
   JOIN [marketing].[v_dim_date] RevenueMonthDimDate
     ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
   JOIN #sales_sourceList SalesSourceList
     ON FactAllocatedTransaction.sales_source = SalesSourceList.sales_source
   LEFT JOIN #TransactionDiscountDetail DiscountDetail
     ON FactAllocatedTransaction.line_number = DiscountDetail.TranItemID
	  AND FactAllocatedTransaction.sales_source = 'MMS'
   WHERE FactAllocatedTransaction.FilteringID = 1


UNION ALL

   SELECT FactAllocatedTransaction.transaction_id,
          FactAllocatedTransaction.sales_source AS SalesSource,
          DimLocation.Region,
          DimLocation.MMSClubName,
          DimLocation.MMSClubID,
          DimLocation.GLClubID,
          PostDimDate.standard_date_name AS SaleDate,   -------- Name Change  no time returned
          PostDimDate.standard_date_name AS PostedDate,
          FactAllocatedTransaction.transaction_type AS TransactionType, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.code
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN CONVERT(VARCHAR(50),DimHealthCheckUSAProduct.product_sku)
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN CONVERT(VARCHAR(50),DimMagentoProduct.sku)
			   END SourceProductID,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  ProductDescription,
 		 CASE  WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,        
 		 CASE  WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  RevenueProductGroup, 
          CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') IN('-997','-998','-999')
		         THEN NULL
               ELSE PrimarySalesDimEmployee.employee_id 
			   END PrimarySellingTeamMemberID,
          CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') IN('-997','-998','-999')
                    THEN NULL
               ELSE PrimarySalesDimEmployee.employee_name_last_first
			   END PrimarySellingTeamMember,
          NULL SecondarySellingTeamMemberID,
          NULL SecondarySellingTeamMember,
          Member.membership_id AS MembershipID,
          Membership.membership_type AS MembershipTypeDescription,
          Member.member_id AS MemberID,
          Member.customer_name_last_first AS MemberName,
          RevenueMonthDimDate.four_digit_year_dash_two_digit_month AS RevenueYearMonth,
          FactAllocatedTransaction.allocated_quantity AS RevenueQuantity,
          FactAllocatedTransaction.allocated_amount AS RevenueAmount,
          FactAllocatedTransaction.transaction_quantity AS SaleQuantity,
          FactAllocatedTransaction.transaction_amount AS SaleAmount,
          FactAllocatedTransaction.discount_amount AS TotalDiscountAmount,
          NULL DiscountAmount1,                                              
          NULL DiscountAmount2,
          NULL DiscountAmount3,
          NULL DiscountAmount4,
          NULL DiscountAmount5,
          NULL Discount1,
          NULL Discount2,
          NULL Discount3,
          NULL Discount4,
          NULL Discount5,
		  DimLocation.LocalCurrencyCode AS CurrencyCode,
		  FactAllocatedTransaction.transaction_dim_date_key AS SaleDimDateKey,
          NULL SaleDimTimeKey,
          Member.first_name AS MemberFirstName,
          Member.last_name AS MemberLastName,
          NULL SoldNotServicedFlag,
          FactAllocatedTransaction.payment_types AS PaymentType,     
          TransactionReason.description AS TransactionReason,
          SalesChannel.description AS SalesChannel,
          0 CorporateTransferAmount,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DivisionName
			   END  DivisionName,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
			   END  SubdivisionName,
          FactAllocatedTransaction.transaction_id AS ECommerceShipmentNumber,
          FactAllocatedTransaction.line_number AS ECommerceOrderNumber, 
          FactAllocatedTransaction.autoship_flag AS ECommerceAutoshipFlag, 
          Null ECommerceOrderEntryTrackingNumber,
          FactAllocatedTransaction.shipping_and_handling_amount AS ECommerceShippingAndHandlingAmount, 
          FactAllocatedTransaction.product_cost AS ECommerceProductCost,
		  0 AS MMSTranItemID,
		  0 AS MMSTranID
   FROM #AllocatedTransactionDetail FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'   
   LEFT JOIN [marketing].[v_dim_mms_member] Member
     ON FactAllocatedTransaction.dim_mms_member_key = Member.dim_mms_member_key
   LEFT JOIN [marketing].[v_dim_mms_membership] Membership
     ON Member.membership_id = Membership.membership_id
   LEFT JOIN [marketing].[v_dim_mms_transaction_reason] TransactionReason
     ON FactAllocatedTransaction.dim_mms_transaction_reason_key = TransactionReason.dim_mms_transaction_reason_key
   LEFT JOIN [marketing].[v_dim_description] SalesChannel
     ON FactAllocatedTransaction.sales_channel_dim_description_key = SalesChannel.dim_description_key
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] PostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = PostDimDate.dim_date_key
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
     ON FactAllocatedTransaction.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key
   JOIN [marketing].[v_dim_date] RevenueMonthDimDate
     ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
   JOIN #sales_sourceList SalesSourceList
     ON FactAllocatedTransaction.sales_source = SalesSourceList.sales_source
   WHERE FactAllocatedTransaction.FilteringID = 2





IF OBJECT_ID('tempdb.dbo.#ExerpProductSaleAndSvcEmployees', 'U') IS NOT NULL
  DROP TABLE #ExerpProductSaleAndSvcEmployees; 

  create table #ExerpProductSaleAndSvcEmployees with (distribution = hash(mms_tran_id)) as
  SELECT MMS.mms_tran_id,
       MMS.tran_item_id,
	   SalesEmployee.employee_id AS Exerp_SaleEmployeeID,
	   CASE WHEN IsNull(SubscriptionParticipation.service_employee_key,'null') = 'null'
	        THEN ClipcardEmployee.employee_id
			ELSE SubscriptionEmployee.employee_id
			END Exerp_ServiceEmployeeID
  --INTO #ExerpProductSaleAndSvcEmployees  
       FROM [marketing].[v_fact_exerp_transaction_log] TranLog
        JOIN [marketing].[v_fact_mms_transaction_item] MMS 
		  ON TranLog.external_id = MMS.external_item_id
   LEFT JOIN [marketing].[v_dim_employee] SalesEmployee
          ON MMS.primary_sales_dim_employee_key = SalesEmployee.dim_employee_key
   LEFT JOIN [marketing].[v_dim_exerp_subscription_period] SubscriptionPeriod 
		  ON TranLog.fact_exerp_transaction_log_key = SubscriptionPeriod.fact_exerp_transaction_log_key
   LEFT JOIN [reporting].[v_exerp_subscription_first_participation_record] SubscriptionParticipation 
		  ON SubscriptionPeriod.dim_exerp_subscription_key = SubscriptionParticipation.dim_exerp_subscription_key
   LEFT JOIN [marketing].[v_dim_employee] SubscriptionEmployee
          ON SubscriptionParticipation.service_employee_key = SubscriptionEmployee.dim_employee_key
   LEFT JOIN [marketing].[v_dim_exerp_clipcard] Clipcard  
		  ON TranLog.fact_exerp_transaction_log_key = Clipcard.fact_exerp_transaction_log_key
   LEFT JOIN [reporting].[v_exerp_clipcard_first_participation_record] ClipcardParticipation
		  ON Clipcard.dim_exerp_clipcard_key = ClipcardParticipation.dim_exerp_clipcard_key
   LEFT JOIN [marketing].[v_dim_employee] ClipcardEmployee
          ON ClipcardParticipation.service_employee_key = ClipcardEmployee.dim_employee_key
WHERE MMS.mms_tran_id in (select transaction_id from #transactiondetail where SalesSource = 'mms')
AND MMS.primary_sales_dim_employee_key <> '-998'


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
       TranDetail.DiscountAmount1,
       TranDetail.DiscountAmount2,
       TranDetail.DiscountAmount3,
       TranDetail.DiscountAmount4,
       TranDetail.DiscountAmount5,
       TranDetail.Discount1,
       TranDetail.Discount2,
       TranDetail.Discount3,
       TranDetail.Discount4,
       TranDetail.Discount5,
       TranDetail.CurrencyCode,
       TranDetail.SaleDimDateKey,
       TranDetail.SaleDimTimeKey,
       TranDetail.MemberFirstName,
       TranDetail.MemberLastName,
       CASE WHEN TranDetail.ProductDescription like '%SNS%'
	        THEN 'Y'
			WHEN IsNull(TranDetail.PrimarySellingTeamMemberID,'-998') = '-998'
			THEN 'N'
			WHEN IsNull(Exerp_ServiceEmployeeID,'-998') = '-998'
			THEN 'N'
			WHEN Exerp_SaleEmployeeID = Exerp_ServiceEmployeeID
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
	   TranDetail.MMSTranID,
	   TranDetail.MMSTranItemID,
	   Exerp.Exerp_SaleEmployeeID AS Exerp_SalesEmployeeID,
	   Exerp.Exerp_ServiceEmployeeID AS Exerp_ServiceEmployeeID
FROM #TransactionDetail TranDetail   
 JOIN #DimReportingHierarchy 
   ON TranDetail.DivisionName = #DimReportingHierarchy.DivisionName
    AND TranDetail.SubdivisionName = #DimReportingHierarchy.SubdivisionName
    AND TranDetail.RevenueReportingDepartmentName = #DimReportingHierarchy.DepartmentName
    AND TranDetail.RevenueProductGroup = #DimReportingHierarchy.ProductGroupName
 LEFT JOIN #ExerpProductSaleAndSvcEmployees  Exerp
    ON TranDetail.MMSTranID = Exerp.mms_tran_id
	AND TranDetail.MMSTranItemID = Exerp.tran_item_id
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
ORDER BY TranDetail.Region, TranDetail.MMSClubName, TranDetail.SaleDimDateKey, TranDetail.SaleDimTimeKey, TranDetail.MemberFirstname, TranDetail.MemberLastName, TranDetail.MemberID,TranDetail.RevenueYearMonth



DROP TABLE #TransactionDetail
DROP TABLE #sales_sourceList
DROP TABLE #DimLocationInfo
DROP TABLE #DimReportingHierarchy  
DROP TABLE #AllocatedTransactionDetail
DROP TABLE #ExerpProductSaleAndSvcEmployees





END
