CREATE PROC [reporting].[proc_TeamMemberSalesAndServiceProductivityByProductGroup] @StartDate [DATETIME],@EndDate [DATETIME],@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@RegionList [VARCHAR](4000),@MMSClubIDList [VARCHAR](4000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
/*
declare @StartDate [DATETIME],@EndDate [DATETIME],@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@RegionList [VARCHAR](4000),@MMSClubIDList [VARCHAR](4000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000)
set @startdate = '2/1/2020'
set @enddate = '2/29/2020'
set @DepartmentMinDimReportingHierarchyKeyList = 'All Departments'
set @DimReportingHierarchyKeyList = 'All Product Groups'
set @RegionList = 'All Regions'
set @MMSClubIDList = '151'
set @DivisionList = 'Personal Training'
set @SubdivisionList = 'All Subdivisions'
*/

 ----- Sample Execution
 ---   Exec [reporting].[proc_TeamMemberSalesAndServiceProductivityByProductGroup] '5/1/2019','5/10/2019','All Departments','All Product Groups','All Regions','167|137','Personal Training','All Subdivisions'
 --     Exec reporting.[proc_TeamMemberSalesAndServiceProductivityByProductGroup] '2/1/2020','2/27/2020','All Departments','All Product Groups','All Regions','-1','Personal Training','All Subdivisions'
 ----


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (SELECT Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           FROM map_utc_time_zone_conversion
                                           WHERE getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @ReportBeginDate DateTime
DECLARE @ReportStartDimDateKey INT
declare @StartMonthEndingDimDateKey varchar(8)
DECLARE @BeginMonthEndingDate DATETIME
DECLARE @ReportBeginDate_Standard Varchar(12)

DECLARE @ReportEndDate DateTime
DECLARE @ReportEndDimDateKey INT
DECLARE @EndMonthEndingDate DATETIME
DECLARE @EndMonthEndingDimDateKey INT
DECLARE @ReportEndDate_Standard Varchar(12)

SET  @ReportBeginDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                                     THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month 
                                     ELSE @StartDate END
SET  @ReportEndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                                     THEN GETDATE()-1 ---- yesterday's date
                                     ELSE @EndDate END


SELECT  @ReportStartDimDateKey = dim_date_key, 
        @BeginMonthEndingDate = month_ending_date,
        @ReportBeginDate_Standard = standard_date_name,
        @StartMonthEndingDimDateKey = month_ending_dim_date_key
   FROM [marketing].[v_dim_date]
   WHERE calendar_date = CAST(@ReportBeginDate AS Date)

SELECT  @ReportEndDimDateKey = dim_date_key, 
        @EndMonthEndingDate = month_ending_date,
        @EndMonthEndingDimDateKey = month_ending_dim_date_key,
        @ReportEndDate_Standard = standard_date_name
   FROM [marketing].[v_dim_date]
   WHERE calendar_date = CAST(@ReportEndDate AS Date)


DECLARE @HeaderDateRange VARCHAR(51)
SET @HeaderDateRange = @ReportBeginDate_Standard + '  through ' + @ReportEndDate_Standard

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

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

DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT MIN(ReportRegionType)  FROM #DimReportingHierarchy)

IF OBJECT_ID('tempdb.dbo.#tmpDimLocation', 'U') IS NOT NULL DROP TABLE #tmpDimLocation;   
create table #tmpDimLocation with (distribution = round_robin, heap) as
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id MMSClubID
  FROM [marketing].[v_dim_club] DimClub
  JOIN [marketing].[v_dim_description]  MMSRegion ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
 where ('|'+@MMSClubIDList+'|' like '%|'+cast(DimClub.club_id as varchar)+'|%' or '|'+@MMSClubIDList+'|' like '%|-1|%' or DimClub.club_id = -13)
   and (   ('|'+@RegionList+'|' like '%|'+cast(MMSRegion.description as varchar)+'|%' and @RegionType = 'MMS Region')
        or ('|'+@RegionList+'|' like '%|'+cast(PTRCLRegion.description as varchar)+'|%' and @RegionType = 'PT RCL Area')
        or ('|'+@RegionList+'|' like '%|'+cast(MemberActivitiesRegion.description as varchar)+'|%' and @Regiontype = 'Member Activities Region')
        or @regionList like '%All Regions%')
   and DimClub.club_id Not In (-1,99,100)
   AND DimClub.club_type = 'Club'
   AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @ReportStartDimDateKey)  

IF OBJECT_ID('tempdb.dbo.#product', 'U') IS NOT NULL DROP TABLE #product
create table #product with (distribution = round_robin, heap) as
select product_master.source_system,
       product_master.dim_mms_product_key,
       product_master.dim_cafe_product_key,
       product_master.dim_hybris_product_key,
       product_master.dim_magento_product_key,
       product_master.dim_healthcheckusa_product_key,
       product_master.reporting_department + ' - ' + isnull(product_master.reporting_product_group,'') department_dash_product_group,
       product_master.effective_date_time,
       product_master.expiration_date_time
  from marketing.v_dim_product_master_history product_master
  JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = product_master.dim_reporting_hierarchy_key

declare @bucks_employee_key varchar(32) = (select dim_employee_key from marketing.v_dim_employee where employee_id = -5)
declare @corp_int_dim_club_key varchar(32) = (select dim_club_key from marketing.v_dim_club where club_id = 13)

IF OBJECT_ID('tempdb.dbo.#sales', 'U') IS NOT NULL DROP TABLE #sales
create table #sales with (distribution = hash(dim_employee_key), heap) as
select case when fact_mms_transaction_item.dim_club_key = @corp_int_dim_club_key and dim_employee.dim_club_key = @corp_int_dim_club_key then fact_mms_transaction_item.transaction_reporting_dim_club_key
            when fact_mms_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key
            else fact_mms_transaction_item.dim_club_key
        end dim_club_key,
       dim_employee.dim_employee_key,
       product.department_dash_product_group,
       sum(fact_mms_transaction_item.sales_dollar_amount) sales_dollar_amount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
  from marketing.v_fact_mms_transaction_item fact_mms_transaction_item
  JOIN [marketing].v_dim_employee dim_employee
    ON fact_mms_transaction_item.primary_sales_dim_employee_key = dim_employee.dim_employee_key
  JOIN #tmpDimLocation
    ON fact_mms_transaction_item.dim_club_key = #tmpDimLocation.DimClubKey
  JOIN [marketing].[v_dim_date] dim_date
    ON fact_mms_transaction_item.post_dim_date_key = dim_date.dim_date_key
  join #product product
    on fact_mms_transaction_item.dim_mms_product_key = product.dim_mms_product_key
   and product.source_system = 'mms'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date
 WHERE fact_mms_transaction_item.post_dim_date_key >= @ReportStartDimDateKey
   AND fact_mms_transaction_item.post_dim_date_key <= @ReportEndDimDateKey
   AND fact_mms_transaction_item.voided_flag = 'N'
   AND fact_mms_transaction_item.primary_sales_dim_employee_key not in ('-999','-998','-997')
   AND (fact_mms_transaction_item.membership_charge_flag = 'Y' OR fact_mms_transaction_item.pos_flag = 'Y')
  group by case when fact_mms_transaction_item.dim_club_key = @corp_int_dim_club_key and dim_employee.dim_club_key = @corp_int_dim_club_key then fact_mms_transaction_item.transaction_reporting_dim_club_key
                when fact_mms_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key
                else fact_mms_transaction_item.dim_club_key
            end,
           dim_employee.dim_employee_key,
           product.department_dash_product_group
UNION ALL
select case when fact_mms_transaction_item_automated_refund.original_transaction_reporting_dim_club_key = @corp_int_dim_club_key and dim_employee.dim_club_key != @corp_int_dim_club_key then dim_employee.dim_club_key
            else fact_mms_transaction_item_automated_refund.original_transaction_reporting_dim_club_key
        end dim_club_key,
       dim_employee.dim_employee_key,
       product.department_dash_product_group,
       sum(fact_mms_transaction_item_automated_refund.refund_dollar_amount) AS sales_dollar_amount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
  FROM [marketing].[v_fact_mms_transaction_item_automated_refund] fact_mms_transaction_item_automated_refund
  JOIN [marketing].[v_dim_employee] dim_employee
    ON fact_mms_transaction_item_automated_refund.original_primary_sales_dim_employee_key = dim_employee.dim_employee_key
  JOIN #tmpDimLocation OriginalTranDimClub
    ON fact_mms_transaction_item_automated_refund.original_transaction_reporting_dim_club_key = OriginalTranDimClub.DimClubKey
  JOIN [marketing].[v_dim_date] dim_date
    ON fact_mms_transaction_item_automated_refund.original_post_dim_date_key = dim_date.dim_date_key --original
  join #product product
    on fact_mms_transaction_item_automated_refund.refund_dim_mms_product_key = product.dim_mms_product_key
   and product.source_system = 'mms'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date

 WHERE fact_mms_transaction_item_automated_refund.refund_post_dim_date_key >= @ReportStartDimDateKey
   AND fact_mms_transaction_item_automated_refund.refund_post_dim_date_key <= @ReportEndDimDateKey
   AND fact_mms_transaction_item_automated_refund.original_primary_sales_dim_employee_key not in ('-999','-998','-997')
   AND fact_mms_transaction_item_automated_refund.refund_void_flag = 'N'
  group by case when fact_mms_transaction_item_automated_refund.original_transaction_reporting_dim_club_key = @corp_int_dim_club_key and dim_employee.dim_club_key != @corp_int_dim_club_key then dim_employee.dim_club_key
            else fact_mms_transaction_item_automated_refund.original_transaction_reporting_dim_club_key
        end,
           dim_employee.dim_employee_key,
           product.department_dash_product_group
union all
select case when fact_hybris_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
            else fact_hybris_transaction_item.dim_club_key
        end dim_club_key,
       dim_employee.dim_employee_key,
       product.department_dash_product_group,
       sum(fact_hybris_transaction_item.transaction_amount_gross) AS sales_dollar_amount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
FROM [marketing].[v_fact_hybris_transaction_item] fact_hybris_transaction_item
JOIN [marketing].[v_dim_employee] dim_employee
  ON fact_hybris_transaction_item.sales_dim_employee_key = dim_employee.dim_employee_key 
JOIN #tmpDimLocation DimClub
    ON fact_hybris_transaction_item.dim_club_key= DimClub.DimClubKey    
JOIN [marketing].[v_dim_date] dim_date
  ON fact_hybris_transaction_item.settlement_dim_date_key = dim_date.dim_date_key
join #product product
  on fact_hybris_transaction_item.dim_hybris_product_key = product.dim_hybris_product_key
 and product.source_system = 'Hybris'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date

WHERE fact_hybris_transaction_item.settlement_dim_date_key >= @ReportStartDimDateKey
  AND fact_hybris_transaction_item.settlement_dim_date_key <= @ReportEndDimDateKey
  AND fact_hybris_transaction_item.sales_dim_employee_key not in ('-999','-998','-997')
group by case when fact_hybris_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
              else fact_hybris_transaction_item.dim_club_key
          end,
         dim_employee.dim_employee_key,
         product.department_dash_product_group
union all
select case when fact_healthcheckusa_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
            else fact_healthcheckusa_transaction_item.dim_club_key
        end dim_club_key,
       dim_employee.dim_employee_key,
       product.department_dash_product_group,
       sum(SIGN(fact_healthcheckusa_transaction_item.sales_quantity) * fact_healthcheckusa_transaction_item.sales_amount) AS SalesDollarAmount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
  FROM [marketing].[v_fact_healthcheckusa_transaction_item] AS fact_healthcheckusa_transaction_item
  JOIN [marketing].[v_dim_employee] dim_employee
    ON fact_healthcheckusa_transaction_item.sales_dim_employee_key  = dim_employee.dim_employee_key
  JOIN #tmpDimLocation DimClub
    ON fact_healthcheckusa_transaction_item.dim_club_key = DimClub.DimClubKey
  JOIN [marketing].[v_dim_date] AS dim_date
    ON fact_healthcheckusa_transaction_item.transaction_post_dim_date_key = dim_date.dim_date_key
  join #product product
    on fact_healthcheckusa_transaction_item.dim_healthcheckusa_product_key = product.dim_healthcheckusa_product_key
   and product.source_system = 'HealthcheckUSA'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date

 WHERE fact_healthcheckusa_transaction_item.transaction_post_dim_date_key >= @ReportStartDimDateKey
   AND fact_healthcheckusa_transaction_item.transaction_post_dim_date_key <= @ReportEndDimDateKey
   AND fact_healthcheckusa_transaction_item.sales_dim_employee_key not in ('-999','-998','-997')
group by case when fact_healthcheckusa_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
            else fact_healthcheckusa_transaction_item.dim_club_key
        end,
       dim_employee.dim_employee_key,
       product.department_dash_product_group
union all
SELECT fact_cafe_transaction_item.dim_club_key,
       fact_cafe_transaction_item.order_commissionable_dim_employee_key dim_employee_key,
       product.department_dash_product_group,
       sum(CASE WHEN fact_cafe_transaction_item.order_refund_flag = 'Y' THEN (fact_cafe_transaction_item.item_quantity * fact_cafe_transaction_item.item_sales_dollar_amount_excluding_tax) 
                ELSE fact_cafe_transaction_item.item_sales_dollar_amount_excluding_tax
            END) SalesDollarAmount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
  FROM [marketing].[v_fact_cafe_transaction_item] fact_cafe_transaction_item
  JOIN #tmpDimLocation DimLocation
    ON fact_cafe_transaction_item.dim_club_key = DimLocation.DimClubKey
  JOIN [marketing].[v_dim_date] dim_date
    ON fact_cafe_transaction_item.order_close_dim_date_key = dim_date.dim_date_key
  join #product product
    on fact_cafe_transaction_item.dim_cafe_product_key = product.dim_cafe_product_key
   and product.source_system = 'Cafe'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date
 where dim_date.dim_date_key >= @ReportStartDimDateKey
   AND dim_date.dim_date_key <= @ReportEndDimDateKey
   AND fact_cafe_transaction_item.order_commissionable_dim_employee_key not in ('-999','-998','-997')
   AND fact_cafe_transaction_item.item_voided_flag = 'N'
   AND fact_cafe_transaction_item.order_void_flag = 'N'
group by fact_cafe_transaction_item.dim_club_key,
       fact_cafe_transaction_item.order_commissionable_dim_employee_key,
       product.department_dash_product_group
union all
select case when fact_magento_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
            else fact_magento_transaction_item.dim_club_key
        end dim_club_key,
       dim_employee.dim_employee_key,
       product.department_dash_product_group,
       sum(fact_magento_transaction_item.transaction_item_amount - fact_magento_transaction_item.transaction_discount_amount) AS sales_dollar_amount,
       0 service_amount,
       0 free_session_count,
       0 paid_session_count,
       0 bucks_session_count
FROM [marketing].[v_fact_magento_transaction_item] fact_magento_transaction_item
JOIN [marketing].[v_dim_employee] dim_employee
  ON fact_magento_transaction_item.dim_employee_key = dim_employee.dim_employee_key
JOIN #tmpDimLocation DimClub
    ON fact_magento_transaction_item.dim_club_key= DimClub.DimClubKey
JOIN [marketing].[v_dim_date] dim_date
  ON fact_magento_transaction_item.invoice_dim_date_key = dim_date.dim_date_key
  join #product product
    on fact_magento_transaction_item.dim_magento_product_key = product.dim_magento_product_key
   and product.source_system = 'Magento'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date

WHERE fact_magento_transaction_item.invoice_dim_date_key >= @ReportStartDimDateKey
  AND fact_magento_transaction_item.invoice_dim_date_key <= @ReportEndDimDateKey
  AND fact_magento_transaction_item.dim_employee_key > '0'
group by case when fact_magento_transaction_item.dim_club_key = @corp_int_dim_club_key then dim_employee.dim_club_key 
              else fact_magento_transaction_item.dim_club_key
          end,
       dim_employee.dim_employee_key,
       product.department_dash_product_group
union all
select case when fact_mms_package_session.delivered_dim_club_key  = @corp_int_dim_club_key then delivered_dim_employee.dim_club_key 
            else fact_mms_package_session.delivered_dim_club_key 
        end dim_club_key,
       delivered_dim_employee.dim_employee_key,
       product.department_dash_product_group,
       0 sales_dollar_amount,
       SUM(fact_mms_package_session.delivered_session_price) service_amount,
       SUM(CASE WHEN fact_mms_package_session.delivered_session_price = 0 AND fact_mms_package_session.package_entered_dim_employee_key <> @bucks_employee_key THEN 1 ELSE 0 END) free_session_count,
       SUM(CASE WHEN fact_mms_package_session.delivered_session_price <> 0 THEN 1 ELSE 0 END) paid_session_count,
       SUM(CASE WHEN fact_mms_package_session.delivered_session_price = 0 AND fact_mms_package_session.package_entered_dim_employee_key = @bucks_employee_key THEN 1 ELSE 0 END) bucks_session_count
  from marketing.v_fact_mms_package_session fact_mms_package_session
  join marketing.v_dim_employee delivered_dim_employee
    on fact_mms_package_session.delivered_dim_employee_key = delivered_dim_employee.dim_employee_key
  JOIN #tmpDimLocation
    ON #tmpDimLocation.DimClubKey = case when fact_mms_package_session.delivered_dim_club_key  = @corp_int_dim_club_key then delivered_dim_employee.dim_club_key 
            else fact_mms_package_session.delivered_dim_club_key 
        end
  join marketing.v_dim_date dim_date
    on fact_mms_package_session.created_dim_date_key = dim_date.dim_date_key -----------------------------------------------------------------delivered or created?
  join #product product
    on fact_mms_package_session.fact_mms_package_dim_product_key = product.dim_mms_product_key
   and product.source_system = 'mms'
   --AND product.effective_date_time < dim_date.next_month_starting_date
   --AND product.expiration_date_time >= dim_date.next_month_starting_date
   AND product.effective_date_time <= dim_date.month_ending_date
   AND product.expiration_date_time >= dim_date.month_ending_date
 WHERE fact_mms_package_session.created_dim_date_key >= @ReportStartDimDateKey -----------------------------------------------------------------delivered or created?
   AND fact_mms_package_session.created_dim_date_key <= @ReportEndDimDateKey
   AND fact_mms_package_session.voided_flag = 'N'
group by case when fact_mms_package_session.delivered_dim_club_key  = @corp_int_dim_club_key then delivered_dim_employee.dim_club_key 
              else fact_mms_package_session.delivered_dim_club_key 
          end,
       delivered_dim_employee.dim_employee_key,
       product.department_dash_product_group;

--delete #sales where dim_club_key not in (select DimClubKey from #tmpDimLocation);

--if exists(select 1 from sys.tables where name = 'productivity_optimized') drop table sandbox.productivity_optimized
--create table sandbox.productivity_optimized with (distribution = hash(teammemberid), heap) as
with 
employee_group_total (dim_employee_key, department_dash_product_group, sales_dollar_amount) as
(
    select dim_employee_key, 
           department_dash_product_group,
           sum(sales_dollar_amount)
      from #sales
     group by dim_employee_key,
              department_dash_product_group
),
employee_total (dim_employee_key, total_sales_amount, total_service_amount, total_amount, total_free_serviced, total_paid_serviced, total_bucks_serviced, total_serviced) as
(
    select dim_employee_key, 
           sum(sales_dollar_amount) total_sales_amount,
           sum(service_amount) total_service_amount,
           sum(sales_dollar_amount) + sum(service_amount) total_amount,
           sum(free_session_count) total_free_serviced,
           sum(paid_session_count) total_paid_serviced,
           sum(bucks_session_count) total_bucks_serviced,
           sum(free_session_count + paid_session_count+ bucks_session_count) total_serviced
     from #sales
    group by dim_employee_key
)
SELECT CASE WHEN @RegionType = 'PT RCL Area' 
                 THEN PTRCLArea.description
            WHEN @RegionType = 'Member Activities Region' 
                 THEN MemberActivitiesRegion.description
            WHEN @RegionType = 'MMS Region' 
                 THEN MMSRegion.description END Region,
       dim_employee.employee_name_last_first TeamMember,
       dim_employee.employee_id TeamMemberID,
       dim_club.club_name TeamMemberHomeClub,
       employee_role.role_name TeamMemberTitle,
       cast(isnull(employee_total.total_amount,0) as decimal(12,2)) TotalProductivityAmount,
       cast(isnull(employee_total.total_service_amount,0) as decimal(12,2)) TotalServiceAmount,
       isnull(employee_total.total_free_serviced,0) TotalFreeSessionProductivityCount,
       isnull(employee_total.total_paid_serviced,0) TotalPaidSessionProductivityCount,
       isnull(employee_total.total_bucks_serviced,0) TotalMyLTBucksProductivityCount,
       cast(isnull(employee_total.total_sales_amount,0) as decimal(12,2)) TotalSalesAmount,
       employee_group_total.department_dash_product_group ReportingDepartmentNameDashProductGroupName,
       cast(isnull(employee_group_total.sales_dollar_amount,0) as decimal(12,2)) Amount,
       @ReportRunDateTime ReportRunDateTime,
       'Local Currency' ReportingCurrencyCode,
       NULL HeaderReportingDepartmentList,
       NULL HeaderRevenueProductGrouplist,
       @HeaderDateRange HeaderDateRange,
       Cast('' as Varchar(74)) HeaderEmptyResult,
       NULL HeaderDivisionList,
       NULL HeaderSubdivisionList
from employee_group_total
join employee_total on employee_group_total.dim_employee_key = employee_total.dim_employee_key  
join marketing.v_dim_employee dim_employee
  on employee_group_total.dim_employee_key = dim_employee.dim_employee_key
JOIN [marketing].[v_dim_club] dim_club
  ON dim_employee.dim_club_key = dim_club.dim_club_key
JOIN [marketing].[v_dim_description] MemberActivitiesRegion
  ON dim_club.member_activities_region_dim_description_key = MemberActivitiesRegion.dim_description_key
JOIN [marketing].[v_dim_description] PTRCLArea
  ON dim_club.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
JOIN [marketing].[v_dim_description] MMSRegion
  ON dim_club.region_dim_description_key = MMSRegion.dim_description_key
JOIN [marketing].[v_dim_employee_bridge_dim_employee_role] employee_role_bridge  
  ON dim_employee.dim_employee_key = employee_role_bridge.dim_employee_key
 AND employee_role_bridge.primary_employee_role_flag = 'Y'
LEFT JOIN [marketing].[v_dim_employee_role] employee_role
  ON employee_role_bridge.dim_employee_role_key = employee_role.dim_employee_role_key
UNION ALL
SELECT Cast(NULL as Varchar(50)) Region,
       Cast(NULL as Varchar(102)) TeamMember,
       NULL TeamMemberID,
       Cast(NULL as Varchar(50)) TeamMemberHomeClub,
       Cast(NULL as Varchar(50)) TeamMemberTitle,
       Cast(NULL as Decimal(12,2)) TotalProductivityAmount,
       Cast(NULL as Decimal(12,2)) TotalServiceAmount,
       NULL TotalFreeSessionProductivityCount,
       NULL TotalPaidSessionProductivityCount,
       NULL TotalMyLTBucksProductivityCount,
       Cast(NULL as Decimal(12,2)) TotalSalesAmount,
       Cast(NULL as Varchar(103)) ReportingDepartmentNameDashProductGroupName,
       Cast(NULL as Varchar(50)) Amount,
       @ReportRunDateTime ReportRunDateTime,
       Cast(NULL as Varchar(50)) ReportingCurrencyCode,
       NULL HeaderReportingDepartmentList,
       NULL HeaderRevenueProductGrouplist,
       @HeaderDateRange HeaderDateRange,
       'There are no records available for the selected parameters. Please re-try.' HeaderEmptyResult,
       NULL HeaderDivisionList,
       NULL HeaderSubdivisionList
 WHERE NOT EXISTS(SELECT top 1 * FROM employee_group_total)




END
