CREATE PROC [reporting].[proc_PayrollExtract_PromptLocationProductGroup] @PayrollExtractName [VARCHAR](100),@BeginningPayWeekStartDimDateKey [INT],@EndingPayWeekStartDimDateKey [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



 ------- Sample Execution
 ----- exec [reporting].[proc_PayrollExtract_PromptLocationProductGroup] 'PT Commissionable Sales and Service',20181201,20181231
 -------

DECLARE @AdjustedPayrollExtractName VARCHAR(50)
SET @AdjustedPayrollExtractName = CASE WHEN @PayrollExtractName = 'Affiliate Programs Commissionable Sales and Service' 
                                            THEN 'PT Commissionable Sales and Service'
                                       ELSE @PayrollExtractName END


  
DECLARE @StartDate DATETIME,
        @EndDate DATETIME
SELECT @StartDate = Min(DimDate.calendar_date),
       @EndDate = Max(DimDate.week_ending_date)
  FROM [marketing].[v_dim_date] DimDate
  WHERE DimDate.dim_date_key >= @BeginningPayWeekStartDimDateKey
    AND DimDate.dim_date_key <= @EndingPayWeekStartDimDateKey
		
DECLARE @AdjEndDate DATETIME
SET @AdjEndDate = DATEADD(minute,59,DATEADD(hour,23,@EndDate))


   ----- Create temp table to return group names and region types
IF OBJECT_ID('tempdb.dbo.#HistoricalSetup', 'U') IS NOT NULL
  DROP TABLE #HistoricalSetup; 

SELECT DISTINCT payroll_standard_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  INTO #HistoricalSetup
  FROM [marketing].[v_dim_mms_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_standard_group_description <> ''  
     
UNION

SELECT DISTINCT payroll_standard_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_cafe_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_standard_group_description <> ''   

UNION

SELECT DISTINCT payroll_standard_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_hybris_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_standard_group_description <> ''  

UNION

SELECT DISTINCT payroll_standard_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_healthcheckusa_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_standard_group_description <> '' 
    
UNION

SELECT DISTINCT payroll_lt_bucks_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_mms_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_lt_bucks_group_description <> ''    

UNION

SELECT DISTINCT payroll_lt_bucks_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_cafe_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_lt_bucks_group_description <> ''

UNION

SELECT DISTINCT payroll_lt_bucks_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_hybris_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_lt_bucks_group_description <> ''

UNION

SELECT DISTINCT payroll_lt_bucks_group_description AS PayrollExtractProductGroupName,
                payroll_region_type AS PayrollExtractRegionType
  FROM [marketing].[v_dim_healthcheckusa_product_history]
 WHERE expiration_date_time > @StartDate
   AND effective_date_time <= @AdjEndDate
   AND payroll_description = @AdjustedPayrollExtractName
   AND payroll_lt_bucks_group_description <> ''


DECLARE @PayrollExtractRegionType VARCHAR(50)
SET @PayrollExtractRegionType = (SELECT MAX(DISTINCT PayrollExtractRegionType) FROM #HistoricalSetup)
 
   ----- Create temp table to return cross join results before linking in desc. views
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;  
                                          
SELECT #HistoricalSetup.PayrollExtractProductGroupName,
       DimLocation.club_code AS ClubCode,
       DimLocation.club_name AS ClubName,
       DimLocation.dim_club_key AS DimClubKey,     ------ name change
	   DimLocation.pt_rcl_area_dim_description_key,
	   DimLocation.member_activities_region_dim_description_key,
	   DimLocation.region_dim_description_key,
       DimLocation.club_code +' - '+ DimLocation.club_name AS ClubCodeDashClubName
   INTO #Results
  FROM [marketing].[v_dim_club] DimLocation
 CROSS JOIN #HistoricalSetup
 WHERE DimLocation.club_type = 'Club'
   AND DimLocation.club_id NOT IN (-1,99,100)
   AND DimLocation.club_id < 900



SELECT Results.PayrollExtractProductGroupName,
       Results.ClubCode,
       Results.ClubName,
       Results.DimClubKey,     ------ name change
       CASE WHEN @PayrollExtractRegionType = 'PT RCL Area' THEN PTRCLArea.description
            WHEN @PayrollExtractRegionType = 'Member Activities Region' THEN MARegion.description
            ELSE MMSRegion.Description 
			END RegionName,
       Results.ClubCodeDashClubName
FROM #Results Results
  JOIN [marketing].[v_dim_description] PTRCLArea
    ON Results.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
  JOIN [marketing].[v_dim_description] MARegion
    ON Results.member_activities_region_dim_description_key = MARegion.dim_description_key
  JOIN [marketing].[v_dim_description] MMSRegion
    ON Results.region_dim_description_key = MMSRegion.dim_description_key
--ORDER BY ClubCodeDashClubName, PayrollExtractProductGroupName


DROP TABLE #HistoricalSetup
DROP TABLE #Results



END
