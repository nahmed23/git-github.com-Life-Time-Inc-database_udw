CREATE PROC [reporting].[proc_PTTrainer_DSSR_LeadGeneratorEmployeeSummary] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

----- This script is executed by an Informatica Job to populate a sandbox table 


DECLARE @ReportDate DATETIME = '1/1/1900'
SET @ReportDate = CASE WHEN @ReportDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END



DECLARE @ReportDateDimDateKey INT

DECLARE @ReportRunDateTime VARCHAR(21) 

SET @ReportDateDimDateKey = (Select dim_date_key from [marketing].[v_dim_date]  where calendar_date = @ReportDate)
SET @ReportRunDateTime = Replace(Substring(convert(varchar,getdate(),100),1,6)+', '+Substring(convert(varchar,GETDATE(),100),8,10)+' '+Substring(convert(varchar,getdate(),100),18,2),'  ',' ')



 IF OBJECT_ID('tempdb.dbo.#LeadGeneratorEmployeeSummary', 'U') IS NOT NULL  
DROP TABLE #LeadGeneratorEmployeeSummary

Select LeadGenEmployeeSummary.dim_club_key AS DimClubKey,
       PTRCLArea.description AS PersonalTrainingRegionalCategoryLeadAreaName,
       DimClub.club_id AS MMSClubID,
       LeadGenEmployeeSummary.delivering_team_member_employee_id AS DeliveringTeamMemberEmployeeID,
       LeadGenEmployeeSummary.row_label AS RowLabel,
       LeadGenEmployeeSummary.row_label_sort_order AS RowLabelSortOrder,
       LeadGenEmployeeSummary.number_of_connections AS NumberOfConnections,
       LeadGenEmployeeSummary.sales_within_14_days_count AS SalesWithin14DaysCount,
       LeadGenEmployeeSummary.closing_percent AS ClosingPercent,
       LeadGenEmployeeSummary.revenue AS Revenue,
       LeadGenEmployeeSummary.avg_revenue_sale AS AvgRevenue_Sale,
       LeadGenEmployeeSummary.avg_revenue_connection AS AvgRevenue_Connection,
       @ReportRunDateTime AS ReportRunDateTime,
       LeadGenEmployeeSummary.header_member_connection_days AS HeaderMemberConnectionDays,
       LeadGenEmployeeSummary.header_date_range AS HeaderDateRange,
       LeadGenEmployeeSummary.report_date AS ReportDate 
	INTO #LeadGeneratorEmployeeSummary
FROM [dbo].[fact_ptdssr_lead_generator_employee_summary] LeadGenEmployeeSummary
 JOIN [marketing].[v_dim_club] DimClub
   ON LeadGenEmployeeSummary.dim_club_key = DimClub.dim_club_key
 JOIN [marketing].[v_dim_description] PTRCLArea
   ON DimClub.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
WHERE LeadGenEmployeeSummary.report_date_dim_date_key = @ReportDateDimDateKey

  ------ Return Employee, Club, Area and Company totals

  SELECT DimClubKey,     
       MMSClubID,
	   DeliveringTeamMemberEmployeeID,
	   RowLabel,
	   RowLabelSortOrder,
	   NumberOfConnections,
	   SalesWithin14DaysCount,
	   ClosingPercent,
	   Revenue,
	   AvgRevenue_Sale,
       AvgRevenue_Connection,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName
FROM #LeadGeneratorEmployeeSummary

UNION ALL

  SELECT DimClubKey,
       MMSClubID,
	   -95 AS DeliveringTeamMemberEmployeeID,
	   RowLabel,
	   RowLabelSortOrder,
	   SUM(NumberOfConnections) AS NumberOfConnections,
	   SUM(SalesWithin14DaysCount) AS SalesWithin14DaysCount,
	   CASE WHEN SUM(SalesWithin14DaysCount)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE (SUM(Convert(Decimal(5,2),SalesWithin14DaysCount))/SUM(Convert(Decimal(5,2),NumberOfConnections))) 
			END ClosingPercent,
	   SUM(Revenue) AS Revenue,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(SalesWithin14DaysCount)= 0
	        THEN 0
			ELSE (SUM(Revenue)/SUM(SalesWithin14DaysCount)) 
			END AvgRevenue_Sale,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE(SUM(Revenue)/SUM(NumberOfConnections)) 
		    END AvgRevenue_Connection,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName
FROM #LeadGeneratorEmployeeSummary
 GROUP BY 
       DimClubKey,
       MMSClubID,
	   RowLabel,
	   RowLabelSortOrder,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName


UNION ALL

  SELECT '0' AS DimClubKey,
       -1 AS MMSClubID,
	   -96 AS DeliveringTeamMemberEmployeeID,
	   RowLabel,
	   RowLabelSortOrder,
	   SUM(NumberOfConnections) AS NumberOfConnections,
	   SUM(SalesWithin14DaysCount) AS SalesWithin14DaysCount,
	   CASE WHEN SUM(SalesWithin14DaysCount)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE (SUM(Convert(Decimal(5,2),SalesWithin14DaysCount))/SUM(Convert(Decimal(5,2),NumberOfConnections))) 
			END ClosingPercent,
	   SUM(Revenue) AS Revenue,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(SalesWithin14DaysCount)= 0
	        THEN 0
			ELSE (SUM(Revenue)/SUM(SalesWithin14DaysCount)) 
			END AvgRevenue_Sale,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE(SUM(Revenue)/SUM(NumberOfConnections)) 
		    END AvgRevenue_Connection,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName
FROM #LeadGeneratorEmployeeSummary
 GROUP BY 
	   RowLabel,
	   RowLabelSortOrder,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName


UNION ALL

  SELECT '0' AS DimClubKey,
       -1 AS MMSClubID,
	   -97 AS DeliveringTeamMemberEmployeeID,
	   RowLabel,
	   RowLabelSortOrder,
	   SUM(NumberOfConnections) AS NumberOfConnections,
	   SUM(SalesWithin14DaysCount) AS SalesWithin14DaysCount,
	   CASE WHEN SUM(SalesWithin14DaysCount)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE (SUM(Convert(Decimal(5,2),SalesWithin14DaysCount))/SUM(Convert(Decimal(5,2),NumberOfConnections))) 
			END ClosingPercent,
	   SUM(Revenue) AS Revenue,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(SalesWithin14DaysCount)= 0
	        THEN 0
			ELSE (SUM(Revenue)/SUM(SalesWithin14DaysCount)) 
			END AvgRevenue_Sale,
	   CASE WHEN SUM(Revenue)= 0 
	        THEN 0
	        WHEN SUM(NumberOfConnections)= 0
	        THEN 0
			ELSE(SUM(Revenue)/SUM(NumberOfConnections)) 
		    END AvgRevenue_Connection,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate,
	   'Entire Company' AS PersonalTrainingRegionalCategoryLeadAreaName
FROM #LeadGeneratorEmployeeSummary
 GROUP BY 
	   RowLabel,
	   RowLabelSortOrder,
	   ReportRunDateTime,
       HeaderMemberConnectionDays,
       HeaderDateRange,
       ReportDate

DROP TABLE #LeadGeneratorEmployeeSummary





END
