CREATE PROC [reporting].[proc_PromptUsageRegionForMonth] @UsageYearMonth [CHAR](7) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
----- Exec reporting.proc_PromptUsageRegionForMonth '2018-07'

DECLARE @FirstOfMonth DATETIME
DECLARE @LastOfMonth DATETIME

SELECT @FirstOfMonth = MIN(Dim_Date.[calendar_date])
	 , @LastOfMonth = MAX(Dim_Date.[calendar_date])
FROM [marketing].[v_dim_date] Dim_Date
WHERE CASE WHEN LEN(Dim_Date.[four_digit_year_dash_two_digit_month]) = 6 
		   THEN LEFT(Dim_Date.[four_digit_year_dash_two_digit_month],5)+'0'+RIGHT(RTRIM(Dim_Date.[four_digit_year_dash_two_digit_month]),1) 
		   ELSE Dim_Date.[four_digit_year_dash_two_digit_month] END = @UsageYearMonth

SELECT DISTINCT DimDescription.description MMSRegionName
FROM [marketing].[v_dim_description] DimDescription
  JOIN [marketing].[v_dim_location] DimLocation
	   ON DimLocation.[region_dim_description_key] = DimDescription.[dim_description_key]
	      AND DimDescription.[source_object] = 'r_mms_val_region'
  JOIN [marketing].[v_dim_date] ClubPreSaleDimDate
       ON CASE WHEN DimLocation.[club_open_dim_date_key] > 0
			THEN CONVERT(DATETIME,CONVERT(varchar(8),DimLocation.[club_open_dim_date_key],101))
			ELSE DimLocation.[club_open_dim_date_key] END = ClubPreSaleDimDate.[calendar_date]
  JOIN [marketing].[v_dim_date] ClubCloseDimDate
       ON DimLocation.[club_close_dim_date_key] = ClubCloseDimDate.[dim_date_key]
WHERE DimLocation.[club_type] = 'Club'
   AND DimLocation.[club_open_dim_date_key] IS NOT NULL
   AND DimLocation.[club_open_dim_date_key] <> -998
   AND ClubPreSaleDimDate.[calendar_date] <= @LastOfMonth
   AND (ClubCloseDimDate.[calendar_date] > @FirstOfMonth OR DimLocation.[club_close_dim_date_key] = -998)
ORDER BY 1

END


