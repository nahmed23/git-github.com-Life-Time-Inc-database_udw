CREATE PROC [reporting].[proc_PromptTransactionClubByRegion] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

DECLARE @ClubCount INT
SELECT @ClubCount = count(*)
FROM marketing.v_Dim_Club
WHERE Club_ID NOT IN (-1,100,996,997)

SELECT CASE WHEN region.description IS NULL THEN 'None Designated'
		ELSE region.description
		END MMSRegionName,
		DimLocation.Club_ID MMSClubID,
		Dimlocation.club_name ClubName,
		DimLocation.club_code ClubCode,
		DimLocation.gl_club_id GLClubID,
		CASE WHEN DimLocation.club_status = 'Presale' Then 'Y'
		ELSE 'N'
		END PresaleFlag,
		DimLocation.club_code + ' - ' + DimLocation.club_name ClubCodeDashClubName,
		DimLocation.dim_club_key DimLocationKey,
		@ClubCount TotalClubCount,
		CASE WHEN DimLocation.club_close_dim_date_key = -998 THEN NULL
		ELSE DimDate.calendar_date
		END ClubCloseDate,
		CASE WHEN DimLocation.club_close_dim_date_key = -998 THEN '2099-12'
		ELSE DimDate.four_digit_year_dash_two_digit_month
		END ClubCloseFourDigitYearDashTwoDigitMonth


FROM marketing.v_dim_club DimLocation
  JOIN marketing.v_dim_description region
    ON DimLocation.region_dim_description_key = region.dim_description_key
JOIN marketing.v_Dim_Date DimDate
  ON DimDate.Dim_Date_Key = DimLocation.Club_Close_Dim_Date_Key 
WHERE DimLocation.Club_ID NOT IN (-1,100,996,997)
ORDER BY DimLocation.club_ID

END