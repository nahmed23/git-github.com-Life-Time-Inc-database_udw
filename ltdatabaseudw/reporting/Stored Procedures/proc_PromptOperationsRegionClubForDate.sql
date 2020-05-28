CREATE PROC [reporting].[proc_PromptOperationsRegionClubForDate] @ReportBeginDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ Execution Sample
------ Exec reporting.proc_PromptOperationsRegionClubForDate '01/01/2019'

			     SELECT DimClub.Dim_club_key  DimClubKey,-- Name changed DimLocationKey
                        MMSRegion.Description MMSRegionName,
                        DimClub.Club_ID MMSClubID,
                        DimClub.Club_Code ClubCode,
                        DimClub.Club_Name ClubName,
                        CASE WHEN(DimClub.Club_Status = 'Presale')
                             THEN('Y')
                             ELSE('N') 
                              END AS PreSaleFlag,
                        DimClub.Club_Code+' - '+ DimClub.Club_Name ClubCodeDashClubName,
                        ClubOpenDimDate.calendar_date AS ClubOpenDate,
                        DateAdd(mm,-1,ClubOpenDimDate.calendar_date) AS OneMonthPriorToClubOpenDate,
                        ClubCloseDimDate.Calendar_Date ClubCloseDate,
                        DimClub.club_close_dim_date_key ClubCloseDimDateKey,
                        DimClub.Club_Type LocationTypeDescription
				   FROM Marketing.v_dim_club DimClub
				   JOIN Marketing.v_Dim_date ClubCloseDimDate
					 ON DimClub.club_close_dim_date_key = ClubCloseDimDate.Dim_Date_Key
			  LEFT JOIN [marketing].[v_dim_description] MMSRegion
					 ON DimClub.region_dim_description_key = MMSRegion.dim_description_key 
				   JOIN Marketing.v_Dim_date ClubOpenDimDate
					 ON DimClub.club_open_dim_date_key = ClubOpenDimDate.Dim_Date_Key
				  WHERE ClubCloseDimDate.Calendar_Date > @ReportBeginDate
					 OR DimClub.club_close_dim_date_key = '-998'
	
END

