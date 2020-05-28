CREATE PROC [reporting].[proc_PackageSessionsSummary] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](8000),@MMSClubIDList [VARCHAR](8000),@DivisionName [VARCHAR](255) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
    SET FMTONLY OFF
    END



------- Sample Execution
------- Exec [reporting].[proc_PackageSessionsSummary] '12/1/2018','12/5/2018','All Regions','151','Personal Training'
-------

SET @StartDate = CASE	WHEN @StartDate = 'Jan 1, 1900' 
					    THEN (SELECT month_starting_date
							    FROM [marketing].[v_dim_date]
						       WHERE calendar_date = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101)) 
					    ELSE @StartDate 
						 END

SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
				    ELSE @EndDate 
					 END

DECLARE @AdjEndDateTime DateTime
SET @AdjEndDateTime = DateAdd(Day,1,@EndDate)

DECLARE @StartDateDimDateKey INT
DECLARE @EndDateDimDateKey INT

SET @StartDateDimDateKey = (SELECT dim_date_key 
							  FROM [marketing].[v_dim_date] 
							 WHERE calendar_date =  @StartDate)
SET @EndDateDimDateKey = (SELECT dim_date_key 
							FROM [marketing].[v_dim_date] 
						   WHERE calendar_date = @EndDate)


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)
						   +', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)
						    +' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time



 ----- Create region temp table   
IF OBJECT_ID('tempdb.dbo.#RegionList', 'U') IS NOT NULL
  DROP TABLE #RegionList; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'region_list'

EXEC marketing.proc_parse_pipe_list @RegionList,@list_table

	SELECT RegionDescription.description AS MMSRegion,
		   RegionDescription.dim_description_key
	  INTO #RegionList
	  FROM #region_list RegionList
	  JOIN [marketing].[v_dim_description] RegionDescription
		ON RegionList.Item = RegionDescription.description
		OR RegionList.Item = 'All Regions'
	  JOIN [marketing].[v_dim_club] DimClub
		ON RegionDescription.dim_description_key = DimClub.region_dim_description_key
  GROUP BY RegionDescription.description,
		   RegionDescription.dim_description_key


  ----- Create club temp table   
IF OBJECT_ID('tempdb.dbo.#DimClubKeyList', 'U') IS NOT NULL
  DROP TABLE #DimClubKeyList; 

SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table

  SELECT DimClub.dim_club_key AS DimClubKey,     -----name change 
		 DimClub.club_name AS ClubName, 
		 DimClub.club_id AS MMSClubID
	INTO #DimClubKeyList                     -----name change
	FROM #club_list DimClubIDList
	JOIN [marketing].[v_dim_club] DimClub
	  ON DimClubIDList.Item = DimClub.club_id
	  OR DimClubIDList.Item = 'All Clubs'
	JOIN #RegionList
	  ON DimClub.region_dim_description_key = #RegionList.dim_description_key

  ----- Create Product temp table   
IF OBJECT_ID('tempdb.dbo.#DimProductKeys', 'U') IS NOT NULL
  DROP TABLE #DimProductKeys; 

SELECT DISTINCT DimProduct.dim_mms_product_key AS DimProductKey, 
       DimProduct.product_description AS PackageProductDescription, 
       DimProduct.department_description AS MMSDepartmentDescription,
       DimProduct.dim_reporting_hierarchy_key
  INTO #DimProductKeys
  FROM [marketing].[v_dim_mms_product] DimProduct
  JOIN [marketing].[v_dim_reporting_hierarchy] DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
 WHERE DimReportingHierarchy.reporting_division = @DivisionName 
       OR @DivisionName = 'All Divisions' 

   ----- Get starting balance 

IF OBJECT_ID('tempdb.dbo.#FactPackageKeys', 'U') IS NOT NULL
   DROP TABLE #FactPackageKeys; 

     SELECT DISTINCT
			DimProductKeys.DimProductKey,
			DimClub.MMSClubID,
			DimClub.ClubName,
			DimProductKeys.MMSDepartmentDescription,
			DimProductKeys.PackageProductDescription, 
			PKG.Created_Dim_Date_Key as CreatedDimDateKey, 
			PKG.Fact_mms_Package_Key as FactPackageKey,
			PKG.Transaction_Post_Dim_Date_Key as TransactionPostDimDateKey,
			PKG.Package_ID as PackageID, 
			PKG.Sessions_Left as SessionsLeft,
			PKG.Balance_Amount as BalanceAmount, 
			CreatedDimDate.Calendar_Date AS PackageCreatedDate,
			Member.Member_ID as MemberID, 
			Member.First_Name as FirstName,
			Member.Last_Name as LastName
	   INTO #FactPackageKeys
	   FROM  [marketing].[v_fact_mms_package] PKG 
	   JOIN #DimClubKeyList  DimClub
	   	 ON PKG.reporting_dim_club_key = DimClub.DimClubKey
	   JOIN #DimProductKeys DimProductKeys
		 ON PKG.dim_mms_product_key = DimProductKeys.DimProductKey
	   JOIN [marketing].[v_dim_date] CreatedDimDate
		 ON PKG.created_dim_date_key = CreatedDimDate.dim_date_key
	   JOIN [marketing].[v_dim_mms_member] Member
		 ON PKG.dim_mms_member_key = Member.dim_mms_member_key
	  WHERE PKG.Price_Per_Session > 0 
	    AND  PKG.Transaction_Void_Flag = 'N'
	    AND PKG.Created_Dim_Date_Key < @StartDateDimDateKey 
	    AND (PKG.Sessions_Left > 0 OR (PKG.Sessions_Left = 0 
	    AND PKG.Updated_Date_Time >= @StartDate))

   IF OBJECT_ID('tempdb.dbo.#DeliveredSessions', 'U') IS NOT NULL
	  DROP TABLE #DeliveredSessions; 

	SELECT PKGS.fact_mms_package_key ,
		   SUM(PKGS.Delivered_Session_Price) AS DeliveredSessionPrice, 
		   COUNT(PKGS.fact_mms_package_session_key) AS DeliveredSessionQuantity
	  INTO #DeliveredSessions
	  FROM [marketing].[v_fact_mms_package_session] PKGS
	  JOIN #FactPackageKeys Keys
	    ON PKGS.fact_mms_package_key = Keys.FactPackageKey
	 WHERE PKGS.delivered_dim_date_key >= @StartDateDimDateKey
	   AND Voided_Flag = 'N'
  GROUP BY PKGS.fact_mms_package_key

IF OBJECT_ID('tempdb.dbo.#Adjustments', 'U') IS NOT NULL
   DROP TABLE #Adjustments; 

	SELECT ADJ.fact_mms_package_key Fact_Package_Key, 
		   SUM(ADJ.Number_Of_Sessions_Adjusted) AS NumberOfSessionsAdjusted,
		   SUM(ADJ.Package_Adjustment_Amount) AS PackageAdjustmentAmount
	  INTO #Adjustments
	  FROM [marketing].[v_fact_mms_package_adjustment]  ADJ
	  JOIN #FactPackageKeys #Keys
	    ON ADJ.fact_mms_package_key = #Keys.FactPackageKey 
	  JOIN [marketing].[v_dim_date] DimDate
	    ON DimDate.Dim_Date_Key =  ADJ.Adjusted_Dim_Date_Key
	 WHERE DimDate.Calendar_Date >= @StartDate
  GROUP BY ADJ.fact_mms_package_key 

IF OBJECT_ID('tempdb.dbo.#StartDate_OutstandingPackages', 'U') IS NOT NULL
   DROP TABLE #StartDate_OutstandingPackages; 

	SELECT #Keys.FactPackageKey,
		   #Keys.ClubName,
		   #Keys.MMSClubID,
		   #Keys.MMSDepartmentDescription,
		   #Keys.PackageID,
		   #Keys.PackageProductDescription,
		   #Keys.PackageCreatedDate,
		   #Keys.MemberID, 
		   #Keys.FirstName,
		   #Keys.LastName,       
		   #Keys.SessionsLeft 
		   + IsNull(#Delivered.DeliveredSessionQuantity,0) 
		   + IsNull(ADJ.NumberOfSessionsAdjusted,0) AS RemainingPackageSessions,
		   #Keys.BalanceAmount + IsNull(#Delivered.DeliveredSessionPrice,0) 
		   + IsNull(ADJ.PackageAdjustmentAmount,0) AS RemainingPackageBalance,
		   0 AS SoldSessionQuantity,
		   0 AS SoldPackagePrice
	  INTO #StartDate_OutstandingPackages
	  FROM  #FactPackageKeys #Keys
 LEFT JOIN #DeliveredSessions #Delivered
        ON  #Keys.FactPackageKey = #Delivered.fact_mms_package_key
 LEFT JOIN #Adjustments ADJ
        ON  #Keys.FactPackageKey = ADJ.Fact_Package_Key

----- get sold packages

IF OBJECT_ID('tempdb.dbo.#SoldPackages', 'U') IS NOT NULL
   DROP TABLE #SoldPackages; 

	SELECT FactPackage.fact_mms_package_key AS FactPackageKey,
		   DimClub.ClubName,
		   DimClub.MMSClubID,
		   DimProduct.MMSDepartmentDescription,
		   FactPackage.package_id AS PackageID,
		   PackageCreatedDimDate.calendar_date AS PackageCreatedDate,
		   DimProduct.PackageProductDescription,
		   PackageDimMember.member_id AS MemberID,
		   PackageDimMember.first_name AS FirstName,
		   PackageDimMember.last_name AS LastName,
		   0 AS RemainingPackageSessions,
		   0 AS RemainingPackageBalance,
		   FactPackage.number_of_sessions AS SoldSessionQuantity,
		   FactPackage.price_per_session * FactPackage.number_of_sessions AS SoldPackagePrice 
	  INTO #SoldPackages
	  FROM [marketing].[v_fact_mms_package] FactPackage
	  JOIN #DimProductKeys  DimProduct
	    ON FactPackage.dim_mms_product_key = DimProduct.DimProductKey
	  JOIN #DimClubKeyList  DimClub
	    ON DimClub.DimClubKey = FactPackage.reporting_dim_club_key
	  JOIN [marketing].[v_dim_date] PackageCreatedDimDate
	    ON FactPackage.created_dim_date_key = PackageCreatedDimDate.dim_date_key
	  JOIN [marketing].[v_dim_mms_member] PackageDimMember
	    ON FactPackage.dim_mms_member_key = PackageDimMember.dim_mms_member_key
	 WHERE FactPackage.Created_Dim_Date_Key >= @StartDateDimDateKey
	   AND FactPackage.Created_Dim_Date_Key <= @EndDateDimDateKey
	   AND FactPackage.Transaction_Void_Flag = 'N'
	   AND FactPackage.Price_Per_Session <> 0

-------- Delivered sessions

IF OBJECT_ID('tempdb.dbo.#DeliveredPackageSessions', 'U') IS NOT NULL
   DROP TABLE #DeliveredPackageSessions;

	SELECT PKGS.fact_mms_package_key AS FactPackageKey, 
		   SUM(PKGS.Delivered_Session_Price) AS DeliveredSessionPrice, 
		   COUNT(PKGS.fact_mms_package_session_key) AS DeliveredSessionQuantity
	  INTO #DeliveredPackageSessions
	  FROM [marketing].[v_fact_mms_package_session] PKGS
	  JOIN [marketing].[v_fact_mms_package] FactPackage
	    ON PKGS.fact_mms_package_key = FactPackage.fact_mms_package_key 
	  JOIN #DimProductKeys DimProduct
	    ON  FactPackage.dim_mms_product_key = DimProduct.DimProductKey
	  JOIN  #DimClubKeyList  DimClub
	    ON DimClub.DimClubKey = FactPackage.reporting_dim_club_key
	 WHERE PKGS.Delivered_Dim_Date_Key >= @StartDateDimDateKey
	   AND PKGS.Delivered_Dim_Date_Key <= @EndDateDimDateKey
	   AND PKGS.Voided_Flag = 'N'
  GROUP BY PKGS.fact_mms_package_key

------ Adjusted packages

IF OBJECT_ID('tempdb.dbo.#AdjustedPackageSessions', 'U') IS NOT NULL
   DROP TABLE #AdjustedPackageSessions; 

	SELECT ADJ.fact_mms_package_key as FactPackageKey, 
		   SUM(ADJ.Number_Of_Sessions_Adjusted) AS NumberOfSessionsAdjusted,
	       SUM(ADJ.Package_Adjustment_Amount) AS PackageAdjustmentAmount,
	       FactPackage.package_id
	  INTO #AdjustedPackageSessions
	  FROM [marketing].[v_fact_mms_package_adjustment] ADJ
	  JOIN [marketing].[v_fact_mms_package] FactPackage
	    ON ADJ.fact_mms_package_key = FactPackage.fact_mms_package_key 
	 WHERE ADJ.Adjusted_Dim_Date_Key >= @StartDateDimDateKey 
	   AND ADJ.Adjusted_Dim_Date_Key <=@EndDateDimDateKey
  GROUP BY ADJ.fact_mms_package_key,FactPackage.package_id

--------- Ending Balance 

IF OBJECT_ID('tempdb.dbo.#FactPackageKeys_Ending', 'U') IS NOT NULL
   DROP TABLE #FactPackageKeys_Ending; 

		    
	SELECT	DimClub.MMSClubID,
			DimClub.ClubName,
			DimProductKeys.MMSDepartmentDescription,
			DimProductKeys.PackageProductDescription, 
			PKG.Created_Dim_Date_Key as CreatedDimDateKey, 
			PKG.Fact_mms_Package_Key as FactPackageKey,
			PKG.Transaction_Post_Dim_Date_Key as TransactionPostDimDateKey,
			PKG.Package_ID as PackageID, 
			PKG.Sessions_Left as SessionsLeft,
			PKG.Balance_Amount as BalanceAmount, 
			CreatedDimDate.Calendar_Date AS PackageCreatedDate,
			Member.Member_ID as MemberID, 
			Member.First_Name as FirstName,
			Member.Last_Name as LastName
	   INTO #FactPackageKeys_Ending
	   FROM [marketing].[v_fact_mms_package] PKG 
	   JOIN #DimClubKeyList  DimClub
		 ON PKG.reporting_dim_club_key = DimClub.DimClubKey
	   JOIN #DimProductKeys DimProductKeys
		 ON PKG.dim_mms_product_key = DimProductKeys.DimProductKey
	   JOIN [marketing].[v_dim_date] CreatedDimDate
		 ON PKG.created_dim_date_key = CreatedDimDate.dim_date_key
	   JOIN [marketing].[v_dim_mms_member] Member
		 ON PKG.dim_mms_member_key = Member.dim_mms_member_key
	  WHERE PKG.Price_Per_Session > 0 
	    AND PKG.Transaction_Void_Flag = 'N'
		AND PKG.Created_Dim_Date_Key <= @EndDateDimDateKey    
		AND (PKG.Sessions_Left > 0 OR (PKG.Sessions_Left = 0 
		AND PKG.Updated_Date_Time > @EndDate))

 IF OBJECT_ID('tempdb.dbo.#DeliveredSessions_Ending', 'U') IS NOT NULL
      DROP TABLE #DeliveredSessions_Ending; 

	SELECT PKGS.fact_mms_package_key AS FactPackageKey, 
		   SUM(PKGS.Delivered_Session_Price) AS DeliveredSessionPrice, 
	       COUNT(PKGS.fact_mms_package_session_key) AS DeliveredSessionQuantity
	  INTO #DeliveredSessions_Ending
	  FROM [marketing].[v_fact_mms_package_session] PKGS
	  JOIN #FactPackageKeys_Ending #Keys
	    ON PKGS.fact_mms_package_key = #Keys.FactPackageKey
	 WHERE PKGS.Delivered_Dim_Date_Key > @EndDateDimDateKey
	   AND PKGS.Voided_Flag = 'N'
  GROUP BY PKGS.fact_mms_package_key

IF OBJECT_ID('tempdb.dbo.#Adjustments_Ending', 'U') IS NOT NULL
   DROP TABLE #Adjustments_Ending

	SELECT ADJ.fact_mms_package_key as FactPackageKey, 
	       SUM(ADJ.Number_Of_Sessions_Adjusted) AS NumberOfSessionsAdjusted,
	       SUM(ADJ.Package_Adjustment_Amount) AS PackageAdjustmentAmount
	  INTO #Adjustments_Ending
	  FROM [marketing].[v_fact_mms_package_adjustment] ADJ
	  JOIN #FactPackageKeys_Ending #Keys
	    ON ADJ.fact_mms_package_key = #Keys.FactPackageKey
	  JOIN [marketing].[v_dim_date]  DimDate
	    ON DimDate.Dim_Date_Key =  ADJ.Adjusted_Dim_Date_Key
	 WHERE DimDate.Calendar_Date >= DateAdd(day,1,@EndDate)
  GROUP BY ADJ.fact_mms_package_key

IF OBJECT_ID('tempdb.dbo.#EndDate_OutstandingPackages', 'U') IS NOT NULL
   DROP TABLE #EndDate_OutstandingPackages; 
  
		SELECT #Keys.FactPackageKey,
			   #Keys.ClubName,
			   #Keys.MMSClubID,
			   #Keys.MMSDepartmentDescription,
			   #Keys.PackageID,
			   #Keys.PackageProductDescription,
			   #Keys.PackageCreatedDate,
			   #Keys.MemberID, 
			   #Keys.FirstName,
			   #Keys.LastName,       
			   #Keys.SessionsLeft 
			   + IsNull(#Delivered.DeliveredSessionQuantity,0) 
			   + IsNull(ADJ.NumberOfSessionsAdjusted,0) AS RemainingPackageSessions_Ending,
			   #Keys.BalanceAmount 
			   + IsNull(#Delivered.DeliveredSessionPrice,0) 
			   + IsNull(ADJ.PackageAdjustmentAmount,0) AS RemainingPackageBalance_Ending,
			   0 AS SoldSessionQuantity,
			   0 AS SoldPackagePrice
		  INTO #EndDate_OutstandingPackages
		  FROM #FactPackageKeys_Ending #Keys
	 LEFT JOIN #DeliveredSessions_Ending #Delivered
		    ON  #Keys.FactPackageKey = #Delivered.FactPackageKey
	 LEFT JOIN #Adjustments_Ending ADJ
		    ON  #Keys.FactPackageKey = ADJ.FactPackageKey

---------------------------------------------

--------------------

IF OBJECT_ID('tempdb.dbo.#PeriodPackages', 'U') IS NOT NULL
   DROP TABLE #PeriodPackages; 

	SELECT FactPackageKey,
		   ClubName,
		   MMSClubID,
		   MMSDepartmentDescription,
		   PackageID,
		   PackageCreatedDate,
		   PackageProductDescription,
		   MemberID,
		   FirstName,
		   LastName,
		   RemainingPackageSessions,
		   RemainingPackageBalance,
		   SoldSessionQuantity,
		   SoldPackagePrice,
		   0 AS RemainingPackageSessions_Ending,
		   0 AS RemainingPackageBalance_Ending
	  INTO #PeriodPackages
	  FROM #StartDate_OutstandingPackages
	
	UNION 

	SELECT FactPackageKey,
			ClubName,
			MMSClubID,
			MMSDepartmentDescription,
			PackageID,
			PackageCreatedDate,
			PackageProductDescription,
			MemberID,
			FirstName,
			LastName,
			RemainingPackageSessions,
			RemainingPackageBalance,
			SoldSessionQuantity,
			SoldPackagePrice,
			0 AS RemainingPackageSessions_Ending,
			0 AS RemainingPackageBalance_Ending
		FROM #SoldPackages
		
	SELECT PeriodPackages.FactPackageKey,
		   PeriodPackages.ClubName,
		   PeriodPackages.MMSClubID,
		   PeriodPackages.MMSDepartmentDescription,
		   PeriodPackages.PackageID,
		   PeriodPackages.PackageCreatedDate,
		   PeriodPackages.PackageProductDescription,
		   PeriodPackages.MemberID,
		   PeriodPackages.FirstName,
		   PeriodPackages.LastName,
		   PeriodPackages.RemainingPackageSessions,
		   PeriodPackages.RemainingPackageBalance,
		   PeriodPackages.SoldSessionQuantity,
		   PeriodPackages.SoldPackagePrice,
		   DeliveredPackageSessions.DeliveredSessionQuantity,
		   DeliveredPackageSessions.DeliveredSessionPrice,
		   AdjustedPackageSessions.NumberOfSessionsAdjusted,
		   AdjustedPackageSessions.PackageAdjustmentAmount,
		   RemainingBalances_End.RemainingPackageSessions_Ending,
		   RemainingBalances_End.RemainingPackageBalance_Ending,
		   @StartDate StartDate,
		   @EndDate EndDate 
	  FROM #PeriodPackages PeriodPackages 
 LEFT JOIN #DeliveredPackageSessions DeliveredPackageSessions
		ON PeriodPackages.FactPackageKey = DeliveredPackageSessions.FactPackageKey
 LEFT JOIN #AdjustedPackageSessions AdjustedPackageSessions
	    ON PeriodPackages.FactPackageKey = AdjustedPackageSessions.FactPackageKey
 LEFT JOIN #EndDate_OutstandingPackages RemainingBalances_End
		ON PeriodPackages.FactPackageKey = RemainingBalances_End.FactPackageKey

DROP TABLE #RegionList
DROP TABLE #DimClubKeyList   
DROP TABLE #DimProductKeys
DROP TABLE #FactPackageKeys
DROP TABLE #DeliveredSessions
DROP TABLE #Adjustments
DROP TABLE #StartDate_OutstandingPackages
DROP TABLE #SoldPackages
DROP TABLE #DeliveredPackageSessions
DROP TABLE #AdjustedPackageSessions
DROP TABLE #FactPackageKeys_Ending
DROP TABLE #DeliveredSessions_Ending
DROP TABLE #Adjustments_Ending
DROP TABLE #EndDate_OutstandingPackages
DROP TABLE #PeriodPackages


END
