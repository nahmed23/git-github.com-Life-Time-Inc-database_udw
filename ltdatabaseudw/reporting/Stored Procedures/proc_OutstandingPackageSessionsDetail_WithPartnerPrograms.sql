CREATE PROC [reporting].[proc_OutstandingPackageSessionsDetail_WithPartnerPrograms] @ClubIDList [VARCHAR](1000),@MMSDeptDescriptionList [VARCHAR](2000),@PartnerProgramList [VARCHAR](8000),@myLTBucksFilter [VARCHAR](100) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
--- Exec [reporting].[proc_OutstandingPackageSessionsDetail_WithPartnerPrograms] '151','Personal Training','BCBS of Minnesota|Health Partners|UCare','Not Limited by myLT Buck$'
-----

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),18,2),'  ',' ')  ---- UDW is in UTC time
 
DECLARE @ReportDateDimDateKey INT 
SET @ReportDateDimDateKey = (Select dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DateAdd(day,-1,Convert(date,@ReportRunDateTime))) ---- Yesterday's date to align with DW date


IF OBJECT_ID('tempdb.dbo.#Locations', 'U') IS NOT NULL
  DROP TABLE #Locations; 

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @ClubIDList,@list_table
	
SELECT DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimDescription.description AS MMSRegion
  INTO #Locations
  FROM #club_list MMSClubIDList
  JOIN [marketing].[v_dim_club] DimClub
    ON MMSClubIDList.Item = DimClub.club_id
  JOIN [marketing].[v_dim_description] DimDescription
   ON DimDescription.dim_description_key = DimClub.region_dim_description_key 
 WHERE DimClub.club_type = 'Club' 

 

   ----- Create MMS Department name temp table to return selected dept names
IF OBJECT_ID('tempdb.dbo.#MMSDepartmentNames', 'U') IS NOT NULL
  DROP TABLE #MMSDepartmentNames; 

SET @list_table = 'department_list'
  EXEC marketing.proc_parse_pipe_list @MMSDeptDescriptionList,@list_table

SELECT MMSDepartment.description AS MMSDepartmentDescription,
       MMSDepartment.dim_mms_department_key     
  INTO #MMSDepartmentNames
  FROM [marketing].[v_dim_mms_department] MMSDepartment
   JOIN #department_list
    ON MMSDepartment.description = #department_list.Item
	  OR #department_list.Item = 'All'
  WHERE MMSDepartment.description IS NOT NULL


     ----- Create table for all products related to selected departments
IF OBJECT_ID('tempdb.dbo.#MMSDepartmentNameAndProduct', 'U') IS NOT NULL
  DROP TABLE #MMSDepartmentNameAndProduct; 


SELECT MMSDepartmentNames.MMSDepartmentDescription,                
       PackageProduct.dim_mms_product_key AS DimProductKey,
	   PackageProduct.product_id AS MMSProductID,
	   PackageProduct.product_description AS ProductDescription   
  INTO #MMSDepartmentNameAndProduct
  FROM [marketing].[v_dim_mms_product]PackageProduct
 JOIN #MMSDepartmentNames MMSDepartmentNames                                                ----- Comment Out for DEV
   ON PackageProduct.department_description = MMSDepartmentNames.MMSDepartmentDescription   ----- Comment Out for DEV
 --JOIN #MMSDepartmentNames MMSDepartmentNames                                                -----Comment Out for QA/Prod - Defect created UDW-7502
 --  ON PackageProduct.mms_department_bk_hash = MMSDepartmentNames.dim_mms_department_key     -----Comment Out for QA/Prod - Defect created UDW-7502
  WHERE MMSDepartmentNames.MMSDepartmentDescription IS NOT NULL




DECLARE @HeaderDepartmentList AS VARCHAR(2000)
SET  @HeaderDepartmentList = (CASE WHEN @MMSDeptDescriptionList = 'All' 
                                   THEN 'All MMS Departments' 
								   ELSE REPLACE(@MMSDeptDescriptionList,'|',',') 
								   END)





   ----- Create reimbursement program temp table
IF OBJECT_ID('tempdb.dbo.#DimProgramList', 'U') IS NOT NULL
  DROP TABLE #DimProgramList; 

SET @list_table = 'program_list'

  EXEC marketing.proc_parse_pipe_list @PartnerProgramList,@list_table

SELECT DISTINCT DimProgram.reimbursement_program_id AS ReimbursementProgramID,
                DimProgram.dim_mms_reimbursement_program_key AS DimReimbursementProgramKey,
                DimProgram.program_name AS ProgramName
  INTO #DimProgramList
  FROM #program_list ProgramNameList
  JOIN [marketing].[v_dim_mms_reimbursement_program] DimProgram
    ON DimProgram.program_name = ProgramNameList.Item
	OR ProgramNameList.Item Like '%< All Partner Program Members >%'



DECLARE @HeaderPartnerProgramList AS VARCHAR(2000)
SET @HeaderPartnerProgramList = (CASE WHEN @PartnerProgramList = 'All' 
                                        THEN 'Not Limited By Partner Program' 
                                     WHEN @PartnerProgramList like '%< All Partner Program Members >%' 
									    THEN 'All Partner Programs'
                                     ELSE REPLACE(@PartnerProgramList,'|',',')  
									 END)


 ----- Create reimbursement program temp table
IF OBJECT_ID('tempdb.dbo.#ReportPackages', 'U') IS NOT NULL
  DROP TABLE #ReportPackages; 

 ---- Performance was much improved by first creating a temp table of the desired Package records
SELECT FactPackage.dim_mms_member_key AS DimMemberKey,
       FactPackage.transaction_void_flag AS TransactionVoidFlag,
       FactPackage.price_per_session AS PricePerSession,
	   FactPackage.reporting_dim_club_key AS ReportingDimClubKey,
	   FactPackage.dim_mms_product_key AS DimProductKey,
	   FactPackage.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
	   FactPackage.package_status_dim_description_key AS PackageStatusDimDescriptionKey,
	   DimDescription.description AS PackageStatusDescription,
	   FactPackage.fact_mms_package_key AS FactPackageKey,
	   FactPackage.created_dim_date_key AS CreatedDimDateKey,
	   FactPackage.created_dim_time_key AS CreatedDimTimeKey,
	   FactPackage.package_id AS PackageID,
	   FactPackage.number_of_sessions AS NumberOfSessions,
	   FactPackage.balance_amount AS BalanceAmount,
	   FactPackage.sessions_left AS SessionsLeft
	INTO #ReportPackages
FROM [marketing].[v_fact_mms_package] FactPackage
	JOIN #Locations DimLocation
	  ON FactPackage.reporting_dim_club_key = DimLocation.dim_club_key
    JOIN #MMSDepartmentNameAndProduct PackageProduct
      ON FactPackage.dim_mms_product_key = PackageProduct.DimProductKey
	JOIN [marketing].[v_dim_description] DimDescription 
      ON FactPackage.package_status_dim_description_key = DimDescription.dim_description_key
	LEFT JOIN [marketing].[v_dim_employee] PackageEnteredEmployee
      ON FactPackage.package_entered_dim_employee_key = PackageEnteredEmployee.dim_employee_key
      AND PackageEnteredEmployee.employee_id = -5
WHERE DimDescription.Description Not In('Completed','Voided')
      AND ((PackageEnteredEmployee.employee_id Is Not Null and @myLTBucksFilter = 'myLT Buck$ Only') ---- Employee ID -5 "Loyalty Program"
            OR
           (PackageEnteredEmployee.employee_id Is Null and @myLTBucksFilter ='Exclude myLT Buck$')
            OR
           (@myLTBucksFilter = 'Not Limited by myLT Buck$')) 




----- setting up table to hold a listing of up to 3 Partner Programs per member

     ----- Drop and Create DimMemberKeys temp table
IF OBJECT_ID('tempdb.dbo.#DimMemberKeys', 'U') IS NOT NULL
  DROP TABLE #DimMemberKeys; 
	 
  ----- Collect distinct list of customer keys to limit looping
SELECT Distinct DimMemberKey
 INTO #DimMemberKeys
  FROM #ReportPackages

     ----- Drop and Create DimMemberKeys temp table
IF OBJECT_ID('tempdb.dbo.#NumberedDimMemberKeys', 'U') IS NOT NULL
  DROP TABLE #NumberedDimMemberKeys; 

 ----- Assign row number to each member key
 SELECT DimMemberKey,
 ROW_NUMBER() OVER(ORDER BY DimMemberKey ASC ) MemberKeySort
INTO #NumberedDimMemberKeys
 FROM #DimMemberKeys



      ----- Drop and Create NumberedDimMemberKeyPrograms temp table
IF OBJECT_ID('tempdb.dbo.#NumberedDimMemberKeyPrograms', 'U') IS NOT NULL
  DROP TABLE #NumberedDimMemberKeyPrograms; 

---- Setting up 
SELECT DISTINCT DimMemberKeys.MemberKeySort,MR.dim_mms_member_key, PP.ProgramName
 INTO #NumberedDimMemberKeyPrograms
FROM [marketing].[v_fact_mms_member_reimbursement_program] MR
JOIN #DimProgramList PP
  ON MR.dim_mms_reimbursement_program_key = PP.DimReimbursementProgramKey
JOIN #NumberedDimMemberKeys DimMemberKeys
  ON MR.dim_mms_member_key = DimMemberKeys.DimMemberKey
WHERE MR.enrollment_dim_date_key < @ReportDateDimDateKey
  AND (MR.termination_dim_date_key >= @ReportDateDimDateKey OR MR.termination_dim_date_key is null)


      ----- Drop and Create NumberedDimMemberKeyAndNumberedPrograms temp table
IF OBJECT_ID('tempdb.dbo.#NumberedDimMemberKeyAndNumberedPrograms', 'U') IS NOT NULL
  DROP TABLE #NumberedDimMemberKeyAndNumberedPrograms; 

Select MemberKeySort,dim_mms_member_key,ProgramName,
ROW_NUMBER() OVER(PARTITION BY MemberKeySort ORDER BY ProgramName ASC ) ProgramNameSort
INTO #NumberedDimMemberKeyAndNumberedPrograms
FROM #NumberedDimMemberKeyPrograms

      ----- Drop and Create MemberPartnerProgramPrelim temp table
IF OBJECT_ID('tempdb.dbo.#MemberPartnerProgramPrelim', 'U') IS NOT NULL
  DROP TABLE #MemberPartnerProgramPrelim;

SELECT MemberKeySort,dim_mms_member_key,ProgramNameSort,
       ProgramName AS Program1,
	   '' AS Program2,
	   '' AS Program3
INTO #MemberPartnerProgramPrelim
FROM #NumberedDimMemberKeyAndNumberedPrograms
WHERE ProgramNameSort = 1

UNION
SELECT MemberKeySort,dim_mms_member_key,ProgramNameSort,
       '' AS Program1,
	   ProgramName AS Program2,
	   '' AS Program3
FROM #NumberedDimMemberKeyAndNumberedPrograms
WHERE ProgramNameSort = 2

UNION

SELECT MemberKeySort,dim_mms_member_key,ProgramNameSort,
       '' AS Program1,
	   '' AS Program2,
	   ProgramName AS Program3
FROM #NumberedDimMemberKeyAndNumberedPrograms
WHERE ProgramNameSort = 3

      ----- Drop and Create #MemberPartnerProgram temp table
IF OBJECT_ID('tempdb.dbo.#MemberPartnerProgram', 'U') IS NOT NULL
  DROP TABLE #MemberPartnerProgram;

Select MemberKeySort,
       dim_mms_member_key,
	   MAX(ProgramNameSort) AS MaxProgramCount,
	   MAX(Program1) AS Program1,
	   MAX(Program2) AS Program2,
	   MAX(Program3) AS Program3,
	   CASE WHEN MAX(ProgramNameSort) = 1
	        THEN MAX(Program1)
			WHEN MAX(ProgramNameSort) = 2
			THEN MAX(Program1)+', '+MAX(Program2)
			WHEN MAX(ProgramNameSort) = 3
			THEN MAX(Program1)+', '+MAX(Program2)+', '+MAX(Program3)
			END PartnerProgramList
 INTO #MemberPartnerProgram
FROM #MemberPartnerProgramPrelim 
GROUP BY MemberKeySort,
       dim_mms_member_key



SELECT DimLocation.MMSRegion AS SalesRegionDescription,
       DimLocation.club_name AS SalesClubname, 
       DimEmployee.employee_id AS EmployeeID, 
       DimEmployee.first_name AS TeamMemberFirstname, 
       DimEmployee.last_name AS TeamMemberLastname,   
       Convert(Datetime,Replace(Substring(convert(varchar,PackageCreatedDate.calendar_date,100),1,6)+', '+Substring(convert(varchar,PackageCreatedDate.calendar_date,100),8,4)+' '+Convert(varchar,PackageCreatedTime.display_12_hour_time),'  ',' ')) AS PackageCreatedDateTime,
	   DimMember.member_id AS MemberID, 
       DimMember.first_name AS MemberFirstname, 
       DimMember.last_name AS MemberLastname, 
       DimProduct.ProductDescription,
       ReportPackages.Packageid, 
       DimProduct.MMSProductID AS Productid,
       ReportPackages.PackageStatusDescription AS PackageStatusDescription,
       MemberHomeClubDimLocation.club_name AS MembershipHomeClub,
       EmployeeHomeClubDimLocation.club_name AS TeamMemberHomeClub, 
       cast(ReportPackages.NumberOfSessions AS decimal(4,1)) AS OriginalNumberOfSessions, 
       cast(ReportPackages.SessionsLeft AS decimal(4,1)) AS SessionsLeft, 
       DimProduct.MMSDepartmentDescription AS MMSDepartment,
       PartnerProgramMembers.PartnerProgramList,
       'Local Currency' AS ReportingCurrencyCode,
	   ReportPackages.BalanceAmount  AS BalanceAmount,
	   @ReportRunDateTime AS ReportRunDateTime,
	   @HeaderDepartmentList AS HeaderDepartmentList,
	   @HeaderPartnerProgramList AS HeaderPartnerProgramList,
	   @myLTBucksFilter AS HeaderMyLTBucks,
	   DimLocation.club_id As SalesClubID

	   FROM #ReportPackages ReportPackages
	    JOIN #Locations DimLocation
		  ON ReportPackages.ReportingDimClubKey = DimLocation.dim_club_key
		JOIN #MMSDepartmentNameAndProduct DimProduct
		  ON ReportPackages.DimProductKey = DimProduct.DimProductKey
		JOIN [marketing].[v_dim_date] PackageCreatedDate
		  ON ReportPackages.CreatedDimDateKey = PackageCreatedDate.dim_date_key
		JOIN [marketing].[v_dim_time] PackageCreatedTime
		  ON ReportPackages.CreatedDimTimeKey = PackageCreatedTime.dim_time_key
		JOIN [marketing].[v_dim_mms_member] DimMember
		  ON ReportPackages.DimMemberKey = DimMember.dim_mms_member_key
		JOIN [marketing].[v_dim_mms_membership] DimMembership
		  ON DimMember.dim_mms_membership_key = DimMembership.dim_mms_membership_key
		JOIN [marketing].[v_dim_club] MemberHomeClubDimLocation
		  ON DimMembership.home_dim_club_key = MemberHomeClubDimLocation.dim_club_key
		JOIN [marketing].[v_dim_employee] DimEmployee
		  ON ReportPackages.PrimarySalesDimEmployeeKey = DimEmployee.dim_employee_key
	    JOIN [marketing].[v_dim_club] EmployeeHomeClubDimLocation
		  ON DimEmployee.dim_club_key = EmployeeHomeClubDimLocation.dim_club_key
		LEFT JOIN #MemberPartnerProgram PartnerProgramMembers
		  ON ReportPackages.DimMemberKey = PartnerProgramMembers.dim_mms_member_key
WHERE IsNull(PartnerProgramMembers.dim_mms_member_key,-999) = CASE WHEN @PartnerProgramList = 'All' 
                                                                THEN IsNull(PartnerProgramMembers.dim_mms_member_key,-999) 
																ELSE ReportPackages.DimMemberKey
																END
 
   DROP TABLE #Locations
   DROP TABLE #DimProgramList
   DROP TABLE #MMSDepartmentNames
   DROP TABLE #MMSDepartmentNameAndProduct
   DROP TABLE #ReportPackages
   DROP TABLE #DimMemberKeys
   DROP TABLE #NumberedDimMemberKeys
   DROP TABLE #NumberedDimMemberKeyPrograms
   DROP TABLE #NumberedDimMemberKeyAndNumberedPrograms
   DROP TABLE #MemberPartnerProgramPrelim
   DROP TABLE #MemberPartnerProgram


END
