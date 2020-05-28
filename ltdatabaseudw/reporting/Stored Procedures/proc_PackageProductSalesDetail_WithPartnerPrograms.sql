CREATE PROC [reporting].[proc_PackageProductSalesDetail_WithPartnerPrograms] @ClubIDs [VARCHAR](1000),@StartDate [DATETIME],@EndDate [DATETIME],@PartnerProgramList [VARCHAR](2000),@MMSDeptDescriptionList [VARCHAR](2000),@myLTBucksFilter [VARCHAR](100) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
--- Exec [reporting].[proc_PackageProductSalesDetail_WithPartnerPrograms] '205|151','1/1/2012','7/12/2015','< All Partner Program Members >','Personal Training|Yoga|Pro Shop','Not Limited by myLT Buck$'
-----


DECLARE @HeaderDateRange VARCHAR(33) 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @HeaderDateRange = convert(varchar(12), @StartDate, 107) + ' to ' + convert(varchar(12), @EndDate, 107)
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ----- UDW in UTC time
 

IF OBJECT_ID('tempdb.dbo.#Locations', 'U') IS NOT NULL
  DROP TABLE #Locations; 

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @ClubIDs,@list_table
	
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



   ----- Create MMS Department temp table
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

  

DECLARE @HeaderDepartmentList AS VARCHAR(2000)
SET  @HeaderDepartmentList = (CASE WHEN @MMSDeptDescriptionList = 'All' 
                                   THEN 'All MMS Departments' 
								   ELSE REPLACE(@MMSDeptDescriptionList,'|',',') 
								   END)
                                   


DECLARE @HeaderPartnerProgramList AS VARCHAR(2000)
SET @HeaderPartnerProgramList = (CASE WHEN @PartnerProgramList = 'All' 
                                        THEN 'Not Limited By Partner Program' 
                                     WHEN @PartnerProgramList like '%< All Partner Program Members >%' 
									    THEN 'All Partner Programs'
                                     ELSE REPLACE(@PartnerProgramList,'|',',')  
									 END)


  ---- Adjust end day to be sure to include all times up to the next day
DECLARE @AdjEndDate DateTime
SET @AdjEndDate = DATEADD(DAY,1,@EndDate)

DECLARE @StartDateDimDateKey INT 
DECLARE @AdjEndDateDimDateKey INT
SET @StartDateDimDateKey = (Select dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @AdjEndDateDimDateKey = (Select dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @AdjEndDate)


   ----- Drop and Create Results temp table
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 

SELECT DimMember.dim_mms_member_key AS DimMemberKey,
DimLocation.MMSRegion AS SaleRegion,
DimLocation.club_name AS SaleClub, 
DimLocation.club_id AS SaleClubid,
DimEmployee.first_name AS EmployeeFirstname, 
DimEmployee.last_name AS EmployeeLastname,
PackageProduct.product_description AS ProductDescription, 
DimDescription.Description AS PackageStatusDescription, 
CASE WHEN FactPackage.transaction_void_flag = 'Y' 
     THEN 0 
	 ELSE 1 
	 END PackageCount, 
CONVERT(DECIMAL(7,1),CASE WHEN FactPackage.transaction_void_flag = 'Y'       
                          THEN 0 
                          ELSE FactPackage.number_of_sessions 
						  END) AS SessionCount, 
CASE WHEN FactPackage.transaction_void_flag = 'Y'  
     THEN 0 
	 ELSE FactPackage.number_of_sessions * FactPackage.price_per_session 
	 END AS PackageSaleAmount,
DimMember.member_id AS Memberid,
DimMember.first_name AS MemberFirstname,
DimMember.last_name AS MemberLastname,  
'Local Currency' as ReportingCurrencyCode,
FactPackage.price_per_session AS PricePerSession,
PackageCreatedDimDate.calendar_date AS PackageSaleDate,
@HeaderDateRange AS HeaderDateRange,
@HeaderDepartmentList AS HeaderDepartmentList,
@HeaderPartnerProgramList AS HeaderPartnerProgramList,
@ReportRunDateTime AS ReportRunDateTime,
@myLTBucksFilter as HeaderMyLTBucks,
FactPackage.transaction_void_flag AS TransactionVoidFlag
INTO #Results
FROM [marketing].[v_fact_mms_package] FactPackage
 JOIN #Locations DimLocation
   ON FactPackage.reporting_dim_club_key = DimLocation.dim_club_key
 LEFT JOIN [marketing].[v_dim_employee] DimEmployee
   ON FactPackage.primary_sales_dim_employee_key = DimEmployee.dim_employee_key
 LEFT JOIN [marketing].[v_dim_mms_product] PackageProduct
   ON FactPackage.dim_mms_product_key = PackageProduct.dim_mms_product_key
 JOIN #MMSDepartmentNames MMSDepartmentNames                                                ----- Comment Out for DEV
   ON PackageProduct.department_description = MMSDepartmentNames.MMSDepartmentDescription   ----- Comment Out for DEV
 --JOIN #MMSDepartmentNames MMSDepartmentNames                                                -----Comment Out for QA/Prod - Defect created UDW-7502
 --  ON PackageProduct.mms_department_bk_hash = MMSDepartmentNames.dim_mms_department_key     -----Comment Out for QA/Prod - Defect created UDW-7502
 JOIN [marketing].[v_dim_mms_member] DimMember
   ON FactPackage.dim_mms_member_key = DimMember.dim_mms_member_key
 JOIN [marketing].[v_dim_date] PackageCreatedDimDate
   ON FactPackage.created_dim_date_key = PackageCreatedDimDate.dim_date_key
 LEFT JOIN [marketing].[v_dim_description] DimDescription     ------- the following key doesn't match anything - setting as left join to move forward - Defect UDW-7505
   ON FactPackage.package_status_dim_description_key = DimDescription.dim_description_key
 LEFT JOIN [marketing].[v_dim_employee] PackageEnteredEmployee
   ON FactPackage.package_entered_dim_employee_key = PackageEnteredEmployee.dim_employee_key
    AND PackageEnteredEmployee.employee_id = -5
WHERE FactPackage.created_dim_date_key >= @StartDateDimDateKey
  AND FactPackage.created_dim_date_key < @AdjEndDateDimDateKey
  AND ((PackageEnteredEmployee.employee_id Is Not Null and @myLTBucksFilter = 'myLT Buck$ Only') ---- Employee ID -5 "Loyalty Program"
        OR
       (PackageEnteredEmployee.employee_id Is Null and @myLTBucksFilter ='Exclude myLT Buck$')
        OR
       (@myLTBucksFilter = 'Not Limited by myLT Buck$')) 





 ----- multiple steps to create a list of up to 3 partner programs per member 	
   ----- Drop and Create DimMemberKeys temp table
IF OBJECT_ID('tempdb.dbo.#DimMemberKeys', 'U') IS NOT NULL
  DROP TABLE #DimMemberKeys; 
  

  ----- Collect distinct list of customer keys to limit looping
SELECT Distinct DimMemberKey
 INTO #DimMemberKeys
  FROM #Results

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

 
SELECT DISTINCT DimMemberKeys.MemberKeySort,MR.dim_mms_member_key, PP.ProgramName
 INTO #NumberedDimMemberKeyPrograms
FROM [marketing].[v_fact_mms_member_reimbursement_program] MR
JOIN #DimProgramList PP
  ON MR.dim_mms_reimbursement_program_key = PP.DimReimbursementProgramKey
JOIN #NumberedDimMemberKeys DimMemberKeys
  ON MR.dim_mms_member_key = DimMemberKeys.DimMemberKey
WHERE MR.enrollment_dim_date_key < @AdjEndDateDimDateKey
  AND (MR.termination_dim_date_key >= @StartDateDimDateKey OR MR.termination_dim_date_key is null)


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




SELECT 
 Results.SaleRegion,
 Results.SaleClub, 
 Results.SaleClubid,
 Results.EmployeeFirstname, 
 Results.EmployeeLastname,
 Results.ProductDescription, 
 Results.PackageStatusDescription, 
 Results.PackageCount, 
 Results.SessionCount, 
 Results.PackageSaleAmount,
 Results.Memberid,
 Results.MemberFirstname,
 Results.MemberLastname,  
 Results.ReportingCurrencyCode,
 Results.PricePerSession,
 Results.PackageSaleDate,
 PartnerProgramMembers.PartnerProgramList AS PartnerProgramList,
 Results.HeaderDateRange,
 Results.HeaderDepartmentList,
 Results.HeaderPartnerProgramList,
 Results.ReportRunDateTime,
 Results.HeaderMyLTBucks,
 Results.TransactionVoidFlag

FROM  #Results Results
 LEFT JOIN #MemberPartnerProgram PartnerProgramMembers
   ON Results.DimMemberKey = PartnerProgramMembers.dim_mms_member_key
WHERE IsNull(PartnerProgramMembers.dim_mms_member_key,-999) = CASE WHEN @PartnerProgramList = 'All' 
                                                                THEN IsNull(PartnerProgramMembers.dim_mms_member_key,-999) 
																ELSE Results.DimMemberKey
																END



   DROP TABLE #Locations
   DROP TABLE #DimProgramList
   DROP TABLE #MMSDepartmentNames
   DROP TABLE #Results
   DROP TABLE #DimMemberKeys
   DROP TABLE #NumberedDimMemberKeys
   DROP TABLE #NumberedDimMemberKeyPrograms
   DROP TABLE #NumberedDimMemberKeyAndNumberedPrograms
   DROP TABLE #MemberPartnerProgramPrelim
   DROP TABLE #MemberPartnerProgram 


END
