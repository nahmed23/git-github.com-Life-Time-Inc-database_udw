CREATE PROC [reporting].[proc_PackageSessionsDeliveredDetail_WithPartnerPrograms] @ClubIDs [VARCHAR](1000),@StartDate [DATETIME],@EndDate [DATETIME],@MMSDeptDescriptionList [VARCHAR](1000),@PartnerProgramList [VARCHAR](2000),@myLTBucksFilter [VARCHAR](100) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
--- exec [reporting].[proc_PackageSessionsDeliveredDetail_WithPartnerPrograms] '205|151','7/1/2012','7/12/2017','Personal Training','All','Not Limited by myLT Buck$'
-----


DECLARE @HeaderDateRange VARCHAR(33) 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @HeaderDateRange = convert(varchar(12), @StartDate, 107) + ' to ' + convert(varchar(12), @EndDate, 107)
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time

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


SET @EndDate = DATEADD(DAY,1,@EndDate)

DECLARE @StartDateDimDateKey INT 
DECLARE @EndDateDimDateKey INT
SET @StartDateDimDateKey = (Select dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @EndDateDimDateKey = (Select dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndDate)


IF OBJECT_ID('tempdb.dbo.#ReportSessions', 'U') IS NOT NULL
  DROP TABLE #ReportSessions; 

 ---- Performance was much improved by first creating a temp table of the desired session records
SELECT FactPackageSession.dim_mms_member_key AS DimMemberKey,
       FactPackageSession.voided_flag AS VoidedFlag,
       FactPackageSession.delivered_session_price AS DeliveredSessionPrice,
	   FactPackageSession.delivered_dim_club_key AS DeliveredDimClubKey,
	   FactPackageSession.fact_mms_package_dim_product_key AS DimProductKey,
	   FactPackageSession.delivered_dim_employee_key AS DeliveredDimEmployeeKey,
	   FactPackageSession.delivered_dim_date_key AS DeliveredDimDateKey,
	   FactPackageSession.delivered_dim_time_key AS DeliveredDimTimeKey,
	   FactPackageSession.package_entered_dim_club_key AS PackageEnteredDimLocationKey,
	   FactPackageSession.package_status_dim_description_key AS PackageStatusDimDescriptionKey,
	   PackageEnteredEmployee.employee_id
	INTO #ReportSessions
FROM [marketing].[v_fact_mms_package_session] FactPackageSession
	JOIN #Locations DeliveryDimLocation
	  ON FactPackageSession.[delivered_dim_club_key] = DeliveryDimLocation.dim_club_key
    JOIN #MMSDepartmentNameAndProduct PackageProduct
      ON FactPackageSession.fact_mms_package_dim_product_key = PackageProduct.DimProductKey
	LEFT JOIN [marketing].[v_dim_employee] PackageEnteredEmployee
      ON FactPackageSession.package_entered_dim_employee_key = PackageEnteredEmployee.dim_employee_key
      AND PackageEnteredEmployee.employee_id = -5
WHERE FactPackageSession.delivered_dim_date_key >= @StartDateDimDateKey
      AND FactPackageSession.delivered_dim_date_key < @EndDateDimDateKey
	  AND ((PackageEnteredEmployee.employee_id Is Not Null and @myLTBucksFilter = 'myLT Buck$ Only') ---- Employee ID -5 "Loyalty Program"
        OR
       (PackageEnteredEmployee.employee_id Is Null and @myLTBucksFilter ='Exclude myLT Buck$')
        OR
       (@myLTBucksFilter = 'Not Limited by myLT Buck$')) 


---- Setting up temp table to return a string of up to 3 partner programs per member

     ----- Drop and Create DimMemberKeys temp table
IF OBJECT_ID('tempdb.dbo.#DimMemberKeys', 'U') IS NOT NULL
  DROP TABLE #DimMemberKeys; 
	 
  ----- Collect distinct list of customer keys to limit looping
SELECT Distinct DimMemberKey
 INTO #DimMemberKeys
  FROM #ReportSessions

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
WHERE MR.enrollment_dim_date_key < @EndDateDimDateKey
  AND (MR.termination_dim_date_key >= @EndDateDimDateKey OR MR.termination_dim_date_key is null)



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



SELECT DeliveryDimLocation.MMSRegion AS RegionDescription,
       DeliveryDimLocation.club_name AS RevenueClub, 
       DeliveryDimLocation.club_id AS RevenueClubID, 
       CASE WHEN FactPackageSession.VoidedFlag = 'Y'  
            THEN 0 
    		ELSE 1.0
			END AS SessionCount, 
       FactPackageSession.DeliveredSessionPrice AS Sessionprice,
       -- COUNT EMPLOYEE FOR DELIVERED CLUB ; ONE EMPLOYEE CAN WORK IN MORE THAN ONE CLUB
       DimEmployee.employee_id AS DeliveredEmployeeID,
       DimEmployee.first_name AS DeliveredEmployeeFirstName,
       DimEmployee.last_name AS DeliveredEmployeeLastName,
       DimMember.member_id AS Memberid, 
       DimMember.first_name AS MemberFirstname, 
       DimMember.last_name AS MemberLastname,  
	   Convert(Datetime,Replace(Substring(convert(varchar,SessionDeliveredDimDate.calendar_date,100),1,6)+', '+Substring(convert(varchar,SessionDeliveredDimDate.calendar_date,100),8,4)+' '+Convert(varchar,SessionDeliveredDimTime.display_12_hour_time),'  ',' ')) AS Deliverddatetime_Sort,
	   Replace(Substring(convert(varchar,SessionDeliveredDimDate.calendar_date,100),1,6)+', '+Substring(convert(varchar,SessionDeliveredDimDate.calendar_date,100),8,4)+' '+convert(varchar,SessionDeliveredDimTime.display_12_hour_time),'  ',' ') as Delivereddatetime,
  	   MMSDepartmentNames.MMSProductid AS ProductID, 
       MMSDepartmentNames.ProductDescription,
       SalesDimLocation.club_name AS SaleClub,        
       CASE WHEN DimDescription.Description = 'Voided' THEN '*' ELSE '' END  VoidedPackageFlag, 
       MemberPartnerPrograms.PartnerProgramList,
       @HeaderDateRange AS HeaderDateRange,
       @HeaderDepartmentList AS HeaderDepartmentList,
       @HeaderPartnerProgramList AS HeaderPartnerProgramList,
       @ReportRunDateTime AS ReportRunDateTime,
       'Local Currency' AS ReportingCurrencyCode,
       @myLTBucksFilter as HeaderMyLTBucks 

	 FROM #ReportSessions FactPackageSession
	   JOIN #Locations DeliveryDimLocation
	     ON FactPackageSession.DeliveredDimClubKey = DeliveryDimLocation.dim_club_key
	   JOIN #MMSDepartmentNameAndProduct MMSDepartmentNames
         ON FactPackageSession.DimProductKey = MMSDepartmentNames.DimProductKey
	   JOIN [marketing].[v_dim_employee] DimEmployee
         ON FactPackageSession.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
	   JOIN [marketing].[v_dim_mms_member] DimMember
         ON FactPackageSession.DimMemberKey = DimMember.dim_mms_member_key
	   JOIN [marketing].[v_dim_date] SessionDeliveredDimDate
         ON FactPackageSession.DeliveredDimDateKey = SessionDeliveredDimDate.dim_date_key
	   JOIN [marketing].[v_dim_time] SessionDeliveredDimTime
	     ON FactPackageSession.DeliveredDimTimeKey = SessionDeliveredDimTime.dim_time_key
	   JOIN [marketing].[v_dim_club] SalesDimLocation
	     ON FactPackageSession.PackageEnteredDimLocationKey = SalesDimLocation.dim_club_key
	   JOIN [marketing].[v_dim_description] DimDescription 
         ON FactPackageSession.PackageStatusDimDescriptionKey = DimDescription.dim_description_key
       LEFT JOIN #MemberPartnerProgram MemberPartnerPrograms
         ON FactPackageSession.DimMemberKey = MemberPartnerPrograms.dim_mms_member_key
      WHERE IsNull(MemberPartnerPrograms.dim_mms_member_key,-999) = CASE WHEN @PartnerProgramList = 'All' 
                                                                THEN IsNull(MemberPartnerPrograms.dim_mms_member_key,-999) 
																ELSE FactPackageSession.DimMemberKey
																END


	

   DROP TABLE #Locations
   DROP TABLE #DimProgramList
   DROP TABLE #MMSDepartmentNames
   DROP TABLE #MMSDepartmentNameAndProduct
   DROP TABLE #ReportSessions
   DROP TABLE #DimMemberKeys
   DROP TABLE #NumberedDimMemberKeys
   DROP TABLE #NumberedDimMemberKeyPrograms
   DROP TABLE #NumberedDimMemberKeyAndNumberedPrograms
   DROP TABLE #MemberPartnerProgramPrelim
   DROP TABLE #MemberPartnerProgram


END
