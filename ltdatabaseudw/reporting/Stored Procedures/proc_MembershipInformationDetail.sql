CREATE PROC [reporting].[proc_MembershipInformationDetail] @DateFilter [VARCHAR](50),@ReportStartDate [DATETIME],@ReportEndDate [DATETIME],@MMSClubIDList [VARCHAR](4000),@MembershipTypeList [VARCHAR](8000),@MembershipStatusDescriptionList [VARCHAR](4000),@ValTerminationReasonIDList [VARCHAR](4000),@CorporatePartnerTypeList [VARCHAR](8000),@PartnerCompanyIDList [VARCHAR](8000),@PartnerProgramIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 


----- Execution Sample  Report ID 142
-- Exec [reporting].[proc_MembershipInformationDetail] 'Non-Terminated Memberships As of Date','3/20/2020','3/20/2020','151|8','All Memberships - Excluding Founders','Active|Pending Termination','< Ignore this prompt >','< Ignore this prompt >','< Ignore this prompt >','< Ignore this prompt >'
-- Exec [reporting].[proc_MembershipInformationDetail] 'Non-Terminated Memberships As of Date','3/20/2020','3/20/2020','151|8','All Memberships - Excluding Founders','Active|Pending Termination','< Ignore this prompt >','Invoicing Program','17956','45'
-----


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')
DECLARE @HeaderDateRange VARCHAR(21),
        @EndDate DATETIME,
        @EndDimDateKey VARCHAR(32),
        @StartDate_FirstOfMonth_DimDateKey VARCHAR(32),
		@StartDate_FirstOfMonth DATETIME,
		@ReportStartDimDateKey VARCHAR(32)
SELECT @HeaderDateRange = standard_date_name,
       @EndDate = month_ending_date,
       @EndDimDateKey = month_ending_dim_date_key,
	   @StartDate_FirstOfMonth_DimDateKey = month_starting_dim_date_key,
	   @StartDate_FirstOfMonth = month_starting_date,
	   @ReportStartDimDateKey = dim_date_key
  FROM [marketing].[v_dim_date] 
 WHERE calendar_date = @ReportStartDate


DECLARE  @StartDate_SecondOfMonth_DimDateKey VARCHAR(32)

SELECT  @StartDate_SecondOfMonth_DimDateKey = DimDate.next_day_dim_date_key
  FROM [marketing].[v_dim_date] DimDate
   WHERE calendar_date = @StartDate_FirstOfMonth 



 ----- This query replaces the function used in LTFDM_Operations "fnParsePipeList"
IF OBJECT_ID('tempdb.dbo.#DimClubKeyList', 'U') IS NOT NULL 
DROP TABLE #DimClubKeyList;   

create table #DimClubKeyList with (distribution = round_robin, heap) as
SELECT DimClub.dim_club_key AS DimClubKey, 
        MMSRegion.description AS MMSRegionName,
		DimClub.club_name AS ClubName,
		DimClub.club_id ClubID, 
        DimClub.local_currency_code AS LocalCurrencyCode      
        
    FROM [marketing].[v_dim_club] DimClub
    JOIN [marketing].[v_dim_description]  MMSRegion 
	  ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
   WHERE ('|'+@MMSClubIDList+'|' like '%|'+cast(DimClub.club_id as varchar)+'|%' or '|'+@MMSClubIDList+'|' like '%|-1|%')
     AND DimClub.club_id Not In (-1,99,100)
     AND DimClub.club_id < 900
     AND DimClub.club_type = 'Club'
     AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @StartDate_FirstOfMonth_DimDateKey)  
GROUP BY DimClub.dim_club_key, 
         MMSRegion.description,
         DimClub.club_name,
		 DimClub.club_id, 
         DimClub.local_currency_code

 ----- Create Termination Reason temp table   
 -----  Too avoid executing the parsing stored procedure for better performance
 -----   we are brining that logic into this stored procedure
IF OBJECT_ID('tempdb.dbo.#TerminationReasonIDList', 'U') IS NOT NULL 
DROP TABLE #TerminationReasonIDList;  

DECLARE @item [varchar](8000),
        @list_table [varchar](500),
        @table_name varchar(500),
        @sql varchar(max),
		@NewLineChar as char(2) = char(13) + char(10)

SET	@list_table = 'reason_list' 
SET @table_name = '#'+@list_table
SET @item = @ValTerminationReasonIDList


SET @sql ='DECLARE @list varchar(8000)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@table_name+''') is not null
          drop table ' + @table_name + @NewLineChar+' create table dbo.'+@table_name + ' (item varchar(8000)  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ ''|'', ''||'', ''|'')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%|%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%|%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @table_name+'(item) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)

SELECT ReasonIDList.Item AS  ValTerminationReasonID
 INTO #TerminationReasonIDList   
 FROM #reason_list ReasonIDList
 

 ----- Create #MembershipStatusDescriptionList temp table   
 -----  Too avoid executing the parsing stored procedure for better performance
 -----   we are brining that logic into this stored procedure
 -----   variables already declared earlier in script
 IF OBJECT_ID('tempdb.dbo.#MembershipStatusDescriptionList', 'U') IS NOT NULL 
DROP TABLE #MembershipStatusDescriptionList;  

SET	@list_table = 'status_list' 
SET @table_name = '#'+@list_table
SET @item = @MembershipStatusDescriptionList


SET @sql ='DECLARE @list varchar(8000)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@table_name+''') is not null
          drop table ' + @table_name + @NewLineChar+' create table dbo.'+@table_name + ' (item varchar(8000)  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ ''|'', ''||'', ''|'')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%|%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%|%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @table_name+'(item) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)


SELECT MembershipStatusDescriptionList.Item MembershipStatusDescription
  INTO #MembershipStatusDescriptionList
  FROM #status_list  MembershipStatusDescriptionList



 ----- Create #MembershipTypeList temp table   
 -----  Too avoid executing the parsing stored procedure for better performance
 -----   we are brining that logic into this stored procedure
 -----   variables already declared earlier in script
 IF OBJECT_ID('tempdb.dbo.#MembershipTypeList', 'U') IS NOT NULL 
DROP TABLE #MembershipTypeList;  

SET	@list_table = 'type_list' 
SET @table_name = '#'+@list_table
SET @item = @MembershipTypeList


SET @sql ='DECLARE @list varchar(8000)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@table_name+''') is not null
          drop table ' + @table_name + @NewLineChar+' create table dbo.'+@table_name + ' (item varchar(8000)  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ ''|'', ''||'', ''|'')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%|%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%|%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @table_name+'(item) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)


SELECT MembershipTypeList.Item MembershipType
  INTO #MembershipTypeList        
  FROM #type_list  MembershipTypeList



DECLARE @HeaderMembershipStatusList VARCHAR(4000)
SET @HeaderMembershipStatusList = CASE WHEN @MembershipStatusDescriptionList like '%< Ignore this prompt >%'
                                       THEN 'All Membership Statuses'
									   ELSE REPLACE(@MembershipStatusDescriptionList,'|',', ') END


DECLARE @HeaderMembershipTypeList VARCHAR(8000)
SET @HeaderMembershipTypeList = CASE WHEN @MembershipTypeList like '%< Ignore this prompt >%'
                                     THEN 'All Membership Types'
									 WHEN @MembershipTypeList like '%Loyalty Membership%'
									 THEN REPLACE(Substring(@MembershipTypeList,21,500),'|',', ')
									 ELSE REPLACE(@MembershipTypeList,'|',', ') END


---- Will keep the execution of this stored proc at this time
---- Performance didn't seem too bad relative to the large amount of code that would need to be
---- otherwise brought into this script to replace the stored proc call

 IF OBJECT_ID('tempdb.dbo.#IncludedMembershipTypeDimProduct', 'U') IS NOT NULL 
DROP TABLE #IncludedMembershipTypeDimProduct;  


DECLARE @list_table_membership VARCHAR(100)
SET @list_table_membership = 'membership_type_dim_product'

EXEC marketing.proc_operations_membership_type_list @MembershipTypeList, @list_table_membership

SELECT dim_mms_product_key as DimProductKey   
INTO #IncludedMembershipTypeDimProduct
FROM #membership_membership_type_dim_product



DECLARE @CorporateMembershipFlag CHAR(1)
SET @CorporateMembershipFlag = CASE WHEN @MembershipTypeList like '%Corporate Memberships%'  THEN 'Y' ELSE 'N' END

DECLARE @FilterByPartnerProgramFlag CHAR(1)
SET @FilterByPartnerProgramFlag = CASE WHEN @CorporatePartnerTypeList like '%< Ignore this prompt >%'
                                        AND @PartnerCompanyIDList like '%< Ignore this prompt >%' 
                                        AND @PartnerProgramIDList like '%< Ignore this prompt >%' 
                                            THEN 'N'
                                       ELSE 'Y' END

 ----- Create #CorporatePartnerTypeList temp table   
 -----  Too avoid executing the parsing stored procedure for better performance
 -----   we are brining that logic into this stored procedure
 -----   variables already declared earlier in script
 IF OBJECT_ID('tempdb.dbo.#CorporatePartnerTypeList', 'U') IS NOT NULL 
DROP TABLE #CorporatePartnerTypeList;  

SET	@list_table = 'cp_type_list' 
SET @table_name = '#'+@list_table
SET @item = @CorporatePartnerTypeList


SET @sql ='DECLARE @list varchar(8000)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@table_name+''') is not null
          drop table ' + @table_name + @NewLineChar+' create table dbo.'+@table_name + ' (item varchar(8000)  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ ''|'', ''||'', ''|'')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%|%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%|%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @table_name+'(item) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)


SELECT TypeList.Item CorporatePartnerType
  INTO #CorporatePartnerTypeList       ----- SELECT * FROM #CorporatePartnerTypeList 
  FROM #cp_type_list  TypeList



----- Returns the temp table of reimbursement programs based on selected values.
 IF OBJECT_ID('tempdb.dbo.#DimReimbursementProgram', 'U') IS NOT NULL 
DROP TABLE #DimReimbursementProgram;  

SELECT DISTINCT
       DimReimbursementProgram.dim_mms_reimbursement_program_key AS DimReimbursementProgramKey,
       DimReimbursementProgram.program_name AS ProgramName,
       DimCompany.dim_mms_company_key AS DimCompanyKey,
       DimCompany.company_name AS CompanyName,
       'Unavailable' as AccountOwner,          -----  static value is returned since this data is not in the DW
	   'Unavailable' as SubsidyMeasurement,    -----  these static values facilitate union of queries found within Cognos app. 
       ProgramTypeDimDescription.Description AS ProgramType
  INTO #DimReimbursementProgram    
  FROM [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram
  JOIN [marketing].[v_dim_mms_company] DimCompany
    ON DimReimbursementProgram.dim_mms_company_key = DimCompany.dim_mms_company_key
  JOIN [marketing].[v_dim_description] ProgramTypeDimDescription
    ON DimReimbursementProgram.program_type_dim_description_key = ProgramTypeDimDescription.dim_description_key
  JOIN #CorporatePartnerTypeList CorporatePartnerTypeList
    ON ProgramTypeDimDescription.Description = CorporatePartnerTypeList.CorporatePartnerType
    OR CorporatePartnerTypeList.CorporatePartnerType = '< Ignore this prompt >'  
  JOIN (SELECT DimCompany.company_id  
          FROM [marketing].[v_dim_mms_company] DimCompany
         WHERE ('|'+@PartnerCompanyIDList+'|' like '%|'+cast(DimCompany.company_id as varchar)+'|%' 
            OR '|'+cast(@PartnerCompanyIDList as varchar)+'|' like '%|< Ignore this prompt >|%') 
         GROUP BY DimCompany.company_id) PartnerCompanyIDList
    ON CONVERT(VARCHAR,DimCompany.company_id) = PartnerCompanyIDList.company_id
  JOIN ( SELECT DimReimbursementProgram.reimbursement_program_id          
         FROM [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram
         WHERE ('|'+@PartnerProgramIDList+'|' like '%|'+cast(DimReimbursementProgram.reimbursement_program_id as varchar)+'|%' 
            OR '|'+cast(@PartnerProgramIDList as varchar)+'|' like '%|< Ignore this prompt >|%') 
         GROUP BY DimReimbursementProgram.reimbursement_program_id) PartnerProgramIDList
    ON CONVERT(VARCHAR,DimReimbursementProgram.reimbursement_program_id) = PartnerProgramIDList.reimbursement_program_id





DECLARE @HeaderCorporatePartnerTypeList VARCHAR(8000),
        @HeaderPartnerCompanyNameList VARCHAR(8000)

SET @HeaderCorporatePartnerTypeList = 
CASE WHEN @CorporatePartnerTypeList like '%< Ignore this prompt >%' 
     THEN ''
     ELSE 'Corporate Partner Type(s): '+REPLACE(@CorporatePartnerTypeList,'|',',')
     END 



----- Returns the temp table of memberships
 IF OBJECT_ID('tempdb.dbo.#Memberships', 'U') IS NOT NULL 
DROP TABLE #Memberships;  
	 
SELECT FactMembership.membership_id AS MembershipID,
	   DimDescription.Description AS MembershipStatus,
       FactMembership.home_dim_club_key AS DimClubKey,     ------- New Name
       PrimaryDimMember.dim_mms_member_key AS PrimaryDimMemberKey,      ------- New Name
       FactMembership.membership_created_dim_date_key AS MembershipCreatedDimDateKey,
       FactMembership.membership_activation_date AS MembershipActivationDate,
       FactMembership.membership_cancellation_request_date AS CancellationRequestDate,
       FactMembership.membership_expiration_date AS MembershipExpirationDate,
       MembershipType.dim_mms_product_key AS EndOfDayMembershipTypeDimProductKey,
       FactMembership.termination_reason_dim_description_key AS TerminationReasonDimDescriptionKey,
       FactMembership.original_sales_dim_employee_key AS AdvisorDimEmployeeKey,
	   AdvisorDimEmployee.employee_name_last_first AS AdvisorName,
       FactMembership.dim_mms_company_key AS DimCompanyKey,
       FactMembership.current_price AS DuesAmount,
       FactMembership.membership_source_dim_description_key AS MembershipSourceDimDescriptionKey,
       FactMembership.membership_sales_channel_dim_description_key AS MembershipSalesChannelDimDescriptionKey,
       FactMembership.eft_option_dim_description_key AS EFTOptionDimDescriptionKey,
       MembershipType.attribute_membership_status_summary_group_description AS RevenueReportingCategoryDescription,
	   FactMembership.dim_mms_membership_key AS DimMMSMembershipKey,
	   MembershipType.check_in_group_description AS MembershipTypeCheckInGroupDescription,
	   #DimClubKeyList.MMSRegionName,
       #DimClubKeyList.ClubName,
       #DimClubKeyList.ClubID,
	   #DimClubKeyList.LocalCurrencyCode,
	   PrimaryDimMember.member_id AS PrimaryMemberID,
       PrimaryDimMember.first_name AS PrimaryMemberFirstName,
       PrimaryDimMember.last_name AS PrimaryMemberLastName,
	   PrimaryDimMember.email_address AS EmailAddress,
	   PrimaryDimMember.join_date AS PrimaryMemberJoinDate
  INTO #Memberships   
  FROM [marketing].[v_dim_mms_membership_history] FactMembership
  JOIN #DimClubKeyList
    ON FactMembership.home_dim_club_key = #DimClubKeyList.DimClubKey
  JOIN [marketing].[v_dim_description] DimDescription
    ON FactMembership.membership_status_dim_description_key = DimDescription.dim_description_key
  LEFT JOIN [marketing].[v_dim_employee] AdvisorDimEmployee
    ON FactMembership.original_sales_dim_employee_key = AdvisorDimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON FactMembership.dim_mms_membership_type_key = MembershipType.dim_mms_membership_type_key
  JOIN [marketing].[v_dim_mms_member_history] PrimaryDimMember
    ON FactMembership.dim_mms_membership_key = PrimaryDimMember.dim_mms_membership_key
	AND PrimaryDimMember.description_member = 'Primary'
 WHERE CAST(FactMembership.effective_date_time AS DATE) <= @ReportStartDate
   AND CAST(FactMembership.expiration_date_time AS DATE) > @ReportStartDate
   AND CAST(PrimaryDimMember.effective_date_time AS DATE) <= @ReportStartDate
   AND CAST(PrimaryDimMember.expiration_date_time AS DATE) > @ReportStartDate
   AND @DateFilter = 'Non-Terminated Memberships As of Date'
   AND @ReportStartDate <> CAST(GetDate() AS Date) 
   AND (@ValTerminationReasonIDList = '< Ignore this prompt >' 
        OR (FactMembership.val_termination_reason_id IN (SELECT ValTerminationReasonID FROM #TerminationReasonIDList)))
   AND (MembershipType.dim_mms_product_key IN (SELECT DimProductKey FROM #IncludedMembershipTypeDimProduct)
        OR (FactMembership.corporate_membership_flag = 'Y' AND @CorporateMembershipFlag = 'Y'))
   AND (DimDescription.Description IN (SELECT MembershipStatusDescription FROM #MembershipStatusDescriptionList) 
        OR(DimDescription.Description != 'Terminated' AND '< Ignore this prompt >' IN (SELECT MembershipStatusDescription FROM #MembershipStatusDescriptionList)))


 ---- returns the most recent modification employee for the membership at the report date
 IF OBJECT_ID('tempdb.dbo.#MembershipModificationEmployees', 'U') IS NOT NULL 
DROP TABLE #MembershipModificationEmployees; 

SELECT FactMembershipModificationRequest.dim_mms_membership_key AS DimMMSMembershipKey,
       FactMembershipModificationRequest.modification_dim_employee_key AS ModificationDimEmployeeKey,
	   ModificationDimEmployee.employee_id AS ModificationEmployeeID,
       ModificationDimEmployee.first_name AS ModificationEmployeeFirstName,
       ModificationDimEmployee.last_name AS ModificationEmployeeLastName

  INTO #MembershipModificationEmployees
  FROM (SELECT SelectedMemberships.DimMMSMembershipKey,
         MAX(MMR.membership_modification_request_id) MembershipModificationRequestID
          FROM #Memberships  SelectedMemberships
		   JOIN [marketing].[v_fact_mms_membership_modification_request] MMR
		     ON SelectedMemberships.DimMMSMembershipKey = MMR.dim_mms_membership_key
		   WHERE MMR.request_dim_date_key <= @ReportStartDimDateKey
	      GROUP BY SelectedMemberships.DimMMSMembershipKey)   MostRecentModificationRequest
  JOIN [marketing].[v_fact_mms_membership_modification_request] FactMembershipModificationRequest
    ON MostRecentModificationRequest.MembershipModificationRequestID = FactMembershipModificationRequest.membership_modification_request_id
  JOIN [marketing].[v_dim_employee] ModificationDimEmployee
    ON FactMembershipModificationRequest.modification_dim_employee_key = ModificationDimEmployee.dim_employee_key

 ---- returns data on the most recent reactivation modification for the membership at the report date
  IF OBJECT_ID('tempdb.dbo.#MembershipModification_ReactivationData', 'U') IS NOT NULL 
DROP TABLE #MembershipModification_ReactivationData;  

SELECT Request.dim_mms_membership_key DimMMSMembershipKey,
       Request.request_date_time AS RequestDateTime,
       Request.effective_date AS EffectiveDate,   
	   RequestType.Description As RequestType,
	   CASE WHEN RequestSource.Description Is Null
        THEN 'Club'
	    ELSE RequestSource.Description
	   END    RequestSource
  INTO #MembershipModification_ReactivationData    

  FROM (SELECT SelectedMemberships.DimMMSMembershipKey,
         MAX(MMR.membership_modification_request_id) MembershipModificationRequestID
          FROM #Memberships  SelectedMemberships
		   JOIN [marketing].[v_fact_mms_membership_modification_request] MMR
		     ON SelectedMemberships.DimMMSMembershipKey = MMR.dim_mms_membership_key
		   JOIN [marketing].[v_dim_description] RequestType
		     ON MMR.request_type_dim_description_key = RequestType.dim_description_key
		   JOIN [marketing].[v_dim_description] RequestStatus
		     ON MMR.request_status_dim_mms_description_key = RequestStatus.dim_description_key
           WHERE RequestType.description in('Full Access Conversion','Student Reactivation')
	        AND RequestStatus.description in('Pending','Completed')    ---- excluding Cancelled
			AND MMR.request_dim_date_key <= @ReportStartDimDateKey
	      GROUP BY SelectedMemberships.DimMMSMembershipKey)   MostRecentModificationRequest
   JOIN [marketing].[v_fact_mms_membership_modification_request] Request
     ON MostRecentModificationRequest.MembershipModificationRequestID = Request.membership_modification_request_id
   LEFT JOIN [marketing].[v_dim_description] RequestSource
     ON Request.membership_modification_request_source_dim_description_key = RequestSource.dim_description_key
   JOIN [marketing].[v_dim_description] RequestType
     ON Request.request_type_dim_description_key = RequestType.dim_description_key
   JOIN [marketing].[v_dim_description] RequestStatus
     ON Request.request_status_dim_mms_description_key = RequestStatus.dim_description_key
  


 ----- find the month's jr. dues for these memberships
   IF OBJECT_ID('tempdb.dbo.#MembershipJuniorDues_MostRecentMonth', 'U') IS NOT NULL 
     DROP TABLE #MembershipJuniorDues_MostRecentMonth;  

 SELECT #Memberships.MembershipID,
        #Memberships.DimMMSMembershipKey,
        #Memberships.PrimaryDimMemberKey,
        Sum(FactSalesTransaction.sales_dollar_amount) AS JuniorDues
	INTO #MembershipJuniorDues_MostRecentMonth     
 FROM #Memberships
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
    ON #Memberships.DimMMSMembershipKey = FactSalesTransaction.dim_mms_membership_key
 WHERE FactSalesTransaction.post_dim_date_key >= @StartDate_FirstOfMonth_DimDateKey
   AND FactSalesTransaction.post_dim_date_key < @StartDate_SecondOfMonth_DimDateKey
   AND FactSalesTransaction.dim_mms_transaction_reason_key = '1EF65A188252668A19588A6F76A7AE3F'   ------ Junior Dues Assessment
   GROUP BY #Memberships.MembershipID,#Memberships.DimMMSMembershipKey,#Memberships.PrimaryDimMemberKey

---- Returns the most recent EFT payment for the membership
   IF OBJECT_ID('tempdb.dbo.#EFTPaymentType', 'U') IS NOT NULL 
     DROP TABLE #EFTPaymentType; 
SELECT MaxEFT.DimMMSMembershipKey,
       PaymentType.Description AS EFTPaymentType
 INTO #EFTPaymentType    
FROM (SELECT #Memberships.DimMMSMembershipKey,
       MAX(eft_id) AS MaxEFTID
        FROM #Memberships
       JOIN [marketing].[v_dim_mms_eft] EFT
         ON #Memberships.DimMMSMembershipKey = EFT.dim_mms_membership_key
	     AND EFT.eft_dim_date_key <= @ReportStartDimDateKey
       GROUP BY #Memberships.DimMMSMembershipKey) MaxEFT
  JOIN [marketing].[v_dim_mms_eft] EFT
    ON MaxEFT.MaxEFTID = EFT.eft_id
  JOIN [marketing].[v_fact_mms_payment] Payment
    ON EFT.fact_mms_payment_key = Payment.fact_mms_payment_key
  JOIN [marketing].[v_dim_description] PaymentType
    ON Payment.payment_type_dim_description_key = PaymentType.dim_description_key



 ----- return preliminary results  for these memberships
   IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL 
     DROP TABLE #Results; 

SELECT #Memberships.MMSRegionName,
       #Memberships.ClubName,
       #Memberships.ClubID,
       #Memberships.MembershipID,
       #Memberships.MembershipStatus,
       #Memberships.PrimaryMemberID,
       #Memberships.PrimaryMemberFirstName,
       #Memberships.PrimaryMemberLastName,
	   CASE WHEN ISNULL(DoNotPhone.membership_communication_preference_id,'') =''
	        THEN ''
			ELSE 'Do Not Phone'
			END DoNotPhone,
       CASE WHEN  ISNULL(EMailOptOut.email_address,'') = ''
	        THEN 'Subscribed'
			ELSE 'Unsubscribed'
			END  EmailSolicitationStatus, 
	   CASE WHEN ISNULL(DoNotMail.membership_communication_preference_id,'') =''
	        THEN ''
			ELSE 'Do Not Mail'
			END DoNotMail,
       '('+Convert(Varchar,MembershipHomePhone.area_code)+')'+SubString(Convert(Varchar,MembershipHomePhone.number),1,3)+'-'+SubString(Convert(Varchar,MembershipHomePhone.number),4,4) HomePhone,
       '('+Convert(Varchar,MembershipBusinessPhone.area_code)+')'+SubString(Convert(Varchar,MembershipBusinessPhone.number),1,3)+'-'+SubString(Convert(Varchar,MembershipBusinessPhone.number),4,4) WorkPhone,
       #Memberships.EmailAddress,
       Membership.membership_address_line_1 AS Address1,
       Membership.membership_address_line_2 AS Address2,
       Membership.membership_address_city AS City,
       Membership.membership_address_state_abbreviation AS State,
       Membership.membership_address_postal_code AS Zip,
       Membership.membership_address_country AS Country,
       MembershipCreatedDimDate.standard_date_name AS MembershipCreatedDate,
       MembershipActivationDimDate.standard_date_name AS MembershipActivationDate,
       PrimaryMemberJoinDimDate.standard_date_name AS PrimaryMemberJoinDate,
       CancellationRequestDimDate.standard_date_name AS CancellationRequestDate,
       TerminationDimDate.standard_date_name AS TerminationDate,
       TerminationReasonDimDescription.Description AS CancellationReason,
       DimProduct.product_id AS MembershipProductID,
       DimProduct.product_description AS MembershipTypeDescription,
       #Memberships.MembershipTypeCheckInGroupDescription AS CheckInGroupDescription,
       #Memberships.LocalCurrencyCode AS LocalCurrencyCode,
	   Cast(#Memberships.DuesAmount as Decimal(12,2)) DuesPrice,
	   #EFTPaymentType.EFTPaymentType AS EFTType,
       EFTOption.Description AS EFTStatus,
	   Cast(FactMembershipBalance.end_of_day_current_balance as Decimal(12,2)) AccountBalance,
       #Memberships.AdvisorName,
       DimCompany.corporate_code AS CorporateCode,
       DimCompany.company_name AS CompanyName,
       #MembershipModificationEmployees.ModificationEmployeeID,
       #MembershipModificationEmployees.ModificationEmployeeFirstName,
       #MembershipModificationEmployees.ModificationEmployeeLastName,
       @ReportRunDateTime AS ReportRunDateTime,
       @HeaderDateRange AS HeaderDateRange,
       @HeaderMembershipStatusList AS HeaderMembershipStatusList,
       CAST('' as Varchar(79)) AS HeaderEmptyResult,
	   'Local Currency' AS ReportingCurrencyCode,
       Membership.membership_source AS OriginalMembershipSource,
       MembershipSalesChannelDimDescription.Description AS OriginalSalesChannel,
	   JuniorDues.JuniorDues,
       #Memberships.RevenueReportingCategoryDescription AS MembershipStatusSummaryTypeGroup,
	   #Memberships.DimMMSMembershipKey,
	   ReactivationData.RequestDateTime AS MembershipReactivationRequestDateTime,
	   ReactivationData.RequestType AS MembershipReactivationRequestType,
	   ReactivationData.RequestSource AS MembershipReactivationRequestSource,
	   ReactivationData.EffectiveDate AS MembershipReactivationEffectiveDate
  INTO #Results
  FROM #Memberships
  JOIN [marketing].[v_dim_mms_membership] Membership
    ON #Memberships.DimMMSMembershipKey = Membership.dim_mms_membership_key 
  LEFT JOIN [marketing].[v_dim_date] MembershipCreatedDimDate
    ON #Memberships.MembershipCreatedDimDateKey = MembershipCreatedDimDate.dim_date_key
  LEFT JOIN [marketing].[v_dim_date]  MembershipActivationDimDate
    ON CAST(#Memberships.MembershipActivationDate AS Date) = MembershipActivationDimDate.calendar_date
  LEFT JOIN [marketing].[v_dim_date] PrimaryMemberJoinDimDate
    ON CAST(#Memberships.PrimaryMemberJoinDate AS Date) = PrimaryMemberJoinDimDate.calendar_date
  LEFT JOIN [marketing].[v_dim_date]  CancellationRequestDimDate
    ON CAST(#Memberships.CancellationRequestDate AS Date) = CancellationRequestDimDate.calendar_date
  LEFT JOIN [marketing].[v_dim_date] TerminationDimDate
    ON CAST(#Memberships.MembershipExpirationDate AS Date) = TerminationDimDate.calendar_date
  LEFT JOIN [marketing].[v_dim_mms_product] DimProduct
    ON #Memberships.EndOfDayMembershipTypeDimProductKey = DimProduct.dim_mms_product_key
  LEFT JOIN [marketing].[v_dim_description] TerminationReasonDimDescription
    ON #Memberships.TerminationReasonDimDescriptionKey = TerminationReasonDimDescription.dim_description_key
  LEFT JOIN [marketing].[v_fact_mms_membership_balance] FactMembershipBalance
    ON #Memberships.DimMMSMembershipKey = FactMembershipBalance.dim_mms_membership_key
  LEFT JOIN #MembershipModificationEmployees
    ON #Memberships.DimMMSMembershipKey = #MembershipModificationEmployees.DimMMSMembershipKey
  LEFT JOIN #MembershipModification_ReactivationData  ReactivationData
    ON #Memberships.DimMMSMembershipKey = ReactivationData.DimMMSMembershipKey
  LEFT JOIN [marketing].[v_dim_mms_company] DimCompany
    ON #Memberships.DimCompanyKey = DimCompany.dim_mms_company_key
  LEFT JOIN [marketing].[v_dim_description] MembershipSalesChannelDimDescription
    ON #Memberships.MembershipSalesChannelDimDescriptionKey = MembershipSalesChannelDimDescription.dim_description_key
  LEFT JOIN #MembershipJuniorDues_MostRecentMonth  JuniorDues
    ON #Memberships.DimMMSMembershipKey = JuniorDues.DimMMSMembershipKey
  LEFT JOIN [marketing].[v_dim_mms_membership_communication_preference] DoNotPhone
    ON #Memberships.DimMMSMembershipKey = DoNotPhone.dim_mms_membership_key
	AND DoNotPhone.val_communication_preference_id = 2     ------ 'Do Not Solicit Via Phone'
	AND DoNotPhone.active_flag = 'Y'
  LEFT JOIN [marketing].[v_dim_mms_membership_communication_preference] DoNotMail
    ON #Memberships.DimMMSMembershipKey = DoNotMail.dim_mms_membership_key
	AND DoNotMail.val_communication_preference_id = 1     ------ 'Do Not Solicit Via Mail'
	AND DoNotMail.active_flag = 'Y'
  LEFT JOIN [marketing].[v_dim_mms_membership_phone] MembershipHomePhone
    ON #Memberships.DimMMSMembershipKey = MembershipHomePhone.dim_mms_membership_key
	AND MembershipHomePhone.phone_type_dim_description_key = 'r_mms_val_phone_type_FE4AD4A6B68FF1211DD9DDE2F9BAEC83'    ------ HomePhone
  LEFT JOIN [marketing].[v_dim_mms_membership_phone] MembershipBusinessPhone
    ON #Memberships.DimMMSMembershipKey = MembershipBusinessPhone.dim_mms_membership_key
	AND MembershipBusinessPhone.phone_type_dim_description_key = 'r_mms_val_phone_type_2A7AC50812AD92E9BD4CD2E95E3BB652'    ------ businessPhone
  LEFT JOIN #EFTPaymentType 
    ON #Memberships.DimMMSMembershipKey = #EFTPaymentType.DimMMSMembershipKey
  LEFT JOIN [marketing].[v_dim_description] EFTOption
    ON #Memberships.EFTOptionDimDescriptionKey = EFTOption.dim_description_key
  LEFT JOIN [marketing].[v_fact_commprefs_user_preferences] EMailOptOut
    ON #Memberships.EmailAddress = EMailOptOut.email_address
	AND EMailOptOut.global_opt_in = 0      ------ False = Opted out



--Get active members on the membership as of the selected report date.  The Rank() is to grab the top 2 secondaries
   IF OBJECT_ID('tempdb.dbo.#MembershipCustomers', 'U') IS NOT NULL 
     DROP TABLE #MembershipCustomers; 
SELECT #Results.DimMMSMembershipKey,
       DimMember.dim_mms_member_key DimMemberKey,
       DimMember.description_member AS MemberTypeDescription,
       RANK() OVER (PARTITION BY #Results.DimMMSMembershipKey,DimMember.description_member
                        ORDER BY DimMember.date_of_birth, DimMember.member_id) SecondaryRanking
  INTO #MembershipCustomers
  FROM #Results
  JOIN [marketing].[v_dim_mms_member] DimMember
    ON #Results.DimMMSMembershipKey = DimMember.dim_mms_membership_key
 WHERE DimMember.member_active_flag = 'Y'


--Connect members in #MembershipCustomers to their active partner programs.
   IF OBJECT_ID('tempdb.dbo.#RankedMembershipCustomersPartnerPrograms', 'U') IS NOT NULL 
     DROP TABLE #RankedMembershipCustomersPartnerPrograms; 

SELECT DISTINCT #MembershipCustomers.DimMMSMembershipKey,
                #MembershipCustomers.DimMemberKey,
                #MembershipCustomers.MemberTypeDescription,
                #DimReimbursementProgram.ProgramName,
                #DimReimbursementProgram.AccountOwner,
				#DimReimbursementProgram.SubsidyMeasurement,
                #MembershipCustomers.SecondaryRanking,
                RANK() OVER (PARTITION BY #MembershipCustomers.DimMemberKey, #MembershipCustomers.MemberTypeDescription
                                 ORDER BY #DimReimbursementProgram.ProgramName) ProgramRanking
  INTO #RankedMembershipCustomersPartnerPrograms
  FROM [marketing].[v_fact_mms_member_reimbursement_program] FactMemberReimbursementProgram
  JOIN #MembershipCustomers
    ON FactmemberReimbursementProgram.dim_mms_member_key = #MembershipCustomers.DimMemberKey
  JOIN #DimReimbursementProgram
    ON FactMemberReimbursementProgram.dim_mms_reimbursement_program_key = #DimReimbursementProgram.DimReimbursementProgramKey
 WHERE #MembershipCustomers.SecondaryRanking < 3
   AND FactMemberReimbursementProgram.enrollment_dim_date_key <= @EndDimDateKey
   AND FactMemberReimbursementProgram.termination_dim_date_key > @EndDimDateKey

   IF OBJECT_ID('tempdb.dbo.#MembershipPartnerPrograms', 'U') IS NOT NULL 
     DROP TABLE #MembershipPartnerPrograms; 

SELECT DimMMSMembershipKey,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 1 THEN ProgramName ELSE NULL END) PartnerProgramName1PrimaryMember,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 1 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName1PrimaryMember,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 2 THEN ProgramName ELSE NULL END) PartnerProgramName2PrimaryMember,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 2 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName2PrimaryMember,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 3 THEN ProgramName ELSE NULL END) PartnerProgramName3PrimaryMember,
       MAX(CASE WHEN MemberTypeDescription = 'Primary' AND ProgramRanking = 3 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName3PrimaryMember,
       
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 1 THEN ProgramName ELSE NULL END) PartnerProgramName1PartnerMember,
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 1 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName1PartnerMember,
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 2 THEN ProgramName ELSE NULL END) PartnerProgramName2PartnerMember,
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 2 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName2PartnerMember,
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 3 THEN ProgramName ELSE NULL END) PartnerProgramName3PartnerMember,
       MAX(CASE WHEN MemberTypeDescription = 'Partner' AND ProgramRanking = 3 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName3PartnerMember,
       
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 1 THEN ProgramName ELSE NULL END) PartnerProgramName1SecondaryMember1,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 1 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName1SecondaryMember1,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 2 THEN ProgramName ELSE NULL END) PartnerProgramName2SecondaryMember1,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 2 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName2SecondaryMember1,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 3 THEN ProgramName ELSE NULL END) PartnerProgramName3SecondaryMember1,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 1 AND ProgramRanking = 3 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName3SecondaryMember1,
       
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 1 THEN ProgramName ELSE NULL END) PartnerProgramName1SecondaryMember2,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 1 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName1SecondaryMember2,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 2 THEN ProgramName ELSE NULL END) PartnerProgramName2SecondaryMember2,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 2 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName2SecondaryMember2,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 3 THEN ProgramName ELSE NULL END) PartnerProgramName3SecondaryMember2,
       MAX(CASE WHEN MemberTypeDescription = 'Secondary' AND SecondaryRanking = 2 AND ProgramRanking = 3 THEN AccountOwner ELSE NULL END) AccountOwnerPartnerProgramName3SecondaryMember2,
       MAX(AccountOwner) CompanyAccountOwner,
	   Max(SubsidyMeasurement) CompanySubsidyMeasurement
  INTO #MembershipPartnerPrograms
  FROM #RankedMembershipCustomersPartnerPrograms
 WHERE ProgramRanking < 4
 GROUP BY DimMMSMembershipKey

SELECT MMSRegionName,
       ClubName,
       ClubID,
       #Results.MembershipID,
       MembershipStatus,
       PrimaryMemberID,
       PrimaryMemberFirstName,
       PrimaryMemberLastName,
       DoNotPhone,
       EmailSolicitationStatus,  
       DoNotMail,
       HomePhone,
       WorkPhone,
       EmailAddress,   
       Address1,
       Address2,
       City,
       State,
       Zip,
       Country,
       MembershipCreatedDate,
       MembershipActivationDate,
       PrimaryMemberJoinDate,
       CancellationRequestDate,
       TerminationDate,
       CancellationReason,
       MembershipProductID,
       MembershipTypeDescription,
       CheckInGroupDescription,
       LocalCurrencyCode,
       DuesPrice,
       EFTType,
       EFTStatus,
       AccountBalance,
       AdvisorName,
       CorporateCode,
       CompanyName,
       ModificationEmployeeID,
       ModificationEmployeeFirstName,
       ModificationEmployeeLastName,
       ReportRunDateTime,
       Cast(HeaderDateRange as Varchar(110)) HeaderDateRange,
       HeaderMembershipStatusList,
       HeaderEmptyResult,
       ReportingCurrencyCode,
       OriginalMembershipSource,
       OriginalSalesChannel,
       MembershipStatusSummaryTypeGroup,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       PartnerProgramName1PrimaryMember,
       PartnerProgramName2PrimaryMember,
       PartnerProgramName3PrimaryMember,
       PartnerProgramName1PartnerMember,
       PartnerProgramName2PartnerMember,
       PartnerProgramName3PartnerMember,
       PartnerProgramName1SecondaryMember1,
       PartnerProgramName2SecondaryMember1,
       PartnerProgramName3SecondaryMember1,
       PartnerProgramName1SecondaryMember2,
       PartnerProgramName2SecondaryMember2,
       PartnerProgramName3SecondaryMember2,
       ISNULL(CompanyAccountOwner,'') CompanyAccountOwner,
	   ISNULL(CompanySubsidyMeasurement,'') CompanySubsidyMeasurement,
       AccountOwnerPartnerProgramName1PrimaryMember,
       AccountOwnerPartnerProgramName2PrimaryMember,
       AccountOwnerPartnerProgramName3PrimaryMember,
       AccountOwnerPartnerProgramName1PartnerMember,
       AccountOwnerPartnerProgramName2PartnerMember,
       AccountOwnerPartnerProgramName3PartnerMember,     
       AccountOwnerPartnerProgramName1SecondaryMember1,
       AccountOwnerPartnerProgramName2SecondaryMember1,
       AccountOwnerPartnerProgramName3SecondaryMember1,
       AccountOwnerPartnerProgramName1SecondaryMember2,
       AccountOwnerPartnerProgramName2SecondaryMember2,
       AccountOwnerPartnerProgramName3SecondaryMember2,  
       @HeaderCorporatePartnerTypeList HeaderCorporatePartnerTypeList,
       NULL  HeaderPartnerCompanyNameList,      ------ must return within Cognos 
	   #Results.JuniorDues,
	   @StartDate_FirstOfMonth  JuniorDuesDate,
	   MembershipReactivationRequestType AS ReactivationRequestType,
	   MembershipReactivationRequestSource AS ReactivationRequestSource,
	   MembershipReactivationRequestDateTime AS ReactivationRequestDateTime,
	   MembershipReactivationEffectiveDate AS  ReactivationRequestEffectiveDate  
  FROM #Results
  LEFT JOIN #MembershipPartnerPrograms
    ON #Results.DimMMSMembershipKey  = #MembershipPartnerPrograms.DimMMSMembershipKey 
 WHERE @FilterByPartnerProgramFlag = 'N'
    OR (@FilterByPartnerProgramFlag = 'Y' AND #MembershipPartnerPrograms.DimMMSMembershipKey  IS NOT NULL)
UNION ALL
SELECT Cast(NULL as Varchar(50)) MMSRegionName,
       Cast(NULL as Varchar(50)) ClubName,
       NULL ClubID,
       NULL MembershipID,
       Cast(NULL as Varchar(50)) MembershipStatus,
       NULL PrimaryMemberID,
       Cast(NULL as Varchar(50)) PrimaryMemberFirstName,
       Cast(NULL as Varchar(80)) PrimaryMemberLastName,
       Cast(NULL as Varchar(12)) DoNotPhone,
       Cast(NULL as Varchar(50)) EmailSolicitationStatus,
       Cast(NULL as Varchar(11)) DoNotMail,
       Cast(NULL as Varchar(13)) HomePhone,
       Cast(NULL as Varchar(13)) WorkPhone,
       Cast(NULL as Varchar(140)) EmailAddress,
       Cast(NULL as Varchar(50)) Address1,
       Cast(NULL as Varchar(50)) Address2,
       Cast(NULL as Varchar(50)) City,
       Cast(NULL as Varchar(50)) State,
       Cast(NULL as Varchar(11)) Zip,
       Cast(NULL as Varchar(15)) Country,
       Cast(NULL as Varchar(12)) MembershipCreatedDate,
       Cast(NULL as Varchar(12)) MembershipActivationDate,
       Cast(NULL as Varchar(12)) PrimaryMemberJoinDate,
       Cast(NULL as Varchar(12)) CancellationRequestDate,
       Cast(NULL as Varchar(12)) TerminationDate,
       Cast(NULL as Varchar(50)) CancellationReason,
       NULL MembershipProductID,
       Cast(NULL as Varchar(50)) MembershipTypeDescription,
       Cast(NULL as Varchar(50)) CheckInGroupDescription,
       Cast(NULL as Varchar(15)) LocalCurrencyCode,
       Cast(NULL as Decimal(12,2)) DuesPrice,
       Cast(NULL as Varchar(50)) EFTType,
       Cast(NULL as Varchar(50)) EFTStatus,
       Cast(NULL as Decimal(12,2)) AccountBalance,
       Cast(NULL as Varchar(102)) AdvisorName,
       Cast(NULL as Varchar(50)) CorporateCode,
       Cast(NULL as Varchar(50)) CompanyName,
       NULL ModificationEmployeeID,
       Cast(NULL as Varchar(50)) ModificationEmployeeFirstName,
       Cast(NULL as Varchar(50)) ModificationEmployeeLastName,
       @ReportRunDateTime ReportRunDateTime,
       Cast(@HeaderDateRange as Varchar(110)) HeaderDateRange,
       @HeaderMembershipStatusList HeaderMembershipStatusList,
       'There are no memberships available for the selected parameters.  Please re-try.' HeaderEmptyResult,
       Cast(NULL as Varchar(15)) ReportingCurrencyCode,
       Cast(NULL as Varchar(50)) OriginalMembershipSource,
       Cast(NULL as Varchar(50)) OriginalSalesChannel,
       Cast(NULL as Varchar(50)) MembershipStatusSummaryTypeGroup,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       Cast(NULL as Varchar(50)) PartnerProgramName1PrimaryMember,
       Cast(NULL as Varchar(50)) PartnerProgramName2PrimaryMember,
       Cast(NULL as Varchar(50)) PartnerProgramName3PrimaryMember,
       Cast(NULL as Varchar(50)) PartnerProgramName1PartnerMember,
       Cast(NULL as Varchar(50)) PartnerProgramName2PartnerMember,
       Cast(NULL as Varchar(50)) PartnerProgramName3PartnerMember,
       Cast(NULL as Varchar(50)) PartnerProgramName1SecondaryMember1,
       Cast(NULL as Varchar(50)) PartnerProgramName2SecondaryMember1,
       Cast(NULL as Varchar(50)) PartnerProgramName3SecondaryMember1,
       Cast(NULL as Varchar(50)) PartnerProgramName1SecondaryMember2,
       Cast(NULL as Varchar(50)) PartnerProgramName2SecondaryMember2,
       Cast(NULL as Varchar(50)) PartnerProgramName3SecondaryMember2,
       CAST(NULL as Varchar(100)) CompanyAccountOwner, 
	   CAST(NULL as Varchar(50)) CompanySubsidyMeasurement,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName1PrimaryMember,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName2PrimaryMember,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName3PrimaryMember,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName1PartnerMember,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName2PartnerMember,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName3PartnerMember,     
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName1SecondaryMember1,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName2SecondaryMember1,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName3SecondaryMember1,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName1SecondaryMember2,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName2SecondaryMember2,
       CAST(NULL as Varchar(50)) AccountOwnerPartnerProgramName3SecondaryMember2,
       @HeaderCorporatePartnerTypeList HeaderCorporatePartnerTypeList,
       @HeaderPartnerCompanyNameList HeaderPartnerCompanyNameList,
	   Cast(NULL as Decimal(12,2)) JuniorDues,
	   @StartDate_FirstOfMonth  JuniorDuesDate,
	   CAST(NULL as Varchar(50)) ReactivationRequestType,
	   CAST(NULL as Varchar(50)) ReactivationRequestSource,
	   CAST(NULL as Varchar(50)) ReactivationRequestDateTime,
	   CAST(NULL as Varchar(50)) ReactivationRequestEffectiveDate  
 WHERE (SELECT COUNT(*) FROM #Results) = 0
   AND @DateFilter = 'Non-Terminated Memberships As of Date'
   AND @ReportStartDate <> Convert(Datetime,Convert(Varchar,GetDate(),101),101)



END
