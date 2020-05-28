CREATE PROC [reporting].[proc_LTBucksAward] @FROMtransdate [DATETIME],@TOtransdate [DATETIME],@HomeClub [varchar](4000),@employeestatus [varchar](50) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

	 --Jira REP-7852
	 --Execution Sample EXEC [reporting].[proc_LTBucksAward] '2019-11-01','2019-11-22', 'All Clubs', 'All'


IF OBJECT_ID('tempdb.dbo.#LTBUCK', 'U') IS NOT NULL DROP TABLE #LTBUCK;
IF OBJECT_ID('tempdb.dbo.#LTUSER', 'U') IS NOT NULL DROP TABLE #LTUSER;
IF OBJECT_ID('tempdb.dbo.#LTMEM', 'U') IS NOT NULL  DROP TABLE #LTMEM;
IF OBJECT_ID('tempdb.dbo.#EMPLOYEE', 'U') IS NOT NULL DROP TABLE #EMPLOYEE;
IF OBJECT_ID('tempdb.dbo.#MEMBERSHIP','U') IS NOT NULL DROP TABLE #MEMBERSHIP;
IF OBJECT_ID('tempdb.dbo.#club_list', 'U') IS NOT NULL  DROP TABLE #club_list;
IF OBJECT_ID('tempdb.dbo.#CLUB', 'U') IS NOT NULL DROP TABLE #CLUB;

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-6,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-6,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-6,GETDATE()) ,100),18,2),'  ',' ')
---------Creating LTBucks Temp Table--------
	
	SELECT 
		ltb.bucks_amount bucks_amt,
		ltb.dim_lt_bucks_user_key lt_bucks_user_key,
		ltb.transaction_date_time,
		ltb.bucks_expiration_date_time
	INTO #LTBUCK
	FROM marketing.v_fact_lt_bucks_transactions ltb		--PROD/QA View: marketing.v_fact_lt_bucks_transactions  DEV View: marketing.v_fact_lt_bucks_transactions
	WHERE ltb.transaction_date_time >= @FROMtransdate
			and ltb.transaction_date_time <= @TOtransdate
			and ltb.bucks_expiration_date_time >= getdate()
			and ltb.bucks_amount > 0

---------Creating LTBucks Users Temp Table------
 
	SELECT
		ltusr.dim_mms_member_key mms_member_key,
		ltusr.dim_lt_bucks_users_key lt_bucks_user_key
	INTO #LTUSER
	FROM marketing.v_dim_lt_bucks_users ltusr

---------Creating LTBucks Members Temp Table------
	
	SELECT 
		mmsm.member_id,
		mmsm.dim_mms_member_key mms_member_key,
		mmsm.dim_mms_membership_key mms_membership_key ,
		mmsm.customer_name,
		mmsm.member_active_flag
	INTO #LTMEM
	FROM marketing.v_dim_mms_member mmsm
	WHERE mmsm.member_active_flag like 'Y'

---------Creating LT Employees Temp Table------
	
	SELECT
		emp.employee_id,
		emp.member_id,
		emp.employee_active_flag
	INTO #EMPLOYEE
	FROM marketing.v_dim_employee emp
	Where emp.employee_active_flag like 'Y'
	
--------Creating Membership Temp table-------------
	
	SELECT
		mmsp.dim_mms_membership_key membership_id,
		mmsp.home_dim_club_key home_club_key
	INTO #MEMBERSHIP 
	FROM marketing.v_dim_mms_membership mmsp

--------Creating club list temp table for multiple selection functions--------

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'
	EXEC marketing.proc_parse_pipe_list @HomeClub,@list_table

--------Creating ClubInfo Temp table-------------

SELECT  
	club.dim_club_key club_key,
	club.club_name,
	club.club_id,
	club.local_currency_code,
	club.club_close_dim_date_key
INTO #CLUB
FROM marketing.v_dim_club club
Join #club_list ClubKeyList 
on ClubKeyList.item=club.club_id or @HomeClub = 'All Clubs'
where club.club_id NOT IN ( -1, 99, 100 )
	AND club.club_close_dim_date_key IN ( '-997', '-998', '-999' )

SELECT
		CONVERT(NVARCHAR,ltb.transaction_date_time,1) as AwardedDate,
		mmsm.customer_name as CustomerName,
		SUM(ltb.bucks_amt) as LTBucksAmount,
		mmsm.member_id as MemberID,
		emp.employee_id as EmployeeID,
		club.club_name as HomeClubName,
		club.local_currency_code as LocalCurrencyCode,
		mmsm.member_active_flag as MemberActiveFlag,
		emp.employee_active_flag as ActiveEmployeeFlag,
		@ReportRunDateTime as ReportRunDateTime

		FROM #LTUSER ltusr
		JOIN #LTBUCK ltb
		ON ltb.lt_bucks_user_key = ltusr.lt_bucks_user_key
		JOIN #LTMEM mmsm
		ON mmsm.mms_member_key = ltusr.mms_member_key
		JOIN #MEMBERSHIP mmsp
		ON mmsp.membership_id = mmsm.mms_membership_key
		JOIN #CLUB club
		ON club.club_key = mmsp.home_club_key
		LEFT JOIN #EMPLOYEE emp
		ON emp.member_id = mmsm.member_id

		WHERE
			 (@employeestatus like 'Employee' AND emp.employee_id IS NOT NULL)
			 OR (@employeestatus like 'Member' AND emp.employee_id IS NULL)
			 OR (@employeestatus like 'All' )


		GROUP BY 
		ltb.transaction_date_time,
		mmsm.customer_name,
		mmsm.member_id,
		emp.employee_id,
		emp.employee_active_flag,
		club.club_name,
		club.local_currency_code,
		mmsm.member_active_flag

		ORDER BY ltb.transaction_date_time ASC, mmsm.member_id

DROP TABLE #LTUSER
DROP TABLE #LTBUCK
DROP TABLE #LTMEM
DROP TABLE #EMPLOYEE
DROP TABLE #CLUB
DROP TABLE #club_list



END
