CREATE PROC [dbo].[proc_temp_fact_mms_new_membership] AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with (DISTRIBUTION = ROUND_ROBIN, HEAP ) as
select 
MembershipID,
MMSClubID, 
EmployeeID,
CreatedDateTime,
MemberID,
DimLocationKey,
PrimarySalesDimEmployeeKey,
DimCustomerKey,
EnrollmentFee,
CorporateMembershipFlag,
IncludeInDSSRFlag,
MembershipTypeID,
OriginalCurrencyCode,
USDMonthlyAverageDimExchangeRateKey,
LocalCurrencyDimPlanExchangeRateKey,
USDDimPlanExchangeRateKey,
LocalCurrencyMonthlyAverageDimExchangeRateKey,
InsertedDateTime
from Temp_FactNewMembership

if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with (DISTRIBUTION = ROUND_ROBIN, HEAP ) as
select 
#etl_step1.MembershipID as MembershipID,
case when #etl_step1.MembershipID in ('-1','-2','-3') then '-998' when #etl_step1.MembershipID  is null then '-998' 
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.MembershipID  as varchar(500))	,'z#@$k%&P' )  )),2) end
	 as dim_mms_membership_key,
case when #etl_step1.MMSClubID in ('-1','-2','-3') then '-998' when #etl_step1.MMSClubID  is null then '-998' 
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.MMSClubID  as varchar(500)),'z#@$k%&P'))),2) end
	 as home_dim_club_key,
case when #etl_step1.MemberID in ('-1','-2','-3') then '-998' when #etl_step1.MemberID  is null then '-998' 
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.MemberID  as varchar(500)),'z#@$k%&P'))),2) end
	 as dim_mms_member_key,
case when #etl_step1.MembershipTypeID in ('-1','-2','-3') then '-998' when #etl_step1.MembershipTypeID  is null then '-998' 
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.MembershipTypeID  as varchar(500)),'z#@$k%&P'))),2) end
	 as dim_mms_membership_type_key,
case when #etl_step1.EmployeeID in ('-1','-2','-3') then '-998' when #etl_step1.EmployeeID  is null then '-998' 
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.EmployeeID  as varchar(500)),'z#@$k%&P'))),2) end
	 as PrimarySalesDimEmployeeKey,	 
case when #etl_step1.CreatedDateTime is null then '-998'   
     else convert(varchar, #etl_step1.CreatedDateTime, 112) end as created_date_time_key
from #etl_step1

if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
create table dbo.#etl_step3 with (DISTRIBUTION = ROUND_ROBIN, HEAP ) as
select 
#etl_step2.dim_mms_membership_key dim_mms_membership_key,
#etl_step1.MembershipID membership_id,
#etl_step2.created_date_time_key created_date_time_key,
#etl_step2.home_dim_club_key home_dim_club_key,
#etl_step2.PrimarySalesDimEmployeeKey primary_sales_dim_employee_key,
#etl_step2.dim_mms_member_key dim_mms_member_key,
#etl_step1.EnrollmentFee enrollment_fee,
#etl_step1.CorporateMembershipFlag corporate_membership_flag,
#etl_step1.IncludeInDSSRFlag include_in_dssr_flag,
#etl_step2.dim_mms_membership_type_key dim_mms_membership_type_key,
#etl_step1.OriginalCurrencyCode original_currency_code,
#etl_step1.USDMonthlyAverageDimExchangeRateKey usd_monthly_average_dim_exchange_rate_key,
#etl_step1.USDDimPlanExchangeRateKey usd_dim_plan_exchange_rate_key,
#etl_step1.LocalCurrencyMonthlyAverageDimExchangeRateKey local_currency_monthly_average_dim_exchange_rate_key,
#etl_step1.LocalCurrencyDimPlanExchangeRateKey local_currency_dim_plan_exchange_rate_key,
#etl_step1.InsertedDateTime dv_load_date_time
from #etl_step1 
    join #etl_step2
         on #etl_step1.MembershipID=#etl_step2.MembershipID

		 
truncate table fact_mms_new_membership
begin tran
 insert into fact_mms_new_membership
           (fact_mms_new_membership_key,
			membership_id,
			created_date_time_key,
			home_dim_club_key,
			primary_sales_dim_employee_key,
			dim_mms_member_key,
			enrollment_fee,
			corporate_membership_flag,
			include_in_dssr_flag,
			dim_mms_membership_type_key,
			original_currency_code,
			usd_monthly_average_dim_exchange_rate_key,
			usd_dim_plan_exchange_rate_key,
			local_currency_monthly_average_dim_exchange_rate_key,
			local_currency_dim_plan_exchange_rate_key,
			dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            dv_inserted_date_time,
            dv_insert_user)

	select  dim_mms_membership_key,
			membership_id,
			created_date_time_key,
			home_dim_club_key,
			primary_sales_dim_employee_key,
			dim_mms_member_key,
			enrollment_fee,
			corporate_membership_flag,
			include_in_dssr_flag,
			dim_mms_membership_type_key,
			original_currency_code,
			'-998',
			'-998',
			'-998',
			'-998',
			dv_load_date_time,
            'dec 31, 9999',
            '-1',
            getdate() ,
            suser_sname()
			from #etl_step3


commit tran
end

