CREATE PROC [dbo].[proc_fact_mms_ACH_charge_back_detail] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_ACH_charge_back_detail)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_ACH_charge_back_detail_key), location=user_db) as 
  select 
     ACH_charge_back_detail.bk_hash fact_mms_ACH_charge_back_detail_key,
	 ACH_charge_back_detail.transaction_date transaction_date,
	 'ACHKickback'  transaction_memo,
	 case when (ACH_charge_back_detail.local_currency_code='USD') then 'LTFOPCO'
	      when (ACH_charge_back_detail.local_currency_code='CAD') then 'LTFOPCOCAN' end as company_id,
	 ACH_charge_back_detail.local_currency_code currency_id,
	 club.workday_region+'-ACHKickback' transaction_id,
	 ACH_charge_back_detail.club_name club_name,
	 ACH_charge_back_detail.transaction_amount transaction_amount,
	 ACH_charge_back_detail.posted_date posted_date,
	 ACH_charge_back_detail.transaction_line_amount transaction_line_amount,
	 'ACHKickback-VISA/MC' as transaction_line_memo,
	club.workday_region as region_id,
	'Club' as cost_center_id,
	'VMC' as tender_type_id,
	 ACH_charge_back_detail.dv_load_date_time dv_load_date_time,
	 'dec 31, 9999'  dv_load_end_date_time ,
	 ACH_charge_back_detail.dv_batch_id dv_batch_id	
	 from d_mms_ACH_charge_back_detail ACH_charge_back_detail
  left join d_mms_club club
  on ACH_charge_back_detail.club_name = club.club_name
  where ACH_charge_back_detail.club_name <> 'EFT INTERNAL'

	if object_id('tempdb..#etl_step_sum') is not null drop table #etl_step_sum
create table dbo.#etl_step_sum with(distribution=hash(club_name), location=user_db) as 
  select  club_name,
          transaction_date,
		  transaction_memo,
		  company_id,
		  currency_id,
		  transaction_id,
		  sum(#etl_step_1.transaction_amount) transaction_amount,
		  posted_date,
		  sum(#etl_step_1.transaction_line_amount) transaction_line_amount,
		  transaction_line_memo,
		  region_id,
		  cost_center_id,
		  tender_type_id,
		  dv_load_date_time,
		  dv_load_end_date_time,
		  dv_batch_id
  from #etl_step_1
  group by club_name,
          transaction_date,
		  transaction_memo,
		  company_id,
		  currency_id,
		  transaction_id,
		  posted_date,
		  transaction_line_memo,
		  region_id,
		  cost_center_id,
		  tender_type_id,
		  dv_load_date_time,
		  dv_load_end_date_time,
		  dv_batch_id
 

truncate table fact_mms_ACH_charge_back_detail
begin tran


   insert into fact_mms_ACH_charge_back_detail
   (
   transaction_date,
   transaction_memo,
   deposit,
   withdrawal,
   company_id,
   currency_id,
   batch_id,
   transaction_id,
   drawer_id,
   transaction_amount,
   shipping_amount,
   discount_amount,
   drawer_close_comments,
   posted_date,
   transaction_line_amount,
   transaction_line_tax_amount,
   transaction_line_memo,
   region_id,
   cost_center_id,
   tender_type_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user)
   
   select
   transaction_date,
   transaction_memo,
   case when transaction_amount>= 0.00 then 'false'
   else 'true'
   end deposit,
      case when transaction_amount>= 0.00 then 'true'
   else 'false'
   end withdrawal,
   company_id,
   currency_id,
   null as batch_id,
   transaction_id,
   null as drawer_id,
   transaction_amount,
   null as shipping_amount,
   null as discount_amount,
   null as drawer_close_comments,
   posted_date,
   transaction_amount,
   null as transaction_line_tax_amount,
   transaction_line_memo,
   region_id,
   cost_center_id,
   tender_type_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
    getdate()  ,
   suser_sname()
   from #etl_step_sum
   
   
  commit tran
  
 end




--GO

--truncate table fact_mms_ACH_charge_back_detail
--exec proc_fact_mms_ACH_charge_back_detail -1
