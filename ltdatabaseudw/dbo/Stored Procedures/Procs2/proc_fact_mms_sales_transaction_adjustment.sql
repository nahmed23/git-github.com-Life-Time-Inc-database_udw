CREATE PROC [dbo].[proc_fact_mms_sales_transaction_adjustment] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/*declare @dv_batch_id bigint = -1*/
/*declare @job_group varchar(500) = 'dv_main_azure'*/
/*declare @begin_extract_date_time datetime = '1753-11-14 01:59:13'*/

declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-1) from fact_mms_sales_transaction_adjustment)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_sales_transaction_adjustment_key), location=user_db) as
select
d_mms_mms_tran.fact_mms_sales_transaction_key as fact_mms_sales_transaction_adjustment_key,
d_mms_mms_tran.mms_tran_id as mms_tran_id,
d_mms_mms_tran.post_dim_date_key as post_dim_date_key,
d_mms_mms_tran.tran_dim_date_key as tran_dim_date_key,
d_mms_mms_tran.dim_club_key as dim_club_key,
case when d_mms_mms_tran.fact_mms_sales_transaction_key in ('-997','-998','-999') then d_mms_mms_tran.fact_mms_sales_transaction_key
     when dim_club.dim_club_key is null then '-998'
     when dim_mms_membership.home_dim_club_key is null then '-998'
     when dim_club.club_id =13 then dim_mms_membership.home_dim_club_key
 else dim_club.dim_club_key end as transaction_reporting_dim_club_key,
d_mms_mms_tran.dim_mms_member_key as dim_mms_member_key,
d_mms_mms_tran.dim_mms_membership_key as dim_mms_membership_key,
d_mms_mms_tran.dim_mms_transaction_reason_key as dim_mms_transaction_reason_key,
d_mms_mms_tran.dim_mms_drawer_activity_key as  dim_mms_drawer_activity_key,
d_mms_mms_tran.pos_amount as pos_amount,
d_mms_mms_tran.tran_amount as tran_amount,
d_mms_mms_tran.transaction_entered_dim_employee_key as transaction_entered_dim_employee_key,
d_mms_mms_tran.voided_flag as voided_flag,
isnull(convert(varchar,d_mms_mms_tran.dv_load_date_time,112),'-998') as udw_inserted_dim_date_key,
case when d_mms_tran_item.mms_tran_id is null then 'N' else 'Y' end as tran_item_exists_flag,
d_mms_mms_tran.dv_batch_id,
d_mms_mms_tran.dv_load_date_time,
row_number() over (partition by d_mms_mms_tran.fact_mms_sales_transaction_key order by d_mms_mms_tran.fact_mms_sales_transaction_key) as duplicate_filter
from
d_mms_mms_tran
left join d_mms_tran_item on 
d_mms_mms_tran.mms_tran_id = d_mms_tran_item.mms_tran_id 
left join dim_mms_membership on
d_mms_mms_tran.dim_mms_membership_key = dim_mms_membership.dim_mms_membership_key
left join dim_club on
d_mms_mms_tran.dim_club_key = dim_club.dim_club_key
where d_mms_mms_tran.dv_batch_id >= @load_dv_batch_id
and d_mms_mms_tran.membership_adjustment_flag = 'Y'  /*----UDW-8784: added filter condition to extarct only Adjustemnt type transactions-------*/

     /*   Delete records from the table that exist*/
     /*   Insert records from temp table for current and missing batches*/

begin tran

delete dbo.fact_mms_sales_transaction_adjustment
    where fact_mms_sales_transaction_adjustment_key in (select fact_mms_sales_transaction_adjustment_key from dbo.#etl_step_1)

        insert into fact_mms_sales_transaction_adjustment
          (      fact_mms_sales_transaction_adjustment_key,
                 mms_tran_id,
                 post_dim_date_key,
                 tran_dim_date_key,
                 dim_club_key,
                 transaction_reporting_dim_club_key,
                 dim_mms_member_key,
                 dim_mms_membership_key,
                 dim_mms_transaction_reason_key,
                 dim_mms_drawer_activity_key,
                 pos_amount,
                 tran_amount,
                 transaction_entered_dim_employee_key,
                 tran_item_exists_flag,
                 voided_flag,
                 udw_inserted_dim_date_key,
                 dv_batch_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_inserted_date_time,
                 dv_insert_user )
                     select
                 fact_mms_sales_transaction_adjustment_key,
                 mms_tran_id,
                 post_dim_date_key,
                 tran_dim_date_key,
                 dim_club_key,
                 transaction_reporting_dim_club_key,
                 dim_mms_member_key,
                 dim_mms_membership_key,
                 dim_mms_transaction_reason_key,
                 dim_mms_drawer_activity_key,
                 pos_amount,
                 tran_amount,
                 transaction_entered_dim_employee_key,
                 tran_item_exists_flag,
                 voided_flag,
                 udw_inserted_dim_date_key,
                 dv_batch_id,
                 dv_load_date_time,
                 'dec 31, 9999',
                 getdate(),
                 suser_sname()
        from #etl_step_1 where duplicate_filter = 1

    commit tran

   end
