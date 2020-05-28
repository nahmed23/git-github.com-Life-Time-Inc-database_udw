CREATE PROC [dbo].[proc_dim_mms_membership_history] @dv_batch_id [bigint] AS

BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

DECLARE @max_dv_batch_id BIGINT = ( SELECT max(isnull(dv_batch_id, - 1)) FROM dim_mms_membership_history )
DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
DECLARE @load_dv_batch_id BIGINT = CASE WHEN @max_dv_batch_id < @current_dv_batch_id
									THEN @max_dv_batch_id
									ELSE @current_dv_batch_id
									END
  
/*
This code figures collapses fact_mms_member_reimbursement_program date ranges by membership, because we potentially need to split dim_mms_membership records
*/

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
    create table #etl_step1 with (distribution = hash(dim_mms_membership_key) ) as
     select 
	 dim_mms_membership_key, 
	 enrollment_date, 
	 termination_date, 
	 max(dv_load_date_time) dv_load_date_time, 
	 max(dv_batch_id) dv_batch_id, 
	 max(dv_load_end_date_time) dv_load_end_date_time
     from fact_mms_member_reimbursement_program
     where dim_mms_membership_key not in ('-997','-998','-999')
     group by dim_mms_membership_key, enrollment_date, termination_date

/*this table will hold the final ranges*/
if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
    create table #etl_step2 
	(
	dim_mms_membership_key varchar(32), 
	enrollment_date datetime, 
	termination_date datetime,
	dv_load_date_time datetime, 
	dv_batch_id bigint,
	dv_load_end_date_time datetime
	) 
	with ( distribution = hash(dim_mms_membership_key) )
while (select count(*) from #etl_step1 where dim_mms_membership_key is not null) > 0
begin

    /*records whose enrollment date isn't between any other date, this should only get populated on the first loop iteration*/
    insert into #etl_step2
    select 
	etl_step1_1.dim_mms_membership_key, 
	etl_step1_1.enrollment_date, 
	max(etl_step1_1.termination_date), 
	max(etl_step1_1.dv_load_date_time), 
	max(etl_step1_1.dv_batch_id),
	max(etl_step1_1.dv_load_end_date_time) dv_load_end_date_time
    from #etl_step1 etl_step1_1
         left join #etl_step1 etl_step1_2 on etl_step1_1.dim_mms_membership_key = etl_step1_2.dim_mms_membership_key and etl_step1_1.enrollment_date > etl_step1_2.enrollment_date and etl_step1_1.enrollment_date <= etl_step1_2.termination_date
         left join #etl_step2 etl_step2 on etl_step1_1.dim_mms_membership_key = etl_step2.dim_mms_membership_key and etl_step1_1.enrollment_date > etl_step2.enrollment_date 
		     and etl_step1_1.enrollment_date <= etl_step2.termination_date
    where etl_step1_2.dim_mms_membership_key is null and etl_step2.dim_mms_membership_key is null
    group by etl_step1_1.dim_mms_membership_key, etl_step1_1.enrollment_date

     /*this is required because complex deletes are not support, will get deleted out with the updates later*/
     /*removes records for which the enrollment range is covered*/
    update #etl_step1
        set enrollment_date = null,
            termination_date = null
        from #etl_step2
        where #etl_step1.dim_mms_membership_key = #etl_step2.dim_mms_membership_key 
		and #etl_step1.enrollment_date >= #etl_step2.enrollment_date 
		and #etl_step1.termination_date <= #etl_step2.termination_date

     /*this is required because complex updates are not supported*/
     /*finds the max term date for each enrollment date*/
if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
    create table #etl_step3 with(distribution = hash(dim_mms_membership_key)) as
    select 
	#etl_step2.dim_mms_membership_key, 
	#etl_step2.enrollment_date, 
	max(#etl_step1.termination_date) termination_date, 
	max(#etl_step1.dv_load_date_time) dv_load_date_time, 
	max(#etl_step1.dv_batch_id) dv_batch_id,
	max(#etl_step1.dv_load_end_date_time) dv_load_end_date_time
    from #etl_step2
    join #etl_step1 on #etl_step2.dim_mms_membership_key = #etl_step1.dim_mms_membership_key and #etl_step1.enrollment_date between #etl_step2.enrollment_date and #etl_step2.termination_date
    group by #etl_step2.dim_mms_membership_key, #etl_step2.enrollment_date

    /*update result table with new max term dates*/
    update #etl_step2
        set termination_date = #etl_step3.termination_date,
           dv_load_date_time = #etl_step3.dv_load_date_time,
           dv_batch_id = #etl_step3.dv_batch_id
        from #etl_step3
        where #etl_step2.dim_mms_membership_key = #etl_step3.dim_mms_membership_key and #etl_step2.enrollment_date = #etl_step3.enrollment_date

     /*this is required because complex deletes are not supported*/
     /*removes records for which the update above covers the enrollment period*/
    update #etl_step1
        set enrollment_date = null,
           termination_date = null
        from #etl_step2
        where #etl_step1.dim_mms_membership_key = #etl_step2.dim_mms_membership_key and #etl_step1.enrollment_date >= #etl_step2.enrollment_date and #etl_step1.termination_date <= #etl_step2.termination_date

     /*remove processed records*/
delete from #etl_step1 where enrollment_date is null and termination_date is null
end
/*end of the corporate_membership_flag, reimbursement "crunching", results held in #etl_step2*/

if object_id('tempdb..#snapshot_first_in_key_series') is not null drop table #snapshot_first_in_key_series
create table #snapshot_first_in_key_series with (distribution = hash(bk_hash)) as
select h1.bk_hash,
       h1.d_mms_membership_snapshot_history_id,
       h1.effective_date_time,
       h1.updated_date_time,
       case when dateadd(hh,23,dateadd(dd,-1,convert(datetime,convert(varchar,h1.dv_inserted_date_time,110),110))) < isnull(h2.dv_inserted_date_time,'dec 31, 9999') 
                 then h1.effective_date_time
            else dateadd(hh,23,dateadd(dd,-1,convert(datetime,convert(varchar,h1.dv_inserted_date_time,110),110)))
        end calc_effective_date_time
from d_mms_membership_snapshot_history h1
left join d_mms_membership_history h2 on h1.bk_hash = h2.bk_hash and h2.dv_first_in_key_series = 1
where h1.dv_first_in_key_series = 1

if object_id('tempdb..#membership_first_in_key_series') is not null drop table #membership_first_in_key_series
create table #membership_first_in_key_series with (distribution = hash(bk_hash)) as
select m.bk_hash,
        m.d_mms_membership_history_id,
        case when ss.effective_date_time = ss.calc_effective_date_time /*if snapshot is the first record*/
                and m.updated_date_time > isnull(ss.updated_date_time,'jan 1, 1763')  /*and membership is "newer"*/
                    then m.updated_date_time
            else m.effective_date_time 
        end calc_effective_date_time
from d_mms_membership_history m
left join #snapshot_first_in_key_series ss on m.bk_hash = ss.bk_hash /*already dv_first_in_key_series*/
where m.dv_first_in_key_series = 1

/*this is required to adjust effective_date_times on dv_first_in_key_series records*/
if object_id('tempdb..#d_mms_membership_snapshot_history') is not null drop table #d_mms_membership_snapshot_history
create table #d_mms_membership_snapshot_history with (distribution = hash(bk_hash)) as
select h1.d_mms_membership_snapshot_history_id,
       h1.bk_hash,
       h1.dim_mms_membership_key,
       h1.membership_id,
       isnull(sfi.calc_effective_date_time,h1.effective_date_time) effective_date_time,
       h1.expiration_date_time,
       h1.advisor_employee_id,
       h1.club_id,
       h1.company_id,
       h1.created_date_time,
       h1.crm_opportunity_id,
       h1.current_price,
       h1.dim_crm_opportunity_key,
       h1.dim_mms_company_key,
       h1.dim_mms_membership_type_key,
       h1.dv_first_in_key_series,
       h1.eft_option_dim_description_key,
       h1.enrollment_type_dim_description_key,
       h1.home_dim_club_key,
       h1.membership_activation_date,
       h1.membership_cancellation_request_date,
       h1.membership_created_date_time,
       h1.membership_created_dim_date_key,
       h1.membership_expiration_date,
       h1.membership_source_dim_description_key,
       h1.membership_status_dim_description_key,
       h1.membership_type_id,
       h1.non_payment_termination_flag,
       h1.original_sales_dim_employee_key,
       h1.prior_plus_dim_membership_type_key,
       h1.prior_plus_membership_type_id,
       h1.prior_plus_price,
       h1.termination_reason_club_type_dim_description_key,
       h1.termination_reason_dim_description_key,
       h1.updated_date_time,
       h1.val_eft_option_id,
       h1.val_enrollment_type_id,
       h1.val_membership_source_id,
       h1.val_membership_status_id,
       h1.val_termination_reason_club_type_id,
       h1.val_termination_reason_id,
	   h1.undiscounted_price,     /* Added for user story UDW-10242 */
	   h1.prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
       h1.dv_load_date_time,
       h1.dv_load_end_date_time,
       h1.dv_batch_id
from d_mms_membership_snapshot_history h1 
left join #snapshot_first_in_key_series sfi on h1.bk_hash = sfi.bk_hash and h1.d_mms_membership_snapshot_history_id = sfi.d_mms_membership_snapshot_history_id


/*this is required to adjust effective_date_times on dv_first_in_key_series records*/
if object_id('tempdb..#d_mms_membership_history') is not null drop table #d_mms_membership_history
create table #d_mms_membership_history with (distribution = hash(bk_hash)) as
select h1.d_mms_membership_history_id,
       h1.bk_hash,
       h1.dim_mms_membership_key,
       h1.membership_id,
       isnull(mfi.calc_effective_date_time,h1.effective_date_time) effective_date_time,
       h1.expiration_date_time,
       h1.advisor_employee_id,
       h1.club_id,
       h1.company_id,
       h1.created_date_time,
       h1.crm_opportunity_id,
       h1.current_price,
       h1.dim_crm_opportunity_key,
       h1.dim_mms_company_key,
       h1.dim_mms_membership_type_key,
       h1.dv_first_in_key_series,
       h1.eft_option_dim_description_key,
       h1.enrollment_type_dim_description_key,
       h1.home_dim_club_key,
       h1.membership_activation_date,
       h1.membership_cancellation_request_date,
       h1.membership_created_date_time,
       h1.membership_created_dim_date_key,
       h1.membership_expiration_date,
       h1.membership_source_dim_description_key,
       h1.membership_status_dim_description_key,
       h1.membership_type_id,
       h1.non_payment_termination_flag,
       h1.original_sales_dim_employee_key,
       h1.prior_plus_dim_membership_type_key,
       h1.prior_plus_membership_type_id,
       h1.prior_plus_price,
       h1.termination_reason_club_type_dim_description_key,
       h1.termination_reason_dim_description_key,
       h1.updated_date_time,
       h1.val_eft_option_id,
       h1.val_enrollment_type_id,
       h1.val_membership_source_id,
       h1.val_membership_status_id,
       h1.val_termination_reason_club_type_id,
       h1.val_termination_reason_id,
	   h1.undiscounted_price,     /* Added for user story UDW-10242 */
	   h1.prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
       h1.dv_load_date_time,
       h1.dv_load_end_date_time,
       h1.dv_batch_id
from d_mms_membership_history h1 
left join #membership_first_in_key_series mfi on h1.bk_hash = mfi.bk_hash and h1.d_mms_membership_history_id = mfi.d_mms_membership_history_id

/*gather all potential effective_date_times*/
if object_id('tempdb..#etl_step4') is not null drop table #etl_step4
create table #etl_step4 with (distribution = hash(bk_hash)) as
    select 
	x.bk_hash, 
	effective_date_time,
	rank() over (partition by x.bk_hash order by effective_date_time) r
    from 
		( select bk_hash,effective_date_time
			from #d_mms_membership_snapshot_history
			/*where dv_batch_id >= @load_dv_batch_id*/
			group by bk_hash,effective_date_time
	
			union
	
			select bk_hash,effective_date_time
			from #d_mms_membership_history
			/*where dv_batch_id >= @load_dv_batch_id*/
			group by bk_hash,effective_date_time
	
			union
	
			select dim_mms_membership_key,enrollment_date /*records were enrolled*/
			from #etl_step2
	
			union
	
			select dim_mms_membership_key,termination_date /*records enrollment ended*/
			from #etl_step2
			where termination_date < getdate()
		) x


if object_id('tempdb..#etl_step5') is not null drop table #etl_step5
create table #etl_step5 with(distribution = hash(bk_hash)) as
    select 
	etl_step4_1.bk_hash, 
	etl_step4_1.effective_date_time, 
	isnull(etl_step4_2.effective_date_time,'dec 31, 9999') expiration_date_time
    from #etl_step4 etl_step4_1
    left join #etl_step4 etl_step4_2 on etl_step4_1.bk_hash = etl_step4_2.bk_hash and etl_step4_1.r+1 = etl_step4_2.r


if object_id('tempdb..#etl_step6') is not null drop table #etl_step6
create table dbo.#etl_step6 with (distribution = hash(dim_mms_membership_key)) as 
	 select		v_dim_mms_unique_membership_attribute.dim_mms_membership_key,
				d_mms_sales_promotion.val_sales_reporting_category_id,
				d_mms_sales_promotion.dv_load_date_time,
				d_mms_sales_promotion.dv_load_end_date_time,
				d_mms_sales_promotion.dv_batch_id
	from 
		dbo.v_dim_mms_unique_membership_attribute
	left join 
		dbo.d_mms_sales_promotion
			 on v_dim_mms_unique_membership_attribute.membership_attribute_value = d_mms_sales_promotion.sales_promotion_id
	where 
		v_dim_mms_unique_membership_attribute.val_membership_attribute_type_id  = 3 /*- /*/*changed the column_name ref_val_membership_attribute_type_id to val_membership_attribute_type_id for UDW-10675*/*/*/
		and (d_mms_sales_promotion.dv_batch_id > @max_dv_batch_id or d_mms_sales_promotion.dv_batch_id = @current_dv_batch_id )

/*----/* Added for user story UDW-11816 */----------*/
if object_id('tempdb..#sales_transaction_item') is not null drop table #sales_transaction_item
create table dbo.#sales_transaction_item with (distribution = hash(dim_mms_membership_key)) as 
 	select 	fact_mms_sales_transaction_item.dim_mms_membership_key, 
			fact_mms_sales_transaction_item.sales_channel_dim_description_key
	from dbo.fact_mms_sales_transaction_item
	where dim_mms_product_key = (select dim_mms_product_key from dbo.dim_mms_product where product_id = 88)
			and fact_mms_sales_transaction_item.active_transaction_flag = 'Y'
			and fact_mms_sales_transaction_item.membership_charge_flag = 'Y'


if object_id('tempdb..#etl_step7') is not null drop table #etl_step7
create table #etl_step7 with (distribution = hash(dim_mms_membership_key)) as
    select 
	#etl_step5.bk_hash dim_mms_membership_key,
    h_mms_membership.membership_id,
	/*#etl_step6.sales_reporting_category_description sales_reporting_category_description, /*/*addition of column sales_reporting_category_description for UDW-9694*/*/*/
	case when r_mms_val_sales_reporting_category.description is null then isnull(dim_mms_membership_type.attribute_dssr_group_description , '')
         else r_mms_val_sales_reporting_category.description
         end sales_reporting_category_description,

    #etl_step5.effective_date_time, 
    #etl_step5.expiration_date_time,
    isnull(d_mms_membership_history.advisor_employee_id, d_mms_membership_snapshot_history.advisor_employee_id) advisor_employee_id,
    isnull(d_mms_membership_history.club_id, d_mms_membership_snapshot_history.club_id) club_id,
    isnull(d_mms_membership_history.company_id, d_mms_membership_snapshot_history.company_id) company_id,
    case when (etl_step2.dim_mms_membership_key is not null or d_mms_membership_history.company_id is not null or d_mms_membership_snapshot_history.company_id is not null) 
	     then 'Y' else 'N' end corporate_membership_flag,
    isnull(d_mms_membership_snapshot_history.created_date_time,d_mms_membership_history.created_date_time) created_date_time,
    isnull(d_mms_membership_snapshot_history.crm_opportunity_id,d_mms_membership_history.crm_opportunity_id) crm_opportunity_id,
    isnull(d_mms_membership_snapshot_history.current_price,d_mms_membership_history.current_price) current_price,
    isnull(d_mms_membership_snapshot_history.dim_crm_opportunity_key,d_mms_membership_history.dim_crm_opportunity_key) dim_crm_opportunity_key,
    isnull(d_mms_membership_snapshot_history.dim_mms_company_key,d_mms_membership_history.dim_mms_company_key) dim_mms_company_key,
    isnull(d_mms_membership_snapshot_history.dim_mms_membership_type_key,d_mms_membership_history.dim_mms_membership_type_key) dim_mms_membership_type_key,
    isnull(d_mms_membership_snapshot_history.eft_option_dim_description_key,d_mms_membership_history.eft_option_dim_description_key) eft_option_dim_description_key,
    isnull(d_mms_membership_snapshot_history.enrollment_type_dim_description_key,d_mms_membership_history.enrollment_type_dim_description_key) enrollment_type_dim_description_key,
    isnull(d_mms_membership_snapshot_history.home_dim_club_key,d_mms_membership_history.home_dim_club_key) home_dim_club_key,
    isnull(d_mms_membership_snapshot_history.membership_activation_date,d_mms_membership_history.membership_activation_date) membership_activation_date,
    isnull(d_mms_membership_snapshot_history.membership_cancellation_request_date,d_mms_membership_history.membership_cancellation_request_date) membership_cancellation_request_date,
    isnull(d_mms_membership_snapshot_history.membership_created_date_time,d_mms_membership_history.membership_created_date_time) membership_created_date_time,
    isnull(d_mms_membership_snapshot_history.membership_created_dim_date_key,d_mms_membership_history.membership_created_dim_date_key) membership_created_dim_date_key,
    isnull(d_mms_membership_snapshot_history.membership_expiration_date,d_mms_membership_history.membership_expiration_date) membership_expiration_date,
    isnull(d_mms_membership_snapshot_history.membership_source_dim_description_key,d_mms_membership_history.membership_source_dim_description_key) membership_source_dim_description_key,
    isnull(d_mms_membership_snapshot_history.membership_status_dim_description_key,d_mms_membership_history.membership_status_dim_description_key) membership_status_dim_description_key,
    isnull(d_mms_membership_snapshot_history.membership_type_id,d_mms_membership_history.membership_type_id) membership_type_id,
    isnull(d_mms_membership_snapshot_history.non_payment_termination_flag,d_mms_membership_history.non_payment_termination_flag) non_payment_termination_flag,
    isnull(d_mms_membership_snapshot_history.original_sales_dim_employee_key,d_mms_membership_history.original_sales_dim_employee_key) original_sales_dim_employee_key,
    isnull(d_mms_membership_snapshot_history.prior_plus_dim_membership_type_key,d_mms_membership_history.prior_plus_dim_membership_type_key) prior_plus_dim_membership_type_key,
    isnull(d_mms_membership_snapshot_history.prior_plus_membership_type_id,d_mms_membership_history.prior_plus_membership_type_id) prior_plus_membership_type_id,
    isnull(d_mms_membership_snapshot_history.prior_plus_price,d_mms_membership_history.prior_plus_price) prior_plus_price,
    isnull(d_mms_membership_snapshot_history.termination_reason_club_type_dim_description_key,d_mms_membership_history.termination_reason_club_type_dim_description_key) termination_reason_club_type_dim_description_key,
    isnull(d_mms_membership_snapshot_history.termination_reason_dim_description_key,d_mms_membership_history.termination_reason_dim_description_key) termination_reason_dim_description_key,
    isnull(d_mms_membership_snapshot_history.val_eft_option_id,d_mms_membership_history.val_eft_option_id) val_eft_option_id,
    isnull(d_mms_membership_snapshot_history.val_enrollment_type_id,d_mms_membership_history.val_enrollment_type_id) val_enrollment_type_id,
    isnull(d_mms_membership_snapshot_history.val_membership_source_id,d_mms_membership_history.val_membership_source_id) val_membership_source_id,
    isnull(d_mms_membership_snapshot_history.val_membership_status_id,d_mms_membership_history.val_membership_status_id) val_membership_status_id,
    isnull(d_mms_membership_snapshot_history.val_termination_reason_club_type_id,d_mms_membership_history.val_termination_reason_club_type_id) val_termination_reason_club_type_id,
    isnull(d_mms_membership_snapshot_history.val_termination_reason_id,d_mms_membership_history.val_termination_reason_id) val_termination_reason_id,
	isnull(d_mms_membership_snapshot_history.undiscounted_price,d_mms_membership_history.undiscounted_price) undiscounted_price,   /* Added for user story UDW-10242 */
	isnull(d_mms_membership_snapshot_history.prior_plus_undiscounted_price,d_mms_membership_history.prior_plus_undiscounted_price) prior_plus_undiscounted_price,   /* Added for user story UDW-10242 */
	case when (d_mms_membership_snapshot_history.val_membership_source_id = 6 or d_mms_membership_history.val_membership_source_id = 6 ) then 'mms_sales_channel_special_employee_-4'  
	     when #sales_transaction_item.dim_mms_membership_key is not null then #sales_transaction_item.sales_channel_dim_description_key
		 else 'mms_sales_channel_mms_default' end membership_sales_channel_dim_description_key, /* Added for user story UDW-11816 */
		 
	case when d_mms_membership_history.dv_load_date_time >= isnull(d_mms_membership_snapshot_history.dv_load_date_time,'Jan 1, 1753') 
	     and d_mms_membership_history.dv_load_date_time >= isnull(etl_step2.dv_load_date_time,'Jan 1, 1753')
         then d_mms_membership_history.dv_load_date_time
         when d_mms_membership_snapshot_history.dv_load_date_time >= isnull(etl_step2.dv_load_date_time,'Jan 1, 1753')
         then d_mms_membership_snapshot_history.dv_load_date_time
         else isnull(etl_step2.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
    case when d_mms_membership_history.dv_load_end_date_time >= isnull(d_mms_membership_snapshot_history.dv_load_end_date_time ,'Jan 1, 1753') 
	     and d_mms_membership_history.dv_load_end_date_time  >= isnull(etl_step2.dv_load_end_date_time ,'Jan 1, 1753')
         then d_mms_membership_history.dv_load_end_date_time 
         when d_mms_membership_snapshot_history.dv_load_end_date_time  >= isnull(etl_step2.dv_load_end_date_time ,'Jan 1, 1753')
         then d_mms_membership_snapshot_history.dv_load_end_date_time 
         else isnull(etl_step2.dv_load_end_date_time ,'Jan 1, 1753') end dv_load_end_date_time,
    case when d_mms_membership_history.dv_batch_id >= isnull(d_mms_membership_snapshot_history.dv_batch_id,-1) 
	     and d_mms_membership_history.dv_batch_id >= isnull(etl_step2.dv_batch_id,-1)
         then d_mms_membership_history.dv_batch_id
         when d_mms_membership_snapshot_history.dv_batch_id >= isnull(etl_step2.dv_batch_id,-1)
         then d_mms_membership_snapshot_history.dv_batch_id
         else isnull(etl_step2.dv_batch_id,-1) end dv_batch_id
	from #etl_step5  /*29877144*/
	join h_mms_membership
		on #etl_step5.bk_hash = h_mms_membership.bk_hash
  
	left join #d_mms_membership_history d_mms_membership_history  /*10844582*/
		on #etl_step5.bk_hash = d_mms_membership_history.bk_hash
		and d_mms_membership_history.effective_date_time <= #etl_step5.effective_date_time
		and d_mms_membership_history.expiration_date_time >= #etl_step5.expiration_date_time
 
	left join #d_mms_membership_snapshot_history d_mms_membership_snapshot_history  /*27476569*/
		on #etl_step5.bk_hash = d_mms_membership_snapshot_history.bk_hash
		and d_mms_membership_snapshot_history.effective_date_time <= #etl_step5.effective_date_time
		and d_mms_membership_snapshot_history.expiration_date_time >= #etl_step5.expiration_date_time
 
	left join #etl_step2 etl_step2
		on #etl_step5.bk_hash = etl_step2.dim_mms_membership_key
		and etl_step2.enrollment_date <= #etl_step5.effective_date_time
		and etl_step2.termination_date >= #etl_step5.expiration_date_time
    
	/*Adding below join for UDW-9694 i.e. addition of column sales_reporting_category_description*/

	left join d_mms_membership
		on h_mms_membership.bk_hash = d_mms_membership.dim_mms_membership_key

	left join dim_mms_membership_type
     on d_mms_membership.dim_mms_membership_type_key = dim_mms_membership_type.dim_mms_membership_type_key
	
	
	left join #etl_step6  
    on d_mms_membership.dim_mms_membership_key = #etl_step6.dim_mms_membership_key
	
	left join dbo.r_mms_val_sales_reporting_category
    on #etl_step6.val_sales_reporting_category_id = r_mms_val_sales_reporting_category.val_sales_reporting_category_id
    and r_mms_val_sales_reporting_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
	
	left join #sales_transaction_item
	on #sales_transaction_item.dim_mms_membership_key = #etl_step5.bk_hash
	
	

delete from #etl_step7 where effective_date_time = expiration_date_time

BEGIN TRAN

DELETE dbo.dim_mms_membership_history
WHERE dim_mms_membership_key IN 
	(
		SELECT dim_mms_membership_key
		FROM dbo.#etl_step7
	)
insert into dim_mms_membership_history 
	(
    dim_mms_membership_key,
    membership_id,
    sales_reporting_category_description,
    effective_date_time,
    expiration_date_time,
    advisor_employee_id,
    club_id,
    company_id,
    corporate_membership_flag,
    created_date_time,
    crm_opportunity_id,
    current_price,
    dim_crm_opportunity_key,
    dim_mms_company_key,
    dim_mms_membership_type_key,
    eft_option_dim_description_key,
    enrollment_type_dim_description_key,
    home_dim_club_key,
    membership_activation_date,
    membership_cancellation_request_date,
    membership_created_date_time,
    membership_created_dim_date_key,
    membership_expiration_date,
    membership_source_dim_description_key,
    membership_status_dim_description_key,
    membership_type_id,
    non_payment_termination_flag,
    original_sales_dim_employee_key,
    prior_plus_dim_membership_type_key,
    prior_plus_membership_type_id,
    prior_plus_price,
    termination_reason_club_type_dim_description_key,
    termination_reason_dim_description_key,
    val_eft_option_id,
    val_enrollment_type_id,
    val_membership_source_id,
    val_membership_status_id,
    val_termination_reason_club_type_id,
    val_termination_reason_id,
	undiscounted_price,
	prior_plus_undiscounted_price,
	membership_sales_channel_dim_description_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user
	)

select dim_mms_membership_key,
    membership_id,
	sales_reporting_category_description,
    effective_date_time, 
    expiration_date_time,
    advisor_employee_id,
    club_id,
    company_id,
    corporate_membership_flag,
    created_date_time,
    crm_opportunity_id,
    current_price,
    dim_crm_opportunity_key,
    dim_mms_company_key,
    dim_mms_membership_type_key,
    eft_option_dim_description_key,
    enrollment_type_dim_description_key,
    home_dim_club_key,
    membership_activation_date,
    membership_cancellation_request_date,
    membership_created_date_time,
    membership_created_dim_date_key,
    membership_expiration_date,
    membership_source_dim_description_key,
    membership_status_dim_description_key,
    membership_type_id,
    non_payment_termination_flag,
    original_sales_dim_employee_key,
    prior_plus_dim_membership_type_key,
    prior_plus_membership_type_id,
    prior_plus_price,
    termination_reason_club_type_dim_description_key,
    termination_reason_dim_description_key,
    val_eft_option_id,
    val_enrollment_type_id,
    val_membership_source_id,
    val_membership_status_id,
    val_termination_reason_club_type_id,
    val_termination_reason_id,
	undiscounted_price,
	prior_plus_undiscounted_price,
	membership_sales_channel_dim_description_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    getdate(),
    suser_sname() 
from #etl_step7
 
COMMIT TRAN

END
