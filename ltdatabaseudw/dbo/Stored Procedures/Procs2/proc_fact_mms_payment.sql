CREATE PROC [dbo].[proc_fact_mms_payment] @dv_batch_id [varchar](500) AS
 begin
 
 set xact_abort on
 set nocount on
 
    declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_payment)
	declare @current_dv_batch_id bigint = @dv_batch_id
	declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end
 
 
	if object_id('tempdb..#mms_payment') is not null drop table #mms_payment
	create table dbo.#mms_payment with(distribution=hash(fact_mms_payment_key), location=user_db) as 
	SELECT distinct 
		d_mms_payment.payment_id as payment_id,
		d_mms_payment.mms_tran_id as mms_tran_id,
		d_mms_payment.payment_type_dim_description_key as payment_type_dim_description_key,
		d_mms_payment.payment_dim_date_key as payment_dim_date_key,
		d_mms_payment.payment_dim_time_key as payment_dim_time_key,
		d_mms_payment.payment_amount as payment_amount,
		d_mms_payment.tip_amount as tip_amount,
		d_mms_payment.approval_code as approval_code,
		d_mms_payment.val_payment_type_id as val_payment_type_id,
		d_mms_payment.mms_inserted_date_time as mms_inserted_date_time,
		d_mms_mms_tran.dim_club_key as dim_club_key,
		d_mms_mms_tran.sales_entered_dim_employee_key as sales_entered_dim_employee_key,
		d_mms_mms_tran.dim_mms_drawer_activity_key as dim_mms_drawer_activity_key,
		d_mms_mms_tran.dim_mms_member_key as dim_mms_member_key,
		d_mms_mms_tran.dim_mms_membership_key as dim_mms_membership_key,
		d_mms_mms_tran.dim_mms_transaction_reason_key as dim_mms_transaction_reason_key,
		case 
			when 
				d_mms_mms_tran.refund_flag is null 
			then 'N' 
			else d_mms_mms_tran.refund_flag 
		end as refund_flag,
		case 
			when 
				d_mms_mms_tran.pos_flag is null 
			then 'N' 
			else d_mms_mms_tran.pos_flag 
		end as pos_flag,
		case 
			when 
				d_mms_mms_tran.voided_flag is null 
			then 'N' 
			else d_mms_mms_tran.voided_flag 
		end as  voided_flag,
		case 
			when 
				d_mms_mms_tran.val_tran_type_id = 2 
			then 'Y'  
			else 'N' 
		end as payment_flag,
		d_mms_payment.fact_mms_payment_key fact_mms_payment_key,
		case 
			when d_mms_mms_tran.bk_hash in ('-997','-998','-999') then d_mms_mms_tran.bk_hash
 			when d_mms_mms_tran.dim_club_key is null then '-998'
 			else d_mms_mms_tran.dim_club_key 
		end as transaction_reporting_dim_club_key,
		d_mms_mms_tran.refund_flag as automated_refund_flag,
		case 
			when 
				d_mms_payment_refund.payment_id is not null 
			then 'Y' 
			else 'N' 
		end as refunded_flag,
		d_mms_payment_refund.payment_status_dim_description_key,
		case 
			when 
				isnull(d_mms_payment.dv_batch_id,'-1') >= isnull(d_mms_mms_tran.dv_batch_id,'-1')
				and isnull(d_mms_payment.dv_batch_id,'-1') >= isnull(d_mms_payment_refund.dv_batch_id,'-1')
			then isnull(d_mms_payment.dv_batch_id,'-1')  
			when 
				isnull(d_mms_mms_tran.dv_batch_id,'-1') >= isnull(d_mms_payment_refund.dv_batch_id,'-1')
			then isnull(d_mms_mms_tran.dv_batch_id,'-1')  
			else isnull(d_mms_payment_refund.dv_batch_id,'-1')
		end as dv_batch_id,
		case 
			when 
				isnull(d_mms_payment.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_mms_tran.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_mms_payment.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_payment_refund.dv_load_date_time,'Jan 1, 1753')
			then isnull(d_mms_payment.dv_load_date_time,'Jan 1, 1753')  
			when
				isnull(d_mms_mms_tran.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_payment_refund.dv_load_date_time,'Jan 1, 1753')
			then isnull(d_mms_mms_tran.dv_load_date_time,'Jan 1, 1753')  
			else isnull(d_mms_payment_refund.dv_load_date_time,'Jan 1, 1753')
		end as dv_load_date_time
	from 
		d_mms_payment
	join 
		d_mms_mms_tran
			on d_mms_payment.mms_tran_id = d_mms_mms_tran.mms_tran_id
	left join
		d_mms_payment_refund
			on d_mms_payment_refund.fact_mms_payment_key = d_mms_payment.fact_mms_payment_key
	where 
		(
			d_mms_payment.dv_batch_id >= @load_dv_batch_id 
			or d_mms_mms_tran.dv_batch_id >= @load_dv_batch_id 
			or d_mms_payment_refund.dv_batch_id >= @load_dv_batch_id 
		)

	/* Delete and re-insert as a single transaction*/
	 /*  Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
 
	begin tran
 
	delete dbo.fact_mms_payment
		where fact_mms_payment_key in (select fact_mms_payment_key from #mms_payment) 
    
    insert into fact_mms_payment
    (
		fact_mms_payment_key,
		payment_id,
		mms_tran_id,
		payment_type_dim_description_key,
		payment_dim_date_key,
		payment_dim_time_key,
		payment_amount,
		tip_amount,
		approval_code,
		dim_club_key,
		sales_entered_dim_employee_key,
		dim_mms_drawer_activity_key,
		dim_mms_member_key,
		dim_mms_membership_key,
		dim_mms_transaction_reason_key,
		refund_flag,
		pos_flag,
		voided_flag,
		payment_flag,
		transaction_reporting_dim_club_key,
		automated_refund_flag,
		refunded_flag,
		payment_status_dim_description_key,
		dv_load_date_time,
		dv_load_end_date_time,
		dv_batch_id,
		dv_inserted_date_time,
		dv_insert_user
	)
 	select 
		fact_mms_payment_key,
		payment_id,
		mms_tran_id,
		payment_type_dim_description_key,
		payment_dim_date_key,
		payment_dim_time_key,
		payment_amount,
		tip_amount,
		approval_code,
		dim_club_key,
		sales_entered_dim_employee_key,
		dim_mms_drawer_activity_key,
		dim_mms_member_key,
		dim_mms_membership_key,
		dim_mms_transaction_reason_key,
		refund_flag,
		pos_flag,
		voided_flag,
		payment_flag,
		transaction_reporting_dim_club_key,
		automated_refund_flag,
		refunded_flag,
		payment_status_dim_description_key,
		dv_load_date_time,
		'dec 31, 9999',
		dv_batch_id,
		getdate() ,
		suser_sname()
	from #mms_payment	
 		
  commit tran	
end
