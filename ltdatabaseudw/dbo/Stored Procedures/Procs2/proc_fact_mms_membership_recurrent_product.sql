CREATE PROC [dbo].[proc_fact_mms_membership_recurrent_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

    declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_membership_recurrent_product)
	declare @current_dv_batch_id bigint = @dv_batch_id
	declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


	if object_id('tempdb..#fact_mms_membership_recurrent_product') is not null drop table #fact_mms_membership_recurrent_product
	create table dbo.#fact_mms_membership_recurrent_product with(distribution=hash(fact_mms_membership_recurrent_product_key), location=user_db, heap) as
	select distinct 
		d_mms_membership_recurrent_product.fact_mms_membership_recurrent_product_key fact_mms_membership_recurrent_product_key,  
		d_mms_membership_recurrent_product.membership_recurrent_product_id membership_recurrent_product_id,
		d_mms_membership_recurrent_product.membership_id membership_id,
		d_mms_membership_recurrent_product.dim_mms_member_key dim_mms_member_key,
		d_mms_membership_recurrent_product.dim_mms_product_key dim_mms_product_key,
		d_mms_membership_recurrent_product.dim_club_key dim_club_key,
		d_mms_membership_recurrent_product.activation_dim_date_key activation_dim_date_key,
		d_mms_membership_recurrent_product.cancellation_request_dim_date_key cancellation_request_dim_date_key,
		d_mms_membership_recurrent_product.val_recurrent_product_termination_reason_id val_recurrent_product_termination_reason_id,
		d_mms_membership_recurrent_product.termination_reason_dim_description_key termination_reason_dim_description_key,
		d_mms_membership_recurrent_product.termination_dim_date_key termination_dim_date_key,
		d_mms_membership_recurrent_product.hold_start_dim_date_key hold_start_dim_date_key,
		d_mms_membership_recurrent_product.hold_end_dim_date_key hold_end_dim_date_key, 
		d_mms_membership_recurrent_product.val_recurrent_product_source_id val_recurrent_product_source_id,
		d_mms_membership_recurrent_product.recurrent_product_source_dim_description_key recurrent_product_source_dim_description_key,
		d_mms_membership_recurrent_product.last_assessment_dim_date_key last_assessment_dim_date_key,
		isnull(r_mms_val_assessment_day.assessment_day,1) assessment_day_of_month,
		d_mms_membership_recurrent_product.price price,
		dim_club.local_currency_code original_currency_code,
		d_mms_membership_recurrent_product.inserted_dim_date_key,
		d_mms_membership_recurrent_product.inserted_dim_time_key,
		d_mms_membership_recurrent_product.updated_dim_date_key,
		d_mms_membership_recurrent_product.updated_dim_time_key,
		d_mms_membership_recurrent_product.created_dim_date_key,
		d_mms_membership_recurrent_product.created_dim_time_key,
		d_mms_membership_recurrent_product.utc_created_dim_date_key,
		d_mms_membership_recurrent_product.utc_created_dim_time_key,
		d_mms_membership_recurrent_product.created_date_time_zone,
		d_mms_membership_recurrent_product.last_updated_dim_date_key,
		d_mms_membership_recurrent_product.last_updated_dim_time_key,
		d_mms_membership_recurrent_product.utc_last_updated_dim_date_key,
		d_mms_membership_recurrent_product.utc_last_updated_dim_time_key, 
		d_mms_membership_recurrent_product.last_updated_date_time_zone,
		d_mms_membership_recurrent_product.last_updated_employee_id,
		d_mms_membership_recurrent_product.last_updated_dim_employee_key,
		d_mms_membership_recurrent_product.comments,
		d_mms_membership_recurrent_product.number_of_sessions,
		d_mms_membership_recurrent_product.price_per_session,
		d_mms_membership_recurrent_product.commission_employee_id,
		d_mms_membership_recurrent_product.commission_dim_mms_employee_key,
		d_mms_membership_recurrent_product.sold_not_serviced_flag,
		d_mms_membership_recurrent_product.retail_price,
		d_mms_membership_recurrent_product.retail_price_per_session,
		d_mms_membership_recurrent_product.promotion_code,
		d_mms_membership_recurrent_product.pricing_discount_id,
		d_mms_membership_recurrent_product.dim_mms_pricing_discount_key,
		d_mms_membership_recurrent_product.val_discount_reason_id,
		d_mms_membership_recurrent_product.display_only_flag,
		case when d_mms_membership_recurrent_product.dv_load_date_time >= isnull(r_mms_val_assessment_day.dv_load_date_time,'jan 1, 1753')
		then d_mms_membership_recurrent_product.dv_load_date_time
		else r_mms_val_assessment_day.dv_load_date_time
		end dv_load_date_time,
		'Dec 31, 9999' dv_load_end_date_time,
		case when d_mms_membership_recurrent_product.dv_batch_id >= isnull(r_mms_val_assessment_day.dv_batch_id,-1)
		then d_mms_membership_recurrent_product.dv_batch_id
		else r_mms_val_assessment_day.dv_batch_id
		end dv_batch_id ,
		getdate() dv_inserted_date_time,
		suser_sname() dv_insert_user
	from 
		d_mms_membership_recurrent_product
	left join 
		r_mms_val_assessment_day  r_mms_val_assessment_day   
			on d_mms_membership_recurrent_product.val_assessment_day_id  = r_mms_val_assessment_day.val_assessment_day_id
			and r_mms_val_assessment_day.dv_load_end_date_time = 'dec 31, 9999' 
	left join 
		dim_club
		 on d_mms_membership_recurrent_product.club_id = dim_club.club_id
	where 
		d_mms_membership_recurrent_product.dv_batch_id >= @load_dv_batch_id 


	/* Delete and re-insert as a single transaction*/
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/

	begin tran

		delete dbo.fact_mms_membership_recurrent_product
			where fact_mms_membership_recurrent_product_key in (select fact_mms_membership_recurrent_product_key from dbo.#fact_mms_membership_recurrent_product) 

		insert into fact_mms_membership_recurrent_product
		(
			 fact_mms_membership_recurrent_product_key,
			 membership_recurrent_product_id,
			 membership_id,
			 dim_mms_member_key,
			 dim_mms_product_key,
			 dim_club_key,
			 activation_dim_date_key,
			 cancellation_request_dim_date_key,
			 val_recurrent_product_termination_reason_id,
			 termination_reason_dim_description_key,
			 termination_dim_date_key,
			 hold_start_dim_date_key,
			 hold_end_dim_date_key,
			 val_recurrent_product_source_id,
			 recurrent_product_source_dim_description_key,
			 last_assessment_dim_date_key,
			 assessment_day_of_month,
			 price,
			 original_currency_code,
			 inserted_dim_date_key,
			inserted_dim_time_key,
			updated_dim_date_key,
			updated_dim_time_key,
			created_dim_date_key,
			created_dim_time_key,
			utc_created_dim_date_key,
			utc_created_dim_time_key,
			created_date_time_zone,
			last_updated_dim_date_key,
			last_updated_dim_time_key,
			utc_last_updated_dim_date_key,
			utc_last_updated_dim_time_key, 
			last_updated_date_time_zone,
			last_updated_employee_id,
			last_updated_dim_employee_key,
			comments,
			number_of_sessions,
			price_per_session,
			commission_employee_id,
			commission_dim_mms_employee_key,
			sold_not_serviced_flag,
			retail_price,
			retail_price_per_session,
			promotion_code,
			pricing_discount_id,
			dim_mms_pricing_discount_key,
			val_discount_reason_id,
			display_only_flag,
			 dv_load_date_time,
			 dv_load_end_date_time,
			 dv_batch_id,
			 dv_inserted_date_time,
			 dv_insert_user
		 )
		select 
			fact_mms_membership_recurrent_product_key,
			membership_recurrent_product_id,
			membership_id,
			dim_mms_member_key,
			dim_mms_product_key,
			dim_club_key,
			activation_dim_date_key,
			cancellation_request_dim_date_key,
			val_recurrent_product_termination_reason_id,
			termination_reason_dim_description_key,
			termination_dim_date_key,
			hold_start_dim_date_key,
			hold_end_dim_date_key,
			val_recurrent_product_source_id,
			recurrent_product_source_dim_description_key,
			last_assessment_dim_date_key,
			assessment_day_of_month,
			price,
			original_currency_code,
			inserted_dim_date_key,
			inserted_dim_time_key,
			updated_dim_date_key,
			updated_dim_time_key,
			created_dim_date_key,
			created_dim_time_key,
			utc_created_dim_date_key,
			utc_created_dim_time_key,
			created_date_time_zone,
			last_updated_dim_date_key,
			last_updated_dim_time_key,
			utc_last_updated_dim_date_key,
			utc_last_updated_dim_time_key, 
			last_updated_date_time_zone,
			last_updated_employee_id,
			last_updated_dim_employee_key,
			comments,
			number_of_sessions,
			price_per_session,
			commission_employee_id,
			commission_dim_mms_employee_key,
			sold_not_serviced_flag,
			retail_price,
			retail_price_per_session,
			promotion_code,
			pricing_discount_id,
			dim_mms_pricing_discount_key,
			val_discount_reason_id,
			display_only_flag,
			dv_load_date_time,
			dv_load_end_date_time,
			dv_batch_id,
			dv_inserted_date_time,
			dv_insert_user
		from 
			#fact_mms_membership_recurrent_product

	commit tran

end
