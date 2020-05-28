CREATE PROC [dbo].[proc_dim_spabiz_customer] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_spabiz_customer)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(dim_spabiz_customer_key), location=user_db) as  
select d_spabiz_customer.bk_hash dim_spabiz_customer_key,
	d_spabiz_customer.store_number store_number,
    d_spabiz_customer.address_city address_city,
    d_spabiz_customer.address_country address_country,
    d_spabiz_customer.address_line_1 address_line_1,
    d_spabiz_customer.address_line_2 address_line_2,
    d_spabiz_customer.address_postal_code address_postal_code,
    d_spabiz_customer.address_state_or_province address_state_or_province,
    d_spabiz_customer.allergies allergies,
    d_spabiz_customer.balance balance,
    d_spabiz_customer.call_days call_days,
    d_spabiz_customer.communicate_via_email_flag communicate_via_email_flag,
    d_spabiz_customer.created_date_time created_date_time,
    d_spabiz_customer.credit_limit credit_limit,
    d_spabiz_customer.customer_id customer_id,
    d_spabiz_customer.customer_type_id customer_type_id,
    d_spabiz_customer.date_of_birth date_of_birth,
    d_spabiz_customer.deleted_date_time deleted_date_time,
    d_spabiz_customer.deleted_flag deleted_flag,
    d_spabiz_customer.dim_mms_member_key dim_mms_member_key,
    d_spabiz_customer.dim_mms_membership_key dim_mms_membership_key,
    d_spabiz_customer.do_not_charge_tax_flag do_not_charge_tax_flag,
    d_spabiz_customer.do_not_print_note_flag do_not_print_note_flag,
    d_spabiz_customer.edit_date_time edit_date_time,
    d_spabiz_customer.email email,
    d_spabiz_customer.employer employer,
    d_spabiz_customer.first_initial_last_name first_initial_last_name,
    d_spabiz_customer.first_name first_name,
    d_spabiz_customer.first_visit_date_time first_visit_date_time,
    d_spabiz_customer.gender_abbreviation gender_abbreviation,
    d_spabiz_customer.home_phone_number home_phone_number,
    d_spabiz_customer.last_appointment_date_time last_appointment_date_time,
    d_spabiz_customer.last_called_date_time last_called_date_time,
    d_spabiz_customer.last_name last_name,
    d_spabiz_customer.marital_status_id marital_status_id,
    d_spabiz_customer.medication medication,
    d_spabiz_customer.member_id member_id,
    d_spabiz_customer.membership_id membership_id,
    d_spabiz_customer.middle_name middle_name,
    d_spabiz_customer.mobile_phone_number mobile_phone_number,
    d_spabiz_customer.note note,
    d_spabiz_customer.note_1 note_1,
    d_spabiz_customer.occupation occupation,
    d_spabiz_customer.pager_number pager_number,
    d_spabiz_customer.primary_dim_spabiz_staff_key primary_dim_spabiz_staff_key,
    d_spabiz_customer.quick_id quick_id,
    d_spabiz_customer.show_note_flag show_note_flag,
    d_spabiz_customer.title title,
    d_spabiz_customer.total_late_show total_late_show,
    d_spabiz_customer.total_no_show total_no_show,
    d_spabiz_customer.total_products_purchased total_products_purchased,
    d_spabiz_customer.total_services_purchased total_services_purchased,
    d_spabiz_customer.total_visits total_visits,
    d_spabiz_customer.urgent_meesage_flag urgent_meesage_flag,
    d_spabiz_customer.work_fax work_fax,
    d_spabiz_customer.work_phone_extension work_phone_extension,
    d_spabiz_customer.work_phone_number work_phone_number,
    d_spabiz_customer.ytd_spent_on_products ytd_spent_on_products,
    d_spabiz_customer.ytd_spent_on_services ytd_spent_on_services,
	case when isnumeric(custcard.serial_num) = 1 then cast(custcard.serial_num as bigint)
	else d_spabiz_customer.member_id end as customer_member_id,
	case when isnumeric(custcard.serial_num) = 1 then 'Y'
	else 'N' end as cust_card_member_id_flag,
	d_spabiz_customer.dv_batch_id,
    d_spabiz_customer.dv_load_date_time
	from d_spabiz_customer
		left join 
	(
 
			select d_spabiz_cust_card.buy_cust_id,d_spabiz_cust_card.customer_id,max(serial_num) as serial_num,d_spabiz_cust_card_exp_dim_date_key as exp_dim_date_key
			from d_spabiz_cust_card 
			join (
				select distinct cust_card_type_id
				from d_spabiz_cust_card_type
					where name like '%Platinum%'
						or name like '%Onyx%'
						or name like '%Diamond%'
						or name like '%Gold%'
						or name like '%Bronze%'
						or name like '%Lifetime membership%'
				) as cust_card_type 
				on cust_card_type.cust_card_type_id = d_spabiz_cust_card.cust_card_id
				where mem_type = 1
	group by d_spabiz_cust_card.buy_cust_id,d_spabiz_cust_card.customer_id, d_spabiz_cust_card.d_spabiz_cust_card_exp_dim_date_key
union 
			select d_spabiz_cust_card.buy_cust_id,d_spabiz_cust_card.customer_id, d_spabiz_cust_card.serial_num,max(d_spabiz_cust_card_exp_dim_date_key) as exp_dim_date_key
			from d_spabiz_cust_card
			join (
				select  distinct cust_card_type_id
					from d_spabiz_cust_card_type
					where name like '%Platinum%'
							or name like '%Onyx%'
							or name like '%Diamond%'
							or name like '%Gold%'
							or name like '%Bronze%'
							or name like '%Lifetime membership%'
				) as cust_card_type
				on cust_card_type.cust_card_type_id = d_spabiz_cust_card.cust_card_id
				where d_spabiz_cust_card.mem_type = 1
				group by d_spabiz_cust_card.buy_cust_id,d_spabiz_cust_card.customer_id, d_spabiz_cust_card.serial_num

	) custcard
	on custcard.buy_cust_id = d_spabiz_customer.customer_id
where  d_spabiz_customer.dv_batch_id >= @load_dv_batch_id







begin tran
     
delete dbo.dim_spabiz_customer
    where dim_spabiz_customer_key in (select dim_spabiz_customer_key from dbo.#etl_step_1) 
                 
        insert into dim_spabiz_customer
          (      dim_spabiz_customer_key,
          store_number,
          address_city,
          address_country,
          address_line_1,
          address_line_2,
          address_postal_code,
          address_state_or_province,
          allergies,
          balance,
          call_days,
          communicate_via_email_flag,
          created_date_time,
          credit_limit,
          customer_id,
          customer_type_id,
          date_of_birth,
          deleted_date_time,
          deleted_flag,
          dim_mms_member_key,
          dim_mms_membership_key,
          do_not_charge_tax_flag,
          do_not_print_note_flag,
          edit_date_time,
          email,
          employer,
          first_initial_last_name,
          first_name,
          first_visit_date_time,
          gender_abbreviation,
          home_phone_number,
          last_appointment_date_time,
          last_called_date_time,
          last_name,
          marital_status_id,
          medication,
          member_id,
          membership_id,
          middle_name,
          mobile_phone_number,
          note,
          note_1,
          occupation,
          pager_number,
          primary_dim_spabiz_staff_key,
          quick_id,
          show_note_flag,
          title,
          total_late_show,
          total_no_show,
          total_products_purchased,
          total_services_purchased,
          total_visits,
          urgent_meesage_flag,
          work_fax,
          work_phone_extension,
          work_phone_number,
          ytd_spent_on_products,
          ytd_spent_on_services,
		  customer_member_id,	
		  cust_card_member_id_flag,
          dv_batch_id,
          dv_load_date_time,
          dv_load_end_date_time,
          dv_inserted_date_time,
          dv_insert_user )
                     select 
                 dim_spabiz_customer_key,
		  store_number,
          address_city,
          address_country,
          address_line_1,
          address_line_2,
          address_postal_code,
          address_state_or_province,
          allergies,
          balance,
          call_days,
          communicate_via_email_flag,
          created_date_time,
          credit_limit,
          customer_id,
          customer_type_id,
          date_of_birth,
          deleted_date_time,
          deleted_flag,
          dim_mms_member_key,
          dim_mms_membership_key,
          do_not_charge_tax_flag,
          do_not_print_note_flag,
          edit_date_time,
          email,
          employer,
          first_initial_last_name,
          first_name,
          first_visit_date_time,
          gender_abbreviation,
          home_phone_number,
          last_appointment_date_time,
          last_called_date_time,
          last_name,
          marital_status_id,
          medication,
          member_id,
          membership_id,
          middle_name,
          mobile_phone_number,
          note,
          note_1,
          occupation,
          pager_number,
          primary_dim_spabiz_staff_key,
          quick_id,
          show_note_flag,
          title,
          total_late_show,
          total_no_show,
          total_products_purchased,
          total_services_purchased,
          total_visits,
          urgent_meesage_flag,
          work_fax,
          work_phone_extension,
          work_phone_number,
          ytd_spent_on_products,
          ytd_spent_on_services,
		  customer_member_id,	
		  cust_card_member_id_flag,
                 dv_batch_id,
                 dv_load_date_time,
                 'dec 31, 9999',
                 getdate(),
                 suser_sname()
        from #etl_step_1

    commit tran
end
