CREATE PROC [dbo].[proc_d_spabiz_customer] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_customer)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_customer_insert') is not null drop table #p_spabiz_customer_insert
create table dbo.#p_spabiz_customer_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_customer.p_spabiz_customer_id,
       p_spabiz_customer.bk_hash
  from dbo.p_spabiz_customer
 where p_spabiz_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_customer.dv_batch_id > @max_dv_batch_id
        or p_spabiz_customer.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_customer.bk_hash,
       p_spabiz_customer.bk_hash dim_spabiz_customer_key,
       p_spabiz_customer.customer_id customer_id,
       l_spabiz_customer.store_number store_number,
       case when s_spabiz_customer.city is null then ''
            else s_spabiz_customer.city
        end address_city,
       case when s_spabiz_customer.country is null then ''
            else s_spabiz_customer.country
        end address_country,
       case when s_spabiz_customer.address_1 is null then ''
            else s_spabiz_customer.address_1
        end address_line_1,
       case when s_spabiz_customer.address_2 is null then ''
            else s_spabiz_customer.address_2
        end address_line_2,
       case when s_spabiz_customer.zip is null then ''
            else s_spabiz_customer.zip
        end address_postal_code,
       case when s_spabiz_customer.state is null then ''
            else s_spabiz_customer.state
        end address_state_or_province,
       case when s_spabiz_customer.allergies is null then ''
            else s_spabiz_customer.allergies
        end allergies,
       s_spabiz_customer.balance balance,
       s_spabiz_customer.call_days call_days,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.email_ok = 1 then 'Y'
            else 'N'
        end communicate_via_email_flag,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.mail_ok = 0 then 'Y'
            else 'N'
        end communicate_via_mail_flag,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_customer.created_date = convert(date, '18991230', 112) then null
            else s_spabiz_customer.created_date
        end created_date_time,
       s_spabiz_customer.credit_limit credit_limit,
       's_spabiz_customer.sex_' + convert(varchar,convert(int,s_spabiz_customer.sex)) customer_type_dim_description_key,
       convert(int,s_spabiz_customer.sex) customer_type_id,
       case when s_spabiz_customer.b_day is null then ''
            else s_spabiz_customer.b_day
        end date_of_birth,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_customer.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_customer.delete_date
        end deleted_date_time,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.customer_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then p_spabiz_customer.bk_hash
            when l_spabiz_customer.member_id is null then '-998'
            when l_spabiz_customer.member_id = '0' then '-998'
       	 when l_spabiz_customer.member_id = '' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_customer.member_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_member_key,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then p_spabiz_customer.bk_hash
            when l_spabiz_customer.membership_id is null then '-998'
            when l_spabiz_customer.membership_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_customer.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then p_spabiz_customer.bk_hash
            when l_spabiz_customer.store_number is null then '-998'
            when l_spabiz_customer.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_customer.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.do_not_charge_tax = 1 then 'Y'
            else 'N'
        end do_not_charge_tax_flag,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.do_not_print_note = 1 then 'Y'
            else 'N'
        end do_not_print_note_flag,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_customer.edit_time = convert(date, '18991230', 112) then null
            else s_spabiz_customer.edit_time
        end edit_date_time,
       case when s_spabiz_customer.email is null then ''
            else s_spabiz_customer.email
        end email,
       case when s_spabiz_customer.employer is null then ''
            else s_spabiz_customer.employer
        end employer,
       case when s_spabiz_customer.f_l_name is null then ''
            else s_spabiz_customer.f_l_name
        end first_initial_last_name,
       isnull(upper(left(s_spabiz_customer.first_name,1))+lower(substring(s_spabiz_customer.first_name,2,len(s_spabiz_customer.first_name))),'') first_name,
       s_spabiz_customer.first_visit first_visit_date_time,
       case when s_spabiz_customer.sex = 1 then 'F'
            when s_spabiz_customer.sex = 2 then 'M'
            else 'U' end gender_abbreviation,
       case when s_spabiz_customer.tel_home is null then ''
            else s_spabiz_customer.tel_home
        end home_phone_number,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_customer.last_ap_date = convert(date, '18991230', 112) then null
            else s_spabiz_customer.last_ap_date
        end last_appointment_date_time,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_customer.last_called = convert(date, '18991230', 112) then null
            else s_spabiz_customer.last_called
        end last_called_date_time,
       case when s_spabiz_customer.last_name is null then ''
            else s_spabiz_customer.last_name
        end last_name,
       s_spabiz_customer.last_date last_ticket_processed_date_time,
       's_spabiz_customer.marital_' + convert(varchar,s_spabiz_customer.marital) marital_status_dim_description_key,
       s_spabiz_customer.marital marital_status_id,
       case when s_spabiz_customer.medication is null then ''
            else s_spabiz_customer.medication
        end medication,
       case when l_spabiz_customer.member_id is null or l_spabiz_customer.member_id = 0 then '' else l_spabiz_customer.member_id end member_id,
       case when l_spabiz_customer.membership_id is null then '0'
            else l_spabiz_customer.membership_id
        end membership_id,
       case when s_spabiz_customer.middle_name is null then ''
            else s_spabiz_customer.middle_name
        end middle_name,
       case when s_spabiz_customer.tel_mobil is null then ''
            else s_spabiz_customer.tel_mobil
        end mobile_phone_number,
       case when s_spabiz_customer.note is null then ''
            else s_spabiz_customer.note
        end note,
       case when s_spabiz_customer.note_1 is null then ''
            else s_spabiz_customer.note_1
        end note_1,
       case when s_spabiz_customer.occupation is null then ''
            else s_spabiz_customer.occupation
        end occupation,
       case when s_spabiz_customer.tel_pager is null then ''
            else s_spabiz_customer.tel_pager
        end pager_number,
       's_spabiz_customer.tel_which_' + convert(varchar,convert(int,s_spabiz_customer.tel_which)) preferred_contact_type_dim_description_key,
       convert(int,s_spabiz_customer.tel_which) preferred_contact_type_id,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then p_spabiz_customer.bk_hash
            when l_spabiz_customer.primary_staff_id is null then '-998'
            when l_spabiz_customer.primary_staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_customer.primary_staff_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_customer.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end primary_dim_spabiz_staff_key,
       case when s_spabiz_customer.quick_id is null then ''
            else s_spabiz_customer.quick_id
        end quick_id,
       s_spabiz_customer.service_visits service_visits,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.show_note = 1 then 'Y'
            else 'N'
        end show_note_flag,
       case when s_spabiz_customer.title is null then ''
            else s_spabiz_customer.title
        end title,
       case when s_spabiz_customer.total_late is null then 0
            else s_spabiz_customer.total_late
        end total_late_show,
       case when s_spabiz_customer.total_no_show is null then 0
            else s_spabiz_customer.total_no_show
        end total_no_show,
       case when s_spabiz_customer.total_product is null then 0
            else s_spabiz_customer.total_product
        end total_products_purchased,
       case when s_spabiz_customer.total_service is null then 0
            else s_spabiz_customer.total_service
        end total_services_purchased,
       case when s_spabiz_customer.total_visits is null then 0
            else s_spabiz_customer.total_visits
        end total_visits,
       case when p_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_customer.urgent = 1 then 'Y'
            else 'N'
        end urgent_meesage_flag,
       case when s_spabiz_customer.tel_work_fax is null then ''
            else s_spabiz_customer.tel_work_fax
        end work_fax,
       case when s_spabiz_customer.tel_work_ext is null then ''
            else s_spabiz_customer.tel_work_ext
        end work_phone_extension,
       case when s_spabiz_customer.tel_work is null then ''
            else s_spabiz_customer.tel_work
        end work_phone_number,
       case when s_spabiz_customer.ytd_product is null then 0
            else s_spabiz_customer.ytd_product
        end ytd_spent_on_products,
       case when s_spabiz_customer.ytd_service is null then 0
            else s_spabiz_customer.ytd_service
        end ytd_spent_on_services,
       isnull(h_spabiz_customer.dv_deleted,0) dv_deleted,
       p_spabiz_customer.p_spabiz_customer_id,
       p_spabiz_customer.dv_batch_id,
       p_spabiz_customer.dv_load_date_time,
       p_spabiz_customer.dv_load_end_date_time
  from dbo.h_spabiz_customer
  join dbo.p_spabiz_customer
    on h_spabiz_customer.bk_hash = p_spabiz_customer.bk_hash
  join #p_spabiz_customer_insert
    on p_spabiz_customer.bk_hash = #p_spabiz_customer_insert.bk_hash
   and p_spabiz_customer.p_spabiz_customer_id = #p_spabiz_customer_insert.p_spabiz_customer_id
  join dbo.l_spabiz_customer
    on p_spabiz_customer.bk_hash = l_spabiz_customer.bk_hash
   and p_spabiz_customer.l_spabiz_customer_id = l_spabiz_customer.l_spabiz_customer_id
  join dbo.s_spabiz_customer
    on p_spabiz_customer.bk_hash = s_spabiz_customer.bk_hash
   and p_spabiz_customer.s_spabiz_customer_id = s_spabiz_customer.s_spabiz_customer_id
 where l_spabiz_customer.store_number not in (1,100,999) OR p_spabiz_customer.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_customer
   where d_spabiz_customer.bk_hash in (select bk_hash from #p_spabiz_customer_insert)

  insert dbo.d_spabiz_customer(
             bk_hash,
             dim_spabiz_customer_key,
             customer_id,
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
             communicate_via_mail_flag,
             created_date_time,
             credit_limit,
             customer_type_dim_description_key,
             customer_type_id,
             date_of_birth,
             deleted_date_time,
             deleted_flag,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_spabiz_store_key,
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
             last_ticket_processed_date_time,
             marital_status_dim_description_key,
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
             preferred_contact_type_dim_description_key,
             preferred_contact_type_id,
             primary_dim_spabiz_staff_key,
             quick_id,
             service_visits,
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
             p_spabiz_customer_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_customer_key,
         customer_id,
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
         communicate_via_mail_flag,
         created_date_time,
         credit_limit,
         customer_type_dim_description_key,
         customer_type_id,
         date_of_birth,
         deleted_date_time,
         deleted_flag,
         dim_mms_member_key,
         dim_mms_membership_key,
         dim_spabiz_store_key,
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
         last_ticket_processed_date_time,
         marital_status_dim_description_key,
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
         preferred_contact_type_dim_description_key,
         preferred_contact_type_id,
         primary_dim_spabiz_staff_key,
         quick_id,
         service_visits,
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
         p_spabiz_customer_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_customer)
--Done!
end
