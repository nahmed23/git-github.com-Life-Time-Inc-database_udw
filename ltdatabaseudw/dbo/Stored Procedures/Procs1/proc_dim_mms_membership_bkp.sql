CREATE PROC [dbo].[proc_dim_mms_membership_bkp] @current_dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

--Start!
exec dbo.proc_util_task_status_insert 'proc_dim_mms_membership','proc_dim_mms_membership start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_dim_mms_membership','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.dim_mms_membership

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
--exec dbo.proc_util_task_status_insert 'proc_dim_mms_membership','#p_mms_membership_insert',@current_dv_batch_id
if object_id('tempdb..#p_mms_membership_insert') is not null drop table #p_mms_membership_insert
create table dbo.#p_mms_membership_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_membership.p_mms_membership_id,
       p_mms_membership.membership_id,
       p_mms_membership.bk_hash,
       row_number() over (order by p_mms_membership_id) row_num
  from dbo.p_mms_membership
  join #batch_id
    on p_mms_membership.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_membership.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_membership.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
       
-- calculate all values of the records to be inserted to make the actual update go as fast as possible
  if object_id('tempdb..#MembershipAddress') is not null drop table #MembershipAddress
  create table dbo.#MembershipAddress with (location = user_db, distribution = hash(membership_id)) as

  select #p_mms_membership_insert.membership_id membership_id,
         isnull(s_mms_membership_address.city,'') membership_address_city,
         isnull(s_mms_membership_address.address_line_1,'') membership_address_line_1,
         isnull(s_mms_membership_address.address_line_2,'') membership_address_line_2,
         isnull(s_mms_membership_address.zip,'') membership_address_postal_code,
		 l_mms_membership_address.val_country_id,
		 l_mms_membership_address.val_state_id
    into #MembershipAddress
    from dbo.p_mms_membership_address
    join dbo.s_mms_membership_address
      on p_mms_membership_address.s_mms_membership_address_id = s_mms_membership_address.s_mms_membership_address_id
    join dbo.l_mms_membership_address
      on p_mms_membership_address.l_mms_membership_address_id = l_mms_membership_address.l_mms_membership_address_id    
    join #p_mms_membership_insert
      on l_mms_membership_address.membership_id = #p_mms_membership_insert.membership_id
   where p_mms_membership_address.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
  if object_id('tempdb..#Membership') is not null drop table #Membership
  create table dbo.#Membership with (location = user_db, distribution = hash(membership_id)) as

  select #p_mms_membership_insert.row_num,
         p_mms_membership.bk_hash dim_mms_membership_key,
		 p_mms_membership.membership_id,
         l_mms_membership.crm_opportunity_id crm_opportunity_id,
		 case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.crm_opportunity_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.crm_opportunity_id as varchar(500)),'z#@$k%&P'))),2)             
         end dim_crm_opportunity_key,
         s_mms_membership.current_price current_price,
         case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.company_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.company_id as varchar(500)),'z#@$k%&P'))),2)
         end dim_mms_company_key,
         case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.membership_type_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.membership_type_id as varchar(500)),'z#@$k%&P'))),2)             
         end dim_mms_membership_type_key,
		 l_mms_membership.membership_type_id,
         case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.club_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.club_id as varchar(500)),'z#@$k%&P'))),2)             
         end home_dim_mms_club_key,
         case when s_mms_membership.activation_date >= convert(datetime, '2100.01.01', 102) then convert(datetime, '9999.12.31', 102)
              else s_mms_membership.activation_date
         end membership_activation_date,
         #MembershipAddress.membership_address_city,
         #MembershipAddress.membership_address_line_1,
         #MembershipAddress.membership_address_line_2,
         #MembershipAddress.membership_address_postal_code,
         s_mms_membership.cancellation_request_date membership_cancellation_request_date,
         case when s_mms_membership.expiration_date >= convert(datetime, '2100.01.01', 102) then convert(datetime, '9999.12.31', 102)
              else s_mms_membership.expiration_date
         end membership_expiration_date,
         case when l_mms_membership.val_termination_reason_id in (24,35) then 'Y'
              else 'N'
         end non_payment_termination_flag,
		 case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.advisor_employee_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.advisor_employee_id as varchar(500)),'z#@$k%&P'))),2)             
         end original_sales_dim_team_member_key,
		 case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
              when l_mms_membership.prior_plus_membership_type_id is null then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.prior_plus_membership_type_id as varchar(500)),'z#@$k%&P'))),2)             
         end prior_plus_membership_type_key,
		 l_mms_membership.prior_plus_membership_type_id,
         s_mms_membership.prior_plus_price prior_plus_price,
         isnull(#MembershipAddress.val_country_id, -998) ref_mms_val_country_id,
         isnull(#MembershipAddress.val_state_id, -998) ref_mms_val_state_id,
         s_mms_membership.created_date_time created_date_time,
         isnull(l_mms_membership.val_eft_option_id, -998) ref_mms_val_eft_option_id,
         l_mms_membership.val_membership_source_id ref_mms_val_membership_source_id,
         l_mms_membership.val_membership_status_id ref_mms_val_membership_status_id,
         l_mms_membership.val_termination_reason_id ref_mms_val_termination_reason_id,
         p_mms_membership.p_mms_membership_id p_mms_membership_id,
         p_mms_membership.dv_batch_id,
         p_mms_membership.dv_load_date_time,
         p_mms_membership.dv_load_end_date_time
    into #Membership
    from dbo.p_mms_membership
    join #p_mms_membership_insert
      on p_mms_membership.p_mms_membership_id = #p_mms_membership_insert.p_mms_membership_id
    join dbo.l_mms_membership
      on p_mms_membership.l_mms_membership_id = l_mms_membership.l_mms_membership_id
    join #MembershipAddress
      on p_mms_membership.membership_id = #MembershipAddress.membership_id
    join dbo.s_mms_membership
      on p_mms_membership.s_mms_membership_id = s_mms_membership.s_mms_membership_id
   where p_mms_membership.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
  if object_id('tempdb..#insert') is not null drop table #insert
  create table dbo.#insert with (location = user_db, distribution = hash(membership_id)) as
  
  select #Membership.row_num,
         #Membership.dim_mms_membership_key dim_mms_membership_key,
         #Membership.membership_id,
         isnull(#Membership.created_date_time, d_dim_mms_member.join_date) created_date_time,
         #Membership.crm_opportunity_id,
		 #Membership.dim_crm_opportunity_key,
         #Membership.current_price,
         #Membership.dim_mms_company_key dim_mms_company_key,
         #Membership.membership_type_id,
		 #Membership.dim_mms_membership_type_key,
         isnull(r_mms_val_eft_option.description,'') eft_option,
         #Membership.home_dim_mms_club_key home_dim_mms_club_key,
         #Membership.membership_activation_date,
         #Membership.membership_address_city,
         isnull(r_mms_val_country.abbreviation,'') membership_address_country,
         #Membership.membership_address_line_1,
         #Membership.membership_address_line_2,
         #Membership.membership_address_postal_code,
         isnull(r_mms_val_state.abbreviation,'') membership_address_state_abbreviation,
         #Membership.membership_cancellation_request_date,
         #Membership.membership_expiration_date,
         #Membership.created_date_time membership_record_created_date_time,
         isnull(r_mms_val_membership_source.description,'') membership_source,
         r_mms_val_membership_status.description membership_status,
         dim_mms_membership_type.membership_type,
         case when #Membership.ref_mms_val_termination_reason_id in (21, 41, 42, 59) and dim_mms_membership_type.attribute_dssr_group_description != 'DSSR_Other' then 'Y'
              else 'N'
         end money_back_cancellation_flag,
         #Membership.non_payment_termination_flag,
         #Membership.original_sales_dim_team_member_key,     
         #Membership.prior_plus_membership_type_key,  
         prior_plus_dim_mms_membership_type.membership_type prior_plus_membership_type,
         #Membership.prior_plus_price,            
         r_mms_val_revenue_reporting_category.description r_mms_revenue_reporting_category_description,
         r_mms_val_sales_reporting_category.description r_mms_sales_reporting_category_description,
         isnull(r_mms_val_termination_reason.description,'') termination_reason,
         dim_mms_membership_type.attribute_dssr_group_description dim_mms_membership_type_attribute_dssr_group_description,
         dim_mms_membership_type.attribute_membership_status_summary_group_description dim_mms_membership_type_attribute_membership_status_summary_group_description,
         d_dim_mms_sales_promotion.exclude_from_attrition_reporting_flag dim_mms_sales_promotion_exclude_from_attrition_reporting_flag,
         #Membership.ref_mms_val_membership_source_id,
         #Membership.ref_mms_val_membership_status_id,
         d_dim_mms_sales_promotion.ref_mms_val_revenue_reporting_category_id ref_mms_val_revenue_reporting_category_id,
         d_dim_mms_sales_promotion.ref_mms_val_sales_reporting_category_id ref_mms_val_sales_reporting_category_id,
         #Membership.ref_mms_val_termination_reason_id,
		 #Membership.ref_mms_val_country_id,
		 #Membership.ref_mms_val_eft_option_id,
		 #Membership.ref_mms_val_state_id,
		 #Membership.p_mms_membership_id,
		 #Membership.dv_batch_id,
         #Membership.dv_load_date_time,
         #Membership.dv_load_end_date_time
    into #insert		 
    from #Membership
    left join dbo.dim_mms_membership_type prior_plus_dim_mms_membership_type
      on #Membership.prior_plus_membership_type_id = prior_plus_dim_mms_membership_type.membership_type_id
    left join dbo.dim_mms_membership_type
      on #Membership.membership_type_id = dim_mms_membership_type.membership_type_id
    left join dbo.d_dim_mms_member
      on #Membership.membership_id = d_dim_mms_member.membership_id
     and d_dim_mms_member.ref_mms_val_member_type_id = 1
    left join dbo.v_dim_mms_unique_membership_attribute
      on #Membership.dim_mms_membership_key = v_dim_mms_unique_membership_attribute.dim_mms_membership_key
     and v_dim_mms_unique_membership_attribute.ref_val_membership_attribute_type_id = 3
    left join dbo.d_dim_mms_sales_promotion
      on v_dim_mms_unique_membership_attribute.membership_attribute_value = d_dim_mms_sales_promotion.sales_promotion_id 
    left join dbo.r_mms_val_country
      on #Membership.ref_mms_val_country_id = r_mms_val_country.val_country_id
     and r_mms_val_country.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_eft_option
      on #Membership.ref_mms_val_eft_option_id = r_mms_val_eft_option.val_eft_option_id
     and r_mms_val_eft_option.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_membership_status
      on #Membership.ref_mms_val_membership_status_id = r_mms_val_membership_status.val_membership_status_id
     and r_mms_val_membership_status.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_membership_source
      on #Membership.ref_mms_val_membership_source_id = r_mms_val_membership_source.val_membership_source_id
     and r_mms_val_membership_source.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
     left join dbo.r_mms_val_revenue_reporting_category
      on d_dim_mms_sales_promotion.ref_mms_val_revenue_reporting_category_id = r_mms_val_revenue_reporting_category.val_revenue_reporting_category_id
     and r_mms_val_revenue_reporting_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_sales_reporting_category
      on d_dim_mms_sales_promotion.ref_mms_val_sales_reporting_category_id = r_mms_val_sales_reporting_category.val_sales_reporting_category_id
     and r_mms_val_sales_reporting_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_state
      on #Membership.ref_mms_val_state_id = r_mms_val_state.val_state_id
     and r_mms_val_state.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_termination_reason
      on #Membership.ref_mms_val_termination_reason_id = r_mms_val_termination_reason.val_termination_reason_id
     and r_mms_val_termination_reason.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'dim_mms_membership', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    --exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_membership',@task_description,@current_dv_batch_id

begin tran
  delete dbo.dim_mms_membership
   where dim_mms_membership.dim_mms_membership_key in (select bk_hash from #p_mms_membership_insert where row_num >= @start and row_num < @start+1000000)

  insert dim_mms_membership(
         --dim_mms_membership_id,
		 dim_mms_membership_key,
		 membership_id,
	     attrition_date,
	     created_date_time,
	     crm_opportunity_id,
	     current_price,	  
		 dim_crm_opportunity_key, 
	     dim_mms_company_key,
	     dim_mms_membership_type_key,
	     eft_option,
	     home_dim_mms_club_key,
	     membership_activation_date,
         membership_address_city,
		 membership_address_country,
	     membership_address_line_1,
	     membership_address_line_2,
	     membership_address_postal_code,
		 membership_address_state_abbreviation,
	     membership_cancellation_request_date,
	     membership_expiration_date,
	     membership_source,
         membership_status,
         membership_type,
		 membership_type_id,
         money_back_cancellation_flag,
	     non_payment_termination_flag,
	     original_sales_dim_team_member_key,
	     prior_plus_membership_type_key,
		 prior_plus_membership_type,
	     prior_plus_price,
	     revenue_reporting_category_description,
	     sales_reporting_category_description,
	     termination_reason,
	     ref_mms_val_country_id,
	     ref_mms_val_eft_option_id,
	     ref_mms_val_membership_source_id,
	     ref_mms_val_membership_status_id,
	     ref_mms_val_revenue_reporting_category_id,
         ref_mms_val_sales_reporting_category_id,
	     ref_mms_val_state_id,
         ref_mms_val_termination_reason_id,
         p_mms_membership_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
		 dv_inserted_date_time,
         dv_insert_user)


  select --@start_p_id + row_num,
         dim_mms_membership_key,
         membership_id,
         case when money_back_cancellation_flag = 'Y' then convert(datetime, '9999.12.31', 102)
              when dim_mms_sales_promotion_exclude_from_attrition_reporting_flag = 'Y' then convert(datetime, '9999.12.31', 102)
              when dim_mms_membership_type_attribute_membership_status_summary_group_description is null then convert(datetime, '9999.12.31', 102)
              when dim_mms_membership_type_attribute_membership_status_summary_group_description not in ('Membership Status Summary Group 2 Revenue', 'Membership Status Summary Group 3 Revenue LTHealth') then convert(datetime, '9999.12.31', 102)
              else membership_expiration_date 
	     end attrition_date,
         created_date_time,
         crm_opportunity_id,
         current_price,
		 dim_crm_opportunity_key,
         dim_mms_company_key,
         dim_mms_membership_type_key,
         eft_option,
         home_dim_mms_club_key,
         membership_activation_date,
         membership_address_city,
         membership_address_country,
         membership_address_line_1,
         membership_address_line_2,
         membership_address_postal_code,
         membership_address_state_abbreviation,
         membership_cancellation_request_date,
         membership_expiration_date,
         membership_source,
         membership_status,
         membership_type,
		 membership_type_id,
         money_back_cancellation_flag,
         non_payment_termination_flag,
         original_sales_dim_team_member_key,
         prior_plus_membership_type_key,
         prior_plus_membership_type,
         prior_plus_price,
         case when r_mms_revenue_reporting_category_description is null then isnull(dim_mms_membership_type_attribute_membership_status_summary_group_description, '')
              else r_mms_revenue_reporting_category_description 
		 end revenue_reporting_category_description,
         case when r_mms_sales_reporting_category_description is null then isnull(dim_mms_membership_type_attribute_dssr_group_description , '')
              else r_mms_sales_reporting_category_description 
		 end sales_reporting_category_description,
         termination_reason,
		 ref_mms_val_country_id,
	     ref_mms_val_eft_option_id,
         ref_mms_val_membership_source_id,
         ref_mms_val_membership_status_id,
         ref_mms_val_revenue_reporting_category_id,
         ref_mms_val_sales_reporting_category_id,
		 ref_mms_val_state_id,
         ref_mms_val_termination_reason_id,
		 p_mms_membership_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
		 getdate(),
         suser_sname()
    from #insert
   where row_num >= @start
     and row_num < @start+1000000
  commit tran

  set @start = @start+1000000
  end


--Done!
exec proc_util_task_status_insert 'proc_dim_mms_membership','proc_dim_mms_membership end',@current_dv_batch_id
end

--exec proc_dim_mms_membership -1

--select dim_mms_membership_key,
--home_dim_mms_club_key
--from marketing.v_dim_mms_membership

--select dim_mms_membership_key,
--home_dim_mms_club_key
--from dim_mms_membership


--select *
--  from 

--truncate table dim_mms_membership



delete dv_d_etl_map where target_object = 'dim_mms_membership'
