CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_subscription] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_subscription)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_subscription_insert') is not null drop table #p_crmcloudsync_ltf_subscription_insert
create table dbo.#p_crmcloudsync_ltf_subscription_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_subscription.p_crmcloudsync_ltf_subscription_id,
       p_crmcloudsync_ltf_subscription.bk_hash
  from dbo.p_crmcloudsync_ltf_subscription
 where p_crmcloudsync_ltf_subscription.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_subscription.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_subscription.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_subscription.bk_hash,
       p_crmcloudsync_ltf_subscription.bk_hash dim_crm_ltf_subscription_key,
       p_crmcloudsync_ltf_subscription.ltf_subscription_id ltf_subscription_id,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then l_crmcloudsync_ltf_subscription.bk_hash 
           when l_crmcloudsync_ltf_subscription.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.created_by as varchar(36)),'z#@$k%&P'))),2) end   created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_subscription.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when s_crmcloudsync_ltf_subscription.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscription.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_subscription.created_on created_on,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then  l_crmcloudsync_ltf_subscription.bk_hash
           when  l_crmcloudsync_ltf_subscription.ltf_account_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.ltf_account_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_account_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash 
           when l_crmcloudsync_ltf_subscription.ltf_club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.ltf_club_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_club_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
             when l_crmcloudsync_ltf_subscription.owner_id is null then '-998'
       	         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash   
          when s_crmcloudsync_ltf_subscription.ltf_subscription_number is null or len(s_crmcloudsync_ltf_subscription.ltf_subscription_number)>=10 then '-998'    
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_crmcloudsync_ltf_subscription.ltf_subscription_number as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_membership_key,
       s_crmcloudsync_ltf_subscription.exchange_rate exchange_rate,
       l_crmcloudsync_ltf_subscription.ltf_account_household ltf_account_household,
       isnull(s_crmcloudsync_ltf_subscription.ltf_account_household_name,'') ltf_account_household_name,
       isnull(s_crmcloudsync_ltf_subscription.ltf_account_id_name,'') ltf_account_id_name,
       s_crmcloudsync_ltf_subscription.ltf_activation_date ltf_activation_date,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
        when s_crmcloudsync_ltf_subscription.ltf_activation_date is null then '-998'   when  convert(varchar, s_crmcloudsync_ltf_subscription.ltf_activation_date, 112) > '20991231' then '99991231'
        when convert(varchar, s_crmcloudsync_ltf_subscription.ltf_activation_date, 112)< '19000101' then '19000101'
         else convert(varchar, s_crmcloudsync_ltf_subscription.ltf_activation_date, 112)    end ltf_activation_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.ltf_activation_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.ltf_activation_date,114), 1, 5),':','') end ltf_activation_dim_time_key,
       s_crmcloudsync_ltf_subscription.ltf_attrition_exclusion ltf_attrition_exclusion,
       case when s_crmcloudsync_ltf_subscription.ltf_attrition_exclusion = 1 then 'Y'        else 'N'  end ltf_attrition_exclusion_flag,
       isnull(s_crmcloudsync_ltf_subscription.ltf_attrition_exclusion_name,'') ltf_attrition_exclusion_name,
       s_crmcloudsync_ltf_subscription.ltf_cancellation_date ltf_cancellation_date,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when s_crmcloudsync_ltf_subscription.ltf_cancellation_date is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscription.ltf_cancellation_date, 112)    end ltf_cancellation_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.ltf_cancellation_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.ltf_cancellation_date,114), 1, 5),':','') end ltf_cancellation_dim_time_key,
       isnull(s_crmcloudsync_ltf_subscription.ltf_club_id_name,'') ltf_club_id_name,
       l_crmcloudsync_ltf_subscription.ltf_club_portfolio_staffing_id ltf_club_portfolio_staffing_id,
       isnull(s_crmcloudsync_ltf_subscription.ltf_club_portfolio_staffing_id_name,'') ltf_club_portfolio_staffing_id_name,
       s_crmcloudsync_ltf_subscription.ltf_cost ltf_cost,
       s_crmcloudsync_ltf_subscription.ltf_cost_base ltf_cost_base,
       isnull(s_crmcloudsync_ltf_subscription.ltf_customer_company_code,'') ltf_customer_company_code,
       s_crmcloudsync_ltf_subscription.ltf_lt_health_reactivation_date ltf_lt_health_reactivation_date,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when s_crmcloudsync_ltf_subscription.ltf_lt_health_reactivation_date is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscription.ltf_lt_health_reactivation_date, 112)    end ltf_lt_health_reactivation_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.ltf_lt_health_reactivation_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.ltf_lt_health_reactivation_date,114), 1, 5),':','') end ltf_lt_health_reactivation_dim_time_key,
       s_crmcloudsync_ltf_subscription.ltf_monthly_cost_of_membership ltf_monthly_cost_of_membership,
       s_crmcloudsync_ltf_subscription.ltf_monthly_cost_of_membership_base ltf_monthly_cost_of_membership_base,
       l_crmcloudsync_ltf_subscription.ltf_product_id ltf_product_id,
       isnull(s_crmcloudsync_ltf_subscription.ltf_product_id_name,'') ltf_product_id_name,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when l_crmcloudsync_ltf_subscription.ltf_referring_contact_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.ltf_referring_contact_id as varchar(36)),'z#@$k%&P'))),2) end ltf_referring_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_ltf_subscription.ltf_referring_contact_id_name,'')  ltf_referring_contact_id_name,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
             when l_crmcloudsync_ltf_subscription.ltf_referring_member_id is null then '-998'
       	     when (l_crmcloudsync_ltf_subscription.ltf_referring_member_id not like '%[0-9]%'  or 
       			 isnumeric(l_crmcloudsync_ltf_subscription.ltf_referring_member_id) = 0 or  len(l_crmcloudsync_ltf_subscription.ltf_referring_member_id) >=10 ) then '-999'
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_subscription.ltf_referring_member_id as int) as varchar(500)),'z#@$k%&P'))),2) end ltf_referring_member_dim_crm_member_key,
       s_crmcloudsync_ltf_subscription.ltf_revenue_unit ltf_revenue_unit,
       case when s_crmcloudsync_ltf_subscription.ltf_revenue_unit = 1 then 'Y'        else 'N'  end ltf_revenue_unit_flag,
       isnull(s_crmcloudsync_ltf_subscription.ltf_revenue_unit_name,'') ltf_revenue_unit_name,
       isnull(s_crmcloudsync_ltf_subscription.ltf_subscription_number,'') ltf_subscription_number,
       s_crmcloudsync_ltf_subscription.ltf_termination_date ltf_termination_date,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
        when s_crmcloudsync_ltf_subscription.ltf_termination_date is null then '-998'   when  convert(varchar, s_crmcloudsync_ltf_subscription.ltf_termination_date, 112) > '20991231' then '99991231'
        when convert(varchar, s_crmcloudsync_ltf_subscription.ltf_termination_date, 112)< '19000101' then '19000101'
         else convert(varchar, s_crmcloudsync_ltf_subscription.ltf_termination_date, 112)    end ltf_termination_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.ltf_termination_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.ltf_termination_date,114), 1, 5),':','') end ltf_termination_dim_time_key,
       isnull(s_crmcloudsync_ltf_subscription.ltf_termination_reason,'') ltf_termination_reason,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash 
           when l_crmcloudsync_ltf_subscription.modified_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_subscription.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       case when p_crmcloudsync_ltf_subscription.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when s_crmcloudsync_ltf_subscription.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscription.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscription.bk_hash
       when s_crmcloudsync_ltf_subscription.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscription.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_ltf_subscription.overridden_created_on overridden_created_on,
       isnull(s_crmcloudsync_ltf_subscription.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_ltf_subscription.owner_id_type,'') owner_id_type,
       l_crmcloudsync_ltf_subscription.owning_business_unit owning_business_unit,
       l_crmcloudsync_ltf_subscription.owning_team owning_team,
       case when p_crmcloudsync_ltf_subscription.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscription.bk_hash
           when l_crmcloudsync_ltf_subscription.owning_user is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscription.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       s_crmcloudsync_ltf_subscription.state_code state_code,
       isnull(s_crmcloudsync_ltf_subscription.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_subscription.status_code status_code,
       isnull(s_crmcloudsync_ltf_subscription.status_code_name,'') status_code_name,
       l_crmcloudsync_ltf_subscription.transaction_currency_id transaction_currency_id,
       isnull(s_crmcloudsync_ltf_subscription.transaction_currency_id_name,'') transaction_currency_id_name,
       s_crmcloudsync_ltf_subscription.version_number version_number,
       isnull(h_crmcloudsync_ltf_subscription.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_subscription.p_crmcloudsync_ltf_subscription_id,
       p_crmcloudsync_ltf_subscription.dv_batch_id,
       p_crmcloudsync_ltf_subscription.dv_load_date_time,
       p_crmcloudsync_ltf_subscription.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_subscription
  join dbo.p_crmcloudsync_ltf_subscription
    on h_crmcloudsync_ltf_subscription.bk_hash = p_crmcloudsync_ltf_subscription.bk_hash
  join #p_crmcloudsync_ltf_subscription_insert
    on p_crmcloudsync_ltf_subscription.bk_hash = #p_crmcloudsync_ltf_subscription_insert.bk_hash
   and p_crmcloudsync_ltf_subscription.p_crmcloudsync_ltf_subscription_id = #p_crmcloudsync_ltf_subscription_insert.p_crmcloudsync_ltf_subscription_id
  join dbo.l_crmcloudsync_ltf_subscription
    on p_crmcloudsync_ltf_subscription.bk_hash = l_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id = l_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id
  join dbo.s_crmcloudsync_ltf_subscription
    on p_crmcloudsync_ltf_subscription.bk_hash = s_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id = s_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_subscription
   where d_crmcloudsync_ltf_subscription.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_subscription_insert)

  insert dbo.d_crmcloudsync_ltf_subscription(
             bk_hash,
             dim_crm_ltf_subscription_key,
             ltf_subscription_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_ltf_account_key,
             dim_crm_ltf_club_key,
             dim_crm_owner_key,
             dim_mms_membership_key,
             exchange_rate,
             ltf_account_household,
             ltf_account_household_name,
             ltf_account_id_name,
             ltf_activation_date,
             ltf_activation_dim_date_key,
             ltf_activation_dim_time_key,
             ltf_attrition_exclusion,
             ltf_attrition_exclusion_flag,
             ltf_attrition_exclusion_name,
             ltf_cancellation_date,
             ltf_cancellation_dim_date_key,
             ltf_cancellation_dim_time_key,
             ltf_club_id_name,
             ltf_club_portfolio_staffing_id,
             ltf_club_portfolio_staffing_id_name,
             ltf_cost,
             ltf_cost_base,
             ltf_customer_company_code,
             ltf_lt_health_reactivation_date,
             ltf_lt_health_reactivation_dim_date_key,
             ltf_lt_health_reactivation_dim_time_key,
             ltf_monthly_cost_of_membership,
             ltf_monthly_cost_of_membership_base,
             ltf_product_id,
             ltf_product_id_name,
             ltf_referring_contact_dim_crm_contact_key,
             ltf_referring_contact_id_name,
             ltf_referring_member_dim_crm_member_key,
             ltf_revenue_unit,
             ltf_revenue_unit_flag,
             ltf_revenue_unit_name,
             ltf_subscription_number,
             ltf_termination_date,
             ltf_termination_dim_date_key,
             ltf_termination_dim_time_key,
             ltf_termination_reason,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             overridden_created_dim_date_key,
             overridden_created_dim_time_key,
             overridden_created_on,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user_dim_crm_system_user_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             transaction_currency_id,
             transaction_currency_id_name,
             version_number,
             deleted_flag,
             p_crmcloudsync_ltf_subscription_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_subscription_key,
         ltf_subscription_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_ltf_account_key,
         dim_crm_ltf_club_key,
         dim_crm_owner_key,
         dim_mms_membership_key,
         exchange_rate,
         ltf_account_household,
         ltf_account_household_name,
         ltf_account_id_name,
         ltf_activation_date,
         ltf_activation_dim_date_key,
         ltf_activation_dim_time_key,
         ltf_attrition_exclusion,
         ltf_attrition_exclusion_flag,
         ltf_attrition_exclusion_name,
         ltf_cancellation_date,
         ltf_cancellation_dim_date_key,
         ltf_cancellation_dim_time_key,
         ltf_club_id_name,
         ltf_club_portfolio_staffing_id,
         ltf_club_portfolio_staffing_id_name,
         ltf_cost,
         ltf_cost_base,
         ltf_customer_company_code,
         ltf_lt_health_reactivation_date,
         ltf_lt_health_reactivation_dim_date_key,
         ltf_lt_health_reactivation_dim_time_key,
         ltf_monthly_cost_of_membership,
         ltf_monthly_cost_of_membership_base,
         ltf_product_id,
         ltf_product_id_name,
         ltf_referring_contact_dim_crm_contact_key,
         ltf_referring_contact_id_name,
         ltf_referring_member_dim_crm_member_key,
         ltf_revenue_unit,
         ltf_revenue_unit_flag,
         ltf_revenue_unit_name,
         ltf_subscription_number,
         ltf_termination_date,
         ltf_termination_dim_date_key,
         ltf_termination_dim_time_key,
         ltf_termination_reason,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         overridden_created_dim_date_key,
         overridden_created_dim_time_key,
         overridden_created_on,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user_dim_crm_system_user_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         transaction_currency_id,
         transaction_currency_id_name,
         version_number,
         dv_deleted,
         p_crmcloudsync_ltf_subscription_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_subscription)
--Done!
end
