CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_subscriber] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_subscriber)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_subscriber_insert') is not null drop table #p_crmcloudsync_ltf_subscriber_insert
create table dbo.#p_crmcloudsync_ltf_subscriber_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_subscriber.p_crmcloudsync_ltf_subscriber_id,
       p_crmcloudsync_ltf_subscriber.bk_hash
  from dbo.p_crmcloudsync_ltf_subscriber
 where p_crmcloudsync_ltf_subscriber.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_subscriber.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_subscriber.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_subscriber.bk_hash,
       p_crmcloudsync_ltf_subscriber.bk_hash dim_crm_ltf_subscriber_key,
       p_crmcloudsync_ltf_subscriber.ltf_subscriber_id ltf_subscriber_id,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
           when l_crmcloudsync_ltf_subscriber.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.created_by as varchar(36)),'z#@$k%&P'))),2) end  created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_subscriber.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash
           when s_crmcloudsync_ltf_subscriber.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscriber.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscriber.bk_hash
       when s_crmcloudsync_ltf_subscriber.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscriber.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_subscriber.created_on created_on,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
           when l_crmcloudsync_ltf_subscriber.ltf_subscription_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.ltf_subscription_id as varchar(36)),'z#@$k%&P'))),2) end  dim_crm_ltf_subscription_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
            when l_crmcloudsync_ltf_subscriber.owner_id is null then '-998' 
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscriber.bk_hash     
         when (s_crmcloudsync_ltf_subscriber.ltf_name is null or s_crmcloudsync_ltf_subscriber.ltf_name ='')then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_crmcloudsync_ltf_subscriber.ltf_name as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       s_crmcloudsync_ltf_subscriber.ltf_connection_pref_source ltf_connection_pref_source,
       s_crmcloudsync_ltf_subscriber.ltf_connection_preference ltf_connection_preference,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
           when l_crmcloudsync_ltf_subscriber.ltf_contact_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.ltf_contact_id as varchar(36)),'z#@$k%&P'))),2) end  ltf_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_ltf_subscriber.ltf_contact_id_name,'') ltf_contact_id_name,
       s_crmcloudsync_ltf_subscriber.ltf_join_date ltf_join_date,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash
        when s_crmcloudsync_ltf_subscriber.ltf_join_date is null then '-998'   when  convert(varchar, s_crmcloudsync_ltf_subscriber.ltf_join_date, 112) > '20991231' then '99991231'
        when convert(varchar, s_crmcloudsync_ltf_subscriber.ltf_join_date, 112)< '19000101' then '19000101'
         else convert(varchar, s_crmcloudsync_ltf_subscriber.ltf_join_date, 112)    end ltf_join_dim_date_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscriber.bk_hash
       when s_crmcloudsync_ltf_subscriber.ltf_join_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscriber.ltf_join_date,114), 1, 5),':','') end ltf_join_dim_time_key,
       isnull(s_crmcloudsync_ltf_subscriber.ltf_name,'') ltf_name,
       s_crmcloudsync_ltf_subscriber.ltf_role ltf_role,
       isnull(s_crmcloudsync_ltf_subscriber.ltf_role_name,'') ltf_role_name,
       isnull(s_crmcloudsync_ltf_subscriber.ltf_subscription_id_name,'') ltf_subscription_id_name,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
           when l_crmcloudsync_ltf_subscriber.modified_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.modified_by as varchar(36)),'z#@$k%&P'))),2) end  modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_subscriber.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash
           when s_crmcloudsync_ltf_subscriber.modified_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscriber.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscriber.bk_hash
       when s_crmcloudsync_ltf_subscriber.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscriber.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_subscriber.modified_on modified_on,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash 
           when l_crmcloudsync_ltf_subscriber.modified_on_behalf_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end  modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_subscriber.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash
           when s_crmcloudsync_ltf_subscriber.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_subscriber.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_subscriber.bk_hash
       when s_crmcloudsync_ltf_subscriber.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_subscriber.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_ltf_subscriber.overridden_created_on overridden_created_on,
       isnull(s_crmcloudsync_ltf_subscriber.owner_id_name,'') owner_id_name,
       s_crmcloudsync_ltf_subscriber.owner_id_type owner_id_type,
       l_crmcloudsync_ltf_subscriber.owning_business_unit owning_business_unit,
       l_crmcloudsync_ltf_subscriber.owning_team owning_team,
       case when p_crmcloudsync_ltf_subscriber.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_subscriber.bk_hash
           when l_crmcloudsync_ltf_subscriber.owning_user is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_subscriber.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       s_crmcloudsync_ltf_subscriber.state_code state_code,
       isnull(s_crmcloudsync_ltf_subscriber.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_subscriber.status_code status_code,
       isnull(s_crmcloudsync_ltf_subscriber.status_code_name,'') status_code_name,
       s_crmcloudsync_ltf_subscriber.time_zone_rule_version_number time_zone_rule_version_number,
       s_crmcloudsync_ltf_subscriber.version_number version_number,
       isnull(h_crmcloudsync_ltf_subscriber.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_subscriber.p_crmcloudsync_ltf_subscriber_id,
       p_crmcloudsync_ltf_subscriber.dv_batch_id,
       p_crmcloudsync_ltf_subscriber.dv_load_date_time,
       p_crmcloudsync_ltf_subscriber.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_subscriber
  join dbo.p_crmcloudsync_ltf_subscriber
    on h_crmcloudsync_ltf_subscriber.bk_hash = p_crmcloudsync_ltf_subscriber.bk_hash
  join #p_crmcloudsync_ltf_subscriber_insert
    on p_crmcloudsync_ltf_subscriber.bk_hash = #p_crmcloudsync_ltf_subscriber_insert.bk_hash
   and p_crmcloudsync_ltf_subscriber.p_crmcloudsync_ltf_subscriber_id = #p_crmcloudsync_ltf_subscriber_insert.p_crmcloudsync_ltf_subscriber_id
  join dbo.l_crmcloudsync_ltf_subscriber
    on p_crmcloudsync_ltf_subscriber.bk_hash = l_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id = l_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id
  join dbo.s_crmcloudsync_ltf_subscriber
    on p_crmcloudsync_ltf_subscriber.bk_hash = s_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id = s_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_subscriber
   where d_crmcloudsync_ltf_subscriber.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_subscriber_insert)

  insert dbo.d_crmcloudsync_ltf_subscriber(
             bk_hash,
             dim_crm_ltf_subscriber_key,
             ltf_subscriber_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_ltf_subscription_key,
             dim_crm_owner_key,
             dim_mms_member_key,
             ltf_connection_pref_source,
             ltf_connection_preference,
             ltf_contact_dim_crm_contact_key,
             ltf_contact_id_name,
             ltf_join_date,
             ltf_join_dim_date_key,
             ltf_join_dim_time_key,
             ltf_name,
             ltf_role,
             ltf_role_name,
             ltf_subscription_id_name,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
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
             time_zone_rule_version_number,
             version_number,
             deleted_flag,
             p_crmcloudsync_ltf_subscriber_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_subscriber_key,
         ltf_subscriber_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_ltf_subscription_key,
         dim_crm_owner_key,
         dim_mms_member_key,
         ltf_connection_pref_source,
         ltf_connection_preference,
         ltf_contact_dim_crm_contact_key,
         ltf_contact_id_name,
         ltf_join_date,
         ltf_join_dim_date_key,
         ltf_join_dim_time_key,
         ltf_name,
         ltf_role,
         ltf_role_name,
         ltf_subscription_id_name,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
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
         time_zone_rule_version_number,
         version_number,
         dv_deleted,
         p_crmcloudsync_ltf_subscriber_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_subscriber)
--Done!
end
