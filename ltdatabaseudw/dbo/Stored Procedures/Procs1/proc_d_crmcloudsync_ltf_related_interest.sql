﻿CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_related_interest] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_related_interest)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_related_interest_insert') is not null drop table #p_crmcloudsync_ltf_related_interest_insert
create table dbo.#p_crmcloudsync_ltf_related_interest_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_related_interest.p_crmcloudsync_ltf_related_interest_id,
       p_crmcloudsync_ltf_related_interest.bk_hash
  from dbo.p_crmcloudsync_ltf_related_interest
 where p_crmcloudsync_ltf_related_interest.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_related_interest.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_related_interest.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_related_interest.bk_hash,
       p_crmcloudsync_ltf_related_interest.bk_hash dim_crm_related_interest_key,
       p_crmcloudsync_ltf_related_interest.ltf_related_interest_id ltf_related_interest_id,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.created_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.created_by as varchar(36)),'z#@$k%&P'))),2) end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_related_interest.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_related_interest.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_related_interest.created_on created_on,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.ltf_contact_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.ltf_contact_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_contact_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.ltf_interest_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.ltf_interest_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_interest_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.ltf_lead_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.ltf_lead_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_lead_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.ltf_opportunity_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.ltf_opportunity_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_opportunity_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.owner_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       s_crmcloudsync_ltf_related_interest.insert_user insert_user,
       s_crmcloudsync_ltf_related_interest.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
           when s_crmcloudsync_ltf_related_interest.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_related_interest.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_crmcloudsync_ltf_related_interest.ltf_add_by ltf_add_by,
       s_crmcloudsync_ltf_related_interest.ltf_add_date ltf_add_date,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.ltf_add_date is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_related_interest.ltf_add_date, 112)    end ltf_add_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.ltf_add_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.ltf_add_date,114), 1, 5),':','') end ltf_add_dim_time_key,
       l_crmcloudsync_ltf_related_interest.ltf_add_source ltf_add_source,
       isnull(s_crmcloudsync_ltf_related_interest.ltf_contact_id_name, '') ltf_contact_id_name,
       l_crmcloudsync_ltf_related_interest.ltf_interest_id ltf_interest_id,
       isnull(s_crmcloudsync_ltf_related_interest.ltf_interest_id_name, '') ltf_interest_id_name,
       isnull(s_crmcloudsync_ltf_related_interest.ltf_lead_id_name, '') ltf_lead_id_name,
       isnull(s_crmcloudsync_ltf_related_interest.ltf_name, '') ltf_name,
       isnull(s_crmcloudsync_ltf_related_interest.ltf_opportunity_id_name, '') ltf_opportunity_id_name,
       l_crmcloudsync_ltf_related_interest.ltf_remove_by ltf_remove_by,
       s_crmcloudsync_ltf_related_interest.ltf_remove_date ltf_remove_date,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.ltf_remove_date is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_related_interest.ltf_remove_date, 112)    end ltf_remove_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.ltf_remove_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.ltf_remove_date,114), 1, 5),':','') end ltf_remove_dim_time_key,
       l_crmcloudsync_ltf_related_interest.ltf_remove_source ltf_remove_source,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.modified_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_related_interest.modified_by_name, '') modified_by_name,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_related_interest.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_related_interest.modified_on modified_on,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.modified_on_behalf_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_related_interest.modified_on_behalf_by_name, '') modified_on_behalf_by_name,
       isnull(s_crmcloudsync_ltf_related_interest.owner_id_name, '') owner_id_name,
       isnull(s_crmcloudsync_ltf_related_interest.owner_id_type, '') owner_id_type,
       l_crmcloudsync_ltf_related_interest.owning_business_unit owning_business_unit,
       l_crmcloudsync_ltf_related_interest.owning_team owning_team,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
    when l_crmcloudsync_ltf_related_interest.owning_user is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_related_interest.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       case when s_crmcloudsync_ltf_related_interest.ltf_primary_interest = 1  then 'Y' else 'N' end primary_interest_flag,
       s_crmcloudsync_ltf_related_interest.state_code state_code,
       isnull(s_crmcloudsync_ltf_related_interest.state_code_name, '') state_code_name,
       s_crmcloudsync_ltf_related_interest.status_code status_code,
       isnull(s_crmcloudsync_ltf_related_interest.status_code_name, '') status_code_name,
       s_crmcloudsync_ltf_related_interest.update_user update_user,
       s_crmcloudsync_ltf_related_interest.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.updated_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_related_interest.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_related_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_related_interest.bk_hash
       when s_crmcloudsync_ltf_related_interest.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_related_interest.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_ltf_related_interest.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_related_interest.p_crmcloudsync_ltf_related_interest_id,
       p_crmcloudsync_ltf_related_interest.dv_batch_id,
       p_crmcloudsync_ltf_related_interest.dv_load_date_time,
       p_crmcloudsync_ltf_related_interest.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_related_interest
  join dbo.p_crmcloudsync_ltf_related_interest
    on h_crmcloudsync_ltf_related_interest.bk_hash = p_crmcloudsync_ltf_related_interest.bk_hash
  join #p_crmcloudsync_ltf_related_interest_insert
    on p_crmcloudsync_ltf_related_interest.bk_hash = #p_crmcloudsync_ltf_related_interest_insert.bk_hash
   and p_crmcloudsync_ltf_related_interest.p_crmcloudsync_ltf_related_interest_id = #p_crmcloudsync_ltf_related_interest_insert.p_crmcloudsync_ltf_related_interest_id
  join dbo.l_crmcloudsync_ltf_related_interest
    on p_crmcloudsync_ltf_related_interest.bk_hash = l_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id = l_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id
  join dbo.s_crmcloudsync_ltf_related_interest
    on p_crmcloudsync_ltf_related_interest.bk_hash = s_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id = s_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_related_interest
   where d_crmcloudsync_ltf_related_interest.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_related_interest_insert)

  insert dbo.d_crmcloudsync_ltf_related_interest(
             bk_hash,
             dim_crm_related_interest_key,
             ltf_related_interest_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_contact_key,
             dim_crm_interest_key,
             dim_crm_lead_key,
             dim_crm_opportunity_key,
             dim_crm_owner_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_add_by,
             ltf_add_date,
             ltf_add_dim_date_key,
             ltf_add_dim_time_key,
             ltf_add_source,
             ltf_contact_id_name,
             ltf_interest_id,
             ltf_interest_id_name,
             ltf_lead_id_name,
             ltf_name,
             ltf_opportunity_id_name,
             ltf_remove_by,
             ltf_remove_date,
             ltf_remove_dim_date_key,
             ltf_remove_dim_time_key,
             ltf_remove_source,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user_dim_crm_system_user_key,
             primary_interest_flag,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_ltf_related_interest_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_related_interest_key,
         ltf_related_interest_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_contact_key,
         dim_crm_interest_key,
         dim_crm_lead_key,
         dim_crm_opportunity_key,
         dim_crm_owner_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_add_by,
         ltf_add_date,
         ltf_add_dim_date_key,
         ltf_add_dim_time_key,
         ltf_add_source,
         ltf_contact_id_name,
         ltf_interest_id,
         ltf_interest_id_name,
         ltf_lead_id_name,
         ltf_name,
         ltf_opportunity_id_name,
         ltf_remove_by,
         ltf_remove_date,
         ltf_remove_dim_date_key,
         ltf_remove_dim_time_key,
         ltf_remove_source,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user_dim_crm_system_user_key,
         primary_interest_flag,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_ltf_related_interest_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_related_interest)
--Done!
end
