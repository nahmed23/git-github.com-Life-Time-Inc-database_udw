CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_program_cycle] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_program_cycle)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_program_cycle_insert') is not null drop table #p_crmcloudsync_ltf_program_cycle_insert
create table dbo.#p_crmcloudsync_ltf_program_cycle_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_program_cycle.p_crmcloudsync_ltf_program_cycle_id,
       p_crmcloudsync_ltf_program_cycle.bk_hash
  from dbo.p_crmcloudsync_ltf_program_cycle
 where p_crmcloudsync_ltf_program_cycle.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_program_cycle.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_program_cycle.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_program_cycle.bk_hash,
       p_crmcloudsync_ltf_program_cycle.bk_hash dim_crm_ltf_program_cycle_key,
       p_crmcloudsync_ltf_program_cycle.ltf_program_cycle_id ltf_program_cycle_id,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.created_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.created_by as varchar(36)),'z#@$k%&P'))),2)
        end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_program_cycle.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_program_cycle.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_program_cycle.created_on created_on,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.created_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)
        end created_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_program_cycle.created_on_behalf_by_name,'') created_on_behalf_by_name,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.ltf_cycle_begin_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_program_cycle.ltf_cycle_begin_date, 112)    end cycle_begin_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.ltf_cycle_begin_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.ltf_cycle_begin_date,114), 1, 5),':','') end cycle_begin_dim_time_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.ltf_cycle_end_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_program_cycle.ltf_cycle_end_date, 112)    end cycle_end_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.ltf_cycle_end_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.ltf_cycle_end_date,114), 1, 5),':','') end cycle_end_dim_time_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.owner_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.owner_id as varchar(36)),'z#@$k%&P'))),2)
        end dim_crm_owner_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
             when l_crmcloudsync_ltf_program_cycle.owning_team is null then '-998'     
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.owning_team as varchar(36)),'z#@$k%&P'))),2)   end dim_crm_team_key,
       isnull(s_crmcloudsync_ltf_program_cycle.insert_user,'') insert_user,
       s_crmcloudsync_ltf_program_cycle.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_program_cycle.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       isnull(s_crmcloudsync_ltf_program_cycle.ltf_cancel_reason,'') ltf_cancel_reason,
       s_crmcloudsync_ltf_program_cycle.ltf_cycle_begin_date ltf_cycle_begin_date,
       s_crmcloudsync_ltf_program_cycle.ltf_cycle_end_date ltf_cycle_end_date,
       isnull(s_crmcloudsync_ltf_program_cycle.ltf_cycle_name,'') ltf_cycle_name,
       l_crmcloudsync_ltf_program_cycle.ltf_program ltf_program,
       isnull(s_crmcloudsync_ltf_program_cycle.ltf_program_name,'') ltf_program_name,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.modified_by as varchar(36)),'z#@$k%&P'))),2)
        end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_program_cycle.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_program_cycle.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_program_cycle.modified_on modified_on,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.modified_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)
        end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_program_cycle.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       isnull(s_crmcloudsync_ltf_program_cycle.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_ltf_program_cycle.owner_id_type,'') owner_id_type,
       l_crmcloudsync_ltf_program_cycle.owning_business_unit owning_business_unit,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when l_crmcloudsync_ltf_program_cycle.owning_user is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_program_cycle.owning_user as varchar(36)),'z#@$k%&P'))),2)
        end owning_user_dim_crm_system_user_key,
       l_crmcloudsync_ltf_program_cycle.state_code state_code,
       isnull(s_crmcloudsync_ltf_program_cycle.state_code_name,'') state_code_name,
       l_crmcloudsync_ltf_program_cycle.status_code status_code,
       isnull(s_crmcloudsync_ltf_program_cycle.status_code_name,'') status_code_name,
       s_crmcloudsync_ltf_program_cycle.time_zone_rule_version_number time_zone_rule_version_number,
       isnull(s_crmcloudsync_ltf_program_cycle.update_user,'') update_user,
       s_crmcloudsync_ltf_program_cycle.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
           when s_crmcloudsync_ltf_program_cycle.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_program_cycle.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_program_cycle.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_program_cycle.bk_hash
       when s_crmcloudsync_ltf_program_cycle.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_program_cycle.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_ltf_program_cycle.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       isnull(h_crmcloudsync_ltf_program_cycle.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_program_cycle.p_crmcloudsync_ltf_program_cycle_id,
       p_crmcloudsync_ltf_program_cycle.dv_batch_id,
       p_crmcloudsync_ltf_program_cycle.dv_load_date_time,
       p_crmcloudsync_ltf_program_cycle.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_program_cycle
  join dbo.p_crmcloudsync_ltf_program_cycle
    on h_crmcloudsync_ltf_program_cycle.bk_hash = p_crmcloudsync_ltf_program_cycle.bk_hash
  join #p_crmcloudsync_ltf_program_cycle_insert
    on p_crmcloudsync_ltf_program_cycle.bk_hash = #p_crmcloudsync_ltf_program_cycle_insert.bk_hash
   and p_crmcloudsync_ltf_program_cycle.p_crmcloudsync_ltf_program_cycle_id = #p_crmcloudsync_ltf_program_cycle_insert.p_crmcloudsync_ltf_program_cycle_id
  join dbo.l_crmcloudsync_ltf_program_cycle
    on p_crmcloudsync_ltf_program_cycle.bk_hash = l_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id = l_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id
  join dbo.s_crmcloudsync_ltf_program_cycle
    on p_crmcloudsync_ltf_program_cycle.bk_hash = s_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id = s_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_program_cycle
   where d_crmcloudsync_ltf_program_cycle.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_program_cycle_insert)

  insert dbo.d_crmcloudsync_ltf_program_cycle(
             bk_hash,
             dim_crm_ltf_program_cycle_key,
             ltf_program_cycle_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             created_on_behalf_by_name,
             cycle_begin_dim_date_key,
             cycle_begin_dim_time_key,
             cycle_end_dim_date_key,
             cycle_end_dim_time_key,
             dim_crm_owner_key,
             dim_crm_team_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_cancel_reason,
             ltf_cycle_begin_date,
             ltf_cycle_end_date,
             ltf_cycle_name,
             ltf_program,
             ltf_program_name,
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
             owning_user_dim_crm_system_user_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             time_zone_rule_version_number,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_conversion_time_zone_code,
             deleted_flag,
             p_crmcloudsync_ltf_program_cycle_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_program_cycle_key,
         ltf_program_cycle_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         created_on_behalf_by_name,
         cycle_begin_dim_date_key,
         cycle_begin_dim_time_key,
         cycle_end_dim_date_key,
         cycle_end_dim_time_key,
         dim_crm_owner_key,
         dim_crm_team_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_cancel_reason,
         ltf_cycle_begin_date,
         ltf_cycle_end_date,
         ltf_cycle_name,
         ltf_program,
         ltf_program_name,
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
         owning_user_dim_crm_system_user_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         time_zone_rule_version_number,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_conversion_time_zone_code,
         dv_deleted,
         p_crmcloudsync_ltf_program_cycle_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_program_cycle)
--Done!
end
