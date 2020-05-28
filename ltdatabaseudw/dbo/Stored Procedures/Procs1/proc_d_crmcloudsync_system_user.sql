CREATE PROC [dbo].[proc_d_crmcloudsync_system_user] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_system_user)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_system_user_insert') is not null drop table #p_crmcloudsync_system_user_insert
create table dbo.#p_crmcloudsync_system_user_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_system_user.p_crmcloudsync_system_user_id,
       p_crmcloudsync_system_user.bk_hash
  from dbo.p_crmcloudsync_system_user
 where p_crmcloudsync_system_user.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_system_user.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_system_user.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_system_user.bk_hash,
       p_crmcloudsync_system_user.bk_hash dim_crm_system_user_key,
       p_crmcloudsync_system_user.system_user_id system_user_id,
       l_crmcloudsync_system_user.business_unit_id business_unit_id,
       isnull(s_crmcloudsync_system_user.business_unit_id_name,'') business_unit_id_name,
       case when p_crmcloudsync_system_user.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
           when l_crmcloudsync_system_user.ltf_club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_system_user.ltf_club_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_club_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
           when l_crmcloudsync_system_user.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_system_user.modified_by as varchar(36)),'z#@$k%&P'))),2) end dim_crm_modified_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
             when l_crmcloudsync_system_user.employee_id is null then '-998'   when isnumeric(l_crmcloudsync_system_user.employee_id) = 0  then '-999'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_crmcloudsync_system_user.employee_id, PATINDEX('%[0-9]%',l_crmcloudsync_system_user.employee_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_employee_key,
       isnull(s_crmcloudsync_system_user.disabled_reason,'') disabled_reason,
       l_crmcloudsync_system_user.employee_id employee_id,
       isnull(s_crmcloudsync_system_user.first_name,'') first_name,
       isnull(s_crmcloudsync_system_user.full_name,'') full_name,
       s_crmcloudsync_system_user.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_system_user.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
           when s_crmcloudsync_system_user.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_system_user.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997','-998','-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_system_user.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       isnull(s_crmcloudsync_system_user.internal_email_address,'') internal_email_address,
       s_crmcloudsync_system_user.is_disabled is_disabled,
       case when s_crmcloudsync_system_user.is_disabled = 1 then 'Y'        else 'N'  end is_disabled_flag,
       isnull(s_crmcloudsync_system_user.is_disabled_name,'') is_disabled_name,
       isnull(s_crmcloudsync_system_user.job_title,'') job_title,
       isnull(s_crmcloudsync_system_user.last_name,'') last_name,
       l_crmcloudsync_system_user.ltf_club_id ltf_club_id,
       isnull(s_crmcloudsync_system_user.ltf_club_id_name,'') ltf_club_id_name,
       isnull(s_crmcloudsync_system_user.middle_name,'') middle_name,
       isnull(s_crmcloudsync_system_user.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_system_user.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_system_user.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997','-998','-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_system_user.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_system_user.modified_on modified_on,
       case when p_crmcloudsync_system_user.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
           when s_crmcloudsync_system_user.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_system_user.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997','-998','-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_system_user.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_system_user.overridden_created_on overridden_created_on,
       l_crmcloudsync_system_user.queue_id queue_id,
       isnull(s_crmcloudsync_system_user.queue_id_name,'') queue_id_name,
       isnull(s_crmcloudsync_system_user.salutation,'') salutation,
       isnull(s_crmcloudsync_system_user.title,'') title,
       s_crmcloudsync_system_user.updated_date_time updated_date_time,
       case when p_crmcloudsync_system_user.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.updated_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_system_user.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_system_user.bk_hash in ('-997','-998','-999') then p_crmcloudsync_system_user.bk_hash
       when s_crmcloudsync_system_user.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_system_user.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_system_user.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       isnull(h_crmcloudsync_system_user.dv_deleted,0) dv_deleted,
       p_crmcloudsync_system_user.p_crmcloudsync_system_user_id,
       p_crmcloudsync_system_user.dv_batch_id,
       p_crmcloudsync_system_user.dv_load_date_time,
       p_crmcloudsync_system_user.dv_load_end_date_time
  from dbo.h_crmcloudsync_system_user
  join dbo.p_crmcloudsync_system_user
    on h_crmcloudsync_system_user.bk_hash = p_crmcloudsync_system_user.bk_hash
  join #p_crmcloudsync_system_user_insert
    on p_crmcloudsync_system_user.bk_hash = #p_crmcloudsync_system_user_insert.bk_hash
   and p_crmcloudsync_system_user.p_crmcloudsync_system_user_id = #p_crmcloudsync_system_user_insert.p_crmcloudsync_system_user_id
  join dbo.l_crmcloudsync_system_user
    on p_crmcloudsync_system_user.bk_hash = l_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.l_crmcloudsync_system_user_id = l_crmcloudsync_system_user.l_crmcloudsync_system_user_id
  join dbo.s_crmcloudsync_system_user
    on p_crmcloudsync_system_user.bk_hash = s_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.s_crmcloudsync_system_user_id = s_crmcloudsync_system_user.s_crmcloudsync_system_user_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_system_user
   where d_crmcloudsync_system_user.bk_hash in (select bk_hash from #p_crmcloudsync_system_user_insert)

  insert dbo.d_crmcloudsync_system_user(
             bk_hash,
             dim_crm_system_user_key,
             system_user_id,
             business_unit_id,
             business_unit_id_name,
             dim_crm_ltf_club_key,
             dim_crm_modified_key,
             dim_mms_employee_key,
             disabled_reason,
             employee_id,
             first_name,
             full_name,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             internal_email_address,
             is_disabled,
             is_disabled_flag,
             is_disabled_name,
             job_title,
             last_name,
             ltf_club_id,
             ltf_club_id_name,
             middle_name,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             overridden_created_dim_date_key,
             overridden_created_dim_time_key,
             overridden_created_on,
             queue_id,
             queue_id_name,
             salutation,
             title,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_conversion_time_zone_code,
             deleted_flag,
             p_crmcloudsync_system_user_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_system_user_key,
         system_user_id,
         business_unit_id,
         business_unit_id_name,
         dim_crm_ltf_club_key,
         dim_crm_modified_key,
         dim_mms_employee_key,
         disabled_reason,
         employee_id,
         first_name,
         full_name,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         internal_email_address,
         is_disabled,
         is_disabled_flag,
         is_disabled_name,
         job_title,
         last_name,
         ltf_club_id,
         ltf_club_id_name,
         middle_name,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         overridden_created_dim_date_key,
         overridden_created_dim_time_key,
         overridden_created_on,
         queue_id,
         queue_id_name,
         salutation,
         title,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_conversion_time_zone_code,
         dv_deleted,
         p_crmcloudsync_system_user_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_system_user)
--Done!
end
