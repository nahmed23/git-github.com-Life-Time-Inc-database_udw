CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_survey] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_survey)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_survey_insert') is not null drop table #p_crmcloudsync_ltf_survey_insert
create table dbo.#p_crmcloudsync_ltf_survey_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_survey.p_crmcloudsync_ltf_survey_id,
       p_crmcloudsync_ltf_survey.bk_hash
  from dbo.p_crmcloudsync_ltf_survey
 where p_crmcloudsync_ltf_survey.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_survey.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_survey.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_survey.bk_hash,
       p_crmcloudsync_ltf_survey.bk_hash dim_crm_ltf_survey_key,
       p_crmcloudsync_ltf_survey.ltf_survey_id ltf_survey_id,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash  
    when l_crmcloudsync_ltf_survey.created_by is null then '-998'  
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_survey.created_by as varchar(36)),'z#@$k%&P'))),2)   end created_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_survey.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_survey.created_on created_on,
       s_crmcloudsync_ltf_survey.insert_user insert_user,
       s_crmcloudsync_ltf_survey.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_survey.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash
           when s_crmcloudsync_ltf_survey.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_survey.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_crmcloudsync_ltf_survey.ltf_connect_member ltf_connect_member,
       l_crmcloudsync_ltf_survey.ltf_employee_id ltf_employee_id,
       l_crmcloudsync_ltf_survey.ltf_member_number ltf_member_number,
       s_crmcloudsync_ltf_survey.ltf_name ltf_name,
       s_crmcloudsync_ltf_survey.ltf_source ltf_source,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash  
    when l_crmcloudsync_ltf_survey.ltf_submitted_by is null then '-998'  
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_survey.ltf_submitted_by as varchar(36)),'z#@$k%&P'))),2)   end ltf_submitted_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.ltf_submitted_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_survey.ltf_submitted_on, 112)    end ltf_submitted_dim_date_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.ltf_submitted_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey.ltf_submitted_on,114), 1, 5),':','') end ltf_submitted_dim_time_key,
       s_crmcloudsync_ltf_survey.ltf_submitted_on ltf_submitted_on,
       l_crmcloudsync_ltf_survey.ltf_subscriber ltf_subscriber,
       l_crmcloudsync_ltf_survey.ltf_survey_tool_id ltf_survey_tool_id,
       s_crmcloudsync_ltf_survey.ltf_survey_type ltf_survey_type,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash  
    when l_crmcloudsync_ltf_survey.modified_by is null then '-998'  
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_survey.modified_by as varchar(36)),'z#@$k%&P'))),2)   end modified_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_survey.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_survey.modified_on modified_on,
       s_crmcloudsync_ltf_survey.state_code state_code,
       s_crmcloudsync_ltf_survey.status_code status_code,
       s_crmcloudsync_ltf_survey.update_user update_user,
       s_crmcloudsync_ltf_survey.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_survey.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.updated_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_survey.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_survey.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey.bk_hash
       when s_crmcloudsync_ltf_survey.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_ltf_survey.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_survey.p_crmcloudsync_ltf_survey_id,
       p_crmcloudsync_ltf_survey.dv_batch_id,
       p_crmcloudsync_ltf_survey.dv_load_date_time,
       p_crmcloudsync_ltf_survey.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_survey
  join dbo.p_crmcloudsync_ltf_survey
    on h_crmcloudsync_ltf_survey.bk_hash = p_crmcloudsync_ltf_survey.bk_hash
  join #p_crmcloudsync_ltf_survey_insert
    on p_crmcloudsync_ltf_survey.bk_hash = #p_crmcloudsync_ltf_survey_insert.bk_hash
   and p_crmcloudsync_ltf_survey.p_crmcloudsync_ltf_survey_id = #p_crmcloudsync_ltf_survey_insert.p_crmcloudsync_ltf_survey_id
  join dbo.l_crmcloudsync_ltf_survey
    on p_crmcloudsync_ltf_survey.bk_hash = l_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id = l_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id
  join dbo.s_crmcloudsync_ltf_survey
    on p_crmcloudsync_ltf_survey.bk_hash = s_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id = s_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_survey
   where d_crmcloudsync_ltf_survey.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_survey_insert)

  insert dbo.d_crmcloudsync_ltf_survey(
             bk_hash,
             dim_crm_ltf_survey_key,
             ltf_survey_id,
             created_by_dim_crm_system_user_key,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_connect_member,
             ltf_employee_id,
             ltf_member_number,
             ltf_name,
             ltf_source,
             ltf_submitted_by_dim_crm_system_user_key,
             ltf_submitted_dim_date_key,
             ltf_submitted_dim_time_key,
             ltf_submitted_on,
             ltf_subscriber,
             ltf_survey_tool_id,
             ltf_survey_type,
             modified_by_dim_crm_system_user_key,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             state_code,
             status_code,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_ltf_survey_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_survey_key,
         ltf_survey_id,
         created_by_dim_crm_system_user_key,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_connect_member,
         ltf_employee_id,
         ltf_member_number,
         ltf_name,
         ltf_source,
         ltf_submitted_by_dim_crm_system_user_key,
         ltf_submitted_dim_date_key,
         ltf_submitted_dim_time_key,
         ltf_submitted_on,
         ltf_subscriber,
         ltf_survey_tool_id,
         ltf_survey_type,
         modified_by_dim_crm_system_user_key,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         state_code,
         status_code,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_ltf_survey_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_survey)
--Done!
end
