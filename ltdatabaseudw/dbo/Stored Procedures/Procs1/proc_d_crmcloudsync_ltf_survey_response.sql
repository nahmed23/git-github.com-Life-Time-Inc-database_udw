CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_survey_response] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_survey_response)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_survey_response_insert') is not null drop table #p_crmcloudsync_ltf_survey_response_insert
create table dbo.#p_crmcloudsync_ltf_survey_response_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_survey_response.p_crmcloudsync_ltf_survey_response_id,
       p_crmcloudsync_ltf_survey_response.bk_hash
  from dbo.p_crmcloudsync_ltf_survey_response
 where p_crmcloudsync_ltf_survey_response.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_survey_response.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_survey_response.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_survey_response.bk_hash,
       p_crmcloudsync_ltf_survey_response.bk_hash dim_crm_ltf_survey_response_key,
       p_crmcloudsync_ltf_survey_response.ltf_survey_response_id ltf_survey_response_id,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash  
    when l_crmcloudsync_ltf_survey_response.created_by is null then '-998'  
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_survey_response.created_by as varchar(36)),'z#@$k%&P'))),2)   end created_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_survey_response.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey_response.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_survey_response.created_on created_on,
       s_crmcloudsync_ltf_survey_response.insert_user insert_user	,
       s_crmcloudsync_ltf_survey_response.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash
           when s_crmcloudsync_ltf_survey_response.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_survey_response.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey_response.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_ltf_survey_response.ltf_question ltf_question,
       s_crmcloudsync_ltf_survey_response.ltf_response ltf_response,
       s_crmcloudsync_ltf_survey_response.ltf_sequence ltf_sequence,
       l_crmcloudsync_ltf_survey_response.ltf_survey ltf_survey,
       s_crmcloudsync_ltf_survey_response.ltf_survey_response ltf_survey_response,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash  
    when l_crmcloudsync_ltf_survey_response.modified_by is null then '-998'  
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_survey_response.modified_by as varchar(36)),'z#@$k%&P'))),2)   end modified_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_survey_response.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey_response.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_survey_response.modified_on modified_on,
       s_crmcloudsync_ltf_survey_response.state_code state_code,
       s_crmcloudsync_ltf_survey_response.status_code status_code,
       s_crmcloudsync_ltf_survey_response.update_user update_user,
       s_crmcloudsync_ltf_survey_response.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.updated_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_survey_response.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_survey_response.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_survey_response.bk_hash
       when s_crmcloudsync_ltf_survey_response.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_survey_response.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_ltf_survey_response.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_survey_response.p_crmcloudsync_ltf_survey_response_id,
       p_crmcloudsync_ltf_survey_response.dv_batch_id,
       p_crmcloudsync_ltf_survey_response.dv_load_date_time,
       p_crmcloudsync_ltf_survey_response.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_survey_response
  join dbo.p_crmcloudsync_ltf_survey_response
    on h_crmcloudsync_ltf_survey_response.bk_hash = p_crmcloudsync_ltf_survey_response.bk_hash
  join #p_crmcloudsync_ltf_survey_response_insert
    on p_crmcloudsync_ltf_survey_response.bk_hash = #p_crmcloudsync_ltf_survey_response_insert.bk_hash
   and p_crmcloudsync_ltf_survey_response.p_crmcloudsync_ltf_survey_response_id = #p_crmcloudsync_ltf_survey_response_insert.p_crmcloudsync_ltf_survey_response_id
  join dbo.l_crmcloudsync_ltf_survey_response
    on p_crmcloudsync_ltf_survey_response.bk_hash = l_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id = l_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id
  join dbo.s_crmcloudsync_ltf_survey_response
    on p_crmcloudsync_ltf_survey_response.bk_hash = s_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id = s_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_survey_response
   where d_crmcloudsync_ltf_survey_response.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_survey_response_insert)

  insert dbo.d_crmcloudsync_ltf_survey_response(
             bk_hash,
             dim_crm_ltf_survey_response_key,
             ltf_survey_response_id,
             created_by_dim_crm_system_user_key,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             insert_user	,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_question,
             ltf_response,
             ltf_sequence,
             ltf_survey,
             ltf_survey_response,
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
             p_crmcloudsync_ltf_survey_response_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_survey_response_key,
         ltf_survey_response_id,
         created_by_dim_crm_system_user_key,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         insert_user	,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_question,
         ltf_response,
         ltf_sequence,
         ltf_survey,
         ltf_survey_response,
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
         p_crmcloudsync_ltf_survey_response_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_survey_response)
--Done!
end
