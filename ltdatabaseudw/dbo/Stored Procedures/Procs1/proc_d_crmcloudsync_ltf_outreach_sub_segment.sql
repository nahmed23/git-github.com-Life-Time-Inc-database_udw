CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_outreach_sub_segment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_outreach_sub_segment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_outreach_sub_segment_insert') is not null drop table #p_crmcloudsync_ltf_outreach_sub_segment_insert
create table dbo.#p_crmcloudsync_ltf_outreach_sub_segment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_outreach_sub_segment.p_crmcloudsync_ltf_outreach_sub_segment_id,
       p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
  from dbo.p_crmcloudsync_ltf_outreach_sub_segment
 where p_crmcloudsync_ltf_outreach_sub_segment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_outreach_sub_segment.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_outreach_sub_segment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_outreach_sub_segment.bk_hash,
       p_crmcloudsync_ltf_outreach_sub_segment.bk_hash dim_crm_ltf_outreach_sub_segment_key,
       p_crmcloudsync_ltf_outreach_sub_segment.ltf_outreach_sub_segment_id ltf_outreach_sub_segment_id,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when l_crmcloudsync_ltf_outreach_sub_segment.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_sub_segment.created_by as varchar(36)),'z#@$k%&P'))),2) end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when s_crmcloudsync_ltf_outreach_sub_segment.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_outreach_sub_segment.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
       when s_crmcloudsync_ltf_outreach_sub_segment.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_outreach_sub_segment.created_on created_on,
       s_crmcloudsync_ltf_outreach_sub_segment.import_sequence_number import_sequence_number,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.insert_user,'') insert_user,
       s_crmcloudsync_ltf_outreach_sub_segment.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when s_crmcloudsync_ltf_outreach_sub_segment.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_outreach_sub_segment.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
       when s_crmcloudsync_ltf_outreach_sub_segment.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_ltf_outreach_sub_segment.ltf_attribute_index ltf_attribute_index,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.ltf_description,'') ltf_description,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.ltf_subsegment,'') ltf_subsegment,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when l_crmcloudsync_ltf_outreach_sub_segment.modified_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_sub_segment.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when s_crmcloudsync_ltf_outreach_sub_segment.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
       when s_crmcloudsync_ltf_outreach_sub_segment.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_outreach_sub_segment.modified_on modified_on,
       s_crmcloudsync_ltf_outreach_sub_segment.state_code state_code,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_outreach_sub_segment.status_code status_code,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.status_code_name,'') status_code_name,
       isnull(s_crmcloudsync_ltf_outreach_sub_segment.update_user,'') update_user,
       s_crmcloudsync_ltf_outreach_sub_segment.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
           when s_crmcloudsync_ltf_outreach_sub_segment.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_sub_segment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
       when s_crmcloudsync_ltf_outreach_sub_segment.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_sub_segment.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_ltf_outreach_sub_segment.version_number version_number,
       isnull(h_crmcloudsync_ltf_outreach_sub_segment.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_outreach_sub_segment.p_crmcloudsync_ltf_outreach_sub_segment_id,
       p_crmcloudsync_ltf_outreach_sub_segment.dv_batch_id,
       p_crmcloudsync_ltf_outreach_sub_segment.dv_load_date_time,
       p_crmcloudsync_ltf_outreach_sub_segment.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_outreach_sub_segment
  join dbo.p_crmcloudsync_ltf_outreach_sub_segment
    on h_crmcloudsync_ltf_outreach_sub_segment.bk_hash = p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
  join #p_crmcloudsync_ltf_outreach_sub_segment_insert
    on p_crmcloudsync_ltf_outreach_sub_segment.bk_hash = #p_crmcloudsync_ltf_outreach_sub_segment_insert.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.p_crmcloudsync_ltf_outreach_sub_segment_id = #p_crmcloudsync_ltf_outreach_sub_segment_insert.p_crmcloudsync_ltf_outreach_sub_segment_id
  join dbo.l_crmcloudsync_ltf_outreach_sub_segment
    on p_crmcloudsync_ltf_outreach_sub_segment.bk_hash = l_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id = l_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id
  join dbo.s_crmcloudsync_ltf_outreach_sub_segment
    on p_crmcloudsync_ltf_outreach_sub_segment.bk_hash = s_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id = s_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_outreach_sub_segment
   where d_crmcloudsync_ltf_outreach_sub_segment.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_outreach_sub_segment_insert)

  insert dbo.d_crmcloudsync_ltf_outreach_sub_segment(
             bk_hash,
             dim_crm_ltf_outreach_sub_segment_key,
             ltf_outreach_sub_segment_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             import_sequence_number,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_attribute_index,
             ltf_description,
             ltf_subsegment,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             version_number,
             deleted_flag,
             p_crmcloudsync_ltf_outreach_sub_segment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_outreach_sub_segment_key,
         ltf_outreach_sub_segment_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         import_sequence_number,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_attribute_index,
         ltf_description,
         ltf_subsegment,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         version_number,
         dv_deleted,
         p_crmcloudsync_ltf_outreach_sub_segment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_outreach_sub_segment)
--Done!
end
