CREATE PROC [dbo].[proc_d_crmcloudsync_annotation] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_annotation)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_annotation_insert') is not null drop table #p_crmcloudsync_annotation_insert
create table dbo.#p_crmcloudsync_annotation_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_annotation.p_crmcloudsync_annotation_id,
       p_crmcloudsync_annotation.bk_hash
  from dbo.p_crmcloudsync_annotation
 where p_crmcloudsync_annotation.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_annotation.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_annotation.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_annotation.bk_hash,
       p_crmcloudsync_annotation.bk_hash dim_crm_annotation_key,
       p_crmcloudsync_annotation.annotation_id annotation_id,
       case when p_crmcloudsync_annotation.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash 
    when l_crmcloudsync_annotation.created_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_annotation.created_by as varchar(36)),'z#@$k%&P'))),2) end  created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_annotation.created_by_name,'') created_by_name,
       case when p_crmcloudsync_annotation.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash
          when s_crmcloudsync_annotation.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_annotation.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_annotation.bk_hash in ('-997','-998','-999') then p_crmcloudsync_annotation.bk_hash
       when s_crmcloudsync_annotation.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_annotation.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_annotation.created_on created_on,
       case when p_crmcloudsync_annotation.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash     
       when l_crmcloudsync_annotation.owner_id is null then '-998'      
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_annotation.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       isnull(s_crmcloudsync_annotation.file_name,'') file_name,
       s_crmcloudsync_annotation.file_size file_size,
       s_crmcloudsync_annotation.is_document is_document,
       isnull(s_crmcloudsync_annotation.is_document_name,'') is_document_name,
       isnull(s_crmcloudsync_annotation.mime_type,'') mime_type,
       case when p_crmcloudsync_annotation.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash 
    when l_crmcloudsync_annotation.modified_by is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_annotation.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_annotation.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_annotation.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash
          when s_crmcloudsync_annotation.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_annotation.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_annotation.bk_hash in ('-997','-998','-999') then p_crmcloudsync_annotation.bk_hash
       when s_crmcloudsync_annotation.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_annotation.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_annotation.modified_on modified_on,
       isnull(s_crmcloudsync_annotation.note_text,'') note_text,
       l_crmcloudsync_annotation.object_id object_id,
       isnull(s_crmcloudsync_annotation.object_id_type_code,'') object_id_type_code,
       isnull(s_crmcloudsync_annotation.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_annotation.owner_id_type,'') owner_id_type,
       l_crmcloudsync_annotation.owning_business_unit owning_business_unit,
       l_crmcloudsync_annotation.owning_team owning_team,
       case when p_crmcloudsync_annotation.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_annotation.bk_hash
    when l_crmcloudsync_annotation.owning_user is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_annotation.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       isnull(s_crmcloudsync_annotation.subject,'') subject,
       isnull(h_crmcloudsync_annotation.dv_deleted,0) dv_deleted,
       p_crmcloudsync_annotation.p_crmcloudsync_annotation_id,
       p_crmcloudsync_annotation.dv_batch_id,
       p_crmcloudsync_annotation.dv_load_date_time,
       p_crmcloudsync_annotation.dv_load_end_date_time
  from dbo.h_crmcloudsync_annotation
  join dbo.p_crmcloudsync_annotation
    on h_crmcloudsync_annotation.bk_hash = p_crmcloudsync_annotation.bk_hash
  join #p_crmcloudsync_annotation_insert
    on p_crmcloudsync_annotation.bk_hash = #p_crmcloudsync_annotation_insert.bk_hash
   and p_crmcloudsync_annotation.p_crmcloudsync_annotation_id = #p_crmcloudsync_annotation_insert.p_crmcloudsync_annotation_id
  join dbo.l_crmcloudsync_annotation
    on p_crmcloudsync_annotation.bk_hash = l_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.l_crmcloudsync_annotation_id = l_crmcloudsync_annotation.l_crmcloudsync_annotation_id
  join dbo.s_crmcloudsync_annotation
    on p_crmcloudsync_annotation.bk_hash = s_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.s_crmcloudsync_annotation_id = s_crmcloudsync_annotation.s_crmcloudsync_annotation_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_annotation
   where d_crmcloudsync_annotation.bk_hash in (select bk_hash from #p_crmcloudsync_annotation_insert)

  insert dbo.d_crmcloudsync_annotation(
             bk_hash,
             dim_crm_annotation_key,
             annotation_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_owner_key,
             file_name,
             file_size,
             is_document,
             is_document_name,
             mime_type,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             note_text,
             object_id,
             object_id_type_code,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user_dim_crm_system_user_key,
             subject,
             deleted_flag,
             p_crmcloudsync_annotation_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_annotation_key,
         annotation_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_owner_key,
         file_name,
         file_size,
         is_document,
         is_document_name,
         mime_type,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         note_text,
         object_id,
         object_id_type_code,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user_dim_crm_system_user_key,
         subject,
         dv_deleted,
         p_crmcloudsync_annotation_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_annotation)
--Done!
end
