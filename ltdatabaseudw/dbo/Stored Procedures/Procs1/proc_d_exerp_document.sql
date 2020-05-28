CREATE PROC [dbo].[proc_d_exerp_document] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_document)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_document_insert') is not null drop table #p_exerp_document_insert
create table dbo.#p_exerp_document_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_document.p_exerp_document_id,
       p_exerp_document.bk_hash
  from dbo.p_exerp_document
 where p_exerp_document.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_document.dv_batch_id > @max_dv_batch_id
        or p_exerp_document.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_document.bk_hash,
       p_exerp_document.document_id document_id,
       l_exerp_document.center_id center_id,
       l_exerp_document.company_id company_id,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash
           when s_exerp_document.creation_datetime is null then '-998'
        else convert(varchar, s_exerp_document.creation_datetime, 112)    end creation_dim_date_key,
       case when p_exerp_document.bk_hash in ('-997','-998','-999') then p_exerp_document.bk_hash
       when s_exerp_document.creation_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_document.creation_datetime,114), 1, 5),':','') end creation_dim_time_key,
       l_exerp_document.creator_person_id creator_person_id,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash
           when l_exerp_document.center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_document.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash
           when l_exerp_document.person_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_document.person_id as varchar(4000)),'z#@$k%&P'))),2)   end d_exerp_person_bk_hash,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash
           when l_exerp_document.company_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_document.company_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mms_company_bk_hash,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash 
              when ((l_exerp_document.person_id is null) OR (l_exerp_document.person_id LIKE '%e%') or (l_exerp_document.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_document.person_id) > 9) or (d_exerp_person.person_type = 'STAFF' and l_exerp_document.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_document.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_document.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_document.attached_file_name document_attached_file_name,
       s_exerp_document.details document_details,
       case when s_exerp_document.require_signature = 1 then 'Y'        else 'N'  end document_require_signature_flag,
       case when s_exerp_document.signatures_missing = 1 then 'Y'        else 'N'  end document_signatures_missing_flag,
       case when s_exerp_document.signatures_signed = 1 then 'Y'        else 'N'  end document_signatures_signed_flag,
       s_exerp_document.subject document_subject,
       s_exerp_document.type document_type,
       s_exerp_document.ets ets,
       case when p_exerp_document.bk_hash in('-997', '-998', '-999') then p_exerp_document.bk_hash
           when s_exerp_document.latest_signed_datetime is null then '-998'
        else convert(varchar, s_exerp_document.latest_signed_datetime, 112)    end latest_signed_dim_date_key,
       case when p_exerp_document.bk_hash in ('-997','-998','-999') then p_exerp_document.bk_hash
       when s_exerp_document.latest_signed_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_document.latest_signed_datetime,114), 1, 5),':','') end latest_signed_dim_time_key,
       l_exerp_document.person_id person_id,
       isnull(h_exerp_document.dv_deleted,0) dv_deleted,
       p_exerp_document.p_exerp_document_id,
       p_exerp_document.dv_batch_id,
       p_exerp_document.dv_load_date_time,
       p_exerp_document.dv_load_end_date_time
  from dbo.h_exerp_document
  join dbo.p_exerp_document
    on h_exerp_document.bk_hash = p_exerp_document.bk_hash
  join #p_exerp_document_insert
    on p_exerp_document.bk_hash = #p_exerp_document_insert.bk_hash
   and p_exerp_document.p_exerp_document_id = #p_exerp_document_insert.p_exerp_document_id
  join dbo.l_exerp_document
    on p_exerp_document.bk_hash = l_exerp_document.bk_hash
   and p_exerp_document.l_exerp_document_id = l_exerp_document.l_exerp_document_id
  join dbo.s_exerp_document
    on p_exerp_document.bk_hash = s_exerp_document.bk_hash
   and p_exerp_document.s_exerp_document_id = s_exerp_document.s_exerp_document_id
 left join 	d_exerp_person		on l_exerp_document.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_document
   where d_exerp_document.bk_hash in (select bk_hash from #p_exerp_document_insert)

  insert dbo.d_exerp_document(
             bk_hash,
             document_id,
             center_id,
             company_id,
             creation_dim_date_key,
             creation_dim_time_key,
             creator_person_id,
             d_exerp_center_bk_hash,
             d_exerp_person_bk_hash,
             d_mms_company_bk_hash,
             dim_mms_member_key,
             document_attached_file_name,
             document_details,
             document_require_signature_flag,
             document_signatures_missing_flag,
             document_signatures_signed_flag,
             document_subject,
             document_type,
             ets,
             latest_signed_dim_date_key,
             latest_signed_dim_time_key,
             person_id,
             deleted_flag,
             p_exerp_document_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         document_id,
         center_id,
         company_id,
         creation_dim_date_key,
         creation_dim_time_key,
         creator_person_id,
         d_exerp_center_bk_hash,
         d_exerp_person_bk_hash,
         d_mms_company_bk_hash,
         dim_mms_member_key,
         document_attached_file_name,
         document_details,
         document_require_signature_flag,
         document_signatures_missing_flag,
         document_signatures_signed_flag,
         document_subject,
         document_type,
         ets,
         latest_signed_dim_date_key,
         latest_signed_dim_time_key,
         person_id,
         dv_deleted,
         p_exerp_document_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_document)
--Done!
end
