CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_inquiry] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_inquiry)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_inquiry_insert') is not null drop table #p_crmcloudsync_ltf_inquiry_insert
create table dbo.#p_crmcloudsync_ltf_inquiry_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id,
       p_crmcloudsync_ltf_inquiry.bk_hash
  from dbo.p_crmcloudsync_ltf_inquiry
 where p_crmcloudsync_ltf_inquiry.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_inquiry.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_inquiry.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_inquiry.bk_hash,
       p_crmcloudsync_ltf_inquiry.bk_hash dim_crm_ltf_inquiry_key,
       p_crmcloudsync_ltf_inquiry.activity_id activity_id,
       ISNULL(s_crmcloudsync_ltf_inquiry.ltf_inquiry_source,'') contact_source,
       case when p_crmcloudsync_ltf_inquiry.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_inquiry.bk_hash       when l_crmcloudsync_ltf_inquiry.ltf_mms_clubid is null then '-998'
else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_inquiry.ltf_mms_clubid as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       s_crmcloudsync_ltf_inquiry.ltf_first_name first_name,
       s_crmcloudsync_ltf_inquiry.ltf_last_name last_name,
       ISNULL(s_crmcloudsync_ltf_inquiry.ltf_inquiry_type,'') lead_source,
       case when p_crmcloudsync_ltf_inquiry.bk_hash in ('-997','-998','-999') then 'N'
            when ISNULL(l_crmcloudsync_ltf_inquiry.ltf_referring_member_id,'') = '' then 'N'
            else 'Y'
        end referring_member_flag,
       s_crmcloudsync_ltf_inquiry.status_code_name state_code_name,
       isnull(h_crmcloudsync_ltf_inquiry.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id,
       p_crmcloudsync_ltf_inquiry.dv_batch_id,
       p_crmcloudsync_ltf_inquiry.dv_load_date_time,
       p_crmcloudsync_ltf_inquiry.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_inquiry
  join dbo.p_crmcloudsync_ltf_inquiry
    on h_crmcloudsync_ltf_inquiry.bk_hash = p_crmcloudsync_ltf_inquiry.bk_hash
  join #p_crmcloudsync_ltf_inquiry_insert
    on p_crmcloudsync_ltf_inquiry.bk_hash = #p_crmcloudsync_ltf_inquiry_insert.bk_hash
   and p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id = #p_crmcloudsync_ltf_inquiry_insert.p_crmcloudsync_ltf_inquiry_id
  join dbo.l_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.bk_hash = l_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id = l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id
  join dbo.s_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.bk_hash = s_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id = s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_inquiry
   where d_crmcloudsync_ltf_inquiry.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_inquiry_insert)

  insert dbo.d_crmcloudsync_ltf_inquiry(
             bk_hash,
             dim_crm_ltf_inquiry_key,
             activity_id,
             contact_source,
             dim_club_key,
             first_name,
             last_name,
             lead_source,
             referring_member_flag,
             state_code_name,
             deleted_flag,
             p_crmcloudsync_ltf_inquiry_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_inquiry_key,
         activity_id,
         contact_source,
         dim_club_key,
         first_name,
         last_name,
         lead_source,
         referring_member_flag,
         state_code_name,
         dv_deleted,
         p_crmcloudsync_ltf_inquiry_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_inquiry)
--Done!
end
