CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_live_chat] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_live_chat)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_live_chat_insert') is not null drop table #p_crmcloudsync_ltf_live_chat_insert
create table dbo.#p_crmcloudsync_ltf_live_chat_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_live_chat.p_crmcloudsync_ltf_live_chat_id,
       p_crmcloudsync_ltf_live_chat.bk_hash
  from dbo.p_crmcloudsync_ltf_live_chat
 where p_crmcloudsync_ltf_live_chat.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_live_chat.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_live_chat.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_live_chat.bk_hash,
       p_crmcloudsync_ltf_live_chat.activity_id activity_id,
       case when p_crmcloudsync_ltf_live_chat.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_live_chat.bk_hash        when s_crmcloudsync_ltf_live_chat.actual_start is null then '-998'    	 else convert(char(8), s_crmcloudsync_ltf_live_chat.actual_start, 112)  end actual_start_dim_date_key,
       s_crmcloudsync_ltf_live_chat.description description,
       case when p_crmcloudsync_ltf_live_chat.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_live_chat.bk_hash        when l_crmcloudsync_ltf_live_chat.ltf_mms_club_id is null then '-998'    	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_live_chat.ltf_mms_club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       s_crmcloudsync_ltf_live_chat.ltf_club_name ltf_club_name,
       s_crmcloudsync_ltf_live_chat.ltf_email_address_1 ltf_email_address_1,
       s_crmcloudsync_ltf_live_chat.ltf_first_name ltf_first_name,
       s_crmcloudsync_ltf_live_chat.ltf_last_name ltf_last_name,
       s_crmcloudsync_ltf_live_chat.ltf_line_of_business ltf_line_of_business,
       s_crmcloudsync_ltf_live_chat.ltf_line_of_business_name ltf_line_of_business_name,
       s_crmcloudsync_ltf_live_chat.ltf_referring_url ltf_referring_url,
       s_crmcloudsync_ltf_live_chat.ltf_transcript ltf_transcript,
       s_crmcloudsync_ltf_live_chat.subject subject,
       isnull(h_crmcloudsync_ltf_live_chat.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_live_chat.p_crmcloudsync_ltf_live_chat_id,
       p_crmcloudsync_ltf_live_chat.dv_batch_id,
       p_crmcloudsync_ltf_live_chat.dv_load_date_time,
       p_crmcloudsync_ltf_live_chat.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_live_chat
  join dbo.p_crmcloudsync_ltf_live_chat
    on h_crmcloudsync_ltf_live_chat.bk_hash = p_crmcloudsync_ltf_live_chat.bk_hash
  join #p_crmcloudsync_ltf_live_chat_insert
    on p_crmcloudsync_ltf_live_chat.bk_hash = #p_crmcloudsync_ltf_live_chat_insert.bk_hash
   and p_crmcloudsync_ltf_live_chat.p_crmcloudsync_ltf_live_chat_id = #p_crmcloudsync_ltf_live_chat_insert.p_crmcloudsync_ltf_live_chat_id
  join dbo.l_crmcloudsync_ltf_live_chat
    on p_crmcloudsync_ltf_live_chat.bk_hash = l_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id = l_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id
  join dbo.s_crmcloudsync_ltf_live_chat
    on p_crmcloudsync_ltf_live_chat.bk_hash = s_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id = s_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_live_chat
   where d_crmcloudsync_ltf_live_chat.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_live_chat_insert)

  insert dbo.d_crmcloudsync_ltf_live_chat(
             bk_hash,
             activity_id,
             actual_start_dim_date_key,
             description,
             dim_club_key,
             ltf_club_name,
             ltf_email_address_1,
             ltf_first_name,
             ltf_last_name,
             ltf_line_of_business,
             ltf_line_of_business_name,
             ltf_referring_url,
             ltf_transcript,
             subject,
             deleted_flag,
             p_crmcloudsync_ltf_live_chat_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         activity_id,
         actual_start_dim_date_key,
         description,
         dim_club_key,
         ltf_club_name,
         ltf_email_address_1,
         ltf_first_name,
         ltf_last_name,
         ltf_line_of_business,
         ltf_line_of_business_name,
         ltf_referring_url,
         ltf_transcript,
         subject,
         dv_deleted,
         p_crmcloudsync_ltf_live_chat_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_live_chat)
--Done!
end
