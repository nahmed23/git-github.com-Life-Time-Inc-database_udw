CREATE PROC [dbo].[proc_d_commprefs_communication_type_channel_membership_segments] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_type_channel_membership_segments)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_commprefs_communication_type_channel_membership_segments_insert') is not null drop table #p_commprefs_communication_type_channel_membership_segments_insert
create table dbo.#p_commprefs_communication_type_channel_membership_segments_insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_type_channel_membership_segments.p_commprefs_communication_type_channel_membership_segments_id,
       p_commprefs_communication_type_channel_membership_segments.bk_hash
  from dbo.p_commprefs_communication_type_channel_membership_segments
 where p_commprefs_communication_type_channel_membership_segments.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_commprefs_communication_type_channel_membership_segments.dv_batch_id > @max_dv_batch_id
        or p_commprefs_communication_type_channel_membership_segments.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_type_channel_membership_segments.bk_hash,
       p_commprefs_communication_type_channel_membership_segments.bk_hash d_commprefs_communication_type_channel_membership_segments_key,
       p_commprefs_communication_type_channel_membership_segments.communication_type_channel_membership_segments_id communication_type_channel_membership_segments_id,
       case when p_commprefs_communication_type_channel_membership_segments.bk_hash in ('-997', '-998', '-999') then p_commprefs_communication_type_channel_membership_segments.bk_hash   
    when s_commprefs_communication_type_channel_membership_segments.created_time is null then '-998'   
	 else convert(char(8), s_commprefs_communication_type_channel_membership_segments.created_time, 112)   end created_date_key,
       case when p_commprefs_communication_type_channel_membership_segments.bk_hash in ('-997','-998','-999') then p_commprefs_communication_type_channel_membership_segments.bk_hash     
  when l_commprefs_communication_type_channel_membership_segments.communication_type_channel_id is null then '-998'   
  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_commprefs_communication_type_channel_membership_segments.communication_type_channel_id as varchar(500)),'z#@$k%&P'))),2)   end d_commprefs_communication_type_channels_bk_hash,
       case when p_commprefs_communication_type_channel_membership_segments.bk_hash in ('-997','-998','-999') then p_commprefs_communication_type_channel_membership_segments.bk_hash     
  when l_commprefs_communication_type_channel_membership_segments.membership_segment_id is null then '-998'   
  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_commprefs_communication_type_channel_membership_segments.membership_segment_id as varchar(500)),'z#@$k%&P'))),2)   end d_commprefs_membership_segments_bk_hash,
       case when s_commprefs_communication_type_channel_membership_segments.opt_in_default= '1' then 'Y'  
else 'N' end opt_in_default_flag,
       case when s_commprefs_communication_type_channel_membership_segments.show= '1' then 'Y'  
else 'N' end show_flag,
       case when p_commprefs_communication_type_channel_membership_segments.bk_hash in ('-997', '-998', '-999') then p_commprefs_communication_type_channel_membership_segments.bk_hash   
    when s_commprefs_communication_type_channel_membership_segments.updated_time is null then '-998'   
	 else convert(char(8), s_commprefs_communication_type_channel_membership_segments.updated_time, 112)   end updated_date_key,
       p_commprefs_communication_type_channel_membership_segments.p_commprefs_communication_type_channel_membership_segments_id,
       p_commprefs_communication_type_channel_membership_segments.dv_batch_id,
       p_commprefs_communication_type_channel_membership_segments.dv_load_date_time,
       p_commprefs_communication_type_channel_membership_segments.dv_load_end_date_time
  from dbo.h_commprefs_communication_type_channel_membership_segments
  join dbo.p_commprefs_communication_type_channel_membership_segments
    on h_commprefs_communication_type_channel_membership_segments.bk_hash = p_commprefs_communication_type_channel_membership_segments.bk_hash  join #p_commprefs_communication_type_channel_membership_segments_insert
    on p_commprefs_communication_type_channel_membership_segments.bk_hash = #p_commprefs_communication_type_channel_membership_segments_insert.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.p_commprefs_communication_type_channel_membership_segments_id = #p_commprefs_communication_type_channel_membership_segments_insert.p_commprefs_communication_type_channel_membership_segments_id
  join dbo.l_commprefs_communication_type_channel_membership_segments
    on p_commprefs_communication_type_channel_membership_segments.bk_hash = l_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id = l_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id
  join dbo.s_commprefs_communication_type_channel_membership_segments
    on p_commprefs_communication_type_channel_membership_segments.bk_hash = s_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id = s_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_commprefs_communication_type_channel_membership_segments
   where d_commprefs_communication_type_channel_membership_segments.bk_hash in (select bk_hash from #p_commprefs_communication_type_channel_membership_segments_insert)

  insert dbo.d_commprefs_communication_type_channel_membership_segments(
             bk_hash,
             d_commprefs_communication_type_channel_membership_segments_key,
             communication_type_channel_membership_segments_id,
             created_date_key,
             d_commprefs_communication_type_channels_bk_hash,
             d_commprefs_membership_segments_bk_hash,
             opt_in_default_flag,
             show_flag,
             updated_date_key,
             p_commprefs_communication_type_channel_membership_segments_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_commprefs_communication_type_channel_membership_segments_key,
         communication_type_channel_membership_segments_id,
         created_date_key,
         d_commprefs_communication_type_channels_bk_hash,
         d_commprefs_membership_segments_bk_hash,
         opt_in_default_flag,
         show_flag,
         updated_date_key,
         p_commprefs_communication_type_channel_membership_segments_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_type_channel_membership_segments)
--Done!
end
