CREATE PROC [dbo].[proc_d_commprefs_communication_values] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_values)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_commprefs_communication_values_insert') is not null drop table #p_commprefs_communication_values_insert
create table dbo.#p_commprefs_communication_values_insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_values.p_commprefs_communication_values_id,
       p_commprefs_communication_values.bk_hash
  from dbo.p_commprefs_communication_values
 where p_commprefs_communication_values.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_commprefs_communication_values.dv_batch_id > @max_dv_batch_id
        or p_commprefs_communication_values.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_values.bk_hash,
       p_commprefs_communication_values.bk_hash d_commprefs_communication_values_key,
       p_commprefs_communication_values.communication_values_id communication_values_id,
       isnull(l_commprefs_communication_values.channel_key,'-998') channel_key,
       case when p_commprefs_communication_values.bk_hash in ('-997', '-998', '-999') then p_commprefs_communication_values.bk_hash   
    when s_commprefs_communication_values.created_time is null then '-998'   
	 else convert(char(8), s_commprefs_communication_values.created_time, 112)   end created_date_key,
       isnull(s_commprefs_communication_values.token,'') token,
       s_commprefs_communication_values.token_created_time token_created_time,
       case when p_commprefs_communication_values.bk_hash in ('-997', '-998', '-999') then p_commprefs_communication_values.bk_hash   
    when s_commprefs_communication_values.updated_time is null then '-998'   
	 else convert(char(8), s_commprefs_communication_values.updated_time, 112)   end updated_date_key,
       isnull(s_commprefs_communication_values.value,'') value,
       p_commprefs_communication_values.p_commprefs_communication_values_id,
       p_commprefs_communication_values.dv_batch_id,
       p_commprefs_communication_values.dv_load_date_time,
       p_commprefs_communication_values.dv_load_end_date_time
  from dbo.h_commprefs_communication_values
  join dbo.p_commprefs_communication_values
    on h_commprefs_communication_values.bk_hash = p_commprefs_communication_values.bk_hash  join #p_commprefs_communication_values_insert
    on p_commprefs_communication_values.bk_hash = #p_commprefs_communication_values_insert.bk_hash
   and p_commprefs_communication_values.p_commprefs_communication_values_id = #p_commprefs_communication_values_insert.p_commprefs_communication_values_id
  join dbo.l_commprefs_communication_values
    on p_commprefs_communication_values.bk_hash = l_commprefs_communication_values.bk_hash
   and p_commprefs_communication_values.l_commprefs_communication_values_id = l_commprefs_communication_values.l_commprefs_communication_values_id
  join dbo.s_commprefs_communication_values
    on p_commprefs_communication_values.bk_hash = s_commprefs_communication_values.bk_hash
   and p_commprefs_communication_values.s_commprefs_communication_values_id = s_commprefs_communication_values.s_commprefs_communication_values_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_commprefs_communication_values
   where d_commprefs_communication_values.bk_hash in (select bk_hash from #p_commprefs_communication_values_insert)

  insert dbo.d_commprefs_communication_values(
             bk_hash,
             d_commprefs_communication_values_key,
             communication_values_id,
             channel_key,
             created_date_key,
             token,
             token_created_time,
             updated_date_key,
             value,
             p_commprefs_communication_values_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_commprefs_communication_values_key,
         communication_values_id,
         channel_key,
         created_date_key,
         token,
         token_created_time,
         updated_date_key,
         value,
         p_commprefs_communication_values_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_values)
--Done!
end
