CREATE PROC [dbo].[proc_d_mms_pt_credit_card_batch] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_batch)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_pt_credit_card_batch_insert') is not null drop table #p_mms_pt_credit_card_batch_insert
create table dbo.#p_mms_pt_credit_card_batch_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_batch.p_mms_pt_credit_card_batch_id,
       p_mms_pt_credit_card_batch.bk_hash
  from dbo.p_mms_pt_credit_card_batch
 where p_mms_pt_credit_card_batch.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_pt_credit_card_batch.dv_batch_id > @max_dv_batch_id
        or p_mms_pt_credit_card_batch.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_batch.bk_hash,
       p_mms_pt_credit_card_batch.bk_hash fact_mms_pt_credit_card_batch_key,
       p_mms_pt_credit_card_batch.pt_credit_card_batch_id pt_credit_card_batch_id,
       s_mms_pt_credit_card_batch.close_date_time batch_close_date_time,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
            when s_mms_pt_credit_card_batch.close_date_time is null then '-998'
            else convert(varchar, s_mms_pt_credit_card_batch.close_date_time, 112)
         end batch_close_dim_date_key,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
            when s_mms_pt_credit_card_batch.close_date_time is null then '-998'
            else '1' + replace(substring(convert(varchar,s_mms_pt_credit_card_batch.close_date_time,114), 1, 5),':','')
        end batch_close_dim_time_key,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997','-998','-999') then null
       	  when s_mms_pt_credit_card_batch.close_date_time is null then 'N' 
             else 'Y'
         end batch_closed_flag,
       s_mms_pt_credit_card_batch.open_date_time batch_open_date_time,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
              when s_mms_pt_credit_card_batch.open_date_time is null then '-998'
              else convert(varchar, s_mms_pt_credit_card_batch.open_date_time, 112)
         end batch_open_dim_date_key,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
              when s_mms_pt_credit_card_batch.open_date_time is null then '-998'
              else '1' + replace(substring(convert(varchar,s_mms_pt_credit_card_batch.open_date_time,114), 1, 5),':','')
        end batch_open_dim_time_key,
       s_mms_pt_credit_card_batch.submit_date_time batch_submit_date_time,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
            when s_mms_pt_credit_card_batch.submit_date_time is null then '-998'
            else convert(varchar, s_mms_pt_credit_card_batch.submit_date_time, 112)
         end batch_submit_dim_date_key,
       case when p_mms_pt_credit_card_batch.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_batch.bk_hash
            when s_mms_pt_credit_card_batch.submit_date_time is null then '-998'
            else '1' + replace(substring(convert(varchar,s_mms_pt_credit_card_batch.submit_date_time,114), 1, 5),':','')
        end batch_submit_dim_time_key,
       case when l_mms_pt_credit_card_batch.bk_hash in ('-997','-998','-999') then l_mms_pt_credit_card_batch.bk_hash
             when l_mms_pt_credit_card_batch.drawer_activity_id is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_batch.drawer_activity_id as varchar(500)),'z#@$k%&P'))),2)
         end dim_mms_drawer_activity_key,
       case when l_mms_pt_credit_card_batch.bk_hash in ('-997','-998','-999') then l_mms_pt_credit_card_batch.bk_hash
             when l_mms_pt_credit_card_batch.pt_credit_card_terminal_id is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_batch.pt_credit_card_terminal_id as varchar(500)),'z#@$k%&P'))),2)
         end fact_mms_pt_credit_card_terminal_key,
       p_mms_pt_credit_card_batch.p_mms_pt_credit_card_batch_id,
       p_mms_pt_credit_card_batch.dv_batch_id,
       p_mms_pt_credit_card_batch.dv_load_date_time,
       p_mms_pt_credit_card_batch.dv_load_end_date_time
  from dbo.p_mms_pt_credit_card_batch
  join #p_mms_pt_credit_card_batch_insert
    on p_mms_pt_credit_card_batch.bk_hash = #p_mms_pt_credit_card_batch_insert.bk_hash
   and p_mms_pt_credit_card_batch.p_mms_pt_credit_card_batch_id = #p_mms_pt_credit_card_batch_insert.p_mms_pt_credit_card_batch_id
  join dbo.l_mms_pt_credit_card_batch
    on p_mms_pt_credit_card_batch.bk_hash = l_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id = l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id
  join dbo.s_mms_pt_credit_card_batch
    on p_mms_pt_credit_card_batch.bk_hash = s_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id = s_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_pt_credit_card_batch
   where d_mms_pt_credit_card_batch.bk_hash in (select bk_hash from #p_mms_pt_credit_card_batch_insert)

  insert dbo.d_mms_pt_credit_card_batch(
             bk_hash,
             fact_mms_pt_credit_card_batch_key,
             pt_credit_card_batch_id,
             batch_close_date_time,
             batch_close_dim_date_key,
             batch_close_dim_time_key,
             batch_closed_flag,
             batch_open_date_time,
             batch_open_dim_date_key,
             batch_open_dim_time_key,
             batch_submit_date_time,
             batch_submit_dim_date_key,
             batch_submit_dim_time_key,
             dim_mms_drawer_activity_key,
             fact_mms_pt_credit_card_terminal_key,
             p_mms_pt_credit_card_batch_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_pt_credit_card_batch_key,
         pt_credit_card_batch_id,
         batch_close_date_time,
         batch_close_dim_date_key,
         batch_close_dim_time_key,
         batch_closed_flag,
         batch_open_date_time,
         batch_open_dim_date_key,
         batch_open_dim_time_key,
         batch_submit_date_time,
         batch_submit_dim_date_key,
         batch_submit_dim_time_key,
         dim_mms_drawer_activity_key,
         fact_mms_pt_credit_card_terminal_key,
         p_mms_pt_credit_card_batch_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_batch)
--Done!
end
