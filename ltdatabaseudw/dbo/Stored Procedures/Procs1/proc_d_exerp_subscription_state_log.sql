CREATE PROC [dbo].[proc_d_exerp_subscription_state_log] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_state_log)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_subscription_state_log_insert') is not null drop table #p_exerp_subscription_state_log_insert
create table dbo.#p_exerp_subscription_state_log_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_state_log.p_exerp_subscription_state_log_id,
       p_exerp_subscription_state_log.bk_hash
  from dbo.p_exerp_subscription_state_log
 where p_exerp_subscription_state_log.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_subscription_state_log.dv_batch_id > @max_dv_batch_id
        or p_exerp_subscription_state_log.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_state_log.bk_hash,
       p_exerp_subscription_state_log.bk_hash dim_exerp_subscription_state_log_key,
       p_exerp_subscription_state_log.subscription_state_log_id subscription_state_log_id,
       case when p_exerp_subscription_state_log.bk_hash in ('-997','-998','-999') then p_exerp_subscription_state_log.bk_hash     
         when l_exerp_subscription_state_log.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription_state_log.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_subscription_state_log.bk_hash in ('-997','-998','-999') then p_exerp_subscription_state_log.bk_hash     
         when l_exerp_subscription_state_log.subscription_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_state_log.subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_subscription_key,
       case when p_exerp_subscription_state_log.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_state_log.bk_hash
           when s_exerp_subscription_state_log.entry_start_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription_state_log.entry_start_datetime, 112)    end entry_start_dim_date_key,
       case when p_exerp_subscription_state_log.bk_hash in ('-997','-998','-999') then p_exerp_subscription_state_log.bk_hash
       when s_exerp_subscription_state_log.entry_start_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription_state_log.entry_start_datetime,114), 1, 5),':','') end entry_start_dim_time_key,
       s_exerp_subscription_state_log.ets ets,
       s_exerp_subscription_state_log.sub_state sub_state,
       s_exerp_subscription_state_log.state subscription_state_log_state,
       isnull(h_exerp_subscription_state_log.dv_deleted,0) dv_deleted,
       p_exerp_subscription_state_log.p_exerp_subscription_state_log_id,
       p_exerp_subscription_state_log.dv_batch_id,
       p_exerp_subscription_state_log.dv_load_date_time,
       p_exerp_subscription_state_log.dv_load_end_date_time
  from dbo.h_exerp_subscription_state_log
  join dbo.p_exerp_subscription_state_log
    on h_exerp_subscription_state_log.bk_hash = p_exerp_subscription_state_log.bk_hash
  join #p_exerp_subscription_state_log_insert
    on p_exerp_subscription_state_log.bk_hash = #p_exerp_subscription_state_log_insert.bk_hash
   and p_exerp_subscription_state_log.p_exerp_subscription_state_log_id = #p_exerp_subscription_state_log_insert.p_exerp_subscription_state_log_id
  join dbo.l_exerp_subscription_state_log
    on p_exerp_subscription_state_log.bk_hash = l_exerp_subscription_state_log.bk_hash
   and p_exerp_subscription_state_log.l_exerp_subscription_state_log_id = l_exerp_subscription_state_log.l_exerp_subscription_state_log_id
  join dbo.s_exerp_subscription_state_log
    on p_exerp_subscription_state_log.bk_hash = s_exerp_subscription_state_log.bk_hash
   and p_exerp_subscription_state_log.s_exerp_subscription_state_log_id = s_exerp_subscription_state_log.s_exerp_subscription_state_log_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_subscription_state_log
   where d_exerp_subscription_state_log.bk_hash in (select bk_hash from #p_exerp_subscription_state_log_insert)

  insert dbo.d_exerp_subscription_state_log(
             bk_hash,
             dim_exerp_subscription_state_log_key,
             subscription_state_log_id,
             dim_club_key,
             dim_exerp_subscription_key,
             entry_start_dim_date_key,
             entry_start_dim_time_key,
             ets,
             sub_state,
             subscription_state_log_state,
             deleted_flag,
             p_exerp_subscription_state_log_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_subscription_state_log_key,
         subscription_state_log_id,
         dim_club_key,
         dim_exerp_subscription_key,
         entry_start_dim_date_key,
         entry_start_dim_time_key,
         ets,
         sub_state,
         subscription_state_log_state,
         dv_deleted,
         p_exerp_subscription_state_log_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_state_log)
--Done!
end
