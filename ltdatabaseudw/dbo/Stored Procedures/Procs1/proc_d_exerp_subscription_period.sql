CREATE PROC [dbo].[proc_d_exerp_subscription_period] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_period)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_subscription_period_insert') is not null drop table #p_exerp_subscription_period_insert
create table dbo.#p_exerp_subscription_period_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_period.p_exerp_subscription_period_id,
       p_exerp_subscription_period.bk_hash
  from dbo.p_exerp_subscription_period
 where p_exerp_subscription_period.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_subscription_period.dv_batch_id > @max_dv_batch_id
        or p_exerp_subscription_period.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_period.bk_hash,
       p_exerp_subscription_period.bk_hash dim_exerp_subscription_period_key,
       p_exerp_subscription_period.subscription_period_id subscription_period_id,
       case when p_exerp_subscription_period.bk_hash in ('-997', '-998', '-999') then p_exerp_subscription_period.bk_hash
             when l_exerp_subscription_period.center_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription_period.center_id as int) as varchar(500)),'z#@$k%&P'))),2)
         end dim_club_key,
       case when p_exerp_subscription_period.bk_hash in ('-997','-998','-999') then p_exerp_subscription_period.bk_hash     
         when l_exerp_subscription_period.subscription_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_period.subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_subscription_key,
       s_exerp_subscription_period.ets ets,
       case when p_exerp_subscription_period.bk_hash in ('-997', '-998', '-999') then p_exerp_subscription_period.bk_hash
             when l_exerp_subscription_period.sale_log_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_period.sale_log_id as varchar(4000)),'z#@$k%&P'))),2)
         end fact_exerp_transaction_log_key,
       case when p_exerp_subscription_period.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_period.bk_hash
           when s_exerp_subscription_period.from_date is null then '-998'
        else convert(varchar, s_exerp_subscription_period.from_date, 112)    end from_dim_date_key,
       s_exerp_subscription_period.state subscription_period_state,
       s_exerp_subscription_period.type subscription_period_type,
       case when p_exerp_subscription_period.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_period.bk_hash
           when s_exerp_subscription_period.to_date is null then '-998'
        else convert(varchar, s_exerp_subscription_period.to_date, 112)    end to_dim_date_key,
       isnull(h_exerp_subscription_period.dv_deleted,0) dv_deleted,
       p_exerp_subscription_period.p_exerp_subscription_period_id,
       p_exerp_subscription_period.dv_batch_id,
       p_exerp_subscription_period.dv_load_date_time,
       p_exerp_subscription_period.dv_load_end_date_time
  from dbo.h_exerp_subscription_period
  join dbo.p_exerp_subscription_period
    on h_exerp_subscription_period.bk_hash = p_exerp_subscription_period.bk_hash
  join #p_exerp_subscription_period_insert
    on p_exerp_subscription_period.bk_hash = #p_exerp_subscription_period_insert.bk_hash
   and p_exerp_subscription_period.p_exerp_subscription_period_id = #p_exerp_subscription_period_insert.p_exerp_subscription_period_id
  join dbo.l_exerp_subscription_period
    on p_exerp_subscription_period.bk_hash = l_exerp_subscription_period.bk_hash
   and p_exerp_subscription_period.l_exerp_subscription_period_id = l_exerp_subscription_period.l_exerp_subscription_period_id
  join dbo.s_exerp_subscription_period
    on p_exerp_subscription_period.bk_hash = s_exerp_subscription_period.bk_hash
   and p_exerp_subscription_period.s_exerp_subscription_period_id = s_exerp_subscription_period.s_exerp_subscription_period_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_subscription_period
   where d_exerp_subscription_period.bk_hash in (select bk_hash from #p_exerp_subscription_period_insert)

  insert dbo.d_exerp_subscription_period(
             bk_hash,
             dim_exerp_subscription_period_key,
             subscription_period_id,
             dim_club_key,
             dim_exerp_subscription_key,
             ets,
             fact_exerp_transaction_log_key,
             from_dim_date_key,
             subscription_period_state,
             subscription_period_type,
             to_dim_date_key,
             deleted_flag,
             p_exerp_subscription_period_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_subscription_period_key,
         subscription_period_id,
         dim_club_key,
         dim_exerp_subscription_key,
         ets,
         fact_exerp_transaction_log_key,
         from_dim_date_key,
         subscription_period_state,
         subscription_period_type,
         to_dim_date_key,
         dv_deleted,
         p_exerp_subscription_period_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_period)
--Done!
end
