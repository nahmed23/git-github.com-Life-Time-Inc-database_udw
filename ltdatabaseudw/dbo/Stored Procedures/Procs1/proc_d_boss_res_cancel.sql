CREATE PROC [dbo].[proc_d_boss_res_cancel] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_res_cancel)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_res_cancel_insert') is not null drop table #p_boss_res_cancel_insert
create table dbo.#p_boss_res_cancel_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_res_cancel.p_boss_res_cancel_id,
       p_boss_res_cancel.bk_hash
  from dbo.p_boss_res_cancel
 where p_boss_res_cancel.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_res_cancel.dv_batch_id > @max_dv_batch_id
        or p_boss_res_cancel.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_res_cancel.bk_hash,
       case when p_boss_res_cancel.bk_hash in ('-997', '-998', '-999') then p_boss_res_cancel.bk_hash
            when p_boss_res_cancel.cancel_date is null then '-998'
            else convert(char(8), p_boss_res_cancel.cancel_date, 112)
        end cancel_dim_date_key,
       case when p_boss_res_cancel.bk_hash in ('-997', '-998', '-999') then p_boss_res_cancel.bk_hash
            when p_boss_res_cancel.reservation is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(p_boss_res_cancel.reservation as varchar(500)),'z#@$k%&P'))),2)
        end dim_boss_reservation_key,
       p_boss_res_cancel.p_boss_res_cancel_id,
       p_boss_res_cancel.dv_batch_id,
       p_boss_res_cancel.dv_load_date_time,
       p_boss_res_cancel.dv_load_end_date_time
  from dbo.p_boss_res_cancel
  join #p_boss_res_cancel_insert
    on p_boss_res_cancel.bk_hash = #p_boss_res_cancel_insert.bk_hash
   and p_boss_res_cancel.p_boss_res_cancel_id = #p_boss_res_cancel_insert.p_boss_res_cancel_id
  join dbo.l_boss_res_cancel
    on p_boss_res_cancel.bk_hash = l_boss_res_cancel.bk_hash
   and p_boss_res_cancel.l_boss_res_cancel_id = l_boss_res_cancel.l_boss_res_cancel_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_res_cancel
   where d_boss_res_cancel.bk_hash in (select bk_hash from #p_boss_res_cancel_insert)

  insert dbo.d_boss_res_cancel(
             bk_hash,
             cancel_dim_date_key,
             dim_boss_reservation_key,
             p_boss_res_cancel_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         cancel_dim_date_key,
         dim_boss_reservation_key,
         p_boss_res_cancel_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_res_cancel)
--Done!
end
