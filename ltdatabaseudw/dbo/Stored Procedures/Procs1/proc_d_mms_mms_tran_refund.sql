CREATE PROC [dbo].[proc_d_mms_mms_tran_refund] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_mms_tran_refund)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_mms_tran_refund_insert') is not null drop table #p_mms_mms_tran_refund_insert
create table dbo.#p_mms_mms_tran_refund_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_mms_tran_refund.p_mms_mms_tran_refund_id,
       p_mms_mms_tran_refund.bk_hash
  from dbo.p_mms_mms_tran_refund
 where p_mms_mms_tran_refund.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_mms_tran_refund.dv_batch_id > @max_dv_batch_id
        or p_mms_mms_tran_refund.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_mms_tran_refund.bk_hash,
       p_mms_mms_tran_refund.mms_tran_refund_id mms_tran_refund_id,
       case when p_mms_mms_tran_refund.bk_hash in('-997', '-998', '-999') then p_mms_mms_tran_refund.bk_hash
           when l_mms_mms_tran_refund.mms_tran_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran_refund.mms_tran_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_sales_transaction_key,
       s_mms_mms_tran_refund.inserted_date_time inserted_date_time,
       case when p_mms_mms_tran_refund.bk_hash in('-997', '-998', '-999') then p_mms_mms_tran_refund.bk_hash
           when s_mms_mms_tran_refund.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_mms_tran_refund.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_mms_tran_refund.bk_hash in ('-997','-998','-999') then p_mms_mms_tran_refund.bk_hash
       when s_mms_mms_tran_refund.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_mms_tran_refund.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_mms_tran_refund.mms_tran_id mms_tran_id,
       l_mms_mms_tran_refund.requesting_club_id requesting_club_id,
       case when p_mms_mms_tran_refund.bk_hash in('-997', '-998', '-999') then p_mms_mms_tran_refund.bk_hash
           when l_mms_mms_tran_refund.requesting_club_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran_refund.requesting_club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end requesting_dim_club_key,
       s_mms_mms_tran_refund.updated_date_time updated_date_time,
       case when p_mms_mms_tran_refund.bk_hash in('-997', '-998', '-999') then p_mms_mms_tran_refund.bk_hash
           when s_mms_mms_tran_refund.updated_date_time is null then '-998'
        else convert(varchar, s_mms_mms_tran_refund.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_mms_tran_refund.bk_hash in ('-997','-998','-999') then p_mms_mms_tran_refund.bk_hash
       when s_mms_mms_tran_refund.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_mms_tran_refund.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_mms_mms_tran_refund.dv_deleted,0) dv_deleted,
       p_mms_mms_tran_refund.p_mms_mms_tran_refund_id,
       p_mms_mms_tran_refund.dv_batch_id,
       p_mms_mms_tran_refund.dv_load_date_time,
       p_mms_mms_tran_refund.dv_load_end_date_time
  from dbo.h_mms_mms_tran_refund
  join dbo.p_mms_mms_tran_refund
    on h_mms_mms_tran_refund.bk_hash = p_mms_mms_tran_refund.bk_hash
  join #p_mms_mms_tran_refund_insert
    on p_mms_mms_tran_refund.bk_hash = #p_mms_mms_tran_refund_insert.bk_hash
   and p_mms_mms_tran_refund.p_mms_mms_tran_refund_id = #p_mms_mms_tran_refund_insert.p_mms_mms_tran_refund_id
  join dbo.l_mms_mms_tran_refund
    on p_mms_mms_tran_refund.bk_hash = l_mms_mms_tran_refund.bk_hash
   and p_mms_mms_tran_refund.l_mms_mms_tran_refund_id = l_mms_mms_tran_refund.l_mms_mms_tran_refund_id
  join dbo.s_mms_mms_tran_refund
    on p_mms_mms_tran_refund.bk_hash = s_mms_mms_tran_refund.bk_hash
   and p_mms_mms_tran_refund.s_mms_mms_tran_refund_id = s_mms_mms_tran_refund.s_mms_mms_tran_refund_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_mms_tran_refund
   where d_mms_mms_tran_refund.bk_hash in (select bk_hash from #p_mms_mms_tran_refund_insert)

  insert dbo.d_mms_mms_tran_refund(
             bk_hash,
             mms_tran_refund_id,
             fact_mms_sales_transaction_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             mms_tran_id,
             requesting_club_id,
             requesting_dim_club_key,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_mms_mms_tran_refund_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         mms_tran_refund_id,
         fact_mms_sales_transaction_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         mms_tran_id,
         requesting_club_id,
         requesting_dim_club_key,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_mms_mms_tran_refund_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_mms_tran_refund)
--Done!
end
