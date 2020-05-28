CREATE PROC [dbo].[proc_d_mms_payment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_payment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_payment_insert') is not null drop table #p_mms_payment_insert
create table dbo.#p_mms_payment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_payment.p_mms_payment_id,
       p_mms_payment.bk_hash
  from dbo.p_mms_payment
 where p_mms_payment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_payment.dv_batch_id > @max_dv_batch_id
        or p_mms_payment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_payment.bk_hash,
       p_mms_payment.bk_hash fact_mms_payment_key,
       p_mms_payment.payment_id payment_id,
       isnull(s_mms_payment.approval_code, '') approval_code,
       s_mms_payment.inserted_date_time mms_inserted_date_time,
       l_mms_payment.mms_tran_id mms_tran_id,
       isnull(s_mms_payment.payment_amount, 0) payment_amount,
       case when p_mms_payment.bk_hash in ('-997', '-998', '-999') then p_mms_payment.bk_hash 
              when s_mms_payment.inserted_date_time is null then '-998'    
              else convert(varchar, s_mms_payment.inserted_date_time, 112)
       end payment_dim_date_key,
       case when p_mms_payment.bk_hash in ('-997', '-998', '-999') then p_mms_payment.bk_hash  
            when s_mms_payment.inserted_date_time is null then '-998'
                else '1' + replace(substring(convert(varchar,s_mms_payment.inserted_date_time,114), 1, 5),':','')
       end payment_dim_time_key,
       case when p_mms_payment.bk_hash in ('-997','-998','-999') then p_mms_payment.bk_hash        
       when l_mms_payment.val_payment_type_id is null then '-998'    
       else 'r_mms_val_payment_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_payment.val_payment_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end payment_type_dim_description_key,
       isnull(s_mms_payment.tip_amount, 0) tip_amount,
       isnull(cast(l_mms_payment.val_payment_type_id as int),-998) val_payment_type_id,
       isnull(h_mms_payment.dv_deleted,0) dv_deleted,
       p_mms_payment.p_mms_payment_id,
       p_mms_payment.dv_batch_id,
       p_mms_payment.dv_load_date_time,
       p_mms_payment.dv_load_end_date_time
  from dbo.h_mms_payment
  join dbo.p_mms_payment
    on h_mms_payment.bk_hash = p_mms_payment.bk_hash
  join #p_mms_payment_insert
    on p_mms_payment.bk_hash = #p_mms_payment_insert.bk_hash
   and p_mms_payment.p_mms_payment_id = #p_mms_payment_insert.p_mms_payment_id
  join dbo.l_mms_payment
    on p_mms_payment.bk_hash = l_mms_payment.bk_hash
   and p_mms_payment.l_mms_payment_id = l_mms_payment.l_mms_payment_id
  join dbo.s_mms_payment
    on p_mms_payment.bk_hash = s_mms_payment.bk_hash
   and p_mms_payment.s_mms_payment_id = s_mms_payment.s_mms_payment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_payment
   where d_mms_payment.bk_hash in (select bk_hash from #p_mms_payment_insert)

  insert dbo.d_mms_payment(
             bk_hash,
             fact_mms_payment_key,
             payment_id,
             approval_code,
             mms_inserted_date_time,
             mms_tran_id,
             payment_amount,
             payment_dim_date_key,
             payment_dim_time_key,
             payment_type_dim_description_key,
             tip_amount,
             val_payment_type_id,
             deleted_flag,
             p_mms_payment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_payment_key,
         payment_id,
         approval_code,
         mms_inserted_date_time,
         mms_tran_id,
         payment_amount,
         payment_dim_date_key,
         payment_dim_time_key,
         payment_type_dim_description_key,
         tip_amount,
         val_payment_type_id,
         dv_deleted,
         p_mms_payment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_payment)
--Done!
end
