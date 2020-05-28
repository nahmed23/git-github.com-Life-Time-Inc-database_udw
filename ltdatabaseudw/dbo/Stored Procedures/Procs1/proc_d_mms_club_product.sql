CREATE PROC [dbo].[proc_d_mms_club_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_club_product_insert') is not null drop table #p_mms_club_product_insert
create table dbo.#p_mms_club_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_product.p_mms_club_product_id,
       p_mms_club_product.bk_hash
  from dbo.p_mms_club_product
 where p_mms_club_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_club_product.dv_batch_id > @max_dv_batch_id
        or p_mms_club_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_product.bk_hash,
       p_mms_club_product.bk_hash dim_club_bridge_dim_mms_product_key,
       p_mms_club_product.club_product_id club_product_id,
       case when p_mms_club_product.bk_hash in ('-997','-998','-999') then p_mms_club_product.bk_hash
            when l_mms_club_product.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_product.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_mms_club_product.bk_hash in ('-997','-998','-999') then p_mms_club_product.bk_hash
            when l_mms_club_product.product_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_product.product_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_product_key,
       s_mms_club_product.price price,
       case when s_mms_club_product.sold_in_pk = 1
                        then 'Y'
                   else 'N'
               end sold_in_pk_flag,
       l_mms_club_product.val_commissionable_id val_commissionable_id,
       isnull(h_mms_club_product.dv_deleted,0) dv_deleted,
       p_mms_club_product.p_mms_club_product_id,
       p_mms_club_product.dv_batch_id,
       p_mms_club_product.dv_load_date_time,
       p_mms_club_product.dv_load_end_date_time
  from dbo.h_mms_club_product
  join dbo.p_mms_club_product
    on h_mms_club_product.bk_hash = p_mms_club_product.bk_hash
  join #p_mms_club_product_insert
    on p_mms_club_product.bk_hash = #p_mms_club_product_insert.bk_hash
   and p_mms_club_product.p_mms_club_product_id = #p_mms_club_product_insert.p_mms_club_product_id
  join dbo.l_mms_club_product
    on p_mms_club_product.bk_hash = l_mms_club_product.bk_hash
   and p_mms_club_product.l_mms_club_product_id = l_mms_club_product.l_mms_club_product_id
  join dbo.s_mms_club_product
    on p_mms_club_product.bk_hash = s_mms_club_product.bk_hash
   and p_mms_club_product.s_mms_club_product_id = s_mms_club_product.s_mms_club_product_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_club_product
   where d_mms_club_product.bk_hash in (select bk_hash from #p_mms_club_product_insert)

  insert dbo.d_mms_club_product(
             bk_hash,
             dim_club_bridge_dim_mms_product_key,
             club_product_id,
             dim_club_key,
             dim_mms_product_key,
             price,
             sold_in_pk_flag,
             val_commissionable_id,
             deleted_flag,
             p_mms_club_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_club_bridge_dim_mms_product_key,
         club_product_id,
         dim_club_key,
         dim_mms_product_key,
         price,
         sold_in_pk_flag,
         val_commissionable_id,
         dv_deleted,
         p_mms_club_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_product)
--Done!
end
