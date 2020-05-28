CREATE PROC [dbo].[proc_d_mms_card_level_price_range] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_card_level_price_range)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_card_level_price_range_insert') is not null drop table #p_mms_card_level_price_range_insert
create table dbo.#p_mms_card_level_price_range_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_card_level_price_range.p_mms_card_level_price_range_id,
       p_mms_card_level_price_range.bk_hash
  from dbo.p_mms_card_level_price_range
 where p_mms_card_level_price_range.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_card_level_price_range.dv_batch_id > @max_dv_batch_id
        or p_mms_card_level_price_range.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_card_level_price_range.bk_hash,
       p_mms_card_level_price_range.card_level_price_range_id card_level_price_range_id,
       case when p_mms_card_level_price_range.bk_hash in('-997', '-998', '-999') then p_mms_card_level_price_range.bk_hash
           when l_mms_card_level_price_range.val_card_level_id is null then '-998'
        else 'r_mms_val_card_level_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_card_level_price_range.val_card_level_id as int) as varchar(500)),'z#@$k%&P'))),2)   end card_level_dim_description_key,
       case when p_mms_card_level_price_range.bk_hash in('-997', '-998', '-999') then p_mms_card_level_price_range.bk_hash
           when l_mms_card_level_price_range.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_card_level_price_range.product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_product_key,
       s_mms_card_level_price_range.ending_price ending_price,
       s_mms_card_level_price_range.inserted_date_time inserted_date_time,
       case when p_mms_card_level_price_range.bk_hash in('-997', '-998', '-999') then p_mms_card_level_price_range.bk_hash
           when s_mms_card_level_price_range.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_card_level_price_range.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_card_level_price_range.bk_hash in ('-997','-998','-999') then p_mms_card_level_price_range.bk_hash
       when s_mms_card_level_price_range.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_card_level_price_range.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_card_level_price_range.product_id product_id,
       s_mms_card_level_price_range.starting_price starting_price,
       s_mms_card_level_price_range.updated_date_time updated_date_time,
       case when p_mms_card_level_price_range.bk_hash in('-997', '-998', '-999') then p_mms_card_level_price_range.bk_hash
           when s_mms_card_level_price_range.updated_date_time is null then '-998'
        else convert(varchar, s_mms_card_level_price_range.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_card_level_price_range.bk_hash in ('-997','-998','-999') then p_mms_card_level_price_range.bk_hash
       when s_mms_card_level_price_range.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_card_level_price_range.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       l_mms_card_level_price_range.val_card_level_id  val_card_level_id,
       isnull(h_mms_card_level_price_range.dv_deleted,0) dv_deleted,
       p_mms_card_level_price_range.p_mms_card_level_price_range_id,
       p_mms_card_level_price_range.dv_batch_id,
       p_mms_card_level_price_range.dv_load_date_time,
       p_mms_card_level_price_range.dv_load_end_date_time
  from dbo.h_mms_card_level_price_range
  join dbo.p_mms_card_level_price_range
    on h_mms_card_level_price_range.bk_hash = p_mms_card_level_price_range.bk_hash
  join #p_mms_card_level_price_range_insert
    on p_mms_card_level_price_range.bk_hash = #p_mms_card_level_price_range_insert.bk_hash
   and p_mms_card_level_price_range.p_mms_card_level_price_range_id = #p_mms_card_level_price_range_insert.p_mms_card_level_price_range_id
  join dbo.l_mms_card_level_price_range
    on p_mms_card_level_price_range.bk_hash = l_mms_card_level_price_range.bk_hash
   and p_mms_card_level_price_range.l_mms_card_level_price_range_id = l_mms_card_level_price_range.l_mms_card_level_price_range_id
  join dbo.s_mms_card_level_price_range
    on p_mms_card_level_price_range.bk_hash = s_mms_card_level_price_range.bk_hash
   and p_mms_card_level_price_range.s_mms_card_level_price_range_id = s_mms_card_level_price_range.s_mms_card_level_price_range_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_card_level_price_range
   where d_mms_card_level_price_range.bk_hash in (select bk_hash from #p_mms_card_level_price_range_insert)

  insert dbo.d_mms_card_level_price_range(
             bk_hash,
             card_level_price_range_id,
             card_level_dim_description_key,
             dim_mms_product_key,
             ending_price,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             product_id,
             starting_price,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             val_card_level_id,
             deleted_flag,
             p_mms_card_level_price_range_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         card_level_price_range_id,
         card_level_dim_description_key,
         dim_mms_product_key,
         ending_price,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         product_id,
         starting_price,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         val_card_level_id,
         dv_deleted,
         p_mms_card_level_price_range_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_card_level_price_range)
--Done!
end
