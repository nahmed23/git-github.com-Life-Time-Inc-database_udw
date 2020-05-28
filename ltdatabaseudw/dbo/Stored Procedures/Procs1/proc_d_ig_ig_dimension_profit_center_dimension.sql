CREATE PROC [dbo].[proc_d_ig_ig_dimension_profit_center_dimension] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_profit_center_dimension)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_ig_dimension_profit_center_dimension_insert') is not null drop table #p_ig_ig_dimension_profit_center_dimension_insert
create table dbo.#p_ig_ig_dimension_profit_center_dimension_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_profit_center_dimension.p_ig_ig_dimension_profit_center_dimension_id,
       p_ig_ig_dimension_profit_center_dimension.bk_hash
  from dbo.p_ig_ig_dimension_profit_center_dimension
 where p_ig_ig_dimension_profit_center_dimension.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_ig_dimension_profit_center_dimension.dv_batch_id > @max_dv_batch_id
        or p_ig_ig_dimension_profit_center_dimension.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_profit_center_dimension.bk_hash,
       p_ig_ig_dimension_profit_center_dimension.bk_hash dummy_bk_hash_key,
       l_ig_ig_dimension_profit_center_dimension.customer_id customer_id,
       case when p_ig_ig_dimension_profit_center_dimension.bk_hash in ('-997','-998','-999') then p_ig_ig_dimension_profit_center_dimension.bk_hash
       when l_ig_ig_dimension_profit_center_dimension.profit_center_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_ig_dimension_profit_center_dimension.profit_center_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_profit_center_key,
       l_ig_ig_dimension_profit_center_dimension.ent_id ent_id,
       case when p_ig_ig_dimension_profit_center_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_profit_center_dimension.bk_hash
             when p_ig_ig_dimension_profit_center_dimension.profit_center_dim_id is null then '-998'
       	  else p_ig_ig_dimension_profit_center_dimension.profit_center_dim_id end profit_center_dim_id,
       l_ig_ig_dimension_profit_center_dimension.profit_center_id profit_center_id,
       s_ig_ig_dimension_profit_center_dimension.profit_center_name profit_center_name,
       isnull(l_ig_ig_dimension_profit_center_dimension.store_id,'') store_id,
       s_ig_ig_dimension_profit_center_dimension.store_name store_name,
       isnull(h_ig_ig_dimension_profit_center_dimension.dv_deleted,0) dv_deleted,
       p_ig_ig_dimension_profit_center_dimension.p_ig_ig_dimension_profit_center_dimension_id,
       p_ig_ig_dimension_profit_center_dimension.dv_batch_id,
       p_ig_ig_dimension_profit_center_dimension.dv_load_date_time,
       p_ig_ig_dimension_profit_center_dimension.dv_load_end_date_time
  from dbo.h_ig_ig_dimension_profit_center_dimension
  join dbo.p_ig_ig_dimension_profit_center_dimension
    on h_ig_ig_dimension_profit_center_dimension.bk_hash = p_ig_ig_dimension_profit_center_dimension.bk_hash
  join #p_ig_ig_dimension_profit_center_dimension_insert
    on p_ig_ig_dimension_profit_center_dimension.bk_hash = #p_ig_ig_dimension_profit_center_dimension_insert.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.p_ig_ig_dimension_profit_center_dimension_id = #p_ig_ig_dimension_profit_center_dimension_insert.p_ig_ig_dimension_profit_center_dimension_id
  join dbo.l_ig_ig_dimension_profit_center_dimension
    on p_ig_ig_dimension_profit_center_dimension.bk_hash = l_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id = l_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id
  join dbo.s_ig_ig_dimension_profit_center_dimension
    on p_ig_ig_dimension_profit_center_dimension.bk_hash = s_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id = s_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id
 where s_ig_ig_dimension_profit_center_dimension.eff_date_to is null 
  and (l_ig_ig_dimension_profit_center_dimension.store_id not in (2, 45)
      or p_ig_ig_dimension_profit_center_dimension.bk_hash in ('-999','-998','997'))
  and NOT ((profit_center_id = -1 and store_id = 11)
            or (profit_center_id = 247 and store_id = 69)
			or (profit_center_id = 303 and store_id = 104))

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_ig_dimension_profit_center_dimension
   where d_ig_ig_dimension_profit_center_dimension.bk_hash in (select bk_hash from #p_ig_ig_dimension_profit_center_dimension_insert)

  insert dbo.d_ig_ig_dimension_profit_center_dimension(
             bk_hash,
             dummy_bk_hash_key,
             customer_id,
             dim_cafe_profit_center_key,
             ent_id,
             profit_center_dim_id,
             profit_center_id,
             profit_center_name,
             store_id,
             store_name,
             deleted_flag,
             p_ig_ig_dimension_profit_center_dimension_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dummy_bk_hash_key,
         customer_id,
         dim_cafe_profit_center_key,
         ent_id,
         profit_center_dim_id,
         profit_center_id,
         profit_center_name,
         store_id,
         store_name,
         dv_deleted,
         p_ig_ig_dimension_profit_center_dimension_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_profit_center_dimension)
--Done!
end
