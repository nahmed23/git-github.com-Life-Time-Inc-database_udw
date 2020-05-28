CREATE PROC [dbo].[proc_d_lt_bucks_product_options] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_product_options)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_product_options_insert') is not null drop table #p_lt_bucks_product_options_insert
create table dbo.#p_lt_bucks_product_options_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_product_options.p_lt_bucks_product_options_id,
       p_lt_bucks_product_options.bk_hash
  from dbo.p_lt_bucks_product_options
 where p_lt_bucks_product_options.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_product_options.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_product_options.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_product_options.bk_hash,
       p_lt_bucks_product_options.bk_hash dim_lt_bucks_product_options_key,
       p_lt_bucks_product_options.poption_id poption_id,
       case when p_lt_bucks_product_options.bk_hash in ('-997','-998','-999') then p_lt_bucks_product_options.bk_hash       
when l_lt_bucks_product_options.poption_product is null then '-998'       
else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_product_options.poption_product as varchar(500)),'z#@$k%&P'))),2)   end dim_lt_bucks_product_key,
       case when p_lt_bucks_product_options.bk_hash in ('-997','-998','-999') then p_lt_bucks_product_options.bk_hash
            when l_lt_bucks_product_options.poption_mms_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_product_options.poption_mms_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_product_key,
       s_lt_bucks_product_options.last_modified_timestamp last_modified_timestamp,
       case when p_lt_bucks_product_options.bk_hash in ('-997', '-998', '-999') then p_lt_bucks_product_options.bk_hash 
     when s_lt_bucks_product_options.last_modified_timestamp is null then '-998'    
   else convert(varchar, s_lt_bucks_product_options.last_modified_timestamp, 112) end last_modified_timestamp_dim_date_key,
       case when p_lt_bucks_product_options.bk_hash in ('-997', '-998', '-999') then p_lt_bucks_product_options.bk_hash 
     when s_lt_bucks_product_options.last_modified_timestamp is null then '-998'
   else '1' + replace(substring(convert(varchar,s_lt_bucks_product_options.last_modified_timestamp,114), 1, 5),':','') end last_modified_timestamp_dim_time_key,
       isnull(s_lt_bucks_product_options.poption_mms_multiplier,1) mms_multiplier,
       case when s_lt_bucks_product_options.poption_active = 1 then 'Y' else 'N' end poption_active_flag,
       isnull(s_lt_bucks_product_options.poption_expiration_days,0) poption_expiration_days,
       s_lt_bucks_product_options.poption_timestamp poption_timestamp,
       case when p_lt_bucks_product_options.bk_hash in ('-997', '-998', '-999') then p_lt_bucks_product_options.bk_hash 
     when s_lt_bucks_product_options.poption_timestamp is null then '-998'    
   else convert(varchar, s_lt_bucks_product_options.poption_timestamp, 112) end poption_timestamp_dim_date_key,
       case when p_lt_bucks_product_options.bk_hash in ('-997', '-998', '-999') then p_lt_bucks_product_options.bk_hash 
     when s_lt_bucks_product_options.poption_timestamp is null then '-998'
   else '1' + replace(substring(convert(varchar,s_lt_bucks_product_options.poption_timestamp,114), 1, 5),':','') end poption_timestamp_dim_time_key,
       isnull(s_lt_bucks_product_options.poption_price,0) price,
       isnull(s_lt_bucks_product_options.poption_desc,'') product_option_description,
       isnull(s_lt_bucks_product_options.poption_title,'') product_option_name,
       h_lt_bucks_product_options.dv_deleted,
       p_lt_bucks_product_options.p_lt_bucks_product_options_id,
       p_lt_bucks_product_options.dv_batch_id,
       p_lt_bucks_product_options.dv_load_date_time,
       p_lt_bucks_product_options.dv_load_end_date_time
  from dbo.h_lt_bucks_product_options
  join dbo.p_lt_bucks_product_options
    on h_lt_bucks_product_options.bk_hash = p_lt_bucks_product_options.bk_hash  join #p_lt_bucks_product_options_insert
    on p_lt_bucks_product_options.bk_hash = #p_lt_bucks_product_options_insert.bk_hash
   and p_lt_bucks_product_options.p_lt_bucks_product_options_id = #p_lt_bucks_product_options_insert.p_lt_bucks_product_options_id
  join dbo.l_lt_bucks_product_options
    on p_lt_bucks_product_options.bk_hash = l_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.l_lt_bucks_product_options_id = l_lt_bucks_product_options.l_lt_bucks_product_options_id
  join dbo.s_lt_bucks_product_options
    on p_lt_bucks_product_options.bk_hash = s_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.s_lt_bucks_product_options_id = s_lt_bucks_product_options.s_lt_bucks_product_options_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_product_options
   where d_lt_bucks_product_options.bk_hash in (select bk_hash from #p_lt_bucks_product_options_insert)

  insert dbo.d_lt_bucks_product_options(
             bk_hash,
             dim_lt_bucks_product_options_key,
             poption_id,
             dim_lt_bucks_product_key,
             dim_mms_product_key,
             last_modified_timestamp,
             last_modified_timestamp_dim_date_key,
             last_modified_timestamp_dim_time_key,
             mms_multiplier,
             poption_active_flag,
             poption_expiration_days,
             poption_timestamp,
             poption_timestamp_dim_date_key,
             poption_timestamp_dim_time_key,
             price,
             product_option_description,
             product_option_name,
             deleted_flag,
             p_lt_bucks_product_options_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_lt_bucks_product_options_key,
         poption_id,
         dim_lt_bucks_product_key,
         dim_mms_product_key,
         last_modified_timestamp,
         last_modified_timestamp_dim_date_key,
         last_modified_timestamp_dim_time_key,
         mms_multiplier,
         poption_active_flag,
         poption_expiration_days,
         poption_timestamp,
         poption_timestamp_dim_date_key,
         poption_timestamp_dim_time_key,
         price,
         product_option_description,
         product_option_name,
         dv_deleted,
         p_lt_bucks_product_options_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_product_options)
--Done!
end
