CREATE PROC [dbo].[proc_d_magento_customer_eav_attribute_website] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_eav_attribute_website)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_eav_attribute_website_insert') is not null drop table #p_magento_customer_eav_attribute_website_insert
create table dbo.#p_magento_customer_eav_attribute_website_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_eav_attribute_website.p_magento_customer_eav_attribute_website_id,
       p_magento_customer_eav_attribute_website.bk_hash
  from dbo.p_magento_customer_eav_attribute_website
 where p_magento_customer_eav_attribute_website.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_eav_attribute_website.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_eav_attribute_website.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_eav_attribute_website.bk_hash,
       p_magento_customer_eav_attribute_website.attribute_id attribute_id,
       p_magento_customer_eav_attribute_website.website_id website_id,
       case when p_magento_customer_eav_attribute_website.bk_hash in('-997', '-998', '-999') then p_magento_customer_eav_attribute_website.bk_hash
           when s_magento_customer_eav_attribute_website.attribute_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_magento_customer_eav_attribute_website.attribute_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_customer_eav_attribute_bk_hash,
       s_magento_customer_eav_attribute_website.is_required is_required,
       s_magento_customer_eav_attribute_website.is_visible is_visible,
       s_magento_customer_eav_attribute_website.multiline_count multiline_count,
       isnull(h_magento_customer_eav_attribute_website.dv_deleted,0) dv_deleted,
       p_magento_customer_eav_attribute_website.p_magento_customer_eav_attribute_website_id,
       p_magento_customer_eav_attribute_website.dv_batch_id,
       p_magento_customer_eav_attribute_website.dv_load_date_time,
       p_magento_customer_eav_attribute_website.dv_load_end_date_time
  from dbo.h_magento_customer_eav_attribute_website
  join dbo.p_magento_customer_eav_attribute_website
    on h_magento_customer_eav_attribute_website.bk_hash = p_magento_customer_eav_attribute_website.bk_hash
  join #p_magento_customer_eav_attribute_website_insert
    on p_magento_customer_eav_attribute_website.bk_hash = #p_magento_customer_eav_attribute_website_insert.bk_hash
   and p_magento_customer_eav_attribute_website.p_magento_customer_eav_attribute_website_id = #p_magento_customer_eav_attribute_website_insert.p_magento_customer_eav_attribute_website_id
  join dbo.s_magento_customer_eav_attribute_website
    on p_magento_customer_eav_attribute_website.bk_hash = s_magento_customer_eav_attribute_website.bk_hash
   and p_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id = s_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_eav_attribute_website
   where d_magento_customer_eav_attribute_website.bk_hash in (select bk_hash from #p_magento_customer_eav_attribute_website_insert)

  insert dbo.d_magento_customer_eav_attribute_website(
             bk_hash,
             attribute_id,
             website_id,
             d_magento_customer_eav_attribute_bk_hash,
             is_required,
             is_visible,
             multiline_count,
             deleted_flag,
             p_magento_customer_eav_attribute_website_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         attribute_id,
         website_id,
         d_magento_customer_eav_attribute_bk_hash,
         is_required,
         is_visible,
         multiline_count,
         dv_deleted,
         p_magento_customer_eav_attribute_website_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_eav_attribute_website)
--Done!
end
