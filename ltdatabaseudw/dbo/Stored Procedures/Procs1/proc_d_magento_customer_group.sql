﻿CREATE PROC [dbo].[proc_d_magento_customer_group] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_group)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_group_insert') is not null drop table #p_magento_customer_group_insert
create table dbo.#p_magento_customer_group_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_group.p_magento_customer_group_id,
       p_magento_customer_group.bk_hash
  from dbo.p_magento_customer_group
 where p_magento_customer_group.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_group.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_group.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_group.bk_hash,
       p_magento_customer_group.customer_group_id customer_group_id,
       s_magento_customer_group.customer_group_code customer_group_code,
       l_magento_customer_group.tax_class_id tax_class_id,
       isnull(h_magento_customer_group.dv_deleted,0) dv_deleted,
       p_magento_customer_group.p_magento_customer_group_id,
       p_magento_customer_group.dv_batch_id,
       p_magento_customer_group.dv_load_date_time,
       p_magento_customer_group.dv_load_end_date_time
  from dbo.h_magento_customer_group
  join dbo.p_magento_customer_group
    on h_magento_customer_group.bk_hash = p_magento_customer_group.bk_hash
  join #p_magento_customer_group_insert
    on p_magento_customer_group.bk_hash = #p_magento_customer_group_insert.bk_hash
   and p_magento_customer_group.p_magento_customer_group_id = #p_magento_customer_group_insert.p_magento_customer_group_id
  join dbo.l_magento_customer_group
    on p_magento_customer_group.bk_hash = l_magento_customer_group.bk_hash
   and p_magento_customer_group.l_magento_customer_group_id = l_magento_customer_group.l_magento_customer_group_id
  join dbo.s_magento_customer_group
    on p_magento_customer_group.bk_hash = s_magento_customer_group.bk_hash
   and p_magento_customer_group.s_magento_customer_group_id = s_magento_customer_group.s_magento_customer_group_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_group
   where d_magento_customer_group.bk_hash in (select bk_hash from #p_magento_customer_group_insert)

  insert dbo.d_magento_customer_group(
             bk_hash,
             customer_group_id,
             customer_group_code,
             tax_class_id,
             deleted_flag,
             p_magento_customer_group_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         customer_group_id,
         customer_group_code,
         tax_class_id,
         dv_deleted,
         p_magento_customer_group_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_group)
--Done!
end
