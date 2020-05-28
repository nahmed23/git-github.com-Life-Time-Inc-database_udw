CREATE PROC [dbo].[proc_d_hybris_fulfillment_partner] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_fulfillment_partner)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_fulfillment_partner_insert') is not null drop table #p_hybris_fulfillment_partner_insert
create table dbo.#p_hybris_fulfillment_partner_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_fulfillment_partner.p_hybris_fulfillment_partner_id,
       p_hybris_fulfillment_partner.bk_hash
  from dbo.p_hybris_fulfillment_partner
 where p_hybris_fulfillment_partner.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_fulfillment_partner.dv_batch_id > @max_dv_batch_id
        or p_hybris_fulfillment_partner.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_fulfillment_partner.bk_hash,
       p_hybris_fulfillment_partner.bk_hash d_hybris_fulfillment_partner_key,
       p_hybris_fulfillment_partner.fulfillment_partner_pk fulfillment_partner_pk,
       s_hybris_fulfillment_partner.acl_ts acl_ts,
       s_hybris_fulfillment_partner.created_ts created_ts,
       s_hybris_fulfillment_partner.hjmpts hjmpts,
       s_hybris_fulfillment_partner.modified_ts modified_ts,
       l_hybris_fulfillment_partner.owner_pk_string owner_pk_string,
       s_hybris_fulfillment_partner.p_code p_code,
       s_hybris_fulfillment_partner.p_display_name p_display_name,
       l_hybris_fulfillment_partner.p_export_file_format p_export_file_format,
       s_hybris_fulfillment_partner.p_ftp_from p_ftp_from,
       s_hybris_fulfillment_partner.p_ftp_to p_ftp_to,
       l_hybris_fulfillment_partner.p_import_file_format p_import_file_format,
       s_hybris_fulfillment_partner.p_inventory_file_format p_inventory_file_format,
       s_hybris_fulfillment_partner.p_inventory_to p_inventory_to,
       l_hybris_fulfillment_partner.p_receiver_code_id p_receiver_code_id,
       l_hybris_fulfillment_partner.p_receiver_id p_receiver_id,
       s_hybris_fulfillment_partner.p_receiver_qualifier p_receiver_qualifier,
       l_hybris_fulfillment_partner.p_sender_id p_sender_id,
       s_hybris_fulfillment_partner.p_sender_qualifier p_sender_qualifier,
       s_hybris_fulfillment_partner.p_work_day_supplier_id p_work_day_supplier_id,
       s_hybris_fulfillment_partner.prop_ts prop_ts,
       l_hybris_fulfillment_partner.type_pk_string type_pk_string,
       p_hybris_fulfillment_partner.p_hybris_fulfillment_partner_id,
       p_hybris_fulfillment_partner.dv_batch_id,
       p_hybris_fulfillment_partner.dv_load_date_time,
       p_hybris_fulfillment_partner.dv_load_end_date_time
  from dbo.h_hybris_fulfillment_partner
  join dbo.p_hybris_fulfillment_partner
    on h_hybris_fulfillment_partner.bk_hash = p_hybris_fulfillment_partner.bk_hash  join #p_hybris_fulfillment_partner_insert
    on p_hybris_fulfillment_partner.bk_hash = #p_hybris_fulfillment_partner_insert.bk_hash
   and p_hybris_fulfillment_partner.p_hybris_fulfillment_partner_id = #p_hybris_fulfillment_partner_insert.p_hybris_fulfillment_partner_id
  join dbo.l_hybris_fulfillment_partner
    on p_hybris_fulfillment_partner.bk_hash = l_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id = l_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id
  join dbo.s_hybris_fulfillment_partner
    on p_hybris_fulfillment_partner.bk_hash = s_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id = s_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_fulfillment_partner
   where d_hybris_fulfillment_partner.bk_hash in (select bk_hash from #p_hybris_fulfillment_partner_insert)

  insert dbo.d_hybris_fulfillment_partner(
             bk_hash,
             d_hybris_fulfillment_partner_key,
             fulfillment_partner_pk,
             acl_ts,
             created_ts,
             hjmpts,
             modified_ts,
             owner_pk_string,
             p_code,
             p_display_name,
             p_export_file_format,
             p_ftp_from,
             p_ftp_to,
             p_import_file_format,
             p_inventory_file_format,
             p_inventory_to,
             p_receiver_code_id,
             p_receiver_id,
             p_receiver_qualifier,
             p_sender_id,
             p_sender_qualifier,
             p_work_day_supplier_id,
             prop_ts,
             type_pk_string,
             p_hybris_fulfillment_partner_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_hybris_fulfillment_partner_key,
         fulfillment_partner_pk,
         acl_ts,
         created_ts,
         hjmpts,
         modified_ts,
         owner_pk_string,
         p_code,
         p_display_name,
         p_export_file_format,
         p_ftp_from,
         p_ftp_to,
         p_import_file_format,
         p_inventory_file_format,
         p_inventory_to,
         p_receiver_code_id,
         p_receiver_id,
         p_receiver_qualifier,
         p_sender_id,
         p_sender_qualifier,
         p_work_day_supplier_id,
         prop_ts,
         type_pk_string,
         p_hybris_fulfillment_partner_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_fulfillment_partner)
--Done!
end
