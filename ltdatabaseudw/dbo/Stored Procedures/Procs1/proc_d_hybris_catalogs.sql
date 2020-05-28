CREATE PROC [dbo].[proc_d_hybris_catalogs] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_catalogs)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_catalogs_insert') is not null drop table #p_hybris_catalogs_insert
create table dbo.#p_hybris_catalogs_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_catalogs.p_hybris_catalogs_id,
       p_hybris_catalogs.bk_hash
  from dbo.p_hybris_catalogs
 where p_hybris_catalogs.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_catalogs.dv_batch_id > @max_dv_batch_id
        or p_hybris_catalogs.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_catalogs.bk_hash,
       p_hybris_catalogs.bk_hash d_hybris_catalogs_key,
       s_hybris_catalogs.catalogs_pk catalogs_pk,
       s_hybris_catalogs.acl_ts acl_ts,
       s_hybris_catalogs.created_ts created_ts,
       s_hybris_catalogs.hjmpts hjmpts,
       s_hybris_catalogs.modified_ts modified_ts,
       l_hybris_catalogs.Owner_Pk_String owner_pk_string,
       l_hybris_catalogs.p_active_catalog_version p_active_catalog_version,
       l_hybris_catalogs.p_buyer p_buyer,
       s_hybris_catalogs.p_default_catalog p_default_catalog,
       s_hybris_catalogs.p_id p_id,
       s_hybris_catalogs.p_preview_url_template p_preview_url_template,
       l_hybris_catalogs.p_supplier p_supplier,
       s_hybris_catalogs.prop_ts prop_ts,
       l_hybris_catalogs.type_pk_string type_pk_string,
       p_hybris_catalogs.p_hybris_catalogs_id,
       p_hybris_catalogs.dv_batch_id,
       p_hybris_catalogs.dv_load_date_time,
       p_hybris_catalogs.dv_load_end_date_time
  from dbo.h_hybris_catalogs
  join dbo.p_hybris_catalogs
    on h_hybris_catalogs.bk_hash = p_hybris_catalogs.bk_hash  join #p_hybris_catalogs_insert
    on p_hybris_catalogs.bk_hash = #p_hybris_catalogs_insert.bk_hash
   and p_hybris_catalogs.p_hybris_catalogs_id = #p_hybris_catalogs_insert.p_hybris_catalogs_id
  join dbo.l_hybris_catalogs
    on p_hybris_catalogs.bk_hash = l_hybris_catalogs.bk_hash
   and p_hybris_catalogs.l_hybris_catalogs_id = l_hybris_catalogs.l_hybris_catalogs_id
  join dbo.s_hybris_catalogs
    on p_hybris_catalogs.bk_hash = s_hybris_catalogs.bk_hash
   and p_hybris_catalogs.s_hybris_catalogs_id = s_hybris_catalogs.s_hybris_catalogs_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_catalogs
   where d_hybris_catalogs.bk_hash in (select bk_hash from #p_hybris_catalogs_insert)

  insert dbo.d_hybris_catalogs(
             bk_hash,
             d_hybris_catalogs_key,
             catalogs_pk,
             acl_ts,
             created_ts,
             hjmpts,
             modified_ts,
             owner_pk_string,
             p_active_catalog_version,
             p_buyer,
             p_default_catalog,
             p_id,
             p_preview_url_template,
             p_supplier,
             prop_ts,
             type_pk_string,
             p_hybris_catalogs_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_hybris_catalogs_key,
         catalogs_pk,
         acl_ts,
         created_ts,
         hjmpts,
         modified_ts,
         owner_pk_string,
         p_active_catalog_version,
         p_buyer,
         p_default_catalog,
         p_id,
         p_preview_url_template,
         p_supplier,
         prop_ts,
         type_pk_string,
         p_hybris_catalogs_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_catalogs)
--Done!
end
