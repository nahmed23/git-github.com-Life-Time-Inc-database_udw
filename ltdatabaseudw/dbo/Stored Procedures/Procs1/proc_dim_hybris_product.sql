CREATE PROC [dbo].[proc_dim_hybris_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_hybris_product

if object_id('tempdb..#dim_hybris_product') is not null drop table #dim_hybris_product
create table dbo.#dim_hybris_product with(distribution=hash(dim_hybris_product_key), location=user_db, heap) as

with
hybris_products (bk_hash, products_pk, product_name, created_date_time, modified_date_time, p_catalog,  p_catalog_version, online_date, offline_date,
                 dim_mms_product_key, fulfillment_dim_mms_product_key, p_unit, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select p_hybris_products.bk_hash,
           p_hybris_products.products_pk,
           s_hybris_products.p_code product_name,
           s_hybris_products.created_ts as created_date_time,
           s_hybris_products.modified_ts as modified_date_time,
           l_hybris_products.p_catalog,
           l_hybris_products.p_catalog_version,
           s_hybris_products.p_online_date online_date,
           s_hybris_products.p_offline_date offline_date,
           case when p_hybris_products.bk_hash in ('-997','-998','-999') then p_hybris_products.bk_hash
                when l_hybris_products.p_ltf_product_id is null then '-998'  
               --util_bk_hash[l_hybris_products.p_ltf_product_id,h_mms_product.product_id]  
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_hybris_products.p_ltf_product_id as varchar(500)),'z#@$k%&P'))),2) end dim_mms_product_key,
           case when p_hybris_products.bk_hash in ('-997','-998','-999') then p_hybris_products.bk_hash
                when l_hybris_products.p_ltf_fulfillment_product_id is null then '-998'  
               --util_bk_hash[l_hybris_products.p_ltf_fulfillment_product_id,h_mms_product.product_id]  
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_hybris_products.p_ltf_fulfillment_product_id as varchar(500)),'z#@$k%&P'))),2) end fulfillment_dim_mms_product_key,
            l_hybris_products.p_unit,
            p_hybris_products.dv_load_date_time,
            p_hybris_products.dv_load_end_date_time,
            p_hybris_products.dv_batch_id,
            case when #dv_batch_id.current_dv_batch_id is not null then 1
                 else 0 end include_batch_flag
      from p_hybris_products
      join l_hybris_products
        on p_hybris_products.l_hybris_products_id = l_hybris_products.l_hybris_products_id
      join s_hybris_products
        on p_hybris_products.s_hybris_products_id = s_hybris_products.s_hybris_products_id
      left join #dv_batch_id
        on p_hybris_products.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_products.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_products.dv_load_end_date_time = 'dec 31, 9999'),

hybris_products_lp (item_pk, product_display_name, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select l_hybris_products_lp.item_pk,
           s_hybris_products_lp.p_name product_display_name,
           p_hybris_products_lp.dv_load_date_time,
           p_hybris_products_lp.dv_load_end_date_time,
           p_hybris_products_lp.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_products_lp
      join l_hybris_products_lp
        on p_hybris_products_lp.l_hybris_products_lp_id = l_hybris_products_lp.l_hybris_products_lp_id
      join s_hybris_products_lp
        on p_hybris_products_lp.s_hybris_products_lp_id = s_hybris_products_lp.s_hybris_products_lp_id
      left join #dv_batch_id
        on p_hybris_products_lp.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_products_lp.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_products_lp.dv_load_end_date_time = 'dec 31, 9999'),

hybris_units (units_pk, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select p_hybris_units.units_pk,
           p_hybris_units.dv_load_date_time,
           p_hybris_units.dv_load_end_date_time,
           p_hybris_units.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_units
      left join #dv_batch_id
        on p_hybris_units.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_units.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_units.dv_load_end_date_time = 'dec 31, 9999'),

hybris_units_lp (item_pk, unit_of_measure, lang_pk, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select l_hybris_units_lp.item_pk,
           s_hybris_units_lp.p_name unit_of_measure,
           l_hybris_units_lp.lang_pk,
           p_hybris_units_lp.dv_load_date_time,
           p_hybris_units_lp.dv_load_end_date_time,
           p_hybris_units_lp.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_units_lp
      join l_hybris_units_lp
        on p_hybris_units_lp.l_hybris_units_lp_id = l_hybris_units_lp.l_hybris_units_lp_id
      join s_hybris_units_lp
        on p_hybris_units_lp.s_hybris_units_lp_id = s_hybris_units_lp.s_hybris_units_lp_id
      left join #dv_batch_id
        on p_hybris_units_lp.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_units_lp.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_units_lp.dv_load_end_date_time = 'dec 31, 9999'),

hybris_catalogs (catalogs_pk, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select p_hybris_catalogs.catalogs_pk,
           p_hybris_catalogs.dv_load_date_time,
           p_hybris_catalogs.dv_load_end_date_time,
           p_hybris_catalogs.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_catalogs
      left join #dv_batch_id
        on p_hybris_catalogs.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_catalogs.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_catalogs.dv_load_end_date_time = 'dec 31, 9999'),

hybris_catalogs_lp (item_pk, lang_pk, catalog_name, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select l_hybris_catalogs_lp.item_pk,
           l_hybris_catalogs_lp.lang_pk,
           s_hybris_catalogs_lp.p_name catalog_name,
           p_hybris_catalogs_lp.dv_load_date_time,
           p_hybris_catalogs_lp.dv_load_end_date_time,
           p_hybris_catalogs_lp.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_catalogs_lp
      join l_hybris_catalogs_lp
        on p_hybris_catalogs_lp.l_hybris_catalogs_lp_id = l_hybris_catalogs_lp.l_hybris_catalogs_lp_id
      join s_hybris_catalogs_lp
        on p_hybris_catalogs_lp.s_hybris_catalogs_lp_id = s_hybris_catalogs_lp.s_hybris_catalogs_lp_id
      left join #dv_batch_id
        on p_hybris_catalogs_lp.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_catalogs_lp.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_catalogs_lp.dv_load_end_date_time = 'dec 31, 9999'),

hybris_catalog_versions (catalog_versions_pk, catalog_version, dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
   (select p_hybris_catalog_versions.catalog_versions_pk,
           s_hybris_catalog_versions.p_version catalog_version,
           p_hybris_catalog_versions.dv_load_date_time,
           p_hybris_catalog_versions.dv_load_end_date_time,
           p_hybris_catalog_versions.dv_batch_id,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_catalog_versions
      join l_hybris_catalog_versions
        on p_hybris_catalog_versions.l_hybris_catalog_versions_id = l_hybris_catalog_versions.l_hybris_catalog_versions_id
      join s_hybris_catalog_versions
        on p_hybris_catalog_versions.s_hybris_catalog_versions_id = s_hybris_catalog_versions.s_hybris_catalog_versions_id
      left join #dv_batch_id
        on p_hybris_catalog_versions.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_catalog_versions.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_catalog_versions.dv_load_end_date_time = 'dec 31, 9999'),

hybris_languages (languages_pk, include_batch_flag) as
   (select p_hybris_languages.languages_pk,
           case when #dv_batch_id.current_dv_batch_id is not null then 1
                else 0 end include_batch_flag
      from p_hybris_languages
      join s_hybris_languages
        on p_hybris_languages.s_hybris_languages_id = s_hybris_languages.s_hybris_languages_id
      left join #dv_batch_id
        on p_hybris_languages.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_hybris_languages.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_hybris_languages.dv_load_end_date_time = 'dec 31, 9999'
       and s_hybris_languages.p_iso_code = 'en')

select hybris_products.bk_hash dim_hybris_product_key,
       hybris_products.products_pk,
       hybris_products.product_name,
       hybris_products.created_date_time,
       hybris_products.modified_date_time,
       hybris_products.online_date,
       hybris_products.offline_date,
       hybris_products.dim_mms_product_key,
       hybris_products.fulfillment_dim_mms_product_key,
       hybris_products_lp.product_display_name,
       hybris_units_lp.unit_of_measure,
       hybris_catalogs_lp.catalog_name,
       hybris_catalog_versions.catalog_version,
       case when hybris_products.dv_load_date_time >= isnull(hybris_products_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_date_time >= isnull(hybris_units.dv_load_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_date_time >= isnull(hybris_units_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_date_time >= isnull(hybris_catalogs.dv_load_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_date_time >= isnull(hybris_catalogs_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_products.dv_load_date_time
            when hybris_products_lp.dv_load_date_time >= isnull(hybris_units.dv_load_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_date_time >= isnull(hybris_units_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_date_time >= isnull(hybris_catalogs.dv_load_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_date_time >= isnull(hybris_catalogs_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_products_lp.dv_load_date_time
            when hybris_units.dv_load_date_time >= isnull(hybris_units_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_date_time >= isnull(hybris_catalogs.dv_load_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_date_time >= isnull(hybris_catalogs_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_units.dv_load_date_time
            when hybris_units_lp.dv_load_date_time >= isnull(hybris_catalogs.dv_load_date_time, 'jan 1, 1900')
             and hybris_units_lp.dv_load_date_time >= isnull(hybris_catalogs_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_units_lp.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_units_lp.dv_load_date_time
            when hybris_catalogs.dv_load_date_time >= isnull(hybris_catalogs_lp.dv_load_date_time, 'jan 1, 1900')
             and hybris_catalogs.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_catalogs.dv_load_date_time
            when hybris_catalogs_lp.dv_load_date_time >= isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900')
            then hybris_catalogs_lp.dv_load_date_time
            else isnull(hybris_catalog_versions.dv_load_date_time, 'jan 1, 1900') end dv_load_date_time,
       case when hybris_products.dv_load_end_date_time >= isnull(hybris_products_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_end_date_time >= isnull(hybris_units.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_end_date_time >= isnull(hybris_units_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_end_date_time >= isnull(hybris_catalogs.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_end_date_time >= isnull(hybris_catalogs_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_products.dv_load_end_date_time
            when hybris_products_lp.dv_load_end_date_time >= isnull(hybris_units.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_end_date_time >= isnull(hybris_units_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_end_date_time >= isnull(hybris_catalogs.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_end_date_time >= isnull(hybris_catalogs_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_products_lp.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_products_lp.dv_load_end_date_time
            when hybris_units.dv_load_end_date_time >= isnull(hybris_units_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_end_date_time >= isnull(hybris_catalogs.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_end_date_time >= isnull(hybris_catalogs_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_units.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_units.dv_load_end_date_time
            when hybris_units_lp.dv_load_end_date_time >= isnull(hybris_catalogs.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_units_lp.dv_load_end_date_time >= isnull(hybris_catalogs_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_units_lp.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_units_lp.dv_load_end_date_time
            when hybris_catalogs.dv_load_end_date_time >= isnull(hybris_catalogs_lp.dv_load_end_date_time, 'jan 1, 1900')
             and hybris_catalogs.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_catalogs.dv_load_end_date_time
            when hybris_catalogs_lp.dv_load_end_date_time >= isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900')
            then hybris_catalogs_lp.dv_load_end_date_time
            else isnull(hybris_catalog_versions.dv_load_end_date_time, 'jan 1, 1900') end dv_load_end_date_time,
       case when hybris_products.dv_batch_id >= isnull(hybris_products_lp.dv_batch_id, -1)
             and hybris_products.dv_batch_id >= isnull(hybris_units.dv_batch_id, -1)
             and hybris_products.dv_batch_id >= isnull(hybris_units_lp.dv_batch_id, -1)
             and hybris_products.dv_batch_id >= isnull(hybris_catalogs.dv_batch_id, -1)
             and hybris_products.dv_batch_id >= isnull(hybris_catalogs_lp.dv_batch_id, -1)
             and hybris_products.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_products.dv_batch_id
            when hybris_products_lp.dv_batch_id >= isnull(hybris_units.dv_batch_id, -1)
             and hybris_products_lp.dv_batch_id >= isnull(hybris_units_lp.dv_batch_id, -1)
             and hybris_products_lp.dv_batch_id >= isnull(hybris_catalogs.dv_batch_id, -1)
             and hybris_products_lp.dv_batch_id >= isnull(hybris_catalogs_lp.dv_batch_id, -1)
             and hybris_products_lp.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_products_lp.dv_batch_id
            when hybris_units.dv_batch_id >= isnull(hybris_units_lp.dv_batch_id, -1)
             and hybris_units.dv_batch_id >= isnull(hybris_catalogs.dv_batch_id, -1)
             and hybris_units.dv_batch_id >= isnull(hybris_catalogs_lp.dv_batch_id, -1)
             and hybris_units.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_units.dv_batch_id
            when hybris_units_lp.dv_batch_id >= isnull(hybris_catalogs_lp.dv_batch_id, -1)
             and hybris_units_lp.dv_batch_id >= isnull(hybris_catalogs.dv_batch_id, -1)
             and hybris_units_lp.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_units_lp.dv_batch_id
            when hybris_catalogs.dv_batch_id >= isnull(hybris_catalogs_lp.dv_batch_id, -1)
             and hybris_catalogs.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_catalogs.dv_batch_id
            when hybris_catalogs_lp.dv_batch_id >= isnull(hybris_catalog_versions.dv_batch_id, -1)
            then hybris_catalogs_lp.dv_batch_id
            else hybris_catalog_versions.dv_batch_id end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from hybris_products
  left join hybris_products_lp
    on hybris_products.products_pk = hybris_products_lp.item_pk
  left join hybris_units
    on hybris_products.p_unit = hybris_units.units_pk
  left join hybris_units_lp
    on hybris_units.units_pk = hybris_units_lp.item_pk
  left join hybris_languages hybris_units_lp_hybris_languages
    on hybris_units_lp.lang_pk = hybris_units_lp_hybris_languages.languages_pk
  left join hybris_catalogs
    on hybris_products.p_catalog = hybris_catalogs.catalogs_pk
  left join hybris_catalogs_lp
    on hybris_catalogs.catalogs_pk = hybris_catalogs_lp.item_pk
  left join hybris_languages hybris_catalogs_lp_hybris_languages
    on hybris_catalogs_lp.lang_pk = hybris_catalogs_lp_hybris_languages.languages_pk
  left join hybris_catalog_versions
    on hybris_products.p_catalog_version = hybris_catalog_versions.catalog_versions_pk
 where (hybris_units_lp_hybris_languages.languages_pk is not null
        or hybris_units_lp.lang_pk is null)
   and (hybris_catalogs_lp_hybris_languages.languages_pk is not null
        or hybris_catalogs_lp.lang_pk is null)
   and (hybris_products.include_batch_flag = 1
        or isnull(hybris_products_lp.include_batch_flag, 0) = 1
        or isnull(hybris_units.include_batch_flag, 0) = 1
        or isnull(hybris_units_lp.include_batch_flag, 0) = 1
        or isnull(hybris_catalogs.include_batch_flag, 0) = 1
        or isnull(hybris_catalogs_lp.include_batch_flag, 0) = 1
        or isnull(hybris_catalog_versions.include_batch_flag, 0) = 1
        or isnull(hybris_units_lp_hybris_languages.include_batch_flag, 0) = 1
        or isnull(hybris_catalogs_lp_hybris_languages.include_batch_flag, 0) = 1)

-- Delete and re-insert
-- Do as a single transaction
--   Delete records from the dim table that exist
--   Insert records from current and missing batches

begin tran
  delete dbo.dim_hybris_product
   where dim_hybris_product_key in (select dim_hybris_product_key from #dim_hybris_product)

  insert dbo.dim_hybris_product(
               dim_hybris_product_key,
               products_pk, 
               catalog_name,
               catalog_version,
               created_date_time,
               dim_mms_product_key,
               fulfillment_dim_mms_product_key,
               modified_date_time,
               offline_date,
               online_date,
               product_display_name,
               product_name,
               unit_of_measure,
               dv_load_date_time,
               dv_load_end_date_time,
               dv_batch_id,
               dv_inserted_date_time,
               dv_insert_user)
  select #dim_hybris_product.dim_hybris_product_key,
         #dim_hybris_product.products_pk, 
         #dim_hybris_product.catalog_name,
         #dim_hybris_product.catalog_version,
         #dim_hybris_product.created_date_time,
         #dim_hybris_product.dim_mms_product_key,
         #dim_hybris_product.fulfillment_dim_mms_product_key,
         #dim_hybris_product.modified_date_time,
         #dim_hybris_product.offline_date,
         #dim_hybris_product.online_date,
         #dim_hybris_product.product_display_name,
         #dim_hybris_product.product_name,
         #dim_hybris_product.unit_of_measure,
         #dim_hybris_product.dv_load_date_time,
         #dim_hybris_product.dv_load_end_date_time,
         #dim_hybris_product.dv_batch_id,
         #dim_hybris_product.dv_inserted_date_time,
         #dim_hybris_product.dv_insert_user
    from #dim_hybris_product
commit tran

end
