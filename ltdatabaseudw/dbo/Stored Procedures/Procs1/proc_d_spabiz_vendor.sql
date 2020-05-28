CREATE PROC [dbo].[proc_d_spabiz_vendor] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_vendor)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_vendor_insert') is not null drop table #p_spabiz_vendor_insert
create table dbo.#p_spabiz_vendor_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_vendor.p_spabiz_vendor_id,
       p_spabiz_vendor.bk_hash
  from dbo.p_spabiz_vendor
 where p_spabiz_vendor.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_vendor.dv_batch_id > @max_dv_batch_id
        or p_spabiz_vendor.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_vendor.bk_hash,
       p_spabiz_vendor.bk_hash dim_spabiz_vendor_key,
       p_spabiz_vendor.vendor_id vendor_id,
       p_spabiz_vendor.store_number store_number,
       case when s_spabiz_vendor.address_1 is null then ''
            else s_spabiz_vendor.address_1
        end address_1,
       case when s_spabiz_vendor.address_2 is null then ''
            else s_spabiz_vendor.address_2
        end address_2,
       case when s_spabiz_vendor.city is null then ''
            else s_spabiz_vendor.city
        end city,
       case when s_spabiz_vendor.customer_num is null then ''
            else s_spabiz_vendor.customer_num
        end customer_number,
       case when p_spabiz_vendor.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_vendor.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_vendor.delete_date
        end deleted_date_time,
       case when p_spabiz_vendor.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_vendor.vendor_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_vendor.bk_hash in ('-997','-998','-999') then p_spabiz_vendor.bk_hash
            when l_spabiz_vendor.store_number is null then '-998'
            when l_spabiz_vendor.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_vendor.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_vendor.edit_time edit_date_time,
       case when s_spabiz_vendor.email is null then ''
            else s_spabiz_vendor.email
        end email,
       case when s_spabiz_vendor.fax is null then ''
            else s_spabiz_vendor.fax
        end fax,
       case when s_spabiz_vendor.name is null then ''
            else s_spabiz_vendor.name
        end name,
       case when s_spabiz_vendor.phone is null then ''
            else s_spabiz_vendor.phone
        end phone_number,
       case when s_spabiz_vendor.zip is null then ''
            else s_spabiz_vendor.zip
        end postal_code,
       case when s_spabiz_vendor.contact_1 is null then ''
            else s_spabiz_vendor.contact_1
        end primary_contact,
       case when s_spabiz_vendor.contact_1_ext is null then ''
            else s_spabiz_vendor.contact_1_ext
        end primary_contact_phone_extension,
       case when s_spabiz_vendor.contact_1_tel is null then ''
            else s_spabiz_vendor.contact_1_tel
        end primary_contact_phone_number,
       case when s_spabiz_vendor.contact_1_title is null then ''
            else s_spabiz_vendor.contact_1_title
        end primary_contact_title,
       case when s_spabiz_vendor.quick_id is null then ''
            else s_spabiz_vendor.quick_id
        end quick_id,
       case when s_spabiz_vendor.contact_2 is null then ''
            else s_spabiz_vendor.contact_2
        end secondary_contact,
       case when s_spabiz_vendor.contact_2_ext is null then ''
            else s_spabiz_vendor.contact_2_ext
        end secondary_contact_phone_extension,
       case when s_spabiz_vendor.contact_2_tel is null then ''
            else s_spabiz_vendor.contact_2_tel
        end secondary_contact_phone_number,
       case when s_spabiz_vendor.contact_2_title is null then ''
            else s_spabiz_vendor.contact_2_title
        end secondary_contact_title,
       case when s_spabiz_vendor.st is null then ''
            else s_spabiz_vendor.st
        end state,
       p_spabiz_vendor.p_spabiz_vendor_id,
       p_spabiz_vendor.dv_batch_id,
       p_spabiz_vendor.dv_load_date_time,
       p_spabiz_vendor.dv_load_end_date_time
  from dbo.p_spabiz_vendor
  join #p_spabiz_vendor_insert
    on p_spabiz_vendor.bk_hash = #p_spabiz_vendor_insert.bk_hash
   and p_spabiz_vendor.p_spabiz_vendor_id = #p_spabiz_vendor_insert.p_spabiz_vendor_id
  join dbo.l_spabiz_vendor
    on p_spabiz_vendor.bk_hash = l_spabiz_vendor.bk_hash
   and p_spabiz_vendor.l_spabiz_vendor_id = l_spabiz_vendor.l_spabiz_vendor_id
  join dbo.s_spabiz_vendor
    on p_spabiz_vendor.bk_hash = s_spabiz_vendor.bk_hash
   and p_spabiz_vendor.s_spabiz_vendor_id = s_spabiz_vendor.s_spabiz_vendor_id
 where l_spabiz_vendor.store_number not in (1,100,999) OR p_spabiz_vendor.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_vendor
   where d_spabiz_vendor.bk_hash in (select bk_hash from #p_spabiz_vendor_insert)

  insert dbo.d_spabiz_vendor(
             bk_hash,
             dim_spabiz_vendor_key,
             vendor_id,
             store_number,
             address_1,
             address_2,
             city,
             customer_number,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_store_key,
             edit_date_time,
             email,
             fax,
             name,
             phone_number,
             postal_code,
             primary_contact,
             primary_contact_phone_extension,
             primary_contact_phone_number,
             primary_contact_title,
             quick_id,
             secondary_contact,
             secondary_contact_phone_extension,
             secondary_contact_phone_number,
             secondary_contact_title,
             state,
             p_spabiz_vendor_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_vendor_key,
         vendor_id,
         store_number,
         address_1,
         address_2,
         city,
         customer_number,
         deleted_date_time,
         deleted_flag,
         dim_spabiz_store_key,
         edit_date_time,
         email,
         fax,
         name,
         phone_number,
         postal_code,
         primary_contact,
         primary_contact_phone_extension,
         primary_contact_phone_number,
         primary_contact_title,
         quick_id,
         secondary_contact,
         secondary_contact_phone_extension,
         secondary_contact_phone_number,
         secondary_contact_title,
         state,
         p_spabiz_vendor_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_vendor)
--Done!
end
