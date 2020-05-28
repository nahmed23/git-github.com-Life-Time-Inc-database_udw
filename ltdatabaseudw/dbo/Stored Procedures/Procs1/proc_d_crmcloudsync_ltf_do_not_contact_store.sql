CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_do_not_contact_store] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_do_not_contact_store)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_do_not_contact_store_insert') is not null drop table #p_crmcloudsync_ltf_do_not_contact_store_insert
create table dbo.#p_crmcloudsync_ltf_do_not_contact_store_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_do_not_contact_store.p_crmcloudsync_ltf_do_not_contact_store_id,
       p_crmcloudsync_ltf_do_not_contact_store.bk_hash
  from dbo.p_crmcloudsync_ltf_do_not_contact_store
 where p_crmcloudsync_ltf_do_not_contact_store.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_do_not_contact_store.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_do_not_contact_store.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_do_not_contact_store.bk_hash,
       p_crmcloudsync_ltf_do_not_contact_store.bk_hash dim_crm_ltf_do_not_contact_store_key,
       l_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id ltf_do_not_contact_store_id,
       case when p_crmcloudsync_ltf_do_not_contact_store.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_do_not_contact_store.bk_hash
           when s_crmcloudsync_ltf_do_not_contact_store.createdon is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_do_not_contact_store.createdon, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_do_not_contact_store.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_do_not_contact_store.bk_hash
       when s_crmcloudsync_ltf_do_not_contact_store.createdon is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_do_not_contact_store.createdon,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_do_not_contact_store.createdon created_on,
       case when p_crmcloudsync_ltf_do_not_contact_store.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_do_not_contact_store.bk_hash
           when s_crmcloudsync_ltf_do_not_contact_store.ltf_contact_lookup is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_ltf_do_not_contact_store.ltf_contact_lookup as varchar(36)),'z#@$k%&P'))),2) end dim_crm_contact_key,
       case when p_crmcloudsync_ltf_do_not_contact_store.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_do_not_contact_store.bk_hash
           when s_crmcloudsync_ltf_do_not_contact_store.ltf_lead_lookup is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_ltf_do_not_contact_store.ltf_lead_lookup as varchar(36)),'z#@$k%&P'))),2) end dim_crm_lead_key,
       isnull(s_crmcloudsync_ltf_do_not_contact_store.ltf_email_address1,'') ltf_email_address1,
       isnull(h_crmcloudsync_ltf_do_not_contact_store.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_do_not_contact_store.p_crmcloudsync_ltf_do_not_contact_store_id,
       p_crmcloudsync_ltf_do_not_contact_store.dv_batch_id,
       p_crmcloudsync_ltf_do_not_contact_store.dv_load_date_time,
       p_crmcloudsync_ltf_do_not_contact_store.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_do_not_contact_store
  join dbo.p_crmcloudsync_ltf_do_not_contact_store
    on h_crmcloudsync_ltf_do_not_contact_store.bk_hash = p_crmcloudsync_ltf_do_not_contact_store.bk_hash
  join #p_crmcloudsync_ltf_do_not_contact_store_insert
    on p_crmcloudsync_ltf_do_not_contact_store.bk_hash = #p_crmcloudsync_ltf_do_not_contact_store_insert.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.p_crmcloudsync_ltf_do_not_contact_store_id = #p_crmcloudsync_ltf_do_not_contact_store_insert.p_crmcloudsync_ltf_do_not_contact_store_id
  join dbo.l_crmcloudsync_ltf_do_not_contact_store
    on p_crmcloudsync_ltf_do_not_contact_store.bk_hash = l_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id = l_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id
  join dbo.s_crmcloudsync_ltf_do_not_contact_store
    on p_crmcloudsync_ltf_do_not_contact_store.bk_hash = s_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id = s_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_do_not_contact_store
   where d_crmcloudsync_ltf_do_not_contact_store.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_do_not_contact_store_insert)

  insert dbo.d_crmcloudsync_ltf_do_not_contact_store(
             bk_hash,
             dim_crm_ltf_do_not_contact_store_key,
             ltf_do_not_contact_store_id,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_contact_key,
             dim_crm_lead_key,
             ltf_email_address1,
             deleted_flag,
             p_crmcloudsync_ltf_do_not_contact_store_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_do_not_contact_store_key,
         ltf_do_not_contact_store_id,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_contact_key,
         dim_crm_lead_key,
         ltf_email_address1,
         dv_deleted,
         p_crmcloudsync_ltf_do_not_contact_store_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_do_not_contact_store)
--Done!
end
