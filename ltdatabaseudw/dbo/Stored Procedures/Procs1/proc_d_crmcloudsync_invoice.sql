CREATE PROC [dbo].[proc_d_crmcloudsync_invoice] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_invoice)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_invoice_insert') is not null drop table #p_crmcloudsync_invoice_insert
create table dbo.#p_crmcloudsync_invoice_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_invoice.p_crmcloudsync_invoice_id,
       p_crmcloudsync_invoice.bk_hash
  from dbo.p_crmcloudsync_invoice
 where p_crmcloudsync_invoice.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_invoice.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_invoice.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_invoice.bk_hash,
       p_crmcloudsync_invoice.bk_hash fact_crm_invoice_key,
       p_crmcloudsync_invoice.invoice_id invoice_id,
       isnull(s_crmcloudsync_invoice.account_id_name,'') account_id_name,
       isnull(s_crmcloudsync_invoice.contact_id_name,'') contact_id_name,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.created_by as varchar(36)),'z#@$k%&P'))),2)  end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_invoice.created_by_name,'') created_by_name,
       case when p_crmcloudsync_invoice.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
          when s_crmcloudsync_invoice.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_invoice.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997','-998','-999') then p_crmcloudsync_invoice.bk_hash
       when s_crmcloudsync_invoice.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_invoice.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_invoice.created_on created_on,
       isnull(s_crmcloudsync_invoice.customer_id_name,'') customer_id_name,
       isnull(s_crmcloudsync_invoice.customer_id_type,'') customer_id_type,
       isnull(s_crmcloudsync_invoice.description,'') description,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.account_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.account_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_account_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.contact_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.contact_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_contact_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.customer_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.customer_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_customer_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.opportunity_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.opportunity_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_opportunity_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.owner_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.transaction_currency_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.transaction_currency_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_transaction_currency_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
    when l_crmcloudsync_invoice.ltf_membership_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_invoice.ltf_membership_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_membership_key,
       isnull(s_crmcloudsync_invoice.insert_user,'') insert_user,
       s_crmcloudsync_invoice.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_invoice.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
          when s_crmcloudsync_invoice.inserted_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_invoice.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997','-998','-999') then p_crmcloudsync_invoice.bk_hash
       when s_crmcloudsync_invoice.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_invoice.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       isnull(s_crmcloudsync_invoice.invoice_number,'') invoice_number,
       isnull(s_crmcloudsync_invoice.ltf_club_id_name,'') ltf_club_id_name,
       l_crmcloudsync_invoice.ltf_membership_id ltf_membership_id,
       s_crmcloudsync_invoice.ltf_membership_source ltf_membership_source,
       isnull(s_crmcloudsync_invoice.ltf_membership_source_name,'') ltf_membership_source_name,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_invoice.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_invoice.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
          when s_crmcloudsync_invoice.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_invoice.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997','-998','-999') then p_crmcloudsync_invoice.bk_hash
       when s_crmcloudsync_invoice.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_invoice.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_invoice.modified_on modified_on,
       isnull(s_crmcloudsync_invoice.name,'') name,
       isnull(s_crmcloudsync_invoice.opportunity_id_name,'') opportunity_id_name,
       isnull(s_crmcloudsync_invoice.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_invoice.owner_id_type,'') owner_id_type,
       l_crmcloudsync_invoice.owning_business_unit owning_business_unit,
       l_crmcloudsync_invoice.owning_team owning_team,
       case when p_crmcloudsync_invoice.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
           when l_crmcloudsync_invoice.owning_user is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_invoice.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       s_crmcloudsync_invoice.state_code state_code,
       isnull(s_crmcloudsync_invoice.state_code_name,'') state_code_name,
       s_crmcloudsync_invoice.status_code status_code,
       isnull(s_crmcloudsync_invoice.status_code_name,'') status_code_name,
       s_crmcloudsync_invoice.time_zone_rule_version_number time_zone_rule_version_number,
       s_crmcloudsync_invoice.total_amount total_amount,
       s_crmcloudsync_invoice.total_amount_base total_amount_base,
       s_crmcloudsync_invoice.total_discount_amount total_discount_amount,
       s_crmcloudsync_invoice.total_discount_amount_base total_discount_amount_base,
       s_crmcloudsync_invoice.total_line_item_amount total_line_item_amount,
       s_crmcloudsync_invoice.total_line_item_amount_base total_line_item_amount_base,
       s_crmcloudsync_invoice.total_line_item_discount_amount total_line_item_discount_amount,
       s_crmcloudsync_invoice.total_line_item_discount_amount_base total_line_item_discount_amount_base,
       s_crmcloudsync_invoice.total_tax total_tax,
       s_crmcloudsync_invoice.total_tax_base total_tax_base,
       isnull(s_crmcloudsync_invoice.transaction_currency_id_name,'') transaction_currency_id_name,
       isnull(s_crmcloudsync_invoice.update_user,'') update_user,
       s_crmcloudsync_invoice.updated_date_time updated_date_time,
       case when p_crmcloudsync_invoice.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_invoice.bk_hash
          when s_crmcloudsync_invoice.updated_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_invoice.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_invoice.bk_hash in ('-997','-998','-999') then p_crmcloudsync_invoice.bk_hash
       when s_crmcloudsync_invoice.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_invoice.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_invoice.dv_deleted,0) dv_deleted,
       p_crmcloudsync_invoice.p_crmcloudsync_invoice_id,
       p_crmcloudsync_invoice.dv_batch_id,
       p_crmcloudsync_invoice.dv_load_date_time,
       p_crmcloudsync_invoice.dv_load_end_date_time
  from dbo.h_crmcloudsync_invoice
  join dbo.p_crmcloudsync_invoice
    on h_crmcloudsync_invoice.bk_hash = p_crmcloudsync_invoice.bk_hash
  join #p_crmcloudsync_invoice_insert
    on p_crmcloudsync_invoice.bk_hash = #p_crmcloudsync_invoice_insert.bk_hash
   and p_crmcloudsync_invoice.p_crmcloudsync_invoice_id = #p_crmcloudsync_invoice_insert.p_crmcloudsync_invoice_id
  join dbo.l_crmcloudsync_invoice
    on p_crmcloudsync_invoice.bk_hash = l_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.l_crmcloudsync_invoice_id = l_crmcloudsync_invoice.l_crmcloudsync_invoice_id
  join dbo.s_crmcloudsync_invoice
    on p_crmcloudsync_invoice.bk_hash = s_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.s_crmcloudsync_invoice_id = s_crmcloudsync_invoice.s_crmcloudsync_invoice_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_invoice
   where d_crmcloudsync_invoice.bk_hash in (select bk_hash from #p_crmcloudsync_invoice_insert)

  insert dbo.d_crmcloudsync_invoice(
             bk_hash,
             fact_crm_invoice_key,
             invoice_id,
             account_id_name,
             contact_id_name,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             customer_id_name,
             customer_id_type,
             description,
             dim_crm_account_key,
             dim_crm_contact_key,
             dim_crm_customer_key,
             dim_crm_opportunity_key,
             dim_crm_owner_key,
             dim_crm_transaction_currency_key,
             dim_mms_membership_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             invoice_number,
             ltf_club_id_name,
             ltf_membership_id,
             ltf_membership_source,
             ltf_membership_source_name,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             name,
             opportunity_id_name,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user_dim_crm_system_user_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             time_zone_rule_version_number,
             total_amount,
             total_amount_base,
             total_discount_amount,
             total_discount_amount_base,
             total_line_item_amount,
             total_line_item_amount_base,
             total_line_item_discount_amount,
             total_line_item_discount_amount_base,
             total_tax,
             total_tax_base,
             transaction_currency_id_name,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_invoice_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_crm_invoice_key,
         invoice_id,
         account_id_name,
         contact_id_name,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         customer_id_name,
         customer_id_type,
         description,
         dim_crm_account_key,
         dim_crm_contact_key,
         dim_crm_customer_key,
         dim_crm_opportunity_key,
         dim_crm_owner_key,
         dim_crm_transaction_currency_key,
         dim_mms_membership_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         invoice_number,
         ltf_club_id_name,
         ltf_membership_id,
         ltf_membership_source,
         ltf_membership_source_name,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         name,
         opportunity_id_name,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user_dim_crm_system_user_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         time_zone_rule_version_number,
         total_amount,
         total_amount_base,
         total_discount_amount,
         total_discount_amount_base,
         total_line_item_amount,
         total_line_item_amount_base,
         total_line_item_discount_amount,
         total_line_item_discount_amount_base,
         total_tax,
         total_tax_base,
         transaction_currency_id_name,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_invoice_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_invoice)
--Done!
end
