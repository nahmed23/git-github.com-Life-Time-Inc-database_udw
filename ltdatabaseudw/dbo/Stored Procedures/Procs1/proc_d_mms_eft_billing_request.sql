CREATE PROC [dbo].[proc_d_mms_eft_billing_request] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_eft_billing_request)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_eft_billing_request_insert') is not null drop table #p_mms_eft_billing_request_insert
create table dbo.#p_mms_eft_billing_request_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_eft_billing_request.p_mms_eft_billing_request_id,
       p_mms_eft_billing_request.bk_hash
  from dbo.p_mms_eft_billing_request
 where p_mms_eft_billing_request.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_eft_billing_request.dv_batch_id > @max_dv_batch_id
        or p_mms_eft_billing_request.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_eft_billing_request.bk_hash,
       p_mms_eft_billing_request.eft_billing_request_id eft_billing_request_id,
       s_mms_eft_billing_request.commission_employee commission_employee,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
         when (l_mms_eft_billing_request.club_id is null or l_mms_eft_billing_request.club_id ='' ) then '-998'   
         when (isnumeric(l_mms_eft_billing_request.club_id) = 0 or charindex('.',l_mms_eft_billing_request.club_id,1) > 0 
         or len(l_mms_eft_billing_request.club_id) >= 10 ) then '-999'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_mms_eft_billing_request.club_id, PATINDEX('%[0-9]%',l_mms_eft_billing_request.club_id), 50) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_club_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
       	when l_mms_eft_billing_request.subscription_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_eft_billing_request.subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_subscription_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
        when (l_mms_eft_billing_request.person_id is null or l_mms_eft_billing_request.person_id ='' ) then '-998' 
       	when (isnumeric(Replace(l_mms_eft_billing_request.person_id,'e','')) = 0  or charindex('.',l_mms_eft_billing_request.person_id,1) > 0 
       	or len(l_mms_eft_billing_request.person_id) >= 10 ) then '-999'
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_mms_eft_billing_request.person_id, PATINDEX('%[0-9]%',l_mms_eft_billing_request.person_id), 50) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
       	when (l_mms_eft_billing_request.product_id is null or l_mms_eft_billing_request.product_id ='' ) then '-998'   
         when ( isnumeric(l_mms_eft_billing_request.product_id) = 0 or charindex('.',l_mms_eft_billing_request.product_id,1) > 0 
       	or len(l_mms_eft_billing_request.product_id) >= 10  ) then '-999'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft_billing_request.product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_product_key,
       l_mms_eft_billing_request.external_item_id external_item_id,
       l_mms_eft_billing_request.external_package_id external_package_id,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
       	when l_mms_eft_billing_request.package_id is null then '-998'   
       	when ( isnumeric(l_mms_eft_billing_request.package_id) = 0 or charindex('.',l_mms_eft_billing_request.package_id,1) > 0) then '-999' 
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft_billing_request.package_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_package_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash     
       	when l_mms_eft_billing_request.mms_tran_id is null then '-998'   
       	when ( isnumeric(l_mms_eft_billing_request.mms_tran_id) = 0 or charindex('.',l_mms_eft_billing_request.mms_tran_id,1) > 0 ) then '-999' 
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft_billing_request.mms_tran_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_sales_transaction_key,
       s_mms_eft_billing_request.file_name file_name,
       case when p_mms_eft_billing_request.bk_hash in('-997', '-998', '-999') then p_mms_eft_billing_request.bk_hash
           when s_mms_eft_billing_request.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_eft_billing_request.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash
       when s_mms_eft_billing_request.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft_billing_request.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_mms_eft_billing_request.message message,
       l_mms_eft_billing_request.original_external_item_id original_external_item_id,
       s_mms_eft_billing_request.payment_request_reference payment_request_reference,
       s_mms_eft_billing_request.product_price product_price,
       s_mms_eft_billing_request.quantity quantity,
       s_mms_eft_billing_request.response_code response_code,
       s_mms_eft_billing_request.total_amount total_amount,
       s_mms_eft_billing_request.transaction_source transaction_source,
       case when p_mms_eft_billing_request.bk_hash in('-997', '-998', '-999') then p_mms_eft_billing_request.bk_hash
           when s_mms_eft_billing_request.updated_date_time is null then '-998'
        else convert(varchar, s_mms_eft_billing_request.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_eft_billing_request.bk_hash in ('-997','-998','-999') then p_mms_eft_billing_request.bk_hash
       when s_mms_eft_billing_request.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft_billing_request.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_mms_eft_billing_request.dv_deleted,0) dv_deleted,
       p_mms_eft_billing_request.p_mms_eft_billing_request_id,
       p_mms_eft_billing_request.dv_batch_id,
       p_mms_eft_billing_request.dv_load_date_time,
       p_mms_eft_billing_request.dv_load_end_date_time
  from dbo.h_mms_eft_billing_request
  join dbo.p_mms_eft_billing_request
    on h_mms_eft_billing_request.bk_hash = p_mms_eft_billing_request.bk_hash
  join #p_mms_eft_billing_request_insert
    on p_mms_eft_billing_request.bk_hash = #p_mms_eft_billing_request_insert.bk_hash
   and p_mms_eft_billing_request.p_mms_eft_billing_request_id = #p_mms_eft_billing_request_insert.p_mms_eft_billing_request_id
  join dbo.l_mms_eft_billing_request
    on p_mms_eft_billing_request.bk_hash = l_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.l_mms_eft_billing_request_id = l_mms_eft_billing_request.l_mms_eft_billing_request_id
  join dbo.s_mms_eft_billing_request
    on p_mms_eft_billing_request.bk_hash = s_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.s_mms_eft_billing_request_id = s_mms_eft_billing_request.s_mms_eft_billing_request_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_eft_billing_request
   where d_mms_eft_billing_request.bk_hash in (select bk_hash from #p_mms_eft_billing_request_insert)

  insert dbo.d_mms_eft_billing_request(
             bk_hash,
             eft_billing_request_id,
             commission_employee,
             dim_club_key,
             dim_exerp_subscription_key,
             dim_mms_member_key,
             dim_mms_product_key,
             external_item_id,
             external_package_id,
             fact_mms_package_key,
             fact_mms_sales_transaction_key,
             file_name,
             inserted_dim_date_key,
             inserted_dim_time_key,
             message,
             original_external_item_id,
             payment_request_reference,
             product_price,
             quantity,
             response_code,
             total_amount,
             transaction_source,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_mms_eft_billing_request_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         eft_billing_request_id,
         commission_employee,
         dim_club_key,
         dim_exerp_subscription_key,
         dim_mms_member_key,
         dim_mms_product_key,
         external_item_id,
         external_package_id,
         fact_mms_package_key,
         fact_mms_sales_transaction_key,
         file_name,
         inserted_dim_date_key,
         inserted_dim_time_key,
         message,
         original_external_item_id,
         payment_request_reference,
         product_price,
         quantity,
         response_code,
         total_amount,
         transaction_source,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_mms_eft_billing_request_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_eft_billing_request)
--Done!
end
