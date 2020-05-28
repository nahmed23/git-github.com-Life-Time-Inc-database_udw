CREATE PROC [dbo].[proc_d_mms_eft] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_eft)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_eft_insert') is not null drop table #p_mms_eft_insert
create table dbo.#p_mms_eft_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_eft.p_mms_eft_id,
       p_mms_eft.bk_hash
  from dbo.p_mms_eft
 where p_mms_eft.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_eft.dv_batch_id > @max_dv_batch_id
        or p_mms_eft.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_eft.bk_hash,
       p_mms_eft.eft_id eft_id,
       s_mms_eft.account_number account_number,
       s_mms_eft.account_owner account_owner,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash         
        when l_mms_eft.eft_return_code_id is null then '-998'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft.eft_return_code_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mms_eft_return_code_bk_hash,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash         
        when l_mms_eft.member_id is null then '-998'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash         
        when l_mms_eft.membership_id is null then '-998'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_membership_key,
       s_mms_eft.dues_amount_used_for_products dues_amount_used_for_products,
       s_mms_eft.eft_amount eft_amount,
       s_mms_eft.eft_amount_products eft_amount_products,
       s_mms_eft.eft_date eft_date,
       case when p_mms_eft.bk_hash in('-997', '-998', '-999') then p_mms_eft.bk_hash
           when s_mms_eft.eft_date is null then '-998'
        else convert(varchar, s_mms_eft.eft_date, 112) end eft_dim_date_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash
       when s_mms_eft.eft_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft.eft_date,114), 1, 5),':','') end eft_dim_time_key,
       l_mms_eft.eft_return_code_id eft_return_code_id,
       s_mms_eft.expiration_date expiration_date,
       case when p_mms_eft.bk_hash in('-997', '-998', '-999') then p_mms_eft.bk_hash
           when s_mms_eft.expiration_date is null then '-998'
        else convert(varchar, s_mms_eft.expiration_date, 112) end expiration_dim_date_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash
       when s_mms_eft.expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft.expiration_date,114), 1, 5),':','') end expiration_dim_time_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash         
        when l_mms_eft.payment_id is null then '-998'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_eft.payment_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_payment_key,
       s_mms_eft.inserted_date_time inserted_date_time,
       case when p_mms_eft.bk_hash in('-997', '-998', '-999') then p_mms_eft.bk_hash
           when s_mms_eft.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_eft.inserted_date_time, 112) end inserted_dim_date_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash
       when s_mms_eft.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_eft.job_task_id job_task_id,
       s_mms_eft.masked_account_number masked_account_number,
       s_mms_eft.masked_account_number64 masked_account_number64,
       l_mms_eft.member_id member_id,
       l_mms_eft.membership_id membership_id,
       s_mms_eft.order_number order_number,
       l_mms_eft.payment_id payment_id,
       s_mms_eft.return_code return_code,
       s_mms_eft.routing_number routing_number,
       s_mms_eft_1.token token,
       s_mms_eft.updated_date_time updated_date_time,
       case when p_mms_eft.bk_hash in('-997', '-998', '-999') then p_mms_eft.bk_hash
           when s_mms_eft.updated_date_time is null then '-998'
        else convert(varchar, s_mms_eft.updated_date_time, 112) end updated_dim_date_key,
       case when p_mms_eft.bk_hash in ('-997','-998','-999') then p_mms_eft.bk_hash
       when s_mms_eft.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_eft.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       l_mms_eft_1.val_eft_account_type_id val_eft_account_type_id,
       l_mms_eft.val_eft_status_id val_eft_status_id,
       l_mms_eft.val_eft_type_id val_eft_type_id,
       l_mms_eft.val_payment_type_id val_payment_type_id,
       isnull(h_mms_eft.dv_deleted,0) dv_deleted,
       p_mms_eft.p_mms_eft_id,
       p_mms_eft.dv_batch_id,
       p_mms_eft.dv_load_date_time,
       p_mms_eft.dv_load_end_date_time
  from dbo.h_mms_eft
  join dbo.p_mms_eft
    on h_mms_eft.bk_hash = p_mms_eft.bk_hash
  join #p_mms_eft_insert
    on p_mms_eft.bk_hash = #p_mms_eft_insert.bk_hash
   and p_mms_eft.p_mms_eft_id = #p_mms_eft_insert.p_mms_eft_id
  join dbo.l_mms_eft
    on p_mms_eft.bk_hash = l_mms_eft.bk_hash
   and p_mms_eft.l_mms_eft_id = l_mms_eft.l_mms_eft_id
  join dbo.l_mms_eft_1
    on p_mms_eft.bk_hash = l_mms_eft_1.bk_hash
   and p_mms_eft.l_mms_eft_1_id = l_mms_eft_1.l_mms_eft_1_id
  join dbo.s_mms_eft
    on p_mms_eft.bk_hash = s_mms_eft.bk_hash
   and p_mms_eft.s_mms_eft_id = s_mms_eft.s_mms_eft_id
  join dbo.s_mms_eft_1
    on p_mms_eft.bk_hash = s_mms_eft_1.bk_hash
   and p_mms_eft.s_mms_eft_1_id = s_mms_eft_1.s_mms_eft_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_eft
   where d_mms_eft.bk_hash in (select bk_hash from #p_mms_eft_insert)

  insert dbo.d_mms_eft(
             bk_hash,
             eft_id,
             account_number,
             account_owner,
             d_mms_eft_return_code_bk_hash,
             dim_mms_member_key,
             dim_mms_membership_key,
             dues_amount_used_for_products,
             eft_amount,
             eft_amount_products,
             eft_date,
             eft_dim_date_key,
             eft_dim_time_key,
             eft_return_code_id,
             expiration_date,
             expiration_dim_date_key,
             expiration_dim_time_key,
             fact_mms_payment_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             job_task_id,
             masked_account_number,
             masked_account_number64,
             member_id,
             membership_id,
             order_number,
             payment_id,
             return_code,
             routing_number,
             token,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             val_eft_account_type_id,
             val_eft_status_id,
             val_eft_type_id,
             val_payment_type_id,
             deleted_flag,
             p_mms_eft_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         eft_id,
         account_number,
         account_owner,
         d_mms_eft_return_code_bk_hash,
         dim_mms_member_key,
         dim_mms_membership_key,
         dues_amount_used_for_products,
         eft_amount,
         eft_amount_products,
         eft_date,
         eft_dim_date_key,
         eft_dim_time_key,
         eft_return_code_id,
         expiration_date,
         expiration_dim_date_key,
         expiration_dim_time_key,
         fact_mms_payment_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         job_task_id,
         masked_account_number,
         masked_account_number64,
         member_id,
         membership_id,
         order_number,
         payment_id,
         return_code,
         routing_number,
         token,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         val_eft_account_type_id,
         val_eft_status_id,
         val_eft_type_id,
         val_payment_type_id,
         dv_deleted,
         p_mms_eft_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_eft)
--Done!
end
