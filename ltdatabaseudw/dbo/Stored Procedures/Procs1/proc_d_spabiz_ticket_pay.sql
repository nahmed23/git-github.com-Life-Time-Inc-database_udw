CREATE PROC [dbo].[proc_d_spabiz_ticket_pay] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_ticket_pay)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_ticket_pay_insert') is not null drop table #p_spabiz_ticket_pay_insert
create table dbo.#p_spabiz_ticket_pay_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ticket_pay.p_spabiz_ticket_pay_id,
       p_spabiz_ticket_pay.bk_hash
  from dbo.p_spabiz_ticket_pay
 where p_spabiz_ticket_pay.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_ticket_pay.dv_batch_id > @max_dv_batch_id
        or p_spabiz_ticket_pay.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ticket_pay.bk_hash,
       p_spabiz_ticket_pay.bk_hash fact_spabiz_ticket_payment_key,
       p_spabiz_ticket_pay.ticket_pay_id ticket_pay_id,
       p_spabiz_ticket_pay.store_number store_number,
       s_spabiz_ticket_pay.date created_date_time,
       case when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash     
              when l_spabiz_ticket_pay.cust_id is null then '-998'   
              when l_spabiz_ticket_pay.cust_id = 0 then '-998'   
              when l_spabiz_ticket_pay.cust_id = -1 then '-998'   
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.cust_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)       
       	   end dim_spabiz_customer_key,
       case
            when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash
            when l_spabiz_ticket_pay.pay_id is null then '-998'
            when l_spabiz_ticket_pay.pay_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.pay_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_payment_type_key,
       case
            when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash
            when l_spabiz_ticket_pay.shift_id is null then '-998'
            when l_spabiz_ticket_pay.shift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_shift_key,
       case
            when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash
            when l_spabiz_ticket_pay.store_number is null then '-998'
            when l_spabiz_ticket_pay.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ticket_pay.edit_time edit_date_time,
       case
            when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash
            when l_spabiz_ticket_pay.ticket_id is null then '-998'
            when l_spabiz_ticket_pay.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       case
            when p_spabiz_ticket_pay.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_pay.bk_hash
            when l_spabiz_ticket_pay.ref_id is null then '-998'
            when l_spabiz_ticket_pay.ref_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.ref_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_pay.store_number as varchar(500)),'z#@$k%&P'))),2)
        end hash_for_reference_record_for_payment_type,
       s_spabiz_ticket_pay.amount payment_amount,
       case
            when s_spabiz_ticket_pay.ok = 1 then 'Y'
            else 'N'
        end payment_checked_during_close,
       s_spabiz_ticket_pay.pay_num payment_number,
       's_spabiz_ticket_pay.status_' + convert(varchar,convert(int,s_spabiz_ticket_pay.status)) payment_status_dim_description_key,
       convert(int,s_spabiz_ticket_pay.status) payment_status_id,
       l_spabiz_ticket_pay.cust_id l_spabiz_ticket_pay_cust_id,
       l_spabiz_ticket_pay.pay_id l_spabiz_ticket_pay_pay_id,
       l_spabiz_ticket_pay.ref_id l_spabiz_ticket_pay_ref_id,
       l_spabiz_ticket_pay.shift_id l_spabiz_ticket_pay_shift_id,
       l_spabiz_ticket_pay.ticket_id l_spabiz_ticket_pay_ticket_id,
       p_spabiz_ticket_pay.p_spabiz_ticket_pay_id,
       p_spabiz_ticket_pay.dv_batch_id,
       p_spabiz_ticket_pay.dv_load_date_time,
       p_spabiz_ticket_pay.dv_load_end_date_time
  from dbo.h_spabiz_ticket_pay
  join dbo.p_spabiz_ticket_pay
    on h_spabiz_ticket_pay.bk_hash = p_spabiz_ticket_pay.bk_hash  join #p_spabiz_ticket_pay_insert
    on p_spabiz_ticket_pay.bk_hash = #p_spabiz_ticket_pay_insert.bk_hash
   and p_spabiz_ticket_pay.p_spabiz_ticket_pay_id = #p_spabiz_ticket_pay_insert.p_spabiz_ticket_pay_id
  join dbo.l_spabiz_ticket_pay
    on p_spabiz_ticket_pay.bk_hash = l_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.l_spabiz_ticket_pay_id = l_spabiz_ticket_pay.l_spabiz_ticket_pay_id
  join dbo.s_spabiz_ticket_pay
    on p_spabiz_ticket_pay.bk_hash = s_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.s_spabiz_ticket_pay_id = s_spabiz_ticket_pay.s_spabiz_ticket_pay_id
 where l_spabiz_ticket_pay.store_number not in (1,100,999) OR p_spabiz_ticket_pay.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_ticket_pay
   where d_spabiz_ticket_pay.bk_hash in (select bk_hash from #p_spabiz_ticket_pay_insert)

  insert dbo.d_spabiz_ticket_pay(
             bk_hash,
             fact_spabiz_ticket_payment_key,
             ticket_pay_id,
             store_number,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_payment_type_key,
             dim_spabiz_shift_key,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_ticket_key,
             hash_for_reference_record_for_payment_type,
             payment_amount,
             payment_checked_during_close,
             payment_number,
             payment_status_dim_description_key,
             payment_status_id,
             l_spabiz_ticket_pay_cust_id,
             l_spabiz_ticket_pay_pay_id,
             l_spabiz_ticket_pay_ref_id,
             l_spabiz_ticket_pay_shift_id,
             l_spabiz_ticket_pay_ticket_id,
             p_spabiz_ticket_pay_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_spabiz_ticket_payment_key,
         ticket_pay_id,
         store_number,
         created_date_time,
         dim_spabiz_customer_key,
         dim_spabiz_payment_type_key,
         dim_spabiz_shift_key,
         dim_spabiz_store_key,
         edit_date_time,
         fact_spabiz_ticket_key,
         hash_for_reference_record_for_payment_type,
         payment_amount,
         payment_checked_during_close,
         payment_number,
         payment_status_dim_description_key,
         payment_status_id,
         l_spabiz_ticket_pay_cust_id,
         l_spabiz_ticket_pay_pay_id,
         l_spabiz_ticket_pay_ref_id,
         l_spabiz_ticket_pay_shift_id,
         l_spabiz_ticket_pay_ticket_id,
         p_spabiz_ticket_pay_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_ticket_pay)
--Done!
end
