CREATE PROC [dbo].[proc_d_spabiz_cust_card] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_cust_card)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_cust_card_insert') is not null drop table #p_spabiz_cust_card_insert
create table dbo.#p_spabiz_cust_card_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_cust_card.p_spabiz_cust_card_id,
       p_spabiz_cust_card.bk_hash
  from dbo.p_spabiz_cust_card
 where p_spabiz_cust_card.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_cust_card.dv_batch_id > @max_dv_batch_id
        or p_spabiz_cust_card.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_cust_card.bk_hash,
       p_spabiz_cust_card.cust_card_id cust_card_id,
       p_spabiz_cust_card.store_number store_number,
       cast(l_spabiz_cust_card.buy_cust_id as bigint) buy_cust_id,
       l_spabiz_cust_card.cancel_id cancel_id,
       s_spabiz_cust_card.cancelled cancelled,
       s_spabiz_cust_card.counter_id counter_id,
       s_spabiz_cust_card.current_installment current_installment,
       l_spabiz_cust_card.cust_id customer_id,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.buy_cust_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.buy_cust_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_buy_cust_card_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
       when l_spabiz_cust_card.cust_card_type_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.cust_card_type_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2) end d_spabiz_cust_card_cust_card_type_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.cust_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.cust_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_customer_key,
       case when p_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card.bk_hash
           when s_spabiz_cust_card.date is null then '-998'
        else convert(varchar,s_spabiz_cust_card.date, 112)    end d_spabiz_cust_card_dim_date_key,
       case when p_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card.bk_hash
           when s_spabiz_cust_card.edit_time is null then '-998'
        else convert(varchar,s_spabiz_cust_card.edit_time, 112)    end d_spabiz_cust_card_edit_dim_date_key,
       case when p_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card.bk_hash
           when s_spabiz_cust_card.exp_date is null then '-998'
        else convert(varchar,s_spabiz_cust_card.exp_date, 112)    end d_spabiz_cust_card_exp_dim_date_key,
       case when p_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card.bk_hash
           when s_spabiz_cust_card.last_used is null then '-998'
        else convert(varchar,s_spabiz_cust_card.last_used, 112)    end d_spabiz_cust_card_last_used_key,
       case when p_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card.bk_hash
           when s_spabiz_cust_card.next_billing_date is null then '-998'
        else convert(varchar,s_spabiz_cust_card.next_billing_date, 112)    end d_spabiz_cust_card_next_billing_dim_date_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.staff_id_create is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.staff_id_create as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_staff_create_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.staff_id_1 is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.staff_id_1 as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_staff_id_key_1,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.staff_id_2 is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.staff_id_2 as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_staff_id_key_2,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.store_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.store_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_store_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.ticket_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.ticket_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_ticket_key,
       case when l_spabiz_cust_card.bk_hash in('-997', '-998', '-999') then l_spabiz_cust_card.bk_hash
           when l_spabiz_cust_card.cust_card_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card.cust_card_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_type_key,
       s_spabiz_cust_card.days_good days_good,
       case when s_spabiz_cust_card.deleted = -1 then 'Y' else 'N' end deleted_flag,
       s_spabiz_cust_card.initial_amount initial_amount,
       s_spabiz_cust_card.mem_type mem_type,
       s_spabiz_cust_card.price price,
       s_spabiz_cust_card.product_sales product_sales,
       s_spabiz_cust_card.prorated_amount prorated_amount,
       s_spabiz_cust_card.recurring recurring,
       s_spabiz_cust_card.recurring_after_expire recurring_after_expire,
       s_spabiz_cust_card.recurring_declined recurring_declined,
       s_spabiz_cust_card.recurring_declined_reason recurring_declined_reason,
       s_spabiz_cust_card.serial_num serial_num,
       s_spabiz_cust_card.service_sales service_sales,
       s_spabiz_cust_card.status status,
       s_spabiz_cust_card.total_sales total_sales,
       s_spabiz_cust_card.ytd_sales ytd_sales,
       isnull(h_spabiz_cust_card.dv_deleted,0) dv_deleted,
       p_spabiz_cust_card.p_spabiz_cust_card_id,
       p_spabiz_cust_card.dv_batch_id,
       p_spabiz_cust_card.dv_load_date_time,
       p_spabiz_cust_card.dv_load_end_date_time
  from dbo.h_spabiz_cust_card
  join dbo.p_spabiz_cust_card
    on h_spabiz_cust_card.bk_hash = p_spabiz_cust_card.bk_hash
  join #p_spabiz_cust_card_insert
    on p_spabiz_cust_card.bk_hash = #p_spabiz_cust_card_insert.bk_hash
   and p_spabiz_cust_card.p_spabiz_cust_card_id = #p_spabiz_cust_card_insert.p_spabiz_cust_card_id
  join dbo.l_spabiz_cust_card
    on p_spabiz_cust_card.bk_hash = l_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.l_spabiz_cust_card_id = l_spabiz_cust_card.l_spabiz_cust_card_id
  join dbo.s_spabiz_cust_card
    on p_spabiz_cust_card.bk_hash = s_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.s_spabiz_cust_card_id = s_spabiz_cust_card.s_spabiz_cust_card_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_cust_card
   where d_spabiz_cust_card.bk_hash in (select bk_hash from #p_spabiz_cust_card_insert)

  insert dbo.d_spabiz_cust_card(
             bk_hash,
             cust_card_id,
             store_number,
             buy_cust_id,
             cancel_id,
             cancelled,
             counter_id,
             current_installment,
             customer_id,
             d_spabiz_buy_cust_card_key,
             d_spabiz_cust_card_cust_card_type_key,
             d_spabiz_cust_card_customer_key,
             d_spabiz_cust_card_dim_date_key,
             d_spabiz_cust_card_edit_dim_date_key,
             d_spabiz_cust_card_exp_dim_date_key,
             d_spabiz_cust_card_last_used_key,
             d_spabiz_cust_card_next_billing_dim_date_key,
             d_spabiz_cust_card_staff_create_key,
             d_spabiz_cust_card_staff_id_key_1,
             d_spabiz_cust_card_staff_id_key_2,
             d_spabiz_cust_card_store_key,
             d_spabiz_cust_card_ticket_key,
             d_spabiz_cust_card_type_key,
             days_good,
             deleted_flag,
             initial_amount,
             mem_type,
             price,
             product_sales,
             prorated_amount,
             recurring,
             recurring_after_expire,
             recurring_declined,
             recurring_declined_reason,
             serial_num,
             service_sales,
             status,
             total_sales,
             ytd_sales,
             p_spabiz_cust_card_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         cust_card_id,
         store_number,
         buy_cust_id,
         cancel_id,
         cancelled,
         counter_id,
         current_installment,
         customer_id,
         d_spabiz_buy_cust_card_key,
         d_spabiz_cust_card_cust_card_type_key,
         d_spabiz_cust_card_customer_key,
         d_spabiz_cust_card_dim_date_key,
         d_spabiz_cust_card_edit_dim_date_key,
         d_spabiz_cust_card_exp_dim_date_key,
         d_spabiz_cust_card_last_used_key,
         d_spabiz_cust_card_next_billing_dim_date_key,
         d_spabiz_cust_card_staff_create_key,
         d_spabiz_cust_card_staff_id_key_1,
         d_spabiz_cust_card_staff_id_key_2,
         d_spabiz_cust_card_store_key,
         d_spabiz_cust_card_ticket_key,
         d_spabiz_cust_card_type_key,
         days_good,
         deleted_flag,
         initial_amount,
         mem_type,
         price,
         product_sales,
         prorated_amount,
         recurring,
         recurring_after_expire,
         recurring_declined,
         recurring_declined_reason,
         serial_num,
         service_sales,
         status,
         total_sales,
         ytd_sales,
         p_spabiz_cust_card_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_cust_card)
--Done!
end
