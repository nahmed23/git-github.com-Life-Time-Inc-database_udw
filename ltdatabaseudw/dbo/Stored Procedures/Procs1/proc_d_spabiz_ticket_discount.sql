CREATE PROC [dbo].[proc_d_spabiz_ticket_discount] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_ticket_discount)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_ticket_discount_insert') is not null drop table #p_spabiz_ticket_discount_insert
create table dbo.#p_spabiz_ticket_discount_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ticket_discount.p_spabiz_ticket_discount_id,
       p_spabiz_ticket_discount.bk_hash
  from dbo.p_spabiz_ticket_discount
 where p_spabiz_ticket_discount.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_ticket_discount.dv_batch_id > @max_dv_batch_id
        or p_spabiz_ticket_discount.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ticket_discount.bk_hash,
       p_spabiz_ticket_discount.bk_hash fact_spabiz_ticket_discount_key,
       p_spabiz_ticket_discount.ticket_discount_id ticket_discount_id,
       p_spabiz_ticket_discount.store_number store_number,
       s_spabiz_ticket_discount.amount amount,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ticket_discount.date = convert(date, '18991230', 112) then null
            else s_spabiz_ticket_discount.date
        end created_date_time,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash  
              when l_spabiz_ticket_discount.cust_id is null then '-998'     
              when l_spabiz_ticket_discount.cust_id = 0 then '-998'
              when l_spabiz_ticket_discount.cust_id = -1 then '-998'    
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.cust_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)       
       	   end dim_spabiz_customer_key,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash
            when l_spabiz_ticket_discount.discount_id is null then '-998'
            when l_spabiz_ticket_discount.discount_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.discount_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_discount_key,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash
            when l_spabiz_ticket_discount.product_id is null then '-998'
            when l_spabiz_ticket_discount.product_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.product_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_product_key,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash
            when l_spabiz_ticket_discount.shift_id is null then '-998'
            when l_spabiz_ticket_discount.shift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_shift_key,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash
            when l_spabiz_ticket_discount.store_number is null then '-998'
            when l_spabiz_ticket_discount.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case when s_spabiz_ticket_discount.status = 1 then 'Y'
            else 'N'
        end discount_processed_flag,
       s_spabiz_ticket_discount.edit_time edit_date_time,
       case when p_spabiz_ticket_discount.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_discount.bk_hash
            when l_spabiz_ticket_discount.ticket_id is null then '-998'
            when l_spabiz_ticket_discount.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       s_spabiz_ticket_discount.ticket_discount_percent ticket_discount_percent,
       l_spabiz_ticket_discount.cust_id l_spabiz_ticket_discount_cust_id,
       l_spabiz_ticket_discount.discount_id l_spabiz_ticket_discount_discount_id,
       l_spabiz_ticket_discount.product_id l_spabiz_ticket_discount_product_id,
       l_spabiz_ticket_discount.shift_id l_spabiz_ticket_discount_shift_id,
       l_spabiz_ticket_discount.ticket_id l_spabiz_ticket_discount_ticket_id,
       p_spabiz_ticket_discount.p_spabiz_ticket_discount_id,
       p_spabiz_ticket_discount.dv_batch_id,
       p_spabiz_ticket_discount.dv_load_date_time,
       p_spabiz_ticket_discount.dv_load_end_date_time
  from dbo.h_spabiz_ticket_discount
  join dbo.p_spabiz_ticket_discount
    on h_spabiz_ticket_discount.bk_hash = p_spabiz_ticket_discount.bk_hash  join #p_spabiz_ticket_discount_insert
    on p_spabiz_ticket_discount.bk_hash = #p_spabiz_ticket_discount_insert.bk_hash
   and p_spabiz_ticket_discount.p_spabiz_ticket_discount_id = #p_spabiz_ticket_discount_insert.p_spabiz_ticket_discount_id
  join dbo.l_spabiz_ticket_discount
    on p_spabiz_ticket_discount.bk_hash = l_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.l_spabiz_ticket_discount_id = l_spabiz_ticket_discount.l_spabiz_ticket_discount_id
  join dbo.s_spabiz_ticket_discount
    on p_spabiz_ticket_discount.bk_hash = s_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.s_spabiz_ticket_discount_id = s_spabiz_ticket_discount.s_spabiz_ticket_discount_id
 where l_spabiz_ticket_discount.store_number not in (1,100,999) OR p_spabiz_ticket_discount.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_ticket_discount
   where d_spabiz_ticket_discount.bk_hash in (select bk_hash from #p_spabiz_ticket_discount_insert)

  insert dbo.d_spabiz_ticket_discount(
             bk_hash,
             fact_spabiz_ticket_discount_key,
             ticket_discount_id,
             store_number,
             amount,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_discount_key,
             dim_spabiz_product_key,
             dim_spabiz_shift_key,
             dim_spabiz_store_key,
             discount_processed_flag,
             edit_date_time,
             fact_spabiz_ticket_key,
             ticket_discount_percent,
             l_spabiz_ticket_discount_cust_id,
             l_spabiz_ticket_discount_discount_id,
             l_spabiz_ticket_discount_product_id,
             l_spabiz_ticket_discount_shift_id,
             l_spabiz_ticket_discount_ticket_id,
             p_spabiz_ticket_discount_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_spabiz_ticket_discount_key,
         ticket_discount_id,
         store_number,
         amount,
         created_date_time,
         dim_spabiz_customer_key,
         dim_spabiz_discount_key,
         dim_spabiz_product_key,
         dim_spabiz_shift_key,
         dim_spabiz_store_key,
         discount_processed_flag,
         edit_date_time,
         fact_spabiz_ticket_key,
         ticket_discount_percent,
         l_spabiz_ticket_discount_cust_id,
         l_spabiz_ticket_discount_discount_id,
         l_spabiz_ticket_discount_product_id,
         l_spabiz_ticket_discount_shift_id,
         l_spabiz_ticket_discount_ticket_id,
         p_spabiz_ticket_discount_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_ticket_discount)
--Done!
end
