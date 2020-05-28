CREATE PROC [dbo].[proc_d_spabiz_series_sold] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_series_sold)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_series_sold_insert') is not null drop table #p_spabiz_series_sold_insert
create table dbo.#p_spabiz_series_sold_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_series_sold.p_spabiz_series_sold_id,
       p_spabiz_series_sold.bk_hash
  from dbo.p_spabiz_series_sold
 where p_spabiz_series_sold.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_series_sold.dv_batch_id > @max_dv_batch_id
        or p_spabiz_series_sold.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_series_sold.bk_hash,
       p_spabiz_series_sold.bk_hash fact_spabiz_series_sold_key,
       p_spabiz_series_sold.series_sold_id series_sold_id,
       p_spabiz_series_sold.store_number store_number,
       s_spabiz_series_sold.balance balance,
       case when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_series_sold.date = convert(date, '18991230', 112) then null
            else s_spabiz_series_sold.date
        end created_date_time,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.cust_id is null then '-998'
            when l_spabiz_series_sold.cust_id in (0,-1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.cust_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_customer_key,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.series_id is null then '-998'
            when l_spabiz_series_sold.series_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.series_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_series_key,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.store_number is null then '-998'
            when l_spabiz_series_sold.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_series_sold.edit_time edit_date_time,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.ticket_id is null then '-998'
            when l_spabiz_series_sold.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.staff_id_1 is null then '-998'
            when l_spabiz_series_sold.staff_id_1 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.staff_id_1 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end first_dim_spabiz_staff_key,
       case when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_series_sold.last_used = convert(date, '18991230', 112) then null
            else s_spabiz_series_sold.last_used
        end last_used_date_time,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.buy_cust_id is null then '-998'
            when l_spabiz_series_sold.buy_cust_id in (0,-1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.buy_cust_id as varchar(500)),'z#@$k%&P'))),2)
        end purchasing_dim_spabiz_customer_key,
       s_spabiz_series_sold.retail_price retail_price,
       case
            when p_spabiz_series_sold.bk_hash in ('-997','-998','-999') then p_spabiz_series_sold.bk_hash
            when l_spabiz_series_sold.staff_id_2 is null then '-998'
            when l_spabiz_series_sold.staff_id_2 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series_sold.staff_id_2 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_series_sold.store_number as varchar(500)),'z#@$k%&P'))),2)
        end second_dim_spabiz_staff_key,
       's_spabiz_series_sold.status_' + convert(varchar,convert(int,s_spabiz_series_sold.status)) status_dim_description_key,
       convert(int,s_spabiz_series_sold.status) status_id,
       l_spabiz_series_sold.buy_cust_id l_spabiz_series_sold_buy_cust_id,
       l_spabiz_series_sold.cust_id l_spabiz_series_sold_cust_id,
       l_spabiz_series_sold.series_id l_spabiz_series_sold_series_id,
       l_spabiz_series_sold.staff_id_1 l_spabiz_series_sold_staff_id_1,
       l_spabiz_series_sold.staff_id_2 l_spabiz_series_sold_staff_id_2,
       l_spabiz_series_sold.ticket_id l_spabiz_series_sold_ticket_id,
       s_spabiz_series_sold.status s_spabiz_series_sold_status,
       p_spabiz_series_sold.p_spabiz_series_sold_id,
       p_spabiz_series_sold.dv_batch_id,
       p_spabiz_series_sold.dv_load_date_time,
       p_spabiz_series_sold.dv_load_end_date_time
  from dbo.h_spabiz_series_sold
  join dbo.p_spabiz_series_sold
    on h_spabiz_series_sold.bk_hash = p_spabiz_series_sold.bk_hash  join #p_spabiz_series_sold_insert
    on p_spabiz_series_sold.bk_hash = #p_spabiz_series_sold_insert.bk_hash
   and p_spabiz_series_sold.p_spabiz_series_sold_id = #p_spabiz_series_sold_insert.p_spabiz_series_sold_id
  join dbo.l_spabiz_series_sold
    on p_spabiz_series_sold.bk_hash = l_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.l_spabiz_series_sold_id = l_spabiz_series_sold.l_spabiz_series_sold_id
  join dbo.s_spabiz_series_sold
    on p_spabiz_series_sold.bk_hash = s_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.s_spabiz_series_sold_id = s_spabiz_series_sold.s_spabiz_series_sold_id
 where l_spabiz_series_sold.store_number not in (1,100,999) OR p_spabiz_series_sold.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_series_sold
   where d_spabiz_series_sold.bk_hash in (select bk_hash from #p_spabiz_series_sold_insert)

  insert dbo.d_spabiz_series_sold(
             bk_hash,
             fact_spabiz_series_sold_key,
             series_sold_id,
             store_number,
             balance,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_series_key,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_ticket_key,
             first_dim_spabiz_staff_key,
             last_used_date_time,
             purchasing_dim_spabiz_customer_key,
             retail_price,
             second_dim_spabiz_staff_key,
             status_dim_description_key,
             status_id,
             l_spabiz_series_sold_buy_cust_id,
             l_spabiz_series_sold_cust_id,
             l_spabiz_series_sold_series_id,
             l_spabiz_series_sold_staff_id_1,
             l_spabiz_series_sold_staff_id_2,
             l_spabiz_series_sold_ticket_id,
             s_spabiz_series_sold_status,
             p_spabiz_series_sold_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_spabiz_series_sold_key,
         series_sold_id,
         store_number,
         balance,
         created_date_time,
         dim_spabiz_customer_key,
         dim_spabiz_series_key,
         dim_spabiz_store_key,
         edit_date_time,
         fact_spabiz_ticket_key,
         first_dim_spabiz_staff_key,
         last_used_date_time,
         purchasing_dim_spabiz_customer_key,
         retail_price,
         second_dim_spabiz_staff_key,
         status_dim_description_key,
         status_id,
         l_spabiz_series_sold_buy_cust_id,
         l_spabiz_series_sold_cust_id,
         l_spabiz_series_sold_series_id,
         l_spabiz_series_sold_staff_id_1,
         l_spabiz_series_sold_staff_id_2,
         l_spabiz_series_sold_ticket_id,
         s_spabiz_series_sold_status,
         p_spabiz_series_sold_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_series_sold)
--Done!
end
