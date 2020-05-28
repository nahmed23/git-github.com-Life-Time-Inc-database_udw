CREATE PROC [dbo].[proc_d_spabiz_gift_certificate] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_gift_certificate)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_gift_certificate_insert') is not null drop table #p_spabiz_gift_certificate_insert
create table dbo.#p_spabiz_gift_certificate_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_gift_certificate.p_spabiz_gift_certificate_id,
       p_spabiz_gift_certificate.bk_hash
  from dbo.p_spabiz_gift_certificate
 where p_spabiz_gift_certificate.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_gift_certificate.dv_batch_id > @max_dv_batch_id
        or p_spabiz_gift_certificate.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_gift_certificate.bk_hash,
       p_spabiz_gift_certificate.bk_hash fact_spabiz_gift_certificate_key,
       p_spabiz_gift_certificate.gift_certificate_id gift_certificate_id,
       p_spabiz_gift_certificate.store_number store_number,
       case
            when s_spabiz_gift_certificate.date = convert(date, '18991230', 112) then null
            else s_spabiz_gift_certificate.date
        end created_date_time,
       case when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
              when l_spabiz_gift_certificate.cust_id is null then '-998'      
              when l_spabiz_gift_certificate.cust_id = 0 then '-998'     
              when l_spabiz_gift_certificate.cust_id = -1 then '-998'     
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.cust_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
              end dim_spabiz_customer_key,
       case
            when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
            when l_spabiz_gift_certificate.gift_id is null then '-998'
            when l_spabiz_gift_certificate.gift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.gift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_gift_certificate_type_key,
       case
            when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
            when l_spabiz_gift_certificate.store_number is null then '-998'
            when l_spabiz_gift_certificate.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_gift_certificate.edit_time edit_date_time,
       case
            when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
            when l_spabiz_gift_certificate.ticket_id is null then '-998'
            when l_spabiz_gift_certificate.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       case
            when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
            when l_spabiz_gift_certificate.staff_id_1 is null then '-998'
            when l_spabiz_gift_certificate.staff_id_1 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.staff_id_1 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
        end first_dim_spabiz_staff_key,
       s_spabiz_gift_certificate.amount gift_certificate_amount,
       s_spabiz_gift_certificate.balance gift_certificate_balance,
       case when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash      
              when l_spabiz_gift_certificate.buy_cust_id is null then '-998'      
              when l_spabiz_gift_certificate.buy_cust_id = 0 then '-998'    
              when l_spabiz_gift_certificate.buy_cust_id = -1 then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.buy_cust_id as varchar(500)),'z#@$k%&P'))),2)	   
       	   end purchasing_dim_spabiz_customer_key,
       case
            when p_spabiz_gift_certificate.bk_hash in ('-997','-998','-999') then p_spabiz_gift_certificate.bk_hash
            when l_spabiz_gift_certificate.staff_id_2 is null then '-998'
            when l_spabiz_gift_certificate.staff_id_2 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.staff_id_2 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_certificate.store_number as varchar(500)),'z#@$k%&P'))),2)
        end second_dim_spabiz_staff_key,
       s_spabiz_gift_certificate.sell_amount selling_amount,
       s_spabiz_gift_certificate.serial_num serial_number,
       convert(int,s_spabiz_gift_certificate.status) status_description_id,
       's_spabiz_gift_certificate.status_' + convert(varchar,convert(int,s_spabiz_gift_certificate.status)) status_dim_description_key,
       s_spabiz_gift_certificate.status status_id,
       l_spabiz_gift_certificate.buy_cust_id l_spabiz_gift_certificate_buy_cust_id,
       l_spabiz_gift_certificate.cust_id l_spabiz_gift_certificate_cust_id,
       l_spabiz_gift_certificate.gift_id l_spabiz_gift_certificate_gift_id,
       l_spabiz_gift_certificate.staff_id_1 l_spabiz_gift_certificate_staff_id_1,
       l_spabiz_gift_certificate.staff_id_2 l_spabiz_gift_certificate_staff_id_2,
       l_spabiz_gift_certificate.ticket_id l_spabiz_gift_certificate_ticket_id,
       p_spabiz_gift_certificate.p_spabiz_gift_certificate_id,
       p_spabiz_gift_certificate.dv_batch_id,
       p_spabiz_gift_certificate.dv_load_date_time,
       p_spabiz_gift_certificate.dv_load_end_date_time
  from dbo.h_spabiz_gift_certificate
  join dbo.p_spabiz_gift_certificate
    on h_spabiz_gift_certificate.bk_hash = p_spabiz_gift_certificate.bk_hash  join #p_spabiz_gift_certificate_insert
    on p_spabiz_gift_certificate.bk_hash = #p_spabiz_gift_certificate_insert.bk_hash
   and p_spabiz_gift_certificate.p_spabiz_gift_certificate_id = #p_spabiz_gift_certificate_insert.p_spabiz_gift_certificate_id
  join dbo.l_spabiz_gift_certificate
    on p_spabiz_gift_certificate.bk_hash = l_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.l_spabiz_gift_certificate_id = l_spabiz_gift_certificate.l_spabiz_gift_certificate_id
  join dbo.s_spabiz_gift_certificate
    on p_spabiz_gift_certificate.bk_hash = s_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.s_spabiz_gift_certificate_id = s_spabiz_gift_certificate.s_spabiz_gift_certificate_id
 where l_spabiz_gift_certificate.store_number not in (1,100,999) OR p_spabiz_gift_certificate.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_gift_certificate
   where d_spabiz_gift_certificate.bk_hash in (select bk_hash from #p_spabiz_gift_certificate_insert)

  insert dbo.d_spabiz_gift_certificate(
             bk_hash,
             fact_spabiz_gift_certificate_key,
             gift_certificate_id,
             store_number,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_gift_certificate_type_key,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_ticket_key,
             first_dim_spabiz_staff_key,
             gift_certificate_amount,
             gift_certificate_balance,
             purchasing_dim_spabiz_customer_key,
             second_dim_spabiz_staff_key,
             selling_amount,
             serial_number,
             status_description_id,
             status_dim_description_key,
             status_id,
             l_spabiz_gift_certificate_buy_cust_id,
             l_spabiz_gift_certificate_cust_id,
             l_spabiz_gift_certificate_gift_id,
             l_spabiz_gift_certificate_staff_id_1,
             l_spabiz_gift_certificate_staff_id_2,
             l_spabiz_gift_certificate_ticket_id,
             p_spabiz_gift_certificate_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_spabiz_gift_certificate_key,
         gift_certificate_id,
         store_number,
         created_date_time,
         dim_spabiz_customer_key,
         dim_spabiz_gift_certificate_type_key,
         dim_spabiz_store_key,
         edit_date_time,
         fact_spabiz_ticket_key,
         first_dim_spabiz_staff_key,
         gift_certificate_amount,
         gift_certificate_balance,
         purchasing_dim_spabiz_customer_key,
         second_dim_spabiz_staff_key,
         selling_amount,
         serial_number,
         status_description_id,
         status_dim_description_key,
         status_id,
         l_spabiz_gift_certificate_buy_cust_id,
         l_spabiz_gift_certificate_cust_id,
         l_spabiz_gift_certificate_gift_id,
         l_spabiz_gift_certificate_staff_id_1,
         l_spabiz_gift_certificate_staff_id_2,
         l_spabiz_gift_certificate_ticket_id,
         p_spabiz_gift_certificate_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_gift_certificate)
--Done!
end
