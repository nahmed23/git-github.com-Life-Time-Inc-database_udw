CREATE PROC [dbo].[proc_d_spabiz_gift_recharge] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_gift_recharge)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_gift_recharge_insert') is not null drop table #p_spabiz_gift_recharge_insert
create table dbo.#p_spabiz_gift_recharge_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_gift_recharge.p_spabiz_gift_recharge_id,
       p_spabiz_gift_recharge.bk_hash
  from dbo.p_spabiz_gift_recharge
 where p_spabiz_gift_recharge.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_gift_recharge.dv_batch_id > @max_dv_batch_id
        or p_spabiz_gift_recharge.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_gift_recharge.bk_hash,
       p_spabiz_gift_recharge.bk_hash fact_spabiz_gift_recharge_key,
       p_spabiz_gift_recharge.gift_recharge_id gift_recharge_id,
       p_spabiz_gift_recharge.store_number store_number,
       s_spabiz_gift_recharge.exp_date expiration_date_time,
       case
            when p_spabiz_gift_recharge.bk_hash in ('-997','-998','-999') then p_spabiz_gift_recharge.bk_hash
            when l_spabiz_gift_recharge.store_number is null then '-998'
            when l_spabiz_gift_recharge.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_gift_recharge.edit_time edit_date_time,
       case
            when p_spabiz_gift_recharge.bk_hash in ('-997','-998','-999') then p_spabiz_gift_recharge.bk_hash
            when l_spabiz_gift_recharge.gift_id is null then '-998'
            when l_spabiz_gift_recharge.gift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.gift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_gift_certificate_key,
       case
            when p_spabiz_gift_recharge.bk_hash in ('-997','-998','-999') then p_spabiz_gift_recharge.bk_hash
            when l_spabiz_gift_recharge.ticket_data_id is null then '-998'
            when l_spabiz_gift_recharge.ticket_data_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.ticket_data_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_item_key,
       case
            when p_spabiz_gift_recharge.bk_hash in ('-997','-998','-999') then p_spabiz_gift_recharge.bk_hash
            when l_spabiz_gift_recharge.ticket_id is null then '-998'
            when l_spabiz_gift_recharge.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_gift_recharge.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       s_spabiz_gift_recharge.amount gift_recharge_amount,
       l_spabiz_gift_recharge.gift_id l_spabiz_gift_recharge_gift_id,
       l_spabiz_gift_recharge.ticket_data_id l_spabiz_gift_recharge_ticket_data_id,
       l_spabiz_gift_recharge.ticket_id l_spabiz_gift_recharge_ticket_id,
       p_spabiz_gift_recharge.p_spabiz_gift_recharge_id,
       p_spabiz_gift_recharge.dv_batch_id,
       p_spabiz_gift_recharge.dv_load_date_time,
       p_spabiz_gift_recharge.dv_load_end_date_time
  from dbo.p_spabiz_gift_recharge
  join #p_spabiz_gift_recharge_insert
    on p_spabiz_gift_recharge.bk_hash = #p_spabiz_gift_recharge_insert.bk_hash
   and p_spabiz_gift_recharge.p_spabiz_gift_recharge_id = #p_spabiz_gift_recharge_insert.p_spabiz_gift_recharge_id
  join dbo.l_spabiz_gift_recharge
    on p_spabiz_gift_recharge.bk_hash = l_spabiz_gift_recharge.bk_hash
   and p_spabiz_gift_recharge.l_spabiz_gift_recharge_id = l_spabiz_gift_recharge.l_spabiz_gift_recharge_id
  join dbo.s_spabiz_gift_recharge
    on p_spabiz_gift_recharge.bk_hash = s_spabiz_gift_recharge.bk_hash
   and p_spabiz_gift_recharge.s_spabiz_gift_recharge_id = s_spabiz_gift_recharge.s_spabiz_gift_recharge_id
 where l_spabiz_gift_recharge.store_number not in (1,100,999) OR p_spabiz_gift_recharge.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_gift_recharge
   where d_spabiz_gift_recharge.bk_hash in (select bk_hash from #p_spabiz_gift_recharge_insert)

  insert dbo.d_spabiz_gift_recharge(
             bk_hash,
             fact_spabiz_gift_recharge_key,
             gift_recharge_id,
             store_number,
             expiration_date_time,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_gift_certificate_key,
             fact_spabiz_ticket_item_key,
             fact_spabiz_ticket_key,
             gift_recharge_amount,
             l_spabiz_gift_recharge_gift_id,
             l_spabiz_gift_recharge_ticket_data_id,
             l_spabiz_gift_recharge_ticket_id,
             p_spabiz_gift_recharge_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_spabiz_gift_recharge_key,
         gift_recharge_id,
         store_number,
         expiration_date_time,
         dim_spabiz_store_key,
         edit_date_time,
         fact_spabiz_gift_certificate_key,
         fact_spabiz_ticket_item_key,
         fact_spabiz_ticket_key,
         gift_recharge_amount,
         l_spabiz_gift_recharge_gift_id,
         l_spabiz_gift_recharge_ticket_data_id,
         l_spabiz_gift_recharge_ticket_id,
         p_spabiz_gift_recharge_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_gift_recharge)
--Done!
end
