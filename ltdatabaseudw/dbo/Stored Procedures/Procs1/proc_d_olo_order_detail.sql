CREATE PROC [dbo].[proc_d_olo_order_detail] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_olo_order_detail)
declare @previous_dv_batch_id bigint =(select max(dv_batch_id) from s_olo_order_detail where dv_batch_id<@current_dv_batch_id)
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_olo_order_detail_insert') is not null drop table #p_olo_order_detail_insert
create table dbo.#p_olo_order_detail_insert with(distribution=hash(bk_hash), location=user_db) as
select p_olo_order_detail.p_olo_order_detail_id,
       p_olo_order_detail.bk_hash
  from dbo.p_olo_order_detail
 where p_olo_order_detail.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (--p_olo_order_detail.dv_batch_id > @max_dv_batch_id
         p_olo_order_detail.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_olo_order_detail.bk_hash,
       p_olo_order_detail.bk_hash fact_olo_order_detail_key,
       l_olo_order_detail.order_id order_id,
       s_olo_order_detail.event_type event_type,
       s_olo_order_detail.adjustment_amount adjustment_amount,
       case when (CHARINDEX('MASTERCARD',s_olo_order_detail.payment_description)) > 0 OR (CHARINDEX('VISA',s_olo_order_detail.payment_description)) > 0 OR (CHARINDEX('MASTER CARD',s_olo_order_detail.payment_description)) > 0 then 'VMC'
         when (CHARINDEX('AMEX',s_olo_order_detail.payment_description)) > 0 OR (CHARINDEX('AMERICAN EXPRESS',s_olo_order_detail.payment_description)) > 0  OR (CHARINDEX('AMERICANEXPRESS',s_olo_order_detail.payment_description)) > 0 then 'AMEX'
         when (CHARINDEX('DISCOVER',s_olo_order_detail.payment_description)) > 0 OR (CHARINDEX('DISC',s_olo_order_detail.payment_description)) > 0 then 'DISC'
        end  payment_description,
       s_olo_order_detail.payment_type payment_type,
       s_olo_order_detail.amount sale_amount,
       s_olo_order_detail.total sale_total,
	    case when CHARINDEX('-',store_number)>1 then substring(store_number,1,CHARINDEX('-',store_number)-1) 
		else store_number end as store_number,
	   cast(substring(s_olo_order_detail.time_closed,1,8) as date) time_closed,
       cast(s_olo_order_detail.time_adjusted as date) time_adjusted,
       cast(s_olo_order_detail.time_cancelled as date) time_cancelled,
       cast(s_olo_order_detail.time_placed as date) time_placed,
       case when (s_olo_order_detail.event_type='OrderClosed') then s_olo_order_detail.amount
         when (s_olo_order_detail.event_type='OrderCancelled') then -s_olo_order_detail.amount
         when (s_olo_order_detail.event_type='OrderAdjusted') then s_olo_order_detail.adjustment_amount
        end 
         transaction_amount,
       case when (s_olo_order_detail.event_type='OrderClosed') then cast(substring(s_olo_order_detail.time_ready,1,8) as date)
         when (s_olo_order_detail.event_type='OrderCancelled') then cast(s_olo_order_detail.time_cancelled as date)
         when (s_olo_order_detail.event_type='OrderAdjusted') then cast(s_olo_order_detail.time_adjusted as date)
        end transaction_date,
       h_olo_order_detail.dv_deleted,
       p_olo_order_detail.p_olo_order_detail_id,
       p_olo_order_detail.dv_batch_id,
       p_olo_order_detail.dv_load_date_time,
       p_olo_order_detail.dv_load_end_date_time
  from dbo.h_olo_order_detail
  join dbo.p_olo_order_detail
    on h_olo_order_detail.bk_hash = p_olo_order_detail.bk_hash
	and p_olo_order_detail.dv_batch_id in( @current_dv_batch_id,@previous_dv_batch_id)
  ---join #p_olo_order_detail_insert
    ---on p_olo_order_detail.bk_hash = #p_olo_order_detail_insert.bk_hash
   ----and p_olo_order_detail.p_olo_order_detail_id = #p_olo_order_detail_insert.p_olo_order_detail_id
  join dbo.l_olo_order_detail
    on p_olo_order_detail.bk_hash = l_olo_order_detail.bk_hash
   and p_olo_order_detail.l_olo_order_detail_id = l_olo_order_detail.l_olo_order_detail_id
  join dbo.s_olo_order_detail
    on p_olo_order_detail.bk_hash = s_olo_order_detail.bk_hash
   and p_olo_order_detail.s_olo_order_detail_id = s_olo_order_detail.s_olo_order_detail_id
   and ((cast(substring(s_olo_order_detail.time_ready,1,8) as date)>(select min(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@previous_dv_batch_id)
   and cast(substring(s_olo_order_detail.time_ready,1,8) as date)<(select max(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@current_dv_batch_id) and s_olo_order_detail.event_type='OrderClosed') 
   or (cast(substring(s_olo_order_detail.time_cancelled,1,8) as date)>(select min(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@previous_dv_batch_id) 
   and cast(substring(s_olo_order_detail.time_cancelled,1,8) as date)<(select max(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@current_dv_batch_id) and s_olo_order_detail.event_type='OrderCancelled')
   or (cast(substring(s_olo_order_detail.time_adjusted,1,8) as date)>(select min(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@previous_dv_batch_id) 
   and cast(substring(s_olo_order_detail.time_adjusted,1,8) as date)<(select max(cast(substring(s_olo_order_detail.time_ready,1,8) as date)) from s_olo_order_detail where dv_batch_id =@current_dv_batch_id) and s_olo_order_detail.event_type='OrderAdjusted')) 

   truncate table dbo.d_olo_order_detail
-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran

  insert dbo.d_olo_order_detail(
             bk_hash,
             fact_olo_order_detail_key,
             order_id,
             event_type,
             adjustment_amount,
             payment_description,
             payment_type,
             sale_amount,
             sale_total,
             store_number,
             time_adjusted,
             time_cancelled,
             time_placed,
             transaction_amount,
             transaction_date,
             p_olo_order_detail_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_olo_order_detail_key,
         order_id,
         event_type,
         adjustment_amount,
         payment_description,
         payment_type,
         sale_amount,
         sale_total,
         store_number,
         time_adjusted,
         time_cancelled,
         time_placed,
         transaction_amount,
         transaction_date,
         p_olo_order_detail_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_olo_order_detail)
--Done!
end
