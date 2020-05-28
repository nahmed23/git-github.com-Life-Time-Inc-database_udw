CREATE PROC [dbo].[proc_d_exerp_subscription_price] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_price)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_subscription_price_insert') is not null drop table #p_exerp_subscription_price_insert
create table dbo.#p_exerp_subscription_price_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_price.p_exerp_subscription_price_id,
       p_exerp_subscription_price.bk_hash
  from dbo.p_exerp_subscription_price
 where p_exerp_subscription_price.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_subscription_price.dv_batch_id > @max_dv_batch_id
        or p_exerp_subscription_price.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_price.bk_hash,
       p_exerp_subscription_price.bk_hash dim_exerp_subscription_price_key,
       p_exerp_subscription_price.subscription_price_id subscription_price_id,
       case when p_exerp_subscription_price.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_price.bk_hash
           when s_exerp_subscription_price.cancel_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription_price.cancel_datetime, 112)    end cancel_dim_date_key,
       case when s_exerp_subscription_price.cancelled = 1 then 'Y'
             else 'N'  end cancelled_flag,
       case when p_exerp_subscription_price.bk_hash in ('-997','-998','-999') then p_exerp_subscription_price.bk_hash     
         when l_exerp_subscription_price.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription_price.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_subscription_price.bk_hash in ('-997','-998','-999') then p_exerp_subscription_price.bk_hash     
         when l_exerp_subscription_price.subscription_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_price.subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_subscription_key,
       case when p_exerp_subscription_price.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_price.bk_hash
           when s_exerp_subscription_price.entry_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription_price.entry_datetime, 112)    end entry_dim_date_key,
       case when p_exerp_subscription_price.bk_hash in ('-997','-998','-999') then p_exerp_subscription_price.bk_hash
       when s_exerp_subscription_price.entry_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription_price.entry_datetime,114), 1, 5),':','') end entry_dim_time_key,
       s_exerp_subscription_price.ets ets,
       case when p_exerp_subscription_price.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_price.bk_hash
           when s_exerp_subscription_price.from_date is null then '-998'
        else convert(varchar, s_exerp_subscription_price.from_date, 112)    end from_dim_date_key,
       s_exerp_subscription_price.price price,
       s_exerp_subscription_price.type subscription_price_type,
       case when p_exerp_subscription_price.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_price.bk_hash
           when s_exerp_subscription_price.to_date is null then '-998'
        else convert(varchar, s_exerp_subscription_price.to_date, 112)    end to_dim_date_key,
       isnull(h_exerp_subscription_price.dv_deleted,0) dv_deleted,
       p_exerp_subscription_price.p_exerp_subscription_price_id,
       p_exerp_subscription_price.dv_batch_id,
       p_exerp_subscription_price.dv_load_date_time,
       p_exerp_subscription_price.dv_load_end_date_time
  from dbo.h_exerp_subscription_price
  join dbo.p_exerp_subscription_price
    on h_exerp_subscription_price.bk_hash = p_exerp_subscription_price.bk_hash
  join #p_exerp_subscription_price_insert
    on p_exerp_subscription_price.bk_hash = #p_exerp_subscription_price_insert.bk_hash
   and p_exerp_subscription_price.p_exerp_subscription_price_id = #p_exerp_subscription_price_insert.p_exerp_subscription_price_id
  join dbo.l_exerp_subscription_price
    on p_exerp_subscription_price.bk_hash = l_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.l_exerp_subscription_price_id = l_exerp_subscription_price.l_exerp_subscription_price_id
  join dbo.s_exerp_subscription_price
    on p_exerp_subscription_price.bk_hash = s_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.s_exerp_subscription_price_id = s_exerp_subscription_price.s_exerp_subscription_price_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_subscription_price
   where d_exerp_subscription_price.bk_hash in (select bk_hash from #p_exerp_subscription_price_insert)

  insert dbo.d_exerp_subscription_price(
             bk_hash,
             dim_exerp_subscription_price_key,
             subscription_price_id,
             cancel_dim_date_key,
             cancelled_flag,
             dim_club_key,
             dim_exerp_subscription_key,
             entry_dim_date_key,
             entry_dim_time_key,
             ets,
             from_dim_date_key,
             price,
             subscription_price_type,
             to_dim_date_key,
             deleted_flag,
             p_exerp_subscription_price_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_subscription_price_key,
         subscription_price_id,
         cancel_dim_date_key,
         cancelled_flag,
         dim_club_key,
         dim_exerp_subscription_key,
         entry_dim_date_key,
         entry_dim_time_key,
         ets,
         from_dim_date_key,
         price,
         subscription_price_type,
         to_dim_date_key,
         dv_deleted,
         p_exerp_subscription_price_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_price)
--Done!
end
