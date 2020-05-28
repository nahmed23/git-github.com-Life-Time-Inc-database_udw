CREATE PROC [dbo].[proc_dim_plan_exchange_rate] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_plan_exchange_rate

if object_id('tempdb..#dim_plan_exchange_rate') is not null drop table #dim_plan_exchange_rate
create table dbo.#dim_plan_exchange_rate with(distribution = hash(dim_plan_exchange_rate_key), location=user_db, heap) as
select distinct
       --util_bk_hash[l_hybris_products.p_ltf_fulfillment_product_id,h_mms_product.product_id]  
       case when p_fdw_dim_period_exchange_rate.bk_hash in ('-997','-998','-999') then p_fdw_dim_period_exchange_rate.bk_hash
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p_fdw_dim_period_exchange_rate.from_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(p_fdw_dim_period_exchange_rate.to_currency_code,'z#@$k%&P'))),2) end dim_plan_exchange_rate_key,
       p_fdw_dim_period_exchange_rate.from_currency_code,
       p_fdw_dim_period_exchange_rate.to_currency_code,
       max(s_fdw_dim_period_exchange_rate.plan_rate) plan_rate,
       max(p_fdw_dim_period_exchange_rate.dv_load_date_time) dv_load_date_time,
       max(p_fdw_dim_period_exchange_rate.dv_load_end_date_time) dv_load_end_date_time,
       max(p_fdw_dim_period_exchange_rate.dv_batch_id) dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from p_fdw_dim_period_exchange_rate
  join s_fdw_dim_period_exchange_rate
    on p_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id = s_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id
  join #dv_batch_id
    on p_fdw_dim_period_exchange_rate.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_fdw_dim_period_exchange_rate.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_fdw_dim_period_exchange_rate.dv_load_end_date_time = 'dec 31, 9999'
 group by
       --util_bk_hash[l_hybris_products.p_ltf_fulfillment_product_id,h_mms_product.product_id]  
       case when p_fdw_dim_period_exchange_rate.bk_hash in ('-997','-998','-999') then p_fdw_dim_period_exchange_rate.bk_hash
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p_fdw_dim_period_exchange_rate.from_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(p_fdw_dim_period_exchange_rate.to_currency_code,'z#@$k%&P'))),2) end,
       p_fdw_dim_period_exchange_rate.from_currency_code,
       p_fdw_dim_period_exchange_rate.to_currency_code

-- Delete and re-insert
-- Do as a single transaction
--   Delete records from the dim table that exist
--   Insert records from current and missing batches

begin tran

  delete dbo.dim_plan_exchange_rate
   where dim_plan_exchange_rate_key in (select dim_plan_exchange_rate_key from #dim_plan_exchange_rate)

  insert dim_plan_exchange_rate(
           dim_plan_exchange_rate_key,
           from_currency_code,
           to_currency_code,
           plan_rate,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           dv_inserted_date_time,
           dv_insert_user)
  select dim_plan_exchange_rate_key,
         from_currency_code,
         to_currency_code,
         plan_rate,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user
    from #dim_plan_exchange_rate

commit tran

end
