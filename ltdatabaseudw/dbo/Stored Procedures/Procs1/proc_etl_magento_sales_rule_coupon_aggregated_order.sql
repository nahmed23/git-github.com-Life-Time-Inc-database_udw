CREATE PROC [dbo].[proc_etl_magento_sales_rule_coupon_aggregated_order] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_salesrule_coupon_aggregated_order

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_salesrule_coupon_aggregated_order (
       bk_hash,
       [id],
       period,
       store_id,
       order_status,
       coupon_code,
       coupon_uses,
       subtotal_amount,
       discount_amount,
       total_amount,
       rule_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       period,
       store_id,
       order_status,
       coupon_code,
       coupon_uses,
       subtotal_amount,
       discount_amount,
       total_amount,
       rule_name,
       dummy_modified_date_time,
       isnull(cast(stage_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_salesrule_coupon_aggregated_order
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_rule_coupon_aggregated_order @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_rule_coupon_aggregated_order (
       bk_hash,
       [id],
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_salesrule_coupon_aggregated_order.bk_hash,
       stage_hash_magento_salesrule_coupon_aggregated_order.[id] [id],
       isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_salesrule_coupon_aggregated_order
  left join h_magento_sales_rule_coupon_aggregated_order
    on stage_hash_magento_salesrule_coupon_aggregated_order.bk_hash = h_magento_sales_rule_coupon_aggregated_order.bk_hash
 where h_magento_sales_rule_coupon_aggregated_order_id is null
   and stage_hash_magento_salesrule_coupon_aggregated_order.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_rule_coupon_aggregated_order
if object_id('tempdb..#l_magento_sales_rule_coupon_aggregated_order_inserts') is not null drop table #l_magento_sales_rule_coupon_aggregated_order_inserts
create table #l_magento_sales_rule_coupon_aggregated_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule_coupon_aggregated_order.bk_hash,
       stage_hash_magento_salesrule_coupon_aggregated_order.[id] [id],
       stage_hash_magento_salesrule_coupon_aggregated_order.store_id store_id,
       isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule_coupon_aggregated_order
 where stage_hash_magento_salesrule_coupon_aggregated_order.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_rule_coupon_aggregated_order records
set @insert_date_time = getdate()
insert into l_magento_sales_rule_coupon_aggregated_order (
       bk_hash,
       [id],
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_rule_coupon_aggregated_order_inserts.bk_hash,
       #l_magento_sales_rule_coupon_aggregated_order_inserts.[id],
       #l_magento_sales_rule_coupon_aggregated_order_inserts.store_id,
       case when l_magento_sales_rule_coupon_aggregated_order.l_magento_sales_rule_coupon_aggregated_order_id is null then isnull(#l_magento_sales_rule_coupon_aggregated_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_rule_coupon_aggregated_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_rule_coupon_aggregated_order_inserts
  left join p_magento_sales_rule_coupon_aggregated_order
    on #l_magento_sales_rule_coupon_aggregated_order_inserts.bk_hash = p_magento_sales_rule_coupon_aggregated_order.bk_hash
   and p_magento_sales_rule_coupon_aggregated_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_rule_coupon_aggregated_order
    on p_magento_sales_rule_coupon_aggregated_order.bk_hash = l_magento_sales_rule_coupon_aggregated_order.bk_hash
   and p_magento_sales_rule_coupon_aggregated_order.l_magento_sales_rule_coupon_aggregated_order_id = l_magento_sales_rule_coupon_aggregated_order.l_magento_sales_rule_coupon_aggregated_order_id
 where l_magento_sales_rule_coupon_aggregated_order.l_magento_sales_rule_coupon_aggregated_order_id is null
    or (l_magento_sales_rule_coupon_aggregated_order.l_magento_sales_rule_coupon_aggregated_order_id is not null
        and l_magento_sales_rule_coupon_aggregated_order.dv_hash <> #l_magento_sales_rule_coupon_aggregated_order_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_rule_coupon_aggregated_order
if object_id('tempdb..#s_magento_sales_rule_coupon_aggregated_order_inserts') is not null drop table #s_magento_sales_rule_coupon_aggregated_order_inserts
create table #s_magento_sales_rule_coupon_aggregated_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule_coupon_aggregated_order.bk_hash,
       stage_hash_magento_salesrule_coupon_aggregated_order.[id] [id],
       stage_hash_magento_salesrule_coupon_aggregated_order.period period,
       stage_hash_magento_salesrule_coupon_aggregated_order.order_status order_status,
       stage_hash_magento_salesrule_coupon_aggregated_order.coupon_code coupon_code,
       stage_hash_magento_salesrule_coupon_aggregated_order.coupon_uses coupon_uses,
       stage_hash_magento_salesrule_coupon_aggregated_order.subtotal_amount subtotal_amount,
       stage_hash_magento_salesrule_coupon_aggregated_order.discount_amount discount_amount,
       stage_hash_magento_salesrule_coupon_aggregated_order.total_amount total_amount,
       stage_hash_magento_salesrule_coupon_aggregated_order.rule_name rule_name,
       stage_hash_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.period as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule_coupon_aggregated_order.order_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule_coupon_aggregated_order.coupon_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.coupon_uses as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.subtotal_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_aggregated_order.total_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule_coupon_aggregated_order.rule_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule_coupon_aggregated_order.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule_coupon_aggregated_order
 where stage_hash_magento_salesrule_coupon_aggregated_order.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_rule_coupon_aggregated_order records
set @insert_date_time = getdate()
insert into s_magento_sales_rule_coupon_aggregated_order (
       bk_hash,
       [id],
       period,
       order_status,
       coupon_code,
       coupon_uses,
       subtotal_amount,
       discount_amount,
       total_amount,
       rule_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_rule_coupon_aggregated_order_inserts.bk_hash,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.[id],
       #s_magento_sales_rule_coupon_aggregated_order_inserts.period,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.order_status,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.coupon_code,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.coupon_uses,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.subtotal_amount,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.discount_amount,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.total_amount,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.rule_name,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.dummy_modified_date_time,
       case when s_magento_sales_rule_coupon_aggregated_order.s_magento_sales_rule_coupon_aggregated_order_id is null then isnull(#s_magento_sales_rule_coupon_aggregated_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_rule_coupon_aggregated_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_rule_coupon_aggregated_order_inserts
  left join p_magento_sales_rule_coupon_aggregated_order
    on #s_magento_sales_rule_coupon_aggregated_order_inserts.bk_hash = p_magento_sales_rule_coupon_aggregated_order.bk_hash
   and p_magento_sales_rule_coupon_aggregated_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_rule_coupon_aggregated_order
    on p_magento_sales_rule_coupon_aggregated_order.bk_hash = s_magento_sales_rule_coupon_aggregated_order.bk_hash
   and p_magento_sales_rule_coupon_aggregated_order.s_magento_sales_rule_coupon_aggregated_order_id = s_magento_sales_rule_coupon_aggregated_order.s_magento_sales_rule_coupon_aggregated_order_id
 where s_magento_sales_rule_coupon_aggregated_order.s_magento_sales_rule_coupon_aggregated_order_id is null
    or (s_magento_sales_rule_coupon_aggregated_order.s_magento_sales_rule_coupon_aggregated_order_id is not null
        and s_magento_sales_rule_coupon_aggregated_order.dv_hash <> #s_magento_sales_rule_coupon_aggregated_order_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_rule_coupon_aggregated_order @current_dv_batch_id

end
