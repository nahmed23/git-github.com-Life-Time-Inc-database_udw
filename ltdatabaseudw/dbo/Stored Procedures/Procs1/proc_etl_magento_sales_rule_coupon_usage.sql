CREATE PROC [dbo].[proc_etl_magento_sales_rule_coupon_usage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_salesrule_coupon_usage

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_salesrule_coupon_usage (
       bk_hash,
       coupon_id,
       customer_id,
       times_used,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(coupon_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(customer_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       coupon_id,
       customer_id,
       times_used,
       dummy_modified_date_time,
       isnull(cast(stage_magento_salesrule_coupon_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_salesrule_coupon_usage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_rule_coupon_usage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_rule_coupon_usage (
       bk_hash,
       coupon_id,
       customer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_salesrule_coupon_usage.bk_hash,
       stage_hash_magento_salesrule_coupon_usage.coupon_id coupon_id,
       stage_hash_magento_salesrule_coupon_usage.customer_id customer_id,
       isnull(cast(stage_hash_magento_salesrule_coupon_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_salesrule_coupon_usage
  left join h_magento_sales_rule_coupon_usage
    on stage_hash_magento_salesrule_coupon_usage.bk_hash = h_magento_sales_rule_coupon_usage.bk_hash
 where h_magento_sales_rule_coupon_usage_id is null
   and stage_hash_magento_salesrule_coupon_usage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_magento_sales_rule_coupon_usage
if object_id('tempdb..#s_magento_sales_rule_coupon_usage_inserts') is not null drop table #s_magento_sales_rule_coupon_usage_inserts
create table #s_magento_sales_rule_coupon_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule_coupon_usage.bk_hash,
       stage_hash_magento_salesrule_coupon_usage.coupon_id coupon_id,
       stage_hash_magento_salesrule_coupon_usage.customer_id customer_id,
       stage_hash_magento_salesrule_coupon_usage.times_used times_used,
       stage_hash_magento_salesrule_coupon_usage.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_salesrule_coupon_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_usage.coupon_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_usage.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon_usage.times_used as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule_coupon_usage.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule_coupon_usage
 where stage_hash_magento_salesrule_coupon_usage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_rule_coupon_usage records
set @insert_date_time = getdate()
insert into s_magento_sales_rule_coupon_usage (
       bk_hash,
       coupon_id,
       customer_id,
       times_used,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_rule_coupon_usage_inserts.bk_hash,
       #s_magento_sales_rule_coupon_usage_inserts.coupon_id,
       #s_magento_sales_rule_coupon_usage_inserts.customer_id,
       #s_magento_sales_rule_coupon_usage_inserts.times_used,
       #s_magento_sales_rule_coupon_usage_inserts.dummy_modified_date_time,
       case when s_magento_sales_rule_coupon_usage.s_magento_sales_rule_coupon_usage_id is null then isnull(#s_magento_sales_rule_coupon_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_rule_coupon_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_rule_coupon_usage_inserts
  left join p_magento_sales_rule_coupon_usage
    on #s_magento_sales_rule_coupon_usage_inserts.bk_hash = p_magento_sales_rule_coupon_usage.bk_hash
   and p_magento_sales_rule_coupon_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_rule_coupon_usage
    on p_magento_sales_rule_coupon_usage.bk_hash = s_magento_sales_rule_coupon_usage.bk_hash
   and p_magento_sales_rule_coupon_usage.s_magento_sales_rule_coupon_usage_id = s_magento_sales_rule_coupon_usage.s_magento_sales_rule_coupon_usage_id
 where s_magento_sales_rule_coupon_usage.s_magento_sales_rule_coupon_usage_id is null
    or (s_magento_sales_rule_coupon_usage.s_magento_sales_rule_coupon_usage_id is not null
        and s_magento_sales_rule_coupon_usage.dv_hash <> #s_magento_sales_rule_coupon_usage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_rule_coupon_usage @current_dv_batch_id

end
