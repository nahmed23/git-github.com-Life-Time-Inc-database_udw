CREATE PROC [dbo].[proc_etl_magento_sales_rule_coupon] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_salesrule_coupon

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_salesrule_coupon (
       bk_hash,
       coupon_id,
       rule_id,
       code,
       usage_limit,
       usage_per_customer,
       times_used,
       expiration_date,
       is_primary,
       created_at,
       type,
       generated_by_dotmailer,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(coupon_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       coupon_id,
       rule_id,
       code,
       usage_limit,
       usage_per_customer,
       times_used,
       expiration_date,
       is_primary,
       created_at,
       type,
       generated_by_dotmailer,
       dummy_modified_date_time,
       isnull(cast(stage_magento_salesrule_coupon.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_salesrule_coupon
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_rule_coupon @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_rule_coupon (
       bk_hash,
       coupon_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_salesrule_coupon.bk_hash,
       stage_hash_magento_salesrule_coupon.coupon_id coupon_id,
       isnull(cast(stage_hash_magento_salesrule_coupon.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_salesrule_coupon
  left join h_magento_sales_rule_coupon
    on stage_hash_magento_salesrule_coupon.bk_hash = h_magento_sales_rule_coupon.bk_hash
 where h_magento_sales_rule_coupon_id is null
   and stage_hash_magento_salesrule_coupon.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_rule_coupon
if object_id('tempdb..#l_magento_sales_rule_coupon_inserts') is not null drop table #l_magento_sales_rule_coupon_inserts
create table #l_magento_sales_rule_coupon_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule_coupon.bk_hash,
       stage_hash_magento_salesrule_coupon.coupon_id coupon_id,
       stage_hash_magento_salesrule_coupon.rule_id rule_id,
       isnull(cast(stage_hash_magento_salesrule_coupon.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.coupon_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.rule_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule_coupon
 where stage_hash_magento_salesrule_coupon.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_rule_coupon records
set @insert_date_time = getdate()
insert into l_magento_sales_rule_coupon (
       bk_hash,
       coupon_id,
       rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_rule_coupon_inserts.bk_hash,
       #l_magento_sales_rule_coupon_inserts.coupon_id,
       #l_magento_sales_rule_coupon_inserts.rule_id,
       case when l_magento_sales_rule_coupon.l_magento_sales_rule_coupon_id is null then isnull(#l_magento_sales_rule_coupon_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_rule_coupon_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_rule_coupon_inserts
  left join p_magento_sales_rule_coupon
    on #l_magento_sales_rule_coupon_inserts.bk_hash = p_magento_sales_rule_coupon.bk_hash
   and p_magento_sales_rule_coupon.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_rule_coupon
    on p_magento_sales_rule_coupon.bk_hash = l_magento_sales_rule_coupon.bk_hash
   and p_magento_sales_rule_coupon.l_magento_sales_rule_coupon_id = l_magento_sales_rule_coupon.l_magento_sales_rule_coupon_id
 where l_magento_sales_rule_coupon.l_magento_sales_rule_coupon_id is null
    or (l_magento_sales_rule_coupon.l_magento_sales_rule_coupon_id is not null
        and l_magento_sales_rule_coupon.dv_hash <> #l_magento_sales_rule_coupon_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_rule_coupon
if object_id('tempdb..#s_magento_sales_rule_coupon_inserts') is not null drop table #s_magento_sales_rule_coupon_inserts
create table #s_magento_sales_rule_coupon_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule_coupon.bk_hash,
       stage_hash_magento_salesrule_coupon.coupon_id coupon_id,
       stage_hash_magento_salesrule_coupon.code code,
       stage_hash_magento_salesrule_coupon.usage_limit usage_limit,
       stage_hash_magento_salesrule_coupon.usage_per_customer usage_per_customer,
       stage_hash_magento_salesrule_coupon.times_used times_used,
       stage_hash_magento_salesrule_coupon.expiration_date expiration_date,
       stage_hash_magento_salesrule_coupon.is_primary is_primary,
       stage_hash_magento_salesrule_coupon.created_at created_at,
       stage_hash_magento_salesrule_coupon.type type,
       stage_hash_magento_salesrule_coupon.generated_by_dotmailer generated_by_dotmailer,
       stage_hash_magento_salesrule_coupon.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_salesrule_coupon.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.coupon_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule_coupon.code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.usage_limit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.usage_per_customer as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.times_used as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule_coupon.expiration_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.is_primary as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule_coupon.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule_coupon.generated_by_dotmailer as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule_coupon.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule_coupon
 where stage_hash_magento_salesrule_coupon.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_rule_coupon records
set @insert_date_time = getdate()
insert into s_magento_sales_rule_coupon (
       bk_hash,
       coupon_id,
       code,
       usage_limit,
       usage_per_customer,
       times_used,
       expiration_date,
       is_primary,
       created_at,
       type,
       generated_by_dotmailer,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_rule_coupon_inserts.bk_hash,
       #s_magento_sales_rule_coupon_inserts.coupon_id,
       #s_magento_sales_rule_coupon_inserts.code,
       #s_magento_sales_rule_coupon_inserts.usage_limit,
       #s_magento_sales_rule_coupon_inserts.usage_per_customer,
       #s_magento_sales_rule_coupon_inserts.times_used,
       #s_magento_sales_rule_coupon_inserts.expiration_date,
       #s_magento_sales_rule_coupon_inserts.is_primary,
       #s_magento_sales_rule_coupon_inserts.created_at,
       #s_magento_sales_rule_coupon_inserts.type,
       #s_magento_sales_rule_coupon_inserts.generated_by_dotmailer,
       #s_magento_sales_rule_coupon_inserts.dummy_modified_date_time,
       case when s_magento_sales_rule_coupon.s_magento_sales_rule_coupon_id is null then isnull(#s_magento_sales_rule_coupon_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_rule_coupon_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_rule_coupon_inserts
  left join p_magento_sales_rule_coupon
    on #s_magento_sales_rule_coupon_inserts.bk_hash = p_magento_sales_rule_coupon.bk_hash
   and p_magento_sales_rule_coupon.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_rule_coupon
    on p_magento_sales_rule_coupon.bk_hash = s_magento_sales_rule_coupon.bk_hash
   and p_magento_sales_rule_coupon.s_magento_sales_rule_coupon_id = s_magento_sales_rule_coupon.s_magento_sales_rule_coupon_id
 where s_magento_sales_rule_coupon.s_magento_sales_rule_coupon_id is null
    or (s_magento_sales_rule_coupon.s_magento_sales_rule_coupon_id is not null
        and s_magento_sales_rule_coupon.dv_hash <> #s_magento_sales_rule_coupon_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_rule_coupon @current_dv_batch_id

end
