CREATE PROC [dbo].[proc_etl_magento_sales_rule] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_salesrule

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_salesrule (
       bk_hash,
       row_id,
       rule_id,
       created_in,
       updated_in,
       name,
       description,
       from_date,
       to_date,
       uses_per_customer,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       is_advanced,
       product_ids,
       sort_order,
       simple_action,
       discount_amount,
       discount_qty,
       discount_step,
       apply_to_shipping,
       times_used,
       is_rss,
       coupon_type,
       use_auto_generation,
       uses_per_coupon,
       simple_free_shipping,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(row_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       row_id,
       rule_id,
       created_in,
       updated_in,
       name,
       description,
       from_date,
       to_date,
       uses_per_customer,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       is_advanced,
       product_ids,
       sort_order,
       simple_action,
       discount_amount,
       discount_qty,
       discount_step,
       apply_to_shipping,
       times_used,
       is_rss,
       coupon_type,
       use_auto_generation,
       uses_per_coupon,
       simple_free_shipping,
       dummy_modified_date_time,
       isnull(cast(stage_magento_salesrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_salesrule
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_rule @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_rule (
       bk_hash,
       row_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_salesrule.bk_hash,
       stage_hash_magento_salesrule.row_id row_id,
       isnull(cast(stage_hash_magento_salesrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_salesrule
  left join h_magento_sales_rule
    on stage_hash_magento_salesrule.bk_hash = h_magento_sales_rule.bk_hash
 where h_magento_sales_rule_id is null
   and stage_hash_magento_salesrule.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_rule
if object_id('tempdb..#l_magento_sales_rule_inserts') is not null drop table #l_magento_sales_rule_inserts
create table #l_magento_sales_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule.bk_hash,
       stage_hash_magento_salesrule.row_id row_id,
       stage_hash_magento_salesrule.rule_id rule_id,
       isnull(cast(stage_hash_magento_salesrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.rule_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule
 where stage_hash_magento_salesrule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_rule records
set @insert_date_time = getdate()
insert into l_magento_sales_rule (
       bk_hash,
       row_id,
       rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_rule_inserts.bk_hash,
       #l_magento_sales_rule_inserts.row_id,
       #l_magento_sales_rule_inserts.rule_id,
       case when l_magento_sales_rule.l_magento_sales_rule_id is null then isnull(#l_magento_sales_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_rule_inserts
  left join p_magento_sales_rule
    on #l_magento_sales_rule_inserts.bk_hash = p_magento_sales_rule.bk_hash
   and p_magento_sales_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_rule
    on p_magento_sales_rule.bk_hash = l_magento_sales_rule.bk_hash
   and p_magento_sales_rule.l_magento_sales_rule_id = l_magento_sales_rule.l_magento_sales_rule_id
 where l_magento_sales_rule.l_magento_sales_rule_id is null
    or (l_magento_sales_rule.l_magento_sales_rule_id is not null
        and l_magento_sales_rule.dv_hash <> #l_magento_sales_rule_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_rule
if object_id('tempdb..#s_magento_sales_rule_inserts') is not null drop table #s_magento_sales_rule_inserts
create table #s_magento_sales_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_salesrule.bk_hash,
       stage_hash_magento_salesrule.row_id row_id,
       stage_hash_magento_salesrule.created_in created_in,
       stage_hash_magento_salesrule.updated_in updated_in,
       stage_hash_magento_salesrule.name name,
       stage_hash_magento_salesrule.description description,
       stage_hash_magento_salesrule.from_date from_date,
       stage_hash_magento_salesrule.to_date to_date,
       stage_hash_magento_salesrule.uses_per_customer uses_per_customer,
       stage_hash_magento_salesrule.is_active is_active,
       stage_hash_magento_salesrule.conditions_serialized conditions_serialized,
       stage_hash_magento_salesrule.actions_serialized actions_serialized,
       stage_hash_magento_salesrule.stop_rules_processing stop_rules_processing,
       stage_hash_magento_salesrule.is_advanced is_advanced,
       stage_hash_magento_salesrule.product_ids product_ids,
       stage_hash_magento_salesrule.sort_order sort_order,
       stage_hash_magento_salesrule.simple_action simple_action,
       stage_hash_magento_salesrule.discount_amount discount_amount,
       stage_hash_magento_salesrule.discount_qty discount_qty,
       stage_hash_magento_salesrule.discount_step discount_step,
       stage_hash_magento_salesrule.apply_to_shipping apply_to_shipping,
       stage_hash_magento_salesrule.times_used times_used,
       stage_hash_magento_salesrule.is_rss is_rss,
       stage_hash_magento_salesrule.coupon_type coupon_type,
       stage_hash_magento_salesrule.use_auto_generation use_auto_generation,
       stage_hash_magento_salesrule.uses_per_coupon uses_per_coupon,
       stage_hash_magento_salesrule.simple_free_shipping simple_free_shipping,
       stage_hash_magento_salesrule.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_salesrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.created_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.updated_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.from_date as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.to_date as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.uses_per_customer as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.conditions_serialized,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.actions_serialized,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.stop_rules_processing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.is_advanced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.product_ids,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.sort_order as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_salesrule.simple_action,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.discount_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.discount_step as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.apply_to_shipping as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.times_used as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.is_rss as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.coupon_type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.use_auto_generation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.uses_per_coupon as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_salesrule.simple_free_shipping as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_salesrule.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_salesrule
 where stage_hash_magento_salesrule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_rule records
set @insert_date_time = getdate()
insert into s_magento_sales_rule (
       bk_hash,
       row_id,
       created_in,
       updated_in,
       name,
       description,
       from_date,
       to_date,
       uses_per_customer,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       is_advanced,
       product_ids,
       sort_order,
       simple_action,
       discount_amount,
       discount_qty,
       discount_step,
       apply_to_shipping,
       times_used,
       is_rss,
       coupon_type,
       use_auto_generation,
       uses_per_coupon,
       simple_free_shipping,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_rule_inserts.bk_hash,
       #s_magento_sales_rule_inserts.row_id,
       #s_magento_sales_rule_inserts.created_in,
       #s_magento_sales_rule_inserts.updated_in,
       #s_magento_sales_rule_inserts.name,
       #s_magento_sales_rule_inserts.description,
       #s_magento_sales_rule_inserts.from_date,
       #s_magento_sales_rule_inserts.to_date,
       #s_magento_sales_rule_inserts.uses_per_customer,
       #s_magento_sales_rule_inserts.is_active,
       #s_magento_sales_rule_inserts.conditions_serialized,
       #s_magento_sales_rule_inserts.actions_serialized,
       #s_magento_sales_rule_inserts.stop_rules_processing,
       #s_magento_sales_rule_inserts.is_advanced,
       #s_magento_sales_rule_inserts.product_ids,
       #s_magento_sales_rule_inserts.sort_order,
       #s_magento_sales_rule_inserts.simple_action,
       #s_magento_sales_rule_inserts.discount_amount,
       #s_magento_sales_rule_inserts.discount_qty,
       #s_magento_sales_rule_inserts.discount_step,
       #s_magento_sales_rule_inserts.apply_to_shipping,
       #s_magento_sales_rule_inserts.times_used,
       #s_magento_sales_rule_inserts.is_rss,
       #s_magento_sales_rule_inserts.coupon_type,
       #s_magento_sales_rule_inserts.use_auto_generation,
       #s_magento_sales_rule_inserts.uses_per_coupon,
       #s_magento_sales_rule_inserts.simple_free_shipping,
       #s_magento_sales_rule_inserts.dummy_modified_date_time,
       case when s_magento_sales_rule.s_magento_sales_rule_id is null then isnull(#s_magento_sales_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_rule_inserts
  left join p_magento_sales_rule
    on #s_magento_sales_rule_inserts.bk_hash = p_magento_sales_rule.bk_hash
   and p_magento_sales_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_rule
    on p_magento_sales_rule.bk_hash = s_magento_sales_rule.bk_hash
   and p_magento_sales_rule.s_magento_sales_rule_id = s_magento_sales_rule.s_magento_sales_rule_id
 where s_magento_sales_rule.s_magento_sales_rule_id is null
    or (s_magento_sales_rule.s_magento_sales_rule_id is not null
        and s_magento_sales_rule.dv_hash <> #s_magento_sales_rule_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_rule @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_rule @current_dv_batch_id

end
