CREATE PROC [dbo].[proc_etl_magento_catalog_rule] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_catalogrule

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_catalogrule (
       bk_hash,
       row_id,
       rule_id,
       created_in,
       updated_in,
       name,
       [description],
       from_date,
       to_date,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       sort_order,
       simple_action,
       discount_amount,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(row_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       row_id,
       rule_id,
       created_in,
       updated_in,
       name,
       [description],
       from_date,
       to_date,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       sort_order,
       simple_action,
       discount_amount,
       dummy_modified_date_time,
       isnull(cast(stage_magento_catalogrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_catalogrule
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_rule @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_rule (
       bk_hash,
       row_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_catalogrule.bk_hash,
       stage_hash_magento_catalogrule.row_id row_id,
       isnull(cast(stage_hash_magento_catalogrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_catalogrule
  left join h_magento_catalog_rule
    on stage_hash_magento_catalogrule.bk_hash = h_magento_catalog_rule.bk_hash
 where h_magento_catalog_rule_id is null
   and stage_hash_magento_catalogrule.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_catalog_rule
if object_id('tempdb..#l_magento_catalog_rule_inserts') is not null drop table #l_magento_catalog_rule_inserts
create table #l_magento_catalog_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalogrule.bk_hash,
       stage_hash_magento_catalogrule.row_id row_id,
       stage_hash_magento_catalogrule.rule_id rule_id,
       isnull(cast(stage_hash_magento_catalogrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.rule_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalogrule
 where stage_hash_magento_catalogrule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_catalog_rule records
set @insert_date_time = getdate()
insert into l_magento_catalog_rule (
       bk_hash,
       row_id,
       rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_catalog_rule_inserts.bk_hash,
       #l_magento_catalog_rule_inserts.row_id,
       #l_magento_catalog_rule_inserts.rule_id,
       case when l_magento_catalog_rule.l_magento_catalog_rule_id is null then isnull(#l_magento_catalog_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_catalog_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_catalog_rule_inserts
  left join p_magento_catalog_rule
    on #l_magento_catalog_rule_inserts.bk_hash = p_magento_catalog_rule.bk_hash
   and p_magento_catalog_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_catalog_rule
    on p_magento_catalog_rule.bk_hash = l_magento_catalog_rule.bk_hash
   and p_magento_catalog_rule.l_magento_catalog_rule_id = l_magento_catalog_rule.l_magento_catalog_rule_id
 where l_magento_catalog_rule.l_magento_catalog_rule_id is null
    or (l_magento_catalog_rule.l_magento_catalog_rule_id is not null
        and l_magento_catalog_rule.dv_hash <> #l_magento_catalog_rule_inserts.source_hash)

--calculate hash and lookup to current s_magento_catalog_rule
if object_id('tempdb..#s_magento_catalog_rule_inserts') is not null drop table #s_magento_catalog_rule_inserts
create table #s_magento_catalog_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalogrule.bk_hash,
       stage_hash_magento_catalogrule.row_id row_id,
       stage_hash_magento_catalogrule.created_in created_in,
       stage_hash_magento_catalogrule.updated_in updated_in,
       stage_hash_magento_catalogrule.name name,
       stage_hash_magento_catalogrule.description [description],
       stage_hash_magento_catalogrule.from_date from_date,
       stage_hash_magento_catalogrule.to_date to_date,
       stage_hash_magento_catalogrule.is_active is_active,
       stage_hash_magento_catalogrule.conditions_serialized conditions_serialized,
       stage_hash_magento_catalogrule.actions_serialized actions_serialized,
       stage_hash_magento_catalogrule.stop_rules_processing stop_rules_processing,
       stage_hash_magento_catalogrule.sort_order sort_order,
       stage_hash_magento_catalogrule.simple_action simple_action,
       stage_hash_magento_catalogrule.discount_amount discount_amount,
       stage_hash_magento_catalogrule.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_catalogrule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.created_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.updated_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalogrule.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalogrule.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.from_date as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.to_date as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalogrule.conditions_serialized,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalogrule.actions_serialized,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.stop_rules_processing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.sort_order as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalogrule.simple_action,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalogrule.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalogrule.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalogrule
 where stage_hash_magento_catalogrule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_rule records
set @insert_date_time = getdate()
insert into s_magento_catalog_rule (
       bk_hash,
       row_id,
       created_in,
       updated_in,
       name,
       [description],
       from_date,
       to_date,
       is_active,
       conditions_serialized,
       actions_serialized,
       stop_rules_processing,
       sort_order,
       simple_action,
       discount_amount,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_rule_inserts.bk_hash,
       #s_magento_catalog_rule_inserts.row_id,
       #s_magento_catalog_rule_inserts.created_in,
       #s_magento_catalog_rule_inserts.updated_in,
       #s_magento_catalog_rule_inserts.name,
       #s_magento_catalog_rule_inserts.[description],
       #s_magento_catalog_rule_inserts.from_date,
       #s_magento_catalog_rule_inserts.to_date,
       #s_magento_catalog_rule_inserts.is_active,
       #s_magento_catalog_rule_inserts.conditions_serialized,
       #s_magento_catalog_rule_inserts.actions_serialized,
       #s_magento_catalog_rule_inserts.stop_rules_processing,
       #s_magento_catalog_rule_inserts.sort_order,
       #s_magento_catalog_rule_inserts.simple_action,
       #s_magento_catalog_rule_inserts.discount_amount,
       #s_magento_catalog_rule_inserts.dummy_modified_date_time,
       case when s_magento_catalog_rule.s_magento_catalog_rule_id is null then isnull(#s_magento_catalog_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_rule_inserts
  left join p_magento_catalog_rule
    on #s_magento_catalog_rule_inserts.bk_hash = p_magento_catalog_rule.bk_hash
   and p_magento_catalog_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_rule
    on p_magento_catalog_rule.bk_hash = s_magento_catalog_rule.bk_hash
   and p_magento_catalog_rule.s_magento_catalog_rule_id = s_magento_catalog_rule.s_magento_catalog_rule_id
 where s_magento_catalog_rule.s_magento_catalog_rule_id is null
    or (s_magento_catalog_rule.s_magento_catalog_rule_id is not null
        and s_magento_catalog_rule.dv_hash <> #s_magento_catalog_rule_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_rule @current_dv_batch_id

end
