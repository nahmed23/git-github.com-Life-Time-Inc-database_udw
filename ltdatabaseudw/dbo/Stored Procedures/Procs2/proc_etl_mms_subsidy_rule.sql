CREATE PROC [dbo].[proc_etl_mms_subsidy_rule] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_SubsidyRule

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_SubsidyRule (
       bk_hash,
       SubsidyRuleID,
       SubsidyCompanyReimbursementProgramID,
       ValReimbursementUsageTypeID,
       Description,
       UsageMinimum,
       MaxVisitsPerDay,
       ReimbursementAmountPerUsage,
       IgnoreUsageMinimumFirstMonthFlag,
       IncludeTaxUsageTierFlag,
       InsertedDateTime,
       UpdatedDateTime,
       IgnoreUsageMinimumPreviousNonAccessFlag,
       ApplyUsageCreditsPreviousAccessFlag,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SubsidyRuleID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       SubsidyRuleID,
       SubsidyCompanyReimbursementProgramID,
       ValReimbursementUsageTypeID,
       Description,
       UsageMinimum,
       MaxVisitsPerDay,
       ReimbursementAmountPerUsage,
       IgnoreUsageMinimumFirstMonthFlag,
       IncludeTaxUsageTierFlag,
       InsertedDateTime,
       UpdatedDateTime,
       IgnoreUsageMinimumPreviousNonAccessFlag,
       ApplyUsageCreditsPreviousAccessFlag,
       isnull(cast(stage_mms_SubsidyRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_SubsidyRule
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_subsidy_rule @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_subsidy_rule (
       bk_hash,
       subsidy_rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_SubsidyRule.bk_hash,
       stage_hash_mms_SubsidyRule.SubsidyRuleID subsidy_rule_id,
       isnull(cast(stage_hash_mms_SubsidyRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_SubsidyRule
  left join h_mms_subsidy_rule
    on stage_hash_mms_SubsidyRule.bk_hash = h_mms_subsidy_rule.bk_hash
 where h_mms_subsidy_rule_id is null
   and stage_hash_mms_SubsidyRule.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_subsidy_rule
if object_id('tempdb..#l_mms_subsidy_rule_inserts') is not null drop table #l_mms_subsidy_rule_inserts
create table #l_mms_subsidy_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SubsidyRule.bk_hash,
       stage_hash_mms_SubsidyRule.SubsidyRuleID subsidy_rule_id,
       stage_hash_mms_SubsidyRule.SubsidyCompanyReimbursementProgramID subsidy_company_reimbursement_program_id,
       stage_hash_mms_SubsidyRule.ValReimbursementUsageTypeID val_reimbursement_usage_type_id,
       isnull(cast(stage_hash_mms_SubsidyRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.SubsidyRuleID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.SubsidyCompanyReimbursementProgramID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.ValReimbursementUsageTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SubsidyRule
 where stage_hash_mms_SubsidyRule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_subsidy_rule records
set @insert_date_time = getdate()
insert into l_mms_subsidy_rule (
       bk_hash,
       subsidy_rule_id,
       subsidy_company_reimbursement_program_id,
       val_reimbursement_usage_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_subsidy_rule_inserts.bk_hash,
       #l_mms_subsidy_rule_inserts.subsidy_rule_id,
       #l_mms_subsidy_rule_inserts.subsidy_company_reimbursement_program_id,
       #l_mms_subsidy_rule_inserts.val_reimbursement_usage_type_id,
       case when l_mms_subsidy_rule.l_mms_subsidy_rule_id is null then isnull(#l_mms_subsidy_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_subsidy_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_subsidy_rule_inserts
  left join p_mms_subsidy_rule
    on #l_mms_subsidy_rule_inserts.bk_hash = p_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_subsidy_rule
    on p_mms_subsidy_rule.bk_hash = l_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.l_mms_subsidy_rule_id = l_mms_subsidy_rule.l_mms_subsidy_rule_id
 where l_mms_subsidy_rule.l_mms_subsidy_rule_id is null
    or (l_mms_subsidy_rule.l_mms_subsidy_rule_id is not null
        and l_mms_subsidy_rule.dv_hash <> #l_mms_subsidy_rule_inserts.source_hash)

--calculate hash and lookup to current s_mms_subsidy_rule
if object_id('tempdb..#s_mms_subsidy_rule_inserts') is not null drop table #s_mms_subsidy_rule_inserts
create table #s_mms_subsidy_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SubsidyRule.bk_hash,
       stage_hash_mms_SubsidyRule.SubsidyRuleID subsidy_rule_id,
       stage_hash_mms_SubsidyRule.Description description,
       stage_hash_mms_SubsidyRule.UsageMinimum usage_minimum,
       stage_hash_mms_SubsidyRule.MaxVisitsPerDay max_visits_per_day,
       stage_hash_mms_SubsidyRule.ReimbursementAmountPerUsage reimbursement_amount_per_usage,
       stage_hash_mms_SubsidyRule.IgnoreUsageMinimumFirstMonthFlag ignore_usage_minimum_first_month_flag,
       stage_hash_mms_SubsidyRule.IncludeTaxUsageTierFlag include_tax_usage_tier_flag,
       stage_hash_mms_SubsidyRule.InsertedDateTime inserted_date_time,
       stage_hash_mms_SubsidyRule.UpdatedDateTime updated_date_time,
       stage_hash_mms_SubsidyRule.IgnoreUsageMinimumPreviousNonAccessFlag ignore_usage_minimum_previous_non_access_flag,
       stage_hash_mms_SubsidyRule.ApplyUsageCreditsPreviousAccessFlag apply_usage_credits_previous_access_flag,
       isnull(cast(stage_hash_mms_SubsidyRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.SubsidyRuleID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_SubsidyRule.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.UsageMinimum as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.MaxVisitsPerDay as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.ReimbursementAmountPerUsage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.IgnoreUsageMinimumFirstMonthFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.IncludeTaxUsageTierFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyRule.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyRule.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.IgnoreUsageMinimumPreviousNonAccessFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyRule.ApplyUsageCreditsPreviousAccessFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SubsidyRule
 where stage_hash_mms_SubsidyRule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_subsidy_rule records
set @insert_date_time = getdate()
insert into s_mms_subsidy_rule (
       bk_hash,
       subsidy_rule_id,
       description,
       usage_minimum,
       max_visits_per_day,
       reimbursement_amount_per_usage,
       ignore_usage_minimum_first_month_flag,
       include_tax_usage_tier_flag,
       inserted_date_time,
       updated_date_time,
       ignore_usage_minimum_previous_non_access_flag,
       apply_usage_credits_previous_access_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_subsidy_rule_inserts.bk_hash,
       #s_mms_subsidy_rule_inserts.subsidy_rule_id,
       #s_mms_subsidy_rule_inserts.description,
       #s_mms_subsidy_rule_inserts.usage_minimum,
       #s_mms_subsidy_rule_inserts.max_visits_per_day,
       #s_mms_subsidy_rule_inserts.reimbursement_amount_per_usage,
       #s_mms_subsidy_rule_inserts.ignore_usage_minimum_first_month_flag,
       #s_mms_subsidy_rule_inserts.include_tax_usage_tier_flag,
       #s_mms_subsidy_rule_inserts.inserted_date_time,
       #s_mms_subsidy_rule_inserts.updated_date_time,
       #s_mms_subsidy_rule_inserts.ignore_usage_minimum_previous_non_access_flag,
       #s_mms_subsidy_rule_inserts.apply_usage_credits_previous_access_flag,
       case when s_mms_subsidy_rule.s_mms_subsidy_rule_id is null then isnull(#s_mms_subsidy_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_subsidy_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_subsidy_rule_inserts
  left join p_mms_subsidy_rule
    on #s_mms_subsidy_rule_inserts.bk_hash = p_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_subsidy_rule
    on p_mms_subsidy_rule.bk_hash = s_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.s_mms_subsidy_rule_id = s_mms_subsidy_rule.s_mms_subsidy_rule_id
 where s_mms_subsidy_rule.s_mms_subsidy_rule_id is null
    or (s_mms_subsidy_rule.s_mms_subsidy_rule_id is not null
        and s_mms_subsidy_rule.dv_hash <> #s_mms_subsidy_rule_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_subsidy_rule @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_subsidy_rule @current_dv_batch_id

end
