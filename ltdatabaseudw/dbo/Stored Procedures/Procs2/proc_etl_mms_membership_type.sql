CREATE PROC [dbo].[proc_etl_mms_membership_type] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipType

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipType (
       bk_hash,
       MembershipTypeID,
       ProductID,
       AssessDueFlag,
       ValMembershipTypeGroupID,
       ValCheckInGroupID,
       InsertedDateTime,
       ValMembershipTypeFamilyStatusID,
       ShortTermMembershipFlag,
       ValEnrollmentTypeID,
       ValUnitTypeID,
       MaxUnitType,
       MemberCardDesignID,
       ExpressMembershipFlag,
       UpdatedDateTime,
       ValWelcomeKitTypeID,
       DisplayName,
       AssessJrMemberDuesFlag,
       WaiveAdminFeeFlag,
       GTASigOverride,
       AllowPartnerProgramFlag,
       MinUnitType,
       MinPrimaryAge,
       ValPricingMethodID,
       WaiveEnrollmentFeeFlag,
       WaiveLateFeeFlag,
       SuppressMembershipCardFlag,
       ValPricingRuleID,
       ValRestrictedGroupID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipTypeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipTypeID,
       ProductID,
       AssessDueFlag,
       ValMembershipTypeGroupID,
       ValCheckInGroupID,
       InsertedDateTime,
       ValMembershipTypeFamilyStatusID,
       ShortTermMembershipFlag,
       ValEnrollmentTypeID,
       ValUnitTypeID,
       MaxUnitType,
       MemberCardDesignID,
       ExpressMembershipFlag,
       UpdatedDateTime,
       ValWelcomeKitTypeID,
       DisplayName,
       AssessJrMemberDuesFlag,
       WaiveAdminFeeFlag,
       GTASigOverride,
       AllowPartnerProgramFlag,
       MinUnitType,
       MinPrimaryAge,
       ValPricingMethodID,
       WaiveEnrollmentFeeFlag,
       WaiveLateFeeFlag,
       SuppressMembershipCardFlag,
       ValPricingRuleID,
       ValRestrictedGroupID,
       isnull(cast(stage_mms_MembershipType.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipType
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_type @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_type (
       bk_hash,
       membership_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_MembershipType.bk_hash,
       stage_hash_mms_MembershipType.MembershipTypeID membership_type_id,
       isnull(cast(stage_hash_mms_MembershipType.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipType
  left join h_mms_membership_type
    on stage_hash_mms_MembershipType.bk_hash = h_mms_membership_type.bk_hash
 where h_mms_membership_type_id is null
   and stage_hash_mms_MembershipType.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_type
if object_id('tempdb..#l_mms_membership_type_inserts') is not null drop table #l_mms_membership_type_inserts
create table #l_mms_membership_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipType.bk_hash,
       stage_hash_mms_MembershipType.MembershipTypeID membership_type_id,
       stage_hash_mms_MembershipType.ProductID product_id,
       stage_hash_mms_MembershipType.ValMembershipTypeGroupID val_membership_type_group_id,
       stage_hash_mms_MembershipType.ValCheckInGroupID val_check_in_group_id,
       stage_hash_mms_MembershipType.ValMembershipTypeFamilyStatusID val_membership_type_family_status_id,
       stage_hash_mms_MembershipType.ValEnrollmentTypeID val_enrollment_type_id,
       stage_hash_mms_MembershipType.ValUnitTypeID val_unit_type_id,
       stage_hash_mms_MembershipType.MemberCardDesignID member_card_design_id,
       stage_hash_mms_MembershipType.ValWelcomeKitTypeID val_welcome_kit_type_id,
       stage_hash_mms_MembershipType.ValPricingMethodID val_pricing_method_id,
       stage_hash_mms_MembershipType.ValPricingRuleID val_pricing_rule_id,
       stage_hash_mms_MembershipType.ValRestrictedGroupID val_restricted_group_id,
       isnull(cast(stage_hash_mms_MembershipType.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MembershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValMembershipTypeGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValCheckInGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValMembershipTypeFamilyStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValEnrollmentTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValUnitTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MemberCardDesignID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValWelcomeKitTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValPricingMethodID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValPricingRuleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ValRestrictedGroupID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipType
 where stage_hash_mms_MembershipType.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_type records
set @insert_date_time = getdate()
insert into l_mms_membership_type (
       bk_hash,
       membership_type_id,
       product_id,
       val_membership_type_group_id,
       val_check_in_group_id,
       val_membership_type_family_status_id,
       val_enrollment_type_id,
       val_unit_type_id,
       member_card_design_id,
       val_welcome_kit_type_id,
       val_pricing_method_id,
       val_pricing_rule_id,
       val_restricted_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_type_inserts.bk_hash,
       #l_mms_membership_type_inserts.membership_type_id,
       #l_mms_membership_type_inserts.product_id,
       #l_mms_membership_type_inserts.val_membership_type_group_id,
       #l_mms_membership_type_inserts.val_check_in_group_id,
       #l_mms_membership_type_inserts.val_membership_type_family_status_id,
       #l_mms_membership_type_inserts.val_enrollment_type_id,
       #l_mms_membership_type_inserts.val_unit_type_id,
       #l_mms_membership_type_inserts.member_card_design_id,
       #l_mms_membership_type_inserts.val_welcome_kit_type_id,
       #l_mms_membership_type_inserts.val_pricing_method_id,
       #l_mms_membership_type_inserts.val_pricing_rule_id,
       #l_mms_membership_type_inserts.val_restricted_group_id,
       case when l_mms_membership_type.l_mms_membership_type_id is null then isnull(#l_mms_membership_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_type_inserts
  left join p_mms_membership_type
    on #l_mms_membership_type_inserts.bk_hash = p_mms_membership_type.bk_hash
   and p_mms_membership_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_type
    on p_mms_membership_type.bk_hash = l_mms_membership_type.bk_hash
   and p_mms_membership_type.l_mms_membership_type_id = l_mms_membership_type.l_mms_membership_type_id
 where l_mms_membership_type.l_mms_membership_type_id is null
    or (l_mms_membership_type.l_mms_membership_type_id is not null
        and l_mms_membership_type.dv_hash <> #l_mms_membership_type_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_type
if object_id('tempdb..#s_mms_membership_type_inserts') is not null drop table #s_mms_membership_type_inserts
create table #s_mms_membership_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipType.bk_hash,
       stage_hash_mms_MembershipType.MembershipTypeID membership_type_id,
       stage_hash_mms_MembershipType.AssessDueFlag assess_due_flag,
       stage_hash_mms_MembershipType.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipType.ShortTermMembershipFlag short_term_membership_flag,
       stage_hash_mms_MembershipType.MaxUnitType max_unit_type,
       stage_hash_mms_MembershipType.ExpressMembershipFlag express_membership_flag,
       stage_hash_mms_MembershipType.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipType.DisplayName display_name,
       stage_hash_mms_MembershipType.AssessJrMemberDuesFlag assess_jr_member_dues_flag,
       stage_hash_mms_MembershipType.WaiveAdminFeeFlag waive_admin_fee_flag,
       stage_hash_mms_MembershipType.GTASigOverride gta_sig_override,
       stage_hash_mms_MembershipType.AllowPartnerProgramFlag allow_partner_program_flag,
       stage_hash_mms_MembershipType.MinUnitType min_unit_type,
       stage_hash_mms_MembershipType.MinPrimaryAge min_primary_age,
       stage_hash_mms_MembershipType.WaiveLateFeeFlag waive_late_fee_flag,
       stage_hash_mms_MembershipType.SuppressMembershipCardFlag suppress_membership_card_flag,
       stage_hash_mms_MembershipType.WaiveEnrollmentFeeFlag waive_enrollment_fee_flag,
       isnull(cast(stage_hash_mms_MembershipType.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MembershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.AssessDueFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipType.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ShortTermMembershipFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MaxUnitType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.ExpressMembershipFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipType.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipType.DisplayName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.WaiveAdminFeeFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipType.GTASigOverride,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.AllowPartnerProgramFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MinUnitType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.MinPrimaryAge as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.WaiveLateFeeFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.SuppressMembershipCardFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipType.WaiveEnrollmentFeeFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipType
 where stage_hash_mms_MembershipType.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_type records
set @insert_date_time = getdate()
insert into s_mms_membership_type (
       bk_hash,
       membership_type_id,
       assess_due_flag,
       inserted_date_time,
       short_term_membership_flag,
       max_unit_type,
       express_membership_flag,
       updated_date_time,
       display_name,
       assess_jr_member_dues_flag,
       waive_admin_fee_flag,
       gta_sig_override,
       allow_partner_program_flag,
       min_unit_type,
       min_primary_age,
       waive_late_fee_flag,
       suppress_membership_card_flag,
       waive_enrollment_fee_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_type_inserts.bk_hash,
       #s_mms_membership_type_inserts.membership_type_id,
       #s_mms_membership_type_inserts.assess_due_flag,
       #s_mms_membership_type_inserts.inserted_date_time,
       #s_mms_membership_type_inserts.short_term_membership_flag,
       #s_mms_membership_type_inserts.max_unit_type,
       #s_mms_membership_type_inserts.express_membership_flag,
       #s_mms_membership_type_inserts.updated_date_time,
       #s_mms_membership_type_inserts.display_name,
       #s_mms_membership_type_inserts.assess_jr_member_dues_flag,
       #s_mms_membership_type_inserts.waive_admin_fee_flag,
       #s_mms_membership_type_inserts.gta_sig_override,
       #s_mms_membership_type_inserts.allow_partner_program_flag,
       #s_mms_membership_type_inserts.min_unit_type,
       #s_mms_membership_type_inserts.min_primary_age,
       #s_mms_membership_type_inserts.waive_late_fee_flag,
       #s_mms_membership_type_inserts.suppress_membership_card_flag,
       #s_mms_membership_type_inserts.waive_enrollment_fee_flag,
       case when s_mms_membership_type.s_mms_membership_type_id is null then isnull(#s_mms_membership_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_type_inserts
  left join p_mms_membership_type
    on #s_mms_membership_type_inserts.bk_hash = p_mms_membership_type.bk_hash
   and p_mms_membership_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_type
    on p_mms_membership_type.bk_hash = s_mms_membership_type.bk_hash
   and p_mms_membership_type.s_mms_membership_type_id = s_mms_membership_type.s_mms_membership_type_id
 where s_mms_membership_type.s_mms_membership_type_id is null
    or (s_mms_membership_type.s_mms_membership_type_id is not null
        and s_mms_membership_type.dv_hash <> #s_mms_membership_type_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_type @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_type @current_dv_batch_id
exec dbo.proc_d_mms_membership_type_history @current_dv_batch_id

end
