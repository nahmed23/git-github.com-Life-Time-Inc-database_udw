CREATE PROC [dbo].[proc_etl_mms_membership_modification_request] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipModificationRequest

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipModificationRequest (
       bk_hash,
       MembershipModificationRequestID,
       MembershipID,
       MemberID,
       RequestDateTime,
       UTCRequestDateTime,
       RequestDateTimeZone,
       EffectiveDate,
       ValMembershipModificationRequestTypeID,
       ValFlexReasonID,
       MembershipTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       ValMembershipModificationRequestStatusID,
       StatusChangedDateTime,
       EmployeeID,
       LastEFTMonth,
       FutureMembershipUpgradeFlag,
       ValMembershipUpgradeDateRangeID,
       ClubID,
       CommisionedEmployeeID,
       FirstMonthsDues,
       TotalMonthlyAmount,
       MemberAgreementStagingID,
       MembershipUpgradeMonthYear,
       AgreementPrice,
       WaiveServiceFeeFlag,
       FullAccessDateExtensionFlag,
       NewMembers,
       AddOnFee,
       ServiceFee,
       DiamondFee,
       ProratedDues,
       DeactivatedMembers,
       JuniorsAssessed,
       PreviousMembershipTypeID,
       MemberFreezeFlag,
       NewPrimaryID,
       UndiscountedPrice,
       ValMembershipModificationRequestSourceID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipModificationRequestID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipModificationRequestID,
       MembershipID,
       MemberID,
       RequestDateTime,
       UTCRequestDateTime,
       RequestDateTimeZone,
       EffectiveDate,
       ValMembershipModificationRequestTypeID,
       ValFlexReasonID,
       MembershipTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       ValMembershipModificationRequestStatusID,
       StatusChangedDateTime,
       EmployeeID,
       LastEFTMonth,
       FutureMembershipUpgradeFlag,
       ValMembershipUpgradeDateRangeID,
       ClubID,
       CommisionedEmployeeID,
       FirstMonthsDues,
       TotalMonthlyAmount,
       MemberAgreementStagingID,
       MembershipUpgradeMonthYear,
       AgreementPrice,
       WaiveServiceFeeFlag,
       FullAccessDateExtensionFlag,
       NewMembers,
       AddOnFee,
       ServiceFee,
       DiamondFee,
       ProratedDues,
       DeactivatedMembers,
       JuniorsAssessed,
       PreviousMembershipTypeID,
       MemberFreezeFlag,
       NewPrimaryID,
       UndiscountedPrice,
       ValMembershipModificationRequestSourceID,
       isnull(cast(stage_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipModificationRequest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_modification_request @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_modification_request (
       bk_hash,
       membership_modification_request_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_MembershipModificationRequest.bk_hash,
       stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID membership_modification_request_id,
       isnull(cast(stage_hash_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipModificationRequest
  left join h_mms_membership_modification_request
    on stage_hash_mms_MembershipModificationRequest.bk_hash = h_mms_membership_modification_request.bk_hash
 where h_mms_membership_modification_request_id is null
   and stage_hash_mms_MembershipModificationRequest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_modification_request
if object_id('tempdb..#l_mms_membership_modification_request_inserts') is not null drop table #l_mms_membership_modification_request_inserts
create table #l_mms_membership_modification_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipModificationRequest.bk_hash,
       stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID membership_modification_request_id,
       stage_hash_mms_MembershipModificationRequest.MembershipID membership_id,
       stage_hash_mms_MembershipModificationRequest.MemberID member_id,
       stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestTypeID val_membership_modification_request_type_id,
       stage_hash_mms_MembershipModificationRequest.ValFlexReasonID val_flex_reason_id,
       stage_hash_mms_MembershipModificationRequest.MembershipTypeID membership_type_id,
       stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestStatusID val_membership_modification_request_status_id,
       stage_hash_mms_MembershipModificationRequest.EmployeeID employee_id,
       stage_hash_mms_MembershipModificationRequest.ValMembershipUpgradeDateRangeID val_membership_upgrade_date_range_id,
       stage_hash_mms_MembershipModificationRequest.ClubID club_id,
       stage_hash_mms_MembershipModificationRequest.CommisionedEmployeeID commisioned_employee_id,
       stage_hash_mms_MembershipModificationRequest.MemberAgreementStagingID member_agreement_staging_id,
       stage_hash_mms_MembershipModificationRequest.PreviousMembershipTypeID previous_membership_type_id,
       isnull(cast(stage_hash_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ValFlexReasonID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.EmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ValMembershipUpgradeDateRangeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.CommisionedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MemberAgreementStagingID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.PreviousMembershipTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipModificationRequest
 where stage_hash_mms_MembershipModificationRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_modification_request records
set @insert_date_time = getdate()
insert into l_mms_membership_modification_request (
       bk_hash,
       membership_modification_request_id,
       membership_id,
       member_id,
       val_membership_modification_request_type_id,
       val_flex_reason_id,
       membership_type_id,
       val_membership_modification_request_status_id,
       employee_id,
       val_membership_upgrade_date_range_id,
       club_id,
       commisioned_employee_id,
       member_agreement_staging_id,
       previous_membership_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_modification_request_inserts.bk_hash,
       #l_mms_membership_modification_request_inserts.membership_modification_request_id,
       #l_mms_membership_modification_request_inserts.membership_id,
       #l_mms_membership_modification_request_inserts.member_id,
       #l_mms_membership_modification_request_inserts.val_membership_modification_request_type_id,
       #l_mms_membership_modification_request_inserts.val_flex_reason_id,
       #l_mms_membership_modification_request_inserts.membership_type_id,
       #l_mms_membership_modification_request_inserts.val_membership_modification_request_status_id,
       #l_mms_membership_modification_request_inserts.employee_id,
       #l_mms_membership_modification_request_inserts.val_membership_upgrade_date_range_id,
       #l_mms_membership_modification_request_inserts.club_id,
       #l_mms_membership_modification_request_inserts.commisioned_employee_id,
       #l_mms_membership_modification_request_inserts.member_agreement_staging_id,
       #l_mms_membership_modification_request_inserts.previous_membership_type_id,
       case when l_mms_membership_modification_request.l_mms_membership_modification_request_id is null then isnull(#l_mms_membership_modification_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_modification_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_modification_request_inserts
  left join p_mms_membership_modification_request
    on #l_mms_membership_modification_request_inserts.bk_hash = p_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_modification_request
    on p_mms_membership_modification_request.bk_hash = l_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.l_mms_membership_modification_request_id = l_mms_membership_modification_request.l_mms_membership_modification_request_id
 where l_mms_membership_modification_request.l_mms_membership_modification_request_id is null
    or (l_mms_membership_modification_request.l_mms_membership_modification_request_id is not null
        and l_mms_membership_modification_request.dv_hash <> #l_mms_membership_modification_request_inserts.source_hash)

--calculate hash and lookup to current l_mms_membership_modification_request_1
if object_id('tempdb..#l_mms_membership_modification_request_1_inserts') is not null drop table #l_mms_membership_modification_request_1_inserts
create table #l_mms_membership_modification_request_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipModificationRequest.bk_hash,
       stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID membership_modification_request_id,
       stage_hash_mms_MembershipModificationRequest.NewPrimaryID new_primary_id,
       stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestSourceID val_membership_modification_request_source_id,
       isnull(cast(stage_hash_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.NewPrimaryID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ValMembershipModificationRequestSourceID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipModificationRequest
 where stage_hash_mms_MembershipModificationRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_modification_request_1 records
set @insert_date_time = getdate()
insert into l_mms_membership_modification_request_1 (
       bk_hash,
       membership_modification_request_id,
       new_primary_id,
       val_membership_modification_request_source_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_modification_request_1_inserts.bk_hash,
       #l_mms_membership_modification_request_1_inserts.membership_modification_request_id,
       #l_mms_membership_modification_request_1_inserts.new_primary_id,
       #l_mms_membership_modification_request_1_inserts.val_membership_modification_request_source_id,
       case when l_mms_membership_modification_request_1.l_mms_membership_modification_request_1_id is null then isnull(#l_mms_membership_modification_request_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_modification_request_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_modification_request_1_inserts
  left join p_mms_membership_modification_request
    on #l_mms_membership_modification_request_1_inserts.bk_hash = p_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_modification_request_1
    on p_mms_membership_modification_request.bk_hash = l_mms_membership_modification_request_1.bk_hash
   and p_mms_membership_modification_request.l_mms_membership_modification_request_1_id = l_mms_membership_modification_request_1.l_mms_membership_modification_request_1_id
 where l_mms_membership_modification_request_1.l_mms_membership_modification_request_1_id is null
    or (l_mms_membership_modification_request_1.l_mms_membership_modification_request_1_id is not null
        and l_mms_membership_modification_request_1.dv_hash <> #l_mms_membership_modification_request_1_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_modification_request
if object_id('tempdb..#s_mms_membership_modification_request_inserts') is not null drop table #s_mms_membership_modification_request_inserts
create table #s_mms_membership_modification_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipModificationRequest.bk_hash,
       stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID membership_modification_request_id,
       stage_hash_mms_MembershipModificationRequest.RequestDateTime request_date_time,
       stage_hash_mms_MembershipModificationRequest.UTCRequestDateTime utc_request_date_time,
       stage_hash_mms_MembershipModificationRequest.RequestDateTimeZone request_date_time_zone,
       stage_hash_mms_MembershipModificationRequest.EffectiveDate effective_date,
       stage_hash_mms_MembershipModificationRequest.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipModificationRequest.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipModificationRequest.StatusChangedDateTime status_changed_date_time,
       stage_hash_mms_MembershipModificationRequest.LastEFTMonth last_eft_month,
       stage_hash_mms_MembershipModificationRequest.FutureMembershipUpgradeFlag future_membership_upgrade_flag,
       stage_hash_mms_MembershipModificationRequest.FirstMonthsDues first_months_dues,
       stage_hash_mms_MembershipModificationRequest.TotalMonthlyAmount total_monthly_amount,
       stage_hash_mms_MembershipModificationRequest.MembershipUpgradeMonthYear membership_upgrade_month_year,
       stage_hash_mms_MembershipModificationRequest.AgreementPrice agreement_price,
       stage_hash_mms_MembershipModificationRequest.WaiveServiceFeeFlag waive_service_fee_flag,
       stage_hash_mms_MembershipModificationRequest.FullAccessDateExtensionFlag full_access_date_extension_flag,
       stage_hash_mms_MembershipModificationRequest.NewMembers new_members,
       stage_hash_mms_MembershipModificationRequest.AddOnFee add_on_fee,
       stage_hash_mms_MembershipModificationRequest.ServiceFee service_fee,
       stage_hash_mms_MembershipModificationRequest.DiamondFee diamond_fee,
       stage_hash_mms_MembershipModificationRequest.ProratedDues pro_rated_dues,
       stage_hash_mms_MembershipModificationRequest.DeactivatedMembers deactivated_members,
       stage_hash_mms_MembershipModificationRequest.JuniorsAssessed juniors_assessed,
       stage_hash_mms_MembershipModificationRequest.MemberFreezeFlag member_freeze_flag,
       isnull(cast(stage_hash_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.RequestDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.UTCRequestDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipModificationRequest.RequestDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.EffectiveDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.StatusChangedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipModificationRequest.LastEFTMonth,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.FutureMembershipUpgradeFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.FirstMonthsDues as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.TotalMonthlyAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipModificationRequest.MembershipUpgradeMonthYear,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.AgreementPrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.WaiveServiceFeeFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.FullAccessDateExtensionFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipModificationRequest.NewMembers,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.AddOnFee as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ServiceFee as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.DiamondFee as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.ProratedDues as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipModificationRequest.DeactivatedMembers,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.JuniorsAssessed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MemberFreezeFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipModificationRequest
 where stage_hash_mms_MembershipModificationRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_modification_request records
set @insert_date_time = getdate()
insert into s_mms_membership_modification_request (
       bk_hash,
       membership_modification_request_id,
       request_date_time,
       utc_request_date_time,
       request_date_time_zone,
       effective_date,
       inserted_date_time,
       updated_date_time,
       status_changed_date_time,
       last_eft_month,
       future_membership_upgrade_flag,
       first_months_dues,
       total_monthly_amount,
       membership_upgrade_month_year,
       agreement_price,
       waive_service_fee_flag,
       full_access_date_extension_flag,
       new_members,
       add_on_fee,
       service_fee,
       diamond_fee,
       pro_rated_dues,
       deactivated_members,
       juniors_assessed,
       member_freeze_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_modification_request_inserts.bk_hash,
       #s_mms_membership_modification_request_inserts.membership_modification_request_id,
       #s_mms_membership_modification_request_inserts.request_date_time,
       #s_mms_membership_modification_request_inserts.utc_request_date_time,
       #s_mms_membership_modification_request_inserts.request_date_time_zone,
       #s_mms_membership_modification_request_inserts.effective_date,
       #s_mms_membership_modification_request_inserts.inserted_date_time,
       #s_mms_membership_modification_request_inserts.updated_date_time,
       #s_mms_membership_modification_request_inserts.status_changed_date_time,
       #s_mms_membership_modification_request_inserts.last_eft_month,
       #s_mms_membership_modification_request_inserts.future_membership_upgrade_flag,
       #s_mms_membership_modification_request_inserts.first_months_dues,
       #s_mms_membership_modification_request_inserts.total_monthly_amount,
       #s_mms_membership_modification_request_inserts.membership_upgrade_month_year,
       #s_mms_membership_modification_request_inserts.agreement_price,
       #s_mms_membership_modification_request_inserts.waive_service_fee_flag,
       #s_mms_membership_modification_request_inserts.full_access_date_extension_flag,
       #s_mms_membership_modification_request_inserts.new_members,
       #s_mms_membership_modification_request_inserts.add_on_fee,
       #s_mms_membership_modification_request_inserts.service_fee,
       #s_mms_membership_modification_request_inserts.diamond_fee,
       #s_mms_membership_modification_request_inserts.pro_rated_dues,
       #s_mms_membership_modification_request_inserts.deactivated_members,
       #s_mms_membership_modification_request_inserts.juniors_assessed,
       #s_mms_membership_modification_request_inserts.member_freeze_flag,
       case when s_mms_membership_modification_request.s_mms_membership_modification_request_id is null then isnull(#s_mms_membership_modification_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_modification_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_modification_request_inserts
  left join p_mms_membership_modification_request
    on #s_mms_membership_modification_request_inserts.bk_hash = p_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_modification_request
    on p_mms_membership_modification_request.bk_hash = s_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.s_mms_membership_modification_request_id = s_mms_membership_modification_request.s_mms_membership_modification_request_id
 where s_mms_membership_modification_request.s_mms_membership_modification_request_id is null
    or (s_mms_membership_modification_request.s_mms_membership_modification_request_id is not null
        and s_mms_membership_modification_request.dv_hash <> #s_mms_membership_modification_request_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_modification_request_1
if object_id('tempdb..#s_mms_membership_modification_request_1_inserts') is not null drop table #s_mms_membership_modification_request_1_inserts
create table #s_mms_membership_modification_request_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipModificationRequest.bk_hash,
       stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID membership_modification_request_id,
       stage_hash_mms_MembershipModificationRequest.UndiscountedPrice undiscounted_price,
       isnull(cast(stage_hash_mms_MembershipModificationRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.MembershipModificationRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipModificationRequest.UndiscountedPrice as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipModificationRequest
 where stage_hash_mms_MembershipModificationRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_modification_request_1 records
set @insert_date_time = getdate()
insert into s_mms_membership_modification_request_1 (
       bk_hash,
       membership_modification_request_id,
       undiscounted_price,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_modification_request_1_inserts.bk_hash,
       #s_mms_membership_modification_request_1_inserts.membership_modification_request_id,
       #s_mms_membership_modification_request_1_inserts.undiscounted_price,
       case when s_mms_membership_modification_request_1.s_mms_membership_modification_request_1_id is null then isnull(#s_mms_membership_modification_request_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_modification_request_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_modification_request_1_inserts
  left join p_mms_membership_modification_request
    on #s_mms_membership_modification_request_1_inserts.bk_hash = p_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_modification_request_1
    on p_mms_membership_modification_request.bk_hash = s_mms_membership_modification_request_1.bk_hash
   and p_mms_membership_modification_request.s_mms_membership_modification_request_1_id = s_mms_membership_modification_request_1.s_mms_membership_modification_request_1_id
 where s_mms_membership_modification_request_1.s_mms_membership_modification_request_1_id is null
    or (s_mms_membership_modification_request_1.s_mms_membership_modification_request_1_id is not null
        and s_mms_membership_modification_request_1.dv_hash <> #s_mms_membership_modification_request_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_modification_request @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_modification_request @current_dv_batch_id
exec dbo.proc_d_mms_membership_modification_request_history @current_dv_batch_id

end
