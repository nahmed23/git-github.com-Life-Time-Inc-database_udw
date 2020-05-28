CREATE PROC [dbo].[proc_etl_mms_membership_snapshot] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipSnapshot

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipSnapshot (
       bk_hash,
       MembershipID,
       ClubID,
       PurchaserID,
       LegacyCode,
       AdvisorEmployeeID,
       ActivationDate,
       ExpirationDate,
       TotalContractAmount,
       CompanyID,
       Comments,
       MandatoryCommentFlag,
       ValEFTOptionID,
       ValEnrollmentTypeID,
       ValTerminationReasonID,
       MembershipTypeID,
       ValMembershipStatusID,
       CancellationRequestDate,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       InsertedDateTime,
       ValMembershipSourceID,
       UpdatedDateTime,
       PromotionID,
       JrMemberDuesProductID,
       Salesforce_Prospect_ID,
       MoneyBackCancelPolicyDays,
       LastUpdatedEmployeeID,
       QualifiedSalesPromotionID,
       JoinFeePaid,
       Salesforce_Account_ID,
       ChildCenterUnrestrictedCheckoutFlag,
       Salesforce_Opportunity_ID,
       ValTerminationReasonClubTypeID,
       CRMOpportunityID,
       CurrentPrice,
       PriorPlusPrice,
       PriorPlusMembershipTypeID,
       CRMAccountID,
       OverdueRecurrentProductBalanceFlag,
       BlockRecurrentProductRenewalFlag,
       ValEFTOptionProductID,
       UndiscountedPrice,
       PriorPlusUndiscountedPrice,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipID,
       ClubID,
       PurchaserID,
       LegacyCode,
       AdvisorEmployeeID,
       ActivationDate,
       ExpirationDate,
       TotalContractAmount,
       CompanyID,
       Comments,
       MandatoryCommentFlag,
       ValEFTOptionID,
       ValEnrollmentTypeID,
       ValTerminationReasonID,
       MembershipTypeID,
       ValMembershipStatusID,
       CancellationRequestDate,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       InsertedDateTime,
       ValMembershipSourceID,
       UpdatedDateTime,
       PromotionID,
       JrMemberDuesProductID,
       Salesforce_Prospect_ID,
       MoneyBackCancelPolicyDays,
       LastUpdatedEmployeeID,
       QualifiedSalesPromotionID,
       JoinFeePaid,
       Salesforce_Account_ID,
       ChildCenterUnrestrictedCheckoutFlag,
       Salesforce_Opportunity_ID,
       ValTerminationReasonClubTypeID,
       CRMOpportunityID,
       CurrentPrice,
       PriorPlusPrice,
       PriorPlusMembershipTypeID,
       CRMAccountID,
       OverdueRecurrentProductBalanceFlag,
       BlockRecurrentProductRenewalFlag,
       ValEFTOptionProductID,
       UndiscountedPrice,
       PriorPlusUndiscountedPrice,
       isnull(cast(stage_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipSnapshot
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_snapshot @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_snapshot (
       bk_hash,
       membership_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_MembershipSnapshot.bk_hash,
       stage_hash_mms_MembershipSnapshot.MembershipID membership_id,
       isnull(cast(stage_hash_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipSnapshot
  left join h_mms_membership_snapshot
    on stage_hash_mms_MembershipSnapshot.bk_hash = h_mms_membership_snapshot.bk_hash
 where h_mms_membership_snapshot_id is null
   and stage_hash_mms_MembershipSnapshot.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_snapshot
if object_id('tempdb..#l_mms_membership_snapshot_inserts') is not null drop table #l_mms_membership_snapshot_inserts
create table #l_mms_membership_snapshot_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipSnapshot.bk_hash,
       stage_hash_mms_MembershipSnapshot.MembershipID membership_id,
       stage_hash_mms_MembershipSnapshot.ClubID club_id,
       stage_hash_mms_MembershipSnapshot.PurchaserID purchaser_id,
       stage_hash_mms_MembershipSnapshot.AdvisorEmployeeID advisor_employee_id,
       stage_hash_mms_MembershipSnapshot.CompanyID company_id,
       stage_hash_mms_MembershipSnapshot.ValEFTOptionID val_eft_option_id,
       stage_hash_mms_MembershipSnapshot.ValEnrollmentTypeID val_enrollment_type_id,
       stage_hash_mms_MembershipSnapshot.ValTerminationReasonID val_termination_reason_id,
       stage_hash_mms_MembershipSnapshot.MembershipTypeID membership_type_id,
       stage_hash_mms_MembershipSnapshot.ValMembershipStatusID val_membership_status_id,
       stage_hash_mms_MembershipSnapshot.ValMembershipSourceID val_membership_source_id,
       stage_hash_mms_MembershipSnapshot.PromotionID promotion_id,
       stage_hash_mms_MembershipSnapshot.JrMemberDuesProductID jr_member_dues_product_id,
       stage_hash_mms_MembershipSnapshot.Salesforce_Prospect_ID sales_force_prospect_id,
       stage_hash_mms_MembershipSnapshot.LastUpdatedEmployeeID last_updated_employee_id,
       stage_hash_mms_MembershipSnapshot.QualifiedSalesPromotionID qualified_sales_promotion_id,
       stage_hash_mms_MembershipSnapshot.Salesforce_Account_ID sales_force_account_id,
       stage_hash_mms_MembershipSnapshot.Salesforce_Opportunity_ID sales_force_opportunity_id,
       stage_hash_mms_MembershipSnapshot.ValTerminationReasonClubTypeID val_termination_reason_club_type_id,
       stage_hash_mms_MembershipSnapshot.CRMOpportunityID crm_opportunity_id,
       stage_hash_mms_MembershipSnapshot.PriorPlusMembershipTypeID prior_plus_membership_type_id,
       stage_hash_mms_MembershipSnapshot.CRMAccountID crm_account_id,
       isnull(cast(stage_hash_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.PurchaserID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.AdvisorEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.CompanyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValEFTOptionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValEnrollmentTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValTerminationReasonID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MembershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValMembershipStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValMembershipSourceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.PromotionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.JrMemberDuesProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.Salesforce_Prospect_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.QualifiedSalesPromotionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.Salesforce_Account_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.Salesforce_Opportunity_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValTerminationReasonClubTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.CRMOpportunityID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.PriorPlusMembershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.CRMAccountID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipSnapshot
 where stage_hash_mms_MembershipSnapshot.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_snapshot records
set @insert_date_time = getdate()
insert into l_mms_membership_snapshot (
       bk_hash,
       membership_id,
       club_id,
       purchaser_id,
       advisor_employee_id,
       company_id,
       val_eft_option_id,
       val_enrollment_type_id,
       val_termination_reason_id,
       membership_type_id,
       val_membership_status_id,
       val_membership_source_id,
       promotion_id,
       jr_member_dues_product_id,
       sales_force_prospect_id,
       last_updated_employee_id,
       qualified_sales_promotion_id,
       sales_force_account_id,
       sales_force_opportunity_id,
       val_termination_reason_club_type_id,
       crm_opportunity_id,
       prior_plus_membership_type_id,
       crm_account_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_snapshot_inserts.bk_hash,
       #l_mms_membership_snapshot_inserts.membership_id,
       #l_mms_membership_snapshot_inserts.club_id,
       #l_mms_membership_snapshot_inserts.purchaser_id,
       #l_mms_membership_snapshot_inserts.advisor_employee_id,
       #l_mms_membership_snapshot_inserts.company_id,
       #l_mms_membership_snapshot_inserts.val_eft_option_id,
       #l_mms_membership_snapshot_inserts.val_enrollment_type_id,
       #l_mms_membership_snapshot_inserts.val_termination_reason_id,
       #l_mms_membership_snapshot_inserts.membership_type_id,
       #l_mms_membership_snapshot_inserts.val_membership_status_id,
       #l_mms_membership_snapshot_inserts.val_membership_source_id,
       #l_mms_membership_snapshot_inserts.promotion_id,
       #l_mms_membership_snapshot_inserts.jr_member_dues_product_id,
       #l_mms_membership_snapshot_inserts.sales_force_prospect_id,
       #l_mms_membership_snapshot_inserts.last_updated_employee_id,
       #l_mms_membership_snapshot_inserts.qualified_sales_promotion_id,
       #l_mms_membership_snapshot_inserts.sales_force_account_id,
       #l_mms_membership_snapshot_inserts.sales_force_opportunity_id,
       #l_mms_membership_snapshot_inserts.val_termination_reason_club_type_id,
       #l_mms_membership_snapshot_inserts.crm_opportunity_id,
       #l_mms_membership_snapshot_inserts.prior_plus_membership_type_id,
       #l_mms_membership_snapshot_inserts.crm_account_id,
       case when l_mms_membership_snapshot.l_mms_membership_snapshot_id is null then isnull(#l_mms_membership_snapshot_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_snapshot_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_snapshot_inserts
  left join p_mms_membership_snapshot
    on #l_mms_membership_snapshot_inserts.bk_hash = p_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_snapshot
    on p_mms_membership_snapshot.bk_hash = l_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.l_mms_membership_snapshot_id = l_mms_membership_snapshot.l_mms_membership_snapshot_id
 where l_mms_membership_snapshot.l_mms_membership_snapshot_id is null
    or (l_mms_membership_snapshot.l_mms_membership_snapshot_id is not null
        and l_mms_membership_snapshot.dv_hash <> #l_mms_membership_snapshot_inserts.source_hash)

--calculate hash and lookup to current l_mms_membership_snapshot_1
if object_id('tempdb..#l_mms_membership_snapshot_1_inserts') is not null drop table #l_mms_membership_snapshot_1_inserts
create table #l_mms_membership_snapshot_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipSnapshot.bk_hash,
       stage_hash_mms_MembershipSnapshot.MembershipID membership_id,
       stage_hash_mms_MembershipSnapshot.ValEFTOptionProductID val_eft_option_product_id,
       isnull(cast(stage_hash_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ValEFTOptionProductID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipSnapshot
 where stage_hash_mms_MembershipSnapshot.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_snapshot_1 records
set @insert_date_time = getdate()
insert into l_mms_membership_snapshot_1 (
       bk_hash,
       membership_id,
       val_eft_option_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_snapshot_1_inserts.bk_hash,
       #l_mms_membership_snapshot_1_inserts.membership_id,
       #l_mms_membership_snapshot_1_inserts.val_eft_option_product_id,
       case when l_mms_membership_snapshot_1.l_mms_membership_snapshot_1_id is null then isnull(#l_mms_membership_snapshot_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_snapshot_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_snapshot_1_inserts
  left join p_mms_membership_snapshot
    on #l_mms_membership_snapshot_1_inserts.bk_hash = p_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_snapshot_1
    on p_mms_membership_snapshot.bk_hash = l_mms_membership_snapshot_1.bk_hash
   and p_mms_membership_snapshot.l_mms_membership_snapshot_1_id = l_mms_membership_snapshot_1.l_mms_membership_snapshot_1_id
 where l_mms_membership_snapshot_1.l_mms_membership_snapshot_1_id is null
    or (l_mms_membership_snapshot_1.l_mms_membership_snapshot_1_id is not null
        and l_mms_membership_snapshot_1.dv_hash <> #l_mms_membership_snapshot_1_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_snapshot
if object_id('tempdb..#s_mms_membership_snapshot_inserts') is not null drop table #s_mms_membership_snapshot_inserts
create table #s_mms_membership_snapshot_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipSnapshot.bk_hash,
       stage_hash_mms_MembershipSnapshot.MembershipID membership_id,
       stage_hash_mms_MembershipSnapshot.LegacyCode legacy_code,
       stage_hash_mms_MembershipSnapshot.ActivationDate activation_date,
       stage_hash_mms_MembershipSnapshot.ExpirationDate expiration_date,
       stage_hash_mms_MembershipSnapshot.TotalContractAmount total_contract_amount,
       stage_hash_mms_MembershipSnapshot.Comments comments,
       stage_hash_mms_MembershipSnapshot.MandatoryCommentFlag mandatory_comment_flag,
       stage_hash_mms_MembershipSnapshot.CancellationRequestDate cancellation_request_date,
       stage_hash_mms_MembershipSnapshot.CreatedDateTime created_date_time,
       stage_hash_mms_MembershipSnapshot.UTCCreatedDateTime utc_created_date_time,
       stage_hash_mms_MembershipSnapshot.CreatedDateTimeZone created_date_time_zone,
       stage_hash_mms_MembershipSnapshot.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipSnapshot.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipSnapshot.MoneyBackCancelPolicyDays money_back_cancel_policy_days,
       stage_hash_mms_MembershipSnapshot.JoinFeePaid join_fee_paid,
       stage_hash_mms_MembershipSnapshot.ChildCenterUnrestrictedCheckoutFlag child_center_unrestricted_checkout_flag,
       stage_hash_mms_MembershipSnapshot.CurrentPrice current_price,
       stage_hash_mms_MembershipSnapshot.PriorPlusPrice prior_plus_price,
       isnull(cast(stage_hash_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.LegacyCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.ActivationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.ExpirationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.TotalContractAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.Comments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MandatoryCommentFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.CancellationRequestDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.CreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.UTCCreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipSnapshot.CreatedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipSnapshot.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MoneyBackCancelPolicyDays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.JoinFeePaid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.ChildCenterUnrestrictedCheckoutFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.CurrentPrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.PriorPlusPrice as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipSnapshot
 where stage_hash_mms_MembershipSnapshot.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_snapshot records
set @insert_date_time = getdate()
insert into s_mms_membership_snapshot (
       bk_hash,
       membership_id,
       legacy_code,
       activation_date,
       expiration_date,
       total_contract_amount,
       comments,
       mandatory_comment_flag,
       cancellation_request_date,
       created_date_time,
       utc_created_date_time,
       created_date_time_zone,
       inserted_date_time,
       updated_date_time,
       money_back_cancel_policy_days,
       join_fee_paid,
       child_center_unrestricted_checkout_flag,
       current_price,
       prior_plus_price,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_snapshot_inserts.bk_hash,
       #s_mms_membership_snapshot_inserts.membership_id,
       #s_mms_membership_snapshot_inserts.legacy_code,
       #s_mms_membership_snapshot_inserts.activation_date,
       #s_mms_membership_snapshot_inserts.expiration_date,
       #s_mms_membership_snapshot_inserts.total_contract_amount,
       #s_mms_membership_snapshot_inserts.comments,
       #s_mms_membership_snapshot_inserts.mandatory_comment_flag,
       #s_mms_membership_snapshot_inserts.cancellation_request_date,
       #s_mms_membership_snapshot_inserts.created_date_time,
       #s_mms_membership_snapshot_inserts.utc_created_date_time,
       #s_mms_membership_snapshot_inserts.created_date_time_zone,
       #s_mms_membership_snapshot_inserts.inserted_date_time,
       #s_mms_membership_snapshot_inserts.updated_date_time,
       #s_mms_membership_snapshot_inserts.money_back_cancel_policy_days,
       #s_mms_membership_snapshot_inserts.join_fee_paid,
       #s_mms_membership_snapshot_inserts.child_center_unrestricted_checkout_flag,
       #s_mms_membership_snapshot_inserts.current_price,
       #s_mms_membership_snapshot_inserts.prior_plus_price,
       case when s_mms_membership_snapshot.s_mms_membership_snapshot_id is null then isnull(#s_mms_membership_snapshot_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_snapshot_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_snapshot_inserts
  left join p_mms_membership_snapshot
    on #s_mms_membership_snapshot_inserts.bk_hash = p_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_snapshot
    on p_mms_membership_snapshot.bk_hash = s_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.s_mms_membership_snapshot_id = s_mms_membership_snapshot.s_mms_membership_snapshot_id
 where s_mms_membership_snapshot.s_mms_membership_snapshot_id is null
    or (s_mms_membership_snapshot.s_mms_membership_snapshot_id is not null
        and s_mms_membership_snapshot.dv_hash <> #s_mms_membership_snapshot_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_snapshot_1
if object_id('tempdb..#s_mms_membership_snapshot_1_inserts') is not null drop table #s_mms_membership_snapshot_1_inserts
create table #s_mms_membership_snapshot_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipSnapshot.bk_hash,
       stage_hash_mms_MembershipSnapshot.MembershipID membership_id,
       stage_hash_mms_MembershipSnapshot.OverdueRecurrentProductBalanceFlag overdue_recurrent_product_balance_flag,
       stage_hash_mms_MembershipSnapshot.BlockRecurrentProductRenewalFlag block_recurrent_product_renewal_flag,
       stage_hash_mms_MembershipSnapshot.UndiscountedPrice undiscounted_price,
       stage_hash_mms_MembershipSnapshot.PriorPlusUndiscountedPrice prior_plus_undiscounted_price,
       isnull(cast(stage_hash_mms_MembershipSnapshot.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.OverdueRecurrentProductBalanceFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.BlockRecurrentProductRenewalFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.UndiscountedPrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipSnapshot.PriorPlusUndiscountedPrice as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipSnapshot
 where stage_hash_mms_MembershipSnapshot.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_snapshot_1 records
set @insert_date_time = getdate()
insert into s_mms_membership_snapshot_1 (
       bk_hash,
       membership_id,
       overdue_recurrent_product_balance_flag,
       block_recurrent_product_renewal_flag,
       undiscounted_price,
       prior_plus_undiscounted_price,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_snapshot_1_inserts.bk_hash,
       #s_mms_membership_snapshot_1_inserts.membership_id,
       #s_mms_membership_snapshot_1_inserts.overdue_recurrent_product_balance_flag,
       #s_mms_membership_snapshot_1_inserts.block_recurrent_product_renewal_flag,
       #s_mms_membership_snapshot_1_inserts.undiscounted_price,
       #s_mms_membership_snapshot_1_inserts.prior_plus_undiscounted_price,
       case when s_mms_membership_snapshot_1.s_mms_membership_snapshot_1_id is null then isnull(#s_mms_membership_snapshot_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_snapshot_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_snapshot_1_inserts
  left join p_mms_membership_snapshot
    on #s_mms_membership_snapshot_1_inserts.bk_hash = p_mms_membership_snapshot.bk_hash
   and p_mms_membership_snapshot.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_snapshot_1
    on p_mms_membership_snapshot.bk_hash = s_mms_membership_snapshot_1.bk_hash
   and p_mms_membership_snapshot.s_mms_membership_snapshot_1_id = s_mms_membership_snapshot_1.s_mms_membership_snapshot_1_id
 where s_mms_membership_snapshot_1.s_mms_membership_snapshot_1_id is null
    or (s_mms_membership_snapshot_1.s_mms_membership_snapshot_1_id is not null
        and s_mms_membership_snapshot_1.dv_hash <> #s_mms_membership_snapshot_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_snapshot @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_snapshot_history @current_dv_batch_id

end
