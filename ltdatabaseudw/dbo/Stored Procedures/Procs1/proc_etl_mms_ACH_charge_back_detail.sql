CREATE PROC [dbo].[proc_etl_mms_ACH_charge_back_detail] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ACHChargeBackDetail

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ACHChargeBackDetail (
       bk_hash,
       RegionDescription,
       ClubName,
       MemberID,
       FirstName,
       LastName,
       ACH_CC,
       PaymentTypeDescription,
       EFTDate,
       ReasonCodeDescription,
       MembershipType_ProductDescription,
       ReturnCodeDescription,
       EFTReturn_StopEFTFlag,
       EFTReturn_RoutingNumber,
       EFTReturn_AccountNumber,
       EFTReturn_AccountExpirationDate,
       MembershipPhone,
       EmailAddress,
       ChargeBack_PostDateTime,
       ChargeBack_MembershipEFTOptionDescription,
       ChargeBack_MMSTranID,
       ChargeBack_TranAmount,
       LocalCurrency_ChargeBack_TranAmount,
       USD_ChargeBack_TranAmount,
       LocalCurrencyCode,
       PlanRate,
       ReportingCurrencyCode,
       EFTReturn_EFTAmount,
       Membership_CurrentBalance,
       LocalCurrency_EFTReturn_EFTAmount,
       LocalCurrency_Membership_CurrentBalance,
       USD_EFTReturn_EFTAmount,
       USD_Membership_CurrentBalance,
       HeaderReturnType,
       HeaderDateRange,
       ReportRunDateTime,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ChargeBack_MMSTranID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       RegionDescription,
       ClubName,
       MemberID,
       FirstName,
       LastName,
       ACH_CC,
       PaymentTypeDescription,
       EFTDate,
       ReasonCodeDescription,
       MembershipType_ProductDescription,
       ReturnCodeDescription,
       EFTReturn_StopEFTFlag,
       EFTReturn_RoutingNumber,
       EFTReturn_AccountNumber,
       EFTReturn_AccountExpirationDate,
       MembershipPhone,
       EmailAddress,
       ChargeBack_PostDateTime,
       ChargeBack_MembershipEFTOptionDescription,
       ChargeBack_MMSTranID,
       ChargeBack_TranAmount,
       LocalCurrency_ChargeBack_TranAmount,
       USD_ChargeBack_TranAmount,
       LocalCurrencyCode,
       PlanRate,
       ReportingCurrencyCode,
       EFTReturn_EFTAmount,
       Membership_CurrentBalance,
       LocalCurrency_EFTReturn_EFTAmount,
       LocalCurrency_Membership_CurrentBalance,
       USD_EFTReturn_EFTAmount,
       USD_Membership_CurrentBalance,
       HeaderReturnType,
       HeaderDateRange,
       ReportRunDateTime,
       jan_one,
       isnull(cast(stage_mms_ACHChargeBackDetail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ACHChargeBackDetail
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_ACH_charge_back_detail @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_ACH_charge_back_detail (
       bk_hash,
       charge_back_mms_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ACHChargeBackDetail.bk_hash,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_MMSTranID charge_back_mms_tran_id,
       isnull(cast(stage_hash_mms_ACHChargeBackDetail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ACHChargeBackDetail
  left join h_mms_ACH_charge_back_detail
    on stage_hash_mms_ACHChargeBackDetail.bk_hash = h_mms_ACH_charge_back_detail.bk_hash
 where h_mms_ACH_charge_back_detail_id is null
   and stage_hash_mms_ACHChargeBackDetail.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_ACH_charge_back_detail
if object_id('tempdb..#l_mms_ACH_charge_back_detail_inserts') is not null drop table #l_mms_ACH_charge_back_detail_inserts
create table #l_mms_ACH_charge_back_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ACHChargeBackDetail.bk_hash,
       stage_hash_mms_ACHChargeBackDetail.MemberID member_id,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_MMSTranID charge_back_mms_tran_id,
       isnull(cast(stage_hash_mms_ACHChargeBackDetail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.ChargeBack_MMSTranID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ACHChargeBackDetail
 where stage_hash_mms_ACHChargeBackDetail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_ACH_charge_back_detail records
set @insert_date_time = getdate()
insert into l_mms_ACH_charge_back_detail (
       bk_hash,
       member_id,
       charge_back_mms_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_ACH_charge_back_detail_inserts.bk_hash,
       #l_mms_ACH_charge_back_detail_inserts.member_id,
       #l_mms_ACH_charge_back_detail_inserts.charge_back_mms_tran_id,
       case when l_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id is null then isnull(#l_mms_ACH_charge_back_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_ACH_charge_back_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_ACH_charge_back_detail_inserts
  left join p_mms_ACH_charge_back_detail
    on #l_mms_ACH_charge_back_detail_inserts.bk_hash = p_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_ACH_charge_back_detail
    on p_mms_ACH_charge_back_detail.bk_hash = l_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id = l_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id
 where l_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id is null
    or (l_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id is not null
        and l_mms_ACH_charge_back_detail.dv_hash <> #l_mms_ACH_charge_back_detail_inserts.source_hash)

--calculate hash and lookup to current s_mms_ACH_charge_back_detail
if object_id('tempdb..#s_mms_ACH_charge_back_detail_inserts') is not null drop table #s_mms_ACH_charge_back_detail_inserts
create table #s_mms_ACH_charge_back_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ACHChargeBackDetail.bk_hash,
       stage_hash_mms_ACHChargeBackDetail.RegionDescription region_description,
       stage_hash_mms_ACHChargeBackDetail.ClubName club_name,
       stage_hash_mms_ACHChargeBackDetail.FirstName first_name,
       stage_hash_mms_ACHChargeBackDetail.LastName last_name,
       stage_hash_mms_ACHChargeBackDetail.ACH_CC ach_cc,
       stage_hash_mms_ACHChargeBackDetail.PaymentTypeDescription payment_type_description,
       stage_hash_mms_ACHChargeBackDetail.EFTDate eft_date,
       stage_hash_mms_ACHChargeBackDetail.ReasonCodeDescription reason_code_description,
       stage_hash_mms_ACHChargeBackDetail.MembershipType_ProductDescription membership_type_product_description,
       stage_hash_mms_ACHChargeBackDetail.ReturnCodeDescription return_code_description,
       stage_hash_mms_ACHChargeBackDetail.EFTReturn_StopEFTFlag eft_return_stop_eft_flag,
       stage_hash_mms_ACHChargeBackDetail.EFTReturn_RoutingNumber eft_return_routing_number,
       stage_hash_mms_ACHChargeBackDetail.EFTReturn_AccountNumber eft_return_account_number,
       stage_hash_mms_ACHChargeBackDetail.EFTReturn_AccountExpirationDate eft_return_account_expiration_date,
       stage_hash_mms_ACHChargeBackDetail.MembershipPhone membership_phone,
       stage_hash_mms_ACHChargeBackDetail.EmailAddress email_address,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_PostDateTime charge_back_post_date_time,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_MembershipEFTOptionDescription charge_back_membership_eft_option_description,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_MMSTranID charge_back_mms_tran_id,
       stage_hash_mms_ACHChargeBackDetail.ChargeBack_TranAmount charge_back_tran_amount,
       stage_hash_mms_ACHChargeBackDetail.LocalCurrency_ChargeBack_TranAmount local_currency_charge_back_tran_amount,
       stage_hash_mms_ACHChargeBackDetail.USD_ChargeBack_TranAmount usd_charge_back_tran_amount,
       stage_hash_mms_ACHChargeBackDetail.LocalCurrencyCode local_currency_code,
       stage_hash_mms_ACHChargeBackDetail.PlanRate plan_rate,
       stage_hash_mms_ACHChargeBackDetail.ReportingCurrencyCode reporting_currency_code,
       stage_hash_mms_ACHChargeBackDetail.EFTReturn_EFTAmount eft_return_eft_amount,
       stage_hash_mms_ACHChargeBackDetail.Membership_CurrentBalance membership_current_balance,
       stage_hash_mms_ACHChargeBackDetail.LocalCurrency_EFTReturn_EFTAmount local_currency_eft_return_eft_amount,
       stage_hash_mms_ACHChargeBackDetail.LocalCurrency_Membership_CurrentBalance local_currency_membership_current_balance,
       stage_hash_mms_ACHChargeBackDetail.USD_EFTReturn_EFTAmount usd_eft_return_eft_amount,
       stage_hash_mms_ACHChargeBackDetail.USD_Membership_CurrentBalance usd_membership_current_balance,
       stage_hash_mms_ACHChargeBackDetail.HeaderReturnType header_return_type,
       stage_hash_mms_ACHChargeBackDetail.HeaderDateRange header_date_range,
       stage_hash_mms_ACHChargeBackDetail.ReportRunDateTime report_run_date_time,
       stage_hash_mms_ACHChargeBackDetail.jan_one jan_one,
       isnull(cast(stage_hash_mms_ACHChargeBackDetail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.RegionDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ClubName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ACH_CC,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.PaymentTypeDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EFTDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ReasonCodeDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.MembershipType_ProductDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ReturnCodeDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EFTReturn_StopEFTFlag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EFTReturn_RoutingNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EFTReturn_AccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EFTReturn_AccountExpirationDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.MembershipPhone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ChargeBack_PostDateTime,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ChargeBack_MembershipEFTOptionDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.ChargeBack_MMSTranID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.ChargeBack_TranAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.LocalCurrency_ChargeBack_TranAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.USD_ChargeBack_TranAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.LocalCurrencyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.PlanRate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ReportingCurrencyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.EFTReturn_EFTAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.Membership_CurrentBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.LocalCurrency_EFTReturn_EFTAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.LocalCurrency_Membership_CurrentBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.USD_EFTReturn_EFTAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ACHChargeBackDetail.USD_Membership_CurrentBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.HeaderReturnType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.HeaderDateRange,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ACHChargeBackDetail.ReportRunDateTime,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ACHChargeBackDetail.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ACHChargeBackDetail
 where stage_hash_mms_ACHChargeBackDetail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_ACH_charge_back_detail records
set @insert_date_time = getdate()
insert into s_mms_ACH_charge_back_detail (
       bk_hash,
       region_description,
       club_name,
       first_name,
       last_name,
       ach_cc,
       payment_type_description,
       eft_date,
       reason_code_description,
       membership_type_product_description,
       return_code_description,
       eft_return_stop_eft_flag,
       eft_return_routing_number,
       eft_return_account_number,
       eft_return_account_expiration_date,
       membership_phone,
       email_address,
       charge_back_post_date_time,
       charge_back_membership_eft_option_description,
       charge_back_mms_tran_id,
       charge_back_tran_amount,
       local_currency_charge_back_tran_amount,
       usd_charge_back_tran_amount,
       local_currency_code,
       plan_rate,
       reporting_currency_code,
       eft_return_eft_amount,
       membership_current_balance,
       local_currency_eft_return_eft_amount,
       local_currency_membership_current_balance,
       usd_eft_return_eft_amount,
       usd_membership_current_balance,
       header_return_type,
       header_date_range,
       report_run_date_time,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_ACH_charge_back_detail_inserts.bk_hash,
       #s_mms_ACH_charge_back_detail_inserts.region_description,
       #s_mms_ACH_charge_back_detail_inserts.club_name,
       #s_mms_ACH_charge_back_detail_inserts.first_name,
       #s_mms_ACH_charge_back_detail_inserts.last_name,
       #s_mms_ACH_charge_back_detail_inserts.ach_cc,
       #s_mms_ACH_charge_back_detail_inserts.payment_type_description,
       #s_mms_ACH_charge_back_detail_inserts.eft_date,
       #s_mms_ACH_charge_back_detail_inserts.reason_code_description,
       #s_mms_ACH_charge_back_detail_inserts.membership_type_product_description,
       #s_mms_ACH_charge_back_detail_inserts.return_code_description,
       #s_mms_ACH_charge_back_detail_inserts.eft_return_stop_eft_flag,
       #s_mms_ACH_charge_back_detail_inserts.eft_return_routing_number,
       #s_mms_ACH_charge_back_detail_inserts.eft_return_account_number,
       #s_mms_ACH_charge_back_detail_inserts.eft_return_account_expiration_date,
       #s_mms_ACH_charge_back_detail_inserts.membership_phone,
       #s_mms_ACH_charge_back_detail_inserts.email_address,
       #s_mms_ACH_charge_back_detail_inserts.charge_back_post_date_time,
       #s_mms_ACH_charge_back_detail_inserts.charge_back_membership_eft_option_description,
       #s_mms_ACH_charge_back_detail_inserts.charge_back_mms_tran_id,
       #s_mms_ACH_charge_back_detail_inserts.charge_back_tran_amount,
       #s_mms_ACH_charge_back_detail_inserts.local_currency_charge_back_tran_amount,
       #s_mms_ACH_charge_back_detail_inserts.usd_charge_back_tran_amount,
       #s_mms_ACH_charge_back_detail_inserts.local_currency_code,
       #s_mms_ACH_charge_back_detail_inserts.plan_rate,
       #s_mms_ACH_charge_back_detail_inserts.reporting_currency_code,
       #s_mms_ACH_charge_back_detail_inserts.eft_return_eft_amount,
       #s_mms_ACH_charge_back_detail_inserts.membership_current_balance,
       #s_mms_ACH_charge_back_detail_inserts.local_currency_eft_return_eft_amount,
       #s_mms_ACH_charge_back_detail_inserts.local_currency_membership_current_balance,
       #s_mms_ACH_charge_back_detail_inserts.usd_eft_return_eft_amount,
       #s_mms_ACH_charge_back_detail_inserts.usd_membership_current_balance,
       #s_mms_ACH_charge_back_detail_inserts.header_return_type,
       #s_mms_ACH_charge_back_detail_inserts.header_date_range,
       #s_mms_ACH_charge_back_detail_inserts.report_run_date_time,
       #s_mms_ACH_charge_back_detail_inserts.jan_one,
       case when s_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id is null then isnull(#s_mms_ACH_charge_back_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_ACH_charge_back_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_ACH_charge_back_detail_inserts
  left join p_mms_ACH_charge_back_detail
    on #s_mms_ACH_charge_back_detail_inserts.bk_hash = p_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_ACH_charge_back_detail
    on p_mms_ACH_charge_back_detail.bk_hash = s_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id = s_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id
 where s_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id is null
    or (s_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id is not null
        and s_mms_ACH_charge_back_detail.dv_hash <> #s_mms_ACH_charge_back_detail_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_ACH_charge_back_detail @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_ACH_charge_back_detail @current_dv_batch_id

--run fact load procs
exec dbo.proc_fact_mms_ACH_charge_back_detail @current_dv_batch_id

end
