CREATE PROC [dbo].[proc_etl_hybris_payment_infos] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_paymentinfos

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_paymentinfos (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       userpk,
       p_issuenumber,
       p_bank,
       p_ccowner,
       p_subscriptionid,
       p_bankidnumber,
       p_validfrommonth,
       p_saved,
       p_baowner,
       p_number,
       p_validfromyear,
       p_validtoyear,
       p_billingaddress,
       p_accountnumber,
       p_validtomonth,
       code,
       p_type,
       p_mockedflag,
       p_nickname,
       originalpk,
       duplicate,
       p_ltfcriteria,
       p_creditcardtoken,
       p_subscriptionvalidated,
       p_paymentid,
       p_payerid,
       p_token,
       p_payer,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       userpk,
       p_issuenumber,
       p_bank,
       p_ccowner,
       p_subscriptionid,
       p_bankidnumber,
       p_validfrommonth,
       p_saved,
       p_baowner,
       p_number,
       p_validfromyear,
       p_validtoyear,
       p_billingaddress,
       p_accountnumber,
       p_validtomonth,
       code,
       p_type,
       p_mockedflag,
       p_nickname,
       originalpk,
       duplicate,
       p_ltfcriteria,
       p_creditcardtoken,
       p_subscriptionvalidated,
       p_paymentid,
       p_payerid,
       p_token,
       p_payer,
       isnull(cast(stage_hybris_paymentinfos.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_paymentinfos
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_payment_infos @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_payment_infos (
       bk_hash,
       payment_infos_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_paymentinfos.bk_hash,
       stage_hash_hybris_paymentinfos.[PK] payment_infos_pk,
       isnull(cast(stage_hash_hybris_paymentinfos.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_paymentinfos
  left join h_hybris_payment_infos
    on stage_hash_hybris_paymentinfos.bk_hash = h_hybris_payment_infos.bk_hash
 where h_hybris_payment_infos_id is null
   and stage_hash_hybris_paymentinfos.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_payment_infos
if object_id('tempdb..#l_hybris_payment_infos_inserts') is not null drop table #l_hybris_payment_infos_inserts
create table #l_hybris_payment_infos_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymentinfos.bk_hash,
       stage_hash_hybris_paymentinfos.TypePkString type_pk_string,
       stage_hash_hybris_paymentinfos.[PK] payment_infos_pk,
       stage_hash_hybris_paymentinfos.OwnerPkString owner_pk_string,
       stage_hash_hybris_paymentinfos.p_number p_number,
       stage_hash_hybris_paymentinfos.p_billingaddress p_billing_address,
       stage_hash_hybris_paymentinfos.code code,
       stage_hash_hybris_paymentinfos.p_type p_type,
       stage_hash_hybris_paymentinfos.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_number,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_billingaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_type as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymentinfos
 where stage_hash_hybris_paymentinfos.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_payment_infos records
set @insert_date_time = getdate()
insert into l_hybris_payment_infos (
       bk_hash,
       type_pk_string,
       payment_infos_pk,
       owner_pk_string,
       p_number,
       p_billing_address,
       code,
       p_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_payment_infos_inserts.bk_hash,
       #l_hybris_payment_infos_inserts.type_pk_string,
       #l_hybris_payment_infos_inserts.payment_infos_pk,
       #l_hybris_payment_infos_inserts.owner_pk_string,
       #l_hybris_payment_infos_inserts.p_number,
       #l_hybris_payment_infos_inserts.p_billing_address,
       #l_hybris_payment_infos_inserts.code,
       #l_hybris_payment_infos_inserts.p_type,
       case when l_hybris_payment_infos.l_hybris_payment_infos_id is null then isnull(#l_hybris_payment_infos_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_payment_infos_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_payment_infos_inserts
  left join p_hybris_payment_infos
    on #l_hybris_payment_infos_inserts.bk_hash = p_hybris_payment_infos.bk_hash
   and p_hybris_payment_infos.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_payment_infos
    on p_hybris_payment_infos.bk_hash = l_hybris_payment_infos.bk_hash
   and p_hybris_payment_infos.l_hybris_payment_infos_id = l_hybris_payment_infos.l_hybris_payment_infos_id
 where l_hybris_payment_infos.l_hybris_payment_infos_id is null
    or (l_hybris_payment_infos.l_hybris_payment_infos_id is not null
        and l_hybris_payment_infos.dv_hash <> #l_hybris_payment_infos_inserts.source_hash)

--calculate hash and lookup to current s_hybris_payment_infos
if object_id('tempdb..#s_hybris_payment_infos_inserts') is not null drop table #s_hybris_payment_infos_inserts
create table #s_hybris_payment_infos_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymentinfos.bk_hash,
       stage_hash_hybris_paymentinfos.hjmpTS hjmpts,
       stage_hash_hybris_paymentinfos.[PK] payment_infos_pk,
       stage_hash_hybris_paymentinfos.createdTS created_ts,
       stage_hash_hybris_paymentinfos.modifiedTS modified_ts,
       stage_hash_hybris_paymentinfos.aCLTS acl_ts,
       stage_hash_hybris_paymentinfos.propTS prop_ts,
       stage_hash_hybris_paymentinfos.userpk user_pk,
       stage_hash_hybris_paymentinfos.p_issuenumber p_issue_number,
       stage_hash_hybris_paymentinfos.p_bank p_bank,
       stage_hash_hybris_paymentinfos.p_ccowner p_ccowner,
       stage_hash_hybris_paymentinfos.p_subscriptionid p_subscription_id,
       stage_hash_hybris_paymentinfos.p_bankidnumber p_bank_id_number,
       stage_hash_hybris_paymentinfos.p_validfrommonth p_valid_from_month,
       stage_hash_hybris_paymentinfos.p_saved p_saved,
       stage_hash_hybris_paymentinfos.p_baowner p_ba_owner,
       stage_hash_hybris_paymentinfos.p_validfromyear p_valid_from_year,
       stage_hash_hybris_paymentinfos.p_validtoyear p_valid_to_year,
       stage_hash_hybris_paymentinfos.p_accountnumber p_account_number,
       stage_hash_hybris_paymentinfos.p_validtomonth p_valid_to_month,
       stage_hash_hybris_paymentinfos.p_mockedflag p_mocked_flag,
       stage_hash_hybris_paymentinfos.p_nickname p_nick_name,
       stage_hash_hybris_paymentinfos.originalpk original_pk,
       stage_hash_hybris_paymentinfos.duplicate duplicate,
       stage_hash_hybris_paymentinfos.p_ltfcriteria p_ltf_criteria,
       stage_hash_hybris_paymentinfos.p_creditcardtoken p_credit_card_token,
       stage_hash_hybris_paymentinfos.p_subscriptionvalidated p_subscription_validated,
       stage_hash_hybris_paymentinfos.p_paymentid p_payment_id,
       stage_hash_hybris_paymentinfos.p_payerid p_payer_id,
       stage_hash_hybris_paymentinfos.p_token p_token,
       stage_hash_hybris_paymentinfos.p_payer p_payer,
       stage_hash_hybris_paymentinfos.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymentinfos.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymentinfos.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.userpk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_issuenumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_bank,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_ccowner,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_subscriptionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_bankidnumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_validfrommonth,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_saved as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_baowner,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_validfromyear,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_validtoyear,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_accountnumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_validtomonth,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_mockedflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_nickname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.originalpk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.duplicate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_ltfcriteria,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_creditcardtoken,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymentinfos.p_subscriptionvalidated as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_paymentid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_payerid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_token,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymentinfos.p_payer,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymentinfos
 where stage_hash_hybris_paymentinfos.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_payment_infos records
set @insert_date_time = getdate()
insert into s_hybris_payment_infos (
       bk_hash,
       hjmpts,
       payment_infos_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       user_pk,
       p_issue_number,
       p_bank,
       p_ccowner,
       p_subscription_id,
       p_bank_id_number,
       p_valid_from_month,
       p_saved,
       p_ba_owner,
       p_valid_from_year,
       p_valid_to_year,
       p_account_number,
       p_valid_to_month,
       p_mocked_flag,
       p_nick_name,
       original_pk,
       duplicate,
       p_ltf_criteria,
       p_credit_card_token,
       p_subscription_validated,
       p_payment_id,
       p_payer_id,
       p_token,
       p_payer,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_payment_infos_inserts.bk_hash,
       #s_hybris_payment_infos_inserts.hjmpts,
       #s_hybris_payment_infos_inserts.payment_infos_pk,
       #s_hybris_payment_infos_inserts.created_ts,
       #s_hybris_payment_infos_inserts.modified_ts,
       #s_hybris_payment_infos_inserts.acl_ts,
       #s_hybris_payment_infos_inserts.prop_ts,
       #s_hybris_payment_infos_inserts.user_pk,
       #s_hybris_payment_infos_inserts.p_issue_number,
       #s_hybris_payment_infos_inserts.p_bank,
       #s_hybris_payment_infos_inserts.p_ccowner,
       #s_hybris_payment_infos_inserts.p_subscription_id,
       #s_hybris_payment_infos_inserts.p_bank_id_number,
       #s_hybris_payment_infos_inserts.p_valid_from_month,
       #s_hybris_payment_infos_inserts.p_saved,
       #s_hybris_payment_infos_inserts.p_ba_owner,
       #s_hybris_payment_infos_inserts.p_valid_from_year,
       #s_hybris_payment_infos_inserts.p_valid_to_year,
       #s_hybris_payment_infos_inserts.p_account_number,
       #s_hybris_payment_infos_inserts.p_valid_to_month,
       #s_hybris_payment_infos_inserts.p_mocked_flag,
       #s_hybris_payment_infos_inserts.p_nick_name,
       #s_hybris_payment_infos_inserts.original_pk,
       #s_hybris_payment_infos_inserts.duplicate,
       #s_hybris_payment_infos_inserts.p_ltf_criteria,
       #s_hybris_payment_infos_inserts.p_credit_card_token,
       #s_hybris_payment_infos_inserts.p_subscription_validated,
       #s_hybris_payment_infos_inserts.p_payment_id,
       #s_hybris_payment_infos_inserts.p_payer_id,
       #s_hybris_payment_infos_inserts.p_token,
       #s_hybris_payment_infos_inserts.p_payer,
       case when s_hybris_payment_infos.s_hybris_payment_infos_id is null then isnull(#s_hybris_payment_infos_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_payment_infos_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_payment_infos_inserts
  left join p_hybris_payment_infos
    on #s_hybris_payment_infos_inserts.bk_hash = p_hybris_payment_infos.bk_hash
   and p_hybris_payment_infos.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_payment_infos
    on p_hybris_payment_infos.bk_hash = s_hybris_payment_infos.bk_hash
   and p_hybris_payment_infos.s_hybris_payment_infos_id = s_hybris_payment_infos.s_hybris_payment_infos_id
 where s_hybris_payment_infos.s_hybris_payment_infos_id is null
    or (s_hybris_payment_infos.s_hybris_payment_infos_id is not null
        and s_hybris_payment_infos.dv_hash <> #s_hybris_payment_infos_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_payment_infos @current_dv_batch_id

end
