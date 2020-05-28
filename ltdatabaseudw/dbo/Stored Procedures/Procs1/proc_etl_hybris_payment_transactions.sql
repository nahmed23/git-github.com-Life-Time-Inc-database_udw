CREATE PROC [dbo].[proc_etl_hybris_payment_transactions] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_paymenttransactions

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_paymenttransactions (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_versionid,
       p_code,
       p_currency,
       p_requestid,
       p_order,
       p_paymentprovider,
       p_requesttoken,
       p_info,
       p_plannedamount,
       p_autherrorcode,
       p_kountresponsecode,
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
       p_versionid,
       p_code,
       p_currency,
       p_requestid,
       p_order,
       p_paymentprovider,
       p_requesttoken,
       p_info,
       p_plannedamount,
       p_autherrorcode,
       p_kountresponsecode,
       isnull(cast(stage_hybris_paymenttransactions.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_paymenttransactions
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_payment_transactions @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_payment_transactions (
       bk_hash,
       payment_transactions_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_paymenttransactions.bk_hash,
       stage_hash_hybris_paymenttransactions.[PK] payment_transactions_pk,
       isnull(cast(stage_hash_hybris_paymenttransactions.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_paymenttransactions
  left join h_hybris_payment_transactions
    on stage_hash_hybris_paymenttransactions.bk_hash = h_hybris_payment_transactions.bk_hash
 where h_hybris_payment_transactions_id is null
   and stage_hash_hybris_paymenttransactions.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_payment_transactions
if object_id('tempdb..#l_hybris_payment_transactions_inserts') is not null drop table #l_hybris_payment_transactions_inserts
create table #l_hybris_payment_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymenttransactions.bk_hash,
       stage_hash_hybris_paymenttransactions.TypePkString type_pk_string,
       stage_hash_hybris_paymenttransactions.[PK] payment_transactions_pk,
       stage_hash_hybris_paymenttransactions.OwnerPkString owner_pk_string,
       stage_hash_hybris_paymenttransactions.p_versionid p_version_id,
       stage_hash_hybris_paymenttransactions.p_code p_code,
       stage_hash_hybris_paymenttransactions.p_requestid p_request_id,
       stage_hash_hybris_paymenttransactions.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_versionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_requestid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymenttransactions
 where stage_hash_hybris_paymenttransactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_payment_transactions records
set @insert_date_time = getdate()
insert into l_hybris_payment_transactions (
       bk_hash,
       type_pk_string,
       payment_transactions_pk,
       owner_pk_string,
       p_version_id,
       p_code,
       p_request_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_payment_transactions_inserts.bk_hash,
       #l_hybris_payment_transactions_inserts.type_pk_string,
       #l_hybris_payment_transactions_inserts.payment_transactions_pk,
       #l_hybris_payment_transactions_inserts.owner_pk_string,
       #l_hybris_payment_transactions_inserts.p_version_id,
       #l_hybris_payment_transactions_inserts.p_code,
       #l_hybris_payment_transactions_inserts.p_request_id,
       case when l_hybris_payment_transactions.l_hybris_payment_transactions_id is null then isnull(#l_hybris_payment_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_payment_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_payment_transactions_inserts
  left join p_hybris_payment_transactions
    on #l_hybris_payment_transactions_inserts.bk_hash = p_hybris_payment_transactions.bk_hash
   and p_hybris_payment_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_payment_transactions
    on p_hybris_payment_transactions.bk_hash = l_hybris_payment_transactions.bk_hash
   and p_hybris_payment_transactions.l_hybris_payment_transactions_id = l_hybris_payment_transactions.l_hybris_payment_transactions_id
 where l_hybris_payment_transactions.l_hybris_payment_transactions_id is null
    or (l_hybris_payment_transactions.l_hybris_payment_transactions_id is not null
        and l_hybris_payment_transactions.dv_hash <> #l_hybris_payment_transactions_inserts.source_hash)

--calculate hash and lookup to current s_hybris_payment_transactions
if object_id('tempdb..#s_hybris_payment_transactions_inserts') is not null drop table #s_hybris_payment_transactions_inserts
create table #s_hybris_payment_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymenttransactions.bk_hash,
       stage_hash_hybris_paymenttransactions.hjmpTS hjmpts,
       stage_hash_hybris_paymenttransactions.[PK] payment_transactions_pk,
       stage_hash_hybris_paymenttransactions.createdTS created_ts,
       stage_hash_hybris_paymenttransactions.modifiedTS modified_ts,
       stage_hash_hybris_paymenttransactions.aCLTS acl_ts,
       stage_hash_hybris_paymenttransactions.propTS prop_ts,
       stage_hash_hybris_paymenttransactions.p_currency p_currency,
       stage_hash_hybris_paymenttransactions.p_order p_order,
       stage_hash_hybris_paymenttransactions.p_paymentprovider p_payment_provider,
       stage_hash_hybris_paymenttransactions.p_requesttoken p_request_token,
       stage_hash_hybris_paymenttransactions.p_info p_info,
       stage_hash_hybris_paymenttransactions.p_plannedamount p_planned_amount,
       stage_hash_hybris_paymenttransactions.p_autherrorcode p_auth_error_code,
       stage_hash_hybris_paymenttransactions.p_kountresponsecode p_kount_response_code,
       stage_hash_hybris_paymenttransactions.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymenttransactions.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymenttransactions.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.p_currency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.p_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_paymentprovider,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_requesttoken,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.p_info as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymenttransactions.p_plannedamount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_autherrorcode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymenttransactions.p_kountresponsecode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymenttransactions
 where stage_hash_hybris_paymenttransactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_payment_transactions records
set @insert_date_time = getdate()
insert into s_hybris_payment_transactions (
       bk_hash,
       hjmpts,
       payment_transactions_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_currency,
       p_order,
       p_payment_provider,
       p_request_token,
       p_info,
       p_planned_amount,
       p_auth_error_code,
       p_kount_response_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_payment_transactions_inserts.bk_hash,
       #s_hybris_payment_transactions_inserts.hjmpts,
       #s_hybris_payment_transactions_inserts.payment_transactions_pk,
       #s_hybris_payment_transactions_inserts.created_ts,
       #s_hybris_payment_transactions_inserts.modified_ts,
       #s_hybris_payment_transactions_inserts.acl_ts,
       #s_hybris_payment_transactions_inserts.prop_ts,
       #s_hybris_payment_transactions_inserts.p_currency,
       #s_hybris_payment_transactions_inserts.p_order,
       #s_hybris_payment_transactions_inserts.p_payment_provider,
       #s_hybris_payment_transactions_inserts.p_request_token,
       #s_hybris_payment_transactions_inserts.p_info,
       #s_hybris_payment_transactions_inserts.p_planned_amount,
       #s_hybris_payment_transactions_inserts.p_auth_error_code,
       #s_hybris_payment_transactions_inserts.p_kount_response_code,
       case when s_hybris_payment_transactions.s_hybris_payment_transactions_id is null then isnull(#s_hybris_payment_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_payment_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_payment_transactions_inserts
  left join p_hybris_payment_transactions
    on #s_hybris_payment_transactions_inserts.bk_hash = p_hybris_payment_transactions.bk_hash
   and p_hybris_payment_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_payment_transactions
    on p_hybris_payment_transactions.bk_hash = s_hybris_payment_transactions.bk_hash
   and p_hybris_payment_transactions.s_hybris_payment_transactions_id = s_hybris_payment_transactions.s_hybris_payment_transactions_id
 where s_hybris_payment_transactions.s_hybris_payment_transactions_id is null
    or (s_hybris_payment_transactions.s_hybris_payment_transactions_id is not null
        and s_hybris_payment_transactions.dv_hash <> #s_hybris_payment_transactions_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_payment_transactions @current_dv_batch_id

end
