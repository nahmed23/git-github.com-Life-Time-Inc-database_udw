CREATE PROC [dbo].[proc_etl_hybris_paymnt_trnsct_entries] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_paymnttrnsctentries

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_paymnttrnsctentries (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_type,
       p_amount,
       p_currency,
       p_time,
       p_transactionstatus,
       p_transactionstatusdetails,
       p_requesttoken,
       p_requestid,
       p_subscriptionid,
       p_code,
       p_versionid,
       p_paymenttransaction,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_type,
       p_amount,
       p_currency,
       p_time,
       p_transactionstatus,
       p_transactionstatusdetails,
       p_requesttoken,
       p_requestid,
       p_subscriptionid,
       p_code,
       p_versionid,
       p_paymenttransaction,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_paymnttrnsctentries.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_paymnttrnsctentries
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_paymnt_trnsct_entries @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_paymnt_trnsct_entries (
       bk_hash,
       paymnt_trnsct_entries_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_paymnttrnsctentries.bk_hash,
       stage_hash_hybris_paymnttrnsctentries.[PK] paymnt_trnsct_entries_pk,
       isnull(cast(stage_hash_hybris_paymnttrnsctentries.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_paymnttrnsctentries
  left join h_hybris_paymnt_trnsct_entries
    on stage_hash_hybris_paymnttrnsctentries.bk_hash = h_hybris_paymnt_trnsct_entries.bk_hash
 where h_hybris_paymnt_trnsct_entries_id is null
   and stage_hash_hybris_paymnttrnsctentries.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_paymnt_trnsct_entries
if object_id('tempdb..#l_hybris_paymnt_trnsct_entries_inserts') is not null drop table #l_hybris_paymnt_trnsct_entries_inserts
create table #l_hybris_paymnt_trnsct_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymnttrnsctentries.bk_hash,
       stage_hash_hybris_paymnttrnsctentries.TypePkString type_pk_string,
       stage_hash_hybris_paymnttrnsctentries.OwnerPkString owner_pk_string,
       stage_hash_hybris_paymnttrnsctentries.[PK] paymnt_trnsct_entries_pk,
       stage_hash_hybris_paymnttrnsctentries.p_type p_type,
       stage_hash_hybris_paymnttrnsctentries.p_currency p_currency,
       stage_hash_hybris_paymnttrnsctentries.p_requesttoken p_request_token,
       stage_hash_hybris_paymnttrnsctentries.p_requestid p_request_id,
       stage_hash_hybris_paymnttrnsctentries.p_subscriptionid p_subscription_id,
       stage_hash_hybris_paymnttrnsctentries.p_versionid p_version_id,
       stage_hash_hybris_paymnttrnsctentries.p_paymenttransaction p_payment_transaction,
       stage_hash_hybris_paymnttrnsctentries.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.p_type as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.p_currency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_requesttoken,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_requestid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_subscriptionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_versionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.p_paymenttransaction as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymnttrnsctentries
 where stage_hash_hybris_paymnttrnsctentries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_paymnt_trnsct_entries records
set @insert_date_time = getdate()
insert into l_hybris_paymnt_trnsct_entries (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       paymnt_trnsct_entries_pk,
       p_type,
       p_currency,
       p_request_token,
       p_request_id,
       p_subscription_id,
       p_version_id,
       p_payment_transaction,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_paymnt_trnsct_entries_inserts.bk_hash,
       #l_hybris_paymnt_trnsct_entries_inserts.type_pk_string,
       #l_hybris_paymnt_trnsct_entries_inserts.owner_pk_string,
       #l_hybris_paymnt_trnsct_entries_inserts.paymnt_trnsct_entries_pk,
       #l_hybris_paymnt_trnsct_entries_inserts.p_type,
       #l_hybris_paymnt_trnsct_entries_inserts.p_currency,
       #l_hybris_paymnt_trnsct_entries_inserts.p_request_token,
       #l_hybris_paymnt_trnsct_entries_inserts.p_request_id,
       #l_hybris_paymnt_trnsct_entries_inserts.p_subscription_id,
       #l_hybris_paymnt_trnsct_entries_inserts.p_version_id,
       #l_hybris_paymnt_trnsct_entries_inserts.p_payment_transaction,
       case when l_hybris_paymnt_trnsct_entries.l_hybris_paymnt_trnsct_entries_id is null then isnull(#l_hybris_paymnt_trnsct_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_paymnt_trnsct_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_paymnt_trnsct_entries_inserts
  left join p_hybris_paymnt_trnsct_entries
    on #l_hybris_paymnt_trnsct_entries_inserts.bk_hash = p_hybris_paymnt_trnsct_entries.bk_hash
   and p_hybris_paymnt_trnsct_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_paymnt_trnsct_entries
    on p_hybris_paymnt_trnsct_entries.bk_hash = l_hybris_paymnt_trnsct_entries.bk_hash
   and p_hybris_paymnt_trnsct_entries.l_hybris_paymnt_trnsct_entries_id = l_hybris_paymnt_trnsct_entries.l_hybris_paymnt_trnsct_entries_id
 where l_hybris_paymnt_trnsct_entries.l_hybris_paymnt_trnsct_entries_id is null
    or (l_hybris_paymnt_trnsct_entries.l_hybris_paymnt_trnsct_entries_id is not null
        and l_hybris_paymnt_trnsct_entries.dv_hash <> #l_hybris_paymnt_trnsct_entries_inserts.source_hash)

--calculate hash and lookup to current s_hybris_paymnt_trnsct_entries
if object_id('tempdb..#s_hybris_paymnt_trnsct_entries_inserts') is not null drop table #s_hybris_paymnt_trnsct_entries_inserts
create table #s_hybris_paymnt_trnsct_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_paymnttrnsctentries.bk_hash,
       stage_hash_hybris_paymnttrnsctentries.hjmpTS hjmpts,
       stage_hash_hybris_paymnttrnsctentries.createdTS created_ts,
       stage_hash_hybris_paymnttrnsctentries.modifiedTS modified_ts,
       stage_hash_hybris_paymnttrnsctentries.[PK] paymnt_trnsct_entries_pk,
       stage_hash_hybris_paymnttrnsctentries.p_amount p_amount,
       stage_hash_hybris_paymnttrnsctentries.p_time p_time,
       stage_hash_hybris_paymnttrnsctentries.p_transactionstatus p_transaction_status,
       stage_hash_hybris_paymnttrnsctentries.p_transactionstatusdetails p_transaction_status_details,
       stage_hash_hybris_paymnttrnsctentries.p_code p_code,
       stage_hash_hybris_paymnttrnsctentries.aCLTS acl_ts,
       stage_hash_hybris_paymnttrnsctentries.propTS prop_ts,
       stage_hash_hybris_paymnttrnsctentries.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymnttrnsctentries.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymnttrnsctentries.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.p_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_paymnttrnsctentries.p_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_transactionstatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_transactionstatusdetails,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_paymnttrnsctentries.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_paymnttrnsctentries.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_paymnttrnsctentries
 where stage_hash_hybris_paymnttrnsctentries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_paymnt_trnsct_entries records
set @insert_date_time = getdate()
insert into s_hybris_paymnt_trnsct_entries (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       paymnt_trnsct_entries_pk,
       p_amount,
       p_time,
       p_transaction_status,
       p_transaction_status_details,
       p_code,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_paymnt_trnsct_entries_inserts.bk_hash,
       #s_hybris_paymnt_trnsct_entries_inserts.hjmpts,
       #s_hybris_paymnt_trnsct_entries_inserts.created_ts,
       #s_hybris_paymnt_trnsct_entries_inserts.modified_ts,
       #s_hybris_paymnt_trnsct_entries_inserts.paymnt_trnsct_entries_pk,
       #s_hybris_paymnt_trnsct_entries_inserts.p_amount,
       #s_hybris_paymnt_trnsct_entries_inserts.p_time,
       #s_hybris_paymnt_trnsct_entries_inserts.p_transaction_status,
       #s_hybris_paymnt_trnsct_entries_inserts.p_transaction_status_details,
       #s_hybris_paymnt_trnsct_entries_inserts.p_code,
       #s_hybris_paymnt_trnsct_entries_inserts.acl_ts,
       #s_hybris_paymnt_trnsct_entries_inserts.prop_ts,
       case when s_hybris_paymnt_trnsct_entries.s_hybris_paymnt_trnsct_entries_id is null then isnull(#s_hybris_paymnt_trnsct_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_paymnt_trnsct_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_paymnt_trnsct_entries_inserts
  left join p_hybris_paymnt_trnsct_entries
    on #s_hybris_paymnt_trnsct_entries_inserts.bk_hash = p_hybris_paymnt_trnsct_entries.bk_hash
   and p_hybris_paymnt_trnsct_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_paymnt_trnsct_entries
    on p_hybris_paymnt_trnsct_entries.bk_hash = s_hybris_paymnt_trnsct_entries.bk_hash
   and p_hybris_paymnt_trnsct_entries.s_hybris_paymnt_trnsct_entries_id = s_hybris_paymnt_trnsct_entries.s_hybris_paymnt_trnsct_entries_id
 where s_hybris_paymnt_trnsct_entries.s_hybris_paymnt_trnsct_entries_id is null
    or (s_hybris_paymnt_trnsct_entries.s_hybris_paymnt_trnsct_entries_id is not null
        and s_hybris_paymnt_trnsct_entries.dv_hash <> #s_hybris_paymnt_trnsct_entries_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_paymnt_trnsct_entries @current_dv_batch_id

end
