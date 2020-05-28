CREATE PROC [dbo].[proc_etl_mms_third_party_pos_payment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ThirdPartyPOSPayment

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ThirdPartyPOSPayment (
       bk_hash,
       ThirdPartyPOSPaymentID,
       ValPaymentStatusID,
       OfflineAuthFlag,
       LTFTranDateTime,
       UTCLTFTranDateTime,
       LTFTranDateTimeZone,
       POSTranDateTime,
       UTCPOSTranDateTime,
       POSTranDateTimeZone,
       POSUniqueTranID,
       POSUniqueTranIDLabel,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ThirdPartyPOSPaymentID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ThirdPartyPOSPaymentID,
       ValPaymentStatusID,
       OfflineAuthFlag,
       LTFTranDateTime,
       UTCLTFTranDateTime,
       LTFTranDateTimeZone,
       POSTranDateTime,
       UTCPOSTranDateTime,
       POSTranDateTimeZone,
       POSUniqueTranID,
       POSUniqueTranIDLabel,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ThirdPartyPOSPayment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ThirdPartyPOSPayment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_third_party_pos_payment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_third_party_pos_payment (
       bk_hash,
       third_party_pos_payment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ThirdPartyPOSPayment.bk_hash,
       stage_hash_mms_ThirdPartyPOSPayment.ThirdPartyPOSPaymentID third_party_pos_payment_id,
       isnull(cast(stage_hash_mms_ThirdPartyPOSPayment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ThirdPartyPOSPayment
  left join h_mms_third_party_pos_payment
    on stage_hash_mms_ThirdPartyPOSPayment.bk_hash = h_mms_third_party_pos_payment.bk_hash
 where h_mms_third_party_pos_payment_id is null
   and stage_hash_mms_ThirdPartyPOSPayment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_third_party_pos_payment
if object_id('tempdb..#l_mms_third_party_pos_payment_inserts') is not null drop table #l_mms_third_party_pos_payment_inserts
create table #l_mms_third_party_pos_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ThirdPartyPOSPayment.bk_hash,
       stage_hash_mms_ThirdPartyPOSPayment.ThirdPartyPOSPaymentID third_party_pos_payment_id,
       stage_hash_mms_ThirdPartyPOSPayment.ValPaymentStatusID val_payment_status_id,
       stage_hash_mms_ThirdPartyPOSPayment.POSUniqueTranID pos_unique_tran_id,
       stage_hash_mms_ThirdPartyPOSPayment.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ThirdPartyPOSPayment.ThirdPartyPOSPaymentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ThirdPartyPOSPayment.ValPaymentStatusID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ThirdPartyPOSPayment.POSUniqueTranID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ThirdPartyPOSPayment
 where stage_hash_mms_ThirdPartyPOSPayment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_third_party_pos_payment records
set @insert_date_time = getdate()
insert into l_mms_third_party_pos_payment (
       bk_hash,
       third_party_pos_payment_id,
       val_payment_status_id,
       pos_unique_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_third_party_pos_payment_inserts.bk_hash,
       #l_mms_third_party_pos_payment_inserts.third_party_pos_payment_id,
       #l_mms_third_party_pos_payment_inserts.val_payment_status_id,
       #l_mms_third_party_pos_payment_inserts.pos_unique_tran_id,
       case when l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id is null then isnull(#l_mms_third_party_pos_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_third_party_pos_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_third_party_pos_payment_inserts
  left join p_mms_third_party_pos_payment
    on #l_mms_third_party_pos_payment_inserts.bk_hash = p_mms_third_party_pos_payment.bk_hash
   and p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_third_party_pos_payment
    on p_mms_third_party_pos_payment.bk_hash = l_mms_third_party_pos_payment.bk_hash
   and p_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id = l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id
 where l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id is null
    or (l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id is not null
        and l_mms_third_party_pos_payment.dv_hash <> #l_mms_third_party_pos_payment_inserts.source_hash)

--calculate hash and lookup to current s_mms_third_party_pos_payment
if object_id('tempdb..#s_mms_third_party_pos_payment_inserts') is not null drop table #s_mms_third_party_pos_payment_inserts
create table #s_mms_third_party_pos_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ThirdPartyPOSPayment.bk_hash,
       stage_hash_mms_ThirdPartyPOSPayment.ThirdPartyPOSPaymentID third_party_pos_payment_id,
       stage_hash_mms_ThirdPartyPOSPayment.OfflineAuthFlag offline_auth_flag,
       stage_hash_mms_ThirdPartyPOSPayment.LTFTranDateTime ltf_tran_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.UTCLTFTranDateTime utc_ltf_tran_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.LTFTranDateTimeZone ltf_tran_date_time_zone,
       stage_hash_mms_ThirdPartyPOSPayment.POSTranDateTime pos_tran_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.UTCPOSTranDateTime utc_pos_tran_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.POSTranDateTimeZone pos_tran_date_time_zone,
       stage_hash_mms_ThirdPartyPOSPayment.POSUniqueTranIDLabel pos_unique_tran_id_label,
       stage_hash_mms_ThirdPartyPOSPayment.InsertedDateTime inserted_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.UpdatedDateTime updated_date_time,
       stage_hash_mms_ThirdPartyPOSPayment.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ThirdPartyPOSPayment.ThirdPartyPOSPaymentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ThirdPartyPOSPayment.OfflineAuthFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.LTFTranDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.UTCLTFTranDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ThirdPartyPOSPayment.LTFTranDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.POSTranDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.UTCPOSTranDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ThirdPartyPOSPayment.POSTranDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ThirdPartyPOSPayment.POSUniqueTranIDLabel,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ThirdPartyPOSPayment.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ThirdPartyPOSPayment
 where stage_hash_mms_ThirdPartyPOSPayment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_third_party_pos_payment records
set @insert_date_time = getdate()
insert into s_mms_third_party_pos_payment (
       bk_hash,
       third_party_pos_payment_id,
       offline_auth_flag,
       ltf_tran_date_time,
       utc_ltf_tran_date_time,
       ltf_tran_date_time_zone,
       pos_tran_date_time,
       utc_pos_tran_date_time,
       pos_tran_date_time_zone,
       pos_unique_tran_id_label,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_third_party_pos_payment_inserts.bk_hash,
       #s_mms_third_party_pos_payment_inserts.third_party_pos_payment_id,
       #s_mms_third_party_pos_payment_inserts.offline_auth_flag,
       #s_mms_third_party_pos_payment_inserts.ltf_tran_date_time,
       #s_mms_third_party_pos_payment_inserts.utc_ltf_tran_date_time,
       #s_mms_third_party_pos_payment_inserts.ltf_tran_date_time_zone,
       #s_mms_third_party_pos_payment_inserts.pos_tran_date_time,
       #s_mms_third_party_pos_payment_inserts.utc_pos_tran_date_time,
       #s_mms_third_party_pos_payment_inserts.pos_tran_date_time_zone,
       #s_mms_third_party_pos_payment_inserts.pos_unique_tran_id_label,
       #s_mms_third_party_pos_payment_inserts.inserted_date_time,
       #s_mms_third_party_pos_payment_inserts.updated_date_time,
       case when s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id is null then isnull(#s_mms_third_party_pos_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_third_party_pos_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_third_party_pos_payment_inserts
  left join p_mms_third_party_pos_payment
    on #s_mms_third_party_pos_payment_inserts.bk_hash = p_mms_third_party_pos_payment.bk_hash
   and p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_third_party_pos_payment
    on p_mms_third_party_pos_payment.bk_hash = s_mms_third_party_pos_payment.bk_hash
   and p_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id = s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id
 where s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id is null
    or (s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id is not null
        and s_mms_third_party_pos_payment.dv_hash <> #s_mms_third_party_pos_payment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_third_party_pos_payment @current_dv_batch_id

end
