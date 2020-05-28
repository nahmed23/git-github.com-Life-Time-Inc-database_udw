CREATE PROC [dbo].[proc_etl_orbital_transaction_data_report] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_orbital_transaction_data_report

set @insert_date_time = getdate()
insert into dbo.stage_hash_orbital_transaction_data_report (
       bk_hash,
       RecordType,
       MerchantNumber,
       Filler,
       BatchNumber,
       TransactionSequenceNumber,
       TranCode,
       CardholderNumber,
       OriginalReferenceNumber,
       Amount,
       AuthorizationCode,
       TransactionDate,
       MOPCode,
       MnemonicCode,
       RejectReasonCode,
       TranType,
       EntryMode,
       ServiceLevel,
       Filler1,
       TransactionId,
       ValidationCode,
       DowngradeReason,
       ProcessDate,
       CustomerDefinedData,
       ChasePayIndicator,
       DigitalTokenMethod,
       PIDCode,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(OriginalReferenceNumber,'z#@$k%&P')+'P%#&z$@k'+isnull(MerchantNumber,'z#@$k%&P')+'P%#&z$@k'+isnull(BatchNumber,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(TransactionSequenceNumber as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(TransactionId,'z#@$k%&P'))),2) bk_hash,
       RecordType,
       MerchantNumber,
       Filler,
       BatchNumber,
       TransactionSequenceNumber,
       TranCode,
       CardholderNumber,
       OriginalReferenceNumber,
       Amount,
       AuthorizationCode,
       TransactionDate,
       MOPCode,
       MnemonicCode,
       RejectReasonCode,
       TranType,
       EntryMode,
       ServiceLevel,
       Filler1,
       TransactionId,
       ValidationCode,
       DowngradeReason,
       ProcessDate,
       CustomerDefinedData,
       ChasePayIndicator,
       DigitalTokenMethod,
       PIDCode,
       dummy_modified_date_time,
       isnull(cast(stage_orbital_transaction_data_report.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_orbital_transaction_data_report
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_orbital_transaction_data_report @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_orbital_transaction_data_report (
       bk_hash,
       OriginalReferenceNumber,
       MerchantNumber,
       BatchNumber,
       TransactionSequenceNumber,
       TransactionId,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_orbital_transaction_data_report.bk_hash,
       stage_hash_orbital_transaction_data_report.OriginalReferenceNumber OriginalReferenceNumber,
       stage_hash_orbital_transaction_data_report.MerchantNumber MerchantNumber,
       stage_hash_orbital_transaction_data_report.BatchNumber BatchNumber,
       stage_hash_orbital_transaction_data_report.TransactionSequenceNumber TransactionSequenceNumber,
       stage_hash_orbital_transaction_data_report.TransactionId TransactionId,
       isnull(cast(stage_hash_orbital_transaction_data_report.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_orbital_transaction_data_report
  left join h_orbital_transaction_data_report
    on stage_hash_orbital_transaction_data_report.bk_hash = h_orbital_transaction_data_report.bk_hash
 where h_orbital_transaction_data_report_id is null
   and stage_hash_orbital_transaction_data_report.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_orbital_transaction_data_report
if object_id('tempdb..#l_orbital_transaction_data_report_inserts') is not null drop table #l_orbital_transaction_data_report_inserts
create table #l_orbital_transaction_data_report_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_orbital_transaction_data_report.bk_hash,
       stage_hash_orbital_transaction_data_report.OriginalReferenceNumber OriginalReferenceNumber,
       stage_hash_orbital_transaction_data_report.MerchantNumber MerchantNumber,
       stage_hash_orbital_transaction_data_report.BatchNumber BatchNumber,
       stage_hash_orbital_transaction_data_report.TransactionSequenceNumber TransactionSequenceNumber,
       stage_hash_orbital_transaction_data_report.TransactionId TransactionId,
       isnull(cast(stage_hash_orbital_transaction_data_report.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.OriginalReferenceNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.MerchantNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.BatchNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_orbital_transaction_data_report.TransactionSequenceNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.TransactionId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_orbital_transaction_data_report
 where stage_hash_orbital_transaction_data_report.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_orbital_transaction_data_report records
set @insert_date_time = getdate()
insert into l_orbital_transaction_data_report (
       bk_hash,
       OriginalReferenceNumber,
       MerchantNumber,
       BatchNumber,
       TransactionSequenceNumber,
       TransactionId,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_orbital_transaction_data_report_inserts.bk_hash,
       #l_orbital_transaction_data_report_inserts.OriginalReferenceNumber,
       #l_orbital_transaction_data_report_inserts.MerchantNumber,
       #l_orbital_transaction_data_report_inserts.BatchNumber,
       #l_orbital_transaction_data_report_inserts.TransactionSequenceNumber,
       #l_orbital_transaction_data_report_inserts.TransactionId,
       case when l_orbital_transaction_data_report.l_orbital_transaction_data_report_id is null then isnull(#l_orbital_transaction_data_report_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #l_orbital_transaction_data_report_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_orbital_transaction_data_report_inserts
  left join p_orbital_transaction_data_report
    on #l_orbital_transaction_data_report_inserts.bk_hash = p_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_orbital_transaction_data_report
    on p_orbital_transaction_data_report.bk_hash = l_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.l_orbital_transaction_data_report_id = l_orbital_transaction_data_report.l_orbital_transaction_data_report_id
 where l_orbital_transaction_data_report.l_orbital_transaction_data_report_id is null
    or (l_orbital_transaction_data_report.l_orbital_transaction_data_report_id is not null
        and l_orbital_transaction_data_report.dv_hash <> #l_orbital_transaction_data_report_inserts.source_hash)

--calculate hash and lookup to current s_orbital_transaction_data_report
if object_id('tempdb..#s_orbital_transaction_data_report_inserts') is not null drop table #s_orbital_transaction_data_report_inserts
create table #s_orbital_transaction_data_report_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_orbital_transaction_data_report.bk_hash,
       stage_hash_orbital_transaction_data_report.OriginalReferenceNumber OriginalReferenceNumber,
       stage_hash_orbital_transaction_data_report.MerchantNumber MerchantNumber,
       stage_hash_orbital_transaction_data_report.BatchNumber BatchNumber,
       stage_hash_orbital_transaction_data_report.TransactionSequenceNumber TransactionSequenceNumber,
       stage_hash_orbital_transaction_data_report.TransactionId TransactionId,
       stage_hash_orbital_transaction_data_report.Filler Filler,
       stage_hash_orbital_transaction_data_report.TranCode TranCode,
       stage_hash_orbital_transaction_data_report.CardholderNumber CardholderNumber,
       stage_hash_orbital_transaction_data_report.Amount Amount,
       stage_hash_orbital_transaction_data_report.AuthorizationCode AuthorizationCode,
       stage_hash_orbital_transaction_data_report.TransactionDate TransactionDate,
       stage_hash_orbital_transaction_data_report.MOPCode MOPCode,
       stage_hash_orbital_transaction_data_report.MnemonicCode MnemonicCode,
       stage_hash_orbital_transaction_data_report.RejectReasonCode RejectReasonCode,
       stage_hash_orbital_transaction_data_report.TranType TranType,
       stage_hash_orbital_transaction_data_report.EntryMode EntryMode,
       stage_hash_orbital_transaction_data_report.ServiceLevel ServiceLevel,
       stage_hash_orbital_transaction_data_report.Filler1 Filler1,
       stage_hash_orbital_transaction_data_report.ValidationCode ValidationCode,
       stage_hash_orbital_transaction_data_report.DowngradeReason DowngradeReason,
       stage_hash_orbital_transaction_data_report.ProcessDate ProcessDate,
       stage_hash_orbital_transaction_data_report.CustomerDefinedData CustomerDefinedData,
       stage_hash_orbital_transaction_data_report.ChasePayIndicator ChasePayIndicator,
       stage_hash_orbital_transaction_data_report.DigitalTokenMethod DigitalTokenMethod,
       stage_hash_orbital_transaction_data_report.PIDCode PIDCode,
       stage_hash_orbital_transaction_data_report.RecordType RecordType,
       isnull(cast(stage_hash_orbital_transaction_data_report.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.OriginalReferenceNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.MerchantNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.BatchNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_orbital_transaction_data_report.TransactionSequenceNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.TransactionId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.Filler,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.TranCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.CardholderNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_orbital_transaction_data_report.Amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.AuthorizationCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.TransactionDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.MOPCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.MnemonicCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.RejectReasonCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.TranType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.EntryMode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.ServiceLevel,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.Filler1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.ValidationCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.DowngradeReason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.ProcessDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.CustomerDefinedData,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.ChasePayIndicator,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.DigitalTokenMethod,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.PIDCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_orbital_transaction_data_report.RecordType,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_orbital_transaction_data_report
 where stage_hash_orbital_transaction_data_report.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_orbital_transaction_data_report records
set @insert_date_time = getdate()
insert into s_orbital_transaction_data_report (
       bk_hash,
       OriginalReferenceNumber,
       MerchantNumber,
       BatchNumber,
       TransactionSequenceNumber,
       TransactionId,
       Filler,
       TranCode,
       CardholderNumber,
       Amount,
       AuthorizationCode,
       TransactionDate,
       MOPCode,
       MnemonicCode,
       RejectReasonCode,
       TranType,
       EntryMode,
       ServiceLevel,
       Filler1,
       ValidationCode,
       DowngradeReason,
       ProcessDate,
       CustomerDefinedData,
       ChasePayIndicator,
       DigitalTokenMethod,
       PIDCode,
       RecordType,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_orbital_transaction_data_report_inserts.bk_hash,
       #s_orbital_transaction_data_report_inserts.OriginalReferenceNumber,
       #s_orbital_transaction_data_report_inserts.MerchantNumber,
       #s_orbital_transaction_data_report_inserts.BatchNumber,
       #s_orbital_transaction_data_report_inserts.TransactionSequenceNumber,
       #s_orbital_transaction_data_report_inserts.TransactionId,
       #s_orbital_transaction_data_report_inserts.Filler,
       #s_orbital_transaction_data_report_inserts.TranCode,
       #s_orbital_transaction_data_report_inserts.CardholderNumber,
       #s_orbital_transaction_data_report_inserts.Amount,
       #s_orbital_transaction_data_report_inserts.AuthorizationCode,
       #s_orbital_transaction_data_report_inserts.TransactionDate,
       #s_orbital_transaction_data_report_inserts.MOPCode,
       #s_orbital_transaction_data_report_inserts.MnemonicCode,
       #s_orbital_transaction_data_report_inserts.RejectReasonCode,
       #s_orbital_transaction_data_report_inserts.TranType,
       #s_orbital_transaction_data_report_inserts.EntryMode,
       #s_orbital_transaction_data_report_inserts.ServiceLevel,
       #s_orbital_transaction_data_report_inserts.Filler1,
       #s_orbital_transaction_data_report_inserts.ValidationCode,
       #s_orbital_transaction_data_report_inserts.DowngradeReason,
       #s_orbital_transaction_data_report_inserts.ProcessDate,
       #s_orbital_transaction_data_report_inserts.CustomerDefinedData,
       #s_orbital_transaction_data_report_inserts.ChasePayIndicator,
       #s_orbital_transaction_data_report_inserts.DigitalTokenMethod,
       #s_orbital_transaction_data_report_inserts.PIDCode,
       #s_orbital_transaction_data_report_inserts.RecordType,
       case when s_orbital_transaction_data_report.s_orbital_transaction_data_report_id is null then isnull(#s_orbital_transaction_data_report_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_orbital_transaction_data_report_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_orbital_transaction_data_report_inserts
  left join p_orbital_transaction_data_report
    on #s_orbital_transaction_data_report_inserts.bk_hash = p_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_orbital_transaction_data_report
    on p_orbital_transaction_data_report.bk_hash = s_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.s_orbital_transaction_data_report_id = s_orbital_transaction_data_report.s_orbital_transaction_data_report_id
 where s_orbital_transaction_data_report.s_orbital_transaction_data_report_id is null
    or (s_orbital_transaction_data_report.s_orbital_transaction_data_report_id is not null
        and s_orbital_transaction_data_report.dv_hash <> #s_orbital_transaction_data_report_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_orbital_transaction_data_report @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_orbital_transaction_data_report @current_dv_batch_id

--run fact procs
exec dbo.proc_fact_orbital_transaction_data_report @current_dv_batch_id

end
