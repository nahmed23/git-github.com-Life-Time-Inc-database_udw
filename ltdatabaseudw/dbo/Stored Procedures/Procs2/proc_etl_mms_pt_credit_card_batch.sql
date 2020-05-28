CREATE PROC [dbo].[proc_etl_mms_pt_credit_card_batch] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PTCreditCardBatch

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PTCreditCardBatch (
       bk_hash,
       PTCreditCardBatchID,
       PTCreditCardTerminalID,
       BatchNumber,
       TransactionCount,
       NetAmount,
       ActionCode,
       ResponseCode,
       ResponseMessage,
       OpenDateTime,
       UTCOpenDateTime,
       OpenDateTimeZone,
       CloseDateTime,
       UTCCloseDateTime,
       CloseDateTimeZone,
       SubmitDateTime,
       UTCSubmitDateTime,
       SubmitDateTimeZone,
       ValCreditCardBatchStatusID,
       InsertedDateTime,
       UpdatedDateTime,
       DrawerActivityID,
       SubmittedEmployeeID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PTCreditCardBatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PTCreditCardBatchID,
       PTCreditCardTerminalID,
       BatchNumber,
       TransactionCount,
       NetAmount,
       ActionCode,
       ResponseCode,
       ResponseMessage,
       OpenDateTime,
       UTCOpenDateTime,
       OpenDateTimeZone,
       CloseDateTime,
       UTCCloseDateTime,
       CloseDateTimeZone,
       SubmitDateTime,
       UTCSubmitDateTime,
       SubmitDateTimeZone,
       ValCreditCardBatchStatusID,
       InsertedDateTime,
       UpdatedDateTime,
       DrawerActivityID,
       SubmittedEmployeeID,
       isnull(cast(stage_mms_PTCreditCardBatch.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PTCreditCardBatch
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pt_credit_card_batch @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pt_credit_card_batch (
       bk_hash,
       pt_credit_card_batch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PTCreditCardBatch.bk_hash,
       stage_hash_mms_PTCreditCardBatch.PTCreditCardBatchID pt_credit_card_batch_id,
       isnull(cast(stage_hash_mms_PTCreditCardBatch.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PTCreditCardBatch
  left join h_mms_pt_credit_card_batch
    on stage_hash_mms_PTCreditCardBatch.bk_hash = h_mms_pt_credit_card_batch.bk_hash
 where h_mms_pt_credit_card_batch_id is null
   and stage_hash_mms_PTCreditCardBatch.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pt_credit_card_batch
if object_id('tempdb..#l_mms_pt_credit_card_batch_inserts') is not null drop table #l_mms_pt_credit_card_batch_inserts
create table #l_mms_pt_credit_card_batch_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardBatch.bk_hash,
       stage_hash_mms_PTCreditCardBatch.PTCreditCardBatchID pt_credit_card_batch_id,
       stage_hash_mms_PTCreditCardBatch.PTCreditCardTerminalID pt_credit_card_terminal_id,
       stage_hash_mms_PTCreditCardBatch.ValCreditCardBatchStatusID val_credit_card_batch_status_id,
       stage_hash_mms_PTCreditCardBatch.DrawerActivityID drawer_activity_id,
       stage_hash_mms_PTCreditCardBatch.SubmittedEmployeeID submitted_employee_id,
       stage_hash_mms_PTCreditCardBatch.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.PTCreditCardBatchID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.PTCreditCardTerminalID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.ValCreditCardBatchStatusID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.DrawerActivityID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.SubmittedEmployeeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardBatch
 where stage_hash_mms_PTCreditCardBatch.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pt_credit_card_batch records
set @insert_date_time = getdate()
insert into l_mms_pt_credit_card_batch (
       bk_hash,
       pt_credit_card_batch_id,
       pt_credit_card_terminal_id,
       val_credit_card_batch_status_id,
       drawer_activity_id,
       submitted_employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pt_credit_card_batch_inserts.bk_hash,
       #l_mms_pt_credit_card_batch_inserts.pt_credit_card_batch_id,
       #l_mms_pt_credit_card_batch_inserts.pt_credit_card_terminal_id,
       #l_mms_pt_credit_card_batch_inserts.val_credit_card_batch_status_id,
       #l_mms_pt_credit_card_batch_inserts.drawer_activity_id,
       #l_mms_pt_credit_card_batch_inserts.submitted_employee_id,
       case when l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id is null then isnull(#l_mms_pt_credit_card_batch_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pt_credit_card_batch_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pt_credit_card_batch_inserts
  left join p_mms_pt_credit_card_batch
    on #l_mms_pt_credit_card_batch_inserts.bk_hash = p_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pt_credit_card_batch
    on p_mms_pt_credit_card_batch.bk_hash = l_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id = l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id
 where l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id is null
    or (l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id is not null
        and l_mms_pt_credit_card_batch.dv_hash <> #l_mms_pt_credit_card_batch_inserts.source_hash)

--calculate hash and lookup to current s_mms_pt_credit_card_batch
if object_id('tempdb..#s_mms_pt_credit_card_batch_inserts') is not null drop table #s_mms_pt_credit_card_batch_inserts
create table #s_mms_pt_credit_card_batch_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardBatch.bk_hash,
       stage_hash_mms_PTCreditCardBatch.PTCreditCardBatchID pt_credit_card_batch_id,
       stage_hash_mms_PTCreditCardBatch.BatchNumber batch_number,
       stage_hash_mms_PTCreditCardBatch.TransactionCount transaction_count,
       stage_hash_mms_PTCreditCardBatch.NetAmount net_amount,
       stage_hash_mms_PTCreditCardBatch.ActionCode action_code,
       stage_hash_mms_PTCreditCardBatch.ResponseCode response_code,
       stage_hash_mms_PTCreditCardBatch.ResponseMessage response_message,
       stage_hash_mms_PTCreditCardBatch.OpenDateTime open_date_time,
       stage_hash_mms_PTCreditCardBatch.UTCOpenDateTime utc_open_date_time,
       stage_hash_mms_PTCreditCardBatch.OpenDateTimeZone open_date_time_zone,
       stage_hash_mms_PTCreditCardBatch.CloseDateTime close_date_time,
       stage_hash_mms_PTCreditCardBatch.UTCCloseDateTime utc_close_date_time,
       stage_hash_mms_PTCreditCardBatch.CloseDateTimeZone close_date_time_zone,
       stage_hash_mms_PTCreditCardBatch.SubmitDateTime submit_date_time,
       stage_hash_mms_PTCreditCardBatch.UTCSubmitDateTime utc_submit_date_time,
       stage_hash_mms_PTCreditCardBatch.SubmitDateTimeZone submit_date_time_zone,
       stage_hash_mms_PTCreditCardBatch.InsertedDateTime inserted_date_time,
       stage_hash_mms_PTCreditCardBatch.UpdatedDateTime updated_date_time,
       stage_hash_mms_PTCreditCardBatch.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.PTCreditCardBatchID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.BatchNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.TransactionCount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardBatch.NetAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.ActionCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.ResponseCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.ResponseMessage,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.OpenDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.UTCOpenDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.OpenDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.CloseDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.UTCCloseDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.CloseDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.SubmitDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.UTCSubmitDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardBatch.SubmitDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardBatch.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardBatch
 where stage_hash_mms_PTCreditCardBatch.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pt_credit_card_batch records
set @insert_date_time = getdate()
insert into s_mms_pt_credit_card_batch (
       bk_hash,
       pt_credit_card_batch_id,
       batch_number,
       transaction_count,
       net_amount,
       action_code,
       response_code,
       response_message,
       open_date_time,
       utc_open_date_time,
       open_date_time_zone,
       close_date_time,
       utc_close_date_time,
       close_date_time_zone,
       submit_date_time,
       utc_submit_date_time,
       submit_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pt_credit_card_batch_inserts.bk_hash,
       #s_mms_pt_credit_card_batch_inserts.pt_credit_card_batch_id,
       #s_mms_pt_credit_card_batch_inserts.batch_number,
       #s_mms_pt_credit_card_batch_inserts.transaction_count,
       #s_mms_pt_credit_card_batch_inserts.net_amount,
       #s_mms_pt_credit_card_batch_inserts.action_code,
       #s_mms_pt_credit_card_batch_inserts.response_code,
       #s_mms_pt_credit_card_batch_inserts.response_message,
       #s_mms_pt_credit_card_batch_inserts.open_date_time,
       #s_mms_pt_credit_card_batch_inserts.utc_open_date_time,
       #s_mms_pt_credit_card_batch_inserts.open_date_time_zone,
       #s_mms_pt_credit_card_batch_inserts.close_date_time,
       #s_mms_pt_credit_card_batch_inserts.utc_close_date_time,
       #s_mms_pt_credit_card_batch_inserts.close_date_time_zone,
       #s_mms_pt_credit_card_batch_inserts.submit_date_time,
       #s_mms_pt_credit_card_batch_inserts.utc_submit_date_time,
       #s_mms_pt_credit_card_batch_inserts.submit_date_time_zone,
       #s_mms_pt_credit_card_batch_inserts.inserted_date_time,
       #s_mms_pt_credit_card_batch_inserts.updated_date_time,
       case when s_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id is null then isnull(#s_mms_pt_credit_card_batch_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pt_credit_card_batch_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pt_credit_card_batch_inserts
  left join p_mms_pt_credit_card_batch
    on #s_mms_pt_credit_card_batch_inserts.bk_hash = p_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pt_credit_card_batch
    on p_mms_pt_credit_card_batch.bk_hash = s_mms_pt_credit_card_batch.bk_hash
   and p_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id = s_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id
 where s_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id is null
    or (s_mms_pt_credit_card_batch.s_mms_pt_credit_card_batch_id is not null
        and s_mms_pt_credit_card_batch.dv_hash <> #s_mms_pt_credit_card_batch_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pt_credit_card_batch @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_pt_credit_card_batch @current_dv_batch_id

end
