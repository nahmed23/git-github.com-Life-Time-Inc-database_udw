CREATE PROC [dbo].[proc_etl_mms_payment_refund] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PaymentRefund

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PaymentRefund (
       bk_hash,
       PaymentRefundID,
       PaymentID,
       ValPaymentStatusID,
       StatusChangeDateTime,
       UTCStatusChangeDateTime,
       StatusChangeDateTimeZone,
       StatusChangeEmployeeID,
       PaymentIssuedDateTime,
       Comment,
       ReferenceNumber,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PaymentRefundID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PaymentRefundID,
       PaymentID,
       ValPaymentStatusID,
       StatusChangeDateTime,
       UTCStatusChangeDateTime,
       StatusChangeDateTimeZone,
       StatusChangeEmployeeID,
       PaymentIssuedDateTime,
       Comment,
       ReferenceNumber,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_PaymentRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_PaymentRefund
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_payment_refund @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_payment_refund (
       bk_hash,
       payment_refund_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_PaymentRefund.bk_hash,
       stage_hash_mms_PaymentRefund.PaymentRefundID payment_refund_id,
       isnull(cast(stage_hash_mms_PaymentRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PaymentRefund
  left join h_mms_payment_refund
    on stage_hash_mms_PaymentRefund.bk_hash = h_mms_payment_refund.bk_hash
 where h_mms_payment_refund_id is null
   and stage_hash_mms_PaymentRefund.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_payment_refund
if object_id('tempdb..#l_mms_payment_refund_inserts') is not null drop table #l_mms_payment_refund_inserts
create table #l_mms_payment_refund_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentRefund.bk_hash,
       stage_hash_mms_PaymentRefund.PaymentRefundID payment_refund_id,
       stage_hash_mms_PaymentRefund.PaymentID payment_id,
       stage_hash_mms_PaymentRefund.ValPaymentStatusID val_payment_status_id,
       stage_hash_mms_PaymentRefund.StatusChangeEmployeeID status_change_employee_id,
       isnull(cast(stage_hash_mms_PaymentRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefund.PaymentRefundID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefund.PaymentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefund.ValPaymentStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefund.StatusChangeEmployeeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentRefund
 where stage_hash_mms_PaymentRefund.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_payment_refund records
set @insert_date_time = getdate()
insert into l_mms_payment_refund (
       bk_hash,
       payment_refund_id,
       payment_id,
       val_payment_status_id,
       status_change_employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_payment_refund_inserts.bk_hash,
       #l_mms_payment_refund_inserts.payment_refund_id,
       #l_mms_payment_refund_inserts.payment_id,
       #l_mms_payment_refund_inserts.val_payment_status_id,
       #l_mms_payment_refund_inserts.status_change_employee_id,
       case when l_mms_payment_refund.l_mms_payment_refund_id is null then isnull(#l_mms_payment_refund_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_payment_refund_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_payment_refund_inserts
  left join p_mms_payment_refund
    on #l_mms_payment_refund_inserts.bk_hash = p_mms_payment_refund.bk_hash
   and p_mms_payment_refund.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_payment_refund
    on p_mms_payment_refund.bk_hash = l_mms_payment_refund.bk_hash
   and p_mms_payment_refund.l_mms_payment_refund_id = l_mms_payment_refund.l_mms_payment_refund_id
 where l_mms_payment_refund.l_mms_payment_refund_id is null
    or (l_mms_payment_refund.l_mms_payment_refund_id is not null
        and l_mms_payment_refund.dv_hash <> #l_mms_payment_refund_inserts.source_hash)

--calculate hash and lookup to current s_mms_payment_refund
if object_id('tempdb..#s_mms_payment_refund_inserts') is not null drop table #s_mms_payment_refund_inserts
create table #s_mms_payment_refund_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentRefund.bk_hash,
       stage_hash_mms_PaymentRefund.PaymentRefundID payment_refund_id,
       stage_hash_mms_PaymentRefund.StatusChangeDateTime status_change_date_time,
       stage_hash_mms_PaymentRefund.UTCStatusChangeDateTime utc_status_change_date_time,
       stage_hash_mms_PaymentRefund.StatusChangeDateTimeZone status_change_date_time_zone,
       stage_hash_mms_PaymentRefund.PaymentIssuedDateTime payment_issued_date_time,
       stage_hash_mms_PaymentRefund.Comment comment,
       stage_hash_mms_PaymentRefund.ReferenceNumber reference_number,
       stage_hash_mms_PaymentRefund.InsertedDateTime inserted_date_time,
       stage_hash_mms_PaymentRefund.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_PaymentRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefund.PaymentRefundID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefund.StatusChangeDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefund.UTCStatusChangeDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefund.StatusChangeDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefund.PaymentIssuedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefund.Comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefund.ReferenceNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefund.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefund.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentRefund
 where stage_hash_mms_PaymentRefund.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_payment_refund records
set @insert_date_time = getdate()
insert into s_mms_payment_refund (
       bk_hash,
       payment_refund_id,
       status_change_date_time,
       utc_status_change_date_time,
       status_change_date_time_zone,
       payment_issued_date_time,
       comment,
       reference_number,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_payment_refund_inserts.bk_hash,
       #s_mms_payment_refund_inserts.payment_refund_id,
       #s_mms_payment_refund_inserts.status_change_date_time,
       #s_mms_payment_refund_inserts.utc_status_change_date_time,
       #s_mms_payment_refund_inserts.status_change_date_time_zone,
       #s_mms_payment_refund_inserts.payment_issued_date_time,
       #s_mms_payment_refund_inserts.comment,
       #s_mms_payment_refund_inserts.reference_number,
       #s_mms_payment_refund_inserts.inserted_date_time,
       #s_mms_payment_refund_inserts.updated_date_time,
       case when s_mms_payment_refund.s_mms_payment_refund_id is null then isnull(#s_mms_payment_refund_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_payment_refund_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_payment_refund_inserts
  left join p_mms_payment_refund
    on #s_mms_payment_refund_inserts.bk_hash = p_mms_payment_refund.bk_hash
   and p_mms_payment_refund.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_payment_refund
    on p_mms_payment_refund.bk_hash = s_mms_payment_refund.bk_hash
   and p_mms_payment_refund.s_mms_payment_refund_id = s_mms_payment_refund.s_mms_payment_refund_id
 where s_mms_payment_refund.s_mms_payment_refund_id is null
    or (s_mms_payment_refund.s_mms_payment_refund_id is not null
        and s_mms_payment_refund.dv_hash <> #s_mms_payment_refund_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_payment_refund @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_payment_refund @current_dv_batch_id

end
