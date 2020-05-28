CREATE PROC [dbo].[proc_etl_mms_payment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Payment

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Payment (
       bk_hash,
       PaymentID,
       ValPaymentTypeID,
       PaymentAmount,
       ApprovalCode,
       MMSTranID,
       InsertedDateTime,
       UpdatedDateTime,
       TipAmount,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PaymentID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PaymentID,
       ValPaymentTypeID,
       PaymentAmount,
       ApprovalCode,
       MMSTranID,
       InsertedDateTime,
       UpdatedDateTime,
       TipAmount,
       isnull(cast(stage_mms_Payment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_Payment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_payment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_payment (
       bk_hash,
       payment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_Payment.bk_hash,
       stage_hash_mms_Payment.PaymentID payment_id,
       isnull(cast(stage_hash_mms_Payment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Payment
  left join h_mms_payment
    on stage_hash_mms_Payment.bk_hash = h_mms_payment.bk_hash
 where h_mms_payment_id is null
   and stage_hash_mms_Payment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_payment
if object_id('tempdb..#l_mms_payment_inserts') is not null drop table #l_mms_payment_inserts
create table #l_mms_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Payment.bk_hash,
       stage_hash_mms_Payment.PaymentID payment_id,
       stage_hash_mms_Payment.ValPaymentTypeID val_payment_type_id,
       stage_hash_mms_Payment.MMSTranID mms_tran_id,
       isnull(cast(stage_hash_mms_Payment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.PaymentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.ValPaymentTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.MMSTranID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Payment
 where stage_hash_mms_Payment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_payment records
set @insert_date_time = getdate()
insert into l_mms_payment (
       bk_hash,
       payment_id,
       val_payment_type_id,
       mms_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_payment_inserts.bk_hash,
       #l_mms_payment_inserts.payment_id,
       #l_mms_payment_inserts.val_payment_type_id,
       #l_mms_payment_inserts.mms_tran_id,
       case when l_mms_payment.l_mms_payment_id is null then isnull(#l_mms_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_payment_inserts
  left join p_mms_payment
    on #l_mms_payment_inserts.bk_hash = p_mms_payment.bk_hash
   and p_mms_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_payment
    on p_mms_payment.bk_hash = l_mms_payment.bk_hash
   and p_mms_payment.l_mms_payment_id = l_mms_payment.l_mms_payment_id
 where l_mms_payment.l_mms_payment_id is null
    or (l_mms_payment.l_mms_payment_id is not null
        and l_mms_payment.dv_hash <> #l_mms_payment_inserts.source_hash)

--calculate hash and lookup to current s_mms_payment
if object_id('tempdb..#s_mms_payment_inserts') is not null drop table #s_mms_payment_inserts
create table #s_mms_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Payment.bk_hash,
       stage_hash_mms_Payment.PaymentID payment_id,
       stage_hash_mms_Payment.PaymentAmount payment_amount,
       stage_hash_mms_Payment.ApprovalCode approval_code,
       stage_hash_mms_Payment.InsertedDateTime inserted_date_time,
       stage_hash_mms_Payment.UpdatedDateTime updated_date_time,
       stage_hash_mms_Payment.TipAmount tip_amount,
       isnull(cast(stage_hash_mms_Payment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.PaymentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.PaymentAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Payment.ApprovalCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Payment.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Payment.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Payment.TipAmount as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Payment
 where stage_hash_mms_Payment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_payment records
set @insert_date_time = getdate()
insert into s_mms_payment (
       bk_hash,
       payment_id,
       payment_amount,
       approval_code,
       inserted_date_time,
       updated_date_time,
       tip_amount,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_payment_inserts.bk_hash,
       #s_mms_payment_inserts.payment_id,
       #s_mms_payment_inserts.payment_amount,
       #s_mms_payment_inserts.approval_code,
       #s_mms_payment_inserts.inserted_date_time,
       #s_mms_payment_inserts.updated_date_time,
       #s_mms_payment_inserts.tip_amount,
       case when s_mms_payment.s_mms_payment_id is null then isnull(#s_mms_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_payment_inserts
  left join p_mms_payment
    on #s_mms_payment_inserts.bk_hash = p_mms_payment.bk_hash
   and p_mms_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_payment
    on p_mms_payment.bk_hash = s_mms_payment.bk_hash
   and p_mms_payment.s_mms_payment_id = s_mms_payment.s_mms_payment_id
 where s_mms_payment.s_mms_payment_id is null
    or (s_mms_payment.s_mms_payment_id is not null
        and s_mms_payment.dv_hash <> #s_mms_payment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_payment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_payment @current_dv_batch_id

end
