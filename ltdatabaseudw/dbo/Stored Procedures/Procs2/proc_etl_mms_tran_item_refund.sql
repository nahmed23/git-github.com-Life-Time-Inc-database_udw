CREATE PROC [dbo].[proc_etl_mms_tran_item_refund] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_TranItemRefund

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_TranItemRefund (
       bk_hash,
       TranItemRefundID,
       TranItemID,
       OriginalTranItemID,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(TranItemRefundID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       TranItemRefundID,
       TranItemID,
       OriginalTranItemID,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_TranItemRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_TranItemRefund
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_tran_item_refund @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_tran_item_refund (
       bk_hash,
       tran_item_refund_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_TranItemRefund.bk_hash,
       stage_hash_mms_TranItemRefund.TranItemRefundID tran_item_refund_id,
       isnull(cast(stage_hash_mms_TranItemRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_TranItemRefund
  left join h_mms_tran_item_refund
    on stage_hash_mms_TranItemRefund.bk_hash = h_mms_tran_item_refund.bk_hash
 where h_mms_tran_item_refund_id is null
   and stage_hash_mms_TranItemRefund.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_tran_item_refund
if object_id('tempdb..#l_mms_tran_item_refund_inserts') is not null drop table #l_mms_tran_item_refund_inserts
create table #l_mms_tran_item_refund_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItemRefund.bk_hash,
       stage_hash_mms_TranItemRefund.TranItemRefundID tran_item_refund_id,
       stage_hash_mms_TranItemRefund.TranItemID tran_item_id,
       stage_hash_mms_TranItemRefund.OriginalTranItemID original_tran_item_id,
       isnull(cast(stage_hash_mms_TranItemRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemRefund.TranItemRefundID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemRefund.TranItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemRefund.OriginalTranItemID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItemRefund
 where stage_hash_mms_TranItemRefund.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_tran_item_refund records
set @insert_date_time = getdate()
insert into l_mms_tran_item_refund (
       bk_hash,
       tran_item_refund_id,
       tran_item_id,
       original_tran_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_tran_item_refund_inserts.bk_hash,
       #l_mms_tran_item_refund_inserts.tran_item_refund_id,
       #l_mms_tran_item_refund_inserts.tran_item_id,
       #l_mms_tran_item_refund_inserts.original_tran_item_id,
       case when l_mms_tran_item_refund.l_mms_tran_item_refund_id is null then isnull(#l_mms_tran_item_refund_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_tran_item_refund_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_tran_item_refund_inserts
  left join p_mms_tran_item_refund
    on #l_mms_tran_item_refund_inserts.bk_hash = p_mms_tran_item_refund.bk_hash
   and p_mms_tran_item_refund.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_tran_item_refund
    on p_mms_tran_item_refund.bk_hash = l_mms_tran_item_refund.bk_hash
   and p_mms_tran_item_refund.l_mms_tran_item_refund_id = l_mms_tran_item_refund.l_mms_tran_item_refund_id
 where l_mms_tran_item_refund.l_mms_tran_item_refund_id is null
    or (l_mms_tran_item_refund.l_mms_tran_item_refund_id is not null
        and l_mms_tran_item_refund.dv_hash <> #l_mms_tran_item_refund_inserts.source_hash)

--calculate hash and lookup to current s_mms_tran_item_refund
if object_id('tempdb..#s_mms_tran_item_refund_inserts') is not null drop table #s_mms_tran_item_refund_inserts
create table #s_mms_tran_item_refund_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItemRefund.bk_hash,
       stage_hash_mms_TranItemRefund.TranItemRefundID tran_item_refund_id,
       stage_hash_mms_TranItemRefund.InsertedDateTime inserted_date_time,
       stage_hash_mms_TranItemRefund.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_TranItemRefund.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemRefund.TranItemRefundID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItemRefund.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItemRefund.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItemRefund
 where stage_hash_mms_TranItemRefund.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_tran_item_refund records
set @insert_date_time = getdate()
insert into s_mms_tran_item_refund (
       bk_hash,
       tran_item_refund_id,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_tran_item_refund_inserts.bk_hash,
       #s_mms_tran_item_refund_inserts.tran_item_refund_id,
       #s_mms_tran_item_refund_inserts.inserted_date_time,
       #s_mms_tran_item_refund_inserts.updated_date_time,
       case when s_mms_tran_item_refund.s_mms_tran_item_refund_id is null then isnull(#s_mms_tran_item_refund_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_tran_item_refund_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_tran_item_refund_inserts
  left join p_mms_tran_item_refund
    on #s_mms_tran_item_refund_inserts.bk_hash = p_mms_tran_item_refund.bk_hash
   and p_mms_tran_item_refund.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_tran_item_refund
    on p_mms_tran_item_refund.bk_hash = s_mms_tran_item_refund.bk_hash
   and p_mms_tran_item_refund.s_mms_tran_item_refund_id = s_mms_tran_item_refund.s_mms_tran_item_refund_id
 where s_mms_tran_item_refund.s_mms_tran_item_refund_id is null
    or (s_mms_tran_item_refund.s_mms_tran_item_refund_id is not null
        and s_mms_tran_item_refund.dv_hash <> #s_mms_tran_item_refund_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_tran_item_refund @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_tran_item_refund @current_dv_batch_id

end
