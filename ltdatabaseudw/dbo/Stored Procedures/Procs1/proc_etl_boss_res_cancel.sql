CREATE PROC [dbo].[proc_etl_boss_res_cancel] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_rescancel

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_rescancel (
       bk_hash,
       reservation,
       cancel_date,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(reservation as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(convert(varchar,cancel_date,120),'z#@$k%&P'))),2) bk_hash,
       reservation,
       cancel_date,
       isnull(cast(stage_boss_rescancel.cancel_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_rescancel
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_res_cancel @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_res_cancel (
       bk_hash,
       reservation,
       cancel_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_rescancel.bk_hash,
       stage_hash_boss_rescancel.reservation reservation,
       stage_hash_boss_rescancel.cancel_date cancel_date,
       isnull(cast(stage_hash_boss_rescancel.cancel_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_rescancel
  left join h_boss_res_cancel
    on stage_hash_boss_rescancel.bk_hash = h_boss_res_cancel.bk_hash
 where h_boss_res_cancel_id is null
   and stage_hash_boss_rescancel.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_res_cancel
if object_id('tempdb..#l_boss_res_cancel_inserts') is not null drop table #l_boss_res_cancel_inserts
create table #l_boss_res_cancel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_rescancel.bk_hash,
       stage_hash_boss_rescancel.reservation reservation,
       stage_hash_boss_rescancel.cancel_date cancel_date,
       isnull(cast(stage_hash_boss_rescancel.cancel_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_rescancel.reservation as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_rescancel.cancel_date,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_rescancel
 where stage_hash_boss_rescancel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_res_cancel records
set @insert_date_time = getdate()
insert into l_boss_res_cancel (
       bk_hash,
       reservation,
       cancel_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_res_cancel_inserts.bk_hash,
       #l_boss_res_cancel_inserts.reservation,
       #l_boss_res_cancel_inserts.cancel_date,
       case when l_boss_res_cancel.l_boss_res_cancel_id is null then isnull(#l_boss_res_cancel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_res_cancel_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_res_cancel_inserts
  left join p_boss_res_cancel
    on #l_boss_res_cancel_inserts.bk_hash = p_boss_res_cancel.bk_hash
   and p_boss_res_cancel.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_res_cancel
    on p_boss_res_cancel.bk_hash = l_boss_res_cancel.bk_hash
   and p_boss_res_cancel.l_boss_res_cancel_id = l_boss_res_cancel.l_boss_res_cancel_id
 where l_boss_res_cancel.l_boss_res_cancel_id is null
    or (l_boss_res_cancel.l_boss_res_cancel_id is not null
        and l_boss_res_cancel.dv_hash <> #l_boss_res_cancel_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_res_cancel @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_res_cancel @current_dv_batch_id

end
