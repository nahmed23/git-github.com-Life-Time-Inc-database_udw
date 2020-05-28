CREATE PROC [dbo].[proc_etl_boss_audit_reserve] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_auditreserve

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_auditreserve (
       bk_hash,
       [id],
       reservation,
       club,
       start_date,
       create_date,
       upc_desc,
       reservation_type,
       audit_type,
       audit_performed,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       reservation,
       club,
       start_date,
       create_date,
       upc_desc,
       reservation_type,
       audit_type,
       audit_performed,
       isnull(cast(stage_boss_auditreserve.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_auditreserve
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_audit_reserve @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_audit_reserve (
       bk_hash,
       audit_reserve_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_auditreserve.bk_hash,
       stage_hash_boss_auditreserve.[id] audit_reserve_id,
       isnull(cast(stage_hash_boss_auditreserve.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_auditreserve
  left join h_boss_audit_reserve
    on stage_hash_boss_auditreserve.bk_hash = h_boss_audit_reserve.bk_hash
 where h_boss_audit_reserve_id is null
   and stage_hash_boss_auditreserve.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_audit_reserve
if object_id('tempdb..#l_boss_audit_reserve_inserts') is not null drop table #l_boss_audit_reserve_inserts
create table #l_boss_audit_reserve_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_auditreserve.bk_hash,
       stage_hash_boss_auditreserve.[id] audit_reserve_id,
       stage_hash_boss_auditreserve.reservation reservation,
       isnull(cast(stage_hash_boss_auditreserve.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_auditreserve.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_auditreserve.reservation as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_auditreserve
 where stage_hash_boss_auditreserve.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_audit_reserve records
set @insert_date_time = getdate()
insert into l_boss_audit_reserve (
       bk_hash,
       audit_reserve_id,
       reservation,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_audit_reserve_inserts.bk_hash,
       #l_boss_audit_reserve_inserts.audit_reserve_id,
       #l_boss_audit_reserve_inserts.reservation,
       case when l_boss_audit_reserve.l_boss_audit_reserve_id is null then isnull(#l_boss_audit_reserve_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_audit_reserve_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_audit_reserve_inserts
  left join p_boss_audit_reserve
    on #l_boss_audit_reserve_inserts.bk_hash = p_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_audit_reserve
    on p_boss_audit_reserve.bk_hash = l_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.l_boss_audit_reserve_id = l_boss_audit_reserve.l_boss_audit_reserve_id
 where l_boss_audit_reserve.l_boss_audit_reserve_id is null
    or (l_boss_audit_reserve.l_boss_audit_reserve_id is not null
        and l_boss_audit_reserve.dv_hash <> #l_boss_audit_reserve_inserts.source_hash)

--calculate hash and lookup to current s_boss_audit_reserve
if object_id('tempdb..#s_boss_audit_reserve_inserts') is not null drop table #s_boss_audit_reserve_inserts
create table #s_boss_audit_reserve_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_auditreserve.bk_hash,
       stage_hash_boss_auditreserve.[id] audit_reserve_id,
       stage_hash_boss_auditreserve.club club,
       stage_hash_boss_auditreserve.start_date start_date,
       stage_hash_boss_auditreserve.create_date create_date,
       stage_hash_boss_auditreserve.upc_desc upc_desc,
       stage_hash_boss_auditreserve.reservation_type reservation_type,
       stage_hash_boss_auditreserve.audit_type audit_type,
       stage_hash_boss_auditreserve.audit_performed audit_performed,
       isnull(cast(stage_hash_boss_auditreserve.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_auditreserve.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_auditreserve.club as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_auditreserve.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_auditreserve.create_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_auditreserve.upc_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_auditreserve.reservation_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_auditreserve.audit_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_auditreserve.audit_performed,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_auditreserve
 where stage_hash_boss_auditreserve.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_audit_reserve records
set @insert_date_time = getdate()
insert into s_boss_audit_reserve (
       bk_hash,
       audit_reserve_id,
       club,
       start_date,
       create_date,
       upc_desc,
       reservation_type,
       audit_type,
       audit_performed,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_audit_reserve_inserts.bk_hash,
       #s_boss_audit_reserve_inserts.audit_reserve_id,
       #s_boss_audit_reserve_inserts.club,
       #s_boss_audit_reserve_inserts.start_date,
       #s_boss_audit_reserve_inserts.create_date,
       #s_boss_audit_reserve_inserts.upc_desc,
       #s_boss_audit_reserve_inserts.reservation_type,
       #s_boss_audit_reserve_inserts.audit_type,
       #s_boss_audit_reserve_inserts.audit_performed,
       case when s_boss_audit_reserve.s_boss_audit_reserve_id is null then isnull(#s_boss_audit_reserve_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_audit_reserve_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_audit_reserve_inserts
  left join p_boss_audit_reserve
    on #s_boss_audit_reserve_inserts.bk_hash = p_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_audit_reserve
    on p_boss_audit_reserve.bk_hash = s_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.s_boss_audit_reserve_id = s_boss_audit_reserve.s_boss_audit_reserve_id
 where s_boss_audit_reserve.s_boss_audit_reserve_id is null
    or (s_boss_audit_reserve.s_boss_audit_reserve_id is not null
        and s_boss_audit_reserve.dv_hash <> #s_boss_audit_reserve_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_audit_reserve @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_audit_reserve @current_dv_batch_id

end
