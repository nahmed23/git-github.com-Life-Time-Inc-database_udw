CREATE PROC [dbo].[proc_etl_boss_asi_resource] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiresource

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiresource (
       bk_hash,
       resource_type,
       interval_len,
       start_time,
       end_time,
       default_upccode,
       availability,
       dept_affinity,
       cancel_notify,
       interest_affinity,
       resource_type_id,
       advance_days,
       min_slots,
       max_slots,
       web_slots_int_mult,
       web_active,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(resource_type_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       resource_type,
       interval_len,
       start_time,
       end_time,
       default_upccode,
       availability,
       dept_affinity,
       cancel_notify,
       interest_affinity,
       resource_type_id,
       advance_days,
       min_slots,
       max_slots,
       web_slots_int_mult,
       web_active,
       isnull(cast(stage_boss_asiresource.start_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asiresource
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_resource @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_resource (
       bk_hash,
       resource_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiresource.bk_hash,
       stage_hash_boss_asiresource.resource_type_id resource_type_id,
       isnull(cast(stage_hash_boss_asiresource.start_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiresource
  left join h_boss_asi_resource
    on stage_hash_boss_asiresource.bk_hash = h_boss_asi_resource.bk_hash
 where h_boss_asi_resource_id is null
   and stage_hash_boss_asiresource.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_resource
if object_id('tempdb..#l_boss_asi_resource_inserts') is not null drop table #l_boss_asi_resource_inserts
create table #l_boss_asi_resource_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiresource.bk_hash,
       stage_hash_boss_asiresource.default_upccode default_upc_code,
       stage_hash_boss_asiresource.resource_type_id resource_type_id,
       isnull(cast(stage_hash_boss_asiresource.start_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_asiresource.default_upccode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.resource_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiresource
 where stage_hash_boss_asiresource.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_resource records
set @insert_date_time = getdate()
insert into l_boss_asi_resource (
       bk_hash,
       default_upc_code,
       resource_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_resource_inserts.bk_hash,
       #l_boss_asi_resource_inserts.default_upc_code,
       #l_boss_asi_resource_inserts.resource_type_id,
       case when l_boss_asi_resource.l_boss_asi_resource_id is null then isnull(#l_boss_asi_resource_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_resource_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_resource_inserts
  left join p_boss_asi_resource
    on #l_boss_asi_resource_inserts.bk_hash = p_boss_asi_resource.bk_hash
   and p_boss_asi_resource.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_resource
    on p_boss_asi_resource.bk_hash = l_boss_asi_resource.bk_hash
   and p_boss_asi_resource.l_boss_asi_resource_id = l_boss_asi_resource.l_boss_asi_resource_id
 where l_boss_asi_resource.l_boss_asi_resource_id is null
    or (l_boss_asi_resource.l_boss_asi_resource_id is not null
        and l_boss_asi_resource.dv_hash <> #l_boss_asi_resource_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_resource
if object_id('tempdb..#s_boss_asi_resource_inserts') is not null drop table #s_boss_asi_resource_inserts
create table #s_boss_asi_resource_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiresource.bk_hash,
       stage_hash_boss_asiresource.resource_type resource_type,
       stage_hash_boss_asiresource.interval_len interval_len,
       stage_hash_boss_asiresource.start_time start_time,
       stage_hash_boss_asiresource.end_time end_time,
       stage_hash_boss_asiresource.availability availability,
       stage_hash_boss_asiresource.dept_affinity dept_affinity,
       stage_hash_boss_asiresource.cancel_notify cancel_notify,
       stage_hash_boss_asiresource.interest_affinity interest_affinity,
       stage_hash_boss_asiresource.resource_type_id resource_type_id,
       stage_hash_boss_asiresource.advance_days advance_days,
       stage_hash_boss_asiresource.min_slots min_slots,
       stage_hash_boss_asiresource.max_slots max_slots,
       stage_hash_boss_asiresource.web_slots_int_mult web_slots_int_mult,
       stage_hash_boss_asiresource.web_active web_active,
       isnull(cast(stage_hash_boss_asiresource.start_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_asiresource.resource_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.interval_len as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresource.start_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresource.end_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asiresource.availability,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.dept_affinity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.cancel_notify as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.interest_affinity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.resource_type_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.advance_days as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.min_slots as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.max_slots as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresource.web_slots_int_mult as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asiresource.web_active,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiresource
 where stage_hash_boss_asiresource.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_resource records
set @insert_date_time = getdate()
insert into s_boss_asi_resource (
       bk_hash,
       resource_type,
       interval_len,
       start_time,
       end_time,
       availability,
       dept_affinity,
       cancel_notify,
       interest_affinity,
       resource_type_id,
       advance_days,
       min_slots,
       max_slots,
       web_slots_int_mult,
       web_active,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_resource_inserts.bk_hash,
       #s_boss_asi_resource_inserts.resource_type,
       #s_boss_asi_resource_inserts.interval_len,
       #s_boss_asi_resource_inserts.start_time,
       #s_boss_asi_resource_inserts.end_time,
       #s_boss_asi_resource_inserts.availability,
       #s_boss_asi_resource_inserts.dept_affinity,
       #s_boss_asi_resource_inserts.cancel_notify,
       #s_boss_asi_resource_inserts.interest_affinity,
       #s_boss_asi_resource_inserts.resource_type_id,
       #s_boss_asi_resource_inserts.advance_days,
       #s_boss_asi_resource_inserts.min_slots,
       #s_boss_asi_resource_inserts.max_slots,
       #s_boss_asi_resource_inserts.web_slots_int_mult,
       #s_boss_asi_resource_inserts.web_active,
       case when s_boss_asi_resource.s_boss_asi_resource_id is null then isnull(#s_boss_asi_resource_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_resource_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_resource_inserts
  left join p_boss_asi_resource
    on #s_boss_asi_resource_inserts.bk_hash = p_boss_asi_resource.bk_hash
   and p_boss_asi_resource.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_resource
    on p_boss_asi_resource.bk_hash = s_boss_asi_resource.bk_hash
   and p_boss_asi_resource.s_boss_asi_resource_id = s_boss_asi_resource.s_boss_asi_resource_id
 where s_boss_asi_resource.s_boss_asi_resource_id is null
    or (s_boss_asi_resource.s_boss_asi_resource_id is not null
        and s_boss_asi_resource.dv_hash <> #s_boss_asi_resource_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_resource @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_resource @current_dv_batch_id

end
