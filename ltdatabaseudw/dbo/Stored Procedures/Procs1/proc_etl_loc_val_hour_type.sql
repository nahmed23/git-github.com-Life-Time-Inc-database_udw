CREATE PROC [dbo].[proc_etl_loc_val_hour_type] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_loc_val_hour_type

set @insert_date_time = getdate()
insert into dbo.stage_hash_loc_val_hour_type (
       bk_hash,
       val_hour_type_id,
       val_hour_type_name,
       display_name,
       val_hour_type_group_id,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       managed_by_udw,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(val_hour_type_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       val_hour_type_id,
       val_hour_type_name,
       display_name,
       val_hour_type_group_id,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       managed_by_udw,
       isnull(cast(stage_loc_val_hour_type.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_loc_val_hour_type
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_loc_val_hour_type @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_loc_val_hour_type (
       bk_hash,
       val_hour_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_loc_val_hour_type.bk_hash,
       stage_hash_loc_val_hour_type.val_hour_type_id val_hour_type_id,
       isnull(cast(stage_hash_loc_val_hour_type.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       50,
       @insert_date_time,
       @user
  from stage_hash_loc_val_hour_type
  left join h_loc_val_hour_type
    on stage_hash_loc_val_hour_type.bk_hash = h_loc_val_hour_type.bk_hash
 where h_loc_val_hour_type_id is null
   and stage_hash_loc_val_hour_type.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_loc_val_hour_type
if object_id('tempdb..#l_loc_val_hour_type_inserts') is not null drop table #l_loc_val_hour_type_inserts
create table #l_loc_val_hour_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_val_hour_type.bk_hash,
       stage_hash_loc_val_hour_type.val_hour_type_id val_hour_type_id,
       stage_hash_loc_val_hour_type.val_hour_type_group_id val_hour_type_group_id,
       isnull(cast(stage_hash_loc_val_hour_type.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_val_hour_type.val_hour_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_val_hour_type.val_hour_type_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_val_hour_type
 where stage_hash_loc_val_hour_type.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_loc_val_hour_type records
set @insert_date_time = getdate()
insert into l_loc_val_hour_type (
       bk_hash,
       val_hour_type_id,
       val_hour_type_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_loc_val_hour_type_inserts.bk_hash,
       #l_loc_val_hour_type_inserts.val_hour_type_id,
       #l_loc_val_hour_type_inserts.val_hour_type_group_id,
       case when l_loc_val_hour_type.l_loc_val_hour_type_id is null then isnull(#l_loc_val_hour_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #l_loc_val_hour_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_loc_val_hour_type_inserts
  left join p_loc_val_hour_type
    on #l_loc_val_hour_type_inserts.bk_hash = p_loc_val_hour_type.bk_hash
   and p_loc_val_hour_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_loc_val_hour_type
    on p_loc_val_hour_type.bk_hash = l_loc_val_hour_type.bk_hash
   and p_loc_val_hour_type.l_loc_val_hour_type_id = l_loc_val_hour_type.l_loc_val_hour_type_id
 where l_loc_val_hour_type.l_loc_val_hour_type_id is null
    or (l_loc_val_hour_type.l_loc_val_hour_type_id is not null
        and l_loc_val_hour_type.dv_hash <> #l_loc_val_hour_type_inserts.source_hash)

--calculate hash and lookup to current s_loc_val_hour_type
if object_id('tempdb..#s_loc_val_hour_type_inserts') is not null drop table #s_loc_val_hour_type_inserts
create table #s_loc_val_hour_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_val_hour_type.bk_hash,
       stage_hash_loc_val_hour_type.val_hour_type_id val_hour_type_id,
       stage_hash_loc_val_hour_type.val_hour_type_name val_hour_type_name,
       stage_hash_loc_val_hour_type.display_name display_name,
       stage_hash_loc_val_hour_type.created_date_time created_date_time,
       stage_hash_loc_val_hour_type.created_by created_by,
       stage_hash_loc_val_hour_type.last_updated_date_time last_updated_date_time,
       stage_hash_loc_val_hour_type.last_updated_by last_updated_by,
       stage_hash_loc_val_hour_type.deleted_date_time deleted_date_time,
       stage_hash_loc_val_hour_type.deleted_by deleted_by,
       stage_hash_loc_val_hour_type.managed_by_udw managed_by_udw,
       isnull(cast(stage_hash_loc_val_hour_type.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_val_hour_type.val_hour_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_hour_type.val_hour_type_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_hour_type.display_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_hour_type.created_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_hour_type.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_hour_type.last_updated_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_hour_type.last_updated_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_hour_type.deleted_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_hour_type.deleted_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_val_hour_type.managed_by_udw as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_val_hour_type
 where stage_hash_loc_val_hour_type.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_loc_val_hour_type records
set @insert_date_time = getdate()
insert into s_loc_val_hour_type (
       bk_hash,
       val_hour_type_id,
       val_hour_type_name,
       display_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       managed_by_udw,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_loc_val_hour_type_inserts.bk_hash,
       #s_loc_val_hour_type_inserts.val_hour_type_id,
       #s_loc_val_hour_type_inserts.val_hour_type_name,
       #s_loc_val_hour_type_inserts.display_name,
       #s_loc_val_hour_type_inserts.created_date_time,
       #s_loc_val_hour_type_inserts.created_by,
       #s_loc_val_hour_type_inserts.last_updated_date_time,
       #s_loc_val_hour_type_inserts.last_updated_by,
       #s_loc_val_hour_type_inserts.deleted_date_time,
       #s_loc_val_hour_type_inserts.deleted_by,
       #s_loc_val_hour_type_inserts.managed_by_udw,
       case when s_loc_val_hour_type.s_loc_val_hour_type_id is null then isnull(#s_loc_val_hour_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #s_loc_val_hour_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_loc_val_hour_type_inserts
  left join p_loc_val_hour_type
    on #s_loc_val_hour_type_inserts.bk_hash = p_loc_val_hour_type.bk_hash
   and p_loc_val_hour_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_loc_val_hour_type
    on p_loc_val_hour_type.bk_hash = s_loc_val_hour_type.bk_hash
   and p_loc_val_hour_type.s_loc_val_hour_type_id = s_loc_val_hour_type.s_loc_val_hour_type_id
 where s_loc_val_hour_type.s_loc_val_hour_type_id is null
    or (s_loc_val_hour_type.s_loc_val_hour_type_id is not null
        and s_loc_val_hour_type.dv_hash <> #s_loc_val_hour_type_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_loc_val_hour_type @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_loc_val_hour_type @current_dv_batch_id

end
