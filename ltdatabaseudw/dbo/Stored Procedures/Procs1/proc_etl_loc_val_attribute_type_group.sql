CREATE PROC [dbo].[proc_etl_loc_val_attribute_type_group] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_loc_val_attribute_type_group

set @insert_date_time = getdate()
insert into dbo.stage_hash_loc_val_attribute_type_group (
       bk_hash,
       val_attribute_type_group_id,
       val_attribute_type_group_name,
       display_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(val_attribute_type_group_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       val_attribute_type_group_id,
       val_attribute_type_group_name,
       display_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       isnull(cast(stage_loc_val_attribute_type_group.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_loc_val_attribute_type_group
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_loc_val_attribute_type_group @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_loc_val_attribute_type_group (
       bk_hash,
       val_attribute_type_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_loc_val_attribute_type_group.bk_hash,
       stage_hash_loc_val_attribute_type_group.val_attribute_type_group_id val_attribute_type_group_id,
       isnull(cast(stage_hash_loc_val_attribute_type_group.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       50,
       @insert_date_time,
       @user
  from stage_hash_loc_val_attribute_type_group
  left join h_loc_val_attribute_type_group
    on stage_hash_loc_val_attribute_type_group.bk_hash = h_loc_val_attribute_type_group.bk_hash
 where h_loc_val_attribute_type_group_id is null
   and stage_hash_loc_val_attribute_type_group.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_loc_val_attribute_type_group
if object_id('tempdb..#s_loc_val_attribute_type_group_inserts') is not null drop table #s_loc_val_attribute_type_group_inserts
create table #s_loc_val_attribute_type_group_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_val_attribute_type_group.bk_hash,
       stage_hash_loc_val_attribute_type_group.val_attribute_type_group_id val_attribute_type_group_id,
       stage_hash_loc_val_attribute_type_group.val_attribute_type_group_name val_attribute_type_group_name,
       stage_hash_loc_val_attribute_type_group.display_name display_name,
       stage_hash_loc_val_attribute_type_group.created_date_time created_date_time,
       stage_hash_loc_val_attribute_type_group.created_by created_by,
       stage_hash_loc_val_attribute_type_group.last_updated_date_time last_updated_date_time,
       stage_hash_loc_val_attribute_type_group.last_updated_by last_updated_by,
       stage_hash_loc_val_attribute_type_group.deleted_date_time deleted_date_time,
       stage_hash_loc_val_attribute_type_group.deleted_by deleted_by,
       isnull(cast(stage_hash_loc_val_attribute_type_group.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_val_attribute_type_group.val_attribute_type_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_attribute_type_group.val_attribute_type_group_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_attribute_type_group.display_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_attribute_type_group.created_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_attribute_type_group.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_attribute_type_group.last_updated_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_attribute_type_group.last_updated_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_val_attribute_type_group.deleted_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_val_attribute_type_group.deleted_by,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_val_attribute_type_group
 where stage_hash_loc_val_attribute_type_group.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_loc_val_attribute_type_group records
set @insert_date_time = getdate()
insert into s_loc_val_attribute_type_group (
       bk_hash,
       val_attribute_type_group_id,
       val_attribute_type_group_name,
       display_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_loc_val_attribute_type_group_inserts.bk_hash,
       #s_loc_val_attribute_type_group_inserts.val_attribute_type_group_id,
       #s_loc_val_attribute_type_group_inserts.val_attribute_type_group_name,
       #s_loc_val_attribute_type_group_inserts.display_name,
       #s_loc_val_attribute_type_group_inserts.created_date_time,
       #s_loc_val_attribute_type_group_inserts.created_by,
       #s_loc_val_attribute_type_group_inserts.last_updated_date_time,
       #s_loc_val_attribute_type_group_inserts.last_updated_by,
       #s_loc_val_attribute_type_group_inserts.deleted_date_time,
       #s_loc_val_attribute_type_group_inserts.deleted_by,
       case when s_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id is null then isnull(#s_loc_val_attribute_type_group_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #s_loc_val_attribute_type_group_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_loc_val_attribute_type_group_inserts
  left join p_loc_val_attribute_type_group
    on #s_loc_val_attribute_type_group_inserts.bk_hash = p_loc_val_attribute_type_group.bk_hash
   and p_loc_val_attribute_type_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_loc_val_attribute_type_group
    on p_loc_val_attribute_type_group.bk_hash = s_loc_val_attribute_type_group.bk_hash
   and p_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id = s_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id
 where s_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id is null
    or (s_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id is not null
        and s_loc_val_attribute_type_group.dv_hash <> #s_loc_val_attribute_type_group_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_loc_val_attribute_type_group @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_loc_val_attribute_type_group @current_dv_batch_id

end
