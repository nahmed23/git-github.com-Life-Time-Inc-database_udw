CREATE PROC [dbo].[proc_etl_loc_location] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_loc_location

set @insert_date_time = getdate()
insert into dbo.stage_hash_loc_location (
       bk_hash,
       location_id,
       udw_business_key,
       val_location_type_id,
       udw_dim_location_key,
       description,
       display_name,
       top_level_location_id,
       udw_source_name,
       parent_location_id,
       hierarchy_level,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       managed_by_udw_flag,
       slug,
       external_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(location_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       location_id,
       udw_business_key,
       val_location_type_id,
       udw_dim_location_key,
       description,
       display_name,
       top_level_location_id,
       udw_source_name,
       parent_location_id,
       hierarchy_level,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       managed_by_udw_flag,
       slug,
       external_id,
       isnull(cast(stage_loc_location.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_loc_location
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_loc_location @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_loc_location (
       bk_hash,
       location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_loc_location.bk_hash,
       stage_hash_loc_location.location_id location_id,
       isnull(cast(stage_hash_loc_location.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       50,
       @insert_date_time,
       @user
  from stage_hash_loc_location
  left join h_loc_location
    on stage_hash_loc_location.bk_hash = h_loc_location.bk_hash
 where h_loc_location_id is null
   and stage_hash_loc_location.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_loc_location
if object_id('tempdb..#l_loc_location_inserts') is not null drop table #l_loc_location_inserts
create table #l_loc_location_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_location.bk_hash,
       stage_hash_loc_location.location_id location_id,
       stage_hash_loc_location.val_location_type_id val_location_type_id,
       stage_hash_loc_location.top_level_location_id top_level_location_id,
       stage_hash_loc_location.parent_location_id parent_location_id,
       stage_hash_loc_location.external_id external_id,
       isnull(cast(stage_hash_loc_location.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_location.location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_location.val_location_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_location.top_level_location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_location.parent_location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_location.external_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_location
 where stage_hash_loc_location.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_loc_location records
set @insert_date_time = getdate()
insert into l_loc_location (
       bk_hash,
       location_id,
       val_location_type_id,
       top_level_location_id,
       parent_location_id,
       external_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_loc_location_inserts.bk_hash,
       #l_loc_location_inserts.location_id,
       #l_loc_location_inserts.val_location_type_id,
       #l_loc_location_inserts.top_level_location_id,
       #l_loc_location_inserts.parent_location_id,
       #l_loc_location_inserts.external_id,
       case when l_loc_location.l_loc_location_id is null then isnull(#l_loc_location_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #l_loc_location_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_loc_location_inserts
  left join p_loc_location
    on #l_loc_location_inserts.bk_hash = p_loc_location.bk_hash
   and p_loc_location.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_loc_location
    on p_loc_location.bk_hash = l_loc_location.bk_hash
   and p_loc_location.l_loc_location_id = l_loc_location.l_loc_location_id
 where l_loc_location.l_loc_location_id is null
    or (l_loc_location.l_loc_location_id is not null
        and l_loc_location.dv_hash <> #l_loc_location_inserts.source_hash)

--calculate hash and lookup to current s_loc_location
if object_id('tempdb..#s_loc_location_inserts') is not null drop table #s_loc_location_inserts
create table #s_loc_location_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_location.bk_hash,
       stage_hash_loc_location.location_id location_id,
       stage_hash_loc_location.udw_business_key udw_business_key,
       stage_hash_loc_location.udw_dim_location_key udw_dim_location_key,
       stage_hash_loc_location.description description,
       stage_hash_loc_location.display_name display_name,
       stage_hash_loc_location.udw_source_name udw_source_name,
       stage_hash_loc_location.hierarchy_level hierarchy_level,
       stage_hash_loc_location.created_date_time created_date_time,
       stage_hash_loc_location.created_by created_by,
       stage_hash_loc_location.deleted_date_time deleted_date_time,
       stage_hash_loc_location.deleted_by deleted_by,
       stage_hash_loc_location.last_updated_date_time last_updated_date_time,
       stage_hash_loc_location.last_updated_by last_updated_by,
       stage_hash_loc_location.managed_by_udw_flag managed_by_udw_flag,
       stage_hash_loc_location.slug slug,
       isnull(cast(stage_hash_loc_location.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_location.location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.udw_business_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.udw_dim_location_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.display_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.udw_source_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_location.hierarchy_level as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_location.created_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_location.deleted_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.deleted_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_location.last_updated_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.last_updated_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.managed_by_udw_flag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_location.slug,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_location
 where stage_hash_loc_location.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_loc_location records
set @insert_date_time = getdate()
insert into s_loc_location (
       bk_hash,
       location_id,
       udw_business_key,
       udw_dim_location_key,
       description,
       display_name,
       udw_source_name,
       hierarchy_level,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       managed_by_udw_flag,
       slug,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_loc_location_inserts.bk_hash,
       #s_loc_location_inserts.location_id,
       #s_loc_location_inserts.udw_business_key,
       #s_loc_location_inserts.udw_dim_location_key,
       #s_loc_location_inserts.description,
       #s_loc_location_inserts.display_name,
       #s_loc_location_inserts.udw_source_name,
       #s_loc_location_inserts.hierarchy_level,
       #s_loc_location_inserts.created_date_time,
       #s_loc_location_inserts.created_by,
       #s_loc_location_inserts.deleted_date_time,
       #s_loc_location_inserts.deleted_by,
       #s_loc_location_inserts.last_updated_date_time,
       #s_loc_location_inserts.last_updated_by,
       #s_loc_location_inserts.managed_by_udw_flag,
       #s_loc_location_inserts.slug,
       case when s_loc_location.s_loc_location_id is null then isnull(#s_loc_location_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #s_loc_location_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_loc_location_inserts
  left join p_loc_location
    on #s_loc_location_inserts.bk_hash = p_loc_location.bk_hash
   and p_loc_location.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_loc_location
    on p_loc_location.bk_hash = s_loc_location.bk_hash
   and p_loc_location.s_loc_location_id = s_loc_location.s_loc_location_id
 where s_loc_location.s_loc_location_id is null
    or (s_loc_location.s_loc_location_id is not null
        and s_loc_location.dv_hash <> #s_loc_location_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_loc_location @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_loc_location @current_dv_batch_id

end
