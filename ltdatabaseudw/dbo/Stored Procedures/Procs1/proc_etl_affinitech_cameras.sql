﻿CREATE PROC [dbo].[proc_etl_affinitech_cameras] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_affinitech_cameras

set @insert_date_time = getdate()
insert into dbo.stage_hash_affinitech_cameras (
       bk_hash,
       cam_id,
       cam_club,
       cam_name,
       cam_ip,
       cam_inverted,
       studio,
       cam_club_it,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cam_id,'z#@$k%&P'))),2) bk_hash,
       cam_id,
       cam_club,
       cam_name,
       cam_ip,
       cam_inverted,
       studio,
       cam_club_it,
       dummy_modified_date_time,
       isnull(cast(stage_affinitech_cameras.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_affinitech_cameras
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_affinitech_cameras @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_affinitech_cameras (
       bk_hash,
       cam_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_affinitech_cameras.bk_hash,
       stage_hash_affinitech_cameras.cam_id cam_id,
       isnull(cast(stage_hash_affinitech_cameras.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       40,
       @insert_date_time,
       @user
  from stage_hash_affinitech_cameras
  left join h_affinitech_cameras
    on stage_hash_affinitech_cameras.bk_hash = h_affinitech_cameras.bk_hash
 where h_affinitech_cameras_id is null
   and stage_hash_affinitech_cameras.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_affinitech_cameras
if object_id('tempdb..#l_affinitech_cameras_inserts') is not null drop table #l_affinitech_cameras_inserts
create table #l_affinitech_cameras_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_affinitech_cameras.bk_hash,
       stage_hash_affinitech_cameras.cam_id cam_id,
       stage_hash_affinitech_cameras.cam_club cam_club,
       isnull(cast(stage_hash_affinitech_cameras.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_club,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_affinitech_cameras
 where stage_hash_affinitech_cameras.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_affinitech_cameras records
set @insert_date_time = getdate()
insert into l_affinitech_cameras (
       bk_hash,
       cam_id,
       cam_club,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_affinitech_cameras_inserts.bk_hash,
       #l_affinitech_cameras_inserts.cam_id,
       #l_affinitech_cameras_inserts.cam_club,
       case when l_affinitech_cameras.l_affinitech_cameras_id is null then isnull(#l_affinitech_cameras_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #l_affinitech_cameras_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_affinitech_cameras_inserts
  left join p_affinitech_cameras
    on #l_affinitech_cameras_inserts.bk_hash = p_affinitech_cameras.bk_hash
   and p_affinitech_cameras.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_affinitech_cameras
    on p_affinitech_cameras.bk_hash = l_affinitech_cameras.bk_hash
   and p_affinitech_cameras.l_affinitech_cameras_id = l_affinitech_cameras.l_affinitech_cameras_id
 where l_affinitech_cameras.l_affinitech_cameras_id is null
    or (l_affinitech_cameras.l_affinitech_cameras_id is not null
        and l_affinitech_cameras.dv_hash <> #l_affinitech_cameras_inserts.source_hash)

--calculate hash and lookup to current s_affinitech_cameras
if object_id('tempdb..#s_affinitech_cameras_inserts') is not null drop table #s_affinitech_cameras_inserts
create table #s_affinitech_cameras_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_affinitech_cameras.bk_hash,
       stage_hash_affinitech_cameras.cam_id cam_id,
       stage_hash_affinitech_cameras.cam_name cam_name,
       stage_hash_affinitech_cameras.cam_ip cam_ip,
       stage_hash_affinitech_cameras.cam_inverted cam_inverted,
       stage_hash_affinitech_cameras.studio studio,
       stage_hash_affinitech_cameras.cam_club_it cam_club_it,
       stage_hash_affinitech_cameras.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_affinitech_cameras.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_ip,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_cameras.cam_inverted as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_cameras.studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_cameras.cam_club_it,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_affinitech_cameras
 where stage_hash_affinitech_cameras.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_affinitech_cameras records
set @insert_date_time = getdate()
insert into s_affinitech_cameras (
       bk_hash,
       cam_id,
       cam_name,
       cam_ip,
       cam_inverted,
       studio,
       cam_club_it,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_affinitech_cameras_inserts.bk_hash,
       #s_affinitech_cameras_inserts.cam_id,
       #s_affinitech_cameras_inserts.cam_name,
       #s_affinitech_cameras_inserts.cam_ip,
       #s_affinitech_cameras_inserts.cam_inverted,
       #s_affinitech_cameras_inserts.studio,
       #s_affinitech_cameras_inserts.cam_club_it,
       #s_affinitech_cameras_inserts.dummy_modified_date_time,
       case when s_affinitech_cameras.s_affinitech_cameras_id is null then isnull(#s_affinitech_cameras_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #s_affinitech_cameras_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_affinitech_cameras_inserts
  left join p_affinitech_cameras
    on #s_affinitech_cameras_inserts.bk_hash = p_affinitech_cameras.bk_hash
   and p_affinitech_cameras.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_affinitech_cameras
    on p_affinitech_cameras.bk_hash = s_affinitech_cameras.bk_hash
   and p_affinitech_cameras.s_affinitech_cameras_id = s_affinitech_cameras.s_affinitech_cameras_id
 where s_affinitech_cameras.s_affinitech_cameras_id is null
    or (s_affinitech_cameras.s_affinitech_cameras_id is not null
        and s_affinitech_cameras.dv_hash <> #s_affinitech_cameras_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_affinitech_cameras @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_affinitech_cameras @current_dv_batch_id

end
