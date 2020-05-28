CREATE PROC [dbo].[proc_etl_affinitech_camera_count] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_affinitech_camera_count

set @insert_date_time = getdate()
insert into dbo.stage_hash_affinitech_camera_count (
       bk_hash,
       DoorDescription,
       StartRange,
       SourceIP,
       EventType,
       DivisionID,
       SiteID,
       DoorID,
       DoorType,
       Enters,
       Exits,
       CumulativeEnters,
       CumulativeExits,
       FileName,
       InsertedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(DoorDescription,'z#@$k%&P')+'P%#&z$@k'+isnull(convert(varchar,StartRange,120),'z#@$k%&P')+'P%#&z$@k'+isnull(SourceIP,'z#@$k%&P'))),2) bk_hash,
       DoorDescription,
       StartRange,
       SourceIP,
       EventType,
       DivisionID,
       SiteID,
       DoorID,
       DoorType,
       Enters,
       Exits,
       CumulativeEnters,
       CumulativeExits,
       FileName,
       InsertedDateTime,
       isnull(cast(stage_affinitech_camera_count.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_affinitech_camera_count
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_affinitech_camera_count @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_affinitech_camera_count (
       bk_hash,
       Door_Description,
       Start_Range,
       Source_IP,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_affinitech_camera_count.bk_hash,
       stage_hash_affinitech_camera_count.DoorDescription Door_Description,
       stage_hash_affinitech_camera_count.StartRange Start_Range,
       stage_hash_affinitech_camera_count.SourceIP Source_IP,
       isnull(cast(stage_hash_affinitech_camera_count.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       40,
       @insert_date_time,
       @user
  from stage_hash_affinitech_camera_count
  left join h_affinitech_camera_count
    on stage_hash_affinitech_camera_count.bk_hash = h_affinitech_camera_count.bk_hash
 where h_affinitech_camera_count_id is null
   and stage_hash_affinitech_camera_count.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_affinitech_camera_count
if object_id('tempdb..#l_affinitech_camera_count_inserts') is not null drop table #l_affinitech_camera_count_inserts
create table #l_affinitech_camera_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_affinitech_camera_count.bk_hash,
       stage_hash_affinitech_camera_count.DoorDescription Door_Description,
       stage_hash_affinitech_camera_count.StartRange Start_Range,
       stage_hash_affinitech_camera_count.SourceIP Source_IP,
       stage_hash_affinitech_camera_count.DivisionID Division_ID,
       stage_hash_affinitech_camera_count.SiteID Site_ID,
       stage_hash_affinitech_camera_count.DoorID Door_ID,
       isnull(cast(stage_hash_affinitech_camera_count.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.DoorDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_affinitech_camera_count.StartRange,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.SourceIP,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.DivisionID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.SiteID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.DoorID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_affinitech_camera_count
 where stage_hash_affinitech_camera_count.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_affinitech_camera_count records
set @insert_date_time = getdate()
insert into l_affinitech_camera_count (
       bk_hash,
       Door_Description,
       Start_Range,
       Source_IP,
       Division_ID,
       Site_ID,
       Door_ID,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_affinitech_camera_count_inserts.bk_hash,
       #l_affinitech_camera_count_inserts.Door_Description,
       #l_affinitech_camera_count_inserts.Start_Range,
       #l_affinitech_camera_count_inserts.Source_IP,
       #l_affinitech_camera_count_inserts.Division_ID,
       #l_affinitech_camera_count_inserts.Site_ID,
       #l_affinitech_camera_count_inserts.Door_ID,
       case when l_affinitech_camera_count.l_affinitech_camera_count_id is null then isnull(#l_affinitech_camera_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #l_affinitech_camera_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_affinitech_camera_count_inserts
  left join p_affinitech_camera_count
    on #l_affinitech_camera_count_inserts.bk_hash = p_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_affinitech_camera_count
    on p_affinitech_camera_count.bk_hash = l_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.l_affinitech_camera_count_id = l_affinitech_camera_count.l_affinitech_camera_count_id
 where l_affinitech_camera_count.l_affinitech_camera_count_id is null
    or (l_affinitech_camera_count.l_affinitech_camera_count_id is not null
        and l_affinitech_camera_count.dv_hash <> #l_affinitech_camera_count_inserts.source_hash)

--calculate hash and lookup to current s_affinitech_camera_count
if object_id('tempdb..#s_affinitech_camera_count_inserts') is not null drop table #s_affinitech_camera_count_inserts
create table #s_affinitech_camera_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_affinitech_camera_count.bk_hash,
       stage_hash_affinitech_camera_count.DoorDescription Door_Description,
       stage_hash_affinitech_camera_count.StartRange Start_Range,
       stage_hash_affinitech_camera_count.SourceIP Source_IP,
       stage_hash_affinitech_camera_count.EventType Event_Type,
       stage_hash_affinitech_camera_count.DoorType Door_Type,
       stage_hash_affinitech_camera_count.Enters Enters,
       stage_hash_affinitech_camera_count.Exits Exits,
       stage_hash_affinitech_camera_count.CumulativeEnters Cumulative_Enters,
       stage_hash_affinitech_camera_count.CumulativeExits Cumulative_Exits,
       stage_hash_affinitech_camera_count.FileName File_Name,
       stage_hash_affinitech_camera_count.InsertedDateTime Inserted_DateTime,
       isnull(cast(stage_hash_affinitech_camera_count.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.DoorDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_affinitech_camera_count.StartRange,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.SourceIP,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.EventType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.DoorType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.Enters as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.Exits as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.CumulativeEnters as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_affinitech_camera_count.CumulativeExits as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_affinitech_camera_count.FileName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_affinitech_camera_count.InsertedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_affinitech_camera_count
 where stage_hash_affinitech_camera_count.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_affinitech_camera_count records
set @insert_date_time = getdate()
insert into s_affinitech_camera_count (
       bk_hash,
       Door_Description,
       Start_Range,
       Source_IP,
       Event_Type,
       Door_Type,
       Enters,
       Exits,
       Cumulative_Enters,
       Cumulative_Exits,
       File_Name,
       Inserted_DateTime,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_affinitech_camera_count_inserts.bk_hash,
       #s_affinitech_camera_count_inserts.Door_Description,
       #s_affinitech_camera_count_inserts.Start_Range,
       #s_affinitech_camera_count_inserts.Source_IP,
       #s_affinitech_camera_count_inserts.Event_Type,
       #s_affinitech_camera_count_inserts.Door_Type,
       #s_affinitech_camera_count_inserts.Enters,
       #s_affinitech_camera_count_inserts.Exits,
       #s_affinitech_camera_count_inserts.Cumulative_Enters,
       #s_affinitech_camera_count_inserts.Cumulative_Exits,
       #s_affinitech_camera_count_inserts.File_Name,
       #s_affinitech_camera_count_inserts.Inserted_DateTime,
       case when s_affinitech_camera_count.s_affinitech_camera_count_id is null then isnull(#s_affinitech_camera_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #s_affinitech_camera_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_affinitech_camera_count_inserts
  left join p_affinitech_camera_count
    on #s_affinitech_camera_count_inserts.bk_hash = p_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_affinitech_camera_count
    on p_affinitech_camera_count.bk_hash = s_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.s_affinitech_camera_count_id = s_affinitech_camera_count.s_affinitech_camera_count_id
 where s_affinitech_camera_count.s_affinitech_camera_count_id is null
    or (s_affinitech_camera_count.s_affinitech_camera_count_id is not null
        and s_affinitech_camera_count.dv_hash <> #s_affinitech_camera_count_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_affinitech_camera_count @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_affinitech_camera_count @current_dv_batch_id

end
