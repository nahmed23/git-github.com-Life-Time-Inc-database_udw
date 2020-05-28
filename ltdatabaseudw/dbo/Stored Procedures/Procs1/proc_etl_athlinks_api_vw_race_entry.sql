CREATE PROC [dbo].[proc_etl_athlinks_api_vw_race_entry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_athlinks_api_vw_RaceEntry

set @insert_date_time = getdate()
insert into dbo.stage_hash_athlinks_api_vw_RaceEntry (
       bk_hash,
       DisplayName,
       IsMember,
       PhotoPath,
       Age,
       BibNum,
       ClassID,
       ClassName,
       EntryStateProvID,
       EntryID,
       EventCourseID,
       Gender,
       RacerID,
       RankO,
       RankG,
       RankA,
       Ticks,
       TicksString,
       RaceID,
       CourseID,
       ResultCount,
       RaceDate,
       MasterID,
       MasterName,
       ResultsDate,
       TotalA,
       TotalG,
       TotalO,
       City,
       CreateDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EntryID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       DisplayName,
       IsMember,
       PhotoPath,
       Age,
       BibNum,
       ClassID,
       ClassName,
       EntryStateProvID,
       EntryID,
       EventCourseID,
       Gender,
       RacerID,
       RankO,
       RankG,
       RankA,
       Ticks,
       TicksString,
       RaceID,
       CourseID,
       ResultCount,
       RaceDate,
       MasterID,
       MasterName,
       ResultsDate,
       TotalA,
       TotalG,
       TotalO,
       City,
       CreateDate,
       isnull(cast(stage_athlinks_api_vw_RaceEntry.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_athlinks_api_vw_RaceEntry
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_athlinks_api_vw_race_entry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_athlinks_api_vw_race_entry (
       bk_hash,
       entry_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_athlinks_api_vw_RaceEntry.bk_hash,
       stage_hash_athlinks_api_vw_RaceEntry.EntryID entry_id,
       isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_athlinks_api_vw_RaceEntry
  left join h_athlinks_api_vw_race_entry
    on stage_hash_athlinks_api_vw_RaceEntry.bk_hash = h_athlinks_api_vw_race_entry.bk_hash
 where h_athlinks_api_vw_race_entry_id is null
   and stage_hash_athlinks_api_vw_RaceEntry.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_athlinks_api_vw_race_entry
if object_id('tempdb..#l_athlinks_api_vw_race_entry_inserts') is not null drop table #l_athlinks_api_vw_race_entry_inserts
create table #l_athlinks_api_vw_race_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_RaceEntry.bk_hash,
       stage_hash_athlinks_api_vw_RaceEntry.ClassID class_id,
       stage_hash_athlinks_api_vw_RaceEntry.EntryStateProvID entry_state_prov_id,
       stage_hash_athlinks_api_vw_RaceEntry.EntryID entry_id,
       stage_hash_athlinks_api_vw_RaceEntry.EventCourseID event_course_id,
       stage_hash_athlinks_api_vw_RaceEntry.RacerID racer_id,
       stage_hash_athlinks_api_vw_RaceEntry.RaceID race_id,
       stage_hash_athlinks_api_vw_RaceEntry.CourseID course_id,
       stage_hash_athlinks_api_vw_RaceEntry.MasterID master_id,
       isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.ClassID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.EntryStateProvID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.EntryID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.EventCourseID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.RacerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.RaceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.CourseID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.MasterID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_RaceEntry
 where stage_hash_athlinks_api_vw_RaceEntry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_athlinks_api_vw_race_entry records
set @insert_date_time = getdate()
insert into l_athlinks_api_vw_race_entry (
       bk_hash,
       class_id,
       entry_state_prov_id,
       entry_id,
       event_course_id,
       racer_id,
       race_id,
       course_id,
       master_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_athlinks_api_vw_race_entry_inserts.bk_hash,
       #l_athlinks_api_vw_race_entry_inserts.class_id,
       #l_athlinks_api_vw_race_entry_inserts.entry_state_prov_id,
       #l_athlinks_api_vw_race_entry_inserts.entry_id,
       #l_athlinks_api_vw_race_entry_inserts.event_course_id,
       #l_athlinks_api_vw_race_entry_inserts.racer_id,
       #l_athlinks_api_vw_race_entry_inserts.race_id,
       #l_athlinks_api_vw_race_entry_inserts.course_id,
       #l_athlinks_api_vw_race_entry_inserts.master_id,
       case when l_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id is null then isnull(#l_athlinks_api_vw_race_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #l_athlinks_api_vw_race_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_athlinks_api_vw_race_entry_inserts
  left join p_athlinks_api_vw_race_entry
    on #l_athlinks_api_vw_race_entry_inserts.bk_hash = p_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_athlinks_api_vw_race_entry
    on p_athlinks_api_vw_race_entry.bk_hash = l_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id = l_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id
 where l_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id is null
    or (l_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id is not null
        and l_athlinks_api_vw_race_entry.dv_hash <> #l_athlinks_api_vw_race_entry_inserts.source_hash)

--calculate hash and lookup to current s_athlinks_api_vw_race_entry
if object_id('tempdb..#s_athlinks_api_vw_race_entry_inserts') is not null drop table #s_athlinks_api_vw_race_entry_inserts
create table #s_athlinks_api_vw_race_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_RaceEntry.bk_hash,
       stage_hash_athlinks_api_vw_RaceEntry.DisplayName display_name,
       stage_hash_athlinks_api_vw_RaceEntry.IsMember is_member,
       stage_hash_athlinks_api_vw_RaceEntry.PhotoPath photo_path,
       stage_hash_athlinks_api_vw_RaceEntry.Age age,
       stage_hash_athlinks_api_vw_RaceEntry.BibNum bib_num,
       stage_hash_athlinks_api_vw_RaceEntry.ClassName class_name,
       stage_hash_athlinks_api_vw_RaceEntry.EntryID entry_id,
       stage_hash_athlinks_api_vw_RaceEntry.Gender gender,
       stage_hash_athlinks_api_vw_RaceEntry.RankO rank_o,
       stage_hash_athlinks_api_vw_RaceEntry.RankG rank_g,
       stage_hash_athlinks_api_vw_RaceEntry.RankA rank_a,
       stage_hash_athlinks_api_vw_RaceEntry.Ticks ticks,
       stage_hash_athlinks_api_vw_RaceEntry.TicksString ticks_string,
       stage_hash_athlinks_api_vw_RaceEntry.ResultCount result_count,
       stage_hash_athlinks_api_vw_RaceEntry.RaceDate race_date,
       stage_hash_athlinks_api_vw_RaceEntry.MasterName master_name,
       stage_hash_athlinks_api_vw_RaceEntry.ResultsDate results_date,
       stage_hash_athlinks_api_vw_RaceEntry.TotalA total_a,
       stage_hash_athlinks_api_vw_RaceEntry.TotalG total_g,
       stage_hash_athlinks_api_vw_RaceEntry.TotalO total_o,
       stage_hash_athlinks_api_vw_RaceEntry.City city,
       stage_hash_athlinks_api_vw_RaceEntry.CreateDate create_date,
       isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.DisplayName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.IsMember as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.PhotoPath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.Age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.BibNum,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.ClassName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.EntryID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.Gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.RankO as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.RankG as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.RankA as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.Ticks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.TicksString,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.ResultCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_RaceEntry.RaceDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.MasterName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_RaceEntry.ResultsDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.TotalA as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.TotalG as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_RaceEntry.TotalO as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_RaceEntry.City,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_RaceEntry
 where stage_hash_athlinks_api_vw_RaceEntry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_athlinks_api_vw_race_entry records
set @insert_date_time = getdate()
insert into s_athlinks_api_vw_race_entry (
       bk_hash,
       display_name,
       is_member,
       photo_path,
       age,
       bib_num,
       class_name,
       entry_id,
       gender,
       rank_o,
       rank_g,
       rank_a,
       ticks,
       ticks_string,
       result_count,
       race_date,
       master_name,
       results_date,
       total_a,
       total_g,
       total_o,
       city,
       create_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_athlinks_api_vw_race_entry_inserts.bk_hash,
       #s_athlinks_api_vw_race_entry_inserts.display_name,
       #s_athlinks_api_vw_race_entry_inserts.is_member,
       #s_athlinks_api_vw_race_entry_inserts.photo_path,
       #s_athlinks_api_vw_race_entry_inserts.age,
       #s_athlinks_api_vw_race_entry_inserts.bib_num,
       #s_athlinks_api_vw_race_entry_inserts.class_name,
       #s_athlinks_api_vw_race_entry_inserts.entry_id,
       #s_athlinks_api_vw_race_entry_inserts.gender,
       #s_athlinks_api_vw_race_entry_inserts.rank_o,
       #s_athlinks_api_vw_race_entry_inserts.rank_g,
       #s_athlinks_api_vw_race_entry_inserts.rank_a,
       #s_athlinks_api_vw_race_entry_inserts.ticks,
       #s_athlinks_api_vw_race_entry_inserts.ticks_string,
       #s_athlinks_api_vw_race_entry_inserts.result_count,
       #s_athlinks_api_vw_race_entry_inserts.race_date,
       #s_athlinks_api_vw_race_entry_inserts.master_name,
       #s_athlinks_api_vw_race_entry_inserts.results_date,
       #s_athlinks_api_vw_race_entry_inserts.total_a,
       #s_athlinks_api_vw_race_entry_inserts.total_g,
       #s_athlinks_api_vw_race_entry_inserts.total_o,
       #s_athlinks_api_vw_race_entry_inserts.city,
       #s_athlinks_api_vw_race_entry_inserts.create_date,
       case when s_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id is null then isnull(#s_athlinks_api_vw_race_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_athlinks_api_vw_race_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_athlinks_api_vw_race_entry_inserts
  left join p_athlinks_api_vw_race_entry
    on #s_athlinks_api_vw_race_entry_inserts.bk_hash = p_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_athlinks_api_vw_race_entry
    on p_athlinks_api_vw_race_entry.bk_hash = s_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id = s_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id
 where s_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id is null
    or (s_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id is not null
        and s_athlinks_api_vw_race_entry.dv_hash <> #s_athlinks_api_vw_race_entry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_athlinks_api_vw_race_entry @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_athlinks_api_vw_race_entry @current_dv_batch_id

end
