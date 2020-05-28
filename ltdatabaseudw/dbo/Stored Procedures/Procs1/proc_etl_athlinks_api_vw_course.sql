CREATE PROC [dbo].[proc_etl_athlinks_api_vw_course] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_athlinks_api_vw_Course

set @insert_date_time = getdate()
insert into dbo.stage_hash_athlinks_api_vw_Course (
       bk_hash,
       CourseID,
       RaceID,
       CourseName,
       RaceCatID,
       RaceCatDesc,
       CoursePatternID,
       CoursePattern,
       CoursePatternOuterID,
       CoursePatternOuterName,
       OverallCount,
       EventCourseID,
       Settings,
       ResultsDate,
       GalleryID,
       DistUnit,
       DistTypeID,
       ResultsUser,
       CreateDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CourseID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       CourseID,
       RaceID,
       CourseName,
       RaceCatID,
       RaceCatDesc,
       CoursePatternID,
       CoursePattern,
       CoursePatternOuterID,
       CoursePatternOuterName,
       OverallCount,
       EventCourseID,
       Settings,
       ResultsDate,
       GalleryID,
       DistUnit,
       DistTypeID,
       ResultsUser,
       CreateDate,
       isnull(cast(stage_athlinks_api_vw_Course.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_athlinks_api_vw_Course
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_athlinks_api_vw_course @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_athlinks_api_vw_course (
       bk_hash,
       course_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_athlinks_api_vw_Course.bk_hash,
       stage_hash_athlinks_api_vw_Course.CourseID course_id,
       isnull(cast(stage_hash_athlinks_api_vw_Course.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_athlinks_api_vw_Course
  left join h_athlinks_api_vw_course
    on stage_hash_athlinks_api_vw_Course.bk_hash = h_athlinks_api_vw_course.bk_hash
 where h_athlinks_api_vw_course_id is null
   and stage_hash_athlinks_api_vw_Course.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_athlinks_api_vw_course
if object_id('tempdb..#l_athlinks_api_vw_course_inserts') is not null drop table #l_athlinks_api_vw_course_inserts
create table #l_athlinks_api_vw_course_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_Course.bk_hash,
       stage_hash_athlinks_api_vw_Course.CourseID course_id,
       stage_hash_athlinks_api_vw_Course.RaceID race_id,
       stage_hash_athlinks_api_vw_Course.RaceCatID race_cat_id,
       stage_hash_athlinks_api_vw_Course.CoursePatternID course_pattern_id,
       stage_hash_athlinks_api_vw_Course.CoursePatternOuterID course_pattern_outer_id,
       stage_hash_athlinks_api_vw_Course.EventCourseID event_course_id,
       isnull(cast(stage_hash_athlinks_api_vw_Course.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.CourseID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.RaceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.RaceCatID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.CoursePatternID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.CoursePatternOuterID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.EventCourseID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_Course
 where stage_hash_athlinks_api_vw_Course.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_athlinks_api_vw_course records
set @insert_date_time = getdate()
insert into l_athlinks_api_vw_course (
       bk_hash,
       course_id,
       race_id,
       race_cat_id,
       course_pattern_id,
       course_pattern_outer_id,
       event_course_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_athlinks_api_vw_course_inserts.bk_hash,
       #l_athlinks_api_vw_course_inserts.course_id,
       #l_athlinks_api_vw_course_inserts.race_id,
       #l_athlinks_api_vw_course_inserts.race_cat_id,
       #l_athlinks_api_vw_course_inserts.course_pattern_id,
       #l_athlinks_api_vw_course_inserts.course_pattern_outer_id,
       #l_athlinks_api_vw_course_inserts.event_course_id,
       case when l_athlinks_api_vw_course.l_athlinks_api_vw_course_id is null then isnull(#l_athlinks_api_vw_course_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #l_athlinks_api_vw_course_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_athlinks_api_vw_course_inserts
  left join p_athlinks_api_vw_course
    on #l_athlinks_api_vw_course_inserts.bk_hash = p_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_athlinks_api_vw_course
    on p_athlinks_api_vw_course.bk_hash = l_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.l_athlinks_api_vw_course_id = l_athlinks_api_vw_course.l_athlinks_api_vw_course_id
 where l_athlinks_api_vw_course.l_athlinks_api_vw_course_id is null
    or (l_athlinks_api_vw_course.l_athlinks_api_vw_course_id is not null
        and l_athlinks_api_vw_course.dv_hash <> #l_athlinks_api_vw_course_inserts.source_hash)

--calculate hash and lookup to current s_athlinks_api_vw_course
if object_id('tempdb..#s_athlinks_api_vw_course_inserts') is not null drop table #s_athlinks_api_vw_course_inserts
create table #s_athlinks_api_vw_course_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_Course.bk_hash,
       stage_hash_athlinks_api_vw_Course.CourseID course_id,
       stage_hash_athlinks_api_vw_Course.CourseName course_name,
       stage_hash_athlinks_api_vw_Course.RaceCatDesc race_cat_desc,
       stage_hash_athlinks_api_vw_Course.CoursePattern course_pattern,
       stage_hash_athlinks_api_vw_Course.CoursePatternOuterName course_pattern_outer_name,
       stage_hash_athlinks_api_vw_Course.OverallCount overall_count,
       stage_hash_athlinks_api_vw_Course.Settings settings,
       stage_hash_athlinks_api_vw_Course.ResultsDate results_date,
       stage_hash_athlinks_api_vw_Course.GalleryID gallery_id,
       stage_hash_athlinks_api_vw_Course.DistUnit dist_unit,
       stage_hash_athlinks_api_vw_Course.DistTypeID dist_type_id,
       stage_hash_athlinks_api_vw_Course.ResultsUser results_user,
       stage_hash_athlinks_api_vw_Course.CreateDate create_date,
       isnull(cast(stage_hash_athlinks_api_vw_Course.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.CourseID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Course.CourseName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Course.RaceCatDesc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Course.CoursePattern,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Course.CoursePatternOuterName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.OverallCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.Settings as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_Course.ResultsDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.GalleryID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.DistUnit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.DistTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Course.ResultsUser as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_Course
 where stage_hash_athlinks_api_vw_Course.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_athlinks_api_vw_course records
set @insert_date_time = getdate()
insert into s_athlinks_api_vw_course (
       bk_hash,
       course_id,
       course_name,
       race_cat_desc,
       course_pattern,
       course_pattern_outer_name,
       overall_count,
       settings,
       results_date,
       gallery_id,
       dist_unit,
       dist_type_id,
       results_user,
       create_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_athlinks_api_vw_course_inserts.bk_hash,
       #s_athlinks_api_vw_course_inserts.course_id,
       #s_athlinks_api_vw_course_inserts.course_name,
       #s_athlinks_api_vw_course_inserts.race_cat_desc,
       #s_athlinks_api_vw_course_inserts.course_pattern,
       #s_athlinks_api_vw_course_inserts.course_pattern_outer_name,
       #s_athlinks_api_vw_course_inserts.overall_count,
       #s_athlinks_api_vw_course_inserts.settings,
       #s_athlinks_api_vw_course_inserts.results_date,
       #s_athlinks_api_vw_course_inserts.gallery_id,
       #s_athlinks_api_vw_course_inserts.dist_unit,
       #s_athlinks_api_vw_course_inserts.dist_type_id,
       #s_athlinks_api_vw_course_inserts.results_user,
       #s_athlinks_api_vw_course_inserts.create_date,
       case when s_athlinks_api_vw_course.s_athlinks_api_vw_course_id is null then isnull(#s_athlinks_api_vw_course_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_athlinks_api_vw_course_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_athlinks_api_vw_course_inserts
  left join p_athlinks_api_vw_course
    on #s_athlinks_api_vw_course_inserts.bk_hash = p_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_athlinks_api_vw_course
    on p_athlinks_api_vw_course.bk_hash = s_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.s_athlinks_api_vw_course_id = s_athlinks_api_vw_course.s_athlinks_api_vw_course_id
 where s_athlinks_api_vw_course.s_athlinks_api_vw_course_id is null
    or (s_athlinks_api_vw_course.s_athlinks_api_vw_course_id is not null
        and s_athlinks_api_vw_course.dv_hash <> #s_athlinks_api_vw_course_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_athlinks_api_vw_course @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_athlinks_api_vw_course @current_dv_batch_id

end
