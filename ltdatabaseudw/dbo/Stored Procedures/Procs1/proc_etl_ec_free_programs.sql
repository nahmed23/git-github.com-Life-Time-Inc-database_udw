CREATE PROC [dbo].[proc_etl_ec_free_programs] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_FreePrograms

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_FreePrograms (
       bk_hash,
       ProgramId,
       ProgramImage,
       ProgramName,
       ProgramDescription,
       Featured,
       Priority,
       Frequency,
       Duration,
       Equipment,
       Exercise,
       Level,
       Goal,
       IsActive,
       CreatedDate,
       UpdatedDate,
       FreeProgramId,
       EndDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(FreeProgramId as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ProgramId,
       ProgramImage,
       ProgramName,
       ProgramDescription,
       Featured,
       Priority,
       Frequency,
       Duration,
       Equipment,
       Exercise,
       Level,
       Goal,
       IsActive,
       CreatedDate,
       UpdatedDate,
       FreeProgramId,
       EndDate,
       isnull(cast(stage_ec_FreePrograms.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_FreePrograms
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_free_programs @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_free_programs (
       bk_hash,
       free_program_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_FreePrograms.bk_hash,
       stage_hash_ec_FreePrograms.FreeProgramId free_program_id,
       isnull(cast(stage_hash_ec_FreePrograms.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_FreePrograms
  left join h_ec_free_programs
    on stage_hash_ec_FreePrograms.bk_hash = h_ec_free_programs.bk_hash
 where h_ec_free_programs_id is null
   and stage_hash_ec_FreePrograms.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_free_programs
if object_id('tempdb..#l_ec_free_programs_inserts') is not null drop table #l_ec_free_programs_inserts
create table #l_ec_free_programs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_FreePrograms.bk_hash,
       stage_hash_ec_FreePrograms.ProgramId program_id,
       stage_hash_ec_FreePrograms.FreeProgramId free_program_id,
       isnull(cast(stage_hash_ec_FreePrograms.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.ProgramId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.FreeProgramId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_FreePrograms
 where stage_hash_ec_FreePrograms.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_free_programs records
set @insert_date_time = getdate()
insert into l_ec_free_programs (
       bk_hash,
       program_id,
       free_program_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_free_programs_inserts.bk_hash,
       #l_ec_free_programs_inserts.program_id,
       #l_ec_free_programs_inserts.free_program_id,
       case when l_ec_free_programs.l_ec_free_programs_id is null then isnull(#l_ec_free_programs_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_free_programs_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_free_programs_inserts
  left join p_ec_free_programs
    on #l_ec_free_programs_inserts.bk_hash = p_ec_free_programs.bk_hash
   and p_ec_free_programs.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_free_programs
    on p_ec_free_programs.bk_hash = l_ec_free_programs.bk_hash
   and p_ec_free_programs.l_ec_free_programs_id = l_ec_free_programs.l_ec_free_programs_id
 where l_ec_free_programs.l_ec_free_programs_id is null
    or (l_ec_free_programs.l_ec_free_programs_id is not null
        and l_ec_free_programs.dv_hash <> #l_ec_free_programs_inserts.source_hash)

--calculate hash and lookup to current s_ec_free_programs
if object_id('tempdb..#s_ec_free_programs_inserts') is not null drop table #s_ec_free_programs_inserts
create table #s_ec_free_programs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_FreePrograms.bk_hash,
       stage_hash_ec_FreePrograms.ProgramImage program_image,
       stage_hash_ec_FreePrograms.ProgramName program_name,
       stage_hash_ec_FreePrograms.ProgramDescription program_description,
       stage_hash_ec_FreePrograms.Featured featured,
       stage_hash_ec_FreePrograms.Priority priority,
       stage_hash_ec_FreePrograms.Frequency frequency,
       stage_hash_ec_FreePrograms.Duration duration,
       stage_hash_ec_FreePrograms.Equipment equipment,
       stage_hash_ec_FreePrograms.Exercise exercise,
       stage_hash_ec_FreePrograms.Level level,
       stage_hash_ec_FreePrograms.Goal goal,
       stage_hash_ec_FreePrograms.IsActive is_active,
       stage_hash_ec_FreePrograms.CreatedDate created_date,
       stage_hash_ec_FreePrograms.UpdatedDate updated_date,
       stage_hash_ec_FreePrograms.FreeProgramId free_program_id,
       stage_hash_ec_FreePrograms.EndDate end_date,
       isnull(cast(stage_hash_ec_FreePrograms.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.ProgramImage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.ProgramName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.ProgramDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.Featured as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.Priority as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Frequency,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Duration,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Equipment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Exercise,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Level,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_FreePrograms.Goal,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.IsActive as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_FreePrograms.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_FreePrograms.UpdatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_FreePrograms.FreeProgramId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_FreePrograms.EndDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_FreePrograms
 where stage_hash_ec_FreePrograms.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_free_programs records
set @insert_date_time = getdate()
insert into s_ec_free_programs (
       bk_hash,
       program_image,
       program_name,
       program_description,
       featured,
       priority,
       frequency,
       duration,
       equipment,
       exercise,
       level,
       goal,
       is_active,
       created_date,
       updated_date,
       free_program_id,
       end_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_free_programs_inserts.bk_hash,
       #s_ec_free_programs_inserts.program_image,
       #s_ec_free_programs_inserts.program_name,
       #s_ec_free_programs_inserts.program_description,
       #s_ec_free_programs_inserts.featured,
       #s_ec_free_programs_inserts.priority,
       #s_ec_free_programs_inserts.frequency,
       #s_ec_free_programs_inserts.duration,
       #s_ec_free_programs_inserts.equipment,
       #s_ec_free_programs_inserts.exercise,
       #s_ec_free_programs_inserts.level,
       #s_ec_free_programs_inserts.goal,
       #s_ec_free_programs_inserts.is_active,
       #s_ec_free_programs_inserts.created_date,
       #s_ec_free_programs_inserts.updated_date,
       #s_ec_free_programs_inserts.free_program_id,
       #s_ec_free_programs_inserts.end_date,
       case when s_ec_free_programs.s_ec_free_programs_id is null then isnull(#s_ec_free_programs_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_free_programs_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_free_programs_inserts
  left join p_ec_free_programs
    on #s_ec_free_programs_inserts.bk_hash = p_ec_free_programs.bk_hash
   and p_ec_free_programs.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_free_programs
    on p_ec_free_programs.bk_hash = s_ec_free_programs.bk_hash
   and p_ec_free_programs.s_ec_free_programs_id = s_ec_free_programs.s_ec_free_programs_id
 where s_ec_free_programs.s_ec_free_programs_id is null
    or (s_ec_free_programs.s_ec_free_programs_id is not null
        and s_ec_free_programs.dv_hash <> #s_ec_free_programs_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_free_programs @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_free_programs @current_dv_batch_id

end
