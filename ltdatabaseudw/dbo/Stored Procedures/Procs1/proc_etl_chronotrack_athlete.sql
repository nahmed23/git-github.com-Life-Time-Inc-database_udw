CREATE PROC [dbo].[proc_etl_chronotrack_athlete] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_chronotrack_athlete

set @insert_date_time = getdate()
insert into dbo.stage_hash_chronotrack_athlete (
       bk_hash,
       id,
       account_id,
       first_name,
       middle_name,
       last_name,
       name_pronunciation,
       sex,
       birthdate,
       age,
       tshirt_size,
       usat_num,
       location_id,
       home_phone,
       mobile_phone,
       email,
       emerg_name,
       emerg_phone,
       emerg_relationship,
       medical_notes,
       ctime,
       mtime,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       account_id,
       first_name,
       middle_name,
       last_name,
       name_pronunciation,
       sex,
       birthdate,
       age,
       tshirt_size,
       usat_num,
       location_id,
       home_phone,
       mobile_phone,
       email,
       emerg_name,
       emerg_phone,
       emerg_relationship,
       medical_notes,
       ctime,
       mtime,
       dummy_modified_date_time,
       isnull(cast(stage_chronotrack_athlete.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_chronotrack_athlete
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_chronotrack_athlete @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_chronotrack_athlete (
       bk_hash,
       athlete_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_chronotrack_athlete.bk_hash,
       stage_hash_chronotrack_athlete.id athlete_id,
       isnull(cast(stage_hash_chronotrack_athlete.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       46,
       @insert_date_time,
       @user
  from stage_hash_chronotrack_athlete
  left join h_chronotrack_athlete
    on stage_hash_chronotrack_athlete.bk_hash = h_chronotrack_athlete.bk_hash
 where h_chronotrack_athlete_id is null
   and stage_hash_chronotrack_athlete.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_chronotrack_athlete
if object_id('tempdb..#l_chronotrack_athlete_inserts') is not null drop table #l_chronotrack_athlete_inserts
create table #l_chronotrack_athlete_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_athlete.bk_hash,
       stage_hash_chronotrack_athlete.id athlete_id,
       stage_hash_chronotrack_athlete.account_id account_id,
       stage_hash_chronotrack_athlete.location_id location_id,
       isnull(cast(stage_hash_chronotrack_athlete.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.account_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.location_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_athlete
 where stage_hash_chronotrack_athlete.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_chronotrack_athlete records
set @insert_date_time = getdate()
insert into l_chronotrack_athlete (
       bk_hash,
       athlete_id,
       account_id,
       location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_chronotrack_athlete_inserts.bk_hash,
       #l_chronotrack_athlete_inserts.athlete_id,
       #l_chronotrack_athlete_inserts.account_id,
       #l_chronotrack_athlete_inserts.location_id,
       case when l_chronotrack_athlete.l_chronotrack_athlete_id is null then isnull(#l_chronotrack_athlete_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #l_chronotrack_athlete_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_chronotrack_athlete_inserts
  left join p_chronotrack_athlete
    on #l_chronotrack_athlete_inserts.bk_hash = p_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_chronotrack_athlete
    on p_chronotrack_athlete.bk_hash = l_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.l_chronotrack_athlete_id = l_chronotrack_athlete.l_chronotrack_athlete_id
 where l_chronotrack_athlete.l_chronotrack_athlete_id is null
    or (l_chronotrack_athlete.l_chronotrack_athlete_id is not null
        and l_chronotrack_athlete.dv_hash <> #l_chronotrack_athlete_inserts.source_hash)

--calculate hash and lookup to current s_chronotrack_athlete
if object_id('tempdb..#s_chronotrack_athlete_inserts') is not null drop table #s_chronotrack_athlete_inserts
create table #s_chronotrack_athlete_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_athlete.bk_hash,
       stage_hash_chronotrack_athlete.id athlete_id,
       stage_hash_chronotrack_athlete.first_name first_name,
       stage_hash_chronotrack_athlete.middle_name middle_name,
       stage_hash_chronotrack_athlete.last_name last_name,
       stage_hash_chronotrack_athlete.name_pronunciation name_pronunciation,
       stage_hash_chronotrack_athlete.sex sex,
       stage_hash_chronotrack_athlete.birthdate birth_date,
       stage_hash_chronotrack_athlete.age age,
       stage_hash_chronotrack_athlete.tshirt_size tshirt_size,
       stage_hash_chronotrack_athlete.usat_num usat_num,
       stage_hash_chronotrack_athlete.home_phone home_phone,
       stage_hash_chronotrack_athlete.mobile_phone mobile_phone,
       stage_hash_chronotrack_athlete.email email,
       stage_hash_chronotrack_athlete.emerg_name emerg_name,
       stage_hash_chronotrack_athlete.emerg_phone emerg_phone,
       stage_hash_chronotrack_athlete.emerg_relationship emerg_relationship,
       stage_hash_chronotrack_athlete.medical_notes medical_notes,
       stage_hash_chronotrack_athlete.ctime create_time,
       stage_hash_chronotrack_athlete.mtime modified_time,
       stage_hash_chronotrack_athlete.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_chronotrack_athlete.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.first_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.middle_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.last_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.name_pronunciation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.sex,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.birthdate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.tshirt_size,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.usat_num,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.home_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.mobile_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.emerg_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.emerg_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.emerg_relationship,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_athlete.medical_notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.ctime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_athlete.mtime as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_athlete
 where stage_hash_chronotrack_athlete.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_chronotrack_athlete records
set @insert_date_time = getdate()
insert into s_chronotrack_athlete (
       bk_hash,
       athlete_id,
       first_name,
       middle_name,
       last_name,
       name_pronunciation,
       sex,
       birth_date,
       age,
       tshirt_size,
       usat_num,
       home_phone,
       mobile_phone,
       email,
       emerg_name,
       emerg_phone,
       emerg_relationship,
       medical_notes,
       create_time,
       modified_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_chronotrack_athlete_inserts.bk_hash,
       #s_chronotrack_athlete_inserts.athlete_id,
       #s_chronotrack_athlete_inserts.first_name,
       #s_chronotrack_athlete_inserts.middle_name,
       #s_chronotrack_athlete_inserts.last_name,
       #s_chronotrack_athlete_inserts.name_pronunciation,
       #s_chronotrack_athlete_inserts.sex,
       #s_chronotrack_athlete_inserts.birth_date,
       #s_chronotrack_athlete_inserts.age,
       #s_chronotrack_athlete_inserts.tshirt_size,
       #s_chronotrack_athlete_inserts.usat_num,
       #s_chronotrack_athlete_inserts.home_phone,
       #s_chronotrack_athlete_inserts.mobile_phone,
       #s_chronotrack_athlete_inserts.email,
       #s_chronotrack_athlete_inserts.emerg_name,
       #s_chronotrack_athlete_inserts.emerg_phone,
       #s_chronotrack_athlete_inserts.emerg_relationship,
       #s_chronotrack_athlete_inserts.medical_notes,
       #s_chronotrack_athlete_inserts.create_time,
       #s_chronotrack_athlete_inserts.modified_time,
       #s_chronotrack_athlete_inserts.dummy_modified_date_time,
       case when s_chronotrack_athlete.s_chronotrack_athlete_id is null then isnull(#s_chronotrack_athlete_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #s_chronotrack_athlete_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_chronotrack_athlete_inserts
  left join p_chronotrack_athlete
    on #s_chronotrack_athlete_inserts.bk_hash = p_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_chronotrack_athlete
    on p_chronotrack_athlete.bk_hash = s_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.s_chronotrack_athlete_id = s_chronotrack_athlete.s_chronotrack_athlete_id
 where s_chronotrack_athlete.s_chronotrack_athlete_id is null
    or (s_chronotrack_athlete.s_chronotrack_athlete_id is not null
        and s_chronotrack_athlete.dv_hash <> #s_chronotrack_athlete_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_chronotrack_athlete @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_chronotrack_athlete @current_dv_batch_id

end
