CREATE PROC [dbo].[proc_etl_exerp_person] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_person

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_person (
       bk_hash,
       id,
       center_id,
       home_center_person_id,
       home_center_id,
       ets,
       can_sms,
       can_email,
       employee_title,
       staff_external_id,
       state,
       county,
       company_id,
       payer_person_id,
       person_status,
       person_type,
       gender,
       city,
       postal_code,
       country_id,
       title,
       duplicate_of_person_id,
       creation_date,
       date_of_birth,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       center_id,
       home_center_person_id,
       home_center_id,
       ets,
       can_sms,
       can_email,
       employee_title,
       staff_external_id,
       state,
       county,
       company_id,
       payer_person_id,
       person_status,
       person_type,
       gender,
       city,
       postal_code,
       country_id,
       title,
       duplicate_of_person_id,
       creation_date,
       date_of_birth,
       isnull(cast(stage_exerp_person.creation_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_person
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_person @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_person (
       bk_hash,
       person_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_person.bk_hash,
       stage_hash_exerp_person.id person_id,
       isnull(cast(stage_hash_exerp_person.creation_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_person
  left join h_exerp_person
    on stage_hash_exerp_person.bk_hash = h_exerp_person.bk_hash
 where h_exerp_person_id is null
   and stage_hash_exerp_person.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_person
if object_id('tempdb..#l_exerp_person_inserts') is not null drop table #l_exerp_person_inserts
create table #l_exerp_person_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_person.bk_hash,
       stage_hash_exerp_person.id person_id,
       stage_hash_exerp_person.center_id center_id,
       stage_hash_exerp_person.home_center_person_id home_center_person_id,
       stage_hash_exerp_person.home_center_id home_center_id,
       isnull(cast(stage_hash_exerp_person.creation_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_person.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.home_center_person_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.home_center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_person
 where stage_hash_exerp_person.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_person records
set @insert_date_time = getdate()
insert into l_exerp_person (
       bk_hash,
       person_id,
       center_id,
       home_center_person_id,
       home_center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_person_inserts.bk_hash,
       #l_exerp_person_inserts.person_id,
       #l_exerp_person_inserts.center_id,
       #l_exerp_person_inserts.home_center_person_id,
       #l_exerp_person_inserts.home_center_id,
       case when l_exerp_person.l_exerp_person_id is null then isnull(#l_exerp_person_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_person_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_person_inserts
  left join p_exerp_person
    on #l_exerp_person_inserts.bk_hash = p_exerp_person.bk_hash
   and p_exerp_person.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_person
    on p_exerp_person.bk_hash = l_exerp_person.bk_hash
   and p_exerp_person.l_exerp_person_id = l_exerp_person.l_exerp_person_id
 where l_exerp_person.l_exerp_person_id is null
    or (l_exerp_person.l_exerp_person_id is not null
        and l_exerp_person.dv_hash <> #l_exerp_person_inserts.source_hash)

--calculate hash and lookup to current s_exerp_person
if object_id('tempdb..#s_exerp_person_inserts') is not null drop table #s_exerp_person_inserts
create table #s_exerp_person_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_person.bk_hash,
       stage_hash_exerp_person.id person_id,
       stage_hash_exerp_person.ets ets,
       stage_hash_exerp_person.can_sms can_sms,
       stage_hash_exerp_person.can_email can_email,
       stage_hash_exerp_person.employee_title employee_title,
       stage_hash_exerp_person.staff_external_id staff_external_id,
       stage_hash_exerp_person.state state,
       stage_hash_exerp_person.county county,
       stage_hash_exerp_person.company_id company_id,
       stage_hash_exerp_person.payer_person_id payer_person_id,
       stage_hash_exerp_person.person_status person_status,
       stage_hash_exerp_person.person_type person_type,
       stage_hash_exerp_person.gender gender,
       stage_hash_exerp_person.city city,
       stage_hash_exerp_person.postal_code postal_code,
       stage_hash_exerp_person.country_id country_id,
       stage_hash_exerp_person.title title,
       stage_hash_exerp_person.duplicate_of_person_id duplicate_of_person_id,
       stage_hash_exerp_person.creation_date creation_date,
       stage_hash_exerp_person.date_of_birth date_of_birth,
       isnull(cast(stage_hash_exerp_person.creation_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_person.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.ets as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.can_sms as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person.can_email as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.employee_title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.staff_external_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.payer_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.person_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.person_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.postal_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.country_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person.duplicate_of_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_person.creation_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_person.date_of_birth,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_person
 where stage_hash_exerp_person.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_person records
set @insert_date_time = getdate()
insert into s_exerp_person (
       bk_hash,
       person_id,
       ets,
       can_sms,
       can_email,
       employee_title,
       staff_external_id,
       state,
       county,
       company_id,
       payer_person_id,
       person_status,
       person_type,
       gender,
       city,
       postal_code,
       country_id,
       title,
       duplicate_of_person_id,
       creation_date,
       date_of_birth,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_person_inserts.bk_hash,
       #s_exerp_person_inserts.person_id,
       #s_exerp_person_inserts.ets,
       #s_exerp_person_inserts.can_sms,
       #s_exerp_person_inserts.can_email,
       #s_exerp_person_inserts.employee_title,
       #s_exerp_person_inserts.staff_external_id,
       #s_exerp_person_inserts.state,
       #s_exerp_person_inserts.county,
       #s_exerp_person_inserts.company_id,
       #s_exerp_person_inserts.payer_person_id,
       #s_exerp_person_inserts.person_status,
       #s_exerp_person_inserts.person_type,
       #s_exerp_person_inserts.gender,
       #s_exerp_person_inserts.city,
       #s_exerp_person_inserts.postal_code,
       #s_exerp_person_inserts.country_id,
       #s_exerp_person_inserts.title,
       #s_exerp_person_inserts.duplicate_of_person_id,
       #s_exerp_person_inserts.creation_date,
       #s_exerp_person_inserts.date_of_birth,
       case when s_exerp_person.s_exerp_person_id is null then isnull(#s_exerp_person_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_person_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_person_inserts
  left join p_exerp_person
    on #s_exerp_person_inserts.bk_hash = p_exerp_person.bk_hash
   and p_exerp_person.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_person
    on p_exerp_person.bk_hash = s_exerp_person.bk_hash
   and p_exerp_person.s_exerp_person_id = s_exerp_person.s_exerp_person_id
 where s_exerp_person.s_exerp_person_id is null
    or (s_exerp_person.s_exerp_person_id is not null
        and s_exerp_person.dv_hash <> #s_exerp_person_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_person @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_person @current_dv_batch_id

end
