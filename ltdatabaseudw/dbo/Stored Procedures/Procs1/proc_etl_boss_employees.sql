CREATE PROC [dbo].[proc_etl_boss_employees] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_employees

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_employees (
       bk_hash,
       last,
       first,
       MI,
       interestID,
       home_club,
       badge,
       roleID,
       status,
       email,
       user_profile,
       nickname,
       cost,
       employee_url,
       employee_id,
       id,
       member_ID,
       phone,
       res_color,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(employee_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       last,
       first,
       MI,
       interestID,
       home_club,
       badge,
       roleID,
       status,
       email,
       user_profile,
       nickname,
       cost,
       employee_url,
       employee_id,
       id,
       member_ID,
       phone,
       res_color,
       jan_one,
       isnull(cast(stage_boss_employees.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_boss_employees
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_employees @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_employees (
       bk_hash,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_employees.bk_hash,
       stage_hash_boss_employees.employee_id employee_id,
       isnull(cast(stage_hash_boss_employees.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_employees
  left join h_boss_employees
    on stage_hash_boss_employees.bk_hash = h_boss_employees.bk_hash
 where h_boss_employees_id is null
   and stage_hash_boss_employees.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_employees
if object_id('tempdb..#l_boss_employees_inserts') is not null drop table #l_boss_employees_inserts
create table #l_boss_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_employees.bk_hash,
       stage_hash_boss_employees.interestID interestID,
       stage_hash_boss_employees.home_club home_club,
       stage_hash_boss_employees.roleID roleID,
       stage_hash_boss_employees.employee_id employee_id,
       stage_hash_boss_employees.id id,
       stage_hash_boss_employees.member_ID member_ID,
       isnull(cast(stage_hash_boss_employees.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_employees.interestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.home_club as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.roleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.member_ID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_employees
 where stage_hash_boss_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_employees records
set @insert_date_time = getdate()
insert into l_boss_employees (
       bk_hash,
       interestID,
       home_club,
       roleID,
       employee_id,
       id,
       member_ID,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_employees_inserts.bk_hash,
       #l_boss_employees_inserts.interestID,
       #l_boss_employees_inserts.home_club,
       #l_boss_employees_inserts.roleID,
       #l_boss_employees_inserts.employee_id,
       #l_boss_employees_inserts.id,
       #l_boss_employees_inserts.member_ID,
       case when l_boss_employees.l_boss_employees_id is null then isnull(#l_boss_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_employees_inserts
  left join p_boss_employees
    on #l_boss_employees_inserts.bk_hash = p_boss_employees.bk_hash
   and p_boss_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_employees
    on p_boss_employees.bk_hash = l_boss_employees.bk_hash
   and p_boss_employees.l_boss_employees_id = l_boss_employees.l_boss_employees_id
 where l_boss_employees.l_boss_employees_id is null
    or (l_boss_employees.l_boss_employees_id is not null
        and l_boss_employees.dv_hash <> #l_boss_employees_inserts.source_hash)

--calculate hash and lookup to current s_boss_employees
if object_id('tempdb..#s_boss_employees_inserts') is not null drop table #s_boss_employees_inserts
create table #s_boss_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_employees.bk_hash,
       stage_hash_boss_employees.last last,
       stage_hash_boss_employees.first first,
       stage_hash_boss_employees.MI MI,
       stage_hash_boss_employees.badge badge,
       stage_hash_boss_employees.status status,
       stage_hash_boss_employees.email email,
       stage_hash_boss_employees.user_profile user_profile,
       stage_hash_boss_employees.nickname nickname,
       stage_hash_boss_employees.cost cost,
       stage_hash_boss_employees.employee_url employee_url,
       stage_hash_boss_employees.employee_id employee_id,
       stage_hash_boss_employees.phone phone,
       stage_hash_boss_employees.res_color res_color,
       isnull(cast(stage_hash_boss_employees.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_employees.last,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.first,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.MI,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.badge,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.user_profile,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.nickname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.employee_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_employees.phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_employees.res_color as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_employees
 where stage_hash_boss_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_employees records
set @insert_date_time = getdate()
insert into s_boss_employees (
       bk_hash,
       last,
       first,
       MI,
       badge,
       status,
       email,
       user_profile,
       nickname,
       cost,
       employee_url,
       employee_id,
       phone,
       res_color,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_employees_inserts.bk_hash,
       #s_boss_employees_inserts.last,
       #s_boss_employees_inserts.first,
       #s_boss_employees_inserts.MI,
       #s_boss_employees_inserts.badge,
       #s_boss_employees_inserts.status,
       #s_boss_employees_inserts.email,
       #s_boss_employees_inserts.user_profile,
       #s_boss_employees_inserts.nickname,
       #s_boss_employees_inserts.cost,
       #s_boss_employees_inserts.employee_url,
       #s_boss_employees_inserts.employee_id,
       #s_boss_employees_inserts.phone,
       #s_boss_employees_inserts.res_color,
       case when s_boss_employees.s_boss_employees_id is null then isnull(#s_boss_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_employees_inserts
  left join p_boss_employees
    on #s_boss_employees_inserts.bk_hash = p_boss_employees.bk_hash
   and p_boss_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_employees
    on p_boss_employees.bk_hash = s_boss_employees.bk_hash
   and p_boss_employees.s_boss_employees_id = s_boss_employees.s_boss_employees_id
 where s_boss_employees.s_boss_employees_id is null
    or (s_boss_employees.s_boss_employees_id is not null
        and s_boss_employees.dv_hash <> #s_boss_employees_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_employees @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_employees @current_dv_batch_id

end
