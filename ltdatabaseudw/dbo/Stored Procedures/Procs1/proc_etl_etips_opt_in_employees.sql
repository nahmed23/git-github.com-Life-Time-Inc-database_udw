CREATE PROC [dbo].[proc_etl_etips_opt_in_employees] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_etips_opt_in_employees

set @insert_date_time = getdate()
insert into dbo.stage_hash_etips_opt_in_employees (
       bk_hash,
       employee_id,
       pay_card_start_date,
       pay_card_status,
       ltf_file_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(employee_id,'z#@$k%&P')+'P%#&z$@k'+isnull(pay_card_start_date,'z#@$k%&P')+'P%#&z$@k'+isnull(pay_card_status,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       employee_id,
       pay_card_start_date,
       pay_card_status,
       ltf_file_name,
       dummy_modified_date_time,
       isnull(cast(stage_etips_opt_in_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_etips_opt_in_employees
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_etips_opt_in_employees @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_etips_opt_in_employees (
       bk_hash,
       employee_id,
       pay_card_start_date,
       pay_card_status,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_etips_opt_in_employees.bk_hash,
       stage_hash_etips_opt_in_employees.employee_id employee_id,
       stage_hash_etips_opt_in_employees.pay_card_start_date pay_card_start_date,
       stage_hash_etips_opt_in_employees.pay_card_status pay_card_status,
       stage_hash_etips_opt_in_employees.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_etips_opt_in_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       49,
       @insert_date_time,
       @user
  from stage_hash_etips_opt_in_employees
  left join h_etips_opt_in_employees
    on stage_hash_etips_opt_in_employees.bk_hash = h_etips_opt_in_employees.bk_hash
 where h_etips_opt_in_employees_id is null
   and stage_hash_etips_opt_in_employees.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_etips_opt_in_employees
if object_id('tempdb..#l_etips_opt_in_employees_inserts') is not null drop table #l_etips_opt_in_employees_inserts
create table #l_etips_opt_in_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_etips_opt_in_employees.bk_hash,
       stage_hash_etips_opt_in_employees.employee_id employee_id,
       stage_hash_etips_opt_in_employees.pay_card_start_date pay_card_start_date,
       stage_hash_etips_opt_in_employees.pay_card_status pay_card_status,
       stage_hash_etips_opt_in_employees.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_etips_opt_in_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.pay_card_start_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.pay_card_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.ltf_file_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_etips_opt_in_employees
 where stage_hash_etips_opt_in_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_etips_opt_in_employees records
set @insert_date_time = getdate()
insert into l_etips_opt_in_employees (
       bk_hash,
       employee_id,
       pay_card_start_date,
       pay_card_status,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_etips_opt_in_employees_inserts.bk_hash,
       #l_etips_opt_in_employees_inserts.employee_id,
       #l_etips_opt_in_employees_inserts.pay_card_start_date,
       #l_etips_opt_in_employees_inserts.pay_card_status,
       #l_etips_opt_in_employees_inserts.ltf_file_name,
       case when l_etips_opt_in_employees.l_etips_opt_in_employees_id is null then isnull(#l_etips_opt_in_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       49,
       #l_etips_opt_in_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_etips_opt_in_employees_inserts
  left join p_etips_opt_in_employees
    on #l_etips_opt_in_employees_inserts.bk_hash = p_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_etips_opt_in_employees
    on p_etips_opt_in_employees.bk_hash = l_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.l_etips_opt_in_employees_id = l_etips_opt_in_employees.l_etips_opt_in_employees_id
 where l_etips_opt_in_employees.l_etips_opt_in_employees_id is null
    or (l_etips_opt_in_employees.l_etips_opt_in_employees_id is not null
        and l_etips_opt_in_employees.dv_hash <> #l_etips_opt_in_employees_inserts.source_hash)

--calculate hash and lookup to current s_etips_opt_in_employees
if object_id('tempdb..#s_etips_opt_in_employees_inserts') is not null drop table #s_etips_opt_in_employees_inserts
create table #s_etips_opt_in_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_etips_opt_in_employees.bk_hash,
       stage_hash_etips_opt_in_employees.employee_id employee_id,
       stage_hash_etips_opt_in_employees.pay_card_start_date pay_card_start_date,
       stage_hash_etips_opt_in_employees.pay_card_status pay_card_status,
       stage_hash_etips_opt_in_employees.ltf_file_name ltf_file_name,
       stage_hash_etips_opt_in_employees.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_etips_opt_in_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.pay_card_start_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.pay_card_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_etips_opt_in_employees.ltf_file_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_etips_opt_in_employees
 where stage_hash_etips_opt_in_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_etips_opt_in_employees records
set @insert_date_time = getdate()
insert into s_etips_opt_in_employees (
       bk_hash,
       employee_id,
       pay_card_start_date,
       pay_card_status,
       ltf_file_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_etips_opt_in_employees_inserts.bk_hash,
       #s_etips_opt_in_employees_inserts.employee_id,
       #s_etips_opt_in_employees_inserts.pay_card_start_date,
       #s_etips_opt_in_employees_inserts.pay_card_status,
       #s_etips_opt_in_employees_inserts.ltf_file_name,
       #s_etips_opt_in_employees_inserts.dummy_modified_date_time,
       case when s_etips_opt_in_employees.s_etips_opt_in_employees_id is null then isnull(#s_etips_opt_in_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       49,
       #s_etips_opt_in_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_etips_opt_in_employees_inserts
  left join p_etips_opt_in_employees
    on #s_etips_opt_in_employees_inserts.bk_hash = p_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_etips_opt_in_employees
    on p_etips_opt_in_employees.bk_hash = s_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.s_etips_opt_in_employees_id = s_etips_opt_in_employees.s_etips_opt_in_employees_id
 where s_etips_opt_in_employees.s_etips_opt_in_employees_id is null
    or (s_etips_opt_in_employees.s_etips_opt_in_employees_id is not null
        and s_etips_opt_in_employees.dv_hash <> #s_etips_opt_in_employees_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_etips_opt_in_employees @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_etips_opt_in_employees @current_dv_batch_id

end
