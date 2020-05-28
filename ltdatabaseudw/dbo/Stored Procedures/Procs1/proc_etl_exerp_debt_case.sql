CREATE PROC [dbo].[proc_etl_exerp_debt_case] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_debt_case

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_debt_case (
       bk_hash,
       id,
       center_id,
       person_id,
       company_id,
       start_datetime,
       amount,
       closed,
       closed_datetime,
       current_step,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       center_id,
       person_id,
       company_id,
       start_datetime,
       amount,
       closed,
       closed_datetime,
       current_step,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_debt_case.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_debt_case
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_debt_case @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_debt_case (
       bk_hash,
       debt_case_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_debt_case.bk_hash,
       stage_hash_exerp_debt_case.id debt_case_id,
       isnull(cast(stage_hash_exerp_debt_case.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_debt_case
  left join h_exerp_debt_case
    on stage_hash_exerp_debt_case.bk_hash = h_exerp_debt_case.bk_hash
 where h_exerp_debt_case_id is null
   and stage_hash_exerp_debt_case.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_debt_case
if object_id('tempdb..#l_exerp_debt_case_inserts') is not null drop table #l_exerp_debt_case_inserts
create table #l_exerp_debt_case_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_debt_case.bk_hash,
       stage_hash_exerp_debt_case.id debt_case_id,
       stage_hash_exerp_debt_case.center_id center_id,
       stage_hash_exerp_debt_case.person_id person_id,
       stage_hash_exerp_debt_case.company_id company_id,
       isnull(cast(stage_hash_exerp_debt_case.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_debt_case.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_debt_case.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_debt_case.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_debt_case.company_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_debt_case
 where stage_hash_exerp_debt_case.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_debt_case records
set @insert_date_time = getdate()
insert into l_exerp_debt_case (
       bk_hash,
       debt_case_id,
       center_id,
       person_id,
       company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_debt_case_inserts.bk_hash,
       #l_exerp_debt_case_inserts.debt_case_id,
       #l_exerp_debt_case_inserts.center_id,
       #l_exerp_debt_case_inserts.person_id,
       #l_exerp_debt_case_inserts.company_id,
       case when l_exerp_debt_case.l_exerp_debt_case_id is null then isnull(#l_exerp_debt_case_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_debt_case_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_debt_case_inserts
  left join p_exerp_debt_case
    on #l_exerp_debt_case_inserts.bk_hash = p_exerp_debt_case.bk_hash
   and p_exerp_debt_case.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_debt_case
    on p_exerp_debt_case.bk_hash = l_exerp_debt_case.bk_hash
   and p_exerp_debt_case.l_exerp_debt_case_id = l_exerp_debt_case.l_exerp_debt_case_id
 where l_exerp_debt_case.l_exerp_debt_case_id is null
    or (l_exerp_debt_case.l_exerp_debt_case_id is not null
        and l_exerp_debt_case.dv_hash <> #l_exerp_debt_case_inserts.source_hash)

--calculate hash and lookup to current s_exerp_debt_case
if object_id('tempdb..#s_exerp_debt_case_inserts') is not null drop table #s_exerp_debt_case_inserts
create table #s_exerp_debt_case_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_debt_case.bk_hash,
       stage_hash_exerp_debt_case.id debt_case_id,
       stage_hash_exerp_debt_case.start_datetime start_datetime,
       stage_hash_exerp_debt_case.amount amount,
       stage_hash_exerp_debt_case.closed closed,
       stage_hash_exerp_debt_case.closed_datetime closed_datetime,
       stage_hash_exerp_debt_case.current_step current_step,
       stage_hash_exerp_debt_case.ets ets,
       stage_hash_exerp_debt_case.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_debt_case.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_debt_case.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_debt_case.start_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_debt_case.amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_debt_case.closed as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_debt_case.closed_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_debt_case.current_step as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_debt_case.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_debt_case
 where stage_hash_exerp_debt_case.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_debt_case records
set @insert_date_time = getdate()
insert into s_exerp_debt_case (
       bk_hash,
       debt_case_id,
       start_datetime,
       amount,
       closed,
       closed_datetime,
       current_step,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_debt_case_inserts.bk_hash,
       #s_exerp_debt_case_inserts.debt_case_id,
       #s_exerp_debt_case_inserts.start_datetime,
       #s_exerp_debt_case_inserts.amount,
       #s_exerp_debt_case_inserts.closed,
       #s_exerp_debt_case_inserts.closed_datetime,
       #s_exerp_debt_case_inserts.current_step,
       #s_exerp_debt_case_inserts.ets,
       #s_exerp_debt_case_inserts.dummy_modified_date_time,
       case when s_exerp_debt_case.s_exerp_debt_case_id is null then isnull(#s_exerp_debt_case_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_debt_case_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_debt_case_inserts
  left join p_exerp_debt_case
    on #s_exerp_debt_case_inserts.bk_hash = p_exerp_debt_case.bk_hash
   and p_exerp_debt_case.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_debt_case
    on p_exerp_debt_case.bk_hash = s_exerp_debt_case.bk_hash
   and p_exerp_debt_case.s_exerp_debt_case_id = s_exerp_debt_case.s_exerp_debt_case_id
 where s_exerp_debt_case.s_exerp_debt_case_id is null
    or (s_exerp_debt_case.s_exerp_debt_case_id is not null
        and s_exerp_debt_case.dv_hash <> #s_exerp_debt_case_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_debt_case @current_dv_batch_id

end
