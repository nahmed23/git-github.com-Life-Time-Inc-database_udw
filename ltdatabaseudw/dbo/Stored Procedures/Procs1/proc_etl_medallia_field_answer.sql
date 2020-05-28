CREATE PROC [dbo].[proc_etl_medallia_field_answer] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_medallia_field_answer

set @insert_date_time = getdate()
insert into dbo.stage_hash_medallia_field_answer (
       bk_hash,
       answer_id,
       answer_name,
       answer_type,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(answer_id,'z#@$k%&P'))),2) bk_hash,
       answer_id,
       answer_name,
       answer_type,
       dummy_modified_date_time,
       isnull(cast(stage_medallia_field_answer.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_medallia_field_answer
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_medallia_field_answer @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_medallia_field_answer (
       bk_hash,
       answer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_medallia_field_answer.bk_hash,
       stage_hash_medallia_field_answer.answer_id answer_id,
       isnull(cast(stage_hash_medallia_field_answer.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       44,
       @insert_date_time,
       @user
  from stage_hash_medallia_field_answer
  left join h_medallia_field_answer
    on stage_hash_medallia_field_answer.bk_hash = h_medallia_field_answer.bk_hash
 where h_medallia_field_answer_id is null
   and stage_hash_medallia_field_answer.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_medallia_field_answer
if object_id('tempdb..#s_medallia_field_answer_inserts') is not null drop table #s_medallia_field_answer_inserts
create table #s_medallia_field_answer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_medallia_field_answer.bk_hash,
       stage_hash_medallia_field_answer.answer_id answer_id,
       stage_hash_medallia_field_answer.answer_name answer_name,
       stage_hash_medallia_field_answer.answer_type answer_type,
       stage_hash_medallia_field_answer.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_medallia_field_answer.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_medallia_field_answer.answer_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field_answer.answer_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field_answer.answer_type,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_medallia_field_answer
 where stage_hash_medallia_field_answer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_medallia_field_answer records
set @insert_date_time = getdate()
insert into s_medallia_field_answer (
       bk_hash,
       answer_id,
       answer_name,
       answer_type,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_medallia_field_answer_inserts.bk_hash,
       #s_medallia_field_answer_inserts.answer_id,
       #s_medallia_field_answer_inserts.answer_name,
       #s_medallia_field_answer_inserts.answer_type,
       #s_medallia_field_answer_inserts.dummy_modified_date_time,
       case when s_medallia_field_answer.s_medallia_field_answer_id is null then isnull(#s_medallia_field_answer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       44,
       #s_medallia_field_answer_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_medallia_field_answer_inserts
  left join p_medallia_field_answer
    on #s_medallia_field_answer_inserts.bk_hash = p_medallia_field_answer.bk_hash
   and p_medallia_field_answer.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_medallia_field_answer
    on p_medallia_field_answer.bk_hash = s_medallia_field_answer.bk_hash
   and p_medallia_field_answer.s_medallia_field_answer_id = s_medallia_field_answer.s_medallia_field_answer_id
 where s_medallia_field_answer.s_medallia_field_answer_id is null
    or (s_medallia_field_answer.s_medallia_field_answer_id is not null
        and s_medallia_field_answer.dv_hash <> #s_medallia_field_answer_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_medallia_field_answer @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_medallia_field_answer @current_dv_batch_id

end
