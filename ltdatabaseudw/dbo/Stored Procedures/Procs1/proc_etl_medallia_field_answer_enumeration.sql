CREATE PROC [dbo].[proc_etl_medallia_field_answer_enumeration] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_medallia_field_answer_enumeration

set @insert_date_time = getdate()
insert into dbo.stage_hash_medallia_field_answer_enumeration (
       bk_hash,
       answer_enumeration_id ,
       answer_name,
       enumeration_value,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(answer_enumeration_id ,'z#@$k%&P'))),2) bk_hash,
       answer_enumeration_id ,
       answer_name,
       enumeration_value,
       dummy_modified_date_time,
       isnull(cast(stage_medallia_field_answer_enumeration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_medallia_field_answer_enumeration
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_medallia_field_answer_enumeration @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_medallia_field_answer_enumeration (
       bk_hash,
       answer_enumeration_id ,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_medallia_field_answer_enumeration.bk_hash,
       stage_hash_medallia_field_answer_enumeration.answer_enumeration_id  answer_enumeration_id ,
       isnull(cast(stage_hash_medallia_field_answer_enumeration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       44,
       @insert_date_time,
       @user
  from stage_hash_medallia_field_answer_enumeration
  left join h_medallia_field_answer_enumeration
    on stage_hash_medallia_field_answer_enumeration.bk_hash = h_medallia_field_answer_enumeration.bk_hash
 where h_medallia_field_answer_enumeration_id is null
   and stage_hash_medallia_field_answer_enumeration.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_medallia_field_answer_enumeration
if object_id('tempdb..#s_medallia_field_answer_enumeration_inserts') is not null drop table #s_medallia_field_answer_enumeration_inserts
create table #s_medallia_field_answer_enumeration_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_medallia_field_answer_enumeration.bk_hash,
       stage_hash_medallia_field_answer_enumeration.answer_enumeration_id  answer_enumeration_id ,
       stage_hash_medallia_field_answer_enumeration.answer_name answer_name,
       stage_hash_medallia_field_answer_enumeration.enumeration_value enumeration_value,
       stage_hash_medallia_field_answer_enumeration.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_medallia_field_answer_enumeration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_medallia_field_answer_enumeration.answer_enumeration_id ,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field_answer_enumeration.answer_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field_answer_enumeration.enumeration_value,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_medallia_field_answer_enumeration
 where stage_hash_medallia_field_answer_enumeration.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_medallia_field_answer_enumeration records
set @insert_date_time = getdate()
insert into s_medallia_field_answer_enumeration (
       bk_hash,
       answer_enumeration_id ,
       answer_name,
       enumeration_value,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_medallia_field_answer_enumeration_inserts.bk_hash,
       #s_medallia_field_answer_enumeration_inserts.answer_enumeration_id ,
       #s_medallia_field_answer_enumeration_inserts.answer_name,
       #s_medallia_field_answer_enumeration_inserts.enumeration_value,
       #s_medallia_field_answer_enumeration_inserts.dummy_modified_date_time,
       case when s_medallia_field_answer_enumeration.s_medallia_field_answer_enumeration_id is null then isnull(#s_medallia_field_answer_enumeration_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       44,
       #s_medallia_field_answer_enumeration_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_medallia_field_answer_enumeration_inserts
  left join p_medallia_field_answer_enumeration
    on #s_medallia_field_answer_enumeration_inserts.bk_hash = p_medallia_field_answer_enumeration.bk_hash
   and p_medallia_field_answer_enumeration.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_medallia_field_answer_enumeration
    on p_medallia_field_answer_enumeration.bk_hash = s_medallia_field_answer_enumeration.bk_hash
   and p_medallia_field_answer_enumeration.s_medallia_field_answer_enumeration_id = s_medallia_field_answer_enumeration.s_medallia_field_answer_enumeration_id
 where s_medallia_field_answer_enumeration.s_medallia_field_answer_enumeration_id is null
    or (s_medallia_field_answer_enumeration.s_medallia_field_answer_enumeration_id is not null
        and s_medallia_field_answer_enumeration.dv_hash <> #s_medallia_field_answer_enumeration_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_medallia_field_answer_enumeration @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_medallia_field_answer_enumeration @current_dv_batch_id

end
