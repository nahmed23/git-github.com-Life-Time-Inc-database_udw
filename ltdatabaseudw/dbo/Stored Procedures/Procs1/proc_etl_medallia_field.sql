CREATE PROC [dbo].[proc_etl_medallia_field] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_medallia_field

set @insert_date_time = getdate()
insert into dbo.stage_hash_medallia_field (
       bk_hash,
       name_in_medallia,
       sr_no,
       name_in_api,
       variable_name,
       answer_id,
       description_question,
       data_type,
       single_select,
       examples,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(name_in_medallia,'z#@$k%&P'))),2) bk_hash,
       name_in_medallia,
       sr_no,
       name_in_api,
       variable_name,
       answer_id,
       description_question,
       data_type,
       single_select,
       examples,
       dummy_modified_date_time,
       isnull(cast(stage_medallia_field.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_medallia_field
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_medallia_field @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_medallia_field (
       bk_hash,
       name_in_medallia,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_medallia_field.bk_hash,
       stage_hash_medallia_field.name_in_medallia name_in_medallia,
       isnull(cast(stage_hash_medallia_field.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       44,
       @insert_date_time,
       @user
  from stage_hash_medallia_field
  left join h_medallia_field
    on stage_hash_medallia_field.bk_hash = h_medallia_field.bk_hash
 where h_medallia_field_id is null
   and stage_hash_medallia_field.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_medallia_field
if object_id('tempdb..#l_medallia_field_inserts') is not null drop table #l_medallia_field_inserts
create table #l_medallia_field_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_medallia_field.bk_hash,
       stage_hash_medallia_field.name_in_medallia name_in_medallia,
       stage_hash_medallia_field.answer_id answer_id,
       isnull(cast(stage_hash_medallia_field.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_medallia_field.name_in_medallia,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.answer_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_medallia_field
 where stage_hash_medallia_field.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_medallia_field records
set @insert_date_time = getdate()
insert into l_medallia_field (
       bk_hash,
       name_in_medallia,
       answer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_medallia_field_inserts.bk_hash,
       #l_medallia_field_inserts.name_in_medallia,
       #l_medallia_field_inserts.answer_id,
       case when l_medallia_field.l_medallia_field_id is null then isnull(#l_medallia_field_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       44,
       #l_medallia_field_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_medallia_field_inserts
  left join p_medallia_field
    on #l_medallia_field_inserts.bk_hash = p_medallia_field.bk_hash
   and p_medallia_field.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_medallia_field
    on p_medallia_field.bk_hash = l_medallia_field.bk_hash
   and p_medallia_field.l_medallia_field_id = l_medallia_field.l_medallia_field_id
 where l_medallia_field.l_medallia_field_id is null
    or (l_medallia_field.l_medallia_field_id is not null
        and l_medallia_field.dv_hash <> #l_medallia_field_inserts.source_hash)

--calculate hash and lookup to current s_medallia_field
if object_id('tempdb..#s_medallia_field_inserts') is not null drop table #s_medallia_field_inserts
create table #s_medallia_field_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_medallia_field.bk_hash,
       stage_hash_medallia_field.name_in_medallia name_in_medallia,
       stage_hash_medallia_field.sr_no sr_no,
       stage_hash_medallia_field.name_in_api name_in_api,
       stage_hash_medallia_field.variable_name variable_name,
       stage_hash_medallia_field.description_question description_question,
       stage_hash_medallia_field.data_type data_type,
       stage_hash_medallia_field.single_select single_select,
       stage_hash_medallia_field.examples examples,
       stage_hash_medallia_field.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_medallia_field.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_medallia_field.name_in_medallia,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.sr_no,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.name_in_api,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.variable_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.description_question,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.data_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.single_select,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_field.examples,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_medallia_field
 where stage_hash_medallia_field.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_medallia_field records
set @insert_date_time = getdate()
insert into s_medallia_field (
       bk_hash,
       name_in_medallia,
       sr_no,
       name_in_api,
       variable_name,
       description_question,
       data_type,
       single_select,
       examples,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_medallia_field_inserts.bk_hash,
       #s_medallia_field_inserts.name_in_medallia,
       #s_medallia_field_inserts.sr_no,
       #s_medallia_field_inserts.name_in_api,
       #s_medallia_field_inserts.variable_name,
       #s_medallia_field_inserts.description_question,
       #s_medallia_field_inserts.data_type,
       #s_medallia_field_inserts.single_select,
       #s_medallia_field_inserts.examples,
       #s_medallia_field_inserts.dummy_modified_date_time,
       case when s_medallia_field.s_medallia_field_id is null then isnull(#s_medallia_field_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       44,
       #s_medallia_field_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_medallia_field_inserts
  left join p_medallia_field
    on #s_medallia_field_inserts.bk_hash = p_medallia_field.bk_hash
   and p_medallia_field.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_medallia_field
    on p_medallia_field.bk_hash = s_medallia_field.bk_hash
   and p_medallia_field.s_medallia_field_id = s_medallia_field.s_medallia_field_id
 where s_medallia_field.s_medallia_field_id is null
    or (s_medallia_field.s_medallia_field_id is not null
        and s_medallia_field.dv_hash <> #s_medallia_field_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_medallia_field @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_medallia_field @current_dv_batch_id

end
