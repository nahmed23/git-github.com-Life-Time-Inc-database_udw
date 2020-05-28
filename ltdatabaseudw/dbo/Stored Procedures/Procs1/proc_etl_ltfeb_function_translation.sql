CREATE PROC [dbo].[proc_etl_ltfeb_function_translation] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ltfeb_FunctionTranslation

set @insert_date_time = getdate()
insert into dbo.stage_hash_ltfeb_FunctionTranslation (
       bk_hash,
       function_name,
       function_value,
       function_value_table_name,
       update_datetime,
       update_userid,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(function_name,'z#@$k%&P'))),2) bk_hash,
       function_name,
       function_value,
       function_value_table_name,
       update_datetime,
       update_userid,
       isnull(cast(stage_ltfeb_FunctionTranslation.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ltfeb_FunctionTranslation
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ltfeb_function_translation @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ltfeb_function_translation (
       bk_hash,
       function_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ltfeb_FunctionTranslation.bk_hash,
       stage_hash_ltfeb_FunctionTranslation.function_name function_name,
       isnull(cast(stage_hash_ltfeb_FunctionTranslation.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       18,
       @insert_date_time,
       @user
  from stage_hash_ltfeb_FunctionTranslation
  left join h_ltfeb_function_translation
    on stage_hash_ltfeb_FunctionTranslation.bk_hash = h_ltfeb_function_translation.bk_hash
 where h_ltfeb_function_translation_id is null
   and stage_hash_ltfeb_FunctionTranslation.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ltfeb_function_translation
if object_id('tempdb..#s_ltfeb_function_translation_inserts') is not null drop table #s_ltfeb_function_translation_inserts
create table #s_ltfeb_function_translation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_FunctionTranslation.bk_hash,
       stage_hash_ltfeb_FunctionTranslation.function_name function_name,
       stage_hash_ltfeb_FunctionTranslation.function_value function_value,
       stage_hash_ltfeb_FunctionTranslation.function_value_table_name function_value_table_name,
       stage_hash_ltfeb_FunctionTranslation.update_datetime update_date_time,
       stage_hash_ltfeb_FunctionTranslation.update_userid update_user_id,
       stage_hash_ltfeb_FunctionTranslation.update_datetime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ltfeb_FunctionTranslation.function_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_FunctionTranslation.function_value,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_FunctionTranslation.function_value_table_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_FunctionTranslation.update_datetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_FunctionTranslation.update_userid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_FunctionTranslation
 where stage_hash_ltfeb_FunctionTranslation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ltfeb_function_translation records
set @insert_date_time = getdate()
insert into s_ltfeb_function_translation (
       bk_hash,
       function_name,
       function_value,
       function_value_table_name,
       update_date_time,
       update_user_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ltfeb_function_translation_inserts.bk_hash,
       #s_ltfeb_function_translation_inserts.function_name,
       #s_ltfeb_function_translation_inserts.function_value,
       #s_ltfeb_function_translation_inserts.function_value_table_name,
       #s_ltfeb_function_translation_inserts.update_date_time,
       #s_ltfeb_function_translation_inserts.update_user_id,
       case when s_ltfeb_function_translation.s_ltfeb_function_translation_id is null then isnull(#s_ltfeb_function_translation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #s_ltfeb_function_translation_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ltfeb_function_translation_inserts
  left join p_ltfeb_function_translation
    on #s_ltfeb_function_translation_inserts.bk_hash = p_ltfeb_function_translation.bk_hash
   and p_ltfeb_function_translation.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ltfeb_function_translation
    on p_ltfeb_function_translation.bk_hash = s_ltfeb_function_translation.bk_hash
   and p_ltfeb_function_translation.s_ltfeb_function_translation_id = s_ltfeb_function_translation.s_ltfeb_function_translation_id
 where s_ltfeb_function_translation.s_ltfeb_function_translation_id is null
    or (s_ltfeb_function_translation.s_ltfeb_function_translation_id is not null
        and s_ltfeb_function_translation.dv_hash <> #s_ltfeb_function_translation_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ltfeb_function_translation @current_dv_batch_id

end
