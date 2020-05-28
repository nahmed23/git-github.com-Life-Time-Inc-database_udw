CREATE PROC [dbo].[proc_etl_magento_eav_attribute] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_eav_attribute

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_eav_attribute (
       bk_hash,
       attribute_id,
       entity_type_id,
       attribute_code,
       attribute_model,
       backend_model,
       backend_type,
       backend_table,
       frontend_model,
       frontend_input,
       frontend_label,
       frontend_class,
       source_model,
       is_required,
       is_user_defined,
       default_value,
       is_unique,
       note,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(attribute_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       attribute_id,
       entity_type_id,
       attribute_code,
       attribute_model,
       backend_model,
       backend_type,
       backend_table,
       frontend_model,
       frontend_input,
       frontend_label,
       frontend_class,
       source_model,
       is_required,
       is_user_defined,
       default_value,
       is_unique,
       note,
       dummy_modified_date_time,
       isnull(cast(stage_magento_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_eav_attribute
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_eav_attribute @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_eav_attribute (
       bk_hash,
       attribute_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_eav_attribute.bk_hash,
       stage_hash_magento_eav_attribute.attribute_id attribute_id,
       isnull(cast(stage_hash_magento_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_eav_attribute
  left join h_magento_eav_attribute
    on stage_hash_magento_eav_attribute.bk_hash = h_magento_eav_attribute.bk_hash
 where h_magento_eav_attribute_id is null
   and stage_hash_magento_eav_attribute.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_eav_attribute
if object_id('tempdb..#l_magento_eav_attribute_inserts') is not null drop table #l_magento_eav_attribute_inserts
create table #l_magento_eav_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_eav_attribute.bk_hash,
       stage_hash_magento_eav_attribute.attribute_id attribute_id,
       stage_hash_magento_eav_attribute.entity_type_id entity_type_id,
       isnull(cast(stage_hash_magento_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.entity_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_eav_attribute
 where stage_hash_magento_eav_attribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_eav_attribute records
set @insert_date_time = getdate()
insert into l_magento_eav_attribute (
       bk_hash,
       attribute_id,
       entity_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_eav_attribute_inserts.bk_hash,
       #l_magento_eav_attribute_inserts.attribute_id,
       #l_magento_eav_attribute_inserts.entity_type_id,
       case when l_magento_eav_attribute.l_magento_eav_attribute_id is null then isnull(#l_magento_eav_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_eav_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_eav_attribute_inserts
  left join p_magento_eav_attribute
    on #l_magento_eav_attribute_inserts.bk_hash = p_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_eav_attribute
    on p_magento_eav_attribute.bk_hash = l_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.l_magento_eav_attribute_id = l_magento_eav_attribute.l_magento_eav_attribute_id
 where l_magento_eav_attribute.l_magento_eav_attribute_id is null
    or (l_magento_eav_attribute.l_magento_eav_attribute_id is not null
        and l_magento_eav_attribute.dv_hash <> #l_magento_eav_attribute_inserts.source_hash)

--calculate hash and lookup to current s_magento_eav_attribute
if object_id('tempdb..#s_magento_eav_attribute_inserts') is not null drop table #s_magento_eav_attribute_inserts
create table #s_magento_eav_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_eav_attribute.bk_hash,
       stage_hash_magento_eav_attribute.attribute_id attribute_id,
       stage_hash_magento_eav_attribute.attribute_code attribute_code,
       stage_hash_magento_eav_attribute.attribute_model attribute_model,
       stage_hash_magento_eav_attribute.backend_model backend_model,
       stage_hash_magento_eav_attribute.backend_type backend_type,
       stage_hash_magento_eav_attribute.backend_table backend_table,
       stage_hash_magento_eav_attribute.frontend_model frontend_model,
       stage_hash_magento_eav_attribute.frontend_input frontend_input,
       stage_hash_magento_eav_attribute.frontend_label frontend_label,
       stage_hash_magento_eav_attribute.frontend_class frontend_class,
       stage_hash_magento_eav_attribute.source_model source_model,
       stage_hash_magento_eav_attribute.is_required is_required,
       stage_hash_magento_eav_attribute.is_user_defined is_user_defined,
       stage_hash_magento_eav_attribute.default_value default_value,
       stage_hash_magento_eav_attribute.is_unique is_unique,
       stage_hash_magento_eav_attribute.note note,
       stage_hash_magento_eav_attribute.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.attribute_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.attribute_model,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.backend_model,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.backend_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.backend_table,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.frontend_model,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.frontend_input,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.frontend_label,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.frontend_class,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.source_model,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.is_required as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.is_user_defined as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.default_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute.is_unique as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute.note,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_eav_attribute.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_eav_attribute
 where stage_hash_magento_eav_attribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_eav_attribute records
set @insert_date_time = getdate()
insert into s_magento_eav_attribute (
       bk_hash,
       attribute_id,
       attribute_code,
       attribute_model,
       backend_model,
       backend_type,
       backend_table,
       frontend_model,
       frontend_input,
       frontend_label,
       frontend_class,
       source_model,
       is_required,
       is_user_defined,
       default_value,
       is_unique,
       note,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_eav_attribute_inserts.bk_hash,
       #s_magento_eav_attribute_inserts.attribute_id,
       #s_magento_eav_attribute_inserts.attribute_code,
       #s_magento_eav_attribute_inserts.attribute_model,
       #s_magento_eav_attribute_inserts.backend_model,
       #s_magento_eav_attribute_inserts.backend_type,
       #s_magento_eav_attribute_inserts.backend_table,
       #s_magento_eav_attribute_inserts.frontend_model,
       #s_magento_eav_attribute_inserts.frontend_input,
       #s_magento_eav_attribute_inserts.frontend_label,
       #s_magento_eav_attribute_inserts.frontend_class,
       #s_magento_eav_attribute_inserts.source_model,
       #s_magento_eav_attribute_inserts.is_required,
       #s_magento_eav_attribute_inserts.is_user_defined,
       #s_magento_eav_attribute_inserts.default_value,
       #s_magento_eav_attribute_inserts.is_unique,
       #s_magento_eav_attribute_inserts.note,
       #s_magento_eav_attribute_inserts.dummy_modified_date_time,
       case when s_magento_eav_attribute.s_magento_eav_attribute_id is null then isnull(#s_magento_eav_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_eav_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_eav_attribute_inserts
  left join p_magento_eav_attribute
    on #s_magento_eav_attribute_inserts.bk_hash = p_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_eav_attribute
    on p_magento_eav_attribute.bk_hash = s_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.s_magento_eav_attribute_id = s_magento_eav_attribute.s_magento_eav_attribute_id
 where s_magento_eav_attribute.s_magento_eav_attribute_id is null
    or (s_magento_eav_attribute.s_magento_eav_attribute_id is not null
        and s_magento_eav_attribute.dv_hash <> #s_magento_eav_attribute_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_eav_attribute @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_eav_attribute @current_dv_batch_id

end
