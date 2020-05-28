CREATE PROC [dbo].[proc_etl_magento_catalog_product_entity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_catalog_product_entity

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_catalog_product_entity (
       bk_hash,
       row_id,
       entity_id,
       created_in,
       updated_in,
       attribute_set_id,
       type_id,
       sku,
       has_options,
       required_options,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(row_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       row_id,
       entity_id,
       created_in,
       updated_in,
       attribute_set_id,
       type_id,
       sku,
       has_options,
       required_options,
       created_at,
       updated_at,
       isnull(cast(stage_magento_catalog_product_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_catalog_product_entity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_product_entity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_product_entity (
       bk_hash,
       row_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_catalog_product_entity.bk_hash,
       stage_hash_magento_catalog_product_entity.row_id row_id,
       isnull(cast(stage_hash_magento_catalog_product_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_catalog_product_entity
  left join h_magento_catalog_product_entity
    on stage_hash_magento_catalog_product_entity.bk_hash = h_magento_catalog_product_entity.bk_hash
 where h_magento_catalog_product_entity_id is null
   and stage_hash_magento_catalog_product_entity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_catalog_product_entity
if object_id('tempdb..#l_magento_catalog_product_entity_inserts') is not null drop table #l_magento_catalog_product_entity_inserts
create table #l_magento_catalog_product_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalog_product_entity.bk_hash,
       stage_hash_magento_catalog_product_entity.row_id row_id,
       stage_hash_magento_catalog_product_entity.entity_id entity_id,
       stage_hash_magento_catalog_product_entity.attribute_set_id attribute_set_id,
       stage_hash_magento_catalog_product_entity.sku sku,
       isnull(cast(stage_hash_magento_catalog_product_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.attribute_set_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_product_entity.sku,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalog_product_entity
 where stage_hash_magento_catalog_product_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_catalog_product_entity records
set @insert_date_time = getdate()
insert into l_magento_catalog_product_entity (
       bk_hash,
       row_id,
       entity_id,
       attribute_set_id,
       sku,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_catalog_product_entity_inserts.bk_hash,
       #l_magento_catalog_product_entity_inserts.row_id,
       #l_magento_catalog_product_entity_inserts.entity_id,
       #l_magento_catalog_product_entity_inserts.attribute_set_id,
       #l_magento_catalog_product_entity_inserts.sku,
       case when l_magento_catalog_product_entity.l_magento_catalog_product_entity_id is null then isnull(#l_magento_catalog_product_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_catalog_product_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_catalog_product_entity_inserts
  left join p_magento_catalog_product_entity
    on #l_magento_catalog_product_entity_inserts.bk_hash = p_magento_catalog_product_entity.bk_hash
   and p_magento_catalog_product_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_catalog_product_entity
    on p_magento_catalog_product_entity.bk_hash = l_magento_catalog_product_entity.bk_hash
   and p_magento_catalog_product_entity.l_magento_catalog_product_entity_id = l_magento_catalog_product_entity.l_magento_catalog_product_entity_id
 where l_magento_catalog_product_entity.l_magento_catalog_product_entity_id is null
    or (l_magento_catalog_product_entity.l_magento_catalog_product_entity_id is not null
        and l_magento_catalog_product_entity.dv_hash <> #l_magento_catalog_product_entity_inserts.source_hash)

--calculate hash and lookup to current s_magento_catalog_product_entity
if object_id('tempdb..#s_magento_catalog_product_entity_inserts') is not null drop table #s_magento_catalog_product_entity_inserts
create table #s_magento_catalog_product_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalog_product_entity.bk_hash,
       stage_hash_magento_catalog_product_entity.row_id row_id,
       stage_hash_magento_catalog_product_entity.created_in created_in,
       stage_hash_magento_catalog_product_entity.updated_in updated_in,
       stage_hash_magento_catalog_product_entity.type_id type_id,
       stage_hash_magento_catalog_product_entity.has_options has_options,
       stage_hash_magento_catalog_product_entity.required_options required_options,
       stage_hash_magento_catalog_product_entity.created_at created_at,
       stage_hash_magento_catalog_product_entity.updated_at updated_at,
       isnull(cast(stage_hash_magento_catalog_product_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.created_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.updated_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_product_entity.type_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.has_options as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_product_entity.required_options as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_product_entity.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_product_entity.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalog_product_entity
 where stage_hash_magento_catalog_product_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_product_entity records
set @insert_date_time = getdate()
insert into s_magento_catalog_product_entity (
       bk_hash,
       row_id,
       created_in,
       updated_in,
       type_id,
       has_options,
       required_options,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_product_entity_inserts.bk_hash,
       #s_magento_catalog_product_entity_inserts.row_id,
       #s_magento_catalog_product_entity_inserts.created_in,
       #s_magento_catalog_product_entity_inserts.updated_in,
       #s_magento_catalog_product_entity_inserts.type_id,
       #s_magento_catalog_product_entity_inserts.has_options,
       #s_magento_catalog_product_entity_inserts.required_options,
       #s_magento_catalog_product_entity_inserts.created_at,
       #s_magento_catalog_product_entity_inserts.updated_at,
       case when s_magento_catalog_product_entity.s_magento_catalog_product_entity_id is null then isnull(#s_magento_catalog_product_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_product_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_product_entity_inserts
  left join p_magento_catalog_product_entity
    on #s_magento_catalog_product_entity_inserts.bk_hash = p_magento_catalog_product_entity.bk_hash
   and p_magento_catalog_product_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_product_entity
    on p_magento_catalog_product_entity.bk_hash = s_magento_catalog_product_entity.bk_hash
   and p_magento_catalog_product_entity.s_magento_catalog_product_entity_id = s_magento_catalog_product_entity.s_magento_catalog_product_entity_id
 where s_magento_catalog_product_entity.s_magento_catalog_product_entity_id is null
    or (s_magento_catalog_product_entity.s_magento_catalog_product_entity_id is not null
        and s_magento_catalog_product_entity.dv_hash <> #s_magento_catalog_product_entity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_product_entity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_catalog_product_entity @current_dv_batch_id

end
