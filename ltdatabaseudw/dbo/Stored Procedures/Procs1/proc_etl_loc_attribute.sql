CREATE PROC [dbo].[proc_etl_loc_attribute] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_loc_attribute

set @insert_date_time = getdate()
insert into dbo.stage_hash_loc_attribute (
       bk_hash,
       attribute_id,
       val_attribute_type_id,
       attribute_value,
       udw_dim_location_attribute_key,
       udw_business_key,
       udw_source_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       location_id,
       managed_by_udw,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(attribute_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       attribute_id,
       val_attribute_type_id,
       attribute_value,
       udw_dim_location_attribute_key,
       udw_business_key,
       udw_source_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       location_id,
       managed_by_udw,
       isnull(cast(stage_loc_attribute.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_loc_attribute
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_loc_attribute @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_loc_attribute (
       bk_hash,
       attribute_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_loc_attribute.bk_hash,
       stage_hash_loc_attribute.attribute_id attribute_id,
       isnull(cast(stage_hash_loc_attribute.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       50,
       @insert_date_time,
       @user
  from stage_hash_loc_attribute
  left join h_loc_attribute
    on stage_hash_loc_attribute.bk_hash = h_loc_attribute.bk_hash
 where h_loc_attribute_id is null
   and stage_hash_loc_attribute.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_loc_attribute
if object_id('tempdb..#l_loc_attribute_inserts') is not null drop table #l_loc_attribute_inserts
create table #l_loc_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_attribute.bk_hash,
       stage_hash_loc_attribute.attribute_id attribute_id,
       stage_hash_loc_attribute.val_attribute_type_id val_attribute_type_id,
       stage_hash_loc_attribute.udw_dim_location_attribute_key udw_dim_location_attribute_key,
       stage_hash_loc_attribute.udw_business_key udw_business_key,
       stage_hash_loc_attribute.location_id location_id,
       isnull(cast(stage_hash_loc_attribute.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_attribute.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_attribute.val_attribute_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.udw_dim_location_attribute_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.udw_business_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_attribute.location_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_attribute
 where stage_hash_loc_attribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_loc_attribute records
set @insert_date_time = getdate()
insert into l_loc_attribute (
       bk_hash,
       attribute_id,
       val_attribute_type_id,
       udw_dim_location_attribute_key,
       udw_business_key,
       location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_loc_attribute_inserts.bk_hash,
       #l_loc_attribute_inserts.attribute_id,
       #l_loc_attribute_inserts.val_attribute_type_id,
       #l_loc_attribute_inserts.udw_dim_location_attribute_key,
       #l_loc_attribute_inserts.udw_business_key,
       #l_loc_attribute_inserts.location_id,
       case when l_loc_attribute.l_loc_attribute_id is null then isnull(#l_loc_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #l_loc_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_loc_attribute_inserts
  left join p_loc_attribute
    on #l_loc_attribute_inserts.bk_hash = p_loc_attribute.bk_hash
   and p_loc_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_loc_attribute
    on p_loc_attribute.bk_hash = l_loc_attribute.bk_hash
   and p_loc_attribute.l_loc_attribute_id = l_loc_attribute.l_loc_attribute_id
 where l_loc_attribute.l_loc_attribute_id is null
    or (l_loc_attribute.l_loc_attribute_id is not null
        and l_loc_attribute.dv_hash <> #l_loc_attribute_inserts.source_hash)

--calculate hash and lookup to current s_loc_attribute
if object_id('tempdb..#s_loc_attribute_inserts') is not null drop table #s_loc_attribute_inserts
create table #s_loc_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_attribute.bk_hash,
       stage_hash_loc_attribute.attribute_id attribute_id,
       stage_hash_loc_attribute.attribute_value attribute_value,
       stage_hash_loc_attribute.udw_source_name udw_source_name,
       stage_hash_loc_attribute.created_date_time created_date_time,
       stage_hash_loc_attribute.created_by created_by,
       stage_hash_loc_attribute.last_updated_date_time last_updated_date_time,
       stage_hash_loc_attribute.last_updated_by last_updated_by,
       stage_hash_loc_attribute.deleted_date_time deleted_date_time,
       stage_hash_loc_attribute.deleted_by deleted_by,
       stage_hash_loc_attribute.managed_by_udw managed_by_udw,
       isnull(cast(stage_hash_loc_attribute.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_attribute.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.attribute_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.udw_source_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_attribute.created_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_attribute.last_updated_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.last_updated_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_attribute.deleted_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_attribute.deleted_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_attribute.managed_by_udw as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_attribute
 where stage_hash_loc_attribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_loc_attribute records
set @insert_date_time = getdate()
insert into s_loc_attribute (
       bk_hash,
       attribute_id,
       attribute_value,
       udw_source_name,
       created_date_time,
       created_by,
       last_updated_date_time,
       last_updated_by,
       deleted_date_time,
       deleted_by,
       managed_by_udw,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_loc_attribute_inserts.bk_hash,
       #s_loc_attribute_inserts.attribute_id,
       #s_loc_attribute_inserts.attribute_value,
       #s_loc_attribute_inserts.udw_source_name,
       #s_loc_attribute_inserts.created_date_time,
       #s_loc_attribute_inserts.created_by,
       #s_loc_attribute_inserts.last_updated_date_time,
       #s_loc_attribute_inserts.last_updated_by,
       #s_loc_attribute_inserts.deleted_date_time,
       #s_loc_attribute_inserts.deleted_by,
       #s_loc_attribute_inserts.managed_by_udw,
       case when s_loc_attribute.s_loc_attribute_id is null then isnull(#s_loc_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #s_loc_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_loc_attribute_inserts
  left join p_loc_attribute
    on #s_loc_attribute_inserts.bk_hash = p_loc_attribute.bk_hash
   and p_loc_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_loc_attribute
    on p_loc_attribute.bk_hash = s_loc_attribute.bk_hash
   and p_loc_attribute.s_loc_attribute_id = s_loc_attribute.s_loc_attribute_id
 where s_loc_attribute.s_loc_attribute_id is null
    or (s_loc_attribute.s_loc_attribute_id is not null
        and s_loc_attribute.dv_hash <> #s_loc_attribute_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_loc_attribute @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_loc_attribute @current_dv_batch_id

end
