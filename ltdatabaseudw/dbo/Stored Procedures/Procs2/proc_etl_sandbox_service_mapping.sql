﻿CREATE PROC [dbo].[proc_etl_sandbox_service_mapping] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_sandbox_ServiceMapping

set @insert_date_time = getdate()
insert into dbo.stage_hash_sandbox_ServiceMapping (
       bk_hash,
       ServiceID,
       Store_Number,
       ServiceName,
       Category,
       Segment,
       UpdatedDateTime,
       CommissionMapping,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ServiceID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(Store_Number as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ServiceID,
       Store_Number,
       ServiceName,
       Category,
       Segment,
       UpdatedDateTime,
       CommissionMapping,
       jan_one,
       isnull(cast(stage_sandbox_ServiceMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_sandbox_ServiceMapping
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_sandbox_service_mapping @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_sandbox_service_mapping (
       bk_hash,
       service_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_sandbox_ServiceMapping.bk_hash,
       stage_hash_sandbox_ServiceMapping.ServiceID service_id,
       stage_hash_sandbox_ServiceMapping.Store_Number store_number,
       isnull(cast(stage_hash_sandbox_ServiceMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       27,
       @insert_date_time,
       @user
  from stage_hash_sandbox_ServiceMapping
  left join h_sandbox_service_mapping
    on stage_hash_sandbox_ServiceMapping.bk_hash = h_sandbox_service_mapping.bk_hash
 where h_sandbox_service_mapping_id is null
   and stage_hash_sandbox_ServiceMapping.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_sandbox_service_mapping
if object_id('tempdb..#l_sandbox_service_mapping_inserts') is not null drop table #l_sandbox_service_mapping_inserts
create table #l_sandbox_service_mapping_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sandbox_ServiceMapping.bk_hash,
       stage_hash_sandbox_ServiceMapping.ServiceID service_id,
       stage_hash_sandbox_ServiceMapping.Store_Number store_number,
       isnull(cast(stage_hash_sandbox_ServiceMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_sandbox_ServiceMapping.ServiceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_sandbox_ServiceMapping.Store_Number as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sandbox_ServiceMapping
 where stage_hash_sandbox_ServiceMapping.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_sandbox_service_mapping records
set @insert_date_time = getdate()
insert into l_sandbox_service_mapping (
       bk_hash,
       service_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_sandbox_service_mapping_inserts.bk_hash,
       #l_sandbox_service_mapping_inserts.service_id,
       #l_sandbox_service_mapping_inserts.store_number,
       case when l_sandbox_service_mapping.l_sandbox_service_mapping_id is null then isnull(#l_sandbox_service_mapping_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       27,
       #l_sandbox_service_mapping_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_sandbox_service_mapping_inserts
  left join p_sandbox_service_mapping
    on #l_sandbox_service_mapping_inserts.bk_hash = p_sandbox_service_mapping.bk_hash
   and p_sandbox_service_mapping.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_sandbox_service_mapping
    on p_sandbox_service_mapping.bk_hash = l_sandbox_service_mapping.bk_hash
   and p_sandbox_service_mapping.l_sandbox_service_mapping_id = l_sandbox_service_mapping.l_sandbox_service_mapping_id
 where l_sandbox_service_mapping.l_sandbox_service_mapping_id is null
    or (l_sandbox_service_mapping.l_sandbox_service_mapping_id is not null
        and l_sandbox_service_mapping.dv_hash <> #l_sandbox_service_mapping_inserts.source_hash)

--calculate hash and lookup to current s_sandbox_service_mapping
if object_id('tempdb..#s_sandbox_service_mapping_inserts') is not null drop table #s_sandbox_service_mapping_inserts
create table #s_sandbox_service_mapping_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sandbox_ServiceMapping.bk_hash,
       stage_hash_sandbox_ServiceMapping.ServiceID service_id,
       stage_hash_sandbox_ServiceMapping.Store_Number store_number,
       stage_hash_sandbox_ServiceMapping.ServiceName service_name,
       stage_hash_sandbox_ServiceMapping.Category category,
       stage_hash_sandbox_ServiceMapping.Segment segment,
       stage_hash_sandbox_ServiceMapping.UpdatedDateTime updated_date_time,
       stage_hash_sandbox_ServiceMapping.CommissionMapping commission_mapping,
       stage_hash_sandbox_ServiceMapping.jan_one jan_one,
       isnull(cast(stage_hash_sandbox_ServiceMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_sandbox_ServiceMapping.ServiceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_sandbox_ServiceMapping.Store_Number as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_sandbox_ServiceMapping.ServiceName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_sandbox_ServiceMapping.Category,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_sandbox_ServiceMapping.Segment,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_sandbox_ServiceMapping.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_sandbox_ServiceMapping.CommissionMapping,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_sandbox_ServiceMapping.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sandbox_ServiceMapping
 where stage_hash_sandbox_ServiceMapping.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_sandbox_service_mapping records
set @insert_date_time = getdate()
insert into s_sandbox_service_mapping (
       bk_hash,
       service_id,
       store_number,
       service_name,
       category,
       segment,
       updated_date_time,
       commission_mapping,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_sandbox_service_mapping_inserts.bk_hash,
       #s_sandbox_service_mapping_inserts.service_id,
       #s_sandbox_service_mapping_inserts.store_number,
       #s_sandbox_service_mapping_inserts.service_name,
       #s_sandbox_service_mapping_inserts.category,
       #s_sandbox_service_mapping_inserts.segment,
       #s_sandbox_service_mapping_inserts.updated_date_time,
       #s_sandbox_service_mapping_inserts.commission_mapping,
       #s_sandbox_service_mapping_inserts.jan_one,
       case when s_sandbox_service_mapping.s_sandbox_service_mapping_id is null then isnull(#s_sandbox_service_mapping_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       27,
       #s_sandbox_service_mapping_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_sandbox_service_mapping_inserts
  left join p_sandbox_service_mapping
    on #s_sandbox_service_mapping_inserts.bk_hash = p_sandbox_service_mapping.bk_hash
   and p_sandbox_service_mapping.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_sandbox_service_mapping
    on p_sandbox_service_mapping.bk_hash = s_sandbox_service_mapping.bk_hash
   and p_sandbox_service_mapping.s_sandbox_service_mapping_id = s_sandbox_service_mapping.s_sandbox_service_mapping_id
 where s_sandbox_service_mapping.s_sandbox_service_mapping_id is null
    or (s_sandbox_service_mapping.s_sandbox_service_mapping_id is not null
        and s_sandbox_service_mapping.dv_hash <> #s_sandbox_service_mapping_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_sandbox_service_mapping @current_dv_batch_id

end
