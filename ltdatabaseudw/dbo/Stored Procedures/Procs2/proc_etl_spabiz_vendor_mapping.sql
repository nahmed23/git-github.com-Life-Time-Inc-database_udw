﻿CREATE PROC [dbo].[proc_etl_spabiz_vendor_mapping] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_vendormapping

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_vendormapping (
       bk_hash,
       idvendormapping,
       spabiz_vendordatabaseid,
       workday_supplierid,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(spabiz_vendordatabaseid as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       idvendormapping,
       spabiz_vendordatabaseid,
       workday_supplierid,
       jan_one,
       isnull(cast(stage_spabiz_vendormapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_vendormapping
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_vendor_mapping @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_vendor_mapping (
       bk_hash,
       spabiz_vendor_database_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_vendormapping.bk_hash,
       stage_hash_spabiz_vendormapping.spabiz_vendordatabaseid spabiz_vendor_database_id,
       isnull(cast(stage_hash_spabiz_vendormapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_vendormapping
  left join h_spabiz_vendor_mapping
    on stage_hash_spabiz_vendormapping.bk_hash = h_spabiz_vendor_mapping.bk_hash
 where h_spabiz_vendor_mapping_id is null
   and stage_hash_spabiz_vendormapping.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_vendor_mapping
if object_id('tempdb..#l_spabiz_vendor_mapping_inserts') is not null drop table #l_spabiz_vendor_mapping_inserts
create table #l_spabiz_vendor_mapping_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_vendormapping.bk_hash,
       stage_hash_spabiz_vendormapping.idvendormapping id_vendor_mapping,
       stage_hash_spabiz_vendormapping.spabiz_vendordatabaseid spabiz_vendor_database_id,
       stage_hash_spabiz_vendormapping.workday_supplierid workday_supplier_id,
       stage_hash_spabiz_vendormapping.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_vendormapping.idvendormapping as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_vendormapping.spabiz_vendordatabaseid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_vendormapping.workday_supplierid as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_vendormapping
 where stage_hash_spabiz_vendormapping.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_vendor_mapping records
set @insert_date_time = getdate()
insert into l_spabiz_vendor_mapping (
       bk_hash,
       id_vendor_mapping,
       spabiz_vendor_database_id,
       workday_supplier_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_vendor_mapping_inserts.bk_hash,
       #l_spabiz_vendor_mapping_inserts.id_vendor_mapping,
       #l_spabiz_vendor_mapping_inserts.spabiz_vendor_database_id,
       #l_spabiz_vendor_mapping_inserts.workday_supplier_id,
       case when l_spabiz_vendor_mapping.l_spabiz_vendor_mapping_id is null then isnull(#l_spabiz_vendor_mapping_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_vendor_mapping_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_vendor_mapping_inserts
  left join p_spabiz_vendor_mapping
    on #l_spabiz_vendor_mapping_inserts.bk_hash = p_spabiz_vendor_mapping.bk_hash
   and p_spabiz_vendor_mapping.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_vendor_mapping
    on p_spabiz_vendor_mapping.bk_hash = l_spabiz_vendor_mapping.bk_hash
   and p_spabiz_vendor_mapping.l_spabiz_vendor_mapping_id = l_spabiz_vendor_mapping.l_spabiz_vendor_mapping_id
 where l_spabiz_vendor_mapping.l_spabiz_vendor_mapping_id is null
    or (l_spabiz_vendor_mapping.l_spabiz_vendor_mapping_id is not null
        and l_spabiz_vendor_mapping.dv_hash <> #l_spabiz_vendor_mapping_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_vendor_mapping
if object_id('tempdb..#s_spabiz_vendor_mapping_inserts') is not null drop table #s_spabiz_vendor_mapping_inserts
create table #s_spabiz_vendor_mapping_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_vendormapping.bk_hash,
       stage_hash_spabiz_vendormapping.spabiz_vendordatabaseid spabiz_vendor_database_id,
       stage_hash_spabiz_vendormapping.jan_one jan_one,
       stage_hash_spabiz_vendormapping.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_vendormapping.spabiz_vendordatabaseid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_vendormapping.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_vendormapping
 where stage_hash_spabiz_vendormapping.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_vendor_mapping records
set @insert_date_time = getdate()
insert into s_spabiz_vendor_mapping (
       bk_hash,
       spabiz_vendor_database_id,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_vendor_mapping_inserts.bk_hash,
       #s_spabiz_vendor_mapping_inserts.spabiz_vendor_database_id,
       #s_spabiz_vendor_mapping_inserts.jan_one,
       case when s_spabiz_vendor_mapping.s_spabiz_vendor_mapping_id is null then isnull(#s_spabiz_vendor_mapping_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_vendor_mapping_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_vendor_mapping_inserts
  left join p_spabiz_vendor_mapping
    on #s_spabiz_vendor_mapping_inserts.bk_hash = p_spabiz_vendor_mapping.bk_hash
   and p_spabiz_vendor_mapping.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_vendor_mapping
    on p_spabiz_vendor_mapping.bk_hash = s_spabiz_vendor_mapping.bk_hash
   and p_spabiz_vendor_mapping.s_spabiz_vendor_mapping_id = s_spabiz_vendor_mapping.s_spabiz_vendor_mapping_id
 where s_spabiz_vendor_mapping.s_spabiz_vendor_mapping_id is null
    or (s_spabiz_vendor_mapping.s_spabiz_vendor_mapping_id is not null
        and s_spabiz_vendor_mapping.dv_hash <> #s_spabiz_vendor_mapping_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_vendor_mapping @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_vendor_mapping @current_dv_batch_id

end
