CREATE PROC [dbo].[proc_etl_hybris_catalogs] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_catalogs

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_catalogs (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_id,
       p_activecatalogversion,
       p_defaultcatalog,
       p_supplier,
       p_buyer,
       p_previewurltemplate,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_id,
       p_activecatalogversion,
       p_defaultcatalog,
       p_supplier,
       p_buyer,
       p_previewurltemplate,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_catalogs.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_catalogs
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_catalogs @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_catalogs (
       bk_hash,
       catalogs_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_catalogs.bk_hash,
       stage_hash_hybris_catalogs.[PK] catalogs_pk,
       isnull(cast(stage_hash_hybris_catalogs.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_catalogs
  left join h_hybris_catalogs
    on stage_hash_hybris_catalogs.bk_hash = h_hybris_catalogs.bk_hash
 where h_hybris_catalogs_id is null
   and stage_hash_hybris_catalogs.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_catalogs
if object_id('tempdb..#l_hybris_catalogs_inserts') is not null drop table #l_hybris_catalogs_inserts
create table #l_hybris_catalogs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogs.bk_hash,
       stage_hash_hybris_catalogs.TypePkString type_pk_string,
       stage_hash_hybris_catalogs.OwnerPkString owner_pk_string,
       stage_hash_hybris_catalogs.[PK] catalogs_pk,
       stage_hash_hybris_catalogs.p_activecatalogversion p_active_catalog_version,
       stage_hash_hybris_catalogs.p_supplier p_supplier,
       stage_hash_hybris_catalogs.p_buyer p_buyer,
       isnull(cast(stage_hash_hybris_catalogs.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.p_activecatalogversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.p_supplier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.p_buyer as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogs
 where stage_hash_hybris_catalogs.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_catalogs records
set @insert_date_time = getdate()
insert into l_hybris_catalogs (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       catalogs_pk,
       p_active_catalog_version,
       p_supplier,
       p_buyer,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_catalogs_inserts.bk_hash,
       #l_hybris_catalogs_inserts.type_pk_string,
       #l_hybris_catalogs_inserts.owner_pk_string,
       #l_hybris_catalogs_inserts.catalogs_pk,
       #l_hybris_catalogs_inserts.p_active_catalog_version,
       #l_hybris_catalogs_inserts.p_supplier,
       #l_hybris_catalogs_inserts.p_buyer,
       case when l_hybris_catalogs.l_hybris_catalogs_id is null then isnull(#l_hybris_catalogs_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_catalogs_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_catalogs_inserts
  left join p_hybris_catalogs
    on #l_hybris_catalogs_inserts.bk_hash = p_hybris_catalogs.bk_hash
   and p_hybris_catalogs.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_catalogs
    on p_hybris_catalogs.bk_hash = l_hybris_catalogs.bk_hash
   and p_hybris_catalogs.l_hybris_catalogs_id = l_hybris_catalogs.l_hybris_catalogs_id
 where l_hybris_catalogs.l_hybris_catalogs_id is null
    or (l_hybris_catalogs.l_hybris_catalogs_id is not null
        and l_hybris_catalogs.dv_hash <> #l_hybris_catalogs_inserts.source_hash)

--calculate hash and lookup to current s_hybris_catalogs
if object_id('tempdb..#s_hybris_catalogs_inserts') is not null drop table #s_hybris_catalogs_inserts
create table #s_hybris_catalogs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogs.bk_hash,
       stage_hash_hybris_catalogs.hjmpTS hjmpts,
       stage_hash_hybris_catalogs.createdTS created_ts,
       stage_hash_hybris_catalogs.modifiedTS modified_ts,
       stage_hash_hybris_catalogs.[PK] catalogs_pk,
       stage_hash_hybris_catalogs.p_id p_id,
       stage_hash_hybris_catalogs.p_defaultcatalog p_default_catalog,
       stage_hash_hybris_catalogs.p_previewurltemplate p_preview_url_template,
       stage_hash_hybris_catalogs.aCLTS acl_ts,
       stage_hash_hybris_catalogs.propTS prop_ts,
       isnull(cast(stage_hash_hybris_catalogs.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogs.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogs.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogs.p_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.p_defaultcatalog as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogs.p_previewurltemplate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogs
 where stage_hash_hybris_catalogs.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_catalogs records
set @insert_date_time = getdate()
insert into s_hybris_catalogs (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       catalogs_pk,
       p_id,
       p_default_catalog,
       p_preview_url_template,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_catalogs_inserts.bk_hash,
       #s_hybris_catalogs_inserts.hjmpts,
       #s_hybris_catalogs_inserts.created_ts,
       #s_hybris_catalogs_inserts.modified_ts,
       #s_hybris_catalogs_inserts.catalogs_pk,
       #s_hybris_catalogs_inserts.p_id,
       #s_hybris_catalogs_inserts.p_default_catalog,
       #s_hybris_catalogs_inserts.p_preview_url_template,
       #s_hybris_catalogs_inserts.acl_ts,
       #s_hybris_catalogs_inserts.prop_ts,
       case when s_hybris_catalogs.s_hybris_catalogs_id is null then isnull(#s_hybris_catalogs_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_catalogs_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_catalogs_inserts
  left join p_hybris_catalogs
    on #s_hybris_catalogs_inserts.bk_hash = p_hybris_catalogs.bk_hash
   and p_hybris_catalogs.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_catalogs
    on p_hybris_catalogs.bk_hash = s_hybris_catalogs.bk_hash
   and p_hybris_catalogs.s_hybris_catalogs_id = s_hybris_catalogs.s_hybris_catalogs_id
 where s_hybris_catalogs.s_hybris_catalogs_id is null
    or (s_hybris_catalogs.s_hybris_catalogs_id is not null
        and s_hybris_catalogs.dv_hash <> #s_hybris_catalogs_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_catalogs @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_catalogs @current_dv_batch_id

end
