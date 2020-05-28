CREATE PROC [dbo].[proc_etl_hybris_catalog_versions] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_catalogversions

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_catalogversions (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       PK,
       p_active,
       p_version,
       p_mimerootdirectory,
       p_generationdate,
       p_defaultcurrency,
       p_inclfreight,
       p_inclpacking,
       p_inclassurance,
       p_inclduty,
       p_territories,
       p_languages,
       p_generatorinfo,
       p_categorysystemid,
       p_previousupdateversion,
       p_catalog,
       p_mnemonic,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PK as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       PK,
       p_active,
       p_version,
       p_mimerootdirectory,
       p_generationdate,
       p_defaultcurrency,
       p_inclfreight,
       p_inclpacking,
       p_inclassurance,
       p_inclduty,
       p_territories,
       p_languages,
       p_generatorinfo,
       p_categorysystemid,
       p_previousupdateversion,
       p_catalog,
       p_mnemonic,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_catalogversions.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_catalogversions
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_catalog_versions @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_catalog_versions (
       bk_hash,
       catalog_versions_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_catalogversions.bk_hash,
       stage_hash_hybris_catalogversions.PK catalog_versions_pk,
       isnull(cast(stage_hash_hybris_catalogversions.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_catalogversions
  left join h_hybris_catalog_versions
    on stage_hash_hybris_catalogversions.bk_hash = h_hybris_catalog_versions.bk_hash
 where h_hybris_catalog_versions_id is null
   and stage_hash_hybris_catalogversions.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_catalog_versions
if object_id('tempdb..#l_hybris_catalog_versions_inserts') is not null drop table #l_hybris_catalog_versions_inserts
create table #l_hybris_catalog_versions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogversions.bk_hash,
       stage_hash_hybris_catalogversions.TypePkString type_pk_string,
       stage_hash_hybris_catalogversions.PK catalog_versions_pk,
       stage_hash_hybris_catalogversions.p_catalog p_catalog,
       stage_hash_hybris_catalogversions.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.PK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_catalog as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogversions
 where stage_hash_hybris_catalogversions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_catalog_versions records
set @insert_date_time = getdate()
insert into l_hybris_catalog_versions (
       bk_hash,
       type_pk_string,
       catalog_versions_pk,
       p_catalog,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_catalog_versions_inserts.bk_hash,
       #l_hybris_catalog_versions_inserts.type_pk_string,
       #l_hybris_catalog_versions_inserts.catalog_versions_pk,
       #l_hybris_catalog_versions_inserts.p_catalog,
       case when l_hybris_catalog_versions.l_hybris_catalog_versions_id is null then isnull(#l_hybris_catalog_versions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_catalog_versions_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_catalog_versions_inserts
  left join p_hybris_catalog_versions
    on #l_hybris_catalog_versions_inserts.bk_hash = p_hybris_catalog_versions.bk_hash
   and p_hybris_catalog_versions.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_catalog_versions
    on p_hybris_catalog_versions.bk_hash = l_hybris_catalog_versions.bk_hash
   and p_hybris_catalog_versions.l_hybris_catalog_versions_id = l_hybris_catalog_versions.l_hybris_catalog_versions_id
 where l_hybris_catalog_versions.l_hybris_catalog_versions_id is null
    or (l_hybris_catalog_versions.l_hybris_catalog_versions_id is not null
        and l_hybris_catalog_versions.dv_hash <> #l_hybris_catalog_versions_inserts.source_hash)

--calculate hash and lookup to current s_hybris_catalog_versions
if object_id('tempdb..#s_hybris_catalog_versions_inserts') is not null drop table #s_hybris_catalog_versions_inserts
create table #s_hybris_catalog_versions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogversions.bk_hash,
       stage_hash_hybris_catalogversions.hjmpTS hjmpts,
       stage_hash_hybris_catalogversions.createdTS created_ts,
       stage_hash_hybris_catalogversions.modifiedTS modified_ts,
       stage_hash_hybris_catalogversions.OwnerPkString owner_pk_string,
       stage_hash_hybris_catalogversions.PK catalog_versions_pk,
       stage_hash_hybris_catalogversions.p_active p_active,
       stage_hash_hybris_catalogversions.p_version p_version,
       stage_hash_hybris_catalogversions.p_mimerootdirectory p_mime_root_directory,
       stage_hash_hybris_catalogversions.p_generationdate p_generation_date,
       stage_hash_hybris_catalogversions.p_defaultcurrency p_default_currency,
       stage_hash_hybris_catalogversions.p_inclfreight p_incl_freight,
       stage_hash_hybris_catalogversions.p_inclpacking p_incl_packing,
       stage_hash_hybris_catalogversions.p_inclassurance p_incl_assurance,
       stage_hash_hybris_catalogversions.p_inclduty p_incl_duty,
       stage_hash_hybris_catalogversions.p_territories p_territories,
       stage_hash_hybris_catalogversions.p_languages p_languages,
       stage_hash_hybris_catalogversions.p_generatorinfo p_generator_info,
       stage_hash_hybris_catalogversions.p_categorysystemid p_category_system_id,
       stage_hash_hybris_catalogversions.p_previousupdateversion p_previous_update_version,
       stage_hash_hybris_catalogversions.p_mnemonic p_mnemonic,
       stage_hash_hybris_catalogversions.aCLTS acl_ts,
       stage_hash_hybris_catalogversions.propTS prop_ts,
       stage_hash_hybris_catalogversions.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogversions.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogversions.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.PK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_active as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_version,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_mimerootdirectory,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogversions.p_generationdate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_defaultcurrency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_inclfreight as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_inclpacking as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_inclassurance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_inclduty as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_territories,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_languages,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_generatorinfo,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_categorysystemid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.p_previousupdateversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogversions.p_mnemonic,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogversions.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogversions
 where stage_hash_hybris_catalogversions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_catalog_versions records
set @insert_date_time = getdate()
insert into s_hybris_catalog_versions (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       owner_pk_string,
       catalog_versions_pk,
       p_active,
       p_version,
       p_mime_root_directory,
       p_generation_date,
       p_default_currency,
       p_incl_freight,
       p_incl_packing,
       p_incl_assurance,
       p_incl_duty,
       p_territories,
       p_languages,
       p_generator_info,
       p_category_system_id,
       p_previous_update_version,
       p_mnemonic,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_catalog_versions_inserts.bk_hash,
       #s_hybris_catalog_versions_inserts.hjmpts,
       #s_hybris_catalog_versions_inserts.created_ts,
       #s_hybris_catalog_versions_inserts.modified_ts,
       #s_hybris_catalog_versions_inserts.owner_pk_string,
       #s_hybris_catalog_versions_inserts.catalog_versions_pk,
       #s_hybris_catalog_versions_inserts.p_active,
       #s_hybris_catalog_versions_inserts.p_version,
       #s_hybris_catalog_versions_inserts.p_mime_root_directory,
       #s_hybris_catalog_versions_inserts.p_generation_date,
       #s_hybris_catalog_versions_inserts.p_default_currency,
       #s_hybris_catalog_versions_inserts.p_incl_freight,
       #s_hybris_catalog_versions_inserts.p_incl_packing,
       #s_hybris_catalog_versions_inserts.p_incl_assurance,
       #s_hybris_catalog_versions_inserts.p_incl_duty,
       #s_hybris_catalog_versions_inserts.p_territories,
       #s_hybris_catalog_versions_inserts.p_languages,
       #s_hybris_catalog_versions_inserts.p_generator_info,
       #s_hybris_catalog_versions_inserts.p_category_system_id,
       #s_hybris_catalog_versions_inserts.p_previous_update_version,
       #s_hybris_catalog_versions_inserts.p_mnemonic,
       #s_hybris_catalog_versions_inserts.acl_ts,
       #s_hybris_catalog_versions_inserts.prop_ts,
       case when s_hybris_catalog_versions.s_hybris_catalog_versions_id is null then isnull(#s_hybris_catalog_versions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_catalog_versions_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_catalog_versions_inserts
  left join p_hybris_catalog_versions
    on #s_hybris_catalog_versions_inserts.bk_hash = p_hybris_catalog_versions.bk_hash
   and p_hybris_catalog_versions.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_catalog_versions
    on p_hybris_catalog_versions.bk_hash = s_hybris_catalog_versions.bk_hash
   and p_hybris_catalog_versions.s_hybris_catalog_versions_id = s_hybris_catalog_versions.s_hybris_catalog_versions_id
 where s_hybris_catalog_versions.s_hybris_catalog_versions_id is null
    or (s_hybris_catalog_versions.s_hybris_catalog_versions_id is not null
        and s_hybris_catalog_versions.dv_hash <> #s_hybris_catalog_versions_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_catalog_versions @current_dv_batch_id

end
