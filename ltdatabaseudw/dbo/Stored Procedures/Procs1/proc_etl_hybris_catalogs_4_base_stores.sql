CREATE PROC [dbo].[proc_etl_hybris_catalogs_4_base_stores] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_catalogs4basestores

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_catalogs4basestores (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       languagepk,
       Qualifier,
       SourcePK,
       TargetPK,
       SequenceNumber,
       RSequenceNumber,
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
       languagepk,
       Qualifier,
       SourcePK,
       TargetPK,
       SequenceNumber,
       RSequenceNumber,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_catalogs4basestores.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_catalogs4basestores
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_catalogs_4_base_stores @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_catalogs_4_base_stores (
       bk_hash,
       catalogs_4_base_stores_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_catalogs4basestores.bk_hash,
       stage_hash_hybris_catalogs4basestores.[PK] catalogs_4_base_stores_pk,
       isnull(cast(stage_hash_hybris_catalogs4basestores.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_catalogs4basestores
  left join h_hybris_catalogs_4_base_stores
    on stage_hash_hybris_catalogs4basestores.bk_hash = h_hybris_catalogs_4_base_stores.bk_hash
 where h_hybris_catalogs_4_base_stores_id is null
   and stage_hash_hybris_catalogs4basestores.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_catalogs_4_base_stores
if object_id('tempdb..#l_hybris_catalogs_4_base_stores_inserts') is not null drop table #l_hybris_catalogs_4_base_stores_inserts
create table #l_hybris_catalogs_4_base_stores_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogs4basestores.bk_hash,
       stage_hash_hybris_catalogs4basestores.TypePkString type_pk_string,
       stage_hash_hybris_catalogs4basestores.OwnerPkString owner_pk_string,
       stage_hash_hybris_catalogs4basestores.[PK] catalogs_4_base_stores_pk,
       stage_hash_hybris_catalogs4basestores.languagepk language_pk,
       stage_hash_hybris_catalogs4basestores.SourcePK source_pk,
       stage_hash_hybris_catalogs4basestores.TargetPK target_pk,
       stage_hash_hybris_catalogs4basestores.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.languagepk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.TargetPK as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogs4basestores
 where stage_hash_hybris_catalogs4basestores.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_catalogs_4_base_stores records
set @insert_date_time = getdate()
insert into l_hybris_catalogs_4_base_stores (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       catalogs_4_base_stores_pk,
       language_pk,
       source_pk,
       target_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_catalogs_4_base_stores_inserts.bk_hash,
       #l_hybris_catalogs_4_base_stores_inserts.type_pk_string,
       #l_hybris_catalogs_4_base_stores_inserts.owner_pk_string,
       #l_hybris_catalogs_4_base_stores_inserts.catalogs_4_base_stores_pk,
       #l_hybris_catalogs_4_base_stores_inserts.language_pk,
       #l_hybris_catalogs_4_base_stores_inserts.source_pk,
       #l_hybris_catalogs_4_base_stores_inserts.target_pk,
       case when l_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id is null then isnull(#l_hybris_catalogs_4_base_stores_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_catalogs_4_base_stores_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_catalogs_4_base_stores_inserts
  left join p_hybris_catalogs_4_base_stores
    on #l_hybris_catalogs_4_base_stores_inserts.bk_hash = p_hybris_catalogs_4_base_stores.bk_hash
   and p_hybris_catalogs_4_base_stores.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_catalogs_4_base_stores
    on p_hybris_catalogs_4_base_stores.bk_hash = l_hybris_catalogs_4_base_stores.bk_hash
   and p_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id = l_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id
 where l_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id is null
    or (l_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id is not null
        and l_hybris_catalogs_4_base_stores.dv_hash <> #l_hybris_catalogs_4_base_stores_inserts.source_hash)

--calculate hash and lookup to current s_hybris_catalogs_4_base_stores
if object_id('tempdb..#s_hybris_catalogs_4_base_stores_inserts') is not null drop table #s_hybris_catalogs_4_base_stores_inserts
create table #s_hybris_catalogs_4_base_stores_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_catalogs4basestores.bk_hash,
       stage_hash_hybris_catalogs4basestores.hjmpTS hjmpts,
       stage_hash_hybris_catalogs4basestores.createdTS created_ts,
       stage_hash_hybris_catalogs4basestores.modifiedTS modified_ts,
       stage_hash_hybris_catalogs4basestores.[PK] catalogs_4_base_stores_pk,
       stage_hash_hybris_catalogs4basestores.Qualifier qualifier,
       stage_hash_hybris_catalogs4basestores.SequenceNumber sequence_number,
       stage_hash_hybris_catalogs4basestores.RSequenceNumber r_sequence_number,
       stage_hash_hybris_catalogs4basestores.aCLTS acl_ts,
       stage_hash_hybris_catalogs4basestores.propTS prop_ts,
       stage_hash_hybris_catalogs4basestores.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogs4basestores.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_catalogs4basestores.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_catalogs4basestores.Qualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.RSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_catalogs4basestores.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_catalogs4basestores
 where stage_hash_hybris_catalogs4basestores.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_catalogs_4_base_stores records
set @insert_date_time = getdate()
insert into s_hybris_catalogs_4_base_stores (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       catalogs_4_base_stores_pk,
       qualifier,
       sequence_number,
       r_sequence_number,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_catalogs_4_base_stores_inserts.bk_hash,
       #s_hybris_catalogs_4_base_stores_inserts.hjmpts,
       #s_hybris_catalogs_4_base_stores_inserts.created_ts,
       #s_hybris_catalogs_4_base_stores_inserts.modified_ts,
       #s_hybris_catalogs_4_base_stores_inserts.catalogs_4_base_stores_pk,
       #s_hybris_catalogs_4_base_stores_inserts.qualifier,
       #s_hybris_catalogs_4_base_stores_inserts.sequence_number,
       #s_hybris_catalogs_4_base_stores_inserts.r_sequence_number,
       #s_hybris_catalogs_4_base_stores_inserts.acl_ts,
       #s_hybris_catalogs_4_base_stores_inserts.prop_ts,
       case when s_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id is null then isnull(#s_hybris_catalogs_4_base_stores_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_catalogs_4_base_stores_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_catalogs_4_base_stores_inserts
  left join p_hybris_catalogs_4_base_stores
    on #s_hybris_catalogs_4_base_stores_inserts.bk_hash = p_hybris_catalogs_4_base_stores.bk_hash
   and p_hybris_catalogs_4_base_stores.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_catalogs_4_base_stores
    on p_hybris_catalogs_4_base_stores.bk_hash = s_hybris_catalogs_4_base_stores.bk_hash
   and p_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id = s_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id
 where s_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id is null
    or (s_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id is not null
        and s_hybris_catalogs_4_base_stores.dv_hash <> #s_hybris_catalogs_4_base_stores_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_catalogs_4_base_stores @current_dv_batch_id

end
