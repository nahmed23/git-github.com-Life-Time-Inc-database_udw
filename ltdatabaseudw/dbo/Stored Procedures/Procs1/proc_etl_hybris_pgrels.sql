CREATE PROC [dbo].[proc_etl_hybris_pgrels] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_pgrels

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_pgrels (
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
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(languagepk as varchar(500)),'z#@$k%&P'))),2) bk_hash,
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
       isnull(cast(stage_hybris_pgrels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_pgrels
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_pgrels @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_pgrels (
       bk_hash,
       pgrels_pk,
       language_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_pgrels.bk_hash,
       stage_hash_hybris_pgrels.[PK] pgrels_pk,
       stage_hash_hybris_pgrels.languagepk language_pk,
       isnull(cast(stage_hash_hybris_pgrels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_pgrels
  left join h_hybris_pgrels
    on stage_hash_hybris_pgrels.bk_hash = h_hybris_pgrels.bk_hash
 where h_hybris_pgrels_id is null
   and stage_hash_hybris_pgrels.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_pgrels
if object_id('tempdb..#l_hybris_pgrels_inserts') is not null drop table #l_hybris_pgrels_inserts
create table #l_hybris_pgrels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pgrels.bk_hash,
       stage_hash_hybris_pgrels.TypePkString type_pk_string,
       stage_hash_hybris_pgrels.OwnerPkString owner_pk_string,
       stage_hash_hybris_pgrels.[PK] pgrels_pk,
       stage_hash_hybris_pgrels.languagepk language_pk,
       stage_hash_hybris_pgrels.SourcePK source_pk,
       stage_hash_hybris_pgrels.TargetPK target_pk,
       stage_hash_hybris_pgrels.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.languagepk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.TargetPK as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pgrels
 where stage_hash_hybris_pgrels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_pgrels records
set @insert_date_time = getdate()
insert into l_hybris_pgrels (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       pgrels_pk,
       language_pk,
       source_pk,
       target_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_pgrels_inserts.bk_hash,
       #l_hybris_pgrels_inserts.type_pk_string,
       #l_hybris_pgrels_inserts.owner_pk_string,
       #l_hybris_pgrels_inserts.pgrels_pk,
       #l_hybris_pgrels_inserts.language_pk,
       #l_hybris_pgrels_inserts.source_pk,
       #l_hybris_pgrels_inserts.target_pk,
       case when l_hybris_pgrels.l_hybris_pgrels_id is null then isnull(#l_hybris_pgrels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_pgrels_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_pgrels_inserts
  left join p_hybris_pgrels
    on #l_hybris_pgrels_inserts.bk_hash = p_hybris_pgrels.bk_hash
   and p_hybris_pgrels.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_pgrels
    on p_hybris_pgrels.bk_hash = l_hybris_pgrels.bk_hash
   and p_hybris_pgrels.l_hybris_pgrels_id = l_hybris_pgrels.l_hybris_pgrels_id
 where l_hybris_pgrels.l_hybris_pgrels_id is null
    or (l_hybris_pgrels.l_hybris_pgrels_id is not null
        and l_hybris_pgrels.dv_hash <> #l_hybris_pgrels_inserts.source_hash)

--calculate hash and lookup to current s_hybris_pgrels
if object_id('tempdb..#s_hybris_pgrels_inserts') is not null drop table #s_hybris_pgrels_inserts
create table #s_hybris_pgrels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pgrels.bk_hash,
       stage_hash_hybris_pgrels.hjmpTS hjmpts,
       stage_hash_hybris_pgrels.createdTS created_ts,
       stage_hash_hybris_pgrels.modifiedTS modified_ts,
       stage_hash_hybris_pgrels.[PK] pgrels_pk,
       stage_hash_hybris_pgrels.languagepk language_pk,
       stage_hash_hybris_pgrels.Qualifier qualifier,
       stage_hash_hybris_pgrels.SourcePK source_pk,
       stage_hash_hybris_pgrels.TargetPK target_pk,
       stage_hash_hybris_pgrels.SequenceNumber sequence_number,
       stage_hash_hybris_pgrels.RSequenceNumber r_sequence_number,
       stage_hash_hybris_pgrels.aCLTS acl_ts,
       stage_hash_hybris_pgrels.propTS prop_ts,
       stage_hash_hybris_pgrels.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pgrels.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pgrels.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.languagepk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_pgrels.Qualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.TargetPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.RSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pgrels.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pgrels
 where stage_hash_hybris_pgrels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_pgrels records
set @insert_date_time = getdate()
insert into s_hybris_pgrels (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       pgrels_pk,
       language_pk,
       qualifier,
       source_pk,
       target_pk,
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
select #s_hybris_pgrels_inserts.bk_hash,
       #s_hybris_pgrels_inserts.hjmpts,
       #s_hybris_pgrels_inserts.created_ts,
       #s_hybris_pgrels_inserts.modified_ts,
       #s_hybris_pgrels_inserts.pgrels_pk,
       #s_hybris_pgrels_inserts.language_pk,
       #s_hybris_pgrels_inserts.qualifier,
       #s_hybris_pgrels_inserts.source_pk,
       #s_hybris_pgrels_inserts.target_pk,
       #s_hybris_pgrels_inserts.sequence_number,
       #s_hybris_pgrels_inserts.r_sequence_number,
       #s_hybris_pgrels_inserts.acl_ts,
       #s_hybris_pgrels_inserts.prop_ts,
       case when s_hybris_pgrels.s_hybris_pgrels_id is null then isnull(#s_hybris_pgrels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_pgrels_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_pgrels_inserts
  left join p_hybris_pgrels
    on #s_hybris_pgrels_inserts.bk_hash = p_hybris_pgrels.bk_hash
   and p_hybris_pgrels.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_pgrels
    on p_hybris_pgrels.bk_hash = s_hybris_pgrels.bk_hash
   and p_hybris_pgrels.s_hybris_pgrels_id = s_hybris_pgrels.s_hybris_pgrels_id
 where s_hybris_pgrels.s_hybris_pgrels_id is null
    or (s_hybris_pgrels.s_hybris_pgrels_id is not null
        and s_hybris_pgrels.dv_hash <> #s_hybris_pgrels_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_pgrels @current_dv_batch_id

end
