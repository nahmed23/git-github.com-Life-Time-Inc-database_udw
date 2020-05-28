CREATE PROC [dbo].[proc_etl_hybris_oeperel] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_oeperel

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_oeperel (
       bk_hash,
       hjmpTS,
       TypePkString,
       OwnerPkString,
       modifiedTS,
       createdTS,
       [PK],
       RSequenceNumber,
       TargetPK,
       SequenceNumber,
       SourcePK,
       Qualifier,
       languagepk,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       OwnerPkString,
       modifiedTS,
       createdTS,
       [PK],
       RSequenceNumber,
       TargetPK,
       SequenceNumber,
       SourcePK,
       Qualifier,
       languagepk,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_oeperel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_oeperel
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_oeperel @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_oeperel (
       bk_hash,
       oeperel_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_oeperel.bk_hash,
       stage_hash_hybris_oeperel.[PK] oeperel_pk,
       isnull(cast(stage_hash_hybris_oeperel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_oeperel
  left join h_hybris_oeperel
    on stage_hash_hybris_oeperel.bk_hash = h_hybris_oeperel.bk_hash
 where h_hybris_oeperel_id is null
   and stage_hash_hybris_oeperel.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_oeperel
if object_id('tempdb..#l_hybris_oeperel_inserts') is not null drop table #l_hybris_oeperel_inserts
create table #l_hybris_oeperel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_oeperel.bk_hash,
       stage_hash_hybris_oeperel.TypePkString type_pk_string,
       stage_hash_hybris_oeperel.OwnerPkString owner_pk_string,
       stage_hash_hybris_oeperel.[PK] oeperel_pk,
       stage_hash_hybris_oeperel.TargetPK target_pk,
       stage_hash_hybris_oeperel.SourcePK source_pk,
       stage_hash_hybris_oeperel.languagepk language_pk,
       isnull(cast(stage_hash_hybris_oeperel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.TargetPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.languagepk as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_oeperel
 where stage_hash_hybris_oeperel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_oeperel records
set @insert_date_time = getdate()
insert into l_hybris_oeperel (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       oeperel_pk,
       target_pk,
       source_pk,
       language_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_oeperel_inserts.bk_hash,
       #l_hybris_oeperel_inserts.type_pk_string,
       #l_hybris_oeperel_inserts.owner_pk_string,
       #l_hybris_oeperel_inserts.oeperel_pk,
       #l_hybris_oeperel_inserts.target_pk,
       #l_hybris_oeperel_inserts.source_pk,
       #l_hybris_oeperel_inserts.language_pk,
       case when l_hybris_oeperel.l_hybris_oeperel_id is null then isnull(#l_hybris_oeperel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_oeperel_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_oeperel_inserts
  left join p_hybris_oeperel
    on #l_hybris_oeperel_inserts.bk_hash = p_hybris_oeperel.bk_hash
   and p_hybris_oeperel.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_oeperel
    on p_hybris_oeperel.bk_hash = l_hybris_oeperel.bk_hash
   and p_hybris_oeperel.l_hybris_oeperel_id = l_hybris_oeperel.l_hybris_oeperel_id
 where l_hybris_oeperel.l_hybris_oeperel_id is null
    or (l_hybris_oeperel.l_hybris_oeperel_id is not null
        and l_hybris_oeperel.dv_hash <> #l_hybris_oeperel_inserts.source_hash)

--calculate hash and lookup to current s_hybris_oeperel
if object_id('tempdb..#s_hybris_oeperel_inserts') is not null drop table #s_hybris_oeperel_inserts
create table #s_hybris_oeperel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_oeperel.bk_hash,
       stage_hash_hybris_oeperel.hjmpTS hjmpts,
       stage_hash_hybris_oeperel.modifiedTS modified_ts,
       stage_hash_hybris_oeperel.createdTS created_ts,
       stage_hash_hybris_oeperel.[PK] oeperel_pk,
       stage_hash_hybris_oeperel.RSequenceNumber r_sequence_number,
       stage_hash_hybris_oeperel.SequenceNumber sequence_number,
       stage_hash_hybris_oeperel.Qualifier qualifier,
       stage_hash_hybris_oeperel.aCLTS acl_ts,
       stage_hash_hybris_oeperel.propTS prop_ts,
       isnull(cast(stage_hash_hybris_oeperel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_oeperel.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_oeperel.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.RSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_oeperel.Qualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_oeperel.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_oeperel
 where stage_hash_hybris_oeperel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_oeperel records
set @insert_date_time = getdate()
insert into s_hybris_oeperel (
       bk_hash,
       hjmpts,
       modified_ts,
       created_ts,
       oeperel_pk,
       r_sequence_number,
       sequence_number,
       qualifier,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_oeperel_inserts.bk_hash,
       #s_hybris_oeperel_inserts.hjmpts,
       #s_hybris_oeperel_inserts.modified_ts,
       #s_hybris_oeperel_inserts.created_ts,
       #s_hybris_oeperel_inserts.oeperel_pk,
       #s_hybris_oeperel_inserts.r_sequence_number,
       #s_hybris_oeperel_inserts.sequence_number,
       #s_hybris_oeperel_inserts.qualifier,
       #s_hybris_oeperel_inserts.acl_ts,
       #s_hybris_oeperel_inserts.prop_ts,
       case when s_hybris_oeperel.s_hybris_oeperel_id is null then isnull(#s_hybris_oeperel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_oeperel_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_oeperel_inserts
  left join p_hybris_oeperel
    on #s_hybris_oeperel_inserts.bk_hash = p_hybris_oeperel.bk_hash
   and p_hybris_oeperel.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_oeperel
    on p_hybris_oeperel.bk_hash = s_hybris_oeperel.bk_hash
   and p_hybris_oeperel.s_hybris_oeperel_id = s_hybris_oeperel.s_hybris_oeperel_id
 where s_hybris_oeperel.s_hybris_oeperel_id is null
    or (s_hybris_oeperel.s_hybris_oeperel_id is not null
        and s_hybris_oeperel.dv_hash <> #s_hybris_oeperel_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_oeperel @current_dv_batch_id

end
