CREATE PROC [dbo].[proc_etl_hybris_cat_2_catrel] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_cat2catrel

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_cat2catrel (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       Qualifier,
       SourcePK,
       TargetPK,
       RSequenceNumber,
       SequenceNumber,
       languagepk,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       Qualifier,
       SourcePK,
       TargetPK,
       RSequenceNumber,
       SequenceNumber,
       languagepk,
       isnull(cast(stage_hybris_cat2catrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_cat2catrel
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_cat_2_catrel @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_cat_2_catrel (
       bk_hash,
       cat_2_catrel_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_cat2catrel.bk_hash,
       stage_hash_hybris_cat2catrel.[PK] cat_2_catrel_pk,
       isnull(cast(stage_hash_hybris_cat2catrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_cat2catrel
  left join h_hybris_cat_2_catrel
    on stage_hash_hybris_cat2catrel.bk_hash = h_hybris_cat_2_catrel.bk_hash
 where h_hybris_cat_2_catrel_id is null
   and stage_hash_hybris_cat2catrel.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_cat_2_catrel
if object_id('tempdb..#l_hybris_cat_2_catrel_inserts') is not null drop table #l_hybris_cat_2_catrel_inserts
create table #l_hybris_cat_2_catrel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_cat2catrel.bk_hash,
       stage_hash_hybris_cat2catrel.hjmpTS hjmpts,
       stage_hash_hybris_cat2catrel.TypePkString type_pk_string,
       stage_hash_hybris_cat2catrel.[PK] cat_2_catrel_pk,
       stage_hash_hybris_cat2catrel.OwnerPkString owner_pk_string,
       stage_hash_hybris_cat2catrel.SourcePK source_pk,
       stage_hash_hybris_cat2catrel.TargetPK target_pk,
       stage_hash_hybris_cat2catrel.SequenceNumber sequence_number,
       stage_hash_hybris_cat2catrel.languagepk language_pk,
       isnull(cast(stage_hash_hybris_cat2catrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.TargetPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.languagepk as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_cat2catrel
 where stage_hash_hybris_cat2catrel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_cat_2_catrel records
set @insert_date_time = getdate()
insert into l_hybris_cat_2_catrel (
       bk_hash,
       hjmpts,
       type_pk_string,
       cat_2_catrel_pk,
       owner_pk_string,
       source_pk,
       target_pk,
       sequence_number,
       language_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_cat_2_catrel_inserts.bk_hash,
       #l_hybris_cat_2_catrel_inserts.hjmpts,
       #l_hybris_cat_2_catrel_inserts.type_pk_string,
       #l_hybris_cat_2_catrel_inserts.cat_2_catrel_pk,
       #l_hybris_cat_2_catrel_inserts.owner_pk_string,
       #l_hybris_cat_2_catrel_inserts.source_pk,
       #l_hybris_cat_2_catrel_inserts.target_pk,
       #l_hybris_cat_2_catrel_inserts.sequence_number,
       #l_hybris_cat_2_catrel_inserts.language_pk,
       case when l_hybris_cat_2_catrel.l_hybris_cat_2_catrel_id is null then isnull(#l_hybris_cat_2_catrel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_cat_2_catrel_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_cat_2_catrel_inserts
  left join p_hybris_cat_2_catrel
    on #l_hybris_cat_2_catrel_inserts.bk_hash = p_hybris_cat_2_catrel.bk_hash
   and p_hybris_cat_2_catrel.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_cat_2_catrel
    on p_hybris_cat_2_catrel.bk_hash = l_hybris_cat_2_catrel.bk_hash
   and p_hybris_cat_2_catrel.l_hybris_cat_2_catrel_id = l_hybris_cat_2_catrel.l_hybris_cat_2_catrel_id
 where l_hybris_cat_2_catrel.l_hybris_cat_2_catrel_id is null
    or (l_hybris_cat_2_catrel.l_hybris_cat_2_catrel_id is not null
        and l_hybris_cat_2_catrel.dv_hash <> #l_hybris_cat_2_catrel_inserts.source_hash)

--calculate hash and lookup to current s_hybris_cat_2_catrel
if object_id('tempdb..#s_hybris_cat_2_catrel_inserts') is not null drop table #s_hybris_cat_2_catrel_inserts
create table #s_hybris_cat_2_catrel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_cat2catrel.bk_hash,
       stage_hash_hybris_cat2catrel.[PK] cat_2_catrel_pk,
       stage_hash_hybris_cat2catrel.createdTS created_ts,
       stage_hash_hybris_cat2catrel.modifiedTS modified_ts,
       stage_hash_hybris_cat2catrel.aCLTS acl_ts,
       stage_hash_hybris_cat2catrel.propTS prop_ts,
       stage_hash_hybris_cat2catrel.Qualifier qualifier,
       stage_hash_hybris_cat2catrel.RSequenceNumber r_sequence_number,
       isnull(cast(stage_hash_hybris_cat2catrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_cat2catrel.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_cat2catrel.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_cat2catrel.Qualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2catrel.RSequenceNumber as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_cat2catrel
 where stage_hash_hybris_cat2catrel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_cat_2_catrel records
set @insert_date_time = getdate()
insert into s_hybris_cat_2_catrel (
       bk_hash,
       cat_2_catrel_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       qualifier,
       r_sequence_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_cat_2_catrel_inserts.bk_hash,
       #s_hybris_cat_2_catrel_inserts.cat_2_catrel_pk,
       #s_hybris_cat_2_catrel_inserts.created_ts,
       #s_hybris_cat_2_catrel_inserts.modified_ts,
       #s_hybris_cat_2_catrel_inserts.acl_ts,
       #s_hybris_cat_2_catrel_inserts.prop_ts,
       #s_hybris_cat_2_catrel_inserts.qualifier,
       #s_hybris_cat_2_catrel_inserts.r_sequence_number,
       case when s_hybris_cat_2_catrel.s_hybris_cat_2_catrel_id is null then isnull(#s_hybris_cat_2_catrel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_cat_2_catrel_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_cat_2_catrel_inserts
  left join p_hybris_cat_2_catrel
    on #s_hybris_cat_2_catrel_inserts.bk_hash = p_hybris_cat_2_catrel.bk_hash
   and p_hybris_cat_2_catrel.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_cat_2_catrel
    on p_hybris_cat_2_catrel.bk_hash = s_hybris_cat_2_catrel.bk_hash
   and p_hybris_cat_2_catrel.s_hybris_cat_2_catrel_id = s_hybris_cat_2_catrel.s_hybris_cat_2_catrel_id
 where s_hybris_cat_2_catrel.s_hybris_cat_2_catrel_id is null
    or (s_hybris_cat_2_catrel.s_hybris_cat_2_catrel_id is not null
        and s_hybris_cat_2_catrel.dv_hash <> #s_hybris_cat_2_catrel_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_cat_2_catrel @current_dv_batch_id

end
