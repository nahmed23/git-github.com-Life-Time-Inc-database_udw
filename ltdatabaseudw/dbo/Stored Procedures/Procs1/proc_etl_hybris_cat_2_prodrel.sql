CREATE PROC [dbo].[proc_etl_hybris_cat_2_prodrel] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_cat2prodrel

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_cat2prodrel (
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
       isnull(cast(stage_hybris_cat2prodrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_cat2prodrel
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_cat_2_prodrel @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_cat_2_prodrel (
       bk_hash,
       pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_cat2prodrel.bk_hash,
       stage_hash_hybris_cat2prodrel.[PK] pk,
       isnull(cast(stage_hash_hybris_cat2prodrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_cat2prodrel
  left join h_hybris_cat_2_prodrel
    on stage_hash_hybris_cat2prodrel.bk_hash = h_hybris_cat_2_prodrel.bk_hash
 where h_hybris_cat_2_prodrel_id is null
   and stage_hash_hybris_cat2prodrel.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_cat_2_prodrel
if object_id('tempdb..#l_hybris_cat_2_prodrel_inserts') is not null drop table #l_hybris_cat_2_prodrel_inserts
create table #l_hybris_cat_2_prodrel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_cat2prodrel.bk_hash,
       stage_hash_hybris_cat2prodrel.hjmpTS hjmpts,
       stage_hash_hybris_cat2prodrel.TypePkString type_pk_string,
       stage_hash_hybris_cat2prodrel.[PK] pk,
       stage_hash_hybris_cat2prodrel.OwnerPkString owner_pk_string,
       stage_hash_hybris_cat2prodrel.SourcePK source_pk,
       stage_hash_hybris_cat2prodrel.TargetPK target_pk,
       stage_hash_hybris_cat2prodrel.SequenceNumber sequence_number,
       stage_hash_hybris_cat2prodrel.languagepk language_pk,
       isnull(cast(stage_hash_hybris_cat2prodrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.SourcePK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.TargetPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.languagepk as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_cat2prodrel
 where stage_hash_hybris_cat2prodrel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_cat_2_prodrel records
set @insert_date_time = getdate()
insert into l_hybris_cat_2_prodrel (
       bk_hash,
       hjmpts,
       type_pk_string,
       pk,
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
select #l_hybris_cat_2_prodrel_inserts.bk_hash,
       #l_hybris_cat_2_prodrel_inserts.hjmpts,
       #l_hybris_cat_2_prodrel_inserts.type_pk_string,
       #l_hybris_cat_2_prodrel_inserts.pk,
       #l_hybris_cat_2_prodrel_inserts.owner_pk_string,
       #l_hybris_cat_2_prodrel_inserts.source_pk,
       #l_hybris_cat_2_prodrel_inserts.target_pk,
       #l_hybris_cat_2_prodrel_inserts.sequence_number,
       #l_hybris_cat_2_prodrel_inserts.language_pk,
       case when l_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id is null then isnull(#l_hybris_cat_2_prodrel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_cat_2_prodrel_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_cat_2_prodrel_inserts
  left join p_hybris_cat_2_prodrel
    on #l_hybris_cat_2_prodrel_inserts.bk_hash = p_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_cat_2_prodrel
    on p_hybris_cat_2_prodrel.bk_hash = l_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id = l_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id
 where l_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id is null
    or (l_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id is not null
        and l_hybris_cat_2_prodrel.dv_hash <> #l_hybris_cat_2_prodrel_inserts.source_hash)

--calculate hash and lookup to current s_hybris_cat_2_prodrel
if object_id('tempdb..#s_hybris_cat_2_prodrel_inserts') is not null drop table #s_hybris_cat_2_prodrel_inserts
create table #s_hybris_cat_2_prodrel_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_cat2prodrel.bk_hash,
       stage_hash_hybris_cat2prodrel.[PK] pk,
       stage_hash_hybris_cat2prodrel.createdTS created_ts,
       stage_hash_hybris_cat2prodrel.modifiedTS modified_ts,
       stage_hash_hybris_cat2prodrel.aCLTS acl_ts,
       stage_hash_hybris_cat2prodrel.propTS prop_ts,
       stage_hash_hybris_cat2prodrel.Qualifier qualifier,
       stage_hash_hybris_cat2prodrel.RSequenceNumber r_sequence_number,
       isnull(cast(stage_hash_hybris_cat2prodrel.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_cat2prodrel.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_cat2prodrel.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_cat2prodrel.Qualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_cat2prodrel.RSequenceNumber as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_cat2prodrel
 where stage_hash_hybris_cat2prodrel.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_cat_2_prodrel records
set @insert_date_time = getdate()
insert into s_hybris_cat_2_prodrel (
       bk_hash,
       pk,
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
select #s_hybris_cat_2_prodrel_inserts.bk_hash,
       #s_hybris_cat_2_prodrel_inserts.pk,
       #s_hybris_cat_2_prodrel_inserts.created_ts,
       #s_hybris_cat_2_prodrel_inserts.modified_ts,
       #s_hybris_cat_2_prodrel_inserts.acl_ts,
       #s_hybris_cat_2_prodrel_inserts.prop_ts,
       #s_hybris_cat_2_prodrel_inserts.qualifier,
       #s_hybris_cat_2_prodrel_inserts.r_sequence_number,
       case when s_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id is null then isnull(#s_hybris_cat_2_prodrel_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_cat_2_prodrel_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_cat_2_prodrel_inserts
  left join p_hybris_cat_2_prodrel
    on #s_hybris_cat_2_prodrel_inserts.bk_hash = p_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_cat_2_prodrel
    on p_hybris_cat_2_prodrel.bk_hash = s_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id = s_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id
 where s_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id is null
    or (s_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id is not null
        and s_hybris_cat_2_prodrel.dv_hash <> #s_hybris_cat_2_prodrel_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_cat_2_prodrel @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_cat_2_prodrel @current_dv_batch_id

end
