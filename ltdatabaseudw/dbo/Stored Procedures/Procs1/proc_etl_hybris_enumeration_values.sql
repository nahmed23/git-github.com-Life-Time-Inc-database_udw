CREATE PROC [dbo].[proc_etl_hybris_enumeration_values] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_enumerationvalues

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_enumerationvalues (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       Code,
       codeLowerCase,
       SequenceNumber,
       p_extensionname,
       p_icon,
       aCLTS,
       propTS,
       Editable,
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
       Code,
       codeLowerCase,
       SequenceNumber,
       p_extensionname,
       p_icon,
       aCLTS,
       propTS,
       Editable,
       isnull(cast(stage_hybris_enumerationvalues.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_enumerationvalues
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_enumeration_values @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_enumeration_values (
       bk_hash,
       enumeration_values_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_enumerationvalues.bk_hash,
       stage_hash_hybris_enumerationvalues.[PK] enumeration_values_pk,
       isnull(cast(stage_hash_hybris_enumerationvalues.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_enumerationvalues
  left join h_hybris_enumeration_values
    on stage_hash_hybris_enumerationvalues.bk_hash = h_hybris_enumeration_values.bk_hash
 where h_hybris_enumeration_values_id is null
   and stage_hash_hybris_enumerationvalues.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_enumeration_values
if object_id('tempdb..#l_hybris_enumeration_values_inserts') is not null drop table #l_hybris_enumeration_values_inserts
create table #l_hybris_enumeration_values_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_enumerationvalues.bk_hash,
       stage_hash_hybris_enumerationvalues.TypePkString type_pk_string,
       stage_hash_hybris_enumerationvalues.OwnerPkString owner_pk_string,
       stage_hash_hybris_enumerationvalues.[PK] enumeration_values_pk,
       stage_hash_hybris_enumerationvalues.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.[PK] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_enumerationvalues
 where stage_hash_hybris_enumerationvalues.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_enumeration_values records
set @insert_date_time = getdate()
insert into l_hybris_enumeration_values (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       enumeration_values_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_enumeration_values_inserts.bk_hash,
       #l_hybris_enumeration_values_inserts.type_pk_string,
       #l_hybris_enumeration_values_inserts.owner_pk_string,
       #l_hybris_enumeration_values_inserts.enumeration_values_pk,
       case when l_hybris_enumeration_values.l_hybris_enumeration_values_id is null then isnull(#l_hybris_enumeration_values_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_enumeration_values_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_enumeration_values_inserts
  left join p_hybris_enumeration_values
    on #l_hybris_enumeration_values_inserts.bk_hash = p_hybris_enumeration_values.bk_hash
   and p_hybris_enumeration_values.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_enumeration_values
    on p_hybris_enumeration_values.bk_hash = l_hybris_enumeration_values.bk_hash
   and p_hybris_enumeration_values.l_hybris_enumeration_values_id = l_hybris_enumeration_values.l_hybris_enumeration_values_id
 where l_hybris_enumeration_values.l_hybris_enumeration_values_id is null
    or (l_hybris_enumeration_values.l_hybris_enumeration_values_id is not null
        and l_hybris_enumeration_values.dv_hash <> #l_hybris_enumeration_values_inserts.source_hash)

--calculate hash and lookup to current s_hybris_enumeration_values
if object_id('tempdb..#s_hybris_enumeration_values_inserts') is not null drop table #s_hybris_enumeration_values_inserts
create table #s_hybris_enumeration_values_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_enumerationvalues.bk_hash,
       stage_hash_hybris_enumerationvalues.hjmpTS hjmpts,
       stage_hash_hybris_enumerationvalues.createdTS created_ts,
       stage_hash_hybris_enumerationvalues.modifiedTS modified_ts,
       stage_hash_hybris_enumerationvalues.[PK] enumeration_values_pk,
       stage_hash_hybris_enumerationvalues.Code code,
       stage_hash_hybris_enumerationvalues.codeLowerCase code_lower_case,
       stage_hash_hybris_enumerationvalues.SequenceNumber sequence_number,
       stage_hash_hybris_enumerationvalues.p_extensionname p_extension_name,
       stage_hash_hybris_enumerationvalues.p_icon p_icon,
       stage_hash_hybris_enumerationvalues.aCLTS acl_ts,
       stage_hash_hybris_enumerationvalues.propTS prop_ts,
       stage_hash_hybris_enumerationvalues.Editable editable,
       stage_hash_hybris_enumerationvalues.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_enumerationvalues.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_enumerationvalues.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_enumerationvalues.Code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_enumerationvalues.codeLowerCase,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.SequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_enumerationvalues.p_extensionname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.p_icon as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_enumerationvalues.Editable as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_enumerationvalues
 where stage_hash_hybris_enumerationvalues.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_enumeration_values records
set @insert_date_time = getdate()
insert into s_hybris_enumeration_values (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       enumeration_values_pk,
       code,
       code_lower_case,
       sequence_number,
       p_extension_name,
       p_icon,
       acl_ts,
       prop_ts,
       editable,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_enumeration_values_inserts.bk_hash,
       #s_hybris_enumeration_values_inserts.hjmpts,
       #s_hybris_enumeration_values_inserts.created_ts,
       #s_hybris_enumeration_values_inserts.modified_ts,
       #s_hybris_enumeration_values_inserts.enumeration_values_pk,
       #s_hybris_enumeration_values_inserts.code,
       #s_hybris_enumeration_values_inserts.code_lower_case,
       #s_hybris_enumeration_values_inserts.sequence_number,
       #s_hybris_enumeration_values_inserts.p_extension_name,
       #s_hybris_enumeration_values_inserts.p_icon,
       #s_hybris_enumeration_values_inserts.acl_ts,
       #s_hybris_enumeration_values_inserts.prop_ts,
       #s_hybris_enumeration_values_inserts.editable,
       case when s_hybris_enumeration_values.s_hybris_enumeration_values_id is null then isnull(#s_hybris_enumeration_values_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_enumeration_values_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_enumeration_values_inserts
  left join p_hybris_enumeration_values
    on #s_hybris_enumeration_values_inserts.bk_hash = p_hybris_enumeration_values.bk_hash
   and p_hybris_enumeration_values.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_enumeration_values
    on p_hybris_enumeration_values.bk_hash = s_hybris_enumeration_values.bk_hash
   and p_hybris_enumeration_values.s_hybris_enumeration_values_id = s_hybris_enumeration_values.s_hybris_enumeration_values_id
 where s_hybris_enumeration_values.s_hybris_enumeration_values_id is null
    or (s_hybris_enumeration_values.s_hybris_enumeration_values_id is not null
        and s_hybris_enumeration_values.dv_hash <> #s_hybris_enumeration_values_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_enumeration_values @current_dv_batch_id

end
