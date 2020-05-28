CREATE PROC [dbo].[proc_etl_hybris_units] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_units

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_units (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_code,
       p_conversion,
       p_unittype,
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
       p_code,
       p_conversion,
       p_unittype,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_units.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_units
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_units @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_units (
       bk_hash,
       units_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_units.bk_hash,
       stage_hash_hybris_units.[PK] units_pk,
       isnull(cast(stage_hash_hybris_units.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_units
  left join h_hybris_units
    on stage_hash_hybris_units.bk_hash = h_hybris_units.bk_hash
 where h_hybris_units_id is null
   and stage_hash_hybris_units.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_units
if object_id('tempdb..#l_hybris_units_inserts') is not null drop table #l_hybris_units_inserts
create table #l_hybris_units_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_units.bk_hash,
       stage_hash_hybris_units.TypePkString type_pk_string,
       stage_hash_hybris_units.OwnerPkString owner_pk_string,
       stage_hash_hybris_units.[PK] units_pk,
       stage_hash_hybris_units.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_units.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.[PK] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_units
 where stage_hash_hybris_units.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_units records
set @insert_date_time = getdate()
insert into l_hybris_units (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       units_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_units_inserts.bk_hash,
       #l_hybris_units_inserts.type_pk_string,
       #l_hybris_units_inserts.owner_pk_string,
       #l_hybris_units_inserts.units_pk,
       case when l_hybris_units.l_hybris_units_id is null then isnull(#l_hybris_units_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_units_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_units_inserts
  left join p_hybris_units
    on #l_hybris_units_inserts.bk_hash = p_hybris_units.bk_hash
   and p_hybris_units.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_units
    on p_hybris_units.bk_hash = l_hybris_units.bk_hash
   and p_hybris_units.l_hybris_units_id = l_hybris_units.l_hybris_units_id
 where l_hybris_units.l_hybris_units_id is null
    or (l_hybris_units.l_hybris_units_id is not null
        and l_hybris_units.dv_hash <> #l_hybris_units_inserts.source_hash)

--calculate hash and lookup to current s_hybris_units
if object_id('tempdb..#s_hybris_units_inserts') is not null drop table #s_hybris_units_inserts
create table #s_hybris_units_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_units.bk_hash,
       stage_hash_hybris_units.hjmpTS hjmpts,
       stage_hash_hybris_units.createdTS created_ts,
       stage_hash_hybris_units.modifiedTS modified_ts,
       stage_hash_hybris_units.[PK] units_pk,
       stage_hash_hybris_units.p_code p_code,
       stage_hash_hybris_units.p_conversion p_conversion,
       stage_hash_hybris_units.p_unittype p_unit_type,
       stage_hash_hybris_units.aCLTS acl_ts,
       stage_hash_hybris_units.propTS prop_ts,
       stage_hash_hybris_units.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_units.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_units.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_units.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_units.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.p_conversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_units.p_unittype,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_units.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_units
 where stage_hash_hybris_units.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_units records
set @insert_date_time = getdate()
insert into s_hybris_units (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       units_pk,
       p_code,
       p_conversion,
       p_unit_type,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_units_inserts.bk_hash,
       #s_hybris_units_inserts.hjmpts,
       #s_hybris_units_inserts.created_ts,
       #s_hybris_units_inserts.modified_ts,
       #s_hybris_units_inserts.units_pk,
       #s_hybris_units_inserts.p_code,
       #s_hybris_units_inserts.p_conversion,
       #s_hybris_units_inserts.p_unit_type,
       #s_hybris_units_inserts.acl_ts,
       #s_hybris_units_inserts.prop_ts,
       case when s_hybris_units.s_hybris_units_id is null then isnull(#s_hybris_units_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_units_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_units_inserts
  left join p_hybris_units
    on #s_hybris_units_inserts.bk_hash = p_hybris_units.bk_hash
   and p_hybris_units.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_units
    on p_hybris_units.bk_hash = s_hybris_units.bk_hash
   and p_hybris_units.s_hybris_units_id = s_hybris_units.s_hybris_units_id
 where s_hybris_units.s_hybris_units_id is null
    or (s_hybris_units.s_hybris_units_id is not null
        and s_hybris_units.dv_hash <> #s_hybris_units_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_units @current_dv_batch_id

end
