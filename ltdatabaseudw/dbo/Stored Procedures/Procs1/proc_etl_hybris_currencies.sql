CREATE PROC [dbo].[proc_etl_hybris_currencies] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_currencies

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_currencies (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_active,
       p_isocode,
       p_base,
       p_conversion,
       p_digits,
       p_symbol,
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
       p_active,
       p_isocode,
       p_base,
       p_conversion,
       p_digits,
       p_symbol,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_currencies.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_currencies
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_currencies @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_currencies (
       bk_hash,
       currencies_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_currencies.bk_hash,
       stage_hash_hybris_currencies.[PK] currencies_pk,
       isnull(cast(stage_hash_hybris_currencies.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_currencies
  left join h_hybris_currencies
    on stage_hash_hybris_currencies.bk_hash = h_hybris_currencies.bk_hash
 where h_hybris_currencies_id is null
   and stage_hash_hybris_currencies.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_currencies
if object_id('tempdb..#l_hybris_currencies_inserts') is not null drop table #l_hybris_currencies_inserts
create table #l_hybris_currencies_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_currencies.bk_hash,
       stage_hash_hybris_currencies.TypePkString type_pk_string,
       stage_hash_hybris_currencies.OwnerPkString owner_pk_string,
       stage_hash_hybris_currencies.[PK] currencies_pk,
       stage_hash_hybris_currencies.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.[PK] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_currencies
 where stage_hash_hybris_currencies.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_currencies records
set @insert_date_time = getdate()
insert into l_hybris_currencies (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       currencies_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_currencies_inserts.bk_hash,
       #l_hybris_currencies_inserts.type_pk_string,
       #l_hybris_currencies_inserts.owner_pk_string,
       #l_hybris_currencies_inserts.currencies_pk,
       case when l_hybris_currencies.l_hybris_currencies_id is null then isnull(#l_hybris_currencies_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_currencies_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_currencies_inserts
  left join p_hybris_currencies
    on #l_hybris_currencies_inserts.bk_hash = p_hybris_currencies.bk_hash
   and p_hybris_currencies.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_currencies
    on p_hybris_currencies.bk_hash = l_hybris_currencies.bk_hash
   and p_hybris_currencies.l_hybris_currencies_id = l_hybris_currencies.l_hybris_currencies_id
 where l_hybris_currencies.l_hybris_currencies_id is null
    or (l_hybris_currencies.l_hybris_currencies_id is not null
        and l_hybris_currencies.dv_hash <> #l_hybris_currencies_inserts.source_hash)

--calculate hash and lookup to current s_hybris_currencies
if object_id('tempdb..#s_hybris_currencies_inserts') is not null drop table #s_hybris_currencies_inserts
create table #s_hybris_currencies_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_currencies.bk_hash,
       stage_hash_hybris_currencies.hjmpTS hjmpts,
       stage_hash_hybris_currencies.createdTS created_ts,
       stage_hash_hybris_currencies.modifiedTS modified_ts,
       stage_hash_hybris_currencies.[PK] currencies_pk,
       stage_hash_hybris_currencies.p_active p_active,
       stage_hash_hybris_currencies.p_isocode p_iso_code,
       stage_hash_hybris_currencies.p_base p_base,
       stage_hash_hybris_currencies.p_conversion p_conversion,
       stage_hash_hybris_currencies.p_digits p_digits,
       stage_hash_hybris_currencies.p_symbol p_symbol,
       stage_hash_hybris_currencies.aCLTS acl_ts,
       stage_hash_hybris_currencies.propTS prop_ts,
       stage_hash_hybris_currencies.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_currencies.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_currencies.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.p_active as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_currencies.p_isocode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.p_base as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.p_conversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.p_digits as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_currencies.p_symbol,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_currencies.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_currencies
 where stage_hash_hybris_currencies.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_currencies records
set @insert_date_time = getdate()
insert into s_hybris_currencies (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       currencies_pk,
       p_active,
       p_iso_code,
       p_base,
       p_conversion,
       p_digits,
       p_symbol,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_currencies_inserts.bk_hash,
       #s_hybris_currencies_inserts.hjmpts,
       #s_hybris_currencies_inserts.created_ts,
       #s_hybris_currencies_inserts.modified_ts,
       #s_hybris_currencies_inserts.currencies_pk,
       #s_hybris_currencies_inserts.p_active,
       #s_hybris_currencies_inserts.p_iso_code,
       #s_hybris_currencies_inserts.p_base,
       #s_hybris_currencies_inserts.p_conversion,
       #s_hybris_currencies_inserts.p_digits,
       #s_hybris_currencies_inserts.p_symbol,
       #s_hybris_currencies_inserts.acl_ts,
       #s_hybris_currencies_inserts.prop_ts,
       case when s_hybris_currencies.s_hybris_currencies_id is null then isnull(#s_hybris_currencies_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_currencies_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_currencies_inserts
  left join p_hybris_currencies
    on #s_hybris_currencies_inserts.bk_hash = p_hybris_currencies.bk_hash
   and p_hybris_currencies.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_currencies
    on p_hybris_currencies.bk_hash = s_hybris_currencies.bk_hash
   and p_hybris_currencies.s_hybris_currencies_id = s_hybris_currencies.s_hybris_currencies_id
 where s_hybris_currencies.s_hybris_currencies_id is null
    or (s_hybris_currencies.s_hybris_currencies_id is not null
        and s_hybris_currencies.dv_hash <> #s_hybris_currencies_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_currencies @current_dv_batch_id

end
