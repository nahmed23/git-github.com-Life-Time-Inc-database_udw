﻿CREATE PROC [dbo].[proc_etl_hybris_consignment_entries] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_consignmententries

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_consignmententries (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_consignment,
       p_quantity,
       p_orderentry,
       p_shippedquantity,
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
       p_consignment,
       p_quantity,
       p_orderentry,
       p_shippedquantity,
       isnull(cast(stage_hybris_consignmententries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_consignmententries
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_consignment_entries @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_consignment_entries (
       bk_hash,
       pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_consignmententries.bk_hash,
       stage_hash_hybris_consignmententries.[PK] pk,
       isnull(cast(stage_hash_hybris_consignmententries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_consignmententries
  left join h_hybris_consignment_entries
    on stage_hash_hybris_consignmententries.bk_hash = h_hybris_consignment_entries.bk_hash
 where h_hybris_consignment_entries_id is null
   and stage_hash_hybris_consignmententries.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_consignment_entries
if object_id('tempdb..#l_hybris_consignment_entries_inserts') is not null drop table #l_hybris_consignment_entries_inserts
create table #l_hybris_consignment_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_consignmententries.bk_hash,
       stage_hash_hybris_consignmententries.TypePkString type_pk_string,
       stage_hash_hybris_consignmententries.[PK] pk,
       stage_hash_hybris_consignmententries.OwnerPkString owner_pk_string,
       stage_hash_hybris_consignmententries.p_consignment p_consignment,
       stage_hash_hybris_consignmententries.p_orderentry p_order_entry,
       isnull(cast(stage_hash_hybris_consignmententries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.p_consignment as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.p_orderentry as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_consignmententries
 where stage_hash_hybris_consignmententries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_consignment_entries records
set @insert_date_time = getdate()
insert into l_hybris_consignment_entries (
       bk_hash,
       type_pk_string,
       pk,
       owner_pk_string,
       p_consignment,
       p_order_entry,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_consignment_entries_inserts.bk_hash,
       #l_hybris_consignment_entries_inserts.type_pk_string,
       #l_hybris_consignment_entries_inserts.pk,
       #l_hybris_consignment_entries_inserts.owner_pk_string,
       #l_hybris_consignment_entries_inserts.p_consignment,
       #l_hybris_consignment_entries_inserts.p_order_entry,
       case when l_hybris_consignment_entries.l_hybris_consignment_entries_id is null then isnull(#l_hybris_consignment_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_consignment_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_consignment_entries_inserts
  left join p_hybris_consignment_entries
    on #l_hybris_consignment_entries_inserts.bk_hash = p_hybris_consignment_entries.bk_hash
   and p_hybris_consignment_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_consignment_entries
    on p_hybris_consignment_entries.bk_hash = l_hybris_consignment_entries.bk_hash
   and p_hybris_consignment_entries.l_hybris_consignment_entries_id = l_hybris_consignment_entries.l_hybris_consignment_entries_id
 where l_hybris_consignment_entries.l_hybris_consignment_entries_id is null
    or (l_hybris_consignment_entries.l_hybris_consignment_entries_id is not null
        and l_hybris_consignment_entries.dv_hash <> #l_hybris_consignment_entries_inserts.source_hash)

--calculate hash and lookup to current s_hybris_consignment_entries
if object_id('tempdb..#s_hybris_consignment_entries_inserts') is not null drop table #s_hybris_consignment_entries_inserts
create table #s_hybris_consignment_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_consignmententries.bk_hash,
       stage_hash_hybris_consignmententries.hjmpTS hjmpts,
       stage_hash_hybris_consignmententries.[PK] pk,
       stage_hash_hybris_consignmententries.createdTS created_ts,
       stage_hash_hybris_consignmententries.modifiedTS modified_ts,
       stage_hash_hybris_consignmententries.aCLTS acl_ts,
       stage_hash_hybris_consignmententries.propTS prop_ts,
       stage_hash_hybris_consignmententries.p_quantity p_quantity,
       stage_hash_hybris_consignmententries.p_shippedquantity p_shipped_quantity,
       isnull(cast(stage_hash_hybris_consignmententries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignmententries.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignmententries.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.p_quantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignmententries.p_shippedquantity as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_consignmententries
 where stage_hash_hybris_consignmententries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_consignment_entries records
set @insert_date_time = getdate()
insert into s_hybris_consignment_entries (
       bk_hash,
       hjmpts,
       pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_quantity,
       p_shipped_quantity,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_consignment_entries_inserts.bk_hash,
       #s_hybris_consignment_entries_inserts.hjmpts,
       #s_hybris_consignment_entries_inserts.pk,
       #s_hybris_consignment_entries_inserts.created_ts,
       #s_hybris_consignment_entries_inserts.modified_ts,
       #s_hybris_consignment_entries_inserts.acl_ts,
       #s_hybris_consignment_entries_inserts.prop_ts,
       #s_hybris_consignment_entries_inserts.p_quantity,
       #s_hybris_consignment_entries_inserts.p_shipped_quantity,
       case when s_hybris_consignment_entries.s_hybris_consignment_entries_id is null then isnull(#s_hybris_consignment_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_consignment_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_consignment_entries_inserts
  left join p_hybris_consignment_entries
    on #s_hybris_consignment_entries_inserts.bk_hash = p_hybris_consignment_entries.bk_hash
   and p_hybris_consignment_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_consignment_entries
    on p_hybris_consignment_entries.bk_hash = s_hybris_consignment_entries.bk_hash
   and p_hybris_consignment_entries.s_hybris_consignment_entries_id = s_hybris_consignment_entries.s_hybris_consignment_entries_id
 where s_hybris_consignment_entries.s_hybris_consignment_entries_id is null
    or (s_hybris_consignment_entries.s_hybris_consignment_entries_id is not null
        and s_hybris_consignment_entries.dv_hash <> #s_hybris_consignment_entries_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_consignment_entries @current_dv_batch_id

end
