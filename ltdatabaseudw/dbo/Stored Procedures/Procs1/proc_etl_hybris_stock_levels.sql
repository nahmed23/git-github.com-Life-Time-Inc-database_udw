CREATE PROC [dbo].[proc_etl_hybris_stock_levels] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_stocklevels

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_stocklevels (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_preorder,
       p_treatnegativeaszero,
       p_overselling,
       p_maxstocklevelhistorycount,
       p_instockstatus,
       p_available,
       p_productcode,
       p_reserved,
       p_warehouse,
       p_maxpreorder,
       p_releasedate,
       p_nextdeliverytime,
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
       p_preorder,
       p_treatnegativeaszero,
       p_overselling,
       p_maxstocklevelhistorycount,
       p_instockstatus,
       p_available,
       p_productcode,
       p_reserved,
       p_warehouse,
       p_maxpreorder,
       p_releasedate,
       p_nextdeliverytime,
       isnull(cast(stage_hybris_stocklevels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_stocklevels
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_stock_levels @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_stock_levels (
       bk_hash,
       stock_levels_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_stocklevels.bk_hash,
       stage_hash_hybris_stocklevels.[PK] stock_levels_pk,
       isnull(cast(stage_hash_hybris_stocklevels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_stocklevels
  left join h_hybris_stock_levels
    on stage_hash_hybris_stocklevels.bk_hash = h_hybris_stock_levels.bk_hash
 where h_hybris_stock_levels_id is null
   and stage_hash_hybris_stocklevels.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_stock_levels
if object_id('tempdb..#l_hybris_stock_levels_inserts') is not null drop table #l_hybris_stock_levels_inserts
create table #l_hybris_stock_levels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_stocklevels.bk_hash,
       stage_hash_hybris_stocklevels.TypePkString type_pk_string,
       stage_hash_hybris_stocklevels.[PK] stock_levels_pk,
       stage_hash_hybris_stocklevels.OwnerPkString owner_pk_string,
       stage_hash_hybris_stocklevels.p_instockstatus p_in_stock_status,
       stage_hash_hybris_stocklevels.p_productcode p_product_code,
       isnull(cast(stage_hash_hybris_stocklevels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_instockstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_stocklevels.p_productcode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_stocklevels
 where stage_hash_hybris_stocklevels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_stock_levels records
set @insert_date_time = getdate()
insert into l_hybris_stock_levels (
       bk_hash,
       type_pk_string,
       stock_levels_pk,
       owner_pk_string,
       p_in_stock_status,
       p_product_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_stock_levels_inserts.bk_hash,
       #l_hybris_stock_levels_inserts.type_pk_string,
       #l_hybris_stock_levels_inserts.stock_levels_pk,
       #l_hybris_stock_levels_inserts.owner_pk_string,
       #l_hybris_stock_levels_inserts.p_in_stock_status,
       #l_hybris_stock_levels_inserts.p_product_code,
       case when l_hybris_stock_levels.l_hybris_stock_levels_id is null then isnull(#l_hybris_stock_levels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_stock_levels_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_stock_levels_inserts
  left join p_hybris_stock_levels
    on #l_hybris_stock_levels_inserts.bk_hash = p_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_stock_levels
    on p_hybris_stock_levels.bk_hash = l_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.l_hybris_stock_levels_id = l_hybris_stock_levels.l_hybris_stock_levels_id
 where l_hybris_stock_levels.l_hybris_stock_levels_id is null
    or (l_hybris_stock_levels.l_hybris_stock_levels_id is not null
        and l_hybris_stock_levels.dv_hash <> #l_hybris_stock_levels_inserts.source_hash)

--calculate hash and lookup to current s_hybris_stock_levels
if object_id('tempdb..#s_hybris_stock_levels_inserts') is not null drop table #s_hybris_stock_levels_inserts
create table #s_hybris_stock_levels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_stocklevels.bk_hash,
       stage_hash_hybris_stocklevels.hjmpTS hjmpts,
       stage_hash_hybris_stocklevels.[PK] stock_levels_pk,
       stage_hash_hybris_stocklevels.createdTS created_ts,
       stage_hash_hybris_stocklevels.modifiedTS modified_ts,
       stage_hash_hybris_stocklevels.aCLTS acl_ts,
       stage_hash_hybris_stocklevels.propTS prop_ts,
       stage_hash_hybris_stocklevels.p_preorder p_preorder,
       stage_hash_hybris_stocklevels.p_treatnegativeaszero p_treat_negative_as_zero,
       stage_hash_hybris_stocklevels.p_overselling p_over_selling,
       stage_hash_hybris_stocklevels.p_maxstocklevelhistorycount p_max_stock_level_history_count,
       stage_hash_hybris_stocklevels.p_available p_available,
       stage_hash_hybris_stocklevels.p_reserved p_reserved,
       stage_hash_hybris_stocklevels.p_warehouse p_warehouse,
       stage_hash_hybris_stocklevels.p_maxpreorder p_max_pre_order,
       stage_hash_hybris_stocklevels.p_releasedate p_release_date,
       stage_hash_hybris_stocklevels.p_nextdeliverytime p_next_delivery_time,
       isnull(cast(stage_hash_hybris_stocklevels.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_stocklevels.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_stocklevels.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_preorder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_treatnegativeaszero as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_overselling as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_maxstocklevelhistorycount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_available as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_reserved as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_warehouse as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_stocklevels.p_maxpreorder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_stocklevels.p_releasedate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_stocklevels.p_nextdeliverytime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_stocklevels
 where stage_hash_hybris_stocklevels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_stock_levels records
set @insert_date_time = getdate()
insert into s_hybris_stock_levels (
       bk_hash,
       hjmpts,
       stock_levels_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_preorder,
       p_treat_negative_as_zero,
       p_over_selling,
       p_max_stock_level_history_count,
       p_available,
       p_reserved,
       p_warehouse,
       p_max_pre_order,
       p_release_date,
       p_next_delivery_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_stock_levels_inserts.bk_hash,
       #s_hybris_stock_levels_inserts.hjmpts,
       #s_hybris_stock_levels_inserts.stock_levels_pk,
       #s_hybris_stock_levels_inserts.created_ts,
       #s_hybris_stock_levels_inserts.modified_ts,
       #s_hybris_stock_levels_inserts.acl_ts,
       #s_hybris_stock_levels_inserts.prop_ts,
       #s_hybris_stock_levels_inserts.p_preorder,
       #s_hybris_stock_levels_inserts.p_treat_negative_as_zero,
       #s_hybris_stock_levels_inserts.p_over_selling,
       #s_hybris_stock_levels_inserts.p_max_stock_level_history_count,
       #s_hybris_stock_levels_inserts.p_available,
       #s_hybris_stock_levels_inserts.p_reserved,
       #s_hybris_stock_levels_inserts.p_warehouse,
       #s_hybris_stock_levels_inserts.p_max_pre_order,
       #s_hybris_stock_levels_inserts.p_release_date,
       #s_hybris_stock_levels_inserts.p_next_delivery_time,
       case when s_hybris_stock_levels.s_hybris_stock_levels_id is null then isnull(#s_hybris_stock_levels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_stock_levels_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_stock_levels_inserts
  left join p_hybris_stock_levels
    on #s_hybris_stock_levels_inserts.bk_hash = p_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_stock_levels
    on p_hybris_stock_levels.bk_hash = s_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.s_hybris_stock_levels_id = s_hybris_stock_levels.s_hybris_stock_levels_id
 where s_hybris_stock_levels.s_hybris_stock_levels_id is null
    or (s_hybris_stock_levels.s_hybris_stock_levels_id is not null
        and s_hybris_stock_levels.dv_hash <> #s_hybris_stock_levels_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_stock_levels @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_stock_levels @current_dv_batch_id

end
