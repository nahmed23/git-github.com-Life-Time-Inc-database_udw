CREATE PROC [dbo].[proc_etl_hybris_promotion_result] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_promotionresult

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_promotionresult (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_promotion,
       p_certainty,
       p_custom,
       p_order,
       p_moduleversion,
       p_ruleversion,
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
       p_promotion,
       p_certainty,
       p_custom,
       p_order,
       p_moduleversion,
       p_ruleversion,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_promotionresult.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_promotionresult
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_promotion_result @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_promotion_result (
       bk_hash,
       promotion_result_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_promotionresult.bk_hash,
       stage_hash_hybris_promotionresult.[PK] promotion_result_pk,
       isnull(cast(stage_hash_hybris_promotionresult.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_promotionresult
  left join h_hybris_promotion_result
    on stage_hash_hybris_promotionresult.bk_hash = h_hybris_promotion_result.bk_hash
 where h_hybris_promotion_result_id is null
   and stage_hash_hybris_promotionresult.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_promotion_result
if object_id('tempdb..#l_hybris_promotion_result_inserts') is not null drop table #l_hybris_promotion_result_inserts
create table #l_hybris_promotion_result_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionresult.bk_hash,
       stage_hash_hybris_promotionresult.TypePkString type_pk_string,
       stage_hash_hybris_promotionresult.OwnerPkString owner_pk_string,
       stage_hash_hybris_promotionresult.[PK] promotion_result_pk,
       stage_hash_hybris_promotionresult.p_promotion p_promotion,
       stage_hash_hybris_promotionresult.p_order p_order,
       stage_hash_hybris_promotionresult.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.p_promotion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.p_order as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionresult
 where stage_hash_hybris_promotionresult.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_promotion_result records
set @insert_date_time = getdate()
insert into l_hybris_promotion_result (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       promotion_result_pk,
       p_promotion,
       p_order,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_promotion_result_inserts.bk_hash,
       #l_hybris_promotion_result_inserts.type_pk_string,
       #l_hybris_promotion_result_inserts.owner_pk_string,
       #l_hybris_promotion_result_inserts.promotion_result_pk,
       #l_hybris_promotion_result_inserts.p_promotion,
       #l_hybris_promotion_result_inserts.p_order,
       case when l_hybris_promotion_result.l_hybris_promotion_result_id is null then isnull(#l_hybris_promotion_result_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_promotion_result_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_promotion_result_inserts
  left join p_hybris_promotion_result
    on #l_hybris_promotion_result_inserts.bk_hash = p_hybris_promotion_result.bk_hash
   and p_hybris_promotion_result.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_promotion_result
    on p_hybris_promotion_result.bk_hash = l_hybris_promotion_result.bk_hash
   and p_hybris_promotion_result.l_hybris_promotion_result_id = l_hybris_promotion_result.l_hybris_promotion_result_id
 where l_hybris_promotion_result.l_hybris_promotion_result_id is null
    or (l_hybris_promotion_result.l_hybris_promotion_result_id is not null
        and l_hybris_promotion_result.dv_hash <> #l_hybris_promotion_result_inserts.source_hash)

--calculate hash and lookup to current s_hybris_promotion_result
if object_id('tempdb..#s_hybris_promotion_result_inserts') is not null drop table #s_hybris_promotion_result_inserts
create table #s_hybris_promotion_result_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionresult.bk_hash,
       stage_hash_hybris_promotionresult.hjmpTS hjmpts,
       stage_hash_hybris_promotionresult.createdTS created_ts,
       stage_hash_hybris_promotionresult.modifiedTS modified_ts,
       stage_hash_hybris_promotionresult.[PK] promotion_result_pk,
       stage_hash_hybris_promotionresult.p_certainty p_certainty,
       stage_hash_hybris_promotionresult.p_custom p_custom,
       stage_hash_hybris_promotionresult.p_moduleversion p_module_version,
       stage_hash_hybris_promotionresult.p_ruleversion p_rule_version,
       stage_hash_hybris_promotionresult.aCLTS acl_ts,
       stage_hash_hybris_promotionresult.propTS prop_ts,
       stage_hash_hybris_promotionresult.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionresult.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionresult.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.p_certainty as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionresult.p_custom,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.p_moduleversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.p_ruleversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionresult.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionresult
 where stage_hash_hybris_promotionresult.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_promotion_result records
set @insert_date_time = getdate()
insert into s_hybris_promotion_result (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       promotion_result_pk,
       p_certainty,
       p_custom,
       p_module_version,
       p_rule_version,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_promotion_result_inserts.bk_hash,
       #s_hybris_promotion_result_inserts.hjmpts,
       #s_hybris_promotion_result_inserts.created_ts,
       #s_hybris_promotion_result_inserts.modified_ts,
       #s_hybris_promotion_result_inserts.promotion_result_pk,
       #s_hybris_promotion_result_inserts.p_certainty,
       #s_hybris_promotion_result_inserts.p_custom,
       #s_hybris_promotion_result_inserts.p_module_version,
       #s_hybris_promotion_result_inserts.p_rule_version,
       #s_hybris_promotion_result_inserts.acl_ts,
       #s_hybris_promotion_result_inserts.prop_ts,
       case when s_hybris_promotion_result.s_hybris_promotion_result_id is null then isnull(#s_hybris_promotion_result_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_promotion_result_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_promotion_result_inserts
  left join p_hybris_promotion_result
    on #s_hybris_promotion_result_inserts.bk_hash = p_hybris_promotion_result.bk_hash
   and p_hybris_promotion_result.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_promotion_result
    on p_hybris_promotion_result.bk_hash = s_hybris_promotion_result.bk_hash
   and p_hybris_promotion_result.s_hybris_promotion_result_id = s_hybris_promotion_result.s_hybris_promotion_result_id
 where s_hybris_promotion_result.s_hybris_promotion_result_id is null
    or (s_hybris_promotion_result.s_hybris_promotion_result_id is not null
        and s_hybris_promotion_result.dv_hash <> #s_hybris_promotion_result_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_promotion_result @current_dv_batch_id

end
