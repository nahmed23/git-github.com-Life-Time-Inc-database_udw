CREATE PROC [dbo].[proc_etl_hybris_promotion_action] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_promotionaction

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_promotionaction (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_markedapplied,
       p_guid,
       p_promotionresult,
       aCLTS,
       propTS,
       p_amount,
       p_orderentryproduct,
       p_orderentryquantity,
       p_orderentrynumber,
       p_freeproduct,
       p_deliverymode,
       p_rule,
       p_strategyid,
       p_amoun0,
       p_product,
       p_quantity,
       p_deliverycost,
       p_replaceddeliverymode,
       p_replaceddeliverycost,
       p_parameters,
       p_couponid,
       p_couponcode,
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
       p_markedapplied,
       p_guid,
       p_promotionresult,
       aCLTS,
       propTS,
       p_amount,
       p_orderentryproduct,
       p_orderentryquantity,
       p_orderentrynumber,
       p_freeproduct,
       p_deliverymode,
       p_rule,
       p_strategyid,
       p_amoun0,
       p_product,
       p_quantity,
       p_deliverycost,
       p_replaceddeliverymode,
       p_replaceddeliverycost,
       p_parameters,
       p_couponid,
       p_couponcode,
       isnull(cast(stage_hybris_promotionaction.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_promotionaction
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_promotion_action @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_promotion_action (
       bk_hash,
       promotion_action_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_promotionaction.bk_hash,
       stage_hash_hybris_promotionaction.[PK] promotion_action_pk,
       isnull(cast(stage_hash_hybris_promotionaction.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_promotionaction
  left join h_hybris_promotion_action
    on stage_hash_hybris_promotionaction.bk_hash = h_hybris_promotion_action.bk_hash
 where h_hybris_promotion_action_id is null
   and stage_hash_hybris_promotionaction.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_promotion_action
if object_id('tempdb..#l_hybris_promotion_action_inserts') is not null drop table #l_hybris_promotion_action_inserts
create table #l_hybris_promotion_action_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionaction.bk_hash,
       stage_hash_hybris_promotionaction.TypePkString type_pk_string,
       stage_hash_hybris_promotionaction.OwnerPkString owner_pk_string,
       stage_hash_hybris_promotionaction.[PK] promotion_action_pk,
       stage_hash_hybris_promotionaction.p_promotionresult p_promotion_result,
       stage_hash_hybris_promotionaction.p_orderentryproduct p_order_entry_product,
       stage_hash_hybris_promotionaction.p_deliverymode p_delivery_mode,
       stage_hash_hybris_promotionaction.p_rule p_rule,
       stage_hash_hybris_promotionaction.p_strategyid p_strategy_id,
       stage_hash_hybris_promotionaction.p_product p_product,
       stage_hash_hybris_promotionaction.p_couponid p_coupon_id,
       stage_hash_hybris_promotionaction.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_promotionresult as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_orderentryproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_deliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_rule as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionaction.p_strategyid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionaction.p_couponid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionaction
 where stage_hash_hybris_promotionaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_promotion_action records
set @insert_date_time = getdate()
insert into l_hybris_promotion_action (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       promotion_action_pk,
       p_promotion_result,
       p_order_entry_product,
       p_delivery_mode,
       p_rule,
       p_strategy_id,
       p_product,
       p_coupon_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_promotion_action_inserts.bk_hash,
       #l_hybris_promotion_action_inserts.type_pk_string,
       #l_hybris_promotion_action_inserts.owner_pk_string,
       #l_hybris_promotion_action_inserts.promotion_action_pk,
       #l_hybris_promotion_action_inserts.p_promotion_result,
       #l_hybris_promotion_action_inserts.p_order_entry_product,
       #l_hybris_promotion_action_inserts.p_delivery_mode,
       #l_hybris_promotion_action_inserts.p_rule,
       #l_hybris_promotion_action_inserts.p_strategy_id,
       #l_hybris_promotion_action_inserts.p_product,
       #l_hybris_promotion_action_inserts.p_coupon_id,
       case when l_hybris_promotion_action.l_hybris_promotion_action_id is null then isnull(#l_hybris_promotion_action_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_promotion_action_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_promotion_action_inserts
  left join p_hybris_promotion_action
    on #l_hybris_promotion_action_inserts.bk_hash = p_hybris_promotion_action.bk_hash
   and p_hybris_promotion_action.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_promotion_action
    on p_hybris_promotion_action.bk_hash = l_hybris_promotion_action.bk_hash
   and p_hybris_promotion_action.l_hybris_promotion_action_id = l_hybris_promotion_action.l_hybris_promotion_action_id
 where l_hybris_promotion_action.l_hybris_promotion_action_id is null
    or (l_hybris_promotion_action.l_hybris_promotion_action_id is not null
        and l_hybris_promotion_action.dv_hash <> #l_hybris_promotion_action_inserts.source_hash)

--calculate hash and lookup to current s_hybris_promotion_action
if object_id('tempdb..#s_hybris_promotion_action_inserts') is not null drop table #s_hybris_promotion_action_inserts
create table #s_hybris_promotion_action_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionaction.bk_hash,
       stage_hash_hybris_promotionaction.hjmpTS hjmpts,
       stage_hash_hybris_promotionaction.createdTS created_ts,
       stage_hash_hybris_promotionaction.modifiedTS modified_ts,
       stage_hash_hybris_promotionaction.[PK] promotion_action_pk,
       stage_hash_hybris_promotionaction.p_markedapplied p_marked_applied,
       stage_hash_hybris_promotionaction.p_guid p_guid,
       stage_hash_hybris_promotionaction.aCLTS acl_ts,
       stage_hash_hybris_promotionaction.propTS prop_ts,
       stage_hash_hybris_promotionaction.p_amount p_amount,
       stage_hash_hybris_promotionaction.p_orderentryquantity p_order_entry_quantity,
       stage_hash_hybris_promotionaction.p_orderentrynumber p_order_entry_number,
       stage_hash_hybris_promotionaction.p_freeproduct p_free_product,
       stage_hash_hybris_promotionaction.p_amoun0 p_amoun0,
       stage_hash_hybris_promotionaction.p_quantity p_quantity,
       stage_hash_hybris_promotionaction.p_deliverycost p_delivery_cost,
       stage_hash_hybris_promotionaction.p_replaceddeliverymode p_replaced_delivery_mode,
       stage_hash_hybris_promotionaction.p_replaceddeliverycost p_replaced_delivery_cost,
       stage_hash_hybris_promotionaction.p_parameters p_parameters,
       stage_hash_hybris_promotionaction.p_couponcode p_coupon_code,
       stage_hash_hybris_promotionaction.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionaction.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionaction.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_markedapplied as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionaction.p_guid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_orderentryquantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_orderentrynumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_freeproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_amoun0 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_quantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_deliverycost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_replaceddeliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionaction.p_replaceddeliverycost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionaction.p_parameters,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionaction.p_couponcode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionaction
 where stage_hash_hybris_promotionaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_promotion_action records
set @insert_date_time = getdate()
insert into s_hybris_promotion_action (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       promotion_action_pk,
       p_marked_applied,
       p_guid,
       acl_ts,
       prop_ts,
       p_amount,
       p_order_entry_quantity,
       p_order_entry_number,
       p_free_product,
       p_amoun0,
       p_quantity,
       p_delivery_cost,
       p_replaced_delivery_mode,
       p_replaced_delivery_cost,
       p_parameters,
       p_coupon_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_promotion_action_inserts.bk_hash,
       #s_hybris_promotion_action_inserts.hjmpts,
       #s_hybris_promotion_action_inserts.created_ts,
       #s_hybris_promotion_action_inserts.modified_ts,
       #s_hybris_promotion_action_inserts.promotion_action_pk,
       #s_hybris_promotion_action_inserts.p_marked_applied,
       #s_hybris_promotion_action_inserts.p_guid,
       #s_hybris_promotion_action_inserts.acl_ts,
       #s_hybris_promotion_action_inserts.prop_ts,
       #s_hybris_promotion_action_inserts.p_amount,
       #s_hybris_promotion_action_inserts.p_order_entry_quantity,
       #s_hybris_promotion_action_inserts.p_order_entry_number,
       #s_hybris_promotion_action_inserts.p_free_product,
       #s_hybris_promotion_action_inserts.p_amoun0,
       #s_hybris_promotion_action_inserts.p_quantity,
       #s_hybris_promotion_action_inserts.p_delivery_cost,
       #s_hybris_promotion_action_inserts.p_replaced_delivery_mode,
       #s_hybris_promotion_action_inserts.p_replaced_delivery_cost,
       #s_hybris_promotion_action_inserts.p_parameters,
       #s_hybris_promotion_action_inserts.p_coupon_code,
       case when s_hybris_promotion_action.s_hybris_promotion_action_id is null then isnull(#s_hybris_promotion_action_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_promotion_action_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_promotion_action_inserts
  left join p_hybris_promotion_action
    on #s_hybris_promotion_action_inserts.bk_hash = p_hybris_promotion_action.bk_hash
   and p_hybris_promotion_action.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_promotion_action
    on p_hybris_promotion_action.bk_hash = s_hybris_promotion_action.bk_hash
   and p_hybris_promotion_action.s_hybris_promotion_action_id = s_hybris_promotion_action.s_hybris_promotion_action_id
 where s_hybris_promotion_action.s_hybris_promotion_action_id is null
    or (s_hybris_promotion_action.s_hybris_promotion_action_id is not null
        and s_hybris_promotion_action.dv_hash <> #s_hybris_promotion_action_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_promotion_action @current_dv_batch_id

end
