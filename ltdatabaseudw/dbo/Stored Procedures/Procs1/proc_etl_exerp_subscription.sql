CREATE PROC [dbo].[proc_etl_exerp_subscription] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_subscription

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_subscription (
       bk_hash,
       person_id,
       id,
       center_id,
       state,
       sub_state,
       renewal_type,
       product_id,
       start_date,
       stop_datetime,
       end_date,
       billed_until_date,
       binding_end_date,
       creation_datetime,
       price,
       binding_price,
       requires_main,
       price_update_excluded,
       type_price_update_excluded,
       freeze_period_product_id,
       transfer_subscription_id,
       extension_subscription_id,
       period_unit,
       period_count,
       reassign_subscription_id,
       stop_person_id,
       stop_cancel_datetime,
       ets,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       person_id,
       id,
       center_id,
       state,
       sub_state,
       renewal_type,
       product_id,
       start_date,
       stop_datetime,
       end_date,
       billed_until_date,
       binding_end_date,
       creation_datetime,
       price,
       binding_price,
       requires_main,
       price_update_excluded,
       type_price_update_excluded,
       freeze_period_product_id,
       transfer_subscription_id,
       extension_subscription_id,
       period_unit,
       period_count,
       reassign_subscription_id,
       stop_person_id,
       stop_cancel_datetime,
       ets,
       isnull(cast(stage_exerp_subscription.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_subscription
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_subscription @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_subscription (
       bk_hash,
       subscription_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_subscription.bk_hash,
       stage_hash_exerp_subscription.id subscription_id,
       isnull(cast(stage_hash_exerp_subscription.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_subscription
  left join h_exerp_subscription
    on stage_hash_exerp_subscription.bk_hash = h_exerp_subscription.bk_hash
 where h_exerp_subscription_id is null
   and stage_hash_exerp_subscription.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_subscription
if object_id('tempdb..#l_exerp_subscription_inserts') is not null drop table #l_exerp_subscription_inserts
create table #l_exerp_subscription_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription.bk_hash,
       stage_hash_exerp_subscription.person_id person_id,
       stage_hash_exerp_subscription.id subscription_id,
       stage_hash_exerp_subscription.center_id center_id,
       stage_hash_exerp_subscription.product_id product_id,
       stage_hash_exerp_subscription.freeze_period_product_id freeze_period_product_id,
       stage_hash_exerp_subscription.transfer_subscription_id transfer_subscription_id,
       stage_hash_exerp_subscription.extension_subscription_id extension_subscription_id,
       stage_hash_exerp_subscription.reassign_subscription_id reassign_subscription_id,
       stage_hash_exerp_subscription.stop_person_id stop_person_id,
       isnull(cast(stage_hash_exerp_subscription.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_subscription.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.product_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.freeze_period_product_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.transfer_subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.extension_subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.reassign_subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.stop_person_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription
 where stage_hash_exerp_subscription.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_subscription records
set @insert_date_time = getdate()
insert into l_exerp_subscription (
       bk_hash,
       person_id,
       subscription_id,
       center_id,
       product_id,
       freeze_period_product_id,
       transfer_subscription_id,
       extension_subscription_id,
       reassign_subscription_id,
       stop_person_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_subscription_inserts.bk_hash,
       #l_exerp_subscription_inserts.person_id,
       #l_exerp_subscription_inserts.subscription_id,
       #l_exerp_subscription_inserts.center_id,
       #l_exerp_subscription_inserts.product_id,
       #l_exerp_subscription_inserts.freeze_period_product_id,
       #l_exerp_subscription_inserts.transfer_subscription_id,
       #l_exerp_subscription_inserts.extension_subscription_id,
       #l_exerp_subscription_inserts.reassign_subscription_id,
       #l_exerp_subscription_inserts.stop_person_id,
       case when l_exerp_subscription.l_exerp_subscription_id is null then isnull(#l_exerp_subscription_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_subscription_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_subscription_inserts
  left join p_exerp_subscription
    on #l_exerp_subscription_inserts.bk_hash = p_exerp_subscription.bk_hash
   and p_exerp_subscription.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_subscription
    on p_exerp_subscription.bk_hash = l_exerp_subscription.bk_hash
   and p_exerp_subscription.l_exerp_subscription_id = l_exerp_subscription.l_exerp_subscription_id
 where l_exerp_subscription.l_exerp_subscription_id is null
    or (l_exerp_subscription.l_exerp_subscription_id is not null
        and l_exerp_subscription.dv_hash <> #l_exerp_subscription_inserts.source_hash)

--calculate hash and lookup to current s_exerp_subscription
if object_id('tempdb..#s_exerp_subscription_inserts') is not null drop table #s_exerp_subscription_inserts
create table #s_exerp_subscription_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription.bk_hash,
       stage_hash_exerp_subscription.id subscription_id,
       stage_hash_exerp_subscription.state state,
       stage_hash_exerp_subscription.sub_state sub_state,
       stage_hash_exerp_subscription.renewal_type renewal_type,
       stage_hash_exerp_subscription.start_date start_date,
       stage_hash_exerp_subscription.stop_datetime stop_datetime,
       stage_hash_exerp_subscription.end_date end_date,
       stage_hash_exerp_subscription.billed_until_date billed_until_date,
       stage_hash_exerp_subscription.binding_end_date binding_end_date,
       stage_hash_exerp_subscription.creation_datetime creation_datetime,
       stage_hash_exerp_subscription.price price,
       stage_hash_exerp_subscription.binding_price binding_price,
       stage_hash_exerp_subscription.requires_main requires_main,
       stage_hash_exerp_subscription.price_update_excluded price_update_excluded,
       stage_hash_exerp_subscription.type_price_update_excluded type_price_update_excluded,
       stage_hash_exerp_subscription.period_unit period_unit,
       stage_hash_exerp_subscription.period_count period_count,
       stage_hash_exerp_subscription.stop_cancel_datetime stop_cancel_datetime,
       stage_hash_exerp_subscription.ets ets,
       isnull(cast(stage_hash_exerp_subscription.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_subscription.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.sub_state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.renewal_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.stop_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.billed_until_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.binding_end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.creation_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.binding_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.requires_main as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.price_update_excluded as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.type_price_update_excluded as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription.period_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.period_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription.stop_cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription
 where stage_hash_exerp_subscription.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_subscription records
set @insert_date_time = getdate()
insert into s_exerp_subscription (
       bk_hash,
       subscription_id,
       state,
       sub_state,
       renewal_type,
       start_date,
       stop_datetime,
       end_date,
       billed_until_date,
       binding_end_date,
       creation_datetime,
       price,
       binding_price,
       requires_main,
       price_update_excluded,
       type_price_update_excluded,
       period_unit,
       period_count,
       stop_cancel_datetime,
       ets,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_subscription_inserts.bk_hash,
       #s_exerp_subscription_inserts.subscription_id,
       #s_exerp_subscription_inserts.state,
       #s_exerp_subscription_inserts.sub_state,
       #s_exerp_subscription_inserts.renewal_type,
       #s_exerp_subscription_inserts.start_date,
       #s_exerp_subscription_inserts.stop_datetime,
       #s_exerp_subscription_inserts.end_date,
       #s_exerp_subscription_inserts.billed_until_date,
       #s_exerp_subscription_inserts.binding_end_date,
       #s_exerp_subscription_inserts.creation_datetime,
       #s_exerp_subscription_inserts.price,
       #s_exerp_subscription_inserts.binding_price,
       #s_exerp_subscription_inserts.requires_main,
       #s_exerp_subscription_inserts.price_update_excluded,
       #s_exerp_subscription_inserts.type_price_update_excluded,
       #s_exerp_subscription_inserts.period_unit,
       #s_exerp_subscription_inserts.period_count,
       #s_exerp_subscription_inserts.stop_cancel_datetime,
       #s_exerp_subscription_inserts.ets,
       case when s_exerp_subscription.s_exerp_subscription_id is null then isnull(#s_exerp_subscription_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_subscription_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_subscription_inserts
  left join p_exerp_subscription
    on #s_exerp_subscription_inserts.bk_hash = p_exerp_subscription.bk_hash
   and p_exerp_subscription.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_subscription
    on p_exerp_subscription.bk_hash = s_exerp_subscription.bk_hash
   and p_exerp_subscription.s_exerp_subscription_id = s_exerp_subscription.s_exerp_subscription_id
 where s_exerp_subscription.s_exerp_subscription_id is null
    or (s_exerp_subscription.s_exerp_subscription_id is not null
        and s_exerp_subscription.dv_hash <> #s_exerp_subscription_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_subscription @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_subscription @current_dv_batch_id

end
