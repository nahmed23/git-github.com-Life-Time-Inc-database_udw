CREATE PROC [dbo].[proc_etl_exerp_subscription_sale] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_subscription_sale

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_subscription_sale (
       bk_hash,
       id,
       subscription_id,
       subscription_center,
       product_id,
       type,
       sale_datetime,
       start_date,
       end_date,
       sale_person_id,
       jf_normal_price,
       jf_discount,
       jf_price,
       jf_sponsored,
       jf_member,
       prorata_period_normal_price,
       prorata_period_discount,
       prorata_period_price,
       prorata_period_sponsored,
       prorata_period_member,
       init_period_normal_price,
       init_period_discount,
       init_period_price,
       init_period_sponsored,
       init_period_member,
       admin_fee_normal_price,
       admin_fee_discount,
       admin_fee_price,
       admin_fee_sponsored,
       admin_fee_member,
       binding_days,
       sale_id,
       init_contract_value,
       state,
       jf_sale_log_id,
       previous_subscription_id,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       subscription_id,
       subscription_center,
       product_id,
       type,
       sale_datetime,
       start_date,
       end_date,
       sale_person_id,
       jf_normal_price,
       jf_discount,
       jf_price,
       jf_sponsored,
       jf_member,
       prorata_period_normal_price,
       prorata_period_discount,
       prorata_period_price,
       prorata_period_sponsored,
       prorata_period_member,
       init_period_normal_price,
       init_period_discount,
       init_period_price,
       init_period_sponsored,
       init_period_member,
       admin_fee_normal_price,
       admin_fee_discount,
       admin_fee_price,
       admin_fee_sponsored,
       admin_fee_member,
       binding_days,
       sale_id,
       init_contract_value,
       state,
       jf_sale_log_id,
       previous_subscription_id,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_subscription_sale.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_subscription_sale
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_subscription_sale @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_subscription_sale (
       bk_hash,
       subscription_sale_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_subscription_sale.bk_hash,
       stage_hash_exerp_subscription_sale.id subscription_sale_id,
       isnull(cast(stage_hash_exerp_subscription_sale.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_subscription_sale
  left join h_exerp_subscription_sale
    on stage_hash_exerp_subscription_sale.bk_hash = h_exerp_subscription_sale.bk_hash
 where h_exerp_subscription_sale_id is null
   and stage_hash_exerp_subscription_sale.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_subscription_sale
if object_id('tempdb..#l_exerp_subscription_sale_inserts') is not null drop table #l_exerp_subscription_sale_inserts
create table #l_exerp_subscription_sale_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription_sale.bk_hash,
       stage_hash_exerp_subscription_sale.id subscription_sale_id,
       stage_hash_exerp_subscription_sale.subscription_id subscription_id,
       stage_hash_exerp_subscription_sale.subscription_center subscription_center,
       stage_hash_exerp_subscription_sale.product_id product_id,
       stage_hash_exerp_subscription_sale.sale_person_id sale_person_id,
       stage_hash_exerp_subscription_sale.sale_id sale_id,
       stage_hash_exerp_subscription_sale.jf_sale_log_id jf_sale_log_id,
       stage_hash_exerp_subscription_sale.previous_subscription_id previous_subscription_id,
       stage_hash_exerp_subscription_sale.center_id center_id,
       isnull(cast(stage_hash_exerp_subscription_sale.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.subscription_center as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.product_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.sale_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.sale_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.jf_sale_log_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.previous_subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription_sale
 where stage_hash_exerp_subscription_sale.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_subscription_sale records
set @insert_date_time = getdate()
insert into l_exerp_subscription_sale (
       bk_hash,
       subscription_sale_id,
       subscription_id,
       subscription_center,
       product_id,
       sale_person_id,
       sale_id,
       jf_sale_log_id,
       previous_subscription_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_subscription_sale_inserts.bk_hash,
       #l_exerp_subscription_sale_inserts.subscription_sale_id,
       #l_exerp_subscription_sale_inserts.subscription_id,
       #l_exerp_subscription_sale_inserts.subscription_center,
       #l_exerp_subscription_sale_inserts.product_id,
       #l_exerp_subscription_sale_inserts.sale_person_id,
       #l_exerp_subscription_sale_inserts.sale_id,
       #l_exerp_subscription_sale_inserts.jf_sale_log_id,
       #l_exerp_subscription_sale_inserts.previous_subscription_id,
       #l_exerp_subscription_sale_inserts.center_id,
       case when l_exerp_subscription_sale.l_exerp_subscription_sale_id is null then isnull(#l_exerp_subscription_sale_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_subscription_sale_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_subscription_sale_inserts
  left join p_exerp_subscription_sale
    on #l_exerp_subscription_sale_inserts.bk_hash = p_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_subscription_sale
    on p_exerp_subscription_sale.bk_hash = l_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.l_exerp_subscription_sale_id = l_exerp_subscription_sale.l_exerp_subscription_sale_id
 where l_exerp_subscription_sale.l_exerp_subscription_sale_id is null
    or (l_exerp_subscription_sale.l_exerp_subscription_sale_id is not null
        and l_exerp_subscription_sale.dv_hash <> #l_exerp_subscription_sale_inserts.source_hash)

--calculate hash and lookup to current s_exerp_subscription_sale
if object_id('tempdb..#s_exerp_subscription_sale_inserts') is not null drop table #s_exerp_subscription_sale_inserts
create table #s_exerp_subscription_sale_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription_sale.bk_hash,
       stage_hash_exerp_subscription_sale.id subscription_sale_id,
       stage_hash_exerp_subscription_sale.type type,
       stage_hash_exerp_subscription_sale.sale_datetime sale_datetime,
       stage_hash_exerp_subscription_sale.start_date start_date,
       stage_hash_exerp_subscription_sale.end_date end_date,
       stage_hash_exerp_subscription_sale.jf_normal_price jf_normal_price,
       stage_hash_exerp_subscription_sale.jf_discount jf_discount,
       stage_hash_exerp_subscription_sale.jf_price jf_price,
       stage_hash_exerp_subscription_sale.jf_sponsored jf_sponsored,
       stage_hash_exerp_subscription_sale.jf_member jf_member,
       stage_hash_exerp_subscription_sale.prorata_period_normal_price prorata_period_normal_price,
       stage_hash_exerp_subscription_sale.prorata_period_discount prorata_period_discount,
       stage_hash_exerp_subscription_sale.prorata_period_price prorata_period_price,
       stage_hash_exerp_subscription_sale.prorata_period_sponsored prorata_period_sponsored,
       stage_hash_exerp_subscription_sale.prorata_period_member prorata_period_member,
       stage_hash_exerp_subscription_sale.init_period_normal_price init_period_normal_price,
       stage_hash_exerp_subscription_sale.init_period_discount init_period_discount,
       stage_hash_exerp_subscription_sale.init_period_price init_period_price,
       stage_hash_exerp_subscription_sale.init_period_sponsored init_period_sponsored,
       stage_hash_exerp_subscription_sale.init_period_member init_period_member,
       stage_hash_exerp_subscription_sale.admin_fee_normal_price admin_fee_normal_price,
       stage_hash_exerp_subscription_sale.admin_fee_discount admin_fee_discount,
       stage_hash_exerp_subscription_sale.admin_fee_price admin_fee_price,
       stage_hash_exerp_subscription_sale.admin_fee_sponsored admin_fee_sponsored,
       stage_hash_exerp_subscription_sale.admin_fee_member admin_fee_member,
       stage_hash_exerp_subscription_sale.binding_days binding_days,
       stage_hash_exerp_subscription_sale.init_contract_value init_contract_value,
       stage_hash_exerp_subscription_sale.state state,
       stage_hash_exerp_subscription_sale.ets ets,
       stage_hash_exerp_subscription_sale.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_subscription_sale.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_sale.sale_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_sale.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_sale.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.jf_normal_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.jf_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.jf_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.jf_sponsored as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.jf_member as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.prorata_period_normal_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.prorata_period_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.prorata_period_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.prorata_period_sponsored as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.prorata_period_member as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_period_normal_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_period_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_period_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_period_sponsored as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_period_member as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.admin_fee_normal_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.admin_fee_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.admin_fee_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.admin_fee_sponsored as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.admin_fee_member as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.binding_days as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.init_contract_value as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_sale.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_sale.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription_sale
 where stage_hash_exerp_subscription_sale.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_subscription_sale records
set @insert_date_time = getdate()
insert into s_exerp_subscription_sale (
       bk_hash,
       subscription_sale_id,
       type,
       sale_datetime,
       start_date,
       end_date,
       jf_normal_price,
       jf_discount,
       jf_price,
       jf_sponsored,
       jf_member,
       prorata_period_normal_price,
       prorata_period_discount,
       prorata_period_price,
       prorata_period_sponsored,
       prorata_period_member,
       init_period_normal_price,
       init_period_discount,
       init_period_price,
       init_period_sponsored,
       init_period_member,
       admin_fee_normal_price,
       admin_fee_discount,
       admin_fee_price,
       admin_fee_sponsored,
       admin_fee_member,
       binding_days,
       init_contract_value,
       state,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_subscription_sale_inserts.bk_hash,
       #s_exerp_subscription_sale_inserts.subscription_sale_id,
       #s_exerp_subscription_sale_inserts.type,
       #s_exerp_subscription_sale_inserts.sale_datetime,
       #s_exerp_subscription_sale_inserts.start_date,
       #s_exerp_subscription_sale_inserts.end_date,
       #s_exerp_subscription_sale_inserts.jf_normal_price,
       #s_exerp_subscription_sale_inserts.jf_discount,
       #s_exerp_subscription_sale_inserts.jf_price,
       #s_exerp_subscription_sale_inserts.jf_sponsored,
       #s_exerp_subscription_sale_inserts.jf_member,
       #s_exerp_subscription_sale_inserts.prorata_period_normal_price,
       #s_exerp_subscription_sale_inserts.prorata_period_discount,
       #s_exerp_subscription_sale_inserts.prorata_period_price,
       #s_exerp_subscription_sale_inserts.prorata_period_sponsored,
       #s_exerp_subscription_sale_inserts.prorata_period_member,
       #s_exerp_subscription_sale_inserts.init_period_normal_price,
       #s_exerp_subscription_sale_inserts.init_period_discount,
       #s_exerp_subscription_sale_inserts.init_period_price,
       #s_exerp_subscription_sale_inserts.init_period_sponsored,
       #s_exerp_subscription_sale_inserts.init_period_member,
       #s_exerp_subscription_sale_inserts.admin_fee_normal_price,
       #s_exerp_subscription_sale_inserts.admin_fee_discount,
       #s_exerp_subscription_sale_inserts.admin_fee_price,
       #s_exerp_subscription_sale_inserts.admin_fee_sponsored,
       #s_exerp_subscription_sale_inserts.admin_fee_member,
       #s_exerp_subscription_sale_inserts.binding_days,
       #s_exerp_subscription_sale_inserts.init_contract_value,
       #s_exerp_subscription_sale_inserts.state,
       #s_exerp_subscription_sale_inserts.ets,
       #s_exerp_subscription_sale_inserts.dummy_modified_date_time,
       case when s_exerp_subscription_sale.s_exerp_subscription_sale_id is null then isnull(#s_exerp_subscription_sale_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_subscription_sale_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_subscription_sale_inserts
  left join p_exerp_subscription_sale
    on #s_exerp_subscription_sale_inserts.bk_hash = p_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_subscription_sale
    on p_exerp_subscription_sale.bk_hash = s_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.s_exerp_subscription_sale_id = s_exerp_subscription_sale.s_exerp_subscription_sale_id
 where s_exerp_subscription_sale.s_exerp_subscription_sale_id is null
    or (s_exerp_subscription_sale.s_exerp_subscription_sale_id is not null
        and s_exerp_subscription_sale.dv_hash <> #s_exerp_subscription_sale_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_subscription_sale @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_subscription_sale @current_dv_batch_id

end
