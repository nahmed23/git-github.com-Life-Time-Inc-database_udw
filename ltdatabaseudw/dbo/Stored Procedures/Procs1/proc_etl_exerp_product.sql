CREATE PROC [dbo].[proc_etl_exerp_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_product

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_product (
       bk_hash,
       id,
       center_id,
       master_product_id,
       product_group_id,
       name,
       type,
       external_id,
       sales_price,
       minimum_price,
       cost_price,
       blocked,
       sales_commission,
       sales_units,
       period_commission,
       included_member_count,
       ets,
       flat_rate_commission,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       center_id,
       master_product_id,
       product_group_id,
       name,
       type,
       external_id,
       sales_price,
       minimum_price,
       cost_price,
       blocked,
       sales_commission,
       sales_units,
       period_commission,
       included_member_count,
       ets,
       flat_rate_commission,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_product.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_product
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_product @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_product (
       bk_hash,
       product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_product.bk_hash,
       stage_hash_exerp_product.id product_id,
       isnull(cast(stage_hash_exerp_product.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_product
  left join h_exerp_product
    on stage_hash_exerp_product.bk_hash = h_exerp_product.bk_hash
 where h_exerp_product_id is null
   and stage_hash_exerp_product.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_product
if object_id('tempdb..#l_exerp_product_inserts') is not null drop table #l_exerp_product_inserts
create table #l_exerp_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_product.bk_hash,
       stage_hash_exerp_product.id product_id,
       stage_hash_exerp_product.center_id center_id,
       stage_hash_exerp_product.master_product_id master_product_id,
       stage_hash_exerp_product.product_group_id product_group_id,
       stage_hash_exerp_product.external_id external_id,
       isnull(cast(stage_hash_exerp_product.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_product.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.master_product_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.product_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product.external_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_product
 where stage_hash_exerp_product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_product records
set @insert_date_time = getdate()
insert into l_exerp_product (
       bk_hash,
       product_id,
       center_id,
       master_product_id,
       product_group_id,
       external_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_product_inserts.bk_hash,
       #l_exerp_product_inserts.product_id,
       #l_exerp_product_inserts.center_id,
       #l_exerp_product_inserts.master_product_id,
       #l_exerp_product_inserts.product_group_id,
       #l_exerp_product_inserts.external_id,
       case when l_exerp_product.l_exerp_product_id is null then isnull(#l_exerp_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_product_inserts
  left join p_exerp_product
    on #l_exerp_product_inserts.bk_hash = p_exerp_product.bk_hash
   and p_exerp_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_product
    on p_exerp_product.bk_hash = l_exerp_product.bk_hash
   and p_exerp_product.l_exerp_product_id = l_exerp_product.l_exerp_product_id
 where l_exerp_product.l_exerp_product_id is null
    or (l_exerp_product.l_exerp_product_id is not null
        and l_exerp_product.dv_hash <> #l_exerp_product_inserts.source_hash)

--calculate hash and lookup to current s_exerp_product
if object_id('tempdb..#s_exerp_product_inserts') is not null drop table #s_exerp_product_inserts
create table #s_exerp_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_product.bk_hash,
       stage_hash_exerp_product.id product_id,
       stage_hash_exerp_product.name name,
       stage_hash_exerp_product.type type,
       stage_hash_exerp_product.sales_price sales_price,
       stage_hash_exerp_product.minimum_price minimum_price,
       stage_hash_exerp_product.cost_price cost_price,
       stage_hash_exerp_product.blocked blocked,
       stage_hash_exerp_product.sales_commission sales_commission,
       stage_hash_exerp_product.sales_units sales_units,
       stage_hash_exerp_product.period_commission period_commission,
       stage_hash_exerp_product.included_member_count included_member_count,
       stage_hash_exerp_product.ets ets,
       stage_hash_exerp_product.flat_rate_commission flat_rate_commission,
       stage_hash_exerp_product.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_product.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_product.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.sales_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.minimum_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.cost_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product.blocked,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.sales_commission as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.sales_units as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.period_commission as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.included_member_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.ets as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product.flat_rate_commission as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_product
 where stage_hash_exerp_product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_product records
set @insert_date_time = getdate()
insert into s_exerp_product (
       bk_hash,
       product_id,
       name,
       type,
       sales_price,
       minimum_price,
       cost_price,
       blocked,
       sales_commission,
       sales_units,
       period_commission,
       included_member_count,
       ets,
       flat_rate_commission,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_product_inserts.bk_hash,
       #s_exerp_product_inserts.product_id,
       #s_exerp_product_inserts.name,
       #s_exerp_product_inserts.type,
       #s_exerp_product_inserts.sales_price,
       #s_exerp_product_inserts.minimum_price,
       #s_exerp_product_inserts.cost_price,
       #s_exerp_product_inserts.blocked,
       #s_exerp_product_inserts.sales_commission,
       #s_exerp_product_inserts.sales_units,
       #s_exerp_product_inserts.period_commission,
       #s_exerp_product_inserts.included_member_count,
       #s_exerp_product_inserts.ets,
       #s_exerp_product_inserts.flat_rate_commission,
       #s_exerp_product_inserts.dummy_modified_date_time,
       case when s_exerp_product.s_exerp_product_id is null then isnull(#s_exerp_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_product_inserts
  left join p_exerp_product
    on #s_exerp_product_inserts.bk_hash = p_exerp_product.bk_hash
   and p_exerp_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_product
    on p_exerp_product.bk_hash = s_exerp_product.bk_hash
   and p_exerp_product.s_exerp_product_id = s_exerp_product.s_exerp_product_id
 where s_exerp_product.s_exerp_product_id is null
    or (s_exerp_product.s_exerp_product_id is not null
        and s_exerp_product.dv_hash <> #s_exerp_product_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_product @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_product @current_dv_batch_id

end
