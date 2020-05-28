CREATE PROC [dbo].[proc_etl_ig_ig_dimension_profit_center_dimension] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_dimension_Profit_Center_Dimension

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_dimension_Profit_Center_Dimension (
       bk_hash,
       profit_center_dim_id,
       customer_id,
       ent_id,
       store_id,
       profit_center_id,
       customer_name,
       ent_name,
       store_name,
       profit_center_name,
       store_tax_no,
       time_zone,
       store_address,
       store_zip,
       store_city,
       store_state,
       store_country,
       store_type_id,
       store_size,
       hierarchy_level,
       hierarchy_name,
       eff_date_from,
       eff_date_to,
       profit_center_desc,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(profit_center_dim_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       profit_center_dim_id,
       customer_id,
       ent_id,
       store_id,
       profit_center_id,
       customer_name,
       ent_name,
       store_name,
       profit_center_name,
       store_tax_no,
       time_zone,
       store_address,
       store_zip,
       store_city,
       store_state,
       store_country,
       store_type_id,
       store_size,
       hierarchy_level,
       hierarchy_name,
       eff_date_from,
       eff_date_to,
       profit_center_desc,
       isnull(cast(stage_ig_ig_dimension_Profit_Center_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_ig_dimension_Profit_Center_Dimension
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_dimension_profit_center_dimension @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_dimension_profit_center_dimension (
       bk_hash,
       profit_center_dim_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_ig_dimension_Profit_Center_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_dim_id profit_center_dim_id,
       isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       13,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_dimension_Profit_Center_Dimension
  left join h_ig_ig_dimension_profit_center_dimension
    on stage_hash_ig_ig_dimension_Profit_Center_Dimension.bk_hash = h_ig_ig_dimension_profit_center_dimension.bk_hash
 where h_ig_ig_dimension_profit_center_dimension_id is null
   and stage_hash_ig_ig_dimension_Profit_Center_Dimension.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_ig_dimension_profit_center_dimension
if object_id('tempdb..#l_ig_ig_dimension_profit_center_dimension_inserts') is not null drop table #l_ig_ig_dimension_profit_center_dimension_inserts
create table #l_ig_ig_dimension_profit_center_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Profit_Center_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.customer_id customer_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.ent_id ent_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_id store_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_id profit_center_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_type_id store_type_id,
       isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.ent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Profit_Center_Dimension
 where stage_hash_ig_ig_dimension_Profit_Center_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_ig_dimension_profit_center_dimension records
set @insert_date_time = getdate()
insert into l_ig_ig_dimension_profit_center_dimension (
       bk_hash,
       profit_center_dim_id,
       customer_id,
       ent_id,
       store_id,
       profit_center_id,
       store_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_ig_dimension_profit_center_dimension_inserts.bk_hash,
       #l_ig_ig_dimension_profit_center_dimension_inserts.profit_center_dim_id,
       #l_ig_ig_dimension_profit_center_dimension_inserts.customer_id,
       #l_ig_ig_dimension_profit_center_dimension_inserts.ent_id,
       #l_ig_ig_dimension_profit_center_dimension_inserts.store_id,
       #l_ig_ig_dimension_profit_center_dimension_inserts.profit_center_id,
       #l_ig_ig_dimension_profit_center_dimension_inserts.store_type_id,
       case when l_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id is null then isnull(#l_ig_ig_dimension_profit_center_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #l_ig_ig_dimension_profit_center_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_ig_dimension_profit_center_dimension_inserts
  left join p_ig_ig_dimension_profit_center_dimension
    on #l_ig_ig_dimension_profit_center_dimension_inserts.bk_hash = p_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_ig_dimension_profit_center_dimension
    on p_ig_ig_dimension_profit_center_dimension.bk_hash = l_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id = l_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id
 where l_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id is null
    or (l_ig_ig_dimension_profit_center_dimension.l_ig_ig_dimension_profit_center_dimension_id is not null
        and l_ig_ig_dimension_profit_center_dimension.dv_hash <> #l_ig_ig_dimension_profit_center_dimension_inserts.source_hash)

--calculate hash and lookup to current s_ig_ig_dimension_profit_center_dimension
if object_id('tempdb..#s_ig_ig_dimension_profit_center_dimension_inserts') is not null drop table #s_ig_ig_dimension_profit_center_dimension_inserts
create table #s_ig_ig_dimension_profit_center_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Profit_Center_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.customer_name customer_name,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.ent_name ent_name,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_name store_name,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_name profit_center_name,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_tax_no store_tax_no,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.time_zone time_zone,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_address store_address,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_zip store_zip,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_city store_city,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_state store_state,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_country store_country,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_size store_size,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.hierarchy_level hierarchy_level,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.hierarchy_name hierarchy_name,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_from eff_date_from,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_to eff_date_to,
       stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_desc profit_center_desc,
       isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.customer_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.ent_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_tax_no,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.time_zone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_address,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_zip,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.store_size,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Profit_Center_Dimension.hierarchy_level as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.hierarchy_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_from,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Profit_Center_Dimension.eff_date_to,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Profit_Center_Dimension.profit_center_desc,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Profit_Center_Dimension
 where stage_hash_ig_ig_dimension_Profit_Center_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_dimension_profit_center_dimension records
set @insert_date_time = getdate()
insert into s_ig_ig_dimension_profit_center_dimension (
       bk_hash,
       profit_center_dim_id,
       customer_name,
       ent_name,
       store_name,
       profit_center_name,
       store_tax_no,
       time_zone,
       store_address,
       store_zip,
       store_city,
       store_state,
       store_country,
       store_size,
       hierarchy_level,
       hierarchy_name,
       eff_date_from,
       eff_date_to,
       profit_center_desc,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_dimension_profit_center_dimension_inserts.bk_hash,
       #s_ig_ig_dimension_profit_center_dimension_inserts.profit_center_dim_id,
       #s_ig_ig_dimension_profit_center_dimension_inserts.customer_name,
       #s_ig_ig_dimension_profit_center_dimension_inserts.ent_name,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_name,
       #s_ig_ig_dimension_profit_center_dimension_inserts.profit_center_name,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_tax_no,
       #s_ig_ig_dimension_profit_center_dimension_inserts.time_zone,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_address,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_zip,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_city,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_state,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_country,
       #s_ig_ig_dimension_profit_center_dimension_inserts.store_size,
       #s_ig_ig_dimension_profit_center_dimension_inserts.hierarchy_level,
       #s_ig_ig_dimension_profit_center_dimension_inserts.hierarchy_name,
       #s_ig_ig_dimension_profit_center_dimension_inserts.eff_date_from,
       #s_ig_ig_dimension_profit_center_dimension_inserts.eff_date_to,
       #s_ig_ig_dimension_profit_center_dimension_inserts.profit_center_desc,
       case when s_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id is null then isnull(#s_ig_ig_dimension_profit_center_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #s_ig_ig_dimension_profit_center_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_dimension_profit_center_dimension_inserts
  left join p_ig_ig_dimension_profit_center_dimension
    on #s_ig_ig_dimension_profit_center_dimension_inserts.bk_hash = p_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_dimension_profit_center_dimension
    on p_ig_ig_dimension_profit_center_dimension.bk_hash = s_ig_ig_dimension_profit_center_dimension.bk_hash
   and p_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id = s_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id
 where s_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id is null
    or (s_ig_ig_dimension_profit_center_dimension.s_ig_ig_dimension_profit_center_dimension_id is not null
        and s_ig_ig_dimension_profit_center_dimension.dv_hash <> #s_ig_ig_dimension_profit_center_dimension_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_dimension_profit_center_dimension @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_dimension_profit_center_dimension @current_dv_batch_id

end
