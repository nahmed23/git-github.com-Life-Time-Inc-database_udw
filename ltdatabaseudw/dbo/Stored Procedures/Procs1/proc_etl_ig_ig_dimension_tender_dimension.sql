CREATE PROC [dbo].[proc_etl_ig_ig_dimension_tender_dimension] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_dimension_Tender_Dimension

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_dimension_Tender_Dimension (
       bk_hash,
       tender_dim_id,
       profit_center_dim_level2_id,
       tender_id,
       tender_name,
       tender_class_id,
       tender_class_name,
       cash_tender_flag,
       comp_tender_flag,
       eff_date_from,
       eff_date_to,
       customer_id,
       ent_id,
       corp_id,
       additional_checkid_code_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(tender_dim_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       tender_dim_id,
       profit_center_dim_level2_id,
       tender_id,
       tender_name,
       tender_class_id,
       tender_class_name,
       cash_tender_flag,
       comp_tender_flag,
       eff_date_from,
       eff_date_to,
       customer_id,
       ent_id,
       corp_id,
       additional_checkid_code_id,
       isnull(cast(stage_ig_ig_dimension_Tender_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_ig_dimension_Tender_Dimension
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_dimension_tender_dimension @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_dimension_tender_dimension (
       bk_hash,
       tender_dim_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_ig_dimension_Tender_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id tender_dim_id,
       isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       13,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_dimension_Tender_Dimension
  left join h_ig_ig_dimension_tender_dimension
    on stage_hash_ig_ig_dimension_Tender_Dimension.bk_hash = h_ig_ig_dimension_tender_dimension.bk_hash
 where h_ig_ig_dimension_tender_dimension_id is null
   and stage_hash_ig_ig_dimension_Tender_Dimension.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_ig_dimension_tender_dimension
if object_id('tempdb..#l_ig_ig_dimension_tender_dimension_inserts') is not null drop table #l_ig_ig_dimension_tender_dimension_inserts
create table #l_ig_ig_dimension_tender_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Tender_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id tender_dim_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.profit_center_dim_level2_id profit_center_dim_level_2_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_id tender_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_class_id tender_class_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.customer_id customer_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.ent_id ent_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.corp_id corp_id,
       isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.profit_center_dim_level2_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.tender_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.tender_class_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.ent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.corp_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Tender_Dimension
 where stage_hash_ig_ig_dimension_Tender_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_ig_dimension_tender_dimension records
set @insert_date_time = getdate()
insert into l_ig_ig_dimension_tender_dimension (
       bk_hash,
       tender_dim_id,
       profit_center_dim_level_2_id,
       tender_id,
       tender_class_id,
       customer_id,
       ent_id,
       corp_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_ig_dimension_tender_dimension_inserts.bk_hash,
       #l_ig_ig_dimension_tender_dimension_inserts.tender_dim_id,
       #l_ig_ig_dimension_tender_dimension_inserts.profit_center_dim_level_2_id,
       #l_ig_ig_dimension_tender_dimension_inserts.tender_id,
       #l_ig_ig_dimension_tender_dimension_inserts.tender_class_id,
       #l_ig_ig_dimension_tender_dimension_inserts.customer_id,
       #l_ig_ig_dimension_tender_dimension_inserts.ent_id,
       #l_ig_ig_dimension_tender_dimension_inserts.corp_id,
       case when l_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_tender_dimension_id is null then isnull(#l_ig_ig_dimension_tender_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #l_ig_ig_dimension_tender_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_ig_dimension_tender_dimension_inserts
  left join p_ig_ig_dimension_tender_dimension
    on #l_ig_ig_dimension_tender_dimension_inserts.bk_hash = p_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_ig_dimension_tender_dimension
    on p_ig_ig_dimension_tender_dimension.bk_hash = l_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_tender_dimension_id = l_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_tender_dimension_id
 where l_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_tender_dimension_id is null
    or (l_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_tender_dimension_id is not null
        and l_ig_ig_dimension_tender_dimension.dv_hash <> #l_ig_ig_dimension_tender_dimension_inserts.source_hash)

--calculate hash and lookup to current s_ig_ig_dimension_tender_dimension
if object_id('tempdb..#s_ig_ig_dimension_tender_dimension_inserts') is not null drop table #s_ig_ig_dimension_tender_dimension_inserts
create table #s_ig_ig_dimension_tender_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Tender_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id tender_dim_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_name tender_name,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_class_name tender_class_name,
       stage_hash_ig_ig_dimension_Tender_Dimension.cash_tender_flag cash_tender_flag,
       stage_hash_ig_ig_dimension_Tender_Dimension.comp_tender_flag comp_tender_flag,
       stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from eff_date_from,
       stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_to eff_date_to,
       isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Tender_Dimension.tender_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Tender_Dimension.tender_class_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.cash_tender_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.comp_tender_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_to,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Tender_Dimension
 where stage_hash_ig_ig_dimension_Tender_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_dimension_tender_dimension records
set @insert_date_time = getdate()
insert into s_ig_ig_dimension_tender_dimension (
       bk_hash,
       tender_dim_id,
       tender_name,
       tender_class_name,
       cash_tender_flag,
       comp_tender_flag,
       eff_date_from,
       eff_date_to,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_dimension_tender_dimension_inserts.bk_hash,
       #s_ig_ig_dimension_tender_dimension_inserts.tender_dim_id,
       #s_ig_ig_dimension_tender_dimension_inserts.tender_name,
       #s_ig_ig_dimension_tender_dimension_inserts.tender_class_name,
       #s_ig_ig_dimension_tender_dimension_inserts.cash_tender_flag,
       #s_ig_ig_dimension_tender_dimension_inserts.comp_tender_flag,
       #s_ig_ig_dimension_tender_dimension_inserts.eff_date_from,
       #s_ig_ig_dimension_tender_dimension_inserts.eff_date_to,
       case when s_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_id is null then isnull(#s_ig_ig_dimension_tender_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #s_ig_ig_dimension_tender_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_dimension_tender_dimension_inserts
  left join p_ig_ig_dimension_tender_dimension
    on #s_ig_ig_dimension_tender_dimension_inserts.bk_hash = p_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_dimension_tender_dimension
    on p_ig_ig_dimension_tender_dimension.bk_hash = s_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_id = s_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_id
 where s_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_id is null
    or (s_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_id is not null
        and s_ig_ig_dimension_tender_dimension.dv_hash <> #s_ig_ig_dimension_tender_dimension_inserts.source_hash)

--calculate hash and lookup to current s_ig_ig_dimension_tender_dimension_1
if object_id('tempdb..#s_ig_ig_dimension_tender_dimension_1_inserts') is not null drop table #s_ig_ig_dimension_tender_dimension_1_inserts
create table #s_ig_ig_dimension_tender_dimension_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Tender_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id tender_dim_id,
       stage_hash_ig_ig_dimension_Tender_Dimension.additional_checkid_code_id additional_check_id_code_id,
       isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.tender_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Tender_Dimension.additional_checkid_code_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Tender_Dimension
 where stage_hash_ig_ig_dimension_Tender_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_dimension_tender_dimension_1 records
set @insert_date_time = getdate()
insert into s_ig_ig_dimension_tender_dimension_1 (
       bk_hash,
       tender_dim_id,
       additional_check_id_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_dimension_tender_dimension_1_inserts.bk_hash,
       #s_ig_ig_dimension_tender_dimension_1_inserts.tender_dim_id,
       #s_ig_ig_dimension_tender_dimension_1_inserts.additional_check_id_code_id,
       case when s_ig_ig_dimension_tender_dimension_1.s_ig_ig_dimension_tender_dimension_1_id is null then isnull(#s_ig_ig_dimension_tender_dimension_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #s_ig_ig_dimension_tender_dimension_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_dimension_tender_dimension_1_inserts
  left join p_ig_ig_dimension_tender_dimension
    on #s_ig_ig_dimension_tender_dimension_1_inserts.bk_hash = p_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_dimension_tender_dimension_1
    on p_ig_ig_dimension_tender_dimension.bk_hash = s_ig_ig_dimension_tender_dimension_1.bk_hash
   and p_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_1_id = s_ig_ig_dimension_tender_dimension_1.s_ig_ig_dimension_tender_dimension_1_id
 where s_ig_ig_dimension_tender_dimension_1.s_ig_ig_dimension_tender_dimension_1_id is null
    or (s_ig_ig_dimension_tender_dimension_1.s_ig_ig_dimension_tender_dimension_1_id is not null
        and s_ig_ig_dimension_tender_dimension_1.dv_hash <> #s_ig_ig_dimension_tender_dimension_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_dimension_tender_dimension @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_dimension_tender_dimension @current_dv_batch_id

end
