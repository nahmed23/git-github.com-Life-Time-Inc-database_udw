CREATE PROC [dbo].[proc_etl_ig_it_cfg_menu_item_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_menu_item_master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_menu_item_master (
       bk_hash,
       ent_id,
       menu_item_id,
       menu_item_name,
       menu_item_abbr1,
       menu_item_abbr2,
       mi_kp_label,
       prod_class_id,
       rev_cat_id,
       tax_grp_id,
       rpt_cat_id,
       mi_sec_id,
       open_modifier_flag,
       mi_open_price_prompt,
       mi_not_active_flag,
       mi_weight_flag,
       mi_weight_tare,
       mi_discountable_flag,
       mi_emp_discountable_flag,
       mi_voidable_flag,
       mi_print_flag,
       sku_no,
       mi_tax_incl_flag,
       mi_cost_amt,
       bargun_id,
       menu_item_group_id,
       mi_receipt_label,
       mi_price_override_flag,
       data_control_group_id,
       store_id,
       store_created_id,
       covers,
       row_version,
       kds_video_label,
       kds_video_id,
       kds_category_id,
       kds_cook_time,
       default_image_id,
       track_id,
       inserted_date_time,
       updated_date_time,
       track_action,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(menu_item_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ent_id,
       menu_item_id,
       menu_item_name,
       menu_item_abbr1,
       menu_item_abbr2,
       mi_kp_label,
       prod_class_id,
       rev_cat_id,
       tax_grp_id,
       rpt_cat_id,
       mi_sec_id,
       open_modifier_flag,
       mi_open_price_prompt,
       mi_not_active_flag,
       mi_weight_flag,
       mi_weight_tare,
       mi_discountable_flag,
       mi_emp_discountable_flag,
       mi_voidable_flag,
       mi_print_flag,
       sku_no,
       mi_tax_incl_flag,
       mi_cost_amt,
       bargun_id,
       menu_item_group_id,
       mi_receipt_label,
       mi_price_override_flag,
       data_control_group_id,
       store_id,
       store_created_id,
       covers,
       row_version,
       kds_video_label,
       kds_video_id,
       kds_category_id,
       kds_cook_time,
       default_image_id,
       track_id,
       inserted_date_time,
       updated_date_time,
       track_action,
       isnull(cast(stage_ig_it_cfg_menu_item_master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_menu_item_master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_menu_item_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_menu_item_master (
       bk_hash,
       ent_id,
       menu_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_menu_item_master.bk_hash,
       stage_hash_ig_it_cfg_menu_item_master.ent_id ent_id,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_id menu_item_id,
       isnull(cast(stage_hash_ig_it_cfg_menu_item_master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_menu_item_master
  left join h_ig_it_cfg_menu_item_master
    on stage_hash_ig_it_cfg_menu_item_master.bk_hash = h_ig_it_cfg_menu_item_master.bk_hash
 where h_ig_it_cfg_menu_item_master_id is null
   and stage_hash_ig_it_cfg_menu_item_master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_menu_item_master
if object_id('tempdb..#l_ig_it_cfg_menu_item_master_inserts') is not null drop table #l_ig_it_cfg_menu_item_master_inserts
create table #l_ig_it_cfg_menu_item_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_menu_item_master.bk_hash,
       stage_hash_ig_it_cfg_menu_item_master.ent_id ent_id,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_id menu_item_id,
       stage_hash_ig_it_cfg_menu_item_master.prod_class_id prod_class_id,
       stage_hash_ig_it_cfg_menu_item_master.rev_cat_id rev_cat_id,
       stage_hash_ig_it_cfg_menu_item_master.tax_grp_id tax_grp_id,
       stage_hash_ig_it_cfg_menu_item_master.rpt_cat_id rpt_cat_id,
       stage_hash_ig_it_cfg_menu_item_master.mi_sec_id mi_sec_id,
       stage_hash_ig_it_cfg_menu_item_master.bargun_id bargun_id,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_group_id menu_item_group_id,
       stage_hash_ig_it_cfg_menu_item_master.data_control_group_id data_control_group_id,
       stage_hash_ig_it_cfg_menu_item_master.store_id store_id,
       stage_hash_ig_it_cfg_menu_item_master.store_created_id store_created_id,
       stage_hash_ig_it_cfg_menu_item_master.kds_video_id kds_video_id,
       stage_hash_ig_it_cfg_menu_item_master.kds_category_id kds_category_id,
       stage_hash_ig_it_cfg_menu_item_master.default_image_id default_image_id,
       isnull(cast(stage_hash_ig_it_cfg_menu_item_master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.menu_item_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.prod_class_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.rev_cat_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.tax_grp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.rpt_cat_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_sec_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.bargun_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.menu_item_group_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.data_control_group_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.store_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.store_created_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.kds_video_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.kds_category_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.default_image_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_menu_item_master
 where stage_hash_ig_it_cfg_menu_item_master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_menu_item_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_menu_item_master (
       bk_hash,
       ent_id,
       menu_item_id,
       prod_class_id,
       rev_cat_id,
       tax_grp_id,
       rpt_cat_id,
       mi_sec_id,
       bargun_id,
       menu_item_group_id,
       data_control_group_id,
       store_id,
       store_created_id,
       kds_video_id,
       kds_category_id,
       default_image_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_menu_item_master_inserts.bk_hash,
       #l_ig_it_cfg_menu_item_master_inserts.ent_id,
       #l_ig_it_cfg_menu_item_master_inserts.menu_item_id,
       #l_ig_it_cfg_menu_item_master_inserts.prod_class_id,
       #l_ig_it_cfg_menu_item_master_inserts.rev_cat_id,
       #l_ig_it_cfg_menu_item_master_inserts.tax_grp_id,
       #l_ig_it_cfg_menu_item_master_inserts.rpt_cat_id,
       #l_ig_it_cfg_menu_item_master_inserts.mi_sec_id,
       #l_ig_it_cfg_menu_item_master_inserts.bargun_id,
       #l_ig_it_cfg_menu_item_master_inserts.menu_item_group_id,
       #l_ig_it_cfg_menu_item_master_inserts.data_control_group_id,
       #l_ig_it_cfg_menu_item_master_inserts.store_id,
       #l_ig_it_cfg_menu_item_master_inserts.store_created_id,
       #l_ig_it_cfg_menu_item_master_inserts.kds_video_id,
       #l_ig_it_cfg_menu_item_master_inserts.kds_category_id,
       #l_ig_it_cfg_menu_item_master_inserts.default_image_id,
       case when l_ig_it_cfg_menu_item_master.l_ig_it_cfg_menu_item_master_id is null then isnull(#l_ig_it_cfg_menu_item_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_menu_item_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_menu_item_master_inserts
  left join p_ig_it_cfg_menu_item_master
    on #l_ig_it_cfg_menu_item_master_inserts.bk_hash = p_ig_it_cfg_menu_item_master.bk_hash
   and p_ig_it_cfg_menu_item_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_menu_item_master
    on p_ig_it_cfg_menu_item_master.bk_hash = l_ig_it_cfg_menu_item_master.bk_hash
   and p_ig_it_cfg_menu_item_master.l_ig_it_cfg_menu_item_master_id = l_ig_it_cfg_menu_item_master.l_ig_it_cfg_menu_item_master_id
 where l_ig_it_cfg_menu_item_master.l_ig_it_cfg_menu_item_master_id is null
    or (l_ig_it_cfg_menu_item_master.l_ig_it_cfg_menu_item_master_id is not null
        and l_ig_it_cfg_menu_item_master.dv_hash <> #l_ig_it_cfg_menu_item_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_menu_item_master
if object_id('tempdb..#s_ig_it_cfg_menu_item_master_inserts') is not null drop table #s_ig_it_cfg_menu_item_master_inserts
create table #s_ig_it_cfg_menu_item_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_menu_item_master.bk_hash,
       stage_hash_ig_it_cfg_menu_item_master.ent_id ent_id,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_id menu_item_id,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_name menu_item_name,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_abbr1 menu_item_abbr1,
       stage_hash_ig_it_cfg_menu_item_master.menu_item_abbr2 menu_item_abbr2,
       stage_hash_ig_it_cfg_menu_item_master.mi_kp_label mi_kp_label,
       stage_hash_ig_it_cfg_menu_item_master.open_modifier_flag open_modifier_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_open_price_prompt mi_open_price_prompt,
       stage_hash_ig_it_cfg_menu_item_master.mi_not_active_flag mi_not_active_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_weight_flag mi_weight_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_weight_tare mi_weight_tare,
       stage_hash_ig_it_cfg_menu_item_master.mi_discountable_flag mi_discountable_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_emp_discountable_flag mi_emp_discountable_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_voidable_flag mi_voidable_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_print_flag mi_print_flag,
       stage_hash_ig_it_cfg_menu_item_master.sku_no sku_no,
       stage_hash_ig_it_cfg_menu_item_master.mi_tax_incl_flag mi_tax_incl_flag,
       stage_hash_ig_it_cfg_menu_item_master.mi_cost_amt mi_cost_amt,
       stage_hash_ig_it_cfg_menu_item_master.mi_receipt_label mi_receipt_label,
       stage_hash_ig_it_cfg_menu_item_master.mi_price_override_flag mi_price_override_flag,
       stage_hash_ig_it_cfg_menu_item_master.covers covers,
       stage_hash_ig_it_cfg_menu_item_master.row_version row_version,
       stage_hash_ig_it_cfg_menu_item_master.kds_video_label kds_video_label,
       stage_hash_ig_it_cfg_menu_item_master.kds_cook_time kds_cook_time,
       stage_hash_ig_it_cfg_menu_item_master.track_id track_id,
       stage_hash_ig_it_cfg_menu_item_master.track_action track_action,
       stage_hash_ig_it_cfg_menu_item_master.inserted_date_time inserted_date_time,
       stage_hash_ig_it_cfg_menu_item_master.updated_date_time updated_date_time,
       isnull(cast(stage_hash_ig_it_cfg_menu_item_master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.menu_item_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.menu_item_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.menu_item_abbr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.menu_item_abbr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.mi_kp_label,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.open_modifier_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_open_price_prompt as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_not_active_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_weight_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_weight_tare as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_discountable_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_emp_discountable_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_voidable_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_print_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.sku_no,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_tax_incl_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_cost_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.mi_receipt_label,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.mi_price_override_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.covers as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ig_it_cfg_menu_item_master.row_version, 2),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.kds_video_label,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.kds_cook_time as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_menu_item_master.track_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_menu_item_master.track_action,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_menu_item_master.inserted_date_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_menu_item_master.updated_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_menu_item_master
 where stage_hash_ig_it_cfg_menu_item_master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_menu_item_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_menu_item_master (
       bk_hash,
       ent_id,
       menu_item_id,
       menu_item_name,
       menu_item_abbr1,
       menu_item_abbr2,
       mi_kp_label,
       open_modifier_flag,
       mi_open_price_prompt,
       mi_not_active_flag,
       mi_weight_flag,
       mi_weight_tare,
       mi_discountable_flag,
       mi_emp_discountable_flag,
       mi_voidable_flag,
       mi_print_flag,
       sku_no,
       mi_tax_incl_flag,
       mi_cost_amt,
       mi_receipt_label,
       mi_price_override_flag,
       covers,
       row_version,
       kds_video_label,
       kds_cook_time,
       track_id,
       track_action,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_menu_item_master_inserts.bk_hash,
       #s_ig_it_cfg_menu_item_master_inserts.ent_id,
       #s_ig_it_cfg_menu_item_master_inserts.menu_item_id,
       #s_ig_it_cfg_menu_item_master_inserts.menu_item_name,
       #s_ig_it_cfg_menu_item_master_inserts.menu_item_abbr1,
       #s_ig_it_cfg_menu_item_master_inserts.menu_item_abbr2,
       #s_ig_it_cfg_menu_item_master_inserts.mi_kp_label,
       #s_ig_it_cfg_menu_item_master_inserts.open_modifier_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_open_price_prompt,
       #s_ig_it_cfg_menu_item_master_inserts.mi_not_active_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_weight_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_weight_tare,
       #s_ig_it_cfg_menu_item_master_inserts.mi_discountable_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_emp_discountable_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_voidable_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_print_flag,
       #s_ig_it_cfg_menu_item_master_inserts.sku_no,
       #s_ig_it_cfg_menu_item_master_inserts.mi_tax_incl_flag,
       #s_ig_it_cfg_menu_item_master_inserts.mi_cost_amt,
       #s_ig_it_cfg_menu_item_master_inserts.mi_receipt_label,
       #s_ig_it_cfg_menu_item_master_inserts.mi_price_override_flag,
       #s_ig_it_cfg_menu_item_master_inserts.covers,
       #s_ig_it_cfg_menu_item_master_inserts.row_version,
       #s_ig_it_cfg_menu_item_master_inserts.kds_video_label,
       #s_ig_it_cfg_menu_item_master_inserts.kds_cook_time,
       #s_ig_it_cfg_menu_item_master_inserts.track_id,
       #s_ig_it_cfg_menu_item_master_inserts.track_action,
       #s_ig_it_cfg_menu_item_master_inserts.inserted_date_time,
       #s_ig_it_cfg_menu_item_master_inserts.updated_date_time,
       case when s_ig_it_cfg_menu_item_master.s_ig_it_cfg_menu_item_master_id is null then isnull(#s_ig_it_cfg_menu_item_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_menu_item_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_menu_item_master_inserts
  left join p_ig_it_cfg_menu_item_master
    on #s_ig_it_cfg_menu_item_master_inserts.bk_hash = p_ig_it_cfg_menu_item_master.bk_hash
   and p_ig_it_cfg_menu_item_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_menu_item_master
    on p_ig_it_cfg_menu_item_master.bk_hash = s_ig_it_cfg_menu_item_master.bk_hash
   and p_ig_it_cfg_menu_item_master.s_ig_it_cfg_menu_item_master_id = s_ig_it_cfg_menu_item_master.s_ig_it_cfg_menu_item_master_id
 where s_ig_it_cfg_menu_item_master.s_ig_it_cfg_menu_item_master_id is null
    or (s_ig_it_cfg_menu_item_master.s_ig_it_cfg_menu_item_master_id is not null
        and s_ig_it_cfg_menu_item_master.dv_hash <> #s_ig_it_cfg_menu_item_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_menu_item_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_menu_item_master @current_dv_batch_id

end
