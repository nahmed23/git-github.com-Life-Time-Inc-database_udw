CREATE PROC [dbo].[proc_etl_boss_asi_invtr] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiinvtr

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiinvtr (
       bk_hash,
       invtr_upccode,
       invtr_dept,
       invtr_class,
       invtr_vendor,
       invtr_desc,
       invtr_size,
       invtr_color,
       invtr_style,
       invtr_price,
       invtr_cost,
       invtr_promo_part,
       invtr_suggestion,
       invtr_active_promo,
       invtr_sku,
       invtr_created,
       invtr_last_sold,
       invtr_display,
       invtr_legacy_prod_id,
       invtr_class_id,
       invtr_vendor_prod_id,
       invtr_target,
       invtr_limit,
       invtr_iskit,
       invtr_category_id,
       invtr_can_reorder,
       waiver_file_id,
       invtr_updated_at,
       invtr_id,
       use_for_LTBucks,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(invtr_upccode,'z#@$k%&P'))),2) bk_hash,
       invtr_upccode,
       invtr_dept,
       invtr_class,
       invtr_vendor,
       invtr_desc,
       invtr_size,
       invtr_color,
       invtr_style,
       invtr_price,
       invtr_cost,
       invtr_promo_part,
       invtr_suggestion,
       invtr_active_promo,
       invtr_sku,
       invtr_created,
       invtr_last_sold,
       invtr_display,
       invtr_legacy_prod_id,
       invtr_class_id,
       invtr_vendor_prod_id,
       invtr_target,
       invtr_limit,
       invtr_iskit,
       invtr_category_id,
       invtr_can_reorder,
       waiver_file_id,
       invtr_updated_at,
       invtr_id,
       use_for_LTBucks,
       isnull(cast(stage_boss_asiinvtr.invtr_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asiinvtr
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_invtr @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_invtr (
       bk_hash,
       invtr_upc_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiinvtr.bk_hash,
       stage_hash_boss_asiinvtr.invtr_upccode invtr_upc_code,
       isnull(cast(stage_hash_boss_asiinvtr.invtr_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiinvtr
  left join h_boss_asi_invtr
    on stage_hash_boss_asiinvtr.bk_hash = h_boss_asi_invtr.bk_hash
 where h_boss_asi_invtr_id is null
   and stage_hash_boss_asiinvtr.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_invtr
if object_id('tempdb..#l_boss_asi_invtr_inserts') is not null drop table #l_boss_asi_invtr_inserts
create table #l_boss_asi_invtr_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiinvtr.bk_hash,
       stage_hash_boss_asiinvtr.invtr_upccode invtr_upc_code,
       stage_hash_boss_asiinvtr.invtr_dept invtr_dept,
       stage_hash_boss_asiinvtr.invtr_class invtr_class,
       stage_hash_boss_asiinvtr.invtr_vendor invtr_vendor,
       stage_hash_boss_asiinvtr.invtr_size invtr_size,
       stage_hash_boss_asiinvtr.invtr_legacy_prod_id invtr_legacy_prod_id,
       stage_hash_boss_asiinvtr.invtr_class_id invtr_class_id,
       stage_hash_boss_asiinvtr.invtr_vendor_prod_id invtr_vendor_prod_id,
       stage_hash_boss_asiinvtr.invtr_category_id invtr_category_id,
       stage_hash_boss_asiinvtr.invtr_can_reorder invtr_can_reorder,
       stage_hash_boss_asiinvtr.waiver_file_id waiver_file_id,
       stage_hash_boss_asiinvtr.invtr_id invtr_id,
       isnull(cast(stage_hash_boss_asiinvtr.invtr_created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_upccode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_dept as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_class as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_vendor,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_size,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_legacy_prod_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_class_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_vendor_prod_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_category_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_can_reorder,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.waiver_file_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiinvtr
 where stage_hash_boss_asiinvtr.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_invtr records
set @insert_date_time = getdate()
insert into l_boss_asi_invtr (
       bk_hash,
       invtr_upc_code,
       invtr_dept,
       invtr_class,
       invtr_vendor,
       invtr_size,
       invtr_legacy_prod_id,
       invtr_class_id,
       invtr_vendor_prod_id,
       invtr_category_id,
       invtr_can_reorder,
       waiver_file_id,
       invtr_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_invtr_inserts.bk_hash,
       #l_boss_asi_invtr_inserts.invtr_upc_code,
       #l_boss_asi_invtr_inserts.invtr_dept,
       #l_boss_asi_invtr_inserts.invtr_class,
       #l_boss_asi_invtr_inserts.invtr_vendor,
       #l_boss_asi_invtr_inserts.invtr_size,
       #l_boss_asi_invtr_inserts.invtr_legacy_prod_id,
       #l_boss_asi_invtr_inserts.invtr_class_id,
       #l_boss_asi_invtr_inserts.invtr_vendor_prod_id,
       #l_boss_asi_invtr_inserts.invtr_category_id,
       #l_boss_asi_invtr_inserts.invtr_can_reorder,
       #l_boss_asi_invtr_inserts.waiver_file_id,
       #l_boss_asi_invtr_inserts.invtr_id,
       case when l_boss_asi_invtr.l_boss_asi_invtr_id is null then isnull(#l_boss_asi_invtr_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_invtr_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_invtr_inserts
  left join p_boss_asi_invtr
    on #l_boss_asi_invtr_inserts.bk_hash = p_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_invtr
    on p_boss_asi_invtr.bk_hash = l_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.l_boss_asi_invtr_id = l_boss_asi_invtr.l_boss_asi_invtr_id
 where l_boss_asi_invtr.l_boss_asi_invtr_id is null
    or (l_boss_asi_invtr.l_boss_asi_invtr_id is not null
        and l_boss_asi_invtr.dv_hash <> #l_boss_asi_invtr_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_invtr
if object_id('tempdb..#s_boss_asi_invtr_inserts') is not null drop table #s_boss_asi_invtr_inserts
create table #s_boss_asi_invtr_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiinvtr.bk_hash,
       stage_hash_boss_asiinvtr.invtr_upccode invtr_upc_code,
       stage_hash_boss_asiinvtr.invtr_desc invtr_desc,
       stage_hash_boss_asiinvtr.invtr_color invtr_color,
       stage_hash_boss_asiinvtr.invtr_style invtr_style,
       stage_hash_boss_asiinvtr.invtr_price invtr_price,
       stage_hash_boss_asiinvtr.invtr_cost invtr_cost,
       stage_hash_boss_asiinvtr.invtr_promo_part invtr_promo_part,
       stage_hash_boss_asiinvtr.invtr_suggestion invtr_suggestion,
       stage_hash_boss_asiinvtr.invtr_active_promo invtr_active_promo,
       stage_hash_boss_asiinvtr.invtr_sku invtr_sku,
       stage_hash_boss_asiinvtr.invtr_created invtr_created,
       stage_hash_boss_asiinvtr.invtr_last_sold invtr_last_sold,
       stage_hash_boss_asiinvtr.invtr_display invtr_display,
       stage_hash_boss_asiinvtr.invtr_target invtr_target,
       stage_hash_boss_asiinvtr.invtr_limit invtr_limit,
       stage_hash_boss_asiinvtr.invtr_iskit invtr_iskit,
       stage_hash_boss_asiinvtr.invtr_updated_at invtr_updated_at,
       stage_hash_boss_asiinvtr.use_for_LTBucks use_for_ltbucks,
       isnull(cast(stage_hash_boss_asiinvtr.invtr_created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_upccode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_color,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_style,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_promo_part,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_suggestion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_active_promo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_sku,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiinvtr.invtr_created,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiinvtr.invtr_last_sold,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_display,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_target as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiinvtr.invtr_limit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.invtr_iskit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiinvtr.invtr_updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiinvtr.use_for_LTBucks,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiinvtr
 where stage_hash_boss_asiinvtr.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_invtr records
set @insert_date_time = getdate()
insert into s_boss_asi_invtr (
       bk_hash,
       invtr_upc_code,
       invtr_desc,
       invtr_color,
       invtr_style,
       invtr_price,
       invtr_cost,
       invtr_promo_part,
       invtr_suggestion,
       invtr_active_promo,
       invtr_sku,
       invtr_created,
       invtr_last_sold,
       invtr_display,
       invtr_target,
       invtr_limit,
       invtr_iskit,
       invtr_updated_at,
       use_for_ltbucks,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_invtr_inserts.bk_hash,
       #s_boss_asi_invtr_inserts.invtr_upc_code,
       #s_boss_asi_invtr_inserts.invtr_desc,
       #s_boss_asi_invtr_inserts.invtr_color,
       #s_boss_asi_invtr_inserts.invtr_style,
       #s_boss_asi_invtr_inserts.invtr_price,
       #s_boss_asi_invtr_inserts.invtr_cost,
       #s_boss_asi_invtr_inserts.invtr_promo_part,
       #s_boss_asi_invtr_inserts.invtr_suggestion,
       #s_boss_asi_invtr_inserts.invtr_active_promo,
       #s_boss_asi_invtr_inserts.invtr_sku,
       #s_boss_asi_invtr_inserts.invtr_created,
       #s_boss_asi_invtr_inserts.invtr_last_sold,
       #s_boss_asi_invtr_inserts.invtr_display,
       #s_boss_asi_invtr_inserts.invtr_target,
       #s_boss_asi_invtr_inserts.invtr_limit,
       #s_boss_asi_invtr_inserts.invtr_iskit,
       #s_boss_asi_invtr_inserts.invtr_updated_at,
       #s_boss_asi_invtr_inserts.use_for_ltbucks,
       case when s_boss_asi_invtr.s_boss_asi_invtr_id is null then isnull(#s_boss_asi_invtr_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_invtr_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_invtr_inserts
  left join p_boss_asi_invtr
    on #s_boss_asi_invtr_inserts.bk_hash = p_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_invtr
    on p_boss_asi_invtr.bk_hash = s_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.s_boss_asi_invtr_id = s_boss_asi_invtr.s_boss_asi_invtr_id
 where s_boss_asi_invtr.s_boss_asi_invtr_id is null
    or (s_boss_asi_invtr.s_boss_asi_invtr_id is not null
        and s_boss_asi_invtr.dv_hash <> #s_boss_asi_invtr_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_invtr @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_invtr @current_dv_batch_id

end
