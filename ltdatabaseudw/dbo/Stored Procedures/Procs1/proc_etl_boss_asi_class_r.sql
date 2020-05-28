CREATE PROC [dbo].[proc_etl_boss_asi_class_r] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiclassr

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiclassr (
       bk_hash,
       classr_dept,
       classr_class,
       classr_desc,
       classr_tax_code,
       classr_unit_order,
       classr_unit_sale,
       classr_comm_part,
       classr_comm_percent,
       classr_comm_amt,
       classr_promo_part,
       classr_suggestion,
       classr_size_name,
       classr_color_name,
       classr_style_name,
       classr_type,
       classr_gl_acct,
       classr_future_acct,
       classr_tax_rate,
       classr_interest_id,
       classr_bill_hrs,
       classr_id,
       classr_format_id,
       classr_web_publish,
       classr_sort_order,
       classr_created_at,
       classr_updated_at,
       classr_grace_days,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(classr_dept as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(classr_class as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       classr_dept,
       classr_class,
       classr_desc,
       classr_tax_code,
       classr_unit_order,
       classr_unit_sale,
       classr_comm_part,
       classr_comm_percent,
       classr_comm_amt,
       classr_promo_part,
       classr_suggestion,
       classr_size_name,
       classr_color_name,
       classr_style_name,
       classr_type,
       classr_gl_acct,
       classr_future_acct,
       classr_tax_rate,
       classr_interest_id,
       classr_bill_hrs,
       classr_id,
       classr_format_id,
       classr_web_publish,
       classr_sort_order,
       classr_created_at,
       classr_updated_at,
       classr_grace_days,
       isnull(cast(stage_boss_asiclassr.classr_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_boss_asiclassr
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_class_r @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_class_r (
       bk_hash,
       class_r_dept,
       class_r_class,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiclassr.bk_hash,
       stage_hash_boss_asiclassr.classr_dept class_r_dept,
       stage_hash_boss_asiclassr.classr_class class_r_class,
       isnull(cast(stage_hash_boss_asiclassr.classr_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiclassr
  left join h_boss_asi_class_r
    on stage_hash_boss_asiclassr.bk_hash = h_boss_asi_class_r.bk_hash
 where h_boss_asi_class_r_id is null
   and stage_hash_boss_asiclassr.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_class_r
if object_id('tempdb..#l_boss_asi_class_r_inserts') is not null drop table #l_boss_asi_class_r_inserts
create table #l_boss_asi_class_r_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiclassr.bk_hash,
       stage_hash_boss_asiclassr.classr_dept class_r_dept,
       stage_hash_boss_asiclassr.classr_class class_r_class,
       stage_hash_boss_asiclassr.classr_tax_code class_r_tax_code,
       stage_hash_boss_asiclassr.classr_interest_id class_r_interest_id,
       stage_hash_boss_asiclassr.classr_id class_r_id,
       stage_hash_boss_asiclassr.classr_format_id class_r_format_id,
       isnull(cast(stage_hash_boss_asiclassr.classr_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_dept as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_class as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_tax_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_interest_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_format_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiclassr
 where stage_hash_boss_asiclassr.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_class_r records
set @insert_date_time = getdate()
insert into l_boss_asi_class_r (
       bk_hash,
       class_r_dept,
       class_r_class,
       class_r_tax_code,
       class_r_interest_id,
       class_r_id,
       class_r_format_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_class_r_inserts.bk_hash,
       #l_boss_asi_class_r_inserts.class_r_dept,
       #l_boss_asi_class_r_inserts.class_r_class,
       #l_boss_asi_class_r_inserts.class_r_tax_code,
       #l_boss_asi_class_r_inserts.class_r_interest_id,
       #l_boss_asi_class_r_inserts.class_r_id,
       #l_boss_asi_class_r_inserts.class_r_format_id,
       case when l_boss_asi_class_r.l_boss_asi_class_r_id is null then isnull(#l_boss_asi_class_r_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_class_r_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_class_r_inserts
  left join p_boss_asi_class_r
    on #l_boss_asi_class_r_inserts.bk_hash = p_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_class_r
    on p_boss_asi_class_r.bk_hash = l_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.l_boss_asi_class_r_id = l_boss_asi_class_r.l_boss_asi_class_r_id
 where l_boss_asi_class_r.l_boss_asi_class_r_id is null
    or (l_boss_asi_class_r.l_boss_asi_class_r_id is not null
        and l_boss_asi_class_r.dv_hash <> #l_boss_asi_class_r_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_class_r
if object_id('tempdb..#s_boss_asi_class_r_inserts') is not null drop table #s_boss_asi_class_r_inserts
create table #s_boss_asi_class_r_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiclassr.bk_hash,
       stage_hash_boss_asiclassr.classr_dept class_r_dept,
       stage_hash_boss_asiclassr.classr_class class_r_class,
       stage_hash_boss_asiclassr.classr_desc class_r_desc,
       stage_hash_boss_asiclassr.classr_unit_order class_r_unit_order,
       stage_hash_boss_asiclassr.classr_unit_sale class_r_unit_sale,
       stage_hash_boss_asiclassr.classr_comm_part class_r_comm_part,
       stage_hash_boss_asiclassr.classr_comm_percent class_r_comm_percent,
       stage_hash_boss_asiclassr.classr_comm_amt class_r_comm_amt,
       stage_hash_boss_asiclassr.classr_promo_part class_r_promo_part,
       stage_hash_boss_asiclassr.classr_suggestion class_r_suggestion,
       stage_hash_boss_asiclassr.classr_size_name class_r_size_name,
       stage_hash_boss_asiclassr.classr_color_name class_r_color_name,
       stage_hash_boss_asiclassr.classr_style_name class_r_style_name,
       stage_hash_boss_asiclassr.classr_type class_r_type,
       stage_hash_boss_asiclassr.classr_gl_acct class_r_gl_acct,
       stage_hash_boss_asiclassr.classr_future_acct class_r_future_acct,
       stage_hash_boss_asiclassr.classr_tax_rate class_r_tax_rate,
       stage_hash_boss_asiclassr.classr_bill_hrs class_r_bill_hrs,
       stage_hash_boss_asiclassr.classr_web_publish class_r_web_publish,
       stage_hash_boss_asiclassr.classr_sort_order class_r_sort_order,
       stage_hash_boss_asiclassr.classr_created_at class_r_created_at,
       stage_hash_boss_asiclassr.classr_updated_at class_r_updated_at,
       stage_hash_boss_asiclassr.classr_grace_days class_r_grace_days,
       isnull(cast(stage_hash_boss_asiclassr.classr_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_dept as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_class as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_unit_order as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_unit_sale as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_comm_part,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_comm_percent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_comm_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_promo_part,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_suggestion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_size_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_color_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_style_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_gl_acct,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_future_acct,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_tax_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_bill_hrs,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclassr.classr_web_publish,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_sort_order as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclassr.classr_created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclassr.classr_updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclassr.classr_grace_days as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiclassr
 where stage_hash_boss_asiclassr.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_class_r records
set @insert_date_time = getdate()
insert into s_boss_asi_class_r (
       bk_hash,
       class_r_dept,
       class_r_class,
       class_r_desc,
       class_r_unit_order,
       class_r_unit_sale,
       class_r_comm_part,
       class_r_comm_percent,
       class_r_comm_amt,
       class_r_promo_part,
       class_r_suggestion,
       class_r_size_name,
       class_r_color_name,
       class_r_style_name,
       class_r_type,
       class_r_gl_acct,
       class_r_future_acct,
       class_r_tax_rate,
       class_r_bill_hrs,
       class_r_web_publish,
       class_r_sort_order,
       class_r_created_at,
       class_r_updated_at,
       class_r_grace_days,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_class_r_inserts.bk_hash,
       #s_boss_asi_class_r_inserts.class_r_dept,
       #s_boss_asi_class_r_inserts.class_r_class,
       #s_boss_asi_class_r_inserts.class_r_desc,
       #s_boss_asi_class_r_inserts.class_r_unit_order,
       #s_boss_asi_class_r_inserts.class_r_unit_sale,
       #s_boss_asi_class_r_inserts.class_r_comm_part,
       #s_boss_asi_class_r_inserts.class_r_comm_percent,
       #s_boss_asi_class_r_inserts.class_r_comm_amt,
       #s_boss_asi_class_r_inserts.class_r_promo_part,
       #s_boss_asi_class_r_inserts.class_r_suggestion,
       #s_boss_asi_class_r_inserts.class_r_size_name,
       #s_boss_asi_class_r_inserts.class_r_color_name,
       #s_boss_asi_class_r_inserts.class_r_style_name,
       #s_boss_asi_class_r_inserts.class_r_type,
       #s_boss_asi_class_r_inserts.class_r_gl_acct,
       #s_boss_asi_class_r_inserts.class_r_future_acct,
       #s_boss_asi_class_r_inserts.class_r_tax_rate,
       #s_boss_asi_class_r_inserts.class_r_bill_hrs,
       #s_boss_asi_class_r_inserts.class_r_web_publish,
       #s_boss_asi_class_r_inserts.class_r_sort_order,
       #s_boss_asi_class_r_inserts.class_r_created_at,
       #s_boss_asi_class_r_inserts.class_r_updated_at,
       #s_boss_asi_class_r_inserts.class_r_grace_days,
       case when s_boss_asi_class_r.s_boss_asi_class_r_id is null then isnull(#s_boss_asi_class_r_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_class_r_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_class_r_inserts
  left join p_boss_asi_class_r
    on #s_boss_asi_class_r_inserts.bk_hash = p_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_class_r
    on p_boss_asi_class_r.bk_hash = s_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.s_boss_asi_class_r_id = s_boss_asi_class_r.s_boss_asi_class_r_id
 where s_boss_asi_class_r.s_boss_asi_class_r_id is null
    or (s_boss_asi_class_r.s_boss_asi_class_r_id is not null
        and s_boss_asi_class_r.dv_hash <> #s_boss_asi_class_r_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_class_r @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_class_r @current_dv_batch_id

end
