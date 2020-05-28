CREATE PROC [dbo].[proc_etl_ig_it_trn_order_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Order_Item

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Order_Item (
       bk_hash,
       check_seq,
       check_type_id,
       check_void_reason_id,
       discount_amt,
       discoup_id,
       item_qty,
       meal_period_id,
       menu_item_id,
       order_hdr_id,
       price_level_id,
       profit_center_id,
       sales_amt_gross,
       server_emp_id,
       split_item_flag,
       tax_amt_incl_disc,
       tax_amt_incl_sales,
       tax_incl_flag,
       void_reason_id,
       void_type_id,
       package_id,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(check_seq as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(order_hdr_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       check_seq,
       check_type_id,
       check_void_reason_id,
       discount_amt,
       discoup_id,
       item_qty,
       meal_period_id,
       menu_item_id,
       order_hdr_id,
       price_level_id,
       profit_center_id,
       sales_amt_gross,
       server_emp_id,
       split_item_flag,
       tax_amt_incl_disc,
       tax_amt_incl_sales,
       tax_incl_flag,
       void_reason_id,
       void_type_id,
       package_id,
       jan_one,
       isnull(cast(stage_ig_it_trn_Order_Item.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_trn_Order_Item
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_order_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_order_item (
       bk_hash,
       check_seq,
       order_hdr_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_trn_Order_Item.bk_hash,
       stage_hash_ig_it_trn_Order_Item.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Item.order_hdr_id order_hdr_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Item.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Order_Item
  left join h_ig_it_trn_order_item
    on stage_hash_ig_it_trn_Order_Item.bk_hash = h_ig_it_trn_order_item.bk_hash
 where h_ig_it_trn_order_item_id is null
   and stage_hash_ig_it_trn_Order_Item.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_trn_order_item
if object_id('tempdb..#l_ig_it_trn_order_item_inserts') is not null drop table #l_ig_it_trn_order_item_inserts
create table #l_ig_it_trn_order_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Item.bk_hash,
       stage_hash_ig_it_trn_Order_Item.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Item.check_type_id check_type_id,
       stage_hash_ig_it_trn_Order_Item.check_void_reason_id check_void_reason_id,
       stage_hash_ig_it_trn_Order_Item.discoup_id discoup_id,
       stage_hash_ig_it_trn_Order_Item.meal_period_id meal_period_id,
       stage_hash_ig_it_trn_Order_Item.menu_item_id menu_item_id,
       stage_hash_ig_it_trn_Order_Item.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Item.price_level_id price_level_id,
       stage_hash_ig_it_trn_Order_Item.profit_center_id profit_center_id,
       stage_hash_ig_it_trn_Order_Item.server_emp_id server_emp_id,
       stage_hash_ig_it_trn_Order_Item.void_reason_id void_reason_id,
       stage_hash_ig_it_trn_Order_Item.void_type_id void_type_id,
       stage_hash_ig_it_trn_Order_Item.package_id package_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Item.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.check_seq as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.check_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.check_void_reason_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.discoup_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.meal_period_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.menu_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.order_hdr_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.price_level_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.profit_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.server_emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.void_reason_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.void_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.package_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Item
 where stage_hash_ig_it_trn_Order_Item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_trn_order_item records
set @insert_date_time = getdate()
insert into l_ig_it_trn_order_item (
       bk_hash,
       check_seq,
       check_type_id,
       check_void_reason_id,
       discoup_id,
       meal_period_id,
       menu_item_id,
       order_hdr_id,
       price_level_id,
       profit_center_id,
       server_emp_id,
       void_reason_id,
       void_type_id,
       package_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_trn_order_item_inserts.bk_hash,
       #l_ig_it_trn_order_item_inserts.check_seq,
       #l_ig_it_trn_order_item_inserts.check_type_id,
       #l_ig_it_trn_order_item_inserts.check_void_reason_id,
       #l_ig_it_trn_order_item_inserts.discoup_id,
       #l_ig_it_trn_order_item_inserts.meal_period_id,
       #l_ig_it_trn_order_item_inserts.menu_item_id,
       #l_ig_it_trn_order_item_inserts.order_hdr_id,
       #l_ig_it_trn_order_item_inserts.price_level_id,
       #l_ig_it_trn_order_item_inserts.profit_center_id,
       #l_ig_it_trn_order_item_inserts.server_emp_id,
       #l_ig_it_trn_order_item_inserts.void_reason_id,
       #l_ig_it_trn_order_item_inserts.void_type_id,
       #l_ig_it_trn_order_item_inserts.package_id,
       case when l_ig_it_trn_order_item.l_ig_it_trn_order_item_id is null then isnull(#l_ig_it_trn_order_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #l_ig_it_trn_order_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_trn_order_item_inserts
  left join p_ig_it_trn_order_item
    on #l_ig_it_trn_order_item_inserts.bk_hash = p_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_trn_order_item
    on p_ig_it_trn_order_item.bk_hash = l_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.l_ig_it_trn_order_item_id = l_ig_it_trn_order_item.l_ig_it_trn_order_item_id
 where l_ig_it_trn_order_item.l_ig_it_trn_order_item_id is null
    or (l_ig_it_trn_order_item.l_ig_it_trn_order_item_id is not null
        and l_ig_it_trn_order_item.dv_hash <> #l_ig_it_trn_order_item_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_trn_order_item
if object_id('tempdb..#s_ig_it_trn_order_item_inserts') is not null drop table #s_ig_it_trn_order_item_inserts
create table #s_ig_it_trn_order_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Item.bk_hash,
       stage_hash_ig_it_trn_Order_Item.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Item.discount_amt discount_amt,
       stage_hash_ig_it_trn_Order_Item.item_qty item_qty,
       stage_hash_ig_it_trn_Order_Item.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Item.sales_amt_gross sales_amt_gross,
       stage_hash_ig_it_trn_Order_Item.split_item_flag split_item_flag,
       stage_hash_ig_it_trn_Order_Item.tax_amt_incl_disc tax_amt_incl_disc,
       stage_hash_ig_it_trn_Order_Item.tax_amt_incl_sales tax_amt_incl_sales,
       stage_hash_ig_it_trn_Order_Item.tax_incl_flag tax_incl_flag,
       stage_hash_ig_it_trn_Order_Item.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_trn_Order_Item.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.check_seq as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.discount_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.item_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.order_hdr_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.sales_amt_gross as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Item.split_item_flag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.tax_amt_incl_disc as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Item.tax_amt_incl_sales as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Item.tax_incl_flag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Item.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Item
 where stage_hash_ig_it_trn_Order_Item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_order_item records
set @insert_date_time = getdate()
insert into s_ig_it_trn_order_item (
       bk_hash,
       check_seq,
       discount_amt,
       item_qty,
       order_hdr_id,
       sales_amt_gross,
       split_item_flag,
       tax_amt_incl_disc,
       tax_amt_incl_sales,
       tax_incl_flag,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_order_item_inserts.bk_hash,
       #s_ig_it_trn_order_item_inserts.check_seq,
       #s_ig_it_trn_order_item_inserts.discount_amt,
       #s_ig_it_trn_order_item_inserts.item_qty,
       #s_ig_it_trn_order_item_inserts.order_hdr_id,
       #s_ig_it_trn_order_item_inserts.sales_amt_gross,
       #s_ig_it_trn_order_item_inserts.split_item_flag,
       #s_ig_it_trn_order_item_inserts.tax_amt_incl_disc,
       #s_ig_it_trn_order_item_inserts.tax_amt_incl_sales,
       #s_ig_it_trn_order_item_inserts.tax_incl_flag,
       #s_ig_it_trn_order_item_inserts.jan_one,
       case when s_ig_it_trn_order_item.s_ig_it_trn_order_item_id is null then isnull(#s_ig_it_trn_order_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_order_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_order_item_inserts
  left join p_ig_it_trn_order_item
    on #s_ig_it_trn_order_item_inserts.bk_hash = p_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_order_item
    on p_ig_it_trn_order_item.bk_hash = s_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.s_ig_it_trn_order_item_id = s_ig_it_trn_order_item.s_ig_it_trn_order_item_id
 where s_ig_it_trn_order_item.s_ig_it_trn_order_item_id is null
    or (s_ig_it_trn_order_item.s_ig_it_trn_order_item_id is not null
        and s_ig_it_trn_order_item.dv_hash <> #s_ig_it_trn_order_item_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_order_item @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_order_item @current_dv_batch_id

end
