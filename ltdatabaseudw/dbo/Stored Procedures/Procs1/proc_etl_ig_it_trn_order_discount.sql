CREATE PROC [dbo].[proc_etl_ig_it_trn_order_discount] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Order_Discount

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Order_Discount (
       bk_hash,
       check_seq,
       discount_amt,
       discoup_id,
       emp_id,
       num_items,
       order_hdr_id,
       tax_amt_incl_disc,
       taxable_disc_flag,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(check_seq as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(order_hdr_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       check_seq,
       discount_amt,
       discoup_id,
       emp_id,
       num_items,
       order_hdr_id,
       tax_amt_incl_disc,
       taxable_disc_flag,
       jan_one,
       isnull(cast(stage_ig_it_trn_Order_Discount.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_trn_Order_Discount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_order_discount @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_order_discount (
       bk_hash,
       check_seq,
       order_hdr_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_trn_Order_Discount.bk_hash,
       stage_hash_ig_it_trn_Order_Discount.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Discount.order_hdr_id order_hdr_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Discount.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Order_Discount
  left join h_ig_it_trn_order_discount
    on stage_hash_ig_it_trn_Order_Discount.bk_hash = h_ig_it_trn_order_discount.bk_hash
 where h_ig_it_trn_order_discount_id is null
   and stage_hash_ig_it_trn_Order_Discount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_trn_order_discount
if object_id('tempdb..#l_ig_it_trn_order_discount_inserts') is not null drop table #l_ig_it_trn_order_discount_inserts
create table #l_ig_it_trn_order_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Discount.bk_hash,
       stage_hash_ig_it_trn_Order_Discount.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Discount.discoup_id discoup_id,
       stage_hash_ig_it_trn_Order_Discount.emp_id emp_id,
       stage_hash_ig_it_trn_Order_Discount.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Discount.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.check_seq as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.discoup_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.emp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.order_hdr_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Discount
 where stage_hash_ig_it_trn_Order_Discount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_trn_order_discount records
set @insert_date_time = getdate()
insert into l_ig_it_trn_order_discount (
       bk_hash,
       check_seq,
       discoup_id,
       emp_id,
       order_hdr_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_trn_order_discount_inserts.bk_hash,
       #l_ig_it_trn_order_discount_inserts.check_seq,
       #l_ig_it_trn_order_discount_inserts.discoup_id,
       #l_ig_it_trn_order_discount_inserts.emp_id,
       #l_ig_it_trn_order_discount_inserts.order_hdr_id,
       case when l_ig_it_trn_order_discount.l_ig_it_trn_order_discount_id is null then isnull(#l_ig_it_trn_order_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #l_ig_it_trn_order_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_trn_order_discount_inserts
  left join p_ig_it_trn_order_discount
    on #l_ig_it_trn_order_discount_inserts.bk_hash = p_ig_it_trn_order_discount.bk_hash
   and p_ig_it_trn_order_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_trn_order_discount
    on p_ig_it_trn_order_discount.bk_hash = l_ig_it_trn_order_discount.bk_hash
   and p_ig_it_trn_order_discount.l_ig_it_trn_order_discount_id = l_ig_it_trn_order_discount.l_ig_it_trn_order_discount_id
 where l_ig_it_trn_order_discount.l_ig_it_trn_order_discount_id is null
    or (l_ig_it_trn_order_discount.l_ig_it_trn_order_discount_id is not null
        and l_ig_it_trn_order_discount.dv_hash <> #l_ig_it_trn_order_discount_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_trn_order_discount
if object_id('tempdb..#s_ig_it_trn_order_discount_inserts') is not null drop table #s_ig_it_trn_order_discount_inserts
create table #s_ig_it_trn_order_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Discount.bk_hash,
       stage_hash_ig_it_trn_Order_Discount.check_seq check_seq,
       stage_hash_ig_it_trn_Order_Discount.discount_amt discount_amt,
       stage_hash_ig_it_trn_Order_Discount.num_items num_items,
       stage_hash_ig_it_trn_Order_Discount.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Discount.tax_amt_incl_disc tax_amt_incl_disc,
       stage_hash_ig_it_trn_Order_Discount.taxable_disc_flag taxable_disc_flag,
       stage_hash_ig_it_trn_Order_Discount.jan_one jan_one,
       stage_hash_ig_it_trn_Order_Discount.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.check_seq as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.discount_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.num_items as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.order_hdr_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.tax_amt_incl_disc as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Discount.taxable_disc_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Discount.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Discount
 where stage_hash_ig_it_trn_Order_Discount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_order_discount records
set @insert_date_time = getdate()
insert into s_ig_it_trn_order_discount (
       bk_hash,
       check_seq,
       discount_amt,
       num_items,
       order_hdr_id,
       tax_amt_incl_disc,
       taxable_disc_flag,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_order_discount_inserts.bk_hash,
       #s_ig_it_trn_order_discount_inserts.check_seq,
       #s_ig_it_trn_order_discount_inserts.discount_amt,
       #s_ig_it_trn_order_discount_inserts.num_items,
       #s_ig_it_trn_order_discount_inserts.order_hdr_id,
       #s_ig_it_trn_order_discount_inserts.tax_amt_incl_disc,
       #s_ig_it_trn_order_discount_inserts.taxable_disc_flag,
       #s_ig_it_trn_order_discount_inserts.jan_one,
       case when s_ig_it_trn_order_discount.s_ig_it_trn_order_discount_id is null then isnull(#s_ig_it_trn_order_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_order_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_order_discount_inserts
  left join p_ig_it_trn_order_discount
    on #s_ig_it_trn_order_discount_inserts.bk_hash = p_ig_it_trn_order_discount.bk_hash
   and p_ig_it_trn_order_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_order_discount
    on p_ig_it_trn_order_discount.bk_hash = s_ig_it_trn_order_discount.bk_hash
   and p_ig_it_trn_order_discount.s_ig_it_trn_order_discount_id = s_ig_it_trn_order_discount.s_ig_it_trn_order_discount_id
 where s_ig_it_trn_order_discount.s_ig_it_trn_order_discount_id is null
    or (s_ig_it_trn_order_discount.s_ig_it_trn_order_discount_id is not null
        and s_ig_it_trn_order_discount.dv_hash <> #s_ig_it_trn_order_discount_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_order_discount @current_dv_batch_id

end
