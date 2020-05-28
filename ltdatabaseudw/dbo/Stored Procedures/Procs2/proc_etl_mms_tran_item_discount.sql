CREATE PROC [dbo].[proc_etl_mms_tran_item_discount] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_TranItemDiscount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_TranItemDiscount (
       bk_hash,
       TranItemDiscountID,
       TranItemID,
       PricingDiscountID,
       AppliedDiscountAmount,
       InsertedDateTime,
       UpdatedDateTime,
       PromotionCode,
       ValDiscountReasonID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(TranItemDiscountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       TranItemDiscountID,
       TranItemID,
       PricingDiscountID,
       AppliedDiscountAmount,
       InsertedDateTime,
       UpdatedDateTime,
       PromotionCode,
       ValDiscountReasonID,
       isnull(cast(stage_mms_TranItemDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_TranItemDiscount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_tran_item_discount @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_tran_item_discount (
       bk_hash,
       tran_item_discount_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_TranItemDiscount.bk_hash,
       stage_hash_mms_TranItemDiscount.TranItemDiscountID tran_item_discount_id,
       isnull(cast(stage_hash_mms_TranItemDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_TranItemDiscount
  left join h_mms_tran_item_discount
    on stage_hash_mms_TranItemDiscount.bk_hash = h_mms_tran_item_discount.bk_hash
 where h_mms_tran_item_discount_id is null
   and stage_hash_mms_TranItemDiscount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_tran_item_discount
if object_id('tempdb..#l_mms_tran_item_discount_inserts') is not null drop table #l_mms_tran_item_discount_inserts
create table #l_mms_tran_item_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItemDiscount.bk_hash,
       stage_hash_mms_TranItemDiscount.TranItemDiscountID tran_item_discount_id,
       stage_hash_mms_TranItemDiscount.TranItemID tran_item_id,
       stage_hash_mms_TranItemDiscount.PricingDiscountID pricing_discount_id,
       stage_hash_mms_TranItemDiscount.ValDiscountReasonID val_discount_reason_id,
       isnull(cast(stage_hash_mms_TranItemDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.TranItemDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.TranItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.PricingDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.ValDiscountReasonID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItemDiscount
 where stage_hash_mms_TranItemDiscount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_tran_item_discount records
set @insert_date_time = getdate()
insert into l_mms_tran_item_discount (
       bk_hash,
       tran_item_discount_id,
       tran_item_id,
       pricing_discount_id,
       val_discount_reason_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_tran_item_discount_inserts.bk_hash,
       #l_mms_tran_item_discount_inserts.tran_item_discount_id,
       #l_mms_tran_item_discount_inserts.tran_item_id,
       #l_mms_tran_item_discount_inserts.pricing_discount_id,
       #l_mms_tran_item_discount_inserts.val_discount_reason_id,
       case when l_mms_tran_item_discount.l_mms_tran_item_discount_id is null then isnull(#l_mms_tran_item_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_tran_item_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_tran_item_discount_inserts
  left join p_mms_tran_item_discount
    on #l_mms_tran_item_discount_inserts.bk_hash = p_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_tran_item_discount
    on p_mms_tran_item_discount.bk_hash = l_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.l_mms_tran_item_discount_id = l_mms_tran_item_discount.l_mms_tran_item_discount_id
 where l_mms_tran_item_discount.l_mms_tran_item_discount_id is null
    or (l_mms_tran_item_discount.l_mms_tran_item_discount_id is not null
        and l_mms_tran_item_discount.dv_hash <> #l_mms_tran_item_discount_inserts.source_hash)

--calculate hash and lookup to current s_mms_tran_item_discount
if object_id('tempdb..#s_mms_tran_item_discount_inserts') is not null drop table #s_mms_tran_item_discount_inserts
create table #s_mms_tran_item_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItemDiscount.bk_hash,
       stage_hash_mms_TranItemDiscount.TranItemDiscountID tran_item_discount_id,
       stage_hash_mms_TranItemDiscount.AppliedDiscountAmount applied_discount_amount,
       stage_hash_mms_TranItemDiscount.InsertedDateTime inserted_date_time,
       stage_hash_mms_TranItemDiscount.UpdatedDateTime updated_date_time,
       stage_hash_mms_TranItemDiscount.PromotionCode promotion_code,
       isnull(cast(stage_hash_mms_TranItemDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.TranItemDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItemDiscount.AppliedDiscountAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItemDiscount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItemDiscount.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_TranItemDiscount.PromotionCode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItemDiscount
 where stage_hash_mms_TranItemDiscount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_tran_item_discount records
set @insert_date_time = getdate()
insert into s_mms_tran_item_discount (
       bk_hash,
       tran_item_discount_id,
       applied_discount_amount,
       inserted_date_time,
       updated_date_time,
       promotion_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_tran_item_discount_inserts.bk_hash,
       #s_mms_tran_item_discount_inserts.tran_item_discount_id,
       #s_mms_tran_item_discount_inserts.applied_discount_amount,
       #s_mms_tran_item_discount_inserts.inserted_date_time,
       #s_mms_tran_item_discount_inserts.updated_date_time,
       #s_mms_tran_item_discount_inserts.promotion_code,
       case when s_mms_tran_item_discount.s_mms_tran_item_discount_id is null then isnull(#s_mms_tran_item_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_tran_item_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_tran_item_discount_inserts
  left join p_mms_tran_item_discount
    on #s_mms_tran_item_discount_inserts.bk_hash = p_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_tran_item_discount
    on p_mms_tran_item_discount.bk_hash = s_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.s_mms_tran_item_discount_id = s_mms_tran_item_discount.s_mms_tran_item_discount_id
 where s_mms_tran_item_discount.s_mms_tran_item_discount_id is null
    or (s_mms_tran_item_discount.s_mms_tran_item_discount_id is not null
        and s_mms_tran_item_discount.dv_hash <> #s_mms_tran_item_discount_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_tran_item_discount @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_tran_item_discount @current_dv_batch_id

end
