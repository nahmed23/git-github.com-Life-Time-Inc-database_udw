CREATE PROC [dbo].[proc_etl_mms_tran_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_TranItem

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_TranItem (
       bk_hash,
       TranItemID,
       MMSTranID,
       ProductID,
       Quantity,
       ItemSalesTax,
       ItemAmount,
       InsertedDateTime,
       SoldNotServicedFlag,
       UpdatedDateTime,
       ItemDiscountAmount,
       ClubID,
       BundleProductID,
       ExternalItemID,
       ItemLTBucksAmount,
       TransactionSource,
       ItemLTBucksSalesTax,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(TranItemID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       TranItemID,
       MMSTranID,
       ProductID,
       Quantity,
       ItemSalesTax,
       ItemAmount,
       InsertedDateTime,
       SoldNotServicedFlag,
       UpdatedDateTime,
       ItemDiscountAmount,
       ClubID,
       BundleProductID,
       ExternalItemID,
       ItemLTBucksAmount,
       TransactionSource,
       ItemLTBucksSalesTax,
       isnull(cast(stage_mms_TranItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_TranItem
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_tran_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_tran_item (
       bk_hash,
       tran_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_TranItem.bk_hash,
       stage_hash_mms_TranItem.TranItemID tran_item_id,
       isnull(cast(stage_hash_mms_TranItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_TranItem
  left join h_mms_tran_item
    on stage_hash_mms_TranItem.bk_hash = h_mms_tran_item.bk_hash
 where h_mms_tran_item_id is null
   and stage_hash_mms_TranItem.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_tran_item
if object_id('tempdb..#l_mms_tran_item_inserts') is not null drop table #l_mms_tran_item_inserts
create table #l_mms_tran_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItem.bk_hash,
       stage_hash_mms_TranItem.TranItemID tran_item_id,
       stage_hash_mms_TranItem.MMSTranID mms_tran_id,
       stage_hash_mms_TranItem.ProductID product_id,
       stage_hash_mms_TranItem.BundleProductID bundle_product_id,
       stage_hash_mms_TranItem.ExternalItemID external_item_id,
       isnull(cast(stage_hash_mms_TranItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.TranItemID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.MMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.BundleProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_TranItem.ExternalItemID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItem
 where stage_hash_mms_TranItem.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_tran_item records
set @insert_date_time = getdate()
insert into l_mms_tran_item (
       bk_hash,
       tran_item_id,
       mms_tran_id,
       product_id,
       bundle_product_id,
       external_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_tran_item_inserts.bk_hash,
       #l_mms_tran_item_inserts.tran_item_id,
       #l_mms_tran_item_inserts.mms_tran_id,
       #l_mms_tran_item_inserts.product_id,
       #l_mms_tran_item_inserts.bundle_product_id,
       #l_mms_tran_item_inserts.external_item_id,
       case when l_mms_tran_item.l_mms_tran_item_id is null then isnull(#l_mms_tran_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_tran_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_tran_item_inserts
  left join p_mms_tran_item
    on #l_mms_tran_item_inserts.bk_hash = p_mms_tran_item.bk_hash
   and p_mms_tran_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_tran_item
    on p_mms_tran_item.bk_hash = l_mms_tran_item.bk_hash
   and p_mms_tran_item.l_mms_tran_item_id = l_mms_tran_item.l_mms_tran_item_id
 where l_mms_tran_item.l_mms_tran_item_id is null
    or (l_mms_tran_item.l_mms_tran_item_id is not null
        and l_mms_tran_item.dv_hash <> #l_mms_tran_item_inserts.source_hash)

--calculate hash and lookup to current s_mms_tran_item
if object_id('tempdb..#s_mms_tran_item_inserts') is not null drop table #s_mms_tran_item_inserts
create table #s_mms_tran_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItem.bk_hash,
       stage_hash_mms_TranItem.TranItemID tran_item_id,
       stage_hash_mms_TranItem.Quantity quantity,
       stage_hash_mms_TranItem.ItemSalesTax item_sales_tax,
       stage_hash_mms_TranItem.ItemAmount item_amount,
       stage_hash_mms_TranItem.InsertedDateTime inserted_date_time,
       stage_hash_mms_TranItem.SoldNotServicedFlag sold_not_serviced_flag,
       stage_hash_mms_TranItem.UpdatedDateTime updated_date_time,
       stage_hash_mms_TranItem.ItemDiscountAmount item_discount_amount,
       stage_hash_mms_TranItem.ClubID club_id,
       stage_hash_mms_TranItem.ItemLTBucksAmount item_lt_bucks_amount,
       stage_hash_mms_TranItem.TransactionSource transaction_source,
       isnull(cast(stage_hash_mms_TranItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.TranItemID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.Quantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ItemSalesTax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ItemAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItem.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.SoldNotServicedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranItem.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ItemDiscountAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ItemLTBucksAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_TranItem.TransactionSource,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItem
 where stage_hash_mms_TranItem.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_tran_item records
set @insert_date_time = getdate()
insert into s_mms_tran_item (
       bk_hash,
       tran_item_id,
       quantity,
       item_sales_tax,
       item_amount,
       inserted_date_time,
       sold_not_serviced_flag,
       updated_date_time,
       item_discount_amount,
       club_id,
       item_lt_bucks_amount,
       transaction_source,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_tran_item_inserts.bk_hash,
       #s_mms_tran_item_inserts.tran_item_id,
       #s_mms_tran_item_inserts.quantity,
       #s_mms_tran_item_inserts.item_sales_tax,
       #s_mms_tran_item_inserts.item_amount,
       #s_mms_tran_item_inserts.inserted_date_time,
       #s_mms_tran_item_inserts.sold_not_serviced_flag,
       #s_mms_tran_item_inserts.updated_date_time,
       #s_mms_tran_item_inserts.item_discount_amount,
       #s_mms_tran_item_inserts.club_id,
       #s_mms_tran_item_inserts.item_lt_bucks_amount,
       #s_mms_tran_item_inserts.transaction_source,
       case when s_mms_tran_item.s_mms_tran_item_id is null then isnull(#s_mms_tran_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_tran_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_tran_item_inserts
  left join p_mms_tran_item
    on #s_mms_tran_item_inserts.bk_hash = p_mms_tran_item.bk_hash
   and p_mms_tran_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_tran_item
    on p_mms_tran_item.bk_hash = s_mms_tran_item.bk_hash
   and p_mms_tran_item.s_mms_tran_item_id = s_mms_tran_item.s_mms_tran_item_id
 where s_mms_tran_item.s_mms_tran_item_id is null
    or (s_mms_tran_item.s_mms_tran_item_id is not null
        and s_mms_tran_item.dv_hash <> #s_mms_tran_item_inserts.source_hash)

--calculate hash and lookup to current s_mms_tran_item_1
if object_id('tempdb..#s_mms_tran_item_1_inserts') is not null drop table #s_mms_tran_item_1_inserts
create table #s_mms_tran_item_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranItem.bk_hash,
       stage_hash_mms_TranItem.TranItemID tran_item_id,
       stage_hash_mms_TranItem.ItemLTBucksSalesTax item_lt_bucks_sales_tax,
       isnull(cast(stage_hash_mms_TranItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.TranItemID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_TranItem.ItemLTBucksSalesTax as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranItem
 where stage_hash_mms_TranItem.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_tran_item_1 records
set @insert_date_time = getdate()
insert into s_mms_tran_item_1 (
       bk_hash,
       tran_item_id,
       item_lt_bucks_sales_tax,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_tran_item_1_inserts.bk_hash,
       #s_mms_tran_item_1_inserts.tran_item_id,
       #s_mms_tran_item_1_inserts.item_lt_bucks_sales_tax,
       case when s_mms_tran_item_1.s_mms_tran_item_1_id is null then isnull(#s_mms_tran_item_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_tran_item_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_tran_item_1_inserts
  left join p_mms_tran_item
    on #s_mms_tran_item_1_inserts.bk_hash = p_mms_tran_item.bk_hash
   and p_mms_tran_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_tran_item_1
    on p_mms_tran_item.bk_hash = s_mms_tran_item_1.bk_hash
   and p_mms_tran_item.s_mms_tran_item_1_id = s_mms_tran_item_1.s_mms_tran_item_1_id
 where s_mms_tran_item_1.s_mms_tran_item_1_id is null
    or (s_mms_tran_item_1.s_mms_tran_item_1_id is not null
        and s_mms_tran_item_1.dv_hash <> #s_mms_tran_item_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_tran_item @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_tran_item @current_dv_batch_id

end
