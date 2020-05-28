CREATE PROC [dbo].[proc_etl_mms_web_order] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_WebOrder

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_WebOrder (
       bk_hash,
       WebOrderID,
       PartyEncryptionID,
       ValProductSalesChannelID,
       PlacedOrderTotal,
       RevisedOrderTotal,
       BalanceDue,
       PlacedDateTime,
       RevisedDateTime,
       IPAddress,
       ExpirationDateTime,
       ValWebOrderStatusID,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(WebOrderID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       WebOrderID,
       PartyEncryptionID,
       ValProductSalesChannelID,
       PlacedOrderTotal,
       RevisedOrderTotal,
       BalanceDue,
       PlacedDateTime,
       RevisedDateTime,
       IPAddress,
       ExpirationDateTime,
       ValWebOrderStatusID,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_WebOrder.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_WebOrder
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_web_order @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_web_order (
       bk_hash,
       web_order_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_WebOrder.bk_hash,
       stage_hash_mms_WebOrder.WebOrderID web_order_id,
       isnull(cast(stage_hash_mms_WebOrder.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_WebOrder
  left join h_mms_web_order
    on stage_hash_mms_WebOrder.bk_hash = h_mms_web_order.bk_hash
 where h_mms_web_order_id is null
   and stage_hash_mms_WebOrder.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_web_order
if object_id('tempdb..#l_mms_web_order_inserts') is not null drop table #l_mms_web_order_inserts
create table #l_mms_web_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_WebOrder.bk_hash,
       stage_hash_mms_WebOrder.WebOrderID web_order_id,
       stage_hash_mms_WebOrder.PartyEncryptionID party_encryption_id,
       stage_hash_mms_WebOrder.ValProductSalesChannelID val_product_sales_channel_id,
       stage_hash_mms_WebOrder.ValWebOrderStatusID val_web_order_status_id,
       isnull(cast(stage_hash_mms_WebOrder.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.WebOrderID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.PartyEncryptionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.ValProductSalesChannelID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.ValWebOrderStatusID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_WebOrder
 where stage_hash_mms_WebOrder.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_web_order records
set @insert_date_time = getdate()
insert into l_mms_web_order (
       bk_hash,
       web_order_id,
       party_encryption_id,
       val_product_sales_channel_id,
       val_web_order_status_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_web_order_inserts.bk_hash,
       #l_mms_web_order_inserts.web_order_id,
       #l_mms_web_order_inserts.party_encryption_id,
       #l_mms_web_order_inserts.val_product_sales_channel_id,
       #l_mms_web_order_inserts.val_web_order_status_id,
       case when l_mms_web_order.l_mms_web_order_id is null then isnull(#l_mms_web_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_web_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_web_order_inserts
  left join p_mms_web_order
    on #l_mms_web_order_inserts.bk_hash = p_mms_web_order.bk_hash
   and p_mms_web_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_web_order
    on p_mms_web_order.bk_hash = l_mms_web_order.bk_hash
   and p_mms_web_order.l_mms_web_order_id = l_mms_web_order.l_mms_web_order_id
 where l_mms_web_order.l_mms_web_order_id is null
    or (l_mms_web_order.l_mms_web_order_id is not null
        and l_mms_web_order.dv_hash <> #l_mms_web_order_inserts.source_hash)

--calculate hash and lookup to current s_mms_web_order
if object_id('tempdb..#s_mms_web_order_inserts') is not null drop table #s_mms_web_order_inserts
create table #s_mms_web_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_WebOrder.bk_hash,
       stage_hash_mms_WebOrder.WebOrderID web_order_id,
       stage_hash_mms_WebOrder.PlacedOrderTotal placed_order_total,
       stage_hash_mms_WebOrder.RevisedOrderTotal revised_order_total,
       stage_hash_mms_WebOrder.BalanceDue balance_due,
       stage_hash_mms_WebOrder.PlacedDateTime placed_date_time,
       stage_hash_mms_WebOrder.RevisedDateTime revised_date_time,
       stage_hash_mms_WebOrder.IPAddress ip_address,
       stage_hash_mms_WebOrder.ExpirationDateTime expiration_date_time,
       stage_hash_mms_WebOrder.InsertedDateTime inserted_date_time,
       stage_hash_mms_WebOrder.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_WebOrder.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.WebOrderID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.PlacedOrderTotal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.RevisedOrderTotal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_WebOrder.BalanceDue as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_WebOrder.PlacedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_WebOrder.RevisedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_WebOrder.IPAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_WebOrder.ExpirationDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_WebOrder.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_WebOrder.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_WebOrder
 where stage_hash_mms_WebOrder.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_web_order records
set @insert_date_time = getdate()
insert into s_mms_web_order (
       bk_hash,
       web_order_id,
       placed_order_total,
       revised_order_total,
       balance_due,
       placed_date_time,
       revised_date_time,
       ip_address,
       expiration_date_time,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_web_order_inserts.bk_hash,
       #s_mms_web_order_inserts.web_order_id,
       #s_mms_web_order_inserts.placed_order_total,
       #s_mms_web_order_inserts.revised_order_total,
       #s_mms_web_order_inserts.balance_due,
       #s_mms_web_order_inserts.placed_date_time,
       #s_mms_web_order_inserts.revised_date_time,
       #s_mms_web_order_inserts.ip_address,
       #s_mms_web_order_inserts.expiration_date_time,
       #s_mms_web_order_inserts.inserted_date_time,
       #s_mms_web_order_inserts.updated_date_time,
       case when s_mms_web_order.s_mms_web_order_id is null then isnull(#s_mms_web_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_web_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_web_order_inserts
  left join p_mms_web_order
    on #s_mms_web_order_inserts.bk_hash = p_mms_web_order.bk_hash
   and p_mms_web_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_web_order
    on p_mms_web_order.bk_hash = s_mms_web_order.bk_hash
   and p_mms_web_order.s_mms_web_order_id = s_mms_web_order.s_mms_web_order_id
 where s_mms_web_order.s_mms_web_order_id is null
    or (s_mms_web_order.s_mms_web_order_id is not null
        and s_mms_web_order.dv_hash <> #s_mms_web_order_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_web_order @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_web_order @current_dv_batch_id

end
