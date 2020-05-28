CREATE PROC [dbo].[proc_etl_olo_order_detail] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_olo_order_detail

set @insert_date_time = getdate()
insert into dbo.stage_hash_olo_order_detail (
       bk_hash,
       Messageid,
       Orderid,
       EventType,
       TimeCancelled,
       CancelReason,
       ExternalReference,
       StoreNumber,
       TimePlaced,
       TimeWanted,
       TimeReady,
       SubTotal,
       SalesTax,
       Tip,
       Delivery,
       Discount,
       Total,
       CustomerDelivery,
       PaymentType,
       PaymentDescription,
       Amount,
       TimeAdjusted,
       AdjustmentAmount,
       AdjustmentType,
       AdjustmentReason,
       Timeclosed,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(Messageid,'z#@$k%&P'))),2) bk_hash,
       Messageid,
       Orderid,
       EventType,
       TimeCancelled,
       CancelReason,
       ExternalReference,
       StoreNumber,
       TimePlaced,
       TimeWanted,
       TimeReady,
       SubTotal,
       SalesTax,
       Tip,
       Delivery,
       Discount,
       Total,
       CustomerDelivery,
       PaymentType,
       PaymentDescription,
       Amount,
       TimeAdjusted,
       AdjustmentAmount,
       AdjustmentType,
       AdjustmentReason,
       Timeclosed,
       jan_one,
       isnull(cast(stage_olo_order_detail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_olo_order_detail
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_olo_order_detail @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_olo_order_detail (
       bk_hash,
       message_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_olo_order_detail.bk_hash,
       stage_hash_olo_order_detail.Messageid message_id,
       isnull(cast(stage_hash_olo_order_detail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       36,
       @insert_date_time,
       @user
  from stage_hash_olo_order_detail
  left join h_olo_order_detail
    on stage_hash_olo_order_detail.bk_hash = h_olo_order_detail.bk_hash
 where h_olo_order_detail_id is null
   and stage_hash_olo_order_detail.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_olo_order_detail
if object_id('tempdb..#l_olo_order_detail_inserts') is not null drop table #l_olo_order_detail_inserts
create table #l_olo_order_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_olo_order_detail.bk_hash,
       stage_hash_olo_order_detail.Messageid message_id,
       stage_hash_olo_order_detail.Orderid order_id,
       isnull(cast(stage_hash_olo_order_detail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_olo_order_detail.Messageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Orderid as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_olo_order_detail
 where stage_hash_olo_order_detail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_olo_order_detail records
set @insert_date_time = getdate()
insert into l_olo_order_detail (
       bk_hash,
       message_id,
       order_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_olo_order_detail_inserts.bk_hash,
       #l_olo_order_detail_inserts.message_id,
       #l_olo_order_detail_inserts.order_id,
       case when l_olo_order_detail.l_olo_order_detail_id is null then isnull(#l_olo_order_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       36,
       #l_olo_order_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_olo_order_detail_inserts
  left join p_olo_order_detail
    on #l_olo_order_detail_inserts.bk_hash = p_olo_order_detail.bk_hash
   and p_olo_order_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_olo_order_detail
    on p_olo_order_detail.bk_hash = l_olo_order_detail.bk_hash
   and p_olo_order_detail.l_olo_order_detail_id = l_olo_order_detail.l_olo_order_detail_id
 where l_olo_order_detail.l_olo_order_detail_id is null
    or (l_olo_order_detail.l_olo_order_detail_id is not null
        and l_olo_order_detail.dv_hash <> #l_olo_order_detail_inserts.source_hash)

--calculate hash and lookup to current s_olo_order_detail
if object_id('tempdb..#s_olo_order_detail_inserts') is not null drop table #s_olo_order_detail_inserts
create table #s_olo_order_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_olo_order_detail.bk_hash,
       stage_hash_olo_order_detail.Messageid message_id,
       stage_hash_olo_order_detail.EventType event_type,
       stage_hash_olo_order_detail.TimeCancelled time_cancelled,
       stage_hash_olo_order_detail.CancelReason cancel_reason,
       stage_hash_olo_order_detail.ExternalReference external_reference,
       stage_hash_olo_order_detail.StoreNumber store_number,
       stage_hash_olo_order_detail.TimePlaced time_placed,
       stage_hash_olo_order_detail.TimeWanted time_wanted,
       stage_hash_olo_order_detail.TimeReady time_ready,
       stage_hash_olo_order_detail.SubTotal sub_total,
       stage_hash_olo_order_detail.SalesTax sales_tax,
       stage_hash_olo_order_detail.Tip tip,
       stage_hash_olo_order_detail.Delivery delivery,
       stage_hash_olo_order_detail.Discount discount,
       stage_hash_olo_order_detail.Total total,
       stage_hash_olo_order_detail.CustomerDelivery customer_delivery,
       stage_hash_olo_order_detail.PaymentType payment_type,
       stage_hash_olo_order_detail.PaymentDescription payment_description,
       stage_hash_olo_order_detail.Amount amount,
       stage_hash_olo_order_detail.TimeAdjusted time_adjusted,
       stage_hash_olo_order_detail.AdjustmentAmount adjustment_amount,
       stage_hash_olo_order_detail.AdjustmentType adjustment_type,
       stage_hash_olo_order_detail.AdjustmentReason adjustment_reason,
       stage_hash_olo_order_detail.Timeclosed time_closed,
       stage_hash_olo_order_detail.jan_one jan_one,
       isnull(cast(stage_hash_olo_order_detail.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_olo_order_detail.Messageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.EventType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.TimeCancelled,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.CancelReason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.ExternalReference,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.StoreNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.TimePlaced,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.TimeWanted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.TimeReady,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.SubTotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.SalesTax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Tip as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Delivery as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.CustomerDelivery as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.PaymentType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.PaymentDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.Amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.TimeAdjusted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_olo_order_detail.AdjustmentAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.AdjustmentType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.AdjustmentReason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_olo_order_detail.Timeclosed,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_olo_order_detail.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_olo_order_detail
 where stage_hash_olo_order_detail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_olo_order_detail records
set @insert_date_time = getdate()
insert into s_olo_order_detail (
       bk_hash,
       message_id,
       event_type,
       time_cancelled,
       cancel_reason,
       external_reference,
       store_number,
       time_placed,
       time_wanted,
       time_ready,
       sub_total,
       sales_tax,
       tip,
       delivery,
       discount,
       total,
       customer_delivery,
       payment_type,
       payment_description,
       amount,
       time_adjusted,
       adjustment_amount,
       adjustment_type,
       adjustment_reason,
       time_closed,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_olo_order_detail_inserts.bk_hash,
       #s_olo_order_detail_inserts.message_id,
       #s_olo_order_detail_inserts.event_type,
       #s_olo_order_detail_inserts.time_cancelled,
       #s_olo_order_detail_inserts.cancel_reason,
       #s_olo_order_detail_inserts.external_reference,
       #s_olo_order_detail_inserts.store_number,
       #s_olo_order_detail_inserts.time_placed,
       #s_olo_order_detail_inserts.time_wanted,
       #s_olo_order_detail_inserts.time_ready,
       #s_olo_order_detail_inserts.sub_total,
       #s_olo_order_detail_inserts.sales_tax,
       #s_olo_order_detail_inserts.tip,
       #s_olo_order_detail_inserts.delivery,
       #s_olo_order_detail_inserts.discount,
       #s_olo_order_detail_inserts.total,
       #s_olo_order_detail_inserts.customer_delivery,
       #s_olo_order_detail_inserts.payment_type,
       #s_olo_order_detail_inserts.payment_description,
       #s_olo_order_detail_inserts.amount,
       #s_olo_order_detail_inserts.time_adjusted,
       #s_olo_order_detail_inserts.adjustment_amount,
       #s_olo_order_detail_inserts.adjustment_type,
       #s_olo_order_detail_inserts.adjustment_reason,
       #s_olo_order_detail_inserts.time_closed,
       #s_olo_order_detail_inserts.jan_one,
       case when s_olo_order_detail.s_olo_order_detail_id is null then isnull(#s_olo_order_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       36,
       #s_olo_order_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_olo_order_detail_inserts
  left join p_olo_order_detail
    on #s_olo_order_detail_inserts.bk_hash = p_olo_order_detail.bk_hash
   and p_olo_order_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_olo_order_detail
    on p_olo_order_detail.bk_hash = s_olo_order_detail.bk_hash
   and p_olo_order_detail.s_olo_order_detail_id = s_olo_order_detail.s_olo_order_detail_id
 where s_olo_order_detail.s_olo_order_detail_id is null
    or (s_olo_order_detail.s_olo_order_detail_id is not null
        and s_olo_order_detail.dv_hash <> #s_olo_order_detail_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_olo_order_detail @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_olo_order_detail @current_dv_batch_id

--run fact procs
exec dbo.proc_fact_olo_order_detail @current_dv_batch_id

end
