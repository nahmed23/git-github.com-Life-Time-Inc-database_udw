CREATE PROC [dbo].[proc_etl_magento_sales_shipment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_shipment

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_shipment (
       bk_hash,
       entity_id,
       store_id,
       total_weight,
       total_qty,
       email_sent,
       send_email,
       order_id,
       customer_id,
       shipping_address_id,
       billing_address_id,
       shipment_status,
       increment_id,
       created_at,
       updated_at,
       packages,
       customer_note,
       customer_note_notify,
       m1_shipment_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       store_id,
       total_weight,
       total_qty,
       email_sent,
       send_email,
       order_id,
       customer_id,
       shipping_address_id,
       billing_address_id,
       shipment_status,
       increment_id,
       created_at,
       updated_at,
       packages,
       customer_note,
       customer_note_notify,
       m1_shipment_id,
       isnull(cast(stage_magento_sales_shipment.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_shipment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_shipment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_shipment (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_shipment.bk_hash,
       stage_hash_magento_sales_shipment.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_shipment.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_shipment
  left join h_magento_sales_shipment
    on stage_hash_magento_sales_shipment.bk_hash = h_magento_sales_shipment.bk_hash
 where h_magento_sales_shipment_id is null
   and stage_hash_magento_sales_shipment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_shipment
if object_id('tempdb..#l_magento_sales_shipment_inserts') is not null drop table #l_magento_sales_shipment_inserts
create table #l_magento_sales_shipment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_shipment.bk_hash,
       stage_hash_magento_sales_shipment.entity_id entity_id,
       stage_hash_magento_sales_shipment.store_id store_id,
       stage_hash_magento_sales_shipment.order_id order_id,
       stage_hash_magento_sales_shipment.customer_id customer_id,
       stage_hash_magento_sales_shipment.shipping_address_id shipping_address_id,
       stage_hash_magento_sales_shipment.billing_address_id billing_address_id,
       stage_hash_magento_sales_shipment.m1_shipment_id m1_shipment_id,
       isnull(cast(stage_hash_magento_sales_shipment.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.shipping_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.billing_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.m1_shipment_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_shipment
 where stage_hash_magento_sales_shipment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_shipment records
set @insert_date_time = getdate()
insert into l_magento_sales_shipment (
       bk_hash,
       entity_id,
       store_id,
       order_id,
       customer_id,
       shipping_address_id,
       billing_address_id,
       m1_shipment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_shipment_inserts.bk_hash,
       #l_magento_sales_shipment_inserts.entity_id,
       #l_magento_sales_shipment_inserts.store_id,
       #l_magento_sales_shipment_inserts.order_id,
       #l_magento_sales_shipment_inserts.customer_id,
       #l_magento_sales_shipment_inserts.shipping_address_id,
       #l_magento_sales_shipment_inserts.billing_address_id,
       #l_magento_sales_shipment_inserts.m1_shipment_id,
       case when l_magento_sales_shipment.l_magento_sales_shipment_id is null then isnull(#l_magento_sales_shipment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_shipment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_shipment_inserts
  left join p_magento_sales_shipment
    on #l_magento_sales_shipment_inserts.bk_hash = p_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_shipment
    on p_magento_sales_shipment.bk_hash = l_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.l_magento_sales_shipment_id = l_magento_sales_shipment.l_magento_sales_shipment_id
 where l_magento_sales_shipment.l_magento_sales_shipment_id is null
    or (l_magento_sales_shipment.l_magento_sales_shipment_id is not null
        and l_magento_sales_shipment.dv_hash <> #l_magento_sales_shipment_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_shipment
if object_id('tempdb..#s_magento_sales_shipment_inserts') is not null drop table #s_magento_sales_shipment_inserts
create table #s_magento_sales_shipment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_shipment.bk_hash,
       stage_hash_magento_sales_shipment.entity_id entity_id,
       stage_hash_magento_sales_shipment.total_weight total_weight,
       stage_hash_magento_sales_shipment.total_qty total_qty,
       stage_hash_magento_sales_shipment.email_sent email_sent,
       stage_hash_magento_sales_shipment.send_email send_email,
       stage_hash_magento_sales_shipment.shipment_status shipment_status,
       stage_hash_magento_sales_shipment.increment_id increment_id,
       stage_hash_magento_sales_shipment.created_at created_at,
       stage_hash_magento_sales_shipment.updated_at updated_at,
       stage_hash_magento_sales_shipment.packages packages,
       stage_hash_magento_sales_shipment.customer_note customer_note,
       stage_hash_magento_sales_shipment.customer_note_notify customer_note_notify,
       isnull(cast(stage_hash_magento_sales_shipment.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.total_weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.total_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.email_sent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.send_email as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.shipment_status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_shipment.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_shipment.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_shipment.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_shipment.packages,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_shipment.customer_note,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_shipment.customer_note_notify as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_shipment
 where stage_hash_magento_sales_shipment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_shipment records
set @insert_date_time = getdate()
insert into s_magento_sales_shipment (
       bk_hash,
       entity_id,
       total_weight,
       total_qty,
       email_sent,
       send_email,
       shipment_status,
       increment_id,
       created_at,
       updated_at,
       packages,
       customer_note,
       customer_note_notify,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_shipment_inserts.bk_hash,
       #s_magento_sales_shipment_inserts.entity_id,
       #s_magento_sales_shipment_inserts.total_weight,
       #s_magento_sales_shipment_inserts.total_qty,
       #s_magento_sales_shipment_inserts.email_sent,
       #s_magento_sales_shipment_inserts.send_email,
       #s_magento_sales_shipment_inserts.shipment_status,
       #s_magento_sales_shipment_inserts.increment_id,
       #s_magento_sales_shipment_inserts.created_at,
       #s_magento_sales_shipment_inserts.updated_at,
       #s_magento_sales_shipment_inserts.packages,
       #s_magento_sales_shipment_inserts.customer_note,
       #s_magento_sales_shipment_inserts.customer_note_notify,
       case when s_magento_sales_shipment.s_magento_sales_shipment_id is null then isnull(#s_magento_sales_shipment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_shipment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_shipment_inserts
  left join p_magento_sales_shipment
    on #s_magento_sales_shipment_inserts.bk_hash = p_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_shipment
    on p_magento_sales_shipment.bk_hash = s_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.s_magento_sales_shipment_id = s_magento_sales_shipment.s_magento_sales_shipment_id
 where s_magento_sales_shipment.s_magento_sales_shipment_id is null
    or (s_magento_sales_shipment.s_magento_sales_shipment_id is not null
        and s_magento_sales_shipment.dv_hash <> #s_magento_sales_shipment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_shipment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_shipment @current_dv_batch_id

end
