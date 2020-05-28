CREATE PROC [dbo].[proc_etl_magento_sales_order_payment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_order_payment

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_order_payment (
       bk_hash,
       entity_id,
       parent_id,
       base_shipping_captured,
       shipping_captured,
       amount_refunded,
       base_amount_paid,
       amount_canceled,
       base_amount_authorized,
       base_amount_paid_online,
       base_amount_refunded_online,
       base_shipping_amount,
       shipping_amount,
       amount_paid,
       amount_authorized,
       base_amount_ordered,
       base_shipping_refunded,
       shipping_refunded,
       base_amount_refunded,
       amount_ordered,
       base_amount_canceled,
       quote_payment_id,
       additional_data,
       cc_exp_month,
       cc_ss_start_year,
       echeck_bank_name,
       method,
       cc_debug_request_body,
       cc_secure_verify,
       protection_eligibility,
       cc_approval,
       cc_last_4,
       cc_status_description,
       echeck_type,
       cc_debug_response_serialized,
       cc_ss_start_month,
       echeck_account_type,
       last_trans_id,
       cc_cid_status,
       cc_owner,
       cc_type,
       po_number,
       cc_exp_year,
       cc_status,
       echeck_routing_number,
       account_status,
       anet_trans_method,
       cc_debug_response_body,
       cc_ss_issue,
       echeck_account_name,
       cc_avs_status,
       cc_number_enc,
       cc_trans_id,
       address_status,
       additional_information,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       parent_id,
       base_shipping_captured,
       shipping_captured,
       amount_refunded,
       base_amount_paid,
       amount_canceled,
       base_amount_authorized,
       base_amount_paid_online,
       base_amount_refunded_online,
       base_shipping_amount,
       shipping_amount,
       amount_paid,
       amount_authorized,
       base_amount_ordered,
       base_shipping_refunded,
       shipping_refunded,
       base_amount_refunded,
       amount_ordered,
       base_amount_canceled,
       quote_payment_id,
       additional_data,
       cc_exp_month,
       cc_ss_start_year,
       echeck_bank_name,
       method,
       cc_debug_request_body,
       cc_secure_verify,
       protection_eligibility,
       cc_approval,
       cc_last_4,
       cc_status_description,
       echeck_type,
       cc_debug_response_serialized,
       cc_ss_start_month,
       echeck_account_type,
       last_trans_id,
       cc_cid_status,
       cc_owner,
       cc_type,
       po_number,
       cc_exp_year,
       cc_status,
       echeck_routing_number,
       account_status,
       anet_trans_method,
       cc_debug_response_body,
       cc_ss_issue,
       echeck_account_name,
       cc_avs_status,
       cc_number_enc,
       cc_trans_id,
       address_status,
       additional_information,
       dummy_modified_date_time,
       isnull(cast(stage_magento_sales_order_payment.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_order_payment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_order_payment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_order_payment (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_magento_sales_order_payment.bk_hash,
       stage_hash_magento_sales_order_payment.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_order_payment.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_order_payment
  left join h_magento_sales_order_payment
    on stage_hash_magento_sales_order_payment.bk_hash = h_magento_sales_order_payment.bk_hash
 where h_magento_sales_order_payment_id is null
   and stage_hash_magento_sales_order_payment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_order_payment
if object_id('tempdb..#l_magento_sales_order_payment_inserts') is not null drop table #l_magento_sales_order_payment_inserts
create table #l_magento_sales_order_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_payment.bk_hash,
       stage_hash_magento_sales_order_payment.entity_id entity_id,
       stage_hash_magento_sales_order_payment.parent_id parent_id,
       stage_hash_magento_sales_order_payment.last_trans_id last_trans_id,
       stage_hash_magento_sales_order_payment.quote_payment_id quote_payment_id,
       isnull(cast(stage_hash_magento_sales_order_payment.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.last_trans_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.quote_payment_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_payment
 where stage_hash_magento_sales_order_payment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_order_payment records
set @insert_date_time = getdate()
insert into l_magento_sales_order_payment (
       bk_hash,
       entity_id,
       parent_id,
       last_trans_id,
       quote_payment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_order_payment_inserts.bk_hash,
       #l_magento_sales_order_payment_inserts.entity_id,
       #l_magento_sales_order_payment_inserts.parent_id,
       #l_magento_sales_order_payment_inserts.last_trans_id,
       #l_magento_sales_order_payment_inserts.quote_payment_id,
       case when l_magento_sales_order_payment.l_magento_sales_order_payment_id is null then isnull(#l_magento_sales_order_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_order_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_order_payment_inserts
  left join p_magento_sales_order_payment
    on #l_magento_sales_order_payment_inserts.bk_hash = p_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_order_payment
    on p_magento_sales_order_payment.bk_hash = l_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.l_magento_sales_order_payment_id = l_magento_sales_order_payment.l_magento_sales_order_payment_id
 where l_magento_sales_order_payment.l_magento_sales_order_payment_id is null
    or (l_magento_sales_order_payment.l_magento_sales_order_payment_id is not null
        and l_magento_sales_order_payment.dv_hash <> #l_magento_sales_order_payment_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_order_payment
if object_id('tempdb..#s_magento_sales_order_payment_inserts') is not null drop table #s_magento_sales_order_payment_inserts
create table #s_magento_sales_order_payment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_payment.bk_hash,
       stage_hash_magento_sales_order_payment.entity_id entity_id,
       stage_hash_magento_sales_order_payment.base_shipping_captured base_shipping_captured,
       stage_hash_magento_sales_order_payment.shipping_captured shipping_captured,
       stage_hash_magento_sales_order_payment.amount_refunded amount_refunded,
       stage_hash_magento_sales_order_payment.base_amount_paid base_amount_paid,
       stage_hash_magento_sales_order_payment.amount_canceled amount_canceled,
       stage_hash_magento_sales_order_payment.base_amount_authorized base_amount_authorized,
       stage_hash_magento_sales_order_payment.base_amount_paid_online base_amount_paid_online,
       stage_hash_magento_sales_order_payment.base_amount_refunded_online base_amount_refunded_online,
       stage_hash_magento_sales_order_payment.base_shipping_amount base_shipping_amount,
       stage_hash_magento_sales_order_payment.shipping_amount shipping_amount,
       stage_hash_magento_sales_order_payment.amount_paid amount_paid,
       stage_hash_magento_sales_order_payment.amount_authorized amount_authorized,
       stage_hash_magento_sales_order_payment.base_amount_ordered base_amount_ordered,
       stage_hash_magento_sales_order_payment.base_shipping_refunded base_shipping_refunded,
       stage_hash_magento_sales_order_payment.shipping_refunded shipping_refunded,
       stage_hash_magento_sales_order_payment.base_amount_refunded base_amount_refunded,
       stage_hash_magento_sales_order_payment.amount_ordered amount_ordered,
       stage_hash_magento_sales_order_payment.base_amount_canceled base_amount_canceled,
       stage_hash_magento_sales_order_payment.additional_data additional_data,
       stage_hash_magento_sales_order_payment.cc_exp_month cc_exp_month,
       stage_hash_magento_sales_order_payment.cc_ss_start_year cc_ss_start_year,
       stage_hash_magento_sales_order_payment.echeck_bank_name echeck_bank_name,
       stage_hash_magento_sales_order_payment.method method,
       stage_hash_magento_sales_order_payment.cc_debug_request_body cc_debug_request_body,
       stage_hash_magento_sales_order_payment.cc_secure_verify cc_secure_verify,
       stage_hash_magento_sales_order_payment.protection_eligibility protection_eligibility,
       stage_hash_magento_sales_order_payment.cc_approval cc_approval,
       stage_hash_magento_sales_order_payment.cc_last_4 cc_last_4,
       stage_hash_magento_sales_order_payment.cc_status_description cc_status_description,
       stage_hash_magento_sales_order_payment.echeck_type echeck_type,
       stage_hash_magento_sales_order_payment.cc_debug_response_serialized cc_debug_response_serialized,
       stage_hash_magento_sales_order_payment.cc_ss_start_month cc_ss_start_month,
       stage_hash_magento_sales_order_payment.echeck_account_type echeck_account_type,
       stage_hash_magento_sales_order_payment.cc_cid_status cc_cid_status,
       stage_hash_magento_sales_order_payment.cc_owner cc_owner,
       stage_hash_magento_sales_order_payment.cc_type cc_type,
       stage_hash_magento_sales_order_payment.po_number po_number,
       stage_hash_magento_sales_order_payment.cc_exp_year cc_exp_year,
       stage_hash_magento_sales_order_payment.cc_status cc_status,
       stage_hash_magento_sales_order_payment.echeck_routing_number echeck_routing_number,
       stage_hash_magento_sales_order_payment.account_status account_status,
       stage_hash_magento_sales_order_payment.anet_trans_method anet_trans_method,
       stage_hash_magento_sales_order_payment.cc_debug_response_body cc_debug_response_body,
       stage_hash_magento_sales_order_payment.cc_ss_issue cc_ss_issue,
       stage_hash_magento_sales_order_payment.echeck_account_name echeck_account_name,
       stage_hash_magento_sales_order_payment.cc_avs_status cc_avs_status,
       stage_hash_magento_sales_order_payment.cc_number_enc cc_number_enc,
       stage_hash_magento_sales_order_payment.cc_trans_id cc_trans_id,
       stage_hash_magento_sales_order_payment.address_status address_status,
       stage_hash_magento_sales_order_payment.additional_information additional_information,
       stage_hash_magento_sales_order_payment.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_sales_order_payment.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_shipping_captured as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.shipping_captured as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_paid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.amount_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_authorized as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_paid_online as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_refunded_online as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.amount_paid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.amount_authorized as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_ordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_shipping_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.shipping_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.amount_ordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_payment.base_amount_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.additional_data,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_exp_month,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_ss_start_year,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.echeck_bank_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.method,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_debug_request_body,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_secure_verify,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.protection_eligibility,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_approval,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_last_4,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_status_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.echeck_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_debug_response_serialized,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_ss_start_month,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.echeck_account_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_cid_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_owner,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.po_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_exp_year,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.echeck_routing_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.account_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.anet_trans_method,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_debug_response_body,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_ss_issue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.echeck_account_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_avs_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_number_enc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.cc_trans_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.address_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_payment.additional_information,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order_payment.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_payment
 where stage_hash_magento_sales_order_payment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_order_payment records
set @insert_date_time = getdate()
insert into s_magento_sales_order_payment (
       bk_hash,
       entity_id,
       base_shipping_captured,
       shipping_captured,
       amount_refunded,
       base_amount_paid,
       amount_canceled,
       base_amount_authorized,
       base_amount_paid_online,
       base_amount_refunded_online,
       base_shipping_amount,
       shipping_amount,
       amount_paid,
       amount_authorized,
       base_amount_ordered,
       base_shipping_refunded,
       shipping_refunded,
       base_amount_refunded,
       amount_ordered,
       base_amount_canceled,
       additional_data,
       cc_exp_month,
       cc_ss_start_year,
       echeck_bank_name,
       method,
       cc_debug_request_body,
       cc_secure_verify,
       protection_eligibility,
       cc_approval,
       cc_last_4,
       cc_status_description,
       echeck_type,
       cc_debug_response_serialized,
       cc_ss_start_month,
       echeck_account_type,
       cc_cid_status,
       cc_owner,
       cc_type,
       po_number,
       cc_exp_year,
       cc_status,
       echeck_routing_number,
       account_status,
       anet_trans_method,
       cc_debug_response_body,
       cc_ss_issue,
       echeck_account_name,
       cc_avs_status,
       cc_number_enc,
       cc_trans_id,
       address_status,
       additional_information,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_order_payment_inserts.bk_hash,
       #s_magento_sales_order_payment_inserts.entity_id,
       #s_magento_sales_order_payment_inserts.base_shipping_captured,
       #s_magento_sales_order_payment_inserts.shipping_captured,
       #s_magento_sales_order_payment_inserts.amount_refunded,
       #s_magento_sales_order_payment_inserts.base_amount_paid,
       #s_magento_sales_order_payment_inserts.amount_canceled,
       #s_magento_sales_order_payment_inserts.base_amount_authorized,
       #s_magento_sales_order_payment_inserts.base_amount_paid_online,
       #s_magento_sales_order_payment_inserts.base_amount_refunded_online,
       #s_magento_sales_order_payment_inserts.base_shipping_amount,
       #s_magento_sales_order_payment_inserts.shipping_amount,
       #s_magento_sales_order_payment_inserts.amount_paid,
       #s_magento_sales_order_payment_inserts.amount_authorized,
       #s_magento_sales_order_payment_inserts.base_amount_ordered,
       #s_magento_sales_order_payment_inserts.base_shipping_refunded,
       #s_magento_sales_order_payment_inserts.shipping_refunded,
       #s_magento_sales_order_payment_inserts.base_amount_refunded,
       #s_magento_sales_order_payment_inserts.amount_ordered,
       #s_magento_sales_order_payment_inserts.base_amount_canceled,
       #s_magento_sales_order_payment_inserts.additional_data,
       #s_magento_sales_order_payment_inserts.cc_exp_month,
       #s_magento_sales_order_payment_inserts.cc_ss_start_year,
       #s_magento_sales_order_payment_inserts.echeck_bank_name,
       #s_magento_sales_order_payment_inserts.method,
       #s_magento_sales_order_payment_inserts.cc_debug_request_body,
       #s_magento_sales_order_payment_inserts.cc_secure_verify,
       #s_magento_sales_order_payment_inserts.protection_eligibility,
       #s_magento_sales_order_payment_inserts.cc_approval,
       #s_magento_sales_order_payment_inserts.cc_last_4,
       #s_magento_sales_order_payment_inserts.cc_status_description,
       #s_magento_sales_order_payment_inserts.echeck_type,
       #s_magento_sales_order_payment_inserts.cc_debug_response_serialized,
       #s_magento_sales_order_payment_inserts.cc_ss_start_month,
       #s_magento_sales_order_payment_inserts.echeck_account_type,
       #s_magento_sales_order_payment_inserts.cc_cid_status,
       #s_magento_sales_order_payment_inserts.cc_owner,
       #s_magento_sales_order_payment_inserts.cc_type,
       #s_magento_sales_order_payment_inserts.po_number,
       #s_magento_sales_order_payment_inserts.cc_exp_year,
       #s_magento_sales_order_payment_inserts.cc_status,
       #s_magento_sales_order_payment_inserts.echeck_routing_number,
       #s_magento_sales_order_payment_inserts.account_status,
       #s_magento_sales_order_payment_inserts.anet_trans_method,
       #s_magento_sales_order_payment_inserts.cc_debug_response_body,
       #s_magento_sales_order_payment_inserts.cc_ss_issue,
       #s_magento_sales_order_payment_inserts.echeck_account_name,
       #s_magento_sales_order_payment_inserts.cc_avs_status,
       #s_magento_sales_order_payment_inserts.cc_number_enc,
       #s_magento_sales_order_payment_inserts.cc_trans_id,
       #s_magento_sales_order_payment_inserts.address_status,
       #s_magento_sales_order_payment_inserts.additional_information,
       #s_magento_sales_order_payment_inserts.dummy_modified_date_time,
       case when s_magento_sales_order_payment.s_magento_sales_order_payment_id is null then isnull(#s_magento_sales_order_payment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_order_payment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_order_payment_inserts
  left join p_magento_sales_order_payment
    on #s_magento_sales_order_payment_inserts.bk_hash = p_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_order_payment
    on p_magento_sales_order_payment.bk_hash = s_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.s_magento_sales_order_payment_id = s_magento_sales_order_payment.s_magento_sales_order_payment_id
 where s_magento_sales_order_payment.s_magento_sales_order_payment_id is null
    or (s_magento_sales_order_payment.s_magento_sales_order_payment_id is not null
        and s_magento_sales_order_payment.dv_hash <> #s_magento_sales_order_payment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_order_payment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_order_payment @current_dv_batch_id

end
