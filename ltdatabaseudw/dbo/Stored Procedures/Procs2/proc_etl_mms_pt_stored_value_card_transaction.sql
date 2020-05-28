CREATE PROC [dbo].[proc_etl_mms_pt_stored_value_card_transaction] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PTStoredValueCardTransaction

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PTStoredValueCardTransaction (
       bk_hash,
       PTStoredValueCardTransactionID,
       PaymentID,
       TranSequenceNumber,
       TransactionCode,
       EntryDataSource,
       PINCapabilityCode,
       MaskedAccountNumber,
       ExpirationDate,
       TranAmount,
       CounterTipAmount,
       PriorApprovedAuthCode,
       CashOutYN,
       PartialRedemptionYN,
       IssuanceCardSequenceNumber,
       IssuanceNCards,
       EmployeeID,
       TipLineParentSVCardTransactionID,
       ResponseActionCode,
       ResponseAuthorizationCode,
       ResponseRetrievalReferenceNumber,
       ResponseMessage,
       ResponseTraceNumber,
       ResponseAuthorizingNetworkID,
       ResponseAuthorizationSource,
       ResponseSVBalanceAmount,
       ResponseSVPreviousBalanceAmount,
       ResponseApprovedAmount,
       ResponseCashOutAmount,
       SVBatchNumber,
       TransactionDateTime,
       UTCTransactionDateTime,
       TransactionDateTimezone,
       InsertedDateTime,
       PTCreditCardBatchID,
       RetrievalReferenceNumber,
       VoidedFlag,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PTStoredValueCardTransactionID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PTStoredValueCardTransactionID,
       PaymentID,
       TranSequenceNumber,
       TransactionCode,
       EntryDataSource,
       PINCapabilityCode,
       MaskedAccountNumber,
       ExpirationDate,
       TranAmount,
       CounterTipAmount,
       PriorApprovedAuthCode,
       CashOutYN,
       PartialRedemptionYN,
       IssuanceCardSequenceNumber,
       IssuanceNCards,
       EmployeeID,
       TipLineParentSVCardTransactionID,
       ResponseActionCode,
       ResponseAuthorizationCode,
       ResponseRetrievalReferenceNumber,
       ResponseMessage,
       ResponseTraceNumber,
       ResponseAuthorizingNetworkID,
       ResponseAuthorizationSource,
       ResponseSVBalanceAmount,
       ResponseSVPreviousBalanceAmount,
       ResponseApprovedAmount,
       ResponseCashOutAmount,
       SVBatchNumber,
       TransactionDateTime,
       UTCTransactionDateTime,
       TransactionDateTimezone,
       InsertedDateTime,
       PTCreditCardBatchID,
       RetrievalReferenceNumber,
       VoidedFlag,
       UpdatedDateTime,
       isnull(cast(stage_mms_PTStoredValueCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PTStoredValueCardTransaction
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pt_stored_value_card_transaction @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pt_stored_value_card_transaction (
       bk_hash,
       pt_stored_value_card_transaction_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PTStoredValueCardTransaction.bk_hash,
       stage_hash_mms_PTStoredValueCardTransaction.PTStoredValueCardTransactionID pt_stored_value_card_transaction_id,
       isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PTStoredValueCardTransaction
  left join h_mms_pt_stored_value_card_transaction
    on stage_hash_mms_PTStoredValueCardTransaction.bk_hash = h_mms_pt_stored_value_card_transaction.bk_hash
 where h_mms_pt_stored_value_card_transaction_id is null
   and stage_hash_mms_PTStoredValueCardTransaction.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pt_stored_value_card_transaction
if object_id('tempdb..#l_mms_pt_stored_value_card_transaction_inserts') is not null drop table #l_mms_pt_stored_value_card_transaction_inserts
create table #l_mms_pt_stored_value_card_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTStoredValueCardTransaction.bk_hash,
       stage_hash_mms_PTStoredValueCardTransaction.PTStoredValueCardTransactionID pt_stored_value_card_transaction_id,
       stage_hash_mms_PTStoredValueCardTransaction.PaymentID payment_id,
       stage_hash_mms_PTStoredValueCardTransaction.EmployeeID employee_id,
       stage_hash_mms_PTStoredValueCardTransaction.TipLineParentSVCardTransactionID tip_line_parent_sv_card_transaction_id,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizingNetworkID response_authorizing_network_id,
       stage_hash_mms_PTStoredValueCardTransaction.PTCreditCardBatchID pt_credit_card_batch_id,
       stage_hash_mms_PTStoredValueCardTransaction.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.PTStoredValueCardTransactionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.PaymentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.EmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.TipLineParentSVCardTransactionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizingNetworkID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.PTCreditCardBatchID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTStoredValueCardTransaction
 where stage_hash_mms_PTStoredValueCardTransaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pt_stored_value_card_transaction records
set @insert_date_time = getdate()
insert into l_mms_pt_stored_value_card_transaction (
       bk_hash,
       pt_stored_value_card_transaction_id,
       payment_id,
       employee_id,
       tip_line_parent_sv_card_transaction_id,
       response_authorizing_network_id,
       pt_credit_card_batch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pt_stored_value_card_transaction_inserts.bk_hash,
       #l_mms_pt_stored_value_card_transaction_inserts.pt_stored_value_card_transaction_id,
       #l_mms_pt_stored_value_card_transaction_inserts.payment_id,
       #l_mms_pt_stored_value_card_transaction_inserts.employee_id,
       #l_mms_pt_stored_value_card_transaction_inserts.tip_line_parent_sv_card_transaction_id,
       #l_mms_pt_stored_value_card_transaction_inserts.response_authorizing_network_id,
       #l_mms_pt_stored_value_card_transaction_inserts.pt_credit_card_batch_id,
       case when l_mms_pt_stored_value_card_transaction.l_mms_pt_stored_value_card_transaction_id is null then isnull(#l_mms_pt_stored_value_card_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pt_stored_value_card_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pt_stored_value_card_transaction_inserts
  left join p_mms_pt_stored_value_card_transaction
    on #l_mms_pt_stored_value_card_transaction_inserts.bk_hash = p_mms_pt_stored_value_card_transaction.bk_hash
   and p_mms_pt_stored_value_card_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pt_stored_value_card_transaction
    on p_mms_pt_stored_value_card_transaction.bk_hash = l_mms_pt_stored_value_card_transaction.bk_hash
   and p_mms_pt_stored_value_card_transaction.l_mms_pt_stored_value_card_transaction_id = l_mms_pt_stored_value_card_transaction.l_mms_pt_stored_value_card_transaction_id
 where l_mms_pt_stored_value_card_transaction.l_mms_pt_stored_value_card_transaction_id is null
    or (l_mms_pt_stored_value_card_transaction.l_mms_pt_stored_value_card_transaction_id is not null
        and l_mms_pt_stored_value_card_transaction.dv_hash <> #l_mms_pt_stored_value_card_transaction_inserts.source_hash)

--calculate hash and lookup to current s_mms_pt_stored_value_card_transaction
if object_id('tempdb..#s_mms_pt_stored_value_card_transaction_inserts') is not null drop table #s_mms_pt_stored_value_card_transaction_inserts
create table #s_mms_pt_stored_value_card_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTStoredValueCardTransaction.bk_hash,
       stage_hash_mms_PTStoredValueCardTransaction.PTStoredValueCardTransactionID pt_stored_value_card_transaction_id,
       stage_hash_mms_PTStoredValueCardTransaction.TranSequenceNumber tran_sequence_number,
       stage_hash_mms_PTStoredValueCardTransaction.TransactionCode transaction_code,
       stage_hash_mms_PTStoredValueCardTransaction.EntryDataSource entry_data_source,
       stage_hash_mms_PTStoredValueCardTransaction.PINCapabilityCode pin_capability_code,
       stage_hash_mms_PTStoredValueCardTransaction.MaskedAccountNumber masked_account_number,
       stage_hash_mms_PTStoredValueCardTransaction.ExpirationDate expiration_date,
       stage_hash_mms_PTStoredValueCardTransaction.TranAmount tran_amount,
       stage_hash_mms_PTStoredValueCardTransaction.CounterTipAmount counter_tip_amount,
       stage_hash_mms_PTStoredValueCardTransaction.PriorApprovedAuthCode prior_approved_auth_code,
       stage_hash_mms_PTStoredValueCardTransaction.CashOutYN cash_out_yn,
       stage_hash_mms_PTStoredValueCardTransaction.PartialRedemptionYN partial_redemption_yn,
       stage_hash_mms_PTStoredValueCardTransaction.IssuanceCardSequenceNumber issuance_card_sequence_number,
       stage_hash_mms_PTStoredValueCardTransaction.IssuanceNCards issuance_n_cards,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseActionCode response_action_code,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizationCode response_authorization_code,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseRetrievalReferenceNumber response_retrieval_reference_number,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseMessage response_message,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseTraceNumber response_trace_number,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizationSource response_authorization_source,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseSVBalanceAmount response_sv_balance_amount,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseSVPreviousBalanceAmount response_sv_previous_balance_amount,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseApprovedAmount response_approved_amount,
       stage_hash_mms_PTStoredValueCardTransaction.ResponseCashOutAmount response_cash_out_amount,
       stage_hash_mms_PTStoredValueCardTransaction.SVBatchNumber sv_batch_number,
       stage_hash_mms_PTStoredValueCardTransaction.TransactionDateTime transaction_date_time,
       stage_hash_mms_PTStoredValueCardTransaction.UTCTransactionDateTime utc_transaction_date_time,
       stage_hash_mms_PTStoredValueCardTransaction.TransactionDateTimezone transaction_date_timezone,
       stage_hash_mms_PTStoredValueCardTransaction.InsertedDateTime inserted_date_time,
       stage_hash_mms_PTStoredValueCardTransaction.RetrievalReferenceNumber retrieval_reference_number,
       stage_hash_mms_PTStoredValueCardTransaction.VoidedFlag voided_flag,
       stage_hash_mms_PTStoredValueCardTransaction.UpdatedDateTime updated_date_time,
       stage_hash_mms_PTStoredValueCardTransaction.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.PTStoredValueCardTransactionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.TranSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.TransactionCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.EntryDataSource as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.PINCapabilityCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.MaskedAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTStoredValueCardTransaction.ExpirationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.TranAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.CounterTipAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.PriorApprovedAuthCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.CashOutYN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.PartialRedemptionYN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.IssuanceCardSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.IssuanceNCards as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.ResponseActionCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizationCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseRetrievalReferenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.ResponseMessage,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.ResponseTraceNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.ResponseAuthorizationSource,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseSVBalanceAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseSVPreviousBalanceAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseApprovedAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.ResponseCashOutAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.SVBatchNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTStoredValueCardTransaction.TransactionDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTStoredValueCardTransaction.UTCTransactionDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTStoredValueCardTransaction.TransactionDateTimezone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTStoredValueCardTransaction.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.RetrievalReferenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTStoredValueCardTransaction.VoidedFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTStoredValueCardTransaction.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTStoredValueCardTransaction
 where stage_hash_mms_PTStoredValueCardTransaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pt_stored_value_card_transaction records
set @insert_date_time = getdate()
insert into s_mms_pt_stored_value_card_transaction (
       bk_hash,
       pt_stored_value_card_transaction_id,
       tran_sequence_number,
       transaction_code,
       entry_data_source,
       pin_capability_code,
       masked_account_number,
       expiration_date,
       tran_amount,
       counter_tip_amount,
       prior_approved_auth_code,
       cash_out_yn,
       partial_redemption_yn,
       issuance_card_sequence_number,
       issuance_n_cards,
       response_action_code,
       response_authorization_code,
       response_retrieval_reference_number,
       response_message,
       response_trace_number,
       response_authorization_source,
       response_sv_balance_amount,
       response_sv_previous_balance_amount,
       response_approved_amount,
       response_cash_out_amount,
       sv_batch_number,
       transaction_date_time,
       utc_transaction_date_time,
       transaction_date_timezone,
       inserted_date_time,
       retrieval_reference_number,
       voided_flag,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pt_stored_value_card_transaction_inserts.bk_hash,
       #s_mms_pt_stored_value_card_transaction_inserts.pt_stored_value_card_transaction_id,
       #s_mms_pt_stored_value_card_transaction_inserts.tran_sequence_number,
       #s_mms_pt_stored_value_card_transaction_inserts.transaction_code,
       #s_mms_pt_stored_value_card_transaction_inserts.entry_data_source,
       #s_mms_pt_stored_value_card_transaction_inserts.pin_capability_code,
       #s_mms_pt_stored_value_card_transaction_inserts.masked_account_number,
       #s_mms_pt_stored_value_card_transaction_inserts.expiration_date,
       #s_mms_pt_stored_value_card_transaction_inserts.tran_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.counter_tip_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.prior_approved_auth_code,
       #s_mms_pt_stored_value_card_transaction_inserts.cash_out_yn,
       #s_mms_pt_stored_value_card_transaction_inserts.partial_redemption_yn,
       #s_mms_pt_stored_value_card_transaction_inserts.issuance_card_sequence_number,
       #s_mms_pt_stored_value_card_transaction_inserts.issuance_n_cards,
       #s_mms_pt_stored_value_card_transaction_inserts.response_action_code,
       #s_mms_pt_stored_value_card_transaction_inserts.response_authorization_code,
       #s_mms_pt_stored_value_card_transaction_inserts.response_retrieval_reference_number,
       #s_mms_pt_stored_value_card_transaction_inserts.response_message,
       #s_mms_pt_stored_value_card_transaction_inserts.response_trace_number,
       #s_mms_pt_stored_value_card_transaction_inserts.response_authorization_source,
       #s_mms_pt_stored_value_card_transaction_inserts.response_sv_balance_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.response_sv_previous_balance_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.response_approved_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.response_cash_out_amount,
       #s_mms_pt_stored_value_card_transaction_inserts.sv_batch_number,
       #s_mms_pt_stored_value_card_transaction_inserts.transaction_date_time,
       #s_mms_pt_stored_value_card_transaction_inserts.utc_transaction_date_time,
       #s_mms_pt_stored_value_card_transaction_inserts.transaction_date_timezone,
       #s_mms_pt_stored_value_card_transaction_inserts.inserted_date_time,
       #s_mms_pt_stored_value_card_transaction_inserts.retrieval_reference_number,
       #s_mms_pt_stored_value_card_transaction_inserts.voided_flag,
       #s_mms_pt_stored_value_card_transaction_inserts.updated_date_time,
       case when s_mms_pt_stored_value_card_transaction.s_mms_pt_stored_value_card_transaction_id is null then isnull(#s_mms_pt_stored_value_card_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pt_stored_value_card_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pt_stored_value_card_transaction_inserts
  left join p_mms_pt_stored_value_card_transaction
    on #s_mms_pt_stored_value_card_transaction_inserts.bk_hash = p_mms_pt_stored_value_card_transaction.bk_hash
   and p_mms_pt_stored_value_card_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pt_stored_value_card_transaction
    on p_mms_pt_stored_value_card_transaction.bk_hash = s_mms_pt_stored_value_card_transaction.bk_hash
   and p_mms_pt_stored_value_card_transaction.s_mms_pt_stored_value_card_transaction_id = s_mms_pt_stored_value_card_transaction.s_mms_pt_stored_value_card_transaction_id
 where s_mms_pt_stored_value_card_transaction.s_mms_pt_stored_value_card_transaction_id is null
    or (s_mms_pt_stored_value_card_transaction.s_mms_pt_stored_value_card_transaction_id is not null
        and s_mms_pt_stored_value_card_transaction.dv_hash <> #s_mms_pt_stored_value_card_transaction_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pt_stored_value_card_transaction @current_dv_batch_id

end
