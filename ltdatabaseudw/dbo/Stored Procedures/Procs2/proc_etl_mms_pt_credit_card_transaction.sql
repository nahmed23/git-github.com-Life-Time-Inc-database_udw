CREATE PROC [dbo].[proc_etl_mms_pt_credit_card_transaction] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PTCreditCardTransaction

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PTCreditCardTransaction (
       bk_hash,
       PTCreditCardTransactionID,
       PTCreditCardBatchID,
       TranSequenceNumber,
       TransactionCode,
       EntryDataSource,
       AccountNumber,
       ExpirationDate,
       TranAmount,
       ReferenceCode,
       TipAmount,
       EmployeeID,
       MemberID,
       CardHolderStreetAddress,
       CardHolderZipCode,
       TransactionDateTime,
       UTCTransactionDateTime,
       TransactionDateTimeZone,
       TransactionAmountChangedFlag,
       IndustryCode,
       AuthorizationNetWorkID,
       AuthorizationSource,
       AuthorizationCode,
       AuthorizationResponseMessage,
       CardType,
       VoidedFlag,
       CardOnFileFlag,
       InsertedDateTime,
       MaskedAccountNumber,
       UpdatedDateTime,
       MaskedAccountNumber64,
       PaymentID,
       CardHolderName,
       TypeIndicator,
       ThirdPartyPOSPaymentID,
       PrepaidTransactionIndicator,
       ECommerceGoodsIndicator,
       POSRetrievalReferenceNumber,
       RequestedAmount,
       PrepaidCardBalance,
       InvoiceNumber,
       SalesTaxAmount,
       CardSubType,
       HbcPaymentFlag,
       Signature,
       EFTAccountFlag,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PTCreditCardTransactionID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PTCreditCardTransactionID,
       PTCreditCardBatchID,
       TranSequenceNumber,
       TransactionCode,
       EntryDataSource,
       AccountNumber,
       ExpirationDate,
       TranAmount,
       ReferenceCode,
       TipAmount,
       EmployeeID,
       MemberID,
       CardHolderStreetAddress,
       CardHolderZipCode,
       TransactionDateTime,
       UTCTransactionDateTime,
       TransactionDateTimeZone,
       TransactionAmountChangedFlag,
       IndustryCode,
       AuthorizationNetWorkID,
       AuthorizationSource,
       AuthorizationCode,
       AuthorizationResponseMessage,
       CardType,
       VoidedFlag,
       CardOnFileFlag,
       InsertedDateTime,
       MaskedAccountNumber,
       UpdatedDateTime,
       MaskedAccountNumber64,
       PaymentID,
       CardHolderName,
       TypeIndicator,
       ThirdPartyPOSPaymentID,
       PrepaidTransactionIndicator,
       ECommerceGoodsIndicator,
       POSRetrievalReferenceNumber,
       RequestedAmount,
       PrepaidCardBalance,
       InvoiceNumber,
       SalesTaxAmount,
       CardSubType,
       HbcPaymentFlag,
       Signature,
       EFTAccountFlag,
       isnull(cast(stage_mms_PTCreditCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PTCreditCardTransaction
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pt_credit_card_transaction @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pt_credit_card_transaction (
       bk_hash,
       pt_credit_card_transaction_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PTCreditCardTransaction.bk_hash,
       stage_hash_mms_PTCreditCardTransaction.PTCreditCardTransactionID pt_credit_card_transaction_id,
       isnull(cast(stage_hash_mms_PTCreditCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PTCreditCardTransaction
  left join h_mms_pt_credit_card_transaction
    on stage_hash_mms_PTCreditCardTransaction.bk_hash = h_mms_pt_credit_card_transaction.bk_hash
 where h_mms_pt_credit_card_transaction_id is null
   and stage_hash_mms_PTCreditCardTransaction.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pt_credit_card_transaction
if object_id('tempdb..#l_mms_pt_credit_card_transaction_inserts') is not null drop table #l_mms_pt_credit_card_transaction_inserts
create table #l_mms_pt_credit_card_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardTransaction.bk_hash,
       stage_hash_mms_PTCreditCardTransaction.PTCreditCardTransactionID pt_credit_card_transaction_id,
       stage_hash_mms_PTCreditCardTransaction.PTCreditCardBatchID ptcreditcardbatchid,
       stage_hash_mms_PTCreditCardTransaction.EmployeeID employee_id,
       stage_hash_mms_PTCreditCardTransaction.MemberID member_id,
       stage_hash_mms_PTCreditCardTransaction.AuthorizationNetWorkID authorization_network_id,
       stage_hash_mms_PTCreditCardTransaction.PaymentID payment_id,
       stage_hash_mms_PTCreditCardTransaction.ThirdPartyPOSPaymentID third_party_pos_payment_id,
       isnull(cast(stage_hash_mms_PTCreditCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.PTCreditCardTransactionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.PTCreditCardBatchID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.EmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.AuthorizationNetWorkID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.PaymentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.ThirdPartyPOSPaymentID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardTransaction
 where stage_hash_mms_PTCreditCardTransaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pt_credit_card_transaction records
set @insert_date_time = getdate()
insert into l_mms_pt_credit_card_transaction (
       bk_hash,
       pt_credit_card_transaction_id,
       ptcreditcardbatchid,
       employee_id,
       member_id,
       authorization_network_id,
       payment_id,
       third_party_pos_payment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pt_credit_card_transaction_inserts.bk_hash,
       #l_mms_pt_credit_card_transaction_inserts.pt_credit_card_transaction_id,
       #l_mms_pt_credit_card_transaction_inserts.ptcreditcardbatchid,
       #l_mms_pt_credit_card_transaction_inserts.employee_id,
       #l_mms_pt_credit_card_transaction_inserts.member_id,
       #l_mms_pt_credit_card_transaction_inserts.authorization_network_id,
       #l_mms_pt_credit_card_transaction_inserts.payment_id,
       #l_mms_pt_credit_card_transaction_inserts.third_party_pos_payment_id,
       case when l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id is null then isnull(#l_mms_pt_credit_card_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pt_credit_card_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pt_credit_card_transaction_inserts
  left join p_mms_pt_credit_card_transaction
    on #l_mms_pt_credit_card_transaction_inserts.bk_hash = p_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pt_credit_card_transaction
    on p_mms_pt_credit_card_transaction.bk_hash = l_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id = l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id
 where l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id is null
    or (l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id is not null
        and l_mms_pt_credit_card_transaction.dv_hash <> #l_mms_pt_credit_card_transaction_inserts.source_hash)

--calculate hash and lookup to current s_mms_pt_credit_card_transaction
if object_id('tempdb..#s_mms_pt_credit_card_transaction_inserts') is not null drop table #s_mms_pt_credit_card_transaction_inserts
create table #s_mms_pt_credit_card_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardTransaction.bk_hash,
       stage_hash_mms_PTCreditCardTransaction.PTCreditCardTransactionID pt_credit_card_transaction_id,
       stage_hash_mms_PTCreditCardTransaction.TranSequenceNumber tran_sequence_number,
       stage_hash_mms_PTCreditCardTransaction.TransactionCode transaction_code,
       stage_hash_mms_PTCreditCardTransaction.EntryDataSource entry_data_source,
       stage_hash_mms_PTCreditCardTransaction.AccountNumber account_number,
       stage_hash_mms_PTCreditCardTransaction.ExpirationDate expiration_date,
       stage_hash_mms_PTCreditCardTransaction.TranAmount tran_amount,
       stage_hash_mms_PTCreditCardTransaction.ReferenceCode reference_code,
       stage_hash_mms_PTCreditCardTransaction.TipAmount tip_amount,
       stage_hash_mms_PTCreditCardTransaction.CardHolderStreetAddress card_holder_street_address,
       stage_hash_mms_PTCreditCardTransaction.CardHolderZipCode card_holder_zip_code,
       stage_hash_mms_PTCreditCardTransaction.TransactionDateTime transaction_date_time,
       stage_hash_mms_PTCreditCardTransaction.UTCTransactionDateTime utc_transaction_date_time,
       stage_hash_mms_PTCreditCardTransaction.TransactionDateTimeZone transaction_date_time_zone,
       stage_hash_mms_PTCreditCardTransaction.TransactionAmountChangedFlag transaction_amount_changed_flag,
       stage_hash_mms_PTCreditCardTransaction.IndustryCode industry_code,
       stage_hash_mms_PTCreditCardTransaction.AuthorizationSource authorization_source,
       stage_hash_mms_PTCreditCardTransaction.AuthorizationCode authorization_code,
       stage_hash_mms_PTCreditCardTransaction.AuthorizationResponseMessage authorization_response_message,
       stage_hash_mms_PTCreditCardTransaction.CardType card_type,
       stage_hash_mms_PTCreditCardTransaction.VoidedFlag voided_flag,
       stage_hash_mms_PTCreditCardTransaction.CardOnFileFlag card_on_file_flag,
       stage_hash_mms_PTCreditCardTransaction.InsertedDateTime inserted_date_time,
       stage_hash_mms_PTCreditCardTransaction.MaskedAccountNumber masked_account_number,
       stage_hash_mms_PTCreditCardTransaction.UpdatedDateTime updated_date_time,
       stage_hash_mms_PTCreditCardTransaction.MaskedAccountNumber64 masked_account_number_6_4,
       stage_hash_mms_PTCreditCardTransaction.CardHolderName card_holder_name,
       stage_hash_mms_PTCreditCardTransaction.TypeIndicator type_indicator,
       stage_hash_mms_PTCreditCardTransaction.PrepaidTransactionIndicator prepaid_transaction_indicator,
       stage_hash_mms_PTCreditCardTransaction.ECommerceGoodsIndicator ecommerce_goods_indicator,
       stage_hash_mms_PTCreditCardTransaction.POSRetrievalReferenceNumber pos_retrieval_reference_number,
       stage_hash_mms_PTCreditCardTransaction.RequestedAmount requested_amount,
       stage_hash_mms_PTCreditCardTransaction.PrepaidCardBalance prepaid_card_balance,
       stage_hash_mms_PTCreditCardTransaction.InvoiceNumber invoice_number,
       stage_hash_mms_PTCreditCardTransaction.SalesTaxAmount sales_tax_amount,
       stage_hash_mms_PTCreditCardTransaction.CardSubType card_sub_type,
       stage_hash_mms_PTCreditCardTransaction.HbcPaymentFlag hbc_payment_flag,
       stage_hash_mms_PTCreditCardTransaction.Signature signature,
       stage_hash_mms_PTCreditCardTransaction.EFTAccountFlag eft_account_flag,
       isnull(cast(stage_hash_mms_PTCreditCardTransaction.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.PTCreditCardTransactionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TranSequenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TransactionCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.EntryDataSource as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.AccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTransaction.ExpirationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TranAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.ReferenceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TipAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.CardHolderStreetAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.CardHolderZipCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTransaction.TransactionDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTransaction.UTCTransactionDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.TransactionDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TransactionAmountChangedFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.IndustryCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.AuthorizationSource,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.AuthorizationCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.AuthorizationResponseMessage,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.CardType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.VoidedFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.CardOnFileFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTransaction.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.MaskedAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTransaction.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.MaskedAccountNumber64,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.CardHolderName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.TypeIndicator as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.PrepaidTransactionIndicator,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.ECommerceGoodsIndicator,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.POSRetrievalReferenceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.RequestedAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.PrepaidCardBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.InvoiceNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.SalesTaxAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.CardSubType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.HbcPaymentFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTransaction.Signature,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTransaction.EFTAccountFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardTransaction
 where stage_hash_mms_PTCreditCardTransaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pt_credit_card_transaction records
set @insert_date_time = getdate()
insert into s_mms_pt_credit_card_transaction (
       bk_hash,
       pt_credit_card_transaction_id,
       tran_sequence_number,
       transaction_code,
       entry_data_source,
       account_number,
       expiration_date,
       tran_amount,
       reference_code,
       tip_amount,
       card_holder_street_address,
       card_holder_zip_code,
       transaction_date_time,
       utc_transaction_date_time,
       transaction_date_time_zone,
       transaction_amount_changed_flag,
       industry_code,
       authorization_source,
       authorization_code,
       authorization_response_message,
       card_type,
       voided_flag,
       card_on_file_flag,
       inserted_date_time,
       masked_account_number,
       updated_date_time,
       masked_account_number_6_4,
       card_holder_name,
       type_indicator,
       prepaid_transaction_indicator,
       ecommerce_goods_indicator,
       pos_retrieval_reference_number,
       requested_amount,
       prepaid_card_balance,
       invoice_number,
       sales_tax_amount,
       card_sub_type,
       hbc_payment_flag,
       signature,
       eft_account_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pt_credit_card_transaction_inserts.bk_hash,
       #s_mms_pt_credit_card_transaction_inserts.pt_credit_card_transaction_id,
       #s_mms_pt_credit_card_transaction_inserts.tran_sequence_number,
       #s_mms_pt_credit_card_transaction_inserts.transaction_code,
       #s_mms_pt_credit_card_transaction_inserts.entry_data_source,
       #s_mms_pt_credit_card_transaction_inserts.account_number,
       #s_mms_pt_credit_card_transaction_inserts.expiration_date,
       #s_mms_pt_credit_card_transaction_inserts.tran_amount,
       #s_mms_pt_credit_card_transaction_inserts.reference_code,
       #s_mms_pt_credit_card_transaction_inserts.tip_amount,
       #s_mms_pt_credit_card_transaction_inserts.card_holder_street_address,
       #s_mms_pt_credit_card_transaction_inserts.card_holder_zip_code,
       #s_mms_pt_credit_card_transaction_inserts.transaction_date_time,
       #s_mms_pt_credit_card_transaction_inserts.utc_transaction_date_time,
       #s_mms_pt_credit_card_transaction_inserts.transaction_date_time_zone,
       #s_mms_pt_credit_card_transaction_inserts.transaction_amount_changed_flag,
       #s_mms_pt_credit_card_transaction_inserts.industry_code,
       #s_mms_pt_credit_card_transaction_inserts.authorization_source,
       #s_mms_pt_credit_card_transaction_inserts.authorization_code,
       #s_mms_pt_credit_card_transaction_inserts.authorization_response_message,
       #s_mms_pt_credit_card_transaction_inserts.card_type,
       #s_mms_pt_credit_card_transaction_inserts.voided_flag,
       #s_mms_pt_credit_card_transaction_inserts.card_on_file_flag,
       #s_mms_pt_credit_card_transaction_inserts.inserted_date_time,
       #s_mms_pt_credit_card_transaction_inserts.masked_account_number,
       #s_mms_pt_credit_card_transaction_inserts.updated_date_time,
       #s_mms_pt_credit_card_transaction_inserts.masked_account_number_6_4,
       #s_mms_pt_credit_card_transaction_inserts.card_holder_name,
       #s_mms_pt_credit_card_transaction_inserts.type_indicator,
       #s_mms_pt_credit_card_transaction_inserts.prepaid_transaction_indicator,
       #s_mms_pt_credit_card_transaction_inserts.ecommerce_goods_indicator,
       #s_mms_pt_credit_card_transaction_inserts.pos_retrieval_reference_number,
       #s_mms_pt_credit_card_transaction_inserts.requested_amount,
       #s_mms_pt_credit_card_transaction_inserts.prepaid_card_balance,
       #s_mms_pt_credit_card_transaction_inserts.invoice_number,
       #s_mms_pt_credit_card_transaction_inserts.sales_tax_amount,
       #s_mms_pt_credit_card_transaction_inserts.card_sub_type,
       #s_mms_pt_credit_card_transaction_inserts.hbc_payment_flag,
       #s_mms_pt_credit_card_transaction_inserts.signature,
       #s_mms_pt_credit_card_transaction_inserts.eft_account_flag,
       case when s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id is null then isnull(#s_mms_pt_credit_card_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pt_credit_card_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pt_credit_card_transaction_inserts
  left join p_mms_pt_credit_card_transaction
    on #s_mms_pt_credit_card_transaction_inserts.bk_hash = p_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pt_credit_card_transaction
    on p_mms_pt_credit_card_transaction.bk_hash = s_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id = s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id
 where s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id is null
    or (s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id is not null
        and s_mms_pt_credit_card_transaction.dv_hash <> #s_mms_pt_credit_card_transaction_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pt_credit_card_transaction @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_pt_credit_card_transaction @current_dv_batch_id

end
