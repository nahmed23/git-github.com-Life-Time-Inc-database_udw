CREATE PROC [dbo].[proc_fact_mms_credit_card_offline_transaction] @dv_batch_id [varchar](500) AS
begin
 
set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       -1 as current_dv_batch_id
  from dbo.fact_mms_credit_card_offline_transaction
 
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
--For a dimension record, the complete record needs to be rebuilt for a change in any field in any of the participating tables, Hence:
-----STEP 1: Collecting all columns from the base table and other tables
-- that are corresponding to the changed Recs from all the participating tables & itself
--Reason being - This particular fact has same columns coming from different driving tables
--Below collection of values are done as per source qualifier query
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if object_id('tempdb..#fact_mms_credit_card_offline_transaction') is not null drop table #fact_mms_credit_card_offline_transaction
create table dbo.#fact_mms_credit_card_offline_transaction with(distribution=hash(fact_mms_credit_card_offline_transaction_key), location=user_db, heap) as

--p_mms_third_party_pos_payment(to collect base records)

      select p_mms_third_party_pos_payment.bk_hash fact_mms_credit_card_offline_transaction_key,
	  p_mms_third_party_pos_payment.third_party_pos_payment_id third_party_pos_payment_id,
	  null pt_credit_card_transaction_id,
	  null pt_credit_card_rejected_transaction_id,
	  null pt_credit_card_undeliverable_transaction_id,
	  null offline_auth_flag,
	  null terminal_area_id,
	  null club_id,
	  null member_id,
	  null pos_tran_date_time,
	  null rejected_transaction_date_time,
	  null card_type,
	  null val_payment_status_id,
	  null pos_unique_tran_id,
	  null rejected_error_message,
	  null card_on_file_flag,
	  null masked_account_number,
	  null transaction_amount,
	  null pt_credit_card_terminal_id,
	  null currency_code,
	  p_mms_third_party_pos_payment.dv_load_date_time dv_load_date_time,
	  p_mms_third_party_pos_payment.dv_load_end_date_time dv_load_end_date_time,
	  p_mms_third_party_pos_payment.dv_batch_id dv_batch_id
	  from p_mms_third_party_pos_payment
	  join l_mms_third_party_pos_payment
	  on p_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id = l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id
	  join #dv_batch_id
	     
	  on p_mms_third_party_pos_payment.dv_batch_id > #dv_batch_id.max_dv_batch_id
	  or p_mms_third_party_pos_payment.dv_batch_id = #dv_batch_id.current_dv_batch_id
	  where p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
	  and  p_mms_third_party_pos_payment.bk_hash in ('-999','-998','-997')

-- Changes to MMSThirdPartyPOSPaymenent related to MMSPTCreditCardRejectedTransaction
	  union
	  select p_mms_third_party_pos_payment.bk_hash fact_mms_credit_card_offline_transaction_key,
	  p_mms_third_party_pos_payment.third_party_pos_payment_id third_party_pos_payment_id,
	  null pt_credit_card_transaction_id,
	  p_mms_pt_credit_card_rejected_transaction.pt_credit_card_rejected_transaction_id pt_credit_card_rejected_transaction_id,
	  null pt_credit_card_undeliverable_transaction_id,
	  s_mms_third_party_pos_payment.offline_auth_flag offline_auth_flag,
	  l_mms_pt_credit_card_terminal.terminal_area_id terminal_area_id,
	  l_mms_pt_credit_card_terminal.club_id club_id,
	  l_mms_pt_credit_card_rejected_transaction.member_id member_id,
	  s_mms_third_party_pos_payment.pos_tran_date_time pos_tran_date_time,
	  s_mms_pt_credit_card_rejected_transaction.transaction_date_time rejected_transaction_date_time,
	  s_mms_pt_credit_card_rejected_transaction.card_type card_type,
	  l_mms_third_party_pos_payment.val_payment_status_id val_payment_status_id,
	  l_mms_third_party_pos_payment.pos_unique_tran_id pos_unique_tran_id,
	  s_mms_pt_credit_card_rejected_transaction.error_message rejected_error_message,
	  s_mms_pt_credit_card_rejected_transaction.card_on_file_flag card_on_file_flag,
	  s_mms_pt_credit_card_rejected_transaction.masked_account_number masked_account_number,
	  s_mms_pt_credit_card_rejected_transaction.tran_amount transaction_amount,
	  p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id pt_credit_card_terminal_id,
	  isnull(r_mms_val_currency_code.currency_code,'USD') original_currency_code, 
	  case when p_mms_pt_credit_card_rejected_transaction.dv_load_date_time>p_mms_third_party_pos_payment.dv_load_date_time then p_mms_pt_credit_card_rejected_transaction.dv_load_date_time
	  else p_mms_third_party_pos_payment.dv_load_date_time
      end dv_load_date_time,
	  case when p_mms_pt_credit_card_rejected_transaction.dv_load_end_date_time>p_mms_third_party_pos_payment.dv_load_end_date_time then p_mms_pt_credit_card_rejected_transaction.dv_load_end_date_time
	  else p_mms_third_party_pos_payment.dv_load_end_date_time
      end dv_load_end_date_time,
	  case when p_mms_pt_credit_card_rejected_transaction.dv_batch_id>p_mms_third_party_pos_payment.dv_batch_id then p_mms_pt_credit_card_rejected_transaction.dv_batch_id
	  else p_mms_third_party_pos_payment.dv_batch_id
      end dv_batch_id	  
	  from p_mms_pt_credit_card_rejected_transaction
	  join l_mms_pt_credit_card_rejected_transaction
	  on p_mms_pt_credit_card_rejected_transaction.l_mms_pt_credit_card_rejected_transaction_id = l_mms_pt_credit_card_rejected_transaction.l_mms_pt_credit_card_rejected_transaction_id
	  join s_mms_pt_credit_card_rejected_transaction
	  on p_mms_pt_credit_card_rejected_transaction.s_mms_pt_credit_card_rejected_transaction_id = s_mms_pt_credit_card_rejected_transaction.s_mms_pt_credit_card_rejected_transaction_id 
	  join p_mms_pt_credit_card_terminal
	  on l_mms_pt_credit_card_rejected_transaction.pt_credit_card_terminal_id = p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id
	  join l_mms_pt_credit_card_terminal
	  on p_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id = l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id
	  join p_mms_third_party_pos_payment
	  on l_mms_pt_credit_card_rejected_transaction.third_party_pos_payment_id = p_mms_third_party_pos_payment.third_party_pos_payment_id
	  join l_mms_third_party_pos_payment
	  on p_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id = l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id
	  join s_mms_third_party_pos_payment
	  on s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id = p_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id
	  join l_mms_club_merchant_number
	  on l_mms_club_merchant_number.merchant_number = l_mms_pt_credit_card_terminal.merchant_number
	  join p_mms_club_merchant_number
	  on p_mms_club_merchant_number.l_mms_club_merchant_number_id = p_mms_club_merchant_number.l_mms_club_merchant_number_id
	  join r_mms_val_currency_code
	  on l_mms_club_merchant_number.val_currency_code_id = r_mms_val_currency_code.val_currency_code_id
	  join #dv_batch_id
	  on p_mms_pt_credit_card_rejected_transaction.dv_batch_id > #dv_batch_id.max_dv_batch_id
	  or p_mms_pt_credit_card_rejected_transaction.dv_batch_id = #dv_batch_id.current_dv_batch_id
	  where p_mms_pt_credit_card_rejected_transaction.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_pt_credit_card_terminal.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_club_merchant_number.dv_load_end_date_time = 'Dec 31, 9999'
	  and r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999'
	  
	  union


	
			--Changes to MMSThirdPartyPOSPaymenent related to MMSPTCreditCardTransaction ---542
		
	select p_mms_third_party_pos_payment.bk_hash fact_mms_credit_card_offline_transaction_key,
	  p_mms_third_party_pos_payment.third_party_pos_payment_id third_party_pos_payment_id,
	  p_mms_pt_credit_card_transaction.pt_credit_card_transaction_id pt_credit_card_transaction_id,
	  null pt_credit_card_rejected_transaction_id,
	  null pt_credit_card_undeliverable_transaction_id,
	  s_mms_third_party_pos_payment.offline_auth_flag offline_auth_flag,
	  l_mms_pt_credit_card_terminal.terminal_area_id terminal_area_id,
	  l_mms_pt_credit_card_terminal.club_id club_id,
	  l_mms_pt_credit_card_transaction.member_id member_id,
	  s_mms_third_party_pos_payment.pos_tran_date_time pos_tran_date_time,
	  null rejected_transaction_date_time,
	  s_mms_pt_credit_card_transaction.card_type card_type,
	  l_mms_third_party_pos_payment.val_payment_status_id val_payment_status_id,
	  l_mms_third_party_pos_payment.pos_unique_tran_id pos_unique_tran_id,
	  null rejected_error_message,
	  s_mms_pt_credit_card_transaction.card_on_file_flag card_on_file_flag,
	  s_mms_pt_credit_card_transaction.masked_account_number masked_account_number,
	  s_mms_pt_credit_card_transaction.tran_amount transaction_amount,
	  p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id pt_credit_card_terminal_id,
	  isnull(r_mms_val_currency_code.currency_code,'USD')  original_currency_code,
	  case when p_mms_pt_credit_card_transaction.dv_load_date_time>p_mms_third_party_pos_payment.dv_load_date_time then p_mms_pt_credit_card_transaction.dv_load_date_time
	  else p_mms_third_party_pos_payment.dv_load_date_time
      end dv_load_date_time,
	  case when p_mms_pt_credit_card_transaction.dv_load_end_date_time>p_mms_third_party_pos_payment.dv_load_end_date_time then p_mms_pt_credit_card_transaction.dv_load_end_date_time
	  else p_mms_third_party_pos_payment.dv_load_end_date_time
      end dv_load_end_date_time,
	  case when p_mms_pt_credit_card_transaction.dv_batch_id>p_mms_third_party_pos_payment.dv_batch_id then p_mms_pt_credit_card_transaction.dv_batch_id
	  else p_mms_third_party_pos_payment.dv_batch_id
      end dv_batch_id	
	from p_mms_pt_credit_card_transaction
	join l_mms_pt_credit_card_transaction
	on p_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id = l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id
	join s_mms_pt_credit_card_transaction
	on p_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id  = s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id
	join l_mms_pt_credit_card_batch
	on l_mms_pt_credit_card_transaction.ptcreditcardbatchid = l_mms_pt_credit_card_batch.pt_credit_card_batch_id
	join p_mms_pt_credit_card_batch
	on l_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id = p_mms_pt_credit_card_batch.l_mms_pt_credit_card_batch_id
	join p_mms_pt_credit_card_terminal
	on l_mms_pt_credit_card_batch.pt_credit_card_terminal_id = p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id
	join l_mms_pt_credit_card_terminal
	on p_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id = l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id
	join p_mms_third_party_pos_payment
	on p_mms_third_party_pos_payment.third_party_pos_payment_id = l_mms_pt_credit_card_transaction.third_party_pos_payment_id
	join s_mms_third_party_pos_payment
	on p_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id = s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id
	join l_mms_third_party_pos_payment
	on p_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id = l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id
	join l_mms_club_merchant_number
	on l_mms_club_merchant_number.merchant_number = l_mms_pt_credit_card_terminal.merchant_number
	join p_mms_club_merchant_number
	on l_mms_club_merchant_number.l_mms_club_merchant_number_id = p_mms_club_merchant_number.l_mms_club_merchant_number_id
	join r_mms_val_currency_code
	on r_mms_val_currency_code.val_currency_code_id = l_mms_club_merchant_number.val_currency_code_id
    join #dv_batch_id
	on p_mms_pt_credit_card_transaction.dv_batch_id > #dv_batch_id.max_dv_batch_id
	or p_mms_pt_credit_card_transaction.dv_batch_id = #dv_batch_id.current_dv_batch_id
	  where p_mms_pt_credit_card_transaction.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_pt_credit_card_batch.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_club_merchant_number.dv_load_end_date_time = 'Dec 31, 9999'
	  and r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999'

	union

	--p_mms_pt_credit_card_undeliverable_transaction
	select p_mms_third_party_pos_payment.bk_hash fact_mms_credit_card_offline_transaction_key,
	  p_mms_third_party_pos_payment.third_party_pos_payment_id third_party_pos_payment_id,
	  null pt_credit_card_transaction_id,
	  null pt_credit_card_rejected_transaction_id,
	  p_mms_pt_credit_card_undeliverable_transaction.pt_credit_card_undeliverable_transaction_id  pt_credit_card_undeliverable_transaction_id,
	  s_mms_third_party_pos_payment.offline_auth_flag offline_auth_flag,
	  l_mms_pt_credit_card_terminal.terminal_area_id terminal_area_id,
	  l_mms_pt_credit_card_terminal.club_id club_id,
	  l_mms_pt_credit_card_undeliverable_transaction.member_id member_id,
	  s_mms_third_party_pos_payment.pos_tran_date_time pos_tran_date_time,
	  null rejected_transaction_date_time,
	  s_mms_pt_credit_card_undeliverable_transaction.card_type card_type,
	  l_mms_third_party_pos_payment.val_payment_status_id val_payment_status_id,
	  l_mms_third_party_pos_payment.pos_unique_tran_id pos_unique_tran_id,
	  null rejected_error_message,
	  s_mms_pt_credit_card_undeliverable_transaction.card_on_file_flag card_on_file_flag,
	  s_mms_pt_credit_card_undeliverable_transaction.masked_account_number masked_account_number,
	  s_mms_pt_credit_card_undeliverable_transaction.tran_amount transaction_amount,
	  l_mms_pt_credit_card_undeliverable_transaction.pt_credit_card_terminal_id pt_credit_card_terminal_id,
	  isnull(r_mms_val_currency_code.currency_code,'USD')  original_currency_code,
	   case when p_mms_pt_credit_card_undeliverable_transaction.dv_load_date_time>p_mms_third_party_pos_payment.dv_load_date_time then p_mms_pt_credit_card_undeliverable_transaction.dv_load_date_time
	  else p_mms_third_party_pos_payment.dv_load_date_time
      end dv_load_date_time,
	  case when p_mms_pt_credit_card_undeliverable_transaction.dv_load_end_date_time>p_mms_third_party_pos_payment.dv_load_end_date_time then p_mms_pt_credit_card_undeliverable_transaction.dv_load_end_date_time
	  else p_mms_third_party_pos_payment.dv_load_end_date_time
      end dv_load_end_date_time,
	  case when p_mms_pt_credit_card_undeliverable_transaction.dv_batch_id>p_mms_third_party_pos_payment.dv_batch_id then p_mms_pt_credit_card_undeliverable_transaction.dv_batch_id
	  else p_mms_third_party_pos_payment.dv_batch_id
      end dv_batch_id	
	from p_mms_pt_credit_card_undeliverable_transaction
	join l_mms_pt_credit_card_undeliverable_transaction
	on p_mms_pt_credit_card_undeliverable_transaction.l_mms_pt_credit_card_undeliverable_transaction_id = l_mms_pt_credit_card_undeliverable_transaction.l_mms_pt_credit_card_undeliverable_transaction_id
	join s_mms_pt_credit_card_undeliverable_transaction
	on p_mms_pt_credit_card_undeliverable_transaction.s_mms_pt_credit_card_undeliverable_transaction_id = s_mms_pt_credit_card_undeliverable_transaction.s_mms_pt_credit_card_undeliverable_transaction_id
	join p_mms_third_party_pos_payment
	on l_mms_pt_credit_card_undeliverable_transaction.third_party_pos_payment_id = p_mms_third_party_pos_payment.third_party_pos_payment_id
	join l_mms_third_party_pos_payment
	on p_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id = l_mms_third_party_pos_payment.l_mms_third_party_pos_payment_id
	join s_mms_third_party_pos_payment
	on p_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id = s_mms_third_party_pos_payment.s_mms_third_party_pos_payment_id
	join p_mms_pt_credit_card_terminal
	on l_mms_pt_credit_card_undeliverable_transaction.pt_credit_card_terminal_id = p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id
	join l_mms_pt_credit_card_terminal
	on p_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id = l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id
	join l_mms_club_merchant_number
	on l_mms_pt_credit_card_terminal.merchant_number = l_mms_club_merchant_number.merchant_number
	join p_mms_club_merchant_number
	on p_mms_club_merchant_number.l_mms_club_merchant_number_id = l_mms_club_merchant_number.l_mms_club_merchant_number_id
	join r_mms_val_currency_code
	on r_mms_val_currency_code.val_currency_code_id = l_mms_club_merchant_number.val_currency_code_id
	join #dv_batch_id
	on p_mms_pt_credit_card_undeliverable_transaction.dv_batch_id > #dv_batch_id.max_dv_batch_id
	or p_mms_pt_credit_card_undeliverable_transaction.dv_batch_id = #dv_batch_id.current_dv_batch_id
	where p_mms_pt_credit_card_undeliverable_transaction.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_third_party_pos_payment.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_pt_credit_card_terminal.dv_load_end_date_time = 'Dec 31, 9999'
	  and p_mms_club_merchant_number.dv_load_end_date_time = 'Dec 31, 9999'
	  and r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999'

----------------END of collection of all columns and related changes that occured--------------------
	
---------Joining with r_date table to fetch values for specific date columns-----------	
	 
    if object_id('tempdb..#fact_mms_credit_card_offline_transaction1') is not null drop table #fact_mms_credit_card_offline_transaction1
    create table dbo.#fact_mms_credit_card_offline_transaction1 with(distribution=hash(fact_mms_credit_card_offline_transaction_key), location=user_db, heap) as
	select 
	     #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key fact_mms_credit_card_offline_transaction_key,
		 #fact_mms_credit_card_offline_transaction.third_party_pos_payment_id third_party_pos_payment_id,
		 #fact_mms_credit_card_offline_transaction.pt_credit_card_transaction_id pt_credit_card_transaction_id,
		 #fact_mms_credit_card_offline_transaction.pt_credit_card_rejected_transaction_id pt_credit_card_rejected_transaction_id,
		 #fact_mms_credit_card_offline_transaction.pt_credit_card_undeliverable_transaction_id pt_credit_card_undeliverable_transaction_id,
		 #fact_mms_credit_card_offline_transaction.offline_auth_flag offline_auth_flag,
		 #fact_mms_credit_card_offline_transaction.terminal_area_id terminal_area_id,
		 #fact_mms_credit_card_offline_transaction.val_payment_status_id val_payment_status_id,
		 #fact_mms_credit_card_offline_transaction.pos_unique_tran_id pos_unique_transaction_id,
		 case when #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key
		 when #fact_mms_credit_card_offline_transaction.club_id is null then '-998'
		 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction.club_id as varchar(500)),'z#@$k%&P'))),2)
		 end dim_mms_location_key,
		 case when #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key
		 when #fact_mms_credit_card_offline_transaction.member_id is null then '-998'
		 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction.member_id as varchar(500)),'z#@$k%&P'))),2)
		 end dim_mms_member_key,
		 #fact_mms_credit_card_offline_transaction.pos_tran_date_time pos_tran_date_time,
		 #fact_mms_credit_card_offline_transaction.rejected_transaction_date_time rejected_transaction_date_time,
		 case when #fact_mms_credit_card_offline_transaction.card_type is null then '-998' 
		 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction.card_type as varchar(500))+'P%#&z$@k'+'PT Credit Card Type','z#@$k%&P'))),2)
		 end  credit_card_type_dim_mms_description_key,
		 case when #fact_mms_credit_card_offline_transaction.val_payment_status_id is null then '-998' 
		 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction.val_payment_status_id as varchar(500))+'P%#&z$@k'+'Payment Status','z#@$k%&P'))),2)
		 end  payment_status_dim_mms_description_key,
		 #fact_mms_credit_card_offline_transaction.rejected_error_message rejected_transaction_error_message,
		 case when #fact_mms_credit_card_offline_transaction.card_on_file_flag = 1 then 'Y'
		 else 'N' end card_on_file_flag,
		 #fact_mms_credit_card_offline_transaction.masked_account_number masked_credit_card_number,
		 #fact_mms_credit_card_offline_transaction.transaction_amount transaction_amount,
		 #fact_mms_credit_card_offline_transaction.currency_code original_currency_code,
		 r_date.month_ending_r_date_id  month_ending_dim_date,
		 r_date.r_date_id dim_date,
		 r_date.year CalendarYear,
		 #fact_mms_credit_card_offline_transaction.dv_load_date_time dv_load_date_time,
		 #fact_mms_credit_card_offline_transaction.dv_load_end_date_time dv_load_end_date_time,
		 #fact_mms_credit_card_offline_transaction.dv_batch_id dv_batch_id
		 from #fact_mms_credit_card_offline_transaction
		left  join r_date
		on cast(cast(#fact_mms_credit_card_offline_transaction.pos_tran_date_time as varchar(12)) as datetime) = r_date.calendar_date 
		where (#fact_mms_credit_card_offline_transaction.terminal_area_id= 2
		and #fact_mms_credit_card_offline_transaction.offline_auth_flag =1)
		or #fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key in ('-999','-998','-997')
 
		 


-- do as a single transaction
--   delete records from the fact table that exist
--   insert records from records from current and missing batches

     begin tran
     delete dbo.fact_mms_credit_card_offline_transaction
     where fact_mms_credit_card_offline_transaction_key in (select fact_mms_credit_card_offline_transaction_key from #fact_mms_credit_card_offline_transaction1 )
	  
										insert  dbo.fact_mms_credit_card_offline_transaction(  
												fact_mms_credit_card_offline_transaction_key,
												pt_credit_card_rejected_transaction_id,
												declined_or_error_flag,
												timed_out_flag,
												dim_mms_location_key,
												--dim_mms_customer_key,
												dim_mms_member_key,
												pos_tran_date_time,
												rejected_transaction_date_time,
												credit_card_type_dim_mms_description_key,
												payment_status_dim_mms_description_key,
												pos_unique_transaction_id,
												rejected_transaction_error_message,
												card_on_file_flag,
												masked_credit_card_number,
												transaction_amount,
												third_party_pos_payment_id,
												original_currency_code,
												usd_monthy_average_dim_exchange_rate_key,
												usd_dim_plan_exchange_rate_key,
												local_currency_monthly_average_dim_mms_exchange_rate_key,
												local_currency_dim_plan_exchange_rate_key,
												--dim_mms_location_currency_code_key,
												dv_load_date_time,
												dv_load_end_date_time,
												dv_batch_id,
												dv_inserted_date_time,
												dv_insert_user)
												select 	distinct 		
												#fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key,
												case when #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key
								                 else #fact_mms_credit_card_offline_transaction1.pt_credit_card_rejected_transaction_id
							                     end fact_mms_credit_card_offline_transaction1,
												 case when #fact_mms_credit_card_offline_transaction1.val_payment_status_id in (4,5) then 'Y'
												 else 'N'
												 end declined_or_error_flag,
												 case when  #fact_mms_credit_card_offline_transaction1.val_payment_status_id = 7  then 'Y'
												 else 'N' end timed_out_flag,
												#fact_mms_credit_card_offline_transaction1.dim_mms_location_key dim_mms_location_key,
											    #fact_mms_credit_card_offline_transaction1.dim_mms_member_key dim_mms_member_key,
												#fact_mms_credit_card_offline_transaction1.pos_tran_date_time pos_tran_date_time,
												#fact_mms_credit_card_offline_transaction1.rejected_transaction_date_time rejected_transaction_date_time,
												#fact_mms_credit_card_offline_transaction1.credit_card_type_dim_mms_description_key credit_card_type_dim_mms_description_key,
												#fact_mms_credit_card_offline_transaction1.payment_status_dim_mms_description_key payment_status_dim_mms_description_key,
												#fact_mms_credit_card_offline_transaction1.pos_unique_transaction_id pos_unique_transaction_id,
												#fact_mms_credit_card_offline_transaction1.rejected_transaction_error_message rejected_transaction_error_message,
											    #fact_mms_credit_card_offline_transaction1.card_on_file_flag ,
												#fact_mms_credit_card_offline_transaction1.masked_credit_card_number masked_credit_card_number,
												#fact_mms_credit_card_offline_transaction1.transaction_amount transaction_amount,
												#fact_mms_credit_card_offline_transaction1.third_party_pos_payment_id,
												#fact_mms_credit_card_offline_transaction1.original_currency_code original_currency_code,
											--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(month_ending_dim_date and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash     
												case when #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key
								                when (#fact_mms_credit_card_offline_transaction1.month_ending_dim_date is null) then '-998'
								                else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.month_ending_dim_date as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.original_currency_code as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+'Monthly Average Exchange Rate')),2)
							                    end usd_monthy_average_dim_exchange_rate_key,
											--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CalendarYear and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
												case when #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key
												when (#fact_mms_credit_card_offline_transaction1.CalendarYear is null) then '-998'
												else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.CalendarYear as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.original_currency_code as varchar(500)),'z#@$k%&P'))),2)
												end usd_dim_plan_exchange_rate_key,
											  --md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(month_ending_dim_date and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
												case when #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key
												when (#fact_mms_credit_card_offline_transaction1.month_ending_dim_date is null) then '-998'
												else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.month_ending_dim_date as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.original_currency_code as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+'Monthly Average Exchange Rate')),2)
												end local_currency_monthly_average_dim_mms_exchange_rate_key,
												--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CalendarYear and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							                   case when #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key in ('-997','-998','-999') then #fact_mms_credit_card_offline_transaction1.fact_mms_credit_card_offline_transaction_key
								               when #fact_mms_credit_card_offline_transaction1.CalendarYear is null then '-998'
											   when #fact_mms_credit_card_offline_transaction1.original_currency_code is null then '-998'
											  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.CalendarYear as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#fact_mms_credit_card_offline_transaction1.original_currency_code as varchar(500)),'z#@$k%&P'))),2)
											  end local_currency_dim_plan_exchange_rate_key,
											  #fact_mms_credit_card_offline_transaction1.dv_load_date_time dv_load_date_time,
											  #fact_mms_credit_card_offline_transaction1.dv_load_end_date_time dv_load_end_date_time,
											  #fact_mms_credit_card_offline_transaction1.dv_batch_id dv_batch_id,
								              getdate(),
								              suser_sname()											  
											  from #fact_mms_credit_card_offline_transaction1
												 
		                                        commit tran

												
       
    

end
--go

--exec [proc_fact_mms_credit_card_offline_transaction] '20171113080128'
--go

--select * from fact_mms_credit_card_offline_transaction
--update dv_job_status set enabled_flag = 1 where job_name = 'wf_bv_fact_mms_credit_card_offline_transaction'
--go


