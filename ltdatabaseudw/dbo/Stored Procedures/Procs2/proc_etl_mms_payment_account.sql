﻿CREATE PROC [dbo].[proc_etl_mms_payment_account] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PaymentAccount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PaymentAccount (
       bk_hash,
       PaymentAccountID,
       PaymentID,
       ExpirationDate,
       AccountNumber,
       Name,
       InsertedDateTime,
       RoutingNumber,
       BankName,
       MaskedAccountNumber,
       UpdatedDateTime,
       MaskedAccountNumber64,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PaymentAccountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PaymentAccountID,
       PaymentID,
       ExpirationDate,
       AccountNumber,
       Name,
       InsertedDateTime,
       RoutingNumber,
       BankName,
       MaskedAccountNumber,
       UpdatedDateTime,
       MaskedAccountNumber64,
       isnull(cast(stage_mms_PaymentAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PaymentAccount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_payment_account @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_payment_account (
       bk_hash,
       payment_account_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PaymentAccount.bk_hash,
       stage_hash_mms_PaymentAccount.PaymentAccountID payment_account_id,
       isnull(cast(stage_hash_mms_PaymentAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PaymentAccount
  left join h_mms_payment_account
    on stage_hash_mms_PaymentAccount.bk_hash = h_mms_payment_account.bk_hash
 where h_mms_payment_account_id is null
   and stage_hash_mms_PaymentAccount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_payment_account
if object_id('tempdb..#l_mms_payment_account_inserts') is not null drop table #l_mms_payment_account_inserts
create table #l_mms_payment_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentAccount.bk_hash,
       stage_hash_mms_PaymentAccount.PaymentAccountID payment_account_id,
       stage_hash_mms_PaymentAccount.PaymentID payment_id,
       stage_hash_mms_PaymentAccount.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentAccount.PaymentAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentAccount.PaymentID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentAccount
 where stage_hash_mms_PaymentAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_payment_account records
set @insert_date_time = getdate()
insert into l_mms_payment_account (
       bk_hash,
       payment_account_id,
       payment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_payment_account_inserts.bk_hash,
       #l_mms_payment_account_inserts.payment_account_id,
       #l_mms_payment_account_inserts.payment_id,
       case when l_mms_payment_account.l_mms_payment_account_id is null then isnull(#l_mms_payment_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_payment_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_payment_account_inserts
  left join p_mms_payment_account
    on #l_mms_payment_account_inserts.bk_hash = p_mms_payment_account.bk_hash
   and p_mms_payment_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_payment_account
    on p_mms_payment_account.bk_hash = l_mms_payment_account.bk_hash
   and p_mms_payment_account.l_mms_payment_account_id = l_mms_payment_account.l_mms_payment_account_id
 where l_mms_payment_account.l_mms_payment_account_id is null
    or (l_mms_payment_account.l_mms_payment_account_id is not null
        and l_mms_payment_account.dv_hash <> #l_mms_payment_account_inserts.source_hash)

--calculate hash and lookup to current s_mms_payment_account
if object_id('tempdb..#s_mms_payment_account_inserts') is not null drop table #s_mms_payment_account_inserts
create table #s_mms_payment_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentAccount.bk_hash,
       stage_hash_mms_PaymentAccount.PaymentAccountID payment_account_id,
       stage_hash_mms_PaymentAccount.ExpirationDate expiration_date,
       stage_hash_mms_PaymentAccount.AccountNumber account_number,
       stage_hash_mms_PaymentAccount.Name name,
       stage_hash_mms_PaymentAccount.InsertedDateTime inserted_date_time,
       stage_hash_mms_PaymentAccount.RoutingNumber routing_number,
       stage_hash_mms_PaymentAccount.BankName bank_name,
       stage_hash_mms_PaymentAccount.MaskedAccountNumber masked_account_number,
       stage_hash_mms_PaymentAccount.UpdatedDateTime updated_date_time,
       stage_hash_mms_PaymentAccount.MaskedAccountNumber64 masked_account_number64,
       stage_hash_mms_PaymentAccount.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentAccount.PaymentAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentAccount.ExpirationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.AccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentAccount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.RoutingNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.BankName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.MaskedAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentAccount.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentAccount.MaskedAccountNumber64,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentAccount
 where stage_hash_mms_PaymentAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_payment_account records
set @insert_date_time = getdate()
insert into s_mms_payment_account (
       bk_hash,
       payment_account_id,
       expiration_date,
       account_number,
       name,
       inserted_date_time,
       routing_number,
       bank_name,
       masked_account_number,
       updated_date_time,
       masked_account_number64,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_payment_account_inserts.bk_hash,
       #s_mms_payment_account_inserts.payment_account_id,
       #s_mms_payment_account_inserts.expiration_date,
       #s_mms_payment_account_inserts.account_number,
       #s_mms_payment_account_inserts.name,
       #s_mms_payment_account_inserts.inserted_date_time,
       #s_mms_payment_account_inserts.routing_number,
       #s_mms_payment_account_inserts.bank_name,
       #s_mms_payment_account_inserts.masked_account_number,
       #s_mms_payment_account_inserts.updated_date_time,
       #s_mms_payment_account_inserts.masked_account_number64,
       case when s_mms_payment_account.s_mms_payment_account_id is null then isnull(#s_mms_payment_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_payment_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_payment_account_inserts
  left join p_mms_payment_account
    on #s_mms_payment_account_inserts.bk_hash = p_mms_payment_account.bk_hash
   and p_mms_payment_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_payment_account
    on p_mms_payment_account.bk_hash = s_mms_payment_account.bk_hash
   and p_mms_payment_account.s_mms_payment_account_id = s_mms_payment_account.s_mms_payment_account_id
 where s_mms_payment_account.s_mms_payment_account_id is null
    or (s_mms_payment_account.s_mms_payment_account_id is not null
        and s_mms_payment_account.dv_hash <> #s_mms_payment_account_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_payment_account @current_dv_batch_id

end
