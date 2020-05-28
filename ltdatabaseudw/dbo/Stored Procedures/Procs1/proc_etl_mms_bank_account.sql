CREATE PROC [dbo].[proc_etl_mms_bank_account] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_BankAccount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_BankAccount (
       bk_hash,
       BankAccountID,
       MembershipID,
       AccountNumber,
       Name,
       PreNotifyFlag,
       RoutingNumber,
       ValPaymentTypeID,
       InsertedDateTime,
       BankName,
       UpdatedDateTime,
       MaskedAccountNumber,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(BankAccountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       BankAccountID,
       MembershipID,
       AccountNumber,
       Name,
       PreNotifyFlag,
       RoutingNumber,
       ValPaymentTypeID,
       InsertedDateTime,
       BankName,
       UpdatedDateTime,
       MaskedAccountNumber,
       isnull(cast(stage_mms_BankAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_BankAccount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_bank_account @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_bank_account (
       bk_hash,
       bank_account_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_BankAccount.bk_hash,
       stage_hash_mms_BankAccount.BankAccountID bank_account_id,
       isnull(cast(stage_hash_mms_BankAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_BankAccount
  left join h_mms_bank_account
    on stage_hash_mms_BankAccount.bk_hash = h_mms_bank_account.bk_hash
 where h_mms_bank_account_id is null
   and stage_hash_mms_BankAccount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_bank_account
if object_id('tempdb..#l_mms_bank_account_inserts') is not null drop table #l_mms_bank_account_inserts
create table #l_mms_bank_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_BankAccount.bk_hash,
       stage_hash_mms_BankAccount.BankAccountID bank_account_id,
       stage_hash_mms_BankAccount.MembershipID membership_id,
       stage_hash_mms_BankAccount.ValPaymentTypeID val_payment_type_id,
       stage_hash_mms_BankAccount.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_BankAccount.BankAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_BankAccount.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_BankAccount.ValPaymentTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_BankAccount
 where stage_hash_mms_BankAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_bank_account records
set @insert_date_time = getdate()
insert into l_mms_bank_account (
       bk_hash,
       bank_account_id,
       membership_id,
       val_payment_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_bank_account_inserts.bk_hash,
       #l_mms_bank_account_inserts.bank_account_id,
       #l_mms_bank_account_inserts.membership_id,
       #l_mms_bank_account_inserts.val_payment_type_id,
       case when l_mms_bank_account.l_mms_bank_account_id is null then isnull(#l_mms_bank_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_bank_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_bank_account_inserts
  left join p_mms_bank_account
    on #l_mms_bank_account_inserts.bk_hash = p_mms_bank_account.bk_hash
   and p_mms_bank_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_bank_account
    on p_mms_bank_account.bk_hash = l_mms_bank_account.bk_hash
   and p_mms_bank_account.l_mms_bank_account_id = l_mms_bank_account.l_mms_bank_account_id
 where l_mms_bank_account.l_mms_bank_account_id is null
    or (l_mms_bank_account.l_mms_bank_account_id is not null
        and l_mms_bank_account.dv_hash <> #l_mms_bank_account_inserts.source_hash)

--calculate hash and lookup to current s_mms_bank_account
if object_id('tempdb..#s_mms_bank_account_inserts') is not null drop table #s_mms_bank_account_inserts
create table #s_mms_bank_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_BankAccount.bk_hash,
       stage_hash_mms_BankAccount.BankAccountID bank_account_id,
       stage_hash_mms_BankAccount.AccountNumber account_number,
       stage_hash_mms_BankAccount.Name name,
       stage_hash_mms_BankAccount.PreNotifyFlag pre_notify_flag,
       stage_hash_mms_BankAccount.RoutingNumber routing_number,
       stage_hash_mms_BankAccount.InsertedDateTime inserted_date_time,
       stage_hash_mms_BankAccount.BankName bank_name,
       stage_hash_mms_BankAccount.UpdatedDateTime updated_date_time,
       stage_hash_mms_BankAccount.MaskedAccountNumber masked_account_number,
       stage_hash_mms_BankAccount.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_BankAccount.BankAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_BankAccount.AccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_BankAccount.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_BankAccount.PreNotifyFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_BankAccount.RoutingNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_BankAccount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_BankAccount.BankName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_BankAccount.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_BankAccount.MaskedAccountNumber,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_BankAccount
 where stage_hash_mms_BankAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_bank_account records
set @insert_date_time = getdate()
insert into s_mms_bank_account (
       bk_hash,
       bank_account_id,
       account_number,
       name,
       pre_notify_flag,
       routing_number,
       inserted_date_time,
       bank_name,
       updated_date_time,
       masked_account_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_bank_account_inserts.bk_hash,
       #s_mms_bank_account_inserts.bank_account_id,
       #s_mms_bank_account_inserts.account_number,
       #s_mms_bank_account_inserts.name,
       #s_mms_bank_account_inserts.pre_notify_flag,
       #s_mms_bank_account_inserts.routing_number,
       #s_mms_bank_account_inserts.inserted_date_time,
       #s_mms_bank_account_inserts.bank_name,
       #s_mms_bank_account_inserts.updated_date_time,
       #s_mms_bank_account_inserts.masked_account_number,
       case when s_mms_bank_account.s_mms_bank_account_id is null then isnull(#s_mms_bank_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_bank_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_bank_account_inserts
  left join p_mms_bank_account
    on #s_mms_bank_account_inserts.bk_hash = p_mms_bank_account.bk_hash
   and p_mms_bank_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_bank_account
    on p_mms_bank_account.bk_hash = s_mms_bank_account.bk_hash
   and p_mms_bank_account.s_mms_bank_account_id = s_mms_bank_account.s_mms_bank_account_id
 where s_mms_bank_account.s_mms_bank_account_id is null
    or (s_mms_bank_account.s_mms_bank_account_id is not null
        and s_mms_bank_account.dv_hash <> #s_mms_bank_account_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_bank_account @current_dv_batch_id

end
