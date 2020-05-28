CREATE PROC [dbo].[proc_etl_mms_club_gl_account] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ClubGLAccount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ClubGLAccount (
       bk_hash,
       ClubGLAccountID,
       ClubID,
       ValCurrencyCodeID,
       GLCashEntryCompanyName,
       GLCashEntryAccount,
       GLReceivablesEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntrySubAccount,
       GLReceivablesEntryCompanyName,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubGLAccountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClubGLAccountID,
       ClubID,
       ValCurrencyCodeID,
       GLCashEntryCompanyName,
       GLCashEntryAccount,
       GLReceivablesEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntrySubAccount,
       GLReceivablesEntryCompanyName,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ClubGLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ClubGLAccount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_club_gl_account @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_club_gl_account (
       bk_hash,
       club_gl_account_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ClubGLAccount.bk_hash,
       stage_hash_mms_ClubGLAccount.ClubGLAccountID club_gl_account_id,
       isnull(cast(stage_hash_mms_ClubGLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ClubGLAccount
  left join h_mms_club_gl_account
    on stage_hash_mms_ClubGLAccount.bk_hash = h_mms_club_gl_account.bk_hash
 where h_mms_club_gl_account_id is null
   and stage_hash_mms_ClubGLAccount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_club_gl_account
if object_id('tempdb..#l_mms_club_gl_account_inserts') is not null drop table #l_mms_club_gl_account_inserts
create table #l_mms_club_gl_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubGLAccount.bk_hash,
       stage_hash_mms_ClubGLAccount.ClubGLAccountID club_gl_account_id,
       stage_hash_mms_ClubGLAccount.ClubID club_id,
       stage_hash_mms_ClubGLAccount.ValCurrencyCodeID val_currency_code_id,
       isnull(cast(stage_hash_mms_ClubGLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubGLAccount.ClubGLAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubGLAccount.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubGLAccount.ValCurrencyCodeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubGLAccount
 where stage_hash_mms_ClubGLAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_club_gl_account records
set @insert_date_time = getdate()
insert into l_mms_club_gl_account (
       bk_hash,
       club_gl_account_id,
       club_id,
       val_currency_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_club_gl_account_inserts.bk_hash,
       #l_mms_club_gl_account_inserts.club_gl_account_id,
       #l_mms_club_gl_account_inserts.club_id,
       #l_mms_club_gl_account_inserts.val_currency_code_id,
       case when l_mms_club_gl_account.l_mms_club_gl_account_id is null then isnull(#l_mms_club_gl_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_club_gl_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_club_gl_account_inserts
  left join p_mms_club_gl_account
    on #l_mms_club_gl_account_inserts.bk_hash = p_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_club_gl_account
    on p_mms_club_gl_account.bk_hash = l_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.l_mms_club_gl_account_id = l_mms_club_gl_account.l_mms_club_gl_account_id
 where l_mms_club_gl_account.l_mms_club_gl_account_id is null
    or (l_mms_club_gl_account.l_mms_club_gl_account_id is not null
        and l_mms_club_gl_account.dv_hash <> #l_mms_club_gl_account_inserts.source_hash)

--calculate hash and lookup to current s_mms_club_gl_account
if object_id('tempdb..#s_mms_club_gl_account_inserts') is not null drop table #s_mms_club_gl_account_inserts
create table #s_mms_club_gl_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubGLAccount.bk_hash,
       stage_hash_mms_ClubGLAccount.ClubGLAccountID club_gl_account_id,
       stage_hash_mms_ClubGLAccount.GLCashEntryCompanyName gl_cash_entry_company_name,
       stage_hash_mms_ClubGLAccount.GLCashEntryAccount gl_cash_entry_account,
       stage_hash_mms_ClubGLAccount.GLReceivablesEntryAccount gl_receivables_entry_account,
       stage_hash_mms_ClubGLAccount.GLCashEntryCashSubAccount gl_cash_entry_cash_sub_account,
       stage_hash_mms_ClubGLAccount.GLCashEntryCreditCardSubAccount gl_cash_entry_credit_card_sub_account,
       stage_hash_mms_ClubGLAccount.GLReceivablesEntrySubAccount gl_receivables_entry_sub_account,
       stage_hash_mms_ClubGLAccount.GLReceivablesEntryCompanyName gl_receivables_entry_company_name,
       stage_hash_mms_ClubGLAccount.InsertedDateTime inserted_date_time,
       stage_hash_mms_ClubGLAccount.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_ClubGLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubGLAccount.ClubGLAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLCashEntryCompanyName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLCashEntryAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLReceivablesEntryAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLCashEntryCashSubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLCashEntryCreditCardSubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLReceivablesEntrySubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubGLAccount.GLReceivablesEntryCompanyName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubGLAccount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubGLAccount.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubGLAccount
 where stage_hash_mms_ClubGLAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_club_gl_account records
set @insert_date_time = getdate()
insert into s_mms_club_gl_account (
       bk_hash,
       club_gl_account_id,
       gl_cash_entry_company_name,
       gl_cash_entry_account,
       gl_receivables_entry_account,
       gl_cash_entry_cash_sub_account,
       gl_cash_entry_credit_card_sub_account,
       gl_receivables_entry_sub_account,
       gl_receivables_entry_company_name,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_club_gl_account_inserts.bk_hash,
       #s_mms_club_gl_account_inserts.club_gl_account_id,
       #s_mms_club_gl_account_inserts.gl_cash_entry_company_name,
       #s_mms_club_gl_account_inserts.gl_cash_entry_account,
       #s_mms_club_gl_account_inserts.gl_receivables_entry_account,
       #s_mms_club_gl_account_inserts.gl_cash_entry_cash_sub_account,
       #s_mms_club_gl_account_inserts.gl_cash_entry_credit_card_sub_account,
       #s_mms_club_gl_account_inserts.gl_receivables_entry_sub_account,
       #s_mms_club_gl_account_inserts.gl_receivables_entry_company_name,
       #s_mms_club_gl_account_inserts.inserted_date_time,
       #s_mms_club_gl_account_inserts.updated_date_time,
       case when s_mms_club_gl_account.s_mms_club_gl_account_id is null then isnull(#s_mms_club_gl_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_club_gl_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_club_gl_account_inserts
  left join p_mms_club_gl_account
    on #s_mms_club_gl_account_inserts.bk_hash = p_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_club_gl_account
    on p_mms_club_gl_account.bk_hash = s_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.s_mms_club_gl_account_id = s_mms_club_gl_account.s_mms_club_gl_account_id
 where s_mms_club_gl_account.s_mms_club_gl_account_id is null
    or (s_mms_club_gl_account.s_mms_club_gl_account_id is not null
        and s_mms_club_gl_account.dv_hash <> #s_mms_club_gl_account_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_club_gl_account @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_club_gl_account @current_dv_batch_id

end
