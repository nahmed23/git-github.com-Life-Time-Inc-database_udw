CREATE PROC [dbo].[proc_etl_mms_membership_balance] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_mms_MembershipBalance where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipBalance (
       bk_hash,
       MembershipBalanceID,
       MembershipID,
       CurrentBalance,
       EFTAmount,
       StatementBalance,
       AssessedDateTime,
       StatementDateTime,
       PreviousStatementBalance,
       PreviousStatementDateTime,
       CommittedBalance,
       InsertedDateTime,
       UpdatedDateTime,
       ResubmitCollectFromBankAccountFlag,
       CommittedBalanceProducts,
       CurrentBalanceProducts,
       EFTAmountProducts,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipBalanceID,
       MembershipID,
       CurrentBalance,
       EFTAmount,
       StatementBalance,
       AssessedDateTime,
       StatementDateTime,
       PreviousStatementBalance,
       PreviousStatementDateTime,
       CommittedBalance,
       InsertedDateTime,
       UpdatedDateTime,
       ResubmitCollectFromBankAccountFlag,
       CommittedBalanceProducts,
       CurrentBalanceProducts,
       EFTAmountProducts,
       isnull(cast(stage_mms_MembershipBalance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_MembershipBalance
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_balance @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_balance (
       bk_hash,
       membership_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MembershipBalance.bk_hash,
       stage_hash_mms_MembershipBalance.MembershipID membership_id,
       isnull(cast(stage_hash_mms_MembershipBalance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipBalance
  left join h_mms_membership_balance
    on stage_hash_mms_MembershipBalance.bk_hash = h_mms_membership_balance.bk_hash
 where h_mms_membership_balance_id is null
   and stage_hash_mms_MembershipBalance.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_balance
if object_id('tempdb..#l_mms_membership_balance_inserts') is not null drop table #l_mms_membership_balance_inserts
create table #l_mms_membership_balance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipBalance.bk_hash,
       stage_hash_mms_MembershipBalance.MembershipBalanceID membership_balance_id,
       stage_hash_mms_MembershipBalance.MembershipID membership_id,
       stage_hash_mms_MembershipBalance.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.MembershipBalanceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.MembershipID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipBalance
 where stage_hash_mms_MembershipBalance.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_balance records
set @insert_date_time = getdate()
insert into l_mms_membership_balance (
       bk_hash,
       membership_balance_id,
       membership_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_balance_inserts.bk_hash,
       #l_mms_membership_balance_inserts.membership_balance_id,
       #l_mms_membership_balance_inserts.membership_id,
       case when l_mms_membership_balance.l_mms_membership_balance_id is null then isnull(#l_mms_membership_balance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_balance_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_balance_inserts
  left join p_mms_membership_balance
    on #l_mms_membership_balance_inserts.bk_hash = p_mms_membership_balance.bk_hash
   and p_mms_membership_balance.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_balance
    on p_mms_membership_balance.bk_hash = l_mms_membership_balance.bk_hash
   and p_mms_membership_balance.l_mms_membership_balance_id = l_mms_membership_balance.l_mms_membership_balance_id
 where l_mms_membership_balance.l_mms_membership_balance_id is null
    or (l_mms_membership_balance.l_mms_membership_balance_id is not null
        and l_mms_membership_balance.dv_hash <> #l_mms_membership_balance_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_balance
if object_id('tempdb..#s_mms_membership_balance_inserts') is not null drop table #s_mms_membership_balance_inserts
create table #s_mms_membership_balance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipBalance.bk_hash,
       stage_hash_mms_MembershipBalance.MembershipID membership_id,
       stage_hash_mms_MembershipBalance.CurrentBalance current_balance,
       stage_hash_mms_MembershipBalance.EFTAmount eft_amount,
       stage_hash_mms_MembershipBalance.StatementBalance statement_balance,
       stage_hash_mms_MembershipBalance.AssessedDateTime assessed_date_time,
       stage_hash_mms_MembershipBalance.StatementDateTime statement_date_time,
       stage_hash_mms_MembershipBalance.PreviousStatementBalance previous_statement_balance,
       stage_hash_mms_MembershipBalance.PreviousStatementDateTime previous_statement_datetime,
       stage_hash_mms_MembershipBalance.CommittedBalance committed_balance,
       stage_hash_mms_MembershipBalance.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipBalance.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipBalance.ResubmitCollectFromBankAccountFlag resubmit_collect_from_bank_account_flag,
       stage_hash_mms_MembershipBalance.CommittedBalanceProducts committed_balance_products,
       stage_hash_mms_MembershipBalance.CurrentBalanceProducts current_balance_products,
       stage_hash_mms_MembershipBalance.EFTAmountProducts eft_amount_products,
       stage_hash_mms_MembershipBalance.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.CurrentBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.EFTAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.StatementBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipBalance.AssessedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipBalance.StatementDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.PreviousStatementBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipBalance.PreviousStatementDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.CommittedBalance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipBalance.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipBalance.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.ResubmitCollectFromBankAccountFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.CommittedBalanceProducts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.CurrentBalanceProducts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipBalance.EFTAmountProducts as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipBalance
 where stage_hash_mms_MembershipBalance.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_balance records
set @insert_date_time = getdate()
insert into s_mms_membership_balance (
       bk_hash,
       membership_id,
       current_balance,
       eft_amount,
       statement_balance,
       assessed_date_time,
       statement_date_time,
       previous_statement_balance,
       previous_statement_datetime,
       committed_balance,
       inserted_date_time,
       updated_date_time,
       resubmit_collect_from_bank_account_flag,
       committed_balance_products,
       current_balance_products,
       eft_amount_products,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_balance_inserts.bk_hash,
       #s_mms_membership_balance_inserts.membership_id,
       #s_mms_membership_balance_inserts.current_balance,
       #s_mms_membership_balance_inserts.eft_amount,
       #s_mms_membership_balance_inserts.statement_balance,
       #s_mms_membership_balance_inserts.assessed_date_time,
       #s_mms_membership_balance_inserts.statement_date_time,
       #s_mms_membership_balance_inserts.previous_statement_balance,
       #s_mms_membership_balance_inserts.previous_statement_datetime,
       #s_mms_membership_balance_inserts.committed_balance,
       #s_mms_membership_balance_inserts.inserted_date_time,
       #s_mms_membership_balance_inserts.updated_date_time,
       #s_mms_membership_balance_inserts.resubmit_collect_from_bank_account_flag,
       #s_mms_membership_balance_inserts.committed_balance_products,
       #s_mms_membership_balance_inserts.current_balance_products,
       #s_mms_membership_balance_inserts.eft_amount_products,
       case when s_mms_membership_balance.s_mms_membership_balance_id is null then isnull(#s_mms_membership_balance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_balance_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_balance_inserts
  left join p_mms_membership_balance
    on #s_mms_membership_balance_inserts.bk_hash = p_mms_membership_balance.bk_hash
   and p_mms_membership_balance.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_balance
    on p_mms_membership_balance.bk_hash = s_mms_membership_balance.bk_hash
   and p_mms_membership_balance.s_mms_membership_balance_id = s_mms_membership_balance.s_mms_membership_balance_id
 where s_mms_membership_balance.s_mms_membership_balance_id is null
    or (s_mms_membership_balance.s_mms_membership_balance_id is not null
        and s_mms_membership_balance.dv_hash <> #s_mms_membership_balance_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_balance @current_dv_batch_id

end
