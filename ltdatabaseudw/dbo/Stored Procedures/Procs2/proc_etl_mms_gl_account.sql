CREATE PROC [dbo].[proc_etl_mms_gl_account] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_GLAccount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_GLAccount (
       bk_hash,
       GLAccountID,
       RevenueGLAccountNumber,
       RefundGLAccountNumber,
       InsertedDateTime,
       UpdatedDateTime,
       DiscountGLAccount,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(GLAccountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       GLAccountID,
       RevenueGLAccountNumber,
       RefundGLAccountNumber,
       InsertedDateTime,
       UpdatedDateTime,
       DiscountGLAccount,
       isnull(cast(stage_mms_GLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_GLAccount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_gl_account @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_gl_account (
       bk_hash,
       gl_account_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_GLAccount.bk_hash,
       stage_hash_mms_GLAccount.GLAccountID gl_account_id,
       isnull(cast(stage_hash_mms_GLAccount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_GLAccount
  left join h_mms_gl_account
    on stage_hash_mms_GLAccount.bk_hash = h_mms_gl_account.bk_hash
 where h_mms_gl_account_id is null
   and stage_hash_mms_GLAccount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mms_gl_account
if object_id('tempdb..#s_mms_gl_account_inserts') is not null drop table #s_mms_gl_account_inserts
create table #s_mms_gl_account_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GLAccount.bk_hash,
       stage_hash_mms_GLAccount.GLAccountID gl_account_id,
       stage_hash_mms_GLAccount.RevenueGLAccountNumber revenue_gl_account_number,
       stage_hash_mms_GLAccount.RefundGLAccountNumber refund_gl_account_number,
       stage_hash_mms_GLAccount.InsertedDateTime inserted_date_time,
       stage_hash_mms_GLAccount.UpdatedDateTime updated_date_time,
       stage_hash_mms_GLAccount.DiscountGLAccount discount_gl_account,
       stage_hash_mms_GLAccount.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GLAccount.GLAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_GLAccount.RevenueGLAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_GLAccount.RefundGLAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GLAccount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GLAccount.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_GLAccount.DiscountGLAccount,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GLAccount
 where stage_hash_mms_GLAccount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_gl_account records
set @insert_date_time = getdate()
insert into s_mms_gl_account (
       bk_hash,
       gl_account_id,
       revenue_gl_account_number,
       refund_gl_account_number,
       inserted_date_time,
       updated_date_time,
       discount_gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_gl_account_inserts.bk_hash,
       #s_mms_gl_account_inserts.gl_account_id,
       #s_mms_gl_account_inserts.revenue_gl_account_number,
       #s_mms_gl_account_inserts.refund_gl_account_number,
       #s_mms_gl_account_inserts.inserted_date_time,
       #s_mms_gl_account_inserts.updated_date_time,
       #s_mms_gl_account_inserts.discount_gl_account,
       case when s_mms_gl_account.s_mms_gl_account_id is null then isnull(#s_mms_gl_account_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_gl_account_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_gl_account_inserts
  left join p_mms_gl_account
    on #s_mms_gl_account_inserts.bk_hash = p_mms_gl_account.bk_hash
   and p_mms_gl_account.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_gl_account
    on p_mms_gl_account.bk_hash = s_mms_gl_account.bk_hash
   and p_mms_gl_account.s_mms_gl_account_id = s_mms_gl_account.s_mms_gl_account_id
 where s_mms_gl_account.s_mms_gl_account_id is null
    or (s_mms_gl_account.s_mms_gl_account_id is not null
        and s_mms_gl_account.dv_hash <> #s_mms_gl_account_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_gl_account @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_gl_account @current_dv_batch_id

end
