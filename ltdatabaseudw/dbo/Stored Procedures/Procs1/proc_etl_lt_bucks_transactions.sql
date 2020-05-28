CREATE PROC [dbo].[proc_etl_lt_bucks_transactions] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_Transactions

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_Transactions (
       bk_hash,
       transaction_id,
       transaction_type,
       transaction_user,
       transaction_amount,
       transaction_session,
       transaction_ext_ref,
       transaction_int1,
       transaction_int2,
       transaction_date1,
       transaction_timestamp,
       transaction_promotion,
       transaction_int3,
       transaction_int4,
       transaction_int5,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(transaction_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       transaction_id,
       transaction_type,
       transaction_user,
       transaction_amount,
       transaction_session,
       transaction_ext_ref,
       transaction_int1,
       transaction_int2,
       transaction_date1,
       transaction_timestamp,
       transaction_promotion,
       transaction_int3,
       transaction_int4,
       transaction_int5,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_Transactions.transaction_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_Transactions
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_transactions @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_transactions (
       bk_hash,
       transaction_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_Transactions.bk_hash,
       stage_hash_lt_bucks_Transactions.transaction_id transaction_id,
       isnull(cast(stage_hash_lt_bucks_Transactions.transaction_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_Transactions
  left join h_lt_bucks_transactions
    on stage_hash_lt_bucks_Transactions.bk_hash = h_lt_bucks_transactions.bk_hash
 where h_lt_bucks_transactions_id is null
   and stage_hash_lt_bucks_Transactions.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_transactions
if object_id('tempdb..#l_lt_bucks_transactions_inserts') is not null drop table #l_lt_bucks_transactions_inserts
create table #l_lt_bucks_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Transactions.bk_hash,
       stage_hash_lt_bucks_Transactions.transaction_id transaction_id,
       stage_hash_lt_bucks_Transactions.transaction_user transaction_user,
       stage_hash_lt_bucks_Transactions.transaction_session transaction_session,
       stage_hash_lt_bucks_Transactions.transaction_promotion transaction_promotion,
       stage_hash_lt_bucks_Transactions.transaction_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_user as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_session as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_promotion as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Transactions
 where stage_hash_lt_bucks_Transactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_transactions records
set @insert_date_time = getdate()
insert into l_lt_bucks_transactions (
       bk_hash,
       transaction_id,
       transaction_user,
       transaction_session,
       transaction_promotion,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_transactions_inserts.bk_hash,
       #l_lt_bucks_transactions_inserts.transaction_id,
       #l_lt_bucks_transactions_inserts.transaction_user,
       #l_lt_bucks_transactions_inserts.transaction_session,
       #l_lt_bucks_transactions_inserts.transaction_promotion,
       case when l_lt_bucks_transactions.l_lt_bucks_transactions_id is null then isnull(#l_lt_bucks_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_transactions_inserts
  left join p_lt_bucks_transactions
    on #l_lt_bucks_transactions_inserts.bk_hash = p_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_transactions
    on p_lt_bucks_transactions.bk_hash = l_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.l_lt_bucks_transactions_id = l_lt_bucks_transactions.l_lt_bucks_transactions_id
 where l_lt_bucks_transactions.l_lt_bucks_transactions_id is null
    or (l_lt_bucks_transactions.l_lt_bucks_transactions_id is not null
        and l_lt_bucks_transactions.dv_hash <> #l_lt_bucks_transactions_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_transactions
if object_id('tempdb..#s_lt_bucks_transactions_inserts') is not null drop table #s_lt_bucks_transactions_inserts
create table #s_lt_bucks_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Transactions.bk_hash,
       stage_hash_lt_bucks_Transactions.transaction_id transaction_id,
       stage_hash_lt_bucks_Transactions.transaction_type transaction_type,
       stage_hash_lt_bucks_Transactions.transaction_amount transaction_amount,
       stage_hash_lt_bucks_Transactions.transaction_ext_ref transaction_ext_ref,
       stage_hash_lt_bucks_Transactions.transaction_int1 transaction_int_1,
       stage_hash_lt_bucks_Transactions.transaction_int2 transaction_int_2,
       stage_hash_lt_bucks_Transactions.transaction_date1 transaction_date_1,
       stage_hash_lt_bucks_Transactions.transaction_timestamp transaction_timestamp,
       stage_hash_lt_bucks_Transactions.transaction_int3 transaction_int_3,
       stage_hash_lt_bucks_Transactions.transaction_int4 transaction_int_4,
       stage_hash_lt_bucks_Transactions.transaction_int5 transaction_int_5,
       stage_hash_lt_bucks_Transactions.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_Transactions.transaction_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_type as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Transactions.transaction_ext_ref,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_int1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_int2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Transactions.transaction_date1,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Transactions.transaction_timestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_int3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_int4 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Transactions.transaction_int5 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Transactions.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Transactions
 where stage_hash_lt_bucks_Transactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_transactions records
set @insert_date_time = getdate()
insert into s_lt_bucks_transactions (
       bk_hash,
       transaction_id,
       transaction_type,
       transaction_amount,
       transaction_ext_ref,
       transaction_int_1,
       transaction_int_2,
       transaction_date_1,
       transaction_timestamp,
       transaction_int_3,
       transaction_int_4,
       transaction_int_5,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_transactions_inserts.bk_hash,
       #s_lt_bucks_transactions_inserts.transaction_id,
       #s_lt_bucks_transactions_inserts.transaction_type,
       #s_lt_bucks_transactions_inserts.transaction_amount,
       #s_lt_bucks_transactions_inserts.transaction_ext_ref,
       #s_lt_bucks_transactions_inserts.transaction_int_1,
       #s_lt_bucks_transactions_inserts.transaction_int_2,
       #s_lt_bucks_transactions_inserts.transaction_date_1,
       #s_lt_bucks_transactions_inserts.transaction_timestamp,
       #s_lt_bucks_transactions_inserts.transaction_int_3,
       #s_lt_bucks_transactions_inserts.transaction_int_4,
       #s_lt_bucks_transactions_inserts.transaction_int_5,
       #s_lt_bucks_transactions_inserts.last_modified_timestamp,
       case when s_lt_bucks_transactions.s_lt_bucks_transactions_id is null then isnull(#s_lt_bucks_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_transactions_inserts
  left join p_lt_bucks_transactions
    on #s_lt_bucks_transactions_inserts.bk_hash = p_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_transactions
    on p_lt_bucks_transactions.bk_hash = s_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.s_lt_bucks_transactions_id = s_lt_bucks_transactions.s_lt_bucks_transactions_id
 where s_lt_bucks_transactions.s_lt_bucks_transactions_id is null
    or (s_lt_bucks_transactions.s_lt_bucks_transactions_id is not null
        and s_lt_bucks_transactions.dv_hash <> #s_lt_bucks_transactions_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_transactions @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_transactions @current_dv_batch_id

end
