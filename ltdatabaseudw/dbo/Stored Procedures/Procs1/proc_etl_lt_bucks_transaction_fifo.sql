CREATE PROC [dbo].[proc_etl_lt_bucks_transaction_fifo] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_TransactionFifo

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_TransactionFifo (
       bk_hash,
       tfifo_id,
       tfifo_transaction1,
       tfifo_transaction2,
       tfifo_amount,
       tfifo_timestamp,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(tfifo_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       tfifo_id,
       tfifo_transaction1,
       tfifo_transaction2,
       tfifo_amount,
       tfifo_timestamp,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_TransactionFifo.tfifo_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_TransactionFifo
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_transaction_fifo @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_transaction_fifo (
       bk_hash,
       tfifo_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_TransactionFifo.bk_hash,
       stage_hash_lt_bucks_TransactionFifo.tfifo_id tfifo_id,
       isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_TransactionFifo
  left join h_lt_bucks_transaction_fifo
    on stage_hash_lt_bucks_TransactionFifo.bk_hash = h_lt_bucks_transaction_fifo.bk_hash
 where h_lt_bucks_transaction_fifo_id is null
   and stage_hash_lt_bucks_TransactionFifo.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_transaction_fifo
if object_id('tempdb..#l_lt_bucks_transaction_fifo_inserts') is not null drop table #l_lt_bucks_transaction_fifo_inserts
create table #l_lt_bucks_transaction_fifo_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_TransactionFifo.bk_hash,
       stage_hash_lt_bucks_TransactionFifo.tfifo_id tfifo_id,
       stage_hash_lt_bucks_TransactionFifo.tfifo_transaction1 tfifo_transaction_1,
       stage_hash_lt_bucks_TransactionFifo.tfifo_transaction2 tfifo_transaction_2,
       stage_hash_lt_bucks_TransactionFifo.tfifo_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_transaction1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_transaction2 as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_TransactionFifo
 where stage_hash_lt_bucks_TransactionFifo.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_transaction_fifo records
set @insert_date_time = getdate()
insert into l_lt_bucks_transaction_fifo (
       bk_hash,
       tfifo_id,
       tfifo_transaction_1,
       tfifo_transaction_2,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_transaction_fifo_inserts.bk_hash,
       #l_lt_bucks_transaction_fifo_inserts.tfifo_id,
       #l_lt_bucks_transaction_fifo_inserts.tfifo_transaction_1,
       #l_lt_bucks_transaction_fifo_inserts.tfifo_transaction_2,
       case when l_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id is null then isnull(#l_lt_bucks_transaction_fifo_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_transaction_fifo_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_transaction_fifo_inserts
  left join p_lt_bucks_transaction_fifo
    on #l_lt_bucks_transaction_fifo_inserts.bk_hash = p_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_transaction_fifo
    on p_lt_bucks_transaction_fifo.bk_hash = l_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id = l_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id
 where l_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id is null
    or (l_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id is not null
        and l_lt_bucks_transaction_fifo.dv_hash <> #l_lt_bucks_transaction_fifo_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_transaction_fifo
if object_id('tempdb..#s_lt_bucks_transaction_fifo_inserts') is not null drop table #s_lt_bucks_transaction_fifo_inserts
create table #s_lt_bucks_transaction_fifo_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_TransactionFifo.bk_hash,
       stage_hash_lt_bucks_TransactionFifo.tfifo_id tfifo_id,
       stage_hash_lt_bucks_TransactionFifo.tfifo_amount tfifo_amount,
       stage_hash_lt_bucks_TransactionFifo.tfifo_timestamp tfifo_timestamp,
       stage_hash_lt_bucks_TransactionFifo.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_TransactionFifo.tfifo_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_TransactionFifo.tfifo_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_TransactionFifo.tfifo_timestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_TransactionFifo.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_TransactionFifo
 where stage_hash_lt_bucks_TransactionFifo.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_transaction_fifo records
set @insert_date_time = getdate()
insert into s_lt_bucks_transaction_fifo (
       bk_hash,
       tfifo_id,
       tfifo_amount,
       tfifo_timestamp,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_transaction_fifo_inserts.bk_hash,
       #s_lt_bucks_transaction_fifo_inserts.tfifo_id,
       #s_lt_bucks_transaction_fifo_inserts.tfifo_amount,
       #s_lt_bucks_transaction_fifo_inserts.tfifo_timestamp,
       #s_lt_bucks_transaction_fifo_inserts.last_modified_timestamp,
       case when s_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id is null then isnull(#s_lt_bucks_transaction_fifo_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_transaction_fifo_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_transaction_fifo_inserts
  left join p_lt_bucks_transaction_fifo
    on #s_lt_bucks_transaction_fifo_inserts.bk_hash = p_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_transaction_fifo
    on p_lt_bucks_transaction_fifo.bk_hash = s_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id = s_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id
 where s_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id is null
    or (s_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id is not null
        and s_lt_bucks_transaction_fifo.dv_hash <> #s_lt_bucks_transaction_fifo_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_transaction_fifo @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_transaction_fifo @current_dv_batch_id

end
