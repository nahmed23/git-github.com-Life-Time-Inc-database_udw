CREATE PROC [dbo].[proc_etl_magento_sales_payment_transaction] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_payment_transaction

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_payment_transaction (
       bk_hash,
       transaction_id,
       parent_id,
       order_id,
       payment_id,
       txn_id,
       parent_txn_id,
       txn_type,
       is_closed,
       created_at,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(transaction_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       transaction_id,
       parent_id,
       order_id,
       payment_id,
       txn_id,
       parent_txn_id,
       txn_type,
       is_closed,
       created_at,
       isnull(cast(stage_magento_sales_payment_transaction.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_payment_transaction
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_payment_transaction @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_payment_transaction (
       bk_hash,
       transaction_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_payment_transaction.bk_hash,
       stage_hash_magento_sales_payment_transaction.transaction_id transaction_id,
       isnull(cast(stage_hash_magento_sales_payment_transaction.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_payment_transaction
  left join h_magento_sales_payment_transaction
    on stage_hash_magento_sales_payment_transaction.bk_hash = h_magento_sales_payment_transaction.bk_hash
 where h_magento_sales_payment_transaction_id is null
   and stage_hash_magento_sales_payment_transaction.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_payment_transaction
if object_id('tempdb..#l_magento_sales_payment_transaction_inserts') is not null drop table #l_magento_sales_payment_transaction_inserts
create table #l_magento_sales_payment_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_payment_transaction.bk_hash,
       stage_hash_magento_sales_payment_transaction.transaction_id transaction_id,
       stage_hash_magento_sales_payment_transaction.parent_id parent_id,
       stage_hash_magento_sales_payment_transaction.order_id order_id,
       stage_hash_magento_sales_payment_transaction.payment_id payment_id,
       stage_hash_magento_sales_payment_transaction.txn_id txn_id,
       stage_hash_magento_sales_payment_transaction.parent_txn_id parent_txn_id,
       isnull(cast(stage_hash_magento_sales_payment_transaction.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.transaction_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.payment_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_payment_transaction.txn_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_payment_transaction.parent_txn_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_payment_transaction
 where stage_hash_magento_sales_payment_transaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_payment_transaction records
set @insert_date_time = getdate()
insert into l_magento_sales_payment_transaction (
       bk_hash,
       transaction_id,
       parent_id,
       order_id,
       payment_id,
       txn_id,
       parent_txn_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_payment_transaction_inserts.bk_hash,
       #l_magento_sales_payment_transaction_inserts.transaction_id,
       #l_magento_sales_payment_transaction_inserts.parent_id,
       #l_magento_sales_payment_transaction_inserts.order_id,
       #l_magento_sales_payment_transaction_inserts.payment_id,
       #l_magento_sales_payment_transaction_inserts.txn_id,
       #l_magento_sales_payment_transaction_inserts.parent_txn_id,
       case when l_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id is null then isnull(#l_magento_sales_payment_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_payment_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_payment_transaction_inserts
  left join p_magento_sales_payment_transaction
    on #l_magento_sales_payment_transaction_inserts.bk_hash = p_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_payment_transaction
    on p_magento_sales_payment_transaction.bk_hash = l_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id = l_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id
 where l_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id is null
    or (l_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id is not null
        and l_magento_sales_payment_transaction.dv_hash <> #l_magento_sales_payment_transaction_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_payment_transaction
if object_id('tempdb..#s_magento_sales_payment_transaction_inserts') is not null drop table #s_magento_sales_payment_transaction_inserts
create table #s_magento_sales_payment_transaction_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_payment_transaction.bk_hash,
       stage_hash_magento_sales_payment_transaction.transaction_id transaction_id,
       stage_hash_magento_sales_payment_transaction.txn_type txn_type,
       stage_hash_magento_sales_payment_transaction.is_closed is_closed,
       stage_hash_magento_sales_payment_transaction.created_at created_at,
       isnull(cast(stage_hash_magento_sales_payment_transaction.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.transaction_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_payment_transaction.txn_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_payment_transaction.is_closed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_payment_transaction.created_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_payment_transaction
 where stage_hash_magento_sales_payment_transaction.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_payment_transaction records
set @insert_date_time = getdate()
insert into s_magento_sales_payment_transaction (
       bk_hash,
       transaction_id,
       txn_type,
       is_closed,
       created_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_payment_transaction_inserts.bk_hash,
       #s_magento_sales_payment_transaction_inserts.transaction_id,
       #s_magento_sales_payment_transaction_inserts.txn_type,
       #s_magento_sales_payment_transaction_inserts.is_closed,
       #s_magento_sales_payment_transaction_inserts.created_at,
       case when s_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id is null then isnull(#s_magento_sales_payment_transaction_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_payment_transaction_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_payment_transaction_inserts
  left join p_magento_sales_payment_transaction
    on #s_magento_sales_payment_transaction_inserts.bk_hash = p_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_payment_transaction
    on p_magento_sales_payment_transaction.bk_hash = s_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id = s_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id
 where s_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id is null
    or (s_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id is not null
        and s_magento_sales_payment_transaction.dv_hash <> #s_magento_sales_payment_transaction_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_payment_transaction @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_payment_transaction @current_dv_batch_id

end
