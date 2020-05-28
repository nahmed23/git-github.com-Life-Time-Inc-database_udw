CREATE PROC [dbo].[proc_etl_healthcheckusa_transactions] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_healthcheckusa_Transactions

set @insert_date_time = getdate()
insert into dbo.stage_hash_healthcheckusa_Transactions (
       bk_hash,
       OrderNumber,
       SKU,
       TransactionType,
       TransactionDate,
       ltfGlclubid,
       ltfEmployeeID,
       Quantity,
       ItemAmount,
       ItemDiscount,
       OrderForEmployeeFlag,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(OrderNumber as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SKU as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       OrderNumber,
       SKU,
       TransactionType,
       TransactionDate,
       ltfGlclubid,
       ltfEmployeeID,
       Quantity,
       ItemAmount,
       ItemDiscount,
       OrderForEmployeeFlag,
       dummy_modified_date_time,
       isnull(cast(stage_healthcheckusa_Transactions.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_healthcheckusa_Transactions
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_healthcheckusa_transactions @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_healthcheckusa_transactions (
       bk_hash,
       order_number,
       sku,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_healthcheckusa_Transactions.bk_hash,
       stage_hash_healthcheckusa_Transactions.OrderNumber order_number,
       stage_hash_healthcheckusa_Transactions.SKU sku,
       isnull(cast(stage_hash_healthcheckusa_Transactions.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       31,
       @insert_date_time,
       @user
  from stage_hash_healthcheckusa_Transactions
  left join h_healthcheckusa_transactions
    on stage_hash_healthcheckusa_Transactions.bk_hash = h_healthcheckusa_transactions.bk_hash
 where h_healthcheckusa_transactions_id is null
   and stage_hash_healthcheckusa_Transactions.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_healthcheckusa_transactions
if object_id('tempdb..#l_healthcheckusa_transactions_inserts') is not null drop table #l_healthcheckusa_transactions_inserts
create table #l_healthcheckusa_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_healthcheckusa_Transactions.bk_hash,
       stage_hash_healthcheckusa_Transactions.OrderNumber order_number,
       stage_hash_healthcheckusa_Transactions.SKU sku,
       stage_hash_healthcheckusa_Transactions.ltfGlclubid ltf_gl_club_id,
       stage_hash_healthcheckusa_Transactions.ltfEmployeeID ltf_employee_id,
       isnull(cast(stage_hash_healthcheckusa_Transactions.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.OrderNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.SKU as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_healthcheckusa_Transactions.ltfGlclubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_healthcheckusa_Transactions.ltfEmployeeID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_healthcheckusa_Transactions
 where stage_hash_healthcheckusa_Transactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_healthcheckusa_transactions records
set @insert_date_time = getdate()
insert into l_healthcheckusa_transactions (
       bk_hash,
       order_number,
       sku,
       ltf_gl_club_id,
       ltf_employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_healthcheckusa_transactions_inserts.bk_hash,
       #l_healthcheckusa_transactions_inserts.order_number,
       #l_healthcheckusa_transactions_inserts.sku,
       #l_healthcheckusa_transactions_inserts.ltf_gl_club_id,
       #l_healthcheckusa_transactions_inserts.ltf_employee_id,
       case when l_healthcheckusa_transactions.l_healthcheckusa_transactions_id is null then isnull(#l_healthcheckusa_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       31,
       #l_healthcheckusa_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_healthcheckusa_transactions_inserts
  left join p_healthcheckusa_transactions
    on #l_healthcheckusa_transactions_inserts.bk_hash = p_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = l_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.l_healthcheckusa_transactions_id = l_healthcheckusa_transactions.l_healthcheckusa_transactions_id
 where l_healthcheckusa_transactions.l_healthcheckusa_transactions_id is null
    or (l_healthcheckusa_transactions.l_healthcheckusa_transactions_id is not null
        and l_healthcheckusa_transactions.dv_hash <> #l_healthcheckusa_transactions_inserts.source_hash)

--calculate hash and lookup to current s_healthcheckusa_transactions
if object_id('tempdb..#s_healthcheckusa_transactions_inserts') is not null drop table #s_healthcheckusa_transactions_inserts
create table #s_healthcheckusa_transactions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_healthcheckusa_Transactions.bk_hash,
       stage_hash_healthcheckusa_Transactions.OrderNumber order_number,
       stage_hash_healthcheckusa_Transactions.SKU sku,
       stage_hash_healthcheckusa_Transactions.TransactionType transaction_type,
       stage_hash_healthcheckusa_Transactions.TransactionDate transaction_date,
       stage_hash_healthcheckusa_Transactions.Quantity quantity,
       stage_hash_healthcheckusa_Transactions.ItemAmount item_amount,
       stage_hash_healthcheckusa_Transactions.ItemDiscount item_discount,
       stage_hash_healthcheckusa_Transactions.OrderForEmployeeFlag order_for_employee_flag,
       stage_hash_healthcheckusa_Transactions.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_healthcheckusa_Transactions.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.OrderNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.SKU as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_healthcheckusa_Transactions.TransactionType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_healthcheckusa_Transactions.TransactionDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.Quantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.ItemAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_healthcheckusa_Transactions.ItemDiscount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_healthcheckusa_Transactions.OrderForEmployeeFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_healthcheckusa_Transactions.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_healthcheckusa_Transactions
 where stage_hash_healthcheckusa_Transactions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_healthcheckusa_transactions records
set @insert_date_time = getdate()
insert into s_healthcheckusa_transactions (
       bk_hash,
       order_number,
       sku,
       transaction_type,
       transaction_date,
       quantity,
       item_amount,
       item_discount,
       order_for_employee_flag,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_healthcheckusa_transactions_inserts.bk_hash,
       #s_healthcheckusa_transactions_inserts.order_number,
       #s_healthcheckusa_transactions_inserts.sku,
       #s_healthcheckusa_transactions_inserts.transaction_type,
       #s_healthcheckusa_transactions_inserts.transaction_date,
       #s_healthcheckusa_transactions_inserts.quantity,
       #s_healthcheckusa_transactions_inserts.item_amount,
       #s_healthcheckusa_transactions_inserts.item_discount,
       #s_healthcheckusa_transactions_inserts.order_for_employee_flag,
       #s_healthcheckusa_transactions_inserts.dummy_modified_date_time,
       case when s_healthcheckusa_transactions.s_healthcheckusa_transactions_id is null then isnull(#s_healthcheckusa_transactions_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       31,
       #s_healthcheckusa_transactions_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_healthcheckusa_transactions_inserts
  left join p_healthcheckusa_transactions
    on #s_healthcheckusa_transactions_inserts.bk_hash = p_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = s_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.s_healthcheckusa_transactions_id = s_healthcheckusa_transactions.s_healthcheckusa_transactions_id
 where s_healthcheckusa_transactions.s_healthcheckusa_transactions_id is null
    or (s_healthcheckusa_transactions.s_healthcheckusa_transactions_id is not null
        and s_healthcheckusa_transactions.dv_hash <> #s_healthcheckusa_transactions_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_healthcheckusa_transactions @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_healthcheckusa_transactions @current_dv_batch_id
exec dbo.proc_d_healthcheckusa_transactions_history @current_dv_batch_id

end
