CREATE PROC [dbo].[proc_etl_exerp_inventory_transaction_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_inventory_transaction_log

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_inventory_transaction_log (
       bk_hash,
       id,
       inventory_id,
       inventory_name,
       type,
       comment,
       product_id,
       book_datetime,
       quantity,
       unit_value,
       balance_quantity,
       balance_value,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       inventory_id,
       inventory_name,
       type,
       comment,
       product_id,
       book_datetime,
       quantity,
       unit_value,
       balance_quantity,
       balance_value,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_inventory_transaction_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_inventory_transaction_log
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_inventory_transaction_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_inventory_transaction_log (
       bk_hash,
       inventory_transaction_log_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_inventory_transaction_log.bk_hash,
       stage_hash_exerp_inventory_transaction_log.id inventory_transaction_log_id,
       isnull(cast(stage_hash_exerp_inventory_transaction_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_inventory_transaction_log
  left join h_exerp_inventory_transaction_log
    on stage_hash_exerp_inventory_transaction_log.bk_hash = h_exerp_inventory_transaction_log.bk_hash
 where h_exerp_inventory_transaction_log_id is null
   and stage_hash_exerp_inventory_transaction_log.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_inventory_transaction_log
if object_id('tempdb..#l_exerp_inventory_transaction_log_inserts') is not null drop table #l_exerp_inventory_transaction_log_inserts
create table #l_exerp_inventory_transaction_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_inventory_transaction_log.bk_hash,
       stage_hash_exerp_inventory_transaction_log.id inventory_transaction_log_id,
       stage_hash_exerp_inventory_transaction_log.inventory_id inventory_id,
       stage_hash_exerp_inventory_transaction_log.product_id product_id,
       stage_hash_exerp_inventory_transaction_log.center_id center_id,
       isnull(cast(stage_hash_exerp_inventory_transaction_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.inventory_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_inventory_transaction_log.product_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_inventory_transaction_log
 where stage_hash_exerp_inventory_transaction_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_inventory_transaction_log records
set @insert_date_time = getdate()
insert into l_exerp_inventory_transaction_log (
       bk_hash,
       inventory_transaction_log_id,
       inventory_id,
       product_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_inventory_transaction_log_inserts.bk_hash,
       #l_exerp_inventory_transaction_log_inserts.inventory_transaction_log_id,
       #l_exerp_inventory_transaction_log_inserts.inventory_id,
       #l_exerp_inventory_transaction_log_inserts.product_id,
       #l_exerp_inventory_transaction_log_inserts.center_id,
       case when l_exerp_inventory_transaction_log.l_exerp_inventory_transaction_log_id is null then isnull(#l_exerp_inventory_transaction_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_inventory_transaction_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_inventory_transaction_log_inserts
  left join p_exerp_inventory_transaction_log
    on #l_exerp_inventory_transaction_log_inserts.bk_hash = p_exerp_inventory_transaction_log.bk_hash
   and p_exerp_inventory_transaction_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_inventory_transaction_log
    on p_exerp_inventory_transaction_log.bk_hash = l_exerp_inventory_transaction_log.bk_hash
   and p_exerp_inventory_transaction_log.l_exerp_inventory_transaction_log_id = l_exerp_inventory_transaction_log.l_exerp_inventory_transaction_log_id
 where l_exerp_inventory_transaction_log.l_exerp_inventory_transaction_log_id is null
    or (l_exerp_inventory_transaction_log.l_exerp_inventory_transaction_log_id is not null
        and l_exerp_inventory_transaction_log.dv_hash <> #l_exerp_inventory_transaction_log_inserts.source_hash)

--calculate hash and lookup to current s_exerp_inventory_transaction_log
if object_id('tempdb..#s_exerp_inventory_transaction_log_inserts') is not null drop table #s_exerp_inventory_transaction_log_inserts
create table #s_exerp_inventory_transaction_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_inventory_transaction_log.bk_hash,
       stage_hash_exerp_inventory_transaction_log.id inventory_transaction_log_id,
       stage_hash_exerp_inventory_transaction_log.inventory_name inventory_name,
       stage_hash_exerp_inventory_transaction_log.type type,
       stage_hash_exerp_inventory_transaction_log.comment comment,
       stage_hash_exerp_inventory_transaction_log.book_datetime book_datetime,
       stage_hash_exerp_inventory_transaction_log.quantity quantity,
       stage_hash_exerp_inventory_transaction_log.unit_value unit_value,
       stage_hash_exerp_inventory_transaction_log.balance_quantity balance_quantity,
       stage_hash_exerp_inventory_transaction_log.balance_value balance_value,
       stage_hash_exerp_inventory_transaction_log.ets ets,
       stage_hash_exerp_inventory_transaction_log.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_inventory_transaction_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_inventory_transaction_log.inventory_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_inventory_transaction_log.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_inventory_transaction_log.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_inventory_transaction_log.book_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.quantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.unit_value as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.balance_quantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.balance_value as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_inventory_transaction_log.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_inventory_transaction_log
 where stage_hash_exerp_inventory_transaction_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_inventory_transaction_log records
set @insert_date_time = getdate()
insert into s_exerp_inventory_transaction_log (
       bk_hash,
       inventory_transaction_log_id,
       inventory_name,
       type,
       comment,
       book_datetime,
       quantity,
       unit_value,
       balance_quantity,
       balance_value,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_inventory_transaction_log_inserts.bk_hash,
       #s_exerp_inventory_transaction_log_inserts.inventory_transaction_log_id,
       #s_exerp_inventory_transaction_log_inserts.inventory_name,
       #s_exerp_inventory_transaction_log_inserts.type,
       #s_exerp_inventory_transaction_log_inserts.comment,
       #s_exerp_inventory_transaction_log_inserts.book_datetime,
       #s_exerp_inventory_transaction_log_inserts.quantity,
       #s_exerp_inventory_transaction_log_inserts.unit_value,
       #s_exerp_inventory_transaction_log_inserts.balance_quantity,
       #s_exerp_inventory_transaction_log_inserts.balance_value,
       #s_exerp_inventory_transaction_log_inserts.ets,
       #s_exerp_inventory_transaction_log_inserts.dummy_modified_date_time,
       case when s_exerp_inventory_transaction_log.s_exerp_inventory_transaction_log_id is null then isnull(#s_exerp_inventory_transaction_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_inventory_transaction_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_inventory_transaction_log_inserts
  left join p_exerp_inventory_transaction_log
    on #s_exerp_inventory_transaction_log_inserts.bk_hash = p_exerp_inventory_transaction_log.bk_hash
   and p_exerp_inventory_transaction_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_inventory_transaction_log
    on p_exerp_inventory_transaction_log.bk_hash = s_exerp_inventory_transaction_log.bk_hash
   and p_exerp_inventory_transaction_log.s_exerp_inventory_transaction_log_id = s_exerp_inventory_transaction_log.s_exerp_inventory_transaction_log_id
 where s_exerp_inventory_transaction_log.s_exerp_inventory_transaction_log_id is null
    or (s_exerp_inventory_transaction_log.s_exerp_inventory_transaction_log_id is not null
        and s_exerp_inventory_transaction_log.dv_hash <> #s_exerp_inventory_transaction_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_inventory_transaction_log @current_dv_batch_id

end
