CREATE PROC [dbo].[proc_etl_exerp_sale_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_sale_log

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_sale_log (
       bk_hash,
       id,
       center_id,
       sale_type,
       person_id,
       company_id,
       is_company,
       sale_person_id,
       entry_datetime,
       book_datetime,
       product_center,
       product_id,
       product_type,
       product_normal_price,
       quantity,
       net_amount,
       vat_amount,
       total_amount,
       sponsor_sale_log_id,
       gl_debit_account,
       gl_credit_account,
       sale_commission,
       sale_units,
       period_commission,
       source_type,
       credit_sale_log_id,
       sale_id,
       cash_register_center_id,
       tts,
       ets,
       flat_rate_commission,
       external_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       center_id,
       sale_type,
       person_id,
       company_id,
       is_company,
       sale_person_id,
       entry_datetime,
       book_datetime,
       product_center,
       product_id,
       product_type,
       product_normal_price,
       quantity,
       net_amount,
       vat_amount,
       total_amount,
       sponsor_sale_log_id,
       gl_debit_account,
       gl_credit_account,
       sale_commission,
       sale_units,
       period_commission,
       source_type,
       credit_sale_log_id,
       sale_id,
       cash_register_center_id,
       tts,
       ets,
       flat_rate_commission,
       external_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_sale_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_sale_log
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_sale_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_sale_log (
       bk_hash,
       sale_log_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_sale_log.bk_hash,
       stage_hash_exerp_sale_log.id sale_log_id,
       isnull(cast(stage_hash_exerp_sale_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_sale_log
  left join h_exerp_sale_log
    on stage_hash_exerp_sale_log.bk_hash = h_exerp_sale_log.bk_hash
 where h_exerp_sale_log_id is null
   and stage_hash_exerp_sale_log.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_sale_log
if object_id('tempdb..#l_exerp_sale_log_inserts') is not null drop table #l_exerp_sale_log_inserts
create table #l_exerp_sale_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_sale_log.bk_hash,
       stage_hash_exerp_sale_log.id sale_log_id,
       stage_hash_exerp_sale_log.center_id center_id,
       stage_hash_exerp_sale_log.person_id person_id,
       stage_hash_exerp_sale_log.company_id company_id,
       stage_hash_exerp_sale_log.sale_person_id sale_person_id,
       stage_hash_exerp_sale_log.product_id product_id,
       stage_hash_exerp_sale_log.sponsor_sale_log_id sponsor_sale_log_id,
       stage_hash_exerp_sale_log.credit_sale_log_id credit_sale_log_id,
       stage_hash_exerp_sale_log.sale_id sale_id,
       stage_hash_exerp_sale_log.cash_register_center_id cash_register_center_id,
       isnull(cast(stage_hash_exerp_sale_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_sale_log.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.sale_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.product_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.sponsor_sale_log_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.credit_sale_log_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.sale_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.cash_register_center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_sale_log
 where stage_hash_exerp_sale_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_sale_log records
set @insert_date_time = getdate()
insert into l_exerp_sale_log (
       bk_hash,
       sale_log_id,
       center_id,
       person_id,
       company_id,
       sale_person_id,
       product_id,
       sponsor_sale_log_id,
       credit_sale_log_id,
       sale_id,
       cash_register_center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_sale_log_inserts.bk_hash,
       #l_exerp_sale_log_inserts.sale_log_id,
       #l_exerp_sale_log_inserts.center_id,
       #l_exerp_sale_log_inserts.person_id,
       #l_exerp_sale_log_inserts.company_id,
       #l_exerp_sale_log_inserts.sale_person_id,
       #l_exerp_sale_log_inserts.product_id,
       #l_exerp_sale_log_inserts.sponsor_sale_log_id,
       #l_exerp_sale_log_inserts.credit_sale_log_id,
       #l_exerp_sale_log_inserts.sale_id,
       #l_exerp_sale_log_inserts.cash_register_center_id,
       case when l_exerp_sale_log.l_exerp_sale_log_id is null then isnull(#l_exerp_sale_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_sale_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_sale_log_inserts
  left join p_exerp_sale_log
    on #l_exerp_sale_log_inserts.bk_hash = p_exerp_sale_log.bk_hash
   and p_exerp_sale_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_sale_log
    on p_exerp_sale_log.bk_hash = l_exerp_sale_log.bk_hash
   and p_exerp_sale_log.l_exerp_sale_log_id = l_exerp_sale_log.l_exerp_sale_log_id
 where l_exerp_sale_log.l_exerp_sale_log_id is null
    or (l_exerp_sale_log.l_exerp_sale_log_id is not null
        and l_exerp_sale_log.dv_hash <> #l_exerp_sale_log_inserts.source_hash)

--calculate hash and lookup to current l_exerp_sale_log_1
if object_id('tempdb..#l_exerp_sale_log_1_inserts') is not null drop table #l_exerp_sale_log_1_inserts
create table #l_exerp_sale_log_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_sale_log.bk_hash,
       stage_hash_exerp_sale_log.id sale_log_id,
       stage_hash_exerp_sale_log.external_id external_id,
       isnull(cast(stage_hash_exerp_sale_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_sale_log.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.external_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_sale_log
 where stage_hash_exerp_sale_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_sale_log_1 records
set @insert_date_time = getdate()
insert into l_exerp_sale_log_1 (
       bk_hash,
       sale_log_id,
       external_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_sale_log_1_inserts.bk_hash,
       #l_exerp_sale_log_1_inserts.sale_log_id,
       #l_exerp_sale_log_1_inserts.external_id,
       case when l_exerp_sale_log_1.l_exerp_sale_log_1_id is null then isnull(#l_exerp_sale_log_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_sale_log_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_sale_log_1_inserts
  left join p_exerp_sale_log
    on #l_exerp_sale_log_1_inserts.bk_hash = p_exerp_sale_log.bk_hash
   and p_exerp_sale_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_sale_log_1
    on p_exerp_sale_log.bk_hash = l_exerp_sale_log_1.bk_hash
   and p_exerp_sale_log.l_exerp_sale_log_1_id = l_exerp_sale_log_1.l_exerp_sale_log_1_id
 where l_exerp_sale_log_1.l_exerp_sale_log_1_id is null
    or (l_exerp_sale_log_1.l_exerp_sale_log_1_id is not null
        and l_exerp_sale_log_1.dv_hash <> #l_exerp_sale_log_1_inserts.source_hash)

--calculate hash and lookup to current s_exerp_sale_log
if object_id('tempdb..#s_exerp_sale_log_inserts') is not null drop table #s_exerp_sale_log_inserts
create table #s_exerp_sale_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_sale_log.bk_hash,
       stage_hash_exerp_sale_log.id sale_log_id,
       stage_hash_exerp_sale_log.sale_type sale_type,
       stage_hash_exerp_sale_log.is_company is_company,
       stage_hash_exerp_sale_log.entry_datetime entry_datetime,
       stage_hash_exerp_sale_log.book_datetime book_datetime,
       stage_hash_exerp_sale_log.product_center product_center,
       stage_hash_exerp_sale_log.product_type product_type,
       stage_hash_exerp_sale_log.product_normal_price product_normal_price,
       stage_hash_exerp_sale_log.quantity quantity,
       stage_hash_exerp_sale_log.net_amount net_amount,
       stage_hash_exerp_sale_log.vat_amount vat_amount,
       stage_hash_exerp_sale_log.total_amount total_amount,
       stage_hash_exerp_sale_log.gl_debit_account gl_debit_account,
       stage_hash_exerp_sale_log.gl_credit_account gl_credit_account,
       stage_hash_exerp_sale_log.sale_commission sale_commission,
       stage_hash_exerp_sale_log.sale_units sale_units,
       stage_hash_exerp_sale_log.period_commission period_commission,
       stage_hash_exerp_sale_log.source_type source_type,
       stage_hash_exerp_sale_log.tts tts,
       stage_hash_exerp_sale_log.ets ets,
       stage_hash_exerp_sale_log.flat_rate_commission flat_rate_commission,
       stage_hash_exerp_sale_log.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_sale_log.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_sale_log.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.sale_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.is_company,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_sale_log.entry_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_sale_log.book_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.product_center as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.product_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.product_normal_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.quantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.net_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.vat_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.total_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.gl_debit_account,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.gl_credit_account,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.sale_commission as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.sale_units as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.period_commission as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_sale_log.source_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_sale_log.tts,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.ets as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_sale_log.flat_rate_commission as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_sale_log
 where stage_hash_exerp_sale_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_sale_log records
set @insert_date_time = getdate()
insert into s_exerp_sale_log (
       bk_hash,
       sale_log_id,
       sale_type,
       is_company,
       entry_datetime,
       book_datetime,
       product_center,
       product_type,
       product_normal_price,
       quantity,
       net_amount,
       vat_amount,
       total_amount,
       gl_debit_account,
       gl_credit_account,
       sale_commission,
       sale_units,
       period_commission,
       source_type,
       tts,
       ets,
       flat_rate_commission,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_sale_log_inserts.bk_hash,
       #s_exerp_sale_log_inserts.sale_log_id,
       #s_exerp_sale_log_inserts.sale_type,
       #s_exerp_sale_log_inserts.is_company,
       #s_exerp_sale_log_inserts.entry_datetime,
       #s_exerp_sale_log_inserts.book_datetime,
       #s_exerp_sale_log_inserts.product_center,
       #s_exerp_sale_log_inserts.product_type,
       #s_exerp_sale_log_inserts.product_normal_price,
       #s_exerp_sale_log_inserts.quantity,
       #s_exerp_sale_log_inserts.net_amount,
       #s_exerp_sale_log_inserts.vat_amount,
       #s_exerp_sale_log_inserts.total_amount,
       #s_exerp_sale_log_inserts.gl_debit_account,
       #s_exerp_sale_log_inserts.gl_credit_account,
       #s_exerp_sale_log_inserts.sale_commission,
       #s_exerp_sale_log_inserts.sale_units,
       #s_exerp_sale_log_inserts.period_commission,
       #s_exerp_sale_log_inserts.source_type,
       #s_exerp_sale_log_inserts.tts,
       #s_exerp_sale_log_inserts.ets,
       #s_exerp_sale_log_inserts.flat_rate_commission,
       #s_exerp_sale_log_inserts.dummy_modified_date_time,
       case when s_exerp_sale_log.s_exerp_sale_log_id is null then isnull(#s_exerp_sale_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_sale_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_sale_log_inserts
  left join p_exerp_sale_log
    on #s_exerp_sale_log_inserts.bk_hash = p_exerp_sale_log.bk_hash
   and p_exerp_sale_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_sale_log
    on p_exerp_sale_log.bk_hash = s_exerp_sale_log.bk_hash
   and p_exerp_sale_log.s_exerp_sale_log_id = s_exerp_sale_log.s_exerp_sale_log_id
 where s_exerp_sale_log.s_exerp_sale_log_id is null
    or (s_exerp_sale_log.s_exerp_sale_log_id is not null
        and s_exerp_sale_log.dv_hash <> #s_exerp_sale_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_sale_log @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_sale_log @current_dv_batch_id

end
