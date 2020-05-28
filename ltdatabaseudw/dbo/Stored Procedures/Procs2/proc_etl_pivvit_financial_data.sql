CREATE PROC [dbo].[proc_etl_pivvit_financial_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_pivvit_financial_data

set @insert_date_time = getdate()
insert into dbo.stage_hash_pivvit_financial_data (
       bk_hash,
       company_id,
       cost_center_id,
       currency_id,
       posted_date,
       club_id,
       pivvit_order_number,
       pivvit_line_number,
       product_amount,
       tax_amount,
       discount_amount,
       discount_reason_text,
       transaction_lineamount,
       transaction_line_category_id,
       transaction_memo,
       mms_member_id,
       mms_product_code,
       offering_id,
       tender_type_id,
       transaction_date,
       transaction_id,
       transaction_amount,
       batch_id,
       batch_close_datetime,
       batch_submitted_datetime,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(batch_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(transaction_id,'z#@$k%&P')+'P%#&z$@k'+isnull(pivvit_order_number,'z#@$k%&P')+'P%#&z$@k'+isnull(pivvit_line_number,'z#@$k%&P'))),2) bk_hash,
       company_id,
       cost_center_id,
       currency_id,
       posted_date,
       club_id,
       pivvit_order_number,
       pivvit_line_number,
       product_amount,
       tax_amount,
       discount_amount,
       discount_reason_text,
       transaction_lineamount,
       transaction_line_category_id,
       transaction_memo,
       mms_member_id,
       mms_product_code,
       offering_id,
       tender_type_id,
       transaction_date,
       transaction_id,
       transaction_amount,
       batch_id,
       batch_close_datetime,
       batch_submitted_datetime,
       dummy_modified_date_time,
       isnull(cast(stage_pivvit_financial_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_pivvit_financial_data
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_pivvit_financial_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_pivvit_financial_data (
       bk_hash,
       batch_id,
       transaction_id,
       pivvit_order_number,
       pivvit_line_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_pivvit_financial_data.bk_hash,
       stage_hash_pivvit_financial_data.batch_id batch_id,
       stage_hash_pivvit_financial_data.transaction_id transaction_id,
       stage_hash_pivvit_financial_data.pivvit_order_number pivvit_order_number,
       stage_hash_pivvit_financial_data.pivvit_line_number pivvit_line_number,
       isnull(cast(stage_hash_pivvit_financial_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       40,
       @insert_date_time,
       @user
  from stage_hash_pivvit_financial_data
  left join h_pivvit_financial_data
    on stage_hash_pivvit_financial_data.bk_hash = h_pivvit_financial_data.bk_hash
 where h_pivvit_financial_data_id is null
   and stage_hash_pivvit_financial_data.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_pivvit_financial_data
if object_id('tempdb..#l_pivvit_financial_data_inserts') is not null drop table #l_pivvit_financial_data_inserts
create table #l_pivvit_financial_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_pivvit_financial_data.bk_hash,
       stage_hash_pivvit_financial_data.batch_id batch_id,
       stage_hash_pivvit_financial_data.transaction_id transaction_id,
       stage_hash_pivvit_financial_data.pivvit_order_number pivvit_order_number,
       stage_hash_pivvit_financial_data.pivvit_line_number pivvit_line_number,
       stage_hash_pivvit_financial_data.company_id company_id,
       stage_hash_pivvit_financial_data.cost_center_id cost_center_id,
       stage_hash_pivvit_financial_data.currency_id currency_id,
       stage_hash_pivvit_financial_data.club_id club_id,
       stage_hash_pivvit_financial_data.offering_id offering_id,
       stage_hash_pivvit_financial_data.tender_type_id tender_type_id,
       stage_hash_pivvit_financial_data.mms_product_code mms_product_code,
       isnull(cast(stage_hash_pivvit_financial_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.batch_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.transaction_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.pivvit_order_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.pivvit_line_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.cost_center_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.currency_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.club_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.offering_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.tender_type_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.mms_product_code,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_pivvit_financial_data
 where stage_hash_pivvit_financial_data.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_pivvit_financial_data records
set @insert_date_time = getdate()
insert into l_pivvit_financial_data (
       bk_hash,
       batch_id,
       transaction_id,
       pivvit_order_number,
       pivvit_line_number,
       company_id,
       cost_center_id,
       currency_id,
       club_id,
       offering_id,
       tender_type_id,
       mms_product_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_pivvit_financial_data_inserts.bk_hash,
       #l_pivvit_financial_data_inserts.batch_id,
       #l_pivvit_financial_data_inserts.transaction_id,
       #l_pivvit_financial_data_inserts.pivvit_order_number,
       #l_pivvit_financial_data_inserts.pivvit_line_number,
       #l_pivvit_financial_data_inserts.company_id,
       #l_pivvit_financial_data_inserts.cost_center_id,
       #l_pivvit_financial_data_inserts.currency_id,
       #l_pivvit_financial_data_inserts.club_id,
       #l_pivvit_financial_data_inserts.offering_id,
       #l_pivvit_financial_data_inserts.tender_type_id,
       #l_pivvit_financial_data_inserts.mms_product_code,
       case when l_pivvit_financial_data.l_pivvit_financial_data_id is null then isnull(#l_pivvit_financial_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #l_pivvit_financial_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_pivvit_financial_data_inserts
  left join p_pivvit_financial_data
    on #l_pivvit_financial_data_inserts.bk_hash = p_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_pivvit_financial_data
    on p_pivvit_financial_data.bk_hash = l_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.l_pivvit_financial_data_id = l_pivvit_financial_data.l_pivvit_financial_data_id
 where l_pivvit_financial_data.l_pivvit_financial_data_id is null
    or (l_pivvit_financial_data.l_pivvit_financial_data_id is not null
        and l_pivvit_financial_data.dv_hash <> #l_pivvit_financial_data_inserts.source_hash)

--calculate hash and lookup to current s_pivvit_financial_data
if object_id('tempdb..#s_pivvit_financial_data_inserts') is not null drop table #s_pivvit_financial_data_inserts
create table #s_pivvit_financial_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_pivvit_financial_data.bk_hash,
       stage_hash_pivvit_financial_data.transaction_id transaction_id,
       stage_hash_pivvit_financial_data.pivvit_order_number pivvit_order_number,
       stage_hash_pivvit_financial_data.pivvit_line_number pivvit_line_number,
       stage_hash_pivvit_financial_data.posted_date posted_date,
       stage_hash_pivvit_financial_data.offering_id offering_id,
       stage_hash_pivvit_financial_data.tender_type_id tender_type_id,
       stage_hash_pivvit_financial_data.transaction_amount transaction_amount,
       stage_hash_pivvit_financial_data.transaction_lineamount transaction_lineamount,
       stage_hash_pivvit_financial_data.tax_amount tax_amount,
       stage_hash_pivvit_financial_data.transaction_date transaction_date,
       stage_hash_pivvit_financial_data.transaction_line_category_id transaction_line_category_id,
       stage_hash_pivvit_financial_data.transaction_memo transaction_memo,
       stage_hash_pivvit_financial_data.discount_amount discount_amount,
       stage_hash_pivvit_financial_data.batch_id batch_id,
       stage_hash_pivvit_financial_data.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_pivvit_financial_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.transaction_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.pivvit_order_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.pivvit_line_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.posted_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.offering_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.tender_type_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.transaction_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.transaction_lineamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.transaction_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.transaction_line_category_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_pivvit_financial_data.transaction_memo,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pivvit_financial_data.batch_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_pivvit_financial_data.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_pivvit_financial_data
 where stage_hash_pivvit_financial_data.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_pivvit_financial_data records
set @insert_date_time = getdate()
insert into s_pivvit_financial_data (
       bk_hash,
       transaction_id,
       pivvit_order_number,
       pivvit_line_number,
       posted_date,
       offering_id,
       tender_type_id,
       transaction_amount,
       transaction_lineamount,
       tax_amount,
       transaction_date,
       transaction_line_category_id,
       transaction_memo,
       discount_amount,
       batch_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_pivvit_financial_data_inserts.bk_hash,
       #s_pivvit_financial_data_inserts.transaction_id,
       #s_pivvit_financial_data_inserts.pivvit_order_number,
       #s_pivvit_financial_data_inserts.pivvit_line_number,
       #s_pivvit_financial_data_inserts.posted_date,
       #s_pivvit_financial_data_inserts.offering_id,
       #s_pivvit_financial_data_inserts.tender_type_id,
       #s_pivvit_financial_data_inserts.transaction_amount,
       #s_pivvit_financial_data_inserts.transaction_lineamount,
       #s_pivvit_financial_data_inserts.tax_amount,
       #s_pivvit_financial_data_inserts.transaction_date,
       #s_pivvit_financial_data_inserts.transaction_line_category_id,
       #s_pivvit_financial_data_inserts.transaction_memo,
       #s_pivvit_financial_data_inserts.discount_amount,
       #s_pivvit_financial_data_inserts.batch_id,
       #s_pivvit_financial_data_inserts.dummy_modified_date_time,
       case when s_pivvit_financial_data.s_pivvit_financial_data_id is null then isnull(#s_pivvit_financial_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       40,
       #s_pivvit_financial_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_pivvit_financial_data_inserts
  left join p_pivvit_financial_data
    on #s_pivvit_financial_data_inserts.bk_hash = p_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_pivvit_financial_data
    on p_pivvit_financial_data.bk_hash = s_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.s_pivvit_financial_data_id = s_pivvit_financial_data.s_pivvit_financial_data_id
 where s_pivvit_financial_data.s_pivvit_financial_data_id is null
    or (s_pivvit_financial_data.s_pivvit_financial_data_id is not null
        and s_pivvit_financial_data.dv_hash <> #s_pivvit_financial_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_pivvit_financial_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_pivvit_financial_data @current_dv_batch_id

--run fact procs
exec dbo.proc_fact_pivvit_financial_data @current_dv_batch_id
end
