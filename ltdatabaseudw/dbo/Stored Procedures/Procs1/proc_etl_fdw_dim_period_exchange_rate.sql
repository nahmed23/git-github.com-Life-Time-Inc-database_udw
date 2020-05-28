CREATE PROC [dbo].[proc_etl_fdw_dim_period_exchange_rate] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fdw_DimPeriodExchangeRate

set @insert_date_time = getdate()
insert into dbo.stage_hash_fdw_DimPeriodExchangeRate (
       bk_hash,
       DimPeriodExchangeRateKey,
       DimAccountingPeriodKey,
       FromCurrencyCode,
       ToCurrencyCode,
       BudgetRate,
       PlanRate,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdatedUser,
       BatchID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(DimAccountingPeriodKey as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(FromCurrencyCode,'z#@$k%&P')+'P%#&z$@k'+isnull(ToCurrencyCode,'z#@$k%&P'))),2) bk_hash,
       DimPeriodExchangeRateKey,
       DimAccountingPeriodKey,
       FromCurrencyCode,
       ToCurrencyCode,
       BudgetRate,
       PlanRate,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdatedUser,
       BatchID,
       isnull(cast(stage_fdw_DimPeriodExchangeRate.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_fdw_DimPeriodExchangeRate
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fdw_dim_period_exchange_rate @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fdw_dim_period_exchange_rate (
       bk_hash,
       dim_accounting_period_key,
       from_currency_code,
       to_currency_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fdw_DimPeriodExchangeRate.bk_hash,
       stage_hash_fdw_DimPeriodExchangeRate.DimAccountingPeriodKey dim_accounting_period_key,
       stage_hash_fdw_DimPeriodExchangeRate.FromCurrencyCode from_currency_code,
       stage_hash_fdw_DimPeriodExchangeRate.ToCurrencyCode to_currency_code,
       isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       23,
       @insert_date_time,
       @user
  from stage_hash_fdw_DimPeriodExchangeRate
  left join h_fdw_dim_period_exchange_rate
    on stage_hash_fdw_DimPeriodExchangeRate.bk_hash = h_fdw_dim_period_exchange_rate.bk_hash
 where h_fdw_dim_period_exchange_rate_id is null
   and stage_hash_fdw_DimPeriodExchangeRate.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_fdw_dim_period_exchange_rate
if object_id('tempdb..#s_fdw_dim_period_exchange_rate_inserts') is not null drop table #s_fdw_dim_period_exchange_rate_inserts
create table #s_fdw_dim_period_exchange_rate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fdw_DimPeriodExchangeRate.bk_hash,
       stage_hash_fdw_DimPeriodExchangeRate.DimPeriodExchangeRateKey dim_period_exchange_rate_key,
       stage_hash_fdw_DimPeriodExchangeRate.DimAccountingPeriodKey dim_accounting_period_key,
       stage_hash_fdw_DimPeriodExchangeRate.FromCurrencyCode from_currency_code,
       stage_hash_fdw_DimPeriodExchangeRate.ToCurrencyCode to_currency_code,
       stage_hash_fdw_DimPeriodExchangeRate.BudgetRate budget_rate,
       stage_hash_fdw_DimPeriodExchangeRate.PlanRate plan_rate,
       stage_hash_fdw_DimPeriodExchangeRate.InsertedDateTime inserted_date_time,
       stage_hash_fdw_DimPeriodExchangeRate.InsertUser insert_user,
       stage_hash_fdw_DimPeriodExchangeRate.UpdatedDateTime updated_date_time,
       stage_hash_fdw_DimPeriodExchangeRate.UpdatedUser updated_user,
       stage_hash_fdw_DimPeriodExchangeRate.BatchID batch_id,
       stage_hash_fdw_DimPeriodExchangeRate.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.DimPeriodExchangeRateKey as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.DimAccountingPeriodKey as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fdw_DimPeriodExchangeRate.FromCurrencyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fdw_DimPeriodExchangeRate.ToCurrencyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.BudgetRate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.PlanRate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fdw_DimPeriodExchangeRate.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fdw_DimPeriodExchangeRate.InsertUser,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fdw_DimPeriodExchangeRate.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fdw_DimPeriodExchangeRate.UpdatedUser,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fdw_DimPeriodExchangeRate.BatchID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fdw_DimPeriodExchangeRate
 where stage_hash_fdw_DimPeriodExchangeRate.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fdw_dim_period_exchange_rate records
set @insert_date_time = getdate()
insert into s_fdw_dim_period_exchange_rate (
       bk_hash,
       dim_period_exchange_rate_key,
       dim_accounting_period_key,
       from_currency_code,
       to_currency_code,
       budget_rate,
       plan_rate,
       inserted_date_time,
       insert_user,
       updated_date_time,
       updated_user,
       batch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fdw_dim_period_exchange_rate_inserts.bk_hash,
       #s_fdw_dim_period_exchange_rate_inserts.dim_period_exchange_rate_key,
       #s_fdw_dim_period_exchange_rate_inserts.dim_accounting_period_key,
       #s_fdw_dim_period_exchange_rate_inserts.from_currency_code,
       #s_fdw_dim_period_exchange_rate_inserts.to_currency_code,
       #s_fdw_dim_period_exchange_rate_inserts.budget_rate,
       #s_fdw_dim_period_exchange_rate_inserts.plan_rate,
       #s_fdw_dim_period_exchange_rate_inserts.inserted_date_time,
       #s_fdw_dim_period_exchange_rate_inserts.insert_user,
       #s_fdw_dim_period_exchange_rate_inserts.updated_date_time,
       #s_fdw_dim_period_exchange_rate_inserts.updated_user,
       #s_fdw_dim_period_exchange_rate_inserts.batch_id,
       case when s_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id is null then isnull(#s_fdw_dim_period_exchange_rate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       23,
       #s_fdw_dim_period_exchange_rate_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fdw_dim_period_exchange_rate_inserts
  left join p_fdw_dim_period_exchange_rate
    on #s_fdw_dim_period_exchange_rate_inserts.bk_hash = p_fdw_dim_period_exchange_rate.bk_hash
   and p_fdw_dim_period_exchange_rate.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fdw_dim_period_exchange_rate
    on p_fdw_dim_period_exchange_rate.bk_hash = s_fdw_dim_period_exchange_rate.bk_hash
   and p_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id = s_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id
 where s_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id is null
    or (s_fdw_dim_period_exchange_rate.s_fdw_dim_period_exchange_rate_id is not null
        and s_fdw_dim_period_exchange_rate.dv_hash <> #s_fdw_dim_period_exchange_rate_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fdw_dim_period_exchange_rate @current_dv_batch_id

end
