CREATE PROC [dbo].[proc_etl_forexintegration_exchange_rate] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_forexintegration_ExchangeRate

set @insert_date_time = getdate()
insert into dbo.stage_hash_forexintegration_ExchangeRate (
       bk_hash,
       exchange_rate_id,
       from_exchange_rate_iso_code,
       to_exchange_rate_iso_code,
       rate_type,
       effective_date,
       daily_average_date,
       currency_rate,
       source,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(exchange_rate_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       exchange_rate_id,
       from_exchange_rate_iso_code,
       to_exchange_rate_iso_code,
       rate_type,
       effective_date,
       daily_average_date,
       currency_rate,
       source,
       inserted_date_time,
       updated_date_time,
       isnull(cast(stage_forexintegration_ExchangeRate.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_forexintegration_ExchangeRate
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_forexintegration_exchange_rate @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_forexintegration_exchange_rate (
       bk_hash,
       exchange_rate_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_forexintegration_ExchangeRate.bk_hash,
       stage_hash_forexintegration_ExchangeRate.exchange_rate_id exchange_rate_id,
       isnull(cast(stage_hash_forexintegration_ExchangeRate.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       22,
       @insert_date_time,
       @user
  from stage_hash_forexintegration_ExchangeRate
  left join h_forexintegration_exchange_rate
    on stage_hash_forexintegration_ExchangeRate.bk_hash = h_forexintegration_exchange_rate.bk_hash
 where h_forexintegration_exchange_rate_id is null
   and stage_hash_forexintegration_ExchangeRate.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_forexintegration_exchange_rate
if object_id('tempdb..#l_forexintegration_exchange_rate_inserts') is not null drop table #l_forexintegration_exchange_rate_inserts
create table #l_forexintegration_exchange_rate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_forexintegration_ExchangeRate.bk_hash,
       stage_hash_forexintegration_ExchangeRate.exchange_rate_id exchange_rate_id,
       isnull(cast(stage_hash_forexintegration_ExchangeRate.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_forexintegration_ExchangeRate.exchange_rate_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_forexintegration_ExchangeRate
 where stage_hash_forexintegration_ExchangeRate.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_forexintegration_exchange_rate records
set @insert_date_time = getdate()
insert into l_forexintegration_exchange_rate (
       bk_hash,
       exchange_rate_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_forexintegration_exchange_rate_inserts.bk_hash,
       #l_forexintegration_exchange_rate_inserts.exchange_rate_id,
       case when l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id is null then isnull(#l_forexintegration_exchange_rate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       22,
       #l_forexintegration_exchange_rate_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_forexintegration_exchange_rate_inserts
  left join p_forexintegration_exchange_rate
    on #l_forexintegration_exchange_rate_inserts.bk_hash = p_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_forexintegration_exchange_rate
    on p_forexintegration_exchange_rate.bk_hash = l_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id = l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id
 where l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id is null
    or (l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id is not null
        and l_forexintegration_exchange_rate.dv_hash <> #l_forexintegration_exchange_rate_inserts.source_hash)

--calculate hash and lookup to current s_forexintegration_exchange_rate
if object_id('tempdb..#s_forexintegration_exchange_rate_inserts') is not null drop table #s_forexintegration_exchange_rate_inserts
create table #s_forexintegration_exchange_rate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_forexintegration_ExchangeRate.bk_hash,
       stage_hash_forexintegration_ExchangeRate.exchange_rate_id exchange_rate_id,
       stage_hash_forexintegration_ExchangeRate.from_exchange_rate_iso_code from_exchange_rate_iso_code,
       stage_hash_forexintegration_ExchangeRate.to_exchange_rate_iso_code to_exchange_rate_iso_code,
       stage_hash_forexintegration_ExchangeRate.rate_type rate_type,
       stage_hash_forexintegration_ExchangeRate.effective_date effective_date,
       stage_hash_forexintegration_ExchangeRate.daily_average_date daily_average_date,
       stage_hash_forexintegration_ExchangeRate.currency_rate currency_rate,
       stage_hash_forexintegration_ExchangeRate.source source,
       stage_hash_forexintegration_ExchangeRate.inserted_date_time inserted_date_time,
       stage_hash_forexintegration_ExchangeRate.updated_date_time updated_date_time,
       isnull(cast(stage_hash_forexintegration_ExchangeRate.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_forexintegration_ExchangeRate.exchange_rate_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_forexintegration_ExchangeRate.from_exchange_rate_iso_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_forexintegration_ExchangeRate.to_exchange_rate_iso_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_forexintegration_ExchangeRate.rate_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_forexintegration_ExchangeRate.effective_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_forexintegration_ExchangeRate.daily_average_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_forexintegration_ExchangeRate.currency_rate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_forexintegration_ExchangeRate.source,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_forexintegration_ExchangeRate.inserted_date_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_forexintegration_ExchangeRate.updated_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_forexintegration_ExchangeRate
 where stage_hash_forexintegration_ExchangeRate.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_forexintegration_exchange_rate records
set @insert_date_time = getdate()
insert into s_forexintegration_exchange_rate (
       bk_hash,
       exchange_rate_id,
       from_exchange_rate_iso_code,
       to_exchange_rate_iso_code,
       rate_type,
       effective_date,
       daily_average_date,
       currency_rate,
       source,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_forexintegration_exchange_rate_inserts.bk_hash,
       #s_forexintegration_exchange_rate_inserts.exchange_rate_id,
       #s_forexintegration_exchange_rate_inserts.from_exchange_rate_iso_code,
       #s_forexintegration_exchange_rate_inserts.to_exchange_rate_iso_code,
       #s_forexintegration_exchange_rate_inserts.rate_type,
       #s_forexintegration_exchange_rate_inserts.effective_date,
       #s_forexintegration_exchange_rate_inserts.daily_average_date,
       #s_forexintegration_exchange_rate_inserts.currency_rate,
       #s_forexintegration_exchange_rate_inserts.source,
       #s_forexintegration_exchange_rate_inserts.inserted_date_time,
       #s_forexintegration_exchange_rate_inserts.updated_date_time,
       case when s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id is null then isnull(#s_forexintegration_exchange_rate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       22,
       #s_forexintegration_exchange_rate_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_forexintegration_exchange_rate_inserts
  left join p_forexintegration_exchange_rate
    on #s_forexintegration_exchange_rate_inserts.bk_hash = p_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_forexintegration_exchange_rate
    on p_forexintegration_exchange_rate.bk_hash = s_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id = s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id
 where s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id is null
    or (s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id is not null
        and s_forexintegration_exchange_rate.dv_hash <> #s_forexintegration_exchange_rate_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_forexintegration_exchange_rate @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_forexintegration_exchange_rate @current_dv_batch_id

end
