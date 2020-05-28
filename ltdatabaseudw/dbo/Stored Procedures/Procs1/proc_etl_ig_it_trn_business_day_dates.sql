CREATE PROC [dbo].[proc_etl_ig_it_trn_business_day_dates] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Business_Day_Dates

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Business_Day_Dates (
       bk_hash,
       BD_end_dttime,
       BD_start_dttime,
       bus_day_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(bus_day_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       BD_end_dttime,
       BD_start_dttime,
       bus_day_id,
       isnull(cast(stage_ig_it_trn_Business_Day_Dates.BD_start_dttime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_trn_Business_Day_Dates
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_business_day_dates @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_business_day_dates (
       bk_hash,
       bus_day_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_trn_Business_Day_Dates.bk_hash,
       stage_hash_ig_it_trn_Business_Day_Dates.bus_day_id bus_day_id,
       isnull(cast(stage_hash_ig_it_trn_Business_Day_Dates.BD_start_dttime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Business_Day_Dates
  left join h_ig_it_trn_business_day_dates
    on stage_hash_ig_it_trn_Business_Day_Dates.bk_hash = h_ig_it_trn_business_day_dates.bk_hash
 where h_ig_it_trn_business_day_dates_id is null
   and stage_hash_ig_it_trn_Business_Day_Dates.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_it_trn_business_day_dates
if object_id('tempdb..#s_ig_it_trn_business_day_dates_inserts') is not null drop table #s_ig_it_trn_business_day_dates_inserts
create table #s_ig_it_trn_business_day_dates_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Business_Day_Dates.bk_hash,
       stage_hash_ig_it_trn_Business_Day_Dates.BD_end_dttime bd_end_dttime,
       stage_hash_ig_it_trn_Business_Day_Dates.BD_start_dttime bd_start_dttime,
       stage_hash_ig_it_trn_Business_Day_Dates.bus_day_id bus_day_id,
       isnull(cast(stage_hash_ig_it_trn_Business_Day_Dates.BD_start_dttime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Business_Day_Dates.BD_end_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Business_Day_Dates.BD_start_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Business_Day_Dates.bus_day_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Business_Day_Dates
 where stage_hash_ig_it_trn_Business_Day_Dates.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_business_day_dates records
set @insert_date_time = getdate()
insert into s_ig_it_trn_business_day_dates (
       bk_hash,
       bd_end_dttime,
       bd_start_dttime,
       bus_day_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_business_day_dates_inserts.bk_hash,
       #s_ig_it_trn_business_day_dates_inserts.bd_end_dttime,
       #s_ig_it_trn_business_day_dates_inserts.bd_start_dttime,
       #s_ig_it_trn_business_day_dates_inserts.bus_day_id,
       case when s_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id is null then isnull(#s_ig_it_trn_business_day_dates_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_business_day_dates_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_business_day_dates_inserts
  left join p_ig_it_trn_business_day_dates
    on #s_ig_it_trn_business_day_dates_inserts.bk_hash = p_ig_it_trn_business_day_dates.bk_hash
   and p_ig_it_trn_business_day_dates.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_business_day_dates
    on p_ig_it_trn_business_day_dates.bk_hash = s_ig_it_trn_business_day_dates.bk_hash
   and p_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id = s_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id
 where s_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id is null
    or (s_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id is not null
        and s_ig_it_trn_business_day_dates.dv_hash <> #s_ig_it_trn_business_day_dates_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_business_day_dates @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_business_day_dates @current_dv_batch_id

end
