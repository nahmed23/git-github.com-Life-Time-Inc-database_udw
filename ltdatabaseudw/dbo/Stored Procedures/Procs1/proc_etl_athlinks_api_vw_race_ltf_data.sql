CREATE PROC [dbo].[proc_etl_athlinks_api_vw_race_ltf_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_athlinks_api_vw_Race_LTFData

set @insert_date_time = getdate()
insert into dbo.stage_hash_athlinks_api_vw_Race_LTFData (
       bk_hash,
       RaceID,
       RaceName,
       RaceDate,
       RaceEndDate,
       City,
       StateProvID,
       StateProvName,
       StateProvAbbrev,
       CountryID,
       CountryID3,
       CountryName,
       RaceCompanyID,
       DateSort,
       WebSite,
       Status,
       Elevation,
       MasterID,
       ResultCount,
       Latitude,
       Longitude,
       Temperature,
       WeatherNotes,
       CreateDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(RaceID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       RaceID,
       RaceName,
       RaceDate,
       RaceEndDate,
       City,
       StateProvID,
       StateProvName,
       StateProvAbbrev,
       CountryID,
       CountryID3,
       CountryName,
       RaceCompanyID,
       DateSort,
       WebSite,
       Status,
       Elevation,
       MasterID,
       ResultCount,
       Latitude,
       Longitude,
       Temperature,
       WeatherNotes,
       CreateDate,
       isnull(cast(stage_athlinks_api_vw_Race_LTFData.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_athlinks_api_vw_Race_LTFData
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_athlinks_api_vw_race_ltf_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_athlinks_api_vw_race_ltf_data (
       bk_hash,
       race_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_athlinks_api_vw_Race_LTFData.bk_hash,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceID race_id,
       isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_athlinks_api_vw_Race_LTFData
  left join h_athlinks_api_vw_race_ltf_data
    on stage_hash_athlinks_api_vw_Race_LTFData.bk_hash = h_athlinks_api_vw_race_ltf_data.bk_hash
 where h_athlinks_api_vw_race_ltf_data_id is null
   and stage_hash_athlinks_api_vw_Race_LTFData.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_athlinks_api_vw_race_ltf_data
if object_id('tempdb..#l_athlinks_api_vw_race_ltf_data_inserts') is not null drop table #l_athlinks_api_vw_race_ltf_data_inserts
create table #l_athlinks_api_vw_race_ltf_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_Race_LTFData.bk_hash,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceID race_id,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceCompanyID race_company_id,
       stage_hash_athlinks_api_vw_Race_LTFData.MasterID master_id,
       isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.RaceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.RaceCompanyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.MasterID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_Race_LTFData
 where stage_hash_athlinks_api_vw_Race_LTFData.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_athlinks_api_vw_race_ltf_data records
set @insert_date_time = getdate()
insert into l_athlinks_api_vw_race_ltf_data (
       bk_hash,
       race_id,
       race_company_id,
       master_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_athlinks_api_vw_race_ltf_data_inserts.bk_hash,
       #l_athlinks_api_vw_race_ltf_data_inserts.race_id,
       #l_athlinks_api_vw_race_ltf_data_inserts.race_company_id,
       #l_athlinks_api_vw_race_ltf_data_inserts.master_id,
       case when l_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id is null then isnull(#l_athlinks_api_vw_race_ltf_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #l_athlinks_api_vw_race_ltf_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_athlinks_api_vw_race_ltf_data_inserts
  left join p_athlinks_api_vw_race_ltf_data
    on #l_athlinks_api_vw_race_ltf_data_inserts.bk_hash = p_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_athlinks_api_vw_race_ltf_data
    on p_athlinks_api_vw_race_ltf_data.bk_hash = l_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id = l_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id
 where l_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id is null
    or (l_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id is not null
        and l_athlinks_api_vw_race_ltf_data.dv_hash <> #l_athlinks_api_vw_race_ltf_data_inserts.source_hash)

--calculate hash and lookup to current s_athlinks_api_vw_race_ltf_data
if object_id('tempdb..#s_athlinks_api_vw_race_ltf_data_inserts') is not null drop table #s_athlinks_api_vw_race_ltf_data_inserts
create table #s_athlinks_api_vw_race_ltf_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_Race_LTFData.bk_hash,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceID race_id,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceName race_name,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceDate race_date,
       stage_hash_athlinks_api_vw_Race_LTFData.RaceEndDate race_end_date,
       stage_hash_athlinks_api_vw_Race_LTFData.City city,
       stage_hash_athlinks_api_vw_Race_LTFData.StateProvID state_prov_id,
       stage_hash_athlinks_api_vw_Race_LTFData.StateProvName state_prov_name,
       stage_hash_athlinks_api_vw_Race_LTFData.StateProvAbbrev state_prov_abbrev,
       stage_hash_athlinks_api_vw_Race_LTFData.CountryID country_id,
       stage_hash_athlinks_api_vw_Race_LTFData.CountryID3 country_id_3,
       stage_hash_athlinks_api_vw_Race_LTFData.CountryName country_name,
       stage_hash_athlinks_api_vw_Race_LTFData.DateSort date_sort,
       stage_hash_athlinks_api_vw_Race_LTFData.WebSite website,
       stage_hash_athlinks_api_vw_Race_LTFData.Status status,
       stage_hash_athlinks_api_vw_Race_LTFData.Elevation elevation,
       stage_hash_athlinks_api_vw_Race_LTFData.ResultCount result_count,
       stage_hash_athlinks_api_vw_Race_LTFData.Latitude latitude,
       stage_hash_athlinks_api_vw_Race_LTFData.Longitude longitude,
       stage_hash_athlinks_api_vw_Race_LTFData.Temperature temperature,
       stage_hash_athlinks_api_vw_Race_LTFData.WeatherNotes weather_notes,
       stage_hash_athlinks_api_vw_Race_LTFData.CreateDate create_date,
       isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.RaceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.RaceName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_Race_LTFData.RaceDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_Race_LTFData.RaceEndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.City,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.StateProvID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.StateProvName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.StateProvAbbrev,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.CountryID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.CountryID3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.CountryName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.DateSort,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.WebSite,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.Status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.Elevation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.ResultCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.Latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.Longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_Race_LTFData.Temperature as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_Race_LTFData.WeatherNotes,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_Race_LTFData
 where stage_hash_athlinks_api_vw_Race_LTFData.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_athlinks_api_vw_race_ltf_data records
set @insert_date_time = getdate()
insert into s_athlinks_api_vw_race_ltf_data (
       bk_hash,
       race_id,
       race_name,
       race_date,
       race_end_date,
       city,
       state_prov_id,
       state_prov_name,
       state_prov_abbrev,
       country_id,
       country_id_3,
       country_name,
       date_sort,
       website,
       status,
       elevation,
       result_count,
       latitude,
       longitude,
       temperature,
       weather_notes,
       create_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_athlinks_api_vw_race_ltf_data_inserts.bk_hash,
       #s_athlinks_api_vw_race_ltf_data_inserts.race_id,
       #s_athlinks_api_vw_race_ltf_data_inserts.race_name,
       #s_athlinks_api_vw_race_ltf_data_inserts.race_date,
       #s_athlinks_api_vw_race_ltf_data_inserts.race_end_date,
       #s_athlinks_api_vw_race_ltf_data_inserts.city,
       #s_athlinks_api_vw_race_ltf_data_inserts.state_prov_id,
       #s_athlinks_api_vw_race_ltf_data_inserts.state_prov_name,
       #s_athlinks_api_vw_race_ltf_data_inserts.state_prov_abbrev,
       #s_athlinks_api_vw_race_ltf_data_inserts.country_id,
       #s_athlinks_api_vw_race_ltf_data_inserts.country_id_3,
       #s_athlinks_api_vw_race_ltf_data_inserts.country_name,
       #s_athlinks_api_vw_race_ltf_data_inserts.date_sort,
       #s_athlinks_api_vw_race_ltf_data_inserts.website,
       #s_athlinks_api_vw_race_ltf_data_inserts.status,
       #s_athlinks_api_vw_race_ltf_data_inserts.elevation,
       #s_athlinks_api_vw_race_ltf_data_inserts.result_count,
       #s_athlinks_api_vw_race_ltf_data_inserts.latitude,
       #s_athlinks_api_vw_race_ltf_data_inserts.longitude,
       #s_athlinks_api_vw_race_ltf_data_inserts.temperature,
       #s_athlinks_api_vw_race_ltf_data_inserts.weather_notes,
       #s_athlinks_api_vw_race_ltf_data_inserts.create_date,
       case when s_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id is null then isnull(#s_athlinks_api_vw_race_ltf_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_athlinks_api_vw_race_ltf_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_athlinks_api_vw_race_ltf_data_inserts
  left join p_athlinks_api_vw_race_ltf_data
    on #s_athlinks_api_vw_race_ltf_data_inserts.bk_hash = p_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_athlinks_api_vw_race_ltf_data
    on p_athlinks_api_vw_race_ltf_data.bk_hash = s_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id = s_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id
 where s_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id is null
    or (s_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id is not null
        and s_athlinks_api_vw_race_ltf_data.dv_hash <> #s_athlinks_api_vw_race_ltf_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_athlinks_api_vw_race_ltf_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_athlinks_api_vw_race_ltf_data @current_dv_batch_id

end
