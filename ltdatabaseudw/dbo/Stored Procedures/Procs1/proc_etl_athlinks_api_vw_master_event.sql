CREATE PROC [dbo].[proc_etl_athlinks_api_vw_master_event] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_athlinks_api_vw_MasterEvent

set @insert_date_time = getdate()
insert into dbo.stage_hash_athlinks_api_vw_MasterEvent (
       bk_hash,
       MasterID,
       NAME,
       ShortUrl,
       RaceCount,
       ResultCount,
       CompanyID,
       LogoPath,
       STATUS,
       ContactName,
       Phone,
       ContactAddress,
       Longitude,
       Latitude,
       Geo,
       Elevation,
       City,
       StateProvID,
       CompanyName,
       StateProvAbbrev,
       CountryID,
       NextDate,
       PrevDate,
       NextRaceID,
       PrevRaceID,
       CuratedDesc,
       Featured,
       CreateDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MasterID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MasterID,
       NAME,
       ShortUrl,
       RaceCount,
       ResultCount,
       CompanyID,
       LogoPath,
       STATUS,
       ContactName,
       Phone,
       ContactAddress,
       Longitude,
       Latitude,
       Geo,
       Elevation,
       City,
       StateProvID,
       CompanyName,
       StateProvAbbrev,
       CountryID,
       NextDate,
       PrevDate,
       NextRaceID,
       PrevRaceID,
       CuratedDesc,
       Featured,
       CreateDate,
       isnull(cast(stage_athlinks_api_vw_MasterEvent.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_athlinks_api_vw_MasterEvent
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_athlinks_api_vw_master_event @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_athlinks_api_vw_master_event (
       bk_hash,
       master_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_athlinks_api_vw_MasterEvent.bk_hash,
       stage_hash_athlinks_api_vw_MasterEvent.MasterID master_id,
       isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_athlinks_api_vw_MasterEvent
  left join h_athlinks_api_vw_master_event
    on stage_hash_athlinks_api_vw_MasterEvent.bk_hash = h_athlinks_api_vw_master_event.bk_hash
 where h_athlinks_api_vw_master_event_id is null
   and stage_hash_athlinks_api_vw_MasterEvent.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_athlinks_api_vw_master_event
if object_id('tempdb..#l_athlinks_api_vw_master_event_inserts') is not null drop table #l_athlinks_api_vw_master_event_inserts
create table #l_athlinks_api_vw_master_event_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_MasterEvent.bk_hash,
       stage_hash_athlinks_api_vw_MasterEvent.MasterID master_id,
       stage_hash_athlinks_api_vw_MasterEvent.CompanyID company_id,
       stage_hash_athlinks_api_vw_MasterEvent.NextRaceID next_race_id,
       stage_hash_athlinks_api_vw_MasterEvent.PrevRaceID prev_race_id,
       isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.MasterID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.CompanyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.NextRaceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.PrevRaceID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_MasterEvent
 where stage_hash_athlinks_api_vw_MasterEvent.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_athlinks_api_vw_master_event records
set @insert_date_time = getdate()
insert into l_athlinks_api_vw_master_event (
       bk_hash,
       master_id,
       company_id,
       next_race_id,
       prev_race_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_athlinks_api_vw_master_event_inserts.bk_hash,
       #l_athlinks_api_vw_master_event_inserts.master_id,
       #l_athlinks_api_vw_master_event_inserts.company_id,
       #l_athlinks_api_vw_master_event_inserts.next_race_id,
       #l_athlinks_api_vw_master_event_inserts.prev_race_id,
       case when l_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id is null then isnull(#l_athlinks_api_vw_master_event_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #l_athlinks_api_vw_master_event_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_athlinks_api_vw_master_event_inserts
  left join p_athlinks_api_vw_master_event
    on #l_athlinks_api_vw_master_event_inserts.bk_hash = p_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_athlinks_api_vw_master_event
    on p_athlinks_api_vw_master_event.bk_hash = l_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id = l_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id
 where l_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id is null
    or (l_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id is not null
        and l_athlinks_api_vw_master_event.dv_hash <> #l_athlinks_api_vw_master_event_inserts.source_hash)

--calculate hash and lookup to current s_athlinks_api_vw_master_event
if object_id('tempdb..#s_athlinks_api_vw_master_event_inserts') is not null drop table #s_athlinks_api_vw_master_event_inserts
create table #s_athlinks_api_vw_master_event_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_MasterEvent.bk_hash,
       stage_hash_athlinks_api_vw_MasterEvent.MasterID master_id,
       stage_hash_athlinks_api_vw_MasterEvent.NAME name,
       stage_hash_athlinks_api_vw_MasterEvent.ShortUrl short_url,
       stage_hash_athlinks_api_vw_MasterEvent.RaceCount race_count,
       stage_hash_athlinks_api_vw_MasterEvent.ResultCount result_count,
       stage_hash_athlinks_api_vw_MasterEvent.LogoPath logo_path,
       stage_hash_athlinks_api_vw_MasterEvent.STATUS status,
       stage_hash_athlinks_api_vw_MasterEvent.ContactName contact_name,
       stage_hash_athlinks_api_vw_MasterEvent.Phone phone,
       stage_hash_athlinks_api_vw_MasterEvent.ContactAddress contact_address,
       stage_hash_athlinks_api_vw_MasterEvent.Longitude longitude,
       stage_hash_athlinks_api_vw_MasterEvent.Latitude latitude,
       stage_hash_athlinks_api_vw_MasterEvent.Geo geo,
       stage_hash_athlinks_api_vw_MasterEvent.Elevation elevation,
       stage_hash_athlinks_api_vw_MasterEvent.City city,
       stage_hash_athlinks_api_vw_MasterEvent.StateProvID state_prov_id,
       stage_hash_athlinks_api_vw_MasterEvent.CompanyName company_name,
       stage_hash_athlinks_api_vw_MasterEvent.StateProvAbbrev state_prov_abbrev,
       stage_hash_athlinks_api_vw_MasterEvent.CountryID country_id,
       stage_hash_athlinks_api_vw_MasterEvent.NextDate next_date,
       stage_hash_athlinks_api_vw_MasterEvent.PrevDate prev_date,
       stage_hash_athlinks_api_vw_MasterEvent.CuratedDesc curated_desc,
       stage_hash_athlinks_api_vw_MasterEvent.Featured featured,
       stage_hash_athlinks_api_vw_MasterEvent.CreateDate create_date,
       isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.MasterID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.NAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.ShortUrl,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.RaceCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.ResultCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.LogoPath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.STATUS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.ContactName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.Phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.ContactAddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.Longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.Latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.Geo,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.Elevation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.City,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.StateProvID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.CompanyName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.StateProvAbbrev,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.CountryID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_MasterEvent.NextDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_MasterEvent.PrevDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_MasterEvent.CuratedDesc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_MasterEvent.Featured as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_MasterEvent
 where stage_hash_athlinks_api_vw_MasterEvent.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_athlinks_api_vw_master_event records
set @insert_date_time = getdate()
insert into s_athlinks_api_vw_master_event (
       bk_hash,
       master_id,
       name,
       short_url,
       race_count,
       result_count,
       logo_path,
       status,
       contact_name,
       phone,
       contact_address,
       longitude,
       latitude,
       geo,
       elevation,
       city,
       state_prov_id,
       company_name,
       state_prov_abbrev,
       country_id,
       next_date,
       prev_date,
       curated_desc,
       featured,
       create_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_athlinks_api_vw_master_event_inserts.bk_hash,
       #s_athlinks_api_vw_master_event_inserts.master_id,
       #s_athlinks_api_vw_master_event_inserts.name,
       #s_athlinks_api_vw_master_event_inserts.short_url,
       #s_athlinks_api_vw_master_event_inserts.race_count,
       #s_athlinks_api_vw_master_event_inserts.result_count,
       #s_athlinks_api_vw_master_event_inserts.logo_path,
       #s_athlinks_api_vw_master_event_inserts.status,
       #s_athlinks_api_vw_master_event_inserts.contact_name,
       #s_athlinks_api_vw_master_event_inserts.phone,
       #s_athlinks_api_vw_master_event_inserts.contact_address,
       #s_athlinks_api_vw_master_event_inserts.longitude,
       #s_athlinks_api_vw_master_event_inserts.latitude,
       #s_athlinks_api_vw_master_event_inserts.geo,
       #s_athlinks_api_vw_master_event_inserts.elevation,
       #s_athlinks_api_vw_master_event_inserts.city,
       #s_athlinks_api_vw_master_event_inserts.state_prov_id,
       #s_athlinks_api_vw_master_event_inserts.company_name,
       #s_athlinks_api_vw_master_event_inserts.state_prov_abbrev,
       #s_athlinks_api_vw_master_event_inserts.country_id,
       #s_athlinks_api_vw_master_event_inserts.next_date,
       #s_athlinks_api_vw_master_event_inserts.prev_date,
       #s_athlinks_api_vw_master_event_inserts.curated_desc,
       #s_athlinks_api_vw_master_event_inserts.featured,
       #s_athlinks_api_vw_master_event_inserts.create_date,
       case when s_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id is null then isnull(#s_athlinks_api_vw_master_event_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_athlinks_api_vw_master_event_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_athlinks_api_vw_master_event_inserts
  left join p_athlinks_api_vw_master_event
    on #s_athlinks_api_vw_master_event_inserts.bk_hash = p_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_athlinks_api_vw_master_event
    on p_athlinks_api_vw_master_event.bk_hash = s_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id = s_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id
 where s_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id is null
    or (s_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id is not null
        and s_athlinks_api_vw_master_event.dv_hash <> #s_athlinks_api_vw_master_event_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_athlinks_api_vw_master_event @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_athlinks_api_vw_master_event @current_dv_batch_id

end
