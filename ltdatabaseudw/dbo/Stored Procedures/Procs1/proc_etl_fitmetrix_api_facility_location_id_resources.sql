CREATE PROC [dbo].[proc_etl_fitmetrix_api_facility_location_id_resources] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_facility_location_id_resources

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_facility_location_id_resources (
       bk_hash,
       FACILITYLOCATIONRESOURCEID,
       FACILITYLOCATIONID,
       MAXCAPACITY,
       NAME,
       EXTERNALID,
       CONFIGURATION,
       USEINTERVALS,
       DEFAULTACTIVITYTYPEID,
       ADDRESS,
       LAT,
       LONG,
       EXTERNALID_base64_decoded,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(FACILITYLOCATIONRESOURCEID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       FACILITYLOCATIONRESOURCEID,
       FACILITYLOCATIONID,
       MAXCAPACITY,
       NAME,
       EXTERNALID,
       CONFIGURATION,
       USEINTERVALS,
       DEFAULTACTIVITYTYPEID,
       ADDRESS,
       LAT,
       LONG,
       EXTERNALID_base64_decoded,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_fitmetrix_api_facility_location_id_resources
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_facility_location_id_resources @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_facility_location_id_resources (
       bk_hash,
       facility_location_resource_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_facility_location_id_resources.bk_hash,
       stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONRESOURCEID facility_location_resource_id,
       isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_facility_location_id_resources
  left join h_fitmetrix_api_facility_location_id_resources
    on stage_hash_fitmetrix_api_facility_location_id_resources.bk_hash = h_fitmetrix_api_facility_location_id_resources.bk_hash
 where h_fitmetrix_api_facility_location_id_resources_id is null
   and stage_hash_fitmetrix_api_facility_location_id_resources.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_facility_location_id_resources
if object_id('tempdb..#l_fitmetrix_api_facility_location_id_resources_inserts') is not null drop table #l_fitmetrix_api_facility_location_id_resources_inserts
create table #l_fitmetrix_api_facility_location_id_resources_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_facility_location_id_resources.bk_hash,
       stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONRESOURCEID facility_location_resource_id,
       stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONID facility_location_id,
       stage_hash_fitmetrix_api_facility_location_id_resources.EXTERNALID external_id,
       stage_hash_fitmetrix_api_facility_location_id_resources.DEFAULTACTIVITYTYPEID default_activity_type_id,
       stage_hash_fitmetrix_api_facility_location_id_resources.EXTERNALID_base64_decoded external_id_base64_decoded,
       isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONRESOURCEID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_location_id_resources.EXTERNALID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.DEFAULTACTIVITYTYPEID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_fitmetrix_api_facility_location_id_resources.EXTERNALID_base64_decoded, 2),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_facility_location_id_resources
 where stage_hash_fitmetrix_api_facility_location_id_resources.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_facility_location_id_resources records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_facility_location_id_resources (
       bk_hash,
       facility_location_resource_id,
       facility_location_id,
       external_id,
       default_activity_type_id,
       external_id_base64_decoded,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_facility_location_id_resources_inserts.bk_hash,
       #l_fitmetrix_api_facility_location_id_resources_inserts.facility_location_resource_id,
       #l_fitmetrix_api_facility_location_id_resources_inserts.facility_location_id,
       #l_fitmetrix_api_facility_location_id_resources_inserts.external_id,
       #l_fitmetrix_api_facility_location_id_resources_inserts.default_activity_type_id,
       #l_fitmetrix_api_facility_location_id_resources_inserts.external_id_base64_decoded,
       case when l_fitmetrix_api_facility_location_id_resources.l_fitmetrix_api_facility_location_id_resources_id is null then isnull(#l_fitmetrix_api_facility_location_id_resources_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_facility_location_id_resources_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_facility_location_id_resources_inserts
  left join p_fitmetrix_api_facility_location_id_resources
    on #l_fitmetrix_api_facility_location_id_resources_inserts.bk_hash = p_fitmetrix_api_facility_location_id_resources.bk_hash
   and p_fitmetrix_api_facility_location_id_resources.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_facility_location_id_resources
    on p_fitmetrix_api_facility_location_id_resources.bk_hash = l_fitmetrix_api_facility_location_id_resources.bk_hash
   and p_fitmetrix_api_facility_location_id_resources.l_fitmetrix_api_facility_location_id_resources_id = l_fitmetrix_api_facility_location_id_resources.l_fitmetrix_api_facility_location_id_resources_id
 where l_fitmetrix_api_facility_location_id_resources.l_fitmetrix_api_facility_location_id_resources_id is null
    or (l_fitmetrix_api_facility_location_id_resources.l_fitmetrix_api_facility_location_id_resources_id is not null
        and l_fitmetrix_api_facility_location_id_resources.dv_hash <> #l_fitmetrix_api_facility_location_id_resources_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_facility_location_id_resources
if object_id('tempdb..#s_fitmetrix_api_facility_location_id_resources_inserts') is not null drop table #s_fitmetrix_api_facility_location_id_resources_inserts
create table #s_fitmetrix_api_facility_location_id_resources_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_facility_location_id_resources.bk_hash,
       stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONRESOURCEID facility_location_resource_id,
       stage_hash_fitmetrix_api_facility_location_id_resources.MAXCAPACITY max_capacity,
       stage_hash_fitmetrix_api_facility_location_id_resources.NAME name,
       stage_hash_fitmetrix_api_facility_location_id_resources.CONFIGURATION configuration,
       stage_hash_fitmetrix_api_facility_location_id_resources.USEINTERVALS use_intervals,
       stage_hash_fitmetrix_api_facility_location_id_resources.ADDRESS address,
       stage_hash_fitmetrix_api_facility_location_id_resources.LAT lat,
       stage_hash_fitmetrix_api_facility_location_id_resources.LONG long,
       stage_hash_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.FACILITYLOCATIONRESOURCEID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.MAXCAPACITY as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_location_id_resources.NAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_location_id_resources.CONFIGURATION,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_location_id_resources.USEINTERVALS,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_location_id_resources.ADDRESS,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.LAT as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_location_id_resources.LONG as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_facility_location_id_resources.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_facility_location_id_resources
 where stage_hash_fitmetrix_api_facility_location_id_resources.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_facility_location_id_resources records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_facility_location_id_resources (
       bk_hash,
       facility_location_resource_id,
       max_capacity,
       name,
       configuration,
       use_intervals,
       address,
       lat,
       long,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_facility_location_id_resources_inserts.bk_hash,
       #s_fitmetrix_api_facility_location_id_resources_inserts.facility_location_resource_id,
       #s_fitmetrix_api_facility_location_id_resources_inserts.max_capacity,
       #s_fitmetrix_api_facility_location_id_resources_inserts.name,
       #s_fitmetrix_api_facility_location_id_resources_inserts.configuration,
       #s_fitmetrix_api_facility_location_id_resources_inserts.use_intervals,
       #s_fitmetrix_api_facility_location_id_resources_inserts.address,
       #s_fitmetrix_api_facility_location_id_resources_inserts.lat,
       #s_fitmetrix_api_facility_location_id_resources_inserts.long,
       #s_fitmetrix_api_facility_location_id_resources_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_facility_location_id_resources.s_fitmetrix_api_facility_location_id_resources_id is null then isnull(#s_fitmetrix_api_facility_location_id_resources_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_facility_location_id_resources_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_facility_location_id_resources_inserts
  left join p_fitmetrix_api_facility_location_id_resources
    on #s_fitmetrix_api_facility_location_id_resources_inserts.bk_hash = p_fitmetrix_api_facility_location_id_resources.bk_hash
   and p_fitmetrix_api_facility_location_id_resources.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_facility_location_id_resources
    on p_fitmetrix_api_facility_location_id_resources.bk_hash = s_fitmetrix_api_facility_location_id_resources.bk_hash
   and p_fitmetrix_api_facility_location_id_resources.s_fitmetrix_api_facility_location_id_resources_id = s_fitmetrix_api_facility_location_id_resources.s_fitmetrix_api_facility_location_id_resources_id
 where s_fitmetrix_api_facility_location_id_resources.s_fitmetrix_api_facility_location_id_resources_id is null
    or (s_fitmetrix_api_facility_location_id_resources.s_fitmetrix_api_facility_location_id_resources_id is not null
        and s_fitmetrix_api_facility_location_id_resources.dv_hash <> #s_fitmetrix_api_facility_location_id_resources_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_facility_location_id_resources @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_facility_location_id_resources @current_dv_batch_id

end
