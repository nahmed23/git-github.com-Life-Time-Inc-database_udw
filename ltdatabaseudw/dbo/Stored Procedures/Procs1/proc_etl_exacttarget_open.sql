CREATE PROC [dbo].[proc_etl_exacttarget_open] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_exacttarget_Open where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_exacttarget_Open (
       bk_hash,
       ClientID,
       SendID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       ListID,
       EventDate,
       EventType,
       BatchID,
       TriggeredSendExternalKey,
       IsUnique,
       IpAddress,
       Country,
       Region,
       City,
       Latitude,
       Longitude,
       MetroCode,
       AreaCode,
       Browser,
       EmailClient,
       OperatingSystem,
       Device,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select distinct 
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(SubscriberKey,'z#@$k%&P')+'P%#&z$@k'+isnull(EmailAddress,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(EventType,'z#@$k%&P')+'P%#&z$@k'+isnull(BatchID,'z#@$k%&P')+'P%#&z$@k'+isnull(TriggeredSendExternalKey,'z#@$k%&P')+'P%#&z$@k'+isnull(IsUnique,'z#@$k%&P')+'P%#&z$@k'+isnull(IpAddress,'z#@$k%&P')+'P%#&z$@k'+isnull(Country,'z#@$k%&P')+'P%#&z$@k'+isnull(Region,'z#@$k%&P')+'P%#&z$@k'+isnull(City,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(Latitude as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(Longitude as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(MetroCode,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(AreaCode as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(Browser,'z#@$k%&P')+'P%#&z$@k'+isnull(EmailClient,'z#@$k%&P')+'P%#&z$@k'+isnull(OperatingSystem,'z#@$k%&P')+'P%#&z$@k'+isnull(Device,'z#@$k%&P'))),2) bk_hash,
       ClientID,
       SendID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       ListID,
       EventDate,
       EventType,
       BatchID,
       TriggeredSendExternalKey,
       IsUnique,
       IpAddress,
       Country,
       Region,
       City,
       Latitude,
       Longitude,
       MetroCode,
       AreaCode,
       Browser,
       EmailClient,
       OperatingSystem,
       Device,
       jan_one,
       isnull(cast(jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exacttarget_Open
 where dv_batch_id = @current_dv_batch_id
   and EventDate is not null
 
--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exacttarget_open (
       bk_hash,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_type,
       batch_id,
       triggered_send_external_key,
       is_unique,
       ip_address,
       country,
       region,
       city,
       latitude,
       longitude,
       metro_code,
       area_code,
       browser,
       email_client,
       operating_system,
       device,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exacttarget_Open.bk_hash,
       stage_hash_exacttarget_Open.ClientID client_id,
       stage_hash_exacttarget_Open.SendID send_id,
       stage_hash_exacttarget_Open.SubscriberKey subscriber_key,
       stage_hash_exacttarget_Open.EmailAddress email_address,
       stage_hash_exacttarget_Open.SubscriberID subscriber_id,
       stage_hash_exacttarget_Open.ListID list_id,
       stage_hash_exacttarget_Open.EventType event_type,
       stage_hash_exacttarget_Open.BatchID batch_id,
       stage_hash_exacttarget_Open.TriggeredSendExternalKey triggered_send_external_key,
       stage_hash_exacttarget_Open.IsUnique is_unique,
       stage_hash_exacttarget_Open.IpAddress ip_address,
       stage_hash_exacttarget_Open.Country country,
       stage_hash_exacttarget_Open.Region region,
       stage_hash_exacttarget_Open.City city,
       stage_hash_exacttarget_Open.Latitude latitude,
       stage_hash_exacttarget_Open.Longitude longitude,
       stage_hash_exacttarget_Open.MetroCode metro_code,
       stage_hash_exacttarget_Open.AreaCode area_code,
       stage_hash_exacttarget_Open.Browser browser,
       stage_hash_exacttarget_Open.EmailClient email_client,
       stage_hash_exacttarget_Open.OperatingSystem operating_system,
       stage_hash_exacttarget_Open.Device device,
       min(stage_hash_exacttarget_Open.dv_load_date_time) dv_load_date_time,
       stage_hash_exacttarget_Open.dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_exacttarget_Open
  left join h_exacttarget_open
    on stage_hash_exacttarget_Open.bk_hash = h_exacttarget_open.bk_hash
 where h_exacttarget_open_id is null
   and stage_hash_exacttarget_Open.dv_batch_id = @current_dv_batch_id
 group by stage_hash_exacttarget_Open.bk_hash,
          stage_hash_exacttarget_Open.ClientID,
          stage_hash_exacttarget_Open.SendID,
          stage_hash_exacttarget_Open.SubscriberKey,
          stage_hash_exacttarget_Open.EmailAddress,
          stage_hash_exacttarget_Open.SubscriberID,
          stage_hash_exacttarget_Open.ListID,
          stage_hash_exacttarget_Open.EventType,
          stage_hash_exacttarget_Open.BatchID,
          stage_hash_exacttarget_Open.TriggeredSendExternalKey,
          stage_hash_exacttarget_Open.IsUnique,
          stage_hash_exacttarget_Open.IpAddress,
          stage_hash_exacttarget_Open.Country,
          stage_hash_exacttarget_Open.Region,
          stage_hash_exacttarget_Open.City,
          stage_hash_exacttarget_Open.Latitude,
          stage_hash_exacttarget_Open.Longitude,
          stage_hash_exacttarget_Open.MetroCode,
          stage_hash_exacttarget_Open.AreaCode,
          stage_hash_exacttarget_Open.Browser,
          stage_hash_exacttarget_Open.EmailClient,
          stage_hash_exacttarget_Open.OperatingSystem,
          stage_hash_exacttarget_Open.Device,
          stage_hash_exacttarget_Open.dv_batch_id

--calculate hash and lookup to current s_exacttarget_open
if object_id('tempdb..#s_exacttarget_open_inserts') is not null drop table #s_exacttarget_open_inserts
create table #s_exacttarget_open_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exacttarget_Open.bk_hash,
       stage_hash_exacttarget_Open.EventDate event_date,
       isnull(cast(stage_hash_exacttarget_Open.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exacttarget_Open.bk_hash,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Open.EventDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exacttarget_Open
 where stage_hash_exacttarget_Open.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exacttarget_open records
set @insert_date_time = getdate()
insert into s_exacttarget_open (
       bk_hash,
       event_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exacttarget_open_inserts.bk_hash,
       #s_exacttarget_open_inserts.event_date,
       case when s_exacttarget_open.s_exacttarget_open_id is null then isnull(#s_exacttarget_open_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_exacttarget_open_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exacttarget_open_inserts
  left join s_exacttarget_open
    on #s_exacttarget_open_inserts.bk_hash = s_exacttarget_open.bk_hash
   and #s_exacttarget_open_inserts.source_hash = s_exacttarget_open.dv_hash
 where s_exacttarget_open.s_exacttarget_open_id is null

 








end
