CREATE PROC [dbo].[proc_etl_exacttarget_open_bkp] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_open @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Open_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Open_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Open
 where (stage_exacttarget_Open_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_open_insert_stage_exacttarget_Open') is not null drop table #h_exacttarget_open_insert_stage_exacttarget_Open
create table #h_exacttarget_open_insert_stage_exacttarget_Open with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Open.stage_exacttarget_Open_id stage_exacttarget_open_id,
       stage_exacttarget_Open.ClientID client_id,
       stage_exacttarget_Open.SendID send_id,
       stage_exacttarget_Open.SubscriberID subscriber_id,
       stage_exacttarget_Open.ListID list_id,
       stage_exacttarget_Open.BatchID batch_id,
       isnull(stage_exacttarget_Open.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_open.h_exacttarget_open_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Open
  join #incrementals
    on stage_exacttarget_Open.stage_exacttarget_Open_id = #incrementals.source_table_id
   and stage_exacttarget_Open.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_open
    on #incrementals.bk_hash = h_exacttarget_open.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_open_insert_stage_exacttarget_Open)

while @start <= @end
begin

insert into h_exacttarget_open (
       bk_hash,
       stage_exacttarget_open_id,
       client_id,
       send_id,
       subscriber_id,
       list_id,
       batch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_open_id,
       client_id,
       send_id,
       subscriber_id,
       list_id,
       batch_id,
       dv_load_date_time,
       @current_dv_batch_id,
       19,
       getdate(),
       @user
  from #h_exacttarget_open_insert_stage_exacttarget_Open
 where h_exacttarget_open_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_open_current') is not null drop table #p_exacttarget_open_current
create table #p_exacttarget_open_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_open.bk_hash,
       p_exacttarget_open.p_exacttarget_open_id,
       p_exacttarget_open.stage_exacttarget_open_id,
       p_exacttarget_open.client_id,
       p_exacttarget_open.send_id,
       p_exacttarget_open.subscriber_id,
       p_exacttarget_open.list_id,
       p_exacttarget_open.batch_id,
       p_exacttarget_open.s_exacttarget_open_id,
       p_exacttarget_open.dv_load_end_date_time
  from dbo.p_exacttarget_open
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_open.bk_hash = inc.bk_hash
 where p_exacttarget_open.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_open current hash
if object_id('tempdb..#s_exacttarget_open_current') is not null drop table #s_exacttarget_open_current
create table #s_exacttarget_open_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_open.s_exacttarget_open_id,
       s_exacttarget_open.bk_hash,
       s_exacttarget_open.dv_hash
  from dbo.s_exacttarget_open
  join #p_exacttarget_open_current
    on s_exacttarget_open.s_exacttarget_open_id = #p_exacttarget_open_current.s_exacttarget_open_id
   and s_exacttarget_open.bk_hash = #p_exacttarget_open_current.bk_hash

--calculate hash and lookup to current s_exacttarget_open
if object_id('tempdb..#s_exacttarget_open_inserts') is not null drop table #s_exacttarget_open_inserts
create table #s_exacttarget_open_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Open.stage_exacttarget_Open_id stage_exacttarget_open_id,
       stage_exacttarget_Open.ClientID client_id,
       stage_exacttarget_Open.SendID send_id,
       stage_exacttarget_Open.SubscriberKey subscriber_key,
       stage_exacttarget_Open.EmailAddress email_address,
       stage_exacttarget_Open.SubscriberID subscriber_id,
       stage_exacttarget_Open.ListID list_id,
       stage_exacttarget_Open.EventDate event_date,
       stage_exacttarget_Open.EventType event_type,
       stage_exacttarget_Open.BatchID batch_id,
       stage_exacttarget_Open.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_Open.IsUnique is_unique,
       stage_exacttarget_Open.IpAddress ip_address,
       stage_exacttarget_Open.Country country,
       stage_exacttarget_Open.Region region,
       stage_exacttarget_Open.City city,
       stage_exacttarget_Open.Latitude latitude,
       stage_exacttarget_Open.Longitude longitude,
       stage_exacttarget_Open.MetroCode metro_code,
       stage_exacttarget_Open.AreaCode area_code,
       stage_exacttarget_Open.Browser browser,
       stage_exacttarget_Open.EmailClient email_client,
       stage_exacttarget_Open.OperatingSystem operating_system,
       stage_exacttarget_Open.Device device,
       stage_exacttarget_Open.jan_one jan_one,
       stage_exacttarget_Open.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Open.stage_exacttarget_Open_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Open.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.IsUnique,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.IpAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.Country,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.Region,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.Latitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.Longitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.MetroCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Open.AreaCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.Browser,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.EmailClient,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.OperatingSystem,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Open.Device,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Open.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_open_current.s_exacttarget_open_id,
       #s_exacttarget_open_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Open
  join #incrementals
    on stage_exacttarget_Open.stage_exacttarget_Open_id = #incrementals.source_table_id
   and stage_exacttarget_Open.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_open_current
    on #incrementals.bk_hash = #s_exacttarget_open_current.bk_hash

--Insert all updated and new s_exacttarget_open records
set @start = 1
set @end = (select max(r) from #s_exacttarget_open_inserts)

while @start <= @end
begin

insert into s_exacttarget_open (
       bk_hash,
       stage_exacttarget_open_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
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
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_open_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
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
       jan_one,
       case when s_exacttarget_open_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_open_inserts
 where (s_exacttarget_open_id is null
        or (s_exacttarget_open_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_open @current_dv_batch_id

--Done!
drop table #incrementals
end
