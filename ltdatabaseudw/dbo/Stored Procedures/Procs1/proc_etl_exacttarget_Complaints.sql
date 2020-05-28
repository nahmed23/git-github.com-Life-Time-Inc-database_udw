CREATE PROC [dbo].[proc_etl_exacttarget_Complaints] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_complaints @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Complaints_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Complaints
 where (stage_exacttarget_Complaints_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_complaints_insert_stage_exacttarget_Complaints') is not null drop table #h_exacttarget_complaints_insert_stage_exacttarget_Complaints
create table #h_exacttarget_complaints_insert_stage_exacttarget_Complaints with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Complaints.stage_exacttarget_Complaints_id stage_exacttarget_complaints_id,
       stage_exacttarget_Complaints.ClientID client_id,
       stage_exacttarget_Complaints.SendID send_id,
       stage_exacttarget_Complaints.SubscriberID subscriber_id,
       stage_exacttarget_Complaints.ListID list_id,
       stage_exacttarget_Complaints.BatchID batch_id,
       isnull(stage_exacttarget_Complaints.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_complaints.h_exacttarget_complaints_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Complaints
  join #incrementals
    on stage_exacttarget_Complaints.stage_exacttarget_Complaints_id = #incrementals.source_table_id
   and stage_exacttarget_Complaints.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_complaints
    on #incrementals.bk_hash = h_exacttarget_complaints.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_complaints_insert_stage_exacttarget_Complaints)

while @start <= @end
begin

insert into h_exacttarget_complaints (
       bk_hash,
       stage_exacttarget_complaints_id,
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
       stage_exacttarget_complaints_id,
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
  from #h_exacttarget_complaints_insert_stage_exacttarget_Complaints
 where h_exacttarget_complaints_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_complaints_current') is not null drop table #p_exacttarget_complaints_current
create table #p_exacttarget_complaints_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_complaints.bk_hash,
       p_exacttarget_complaints.p_exacttarget_complaints_id,
       p_exacttarget_complaints.stage_exacttarget_Complaints_id,
       p_exacttarget_complaints.client_id,
       p_exacttarget_complaints.send_id,
       p_exacttarget_complaints.subscriber_id,
       p_exacttarget_complaints.list_id,
       p_exacttarget_complaints.batch_id,
       p_exacttarget_complaints.s_exacttarget_complaints_id,
       p_exacttarget_complaints.dv_load_end_date_time
  from dbo.p_exacttarget_complaints
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_complaints.bk_hash = inc.bk_hash
 where p_exacttarget_complaints.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_complaints current hash
if object_id('tempdb..#s_exacttarget_complaints_current') is not null drop table #s_exacttarget_complaints_current
create table #s_exacttarget_complaints_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_complaints.s_exacttarget_complaints_id,
       s_exacttarget_complaints.bk_hash,
       s_exacttarget_complaints.dv_hash
  from dbo.s_exacttarget_complaints
  join #p_exacttarget_complaints_current
    on s_exacttarget_complaints.s_exacttarget_complaints_id = #p_exacttarget_complaints_current.s_exacttarget_complaints_id
   and s_exacttarget_complaints.bk_hash = #p_exacttarget_complaints_current.bk_hash

--calculate hash and lookup to current s_exacttarget_complaints
if object_id('tempdb..#s_exacttarget_complaints_inserts') is not null drop table #s_exacttarget_complaints_inserts
create table #s_exacttarget_complaints_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Complaints.stage_exacttarget_Complaints_id stage_exacttarget_complaints_id,
       stage_exacttarget_Complaints.ClientID client_id,
       stage_exacttarget_Complaints.SendID send_id,
       stage_exacttarget_Complaints.SubscriberKey subscriber_key,
       stage_exacttarget_Complaints.EmailAddress email_address,
       stage_exacttarget_Complaints.SubscriberID subscriber_id,
       stage_exacttarget_Complaints.ListID list_id,
       stage_exacttarget_Complaints.EventDate event_date,
       stage_exacttarget_Complaints.EventType event_type,
       stage_exacttarget_Complaints.BatchID batch_id,
       stage_exacttarget_Complaints.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_Complaints.Domain domain,
       stage_exacttarget_Complaints.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints.stage_exacttarget_Complaints_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Complaints.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Complaints.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Complaints.Domain,'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_complaints_current.s_exacttarget_complaints_id,
       #s_exacttarget_complaints_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Complaints
  join #incrementals
    on stage_exacttarget_Complaints.stage_exacttarget_Complaints_id = #incrementals.source_table_id
   and stage_exacttarget_Complaints.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_complaints_current
    on #incrementals.bk_hash = #s_exacttarget_complaints_current.bk_hash

--Insert all updated and new s_exacttarget_complaints records
set @start = 1
set @end = (select max(r) from #s_exacttarget_complaints_inserts)

while @start <= @end
begin

insert into s_exacttarget_complaints (
       bk_hash,
       stage_exacttarget_complaints_id,
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
       domain,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_complaints_id,
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
       domain,
       case when s_exacttarget_complaints_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_complaints_inserts
 where (s_exacttarget_complaints_id is null
        or (s_exacttarget_complaints_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_complaints @current_dv_batch_id

--Done!
drop table #incrementals
end
