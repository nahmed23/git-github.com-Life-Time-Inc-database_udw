CREATE PROC [dbo].[proc_etl_exacttarget_lists] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_lists @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Lists_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Lists
 where (ClientID is not null
        or ListID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_lists_insert_stage_exacttarget_Lists') is not null drop table #h_exacttarget_lists_insert_stage_exacttarget_Lists
create table #h_exacttarget_lists_insert_stage_exacttarget_Lists with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Lists.ClientID client_id,
       stage_exacttarget_Lists.ListID list_id,
       isnull(stage_exacttarget_Lists.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_lists.h_exacttarget_lists_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Lists
  join #incrementals
    on stage_exacttarget_Lists.stage_exacttarget_Lists_id = #incrementals.source_table_id
   and stage_exacttarget_Lists.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_lists
    on #incrementals.bk_hash = h_exacttarget_lists.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_lists_insert_stage_exacttarget_Lists)

while @start <= @end
begin

insert into h_exacttarget_lists (
       bk_hash,
       client_id,
       list_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       client_id,
       list_id,
       dv_load_date_time,
       @current_dv_batch_id,
       19,
       getdate(),
       @user
  from #h_exacttarget_lists_insert_stage_exacttarget_Lists
 where h_exacttarget_lists_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_lists_current') is not null drop table #p_exacttarget_lists_current
create table #p_exacttarget_lists_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_lists.bk_hash,
       p_exacttarget_lists.p_exacttarget_lists_id,
       p_exacttarget_lists.client_id,
       p_exacttarget_lists.list_id,
       p_exacttarget_lists.s_exacttarget_lists_id,
       p_exacttarget_lists.dv_load_end_date_time
  from dbo.p_exacttarget_lists
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_lists.bk_hash = inc.bk_hash
 where p_exacttarget_lists.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_lists current hash
if object_id('tempdb..#s_exacttarget_lists_current') is not null drop table #s_exacttarget_lists_current
create table #s_exacttarget_lists_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_lists.s_exacttarget_lists_id,
       s_exacttarget_lists.bk_hash,
       s_exacttarget_lists.dv_hash
  from dbo.s_exacttarget_lists
  join #p_exacttarget_lists_current
    on s_exacttarget_lists.s_exacttarget_lists_id = #p_exacttarget_lists_current.s_exacttarget_lists_id
   and s_exacttarget_lists.bk_hash = #p_exacttarget_lists_current.bk_hash

--calculate hash and lookup to current s_exacttarget_lists
if object_id('tempdb..#s_exacttarget_lists_inserts') is not null drop table #s_exacttarget_lists_inserts
create table #s_exacttarget_lists_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Lists.ClientID client_id,
       stage_exacttarget_Lists.ListID list_id,
       stage_exacttarget_Lists.Name name,
       stage_exacttarget_Lists.Description description,
       stage_exacttarget_Lists.DateCreated date_created,
       stage_exacttarget_Lists.Status status,
       stage_exacttarget_Lists.ListType list_type,
       stage_exacttarget_Lists.jan_one jan_one,
       stage_exacttarget_Lists.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Lists.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Lists.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Lists.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Lists.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Lists.DateCreated,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Lists.Status,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Lists.ListType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Lists.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_lists_current.s_exacttarget_lists_id,
       #s_exacttarget_lists_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Lists
  join #incrementals
    on stage_exacttarget_Lists.stage_exacttarget_Lists_id = #incrementals.source_table_id
   and stage_exacttarget_Lists.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_lists_current
    on #incrementals.bk_hash = #s_exacttarget_lists_current.bk_hash

--Insert all updated and new s_exacttarget_lists records
set @start = 1
set @end = (select max(r) from #s_exacttarget_lists_inserts)

while @start <= @end
begin

insert into s_exacttarget_lists (
       bk_hash,
       client_id,
       list_id,
       name,
       description,
       date_created,
       status,
       list_type,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       client_id,
       list_id,
       name,
       description,
       date_created,
       status,
       list_type,
       jan_one,
       case when s_exacttarget_lists_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_lists_inserts
 where (s_exacttarget_lists_id is null
        or (s_exacttarget_lists_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_lists @current_dv_batch_id

--Done!
drop table #incrementals
end
