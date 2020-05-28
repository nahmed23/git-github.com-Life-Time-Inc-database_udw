CREATE PROC [dbo].[proc_etl_sfmc_content_send_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_sfmc_content_send_log

set @insert_date_time = getdate()
insert into dbo.stage_hash_sfmc_content_send_log (
       bk_hash,
       JobID,
       Member_ID,
       ContentGUID,
       IsTestSend,
       SubscriberKey,
       InsertDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(Member_ID,'z#@$k%&P')+'P%#&z$@k'+isnull(ContentGUID,'z#@$k%&P'))),2) bk_hash,
       JobID,
       Member_ID,
       ContentGUID,
       IsTestSend,
       SubscriberKey,
       InsertDateTime,
       isnull(cast(stage_sfmc_content_send_log.InsertDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_sfmc_content_send_log
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_sfmc_content_send_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_sfmc_content_send_log (
       bk_hash,
       member_id,
       content_guid,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_sfmc_content_send_log.bk_hash,
       stage_hash_sfmc_content_send_log.Member_ID member_id,
       stage_hash_sfmc_content_send_log.ContentGUID content_guid,
       isnull(cast(stage_hash_sfmc_content_send_log.InsertDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_sfmc_content_send_log
  left join h_sfmc_content_send_log
    on stage_hash_sfmc_content_send_log.bk_hash = h_sfmc_content_send_log.bk_hash
 where h_sfmc_content_send_log_id is null
   and stage_hash_sfmc_content_send_log.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_sfmc_content_send_log
if object_id('tempdb..#l_sfmc_content_send_log_inserts') is not null drop table #l_sfmc_content_send_log_inserts
create table #l_sfmc_content_send_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sfmc_content_send_log.bk_hash,
       stage_hash_sfmc_content_send_log.JobID job_id,
       stage_hash_sfmc_content_send_log.Member_ID member_id,
       stage_hash_sfmc_content_send_log.ContentGUID content_guid,
       isnull(cast(stage_hash_sfmc_content_send_log.InsertDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.JobID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.Member_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.ContentGUID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sfmc_content_send_log
 where stage_hash_sfmc_content_send_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_sfmc_content_send_log records
set @insert_date_time = getdate()
insert into l_sfmc_content_send_log (
       bk_hash,
       job_id,
       member_id,
       content_guid,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_sfmc_content_send_log_inserts.bk_hash,
       #l_sfmc_content_send_log_inserts.job_id,
       #l_sfmc_content_send_log_inserts.member_id,
       #l_sfmc_content_send_log_inserts.content_guid,
       case when l_sfmc_content_send_log.l_sfmc_content_send_log_id is null then isnull(#l_sfmc_content_send_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #l_sfmc_content_send_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_sfmc_content_send_log_inserts
  left join p_sfmc_content_send_log
    on #l_sfmc_content_send_log_inserts.bk_hash = p_sfmc_content_send_log.bk_hash
   and p_sfmc_content_send_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_sfmc_content_send_log
    on p_sfmc_content_send_log.bk_hash = l_sfmc_content_send_log.bk_hash
   and p_sfmc_content_send_log.l_sfmc_content_send_log_id = l_sfmc_content_send_log.l_sfmc_content_send_log_id
 where l_sfmc_content_send_log.l_sfmc_content_send_log_id is null
    or (l_sfmc_content_send_log.l_sfmc_content_send_log_id is not null
        and l_sfmc_content_send_log.dv_hash <> #l_sfmc_content_send_log_inserts.source_hash)

--calculate hash and lookup to current s_sfmc_content_send_log
if object_id('tempdb..#s_sfmc_content_send_log_inserts') is not null drop table #s_sfmc_content_send_log_inserts
create table #s_sfmc_content_send_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sfmc_content_send_log.bk_hash,
       stage_hash_sfmc_content_send_log.Member_ID member_id,
       stage_hash_sfmc_content_send_log.ContentGUID content_guid,
       stage_hash_sfmc_content_send_log.IsTestSend is_test_send,
       stage_hash_sfmc_content_send_log.SubscriberKey subscriber_key,
       stage_hash_sfmc_content_send_log.InsertDateTime insert_date_time,
       isnull(cast(stage_hash_sfmc_content_send_log.InsertDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.Member_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.ContentGUID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.IsTestSend,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_send_log.SubscriberKey,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_send_log.InsertDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sfmc_content_send_log
 where stage_hash_sfmc_content_send_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_sfmc_content_send_log records
set @insert_date_time = getdate()
insert into s_sfmc_content_send_log (
       bk_hash,
       member_id,
       content_guid,
       is_test_send,
       subscriber_key,
       insert_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_sfmc_content_send_log_inserts.bk_hash,
       #s_sfmc_content_send_log_inserts.member_id,
       #s_sfmc_content_send_log_inserts.content_guid,
       #s_sfmc_content_send_log_inserts.is_test_send,
       #s_sfmc_content_send_log_inserts.subscriber_key,
       #s_sfmc_content_send_log_inserts.insert_date_time,
       case when s_sfmc_content_send_log.s_sfmc_content_send_log_id is null then isnull(#s_sfmc_content_send_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_sfmc_content_send_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_sfmc_content_send_log_inserts
  left join p_sfmc_content_send_log
    on #s_sfmc_content_send_log_inserts.bk_hash = p_sfmc_content_send_log.bk_hash
   and p_sfmc_content_send_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_sfmc_content_send_log
    on p_sfmc_content_send_log.bk_hash = s_sfmc_content_send_log.bk_hash
   and p_sfmc_content_send_log.s_sfmc_content_send_log_id = s_sfmc_content_send_log.s_sfmc_content_send_log_id
 where s_sfmc_content_send_log.s_sfmc_content_send_log_id is null
    or (s_sfmc_content_send_log.s_sfmc_content_send_log_id is not null
        and s_sfmc_content_send_log.dv_hash <> #s_sfmc_content_send_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_sfmc_content_send_log @current_dv_batch_id

end
