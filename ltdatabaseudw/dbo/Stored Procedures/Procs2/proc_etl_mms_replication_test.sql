CREATE PROC [dbo].[proc_etl_mms_replication_test] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ReplicationTest

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ReplicationTest (
       bk_hash,
       ReplicationTestID,
       ReplicationDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ReplicationTestID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ReplicationTestID,
       ReplicationDateTime,
       isnull(cast(stage_mms_ReplicationTest.ReplicationDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ReplicationTest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_replication_test @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_replication_test (
       bk_hash,
       replication_test_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ReplicationTest.bk_hash,
       stage_hash_mms_ReplicationTest.ReplicationTestID replication_test_id,
       isnull(cast(stage_hash_mms_ReplicationTest.ReplicationDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ReplicationTest
  left join h_mms_replication_test
    on stage_hash_mms_ReplicationTest.bk_hash = h_mms_replication_test.bk_hash
 where h_mms_replication_test_id is null
   and stage_hash_mms_ReplicationTest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mms_replication_test
if object_id('tempdb..#s_mms_replication_test_inserts') is not null drop table #s_mms_replication_test_inserts
create table #s_mms_replication_test_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReplicationTest.bk_hash,
       stage_hash_mms_ReplicationTest.ReplicationTestID replication_test_id,
       stage_hash_mms_ReplicationTest.ReplicationDateTime replication_date_time,
       stage_hash_mms_ReplicationTest.ReplicationDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReplicationTest.ReplicationTestID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReplicationTest.ReplicationDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReplicationTest
 where stage_hash_mms_ReplicationTest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_replication_test records
set @insert_date_time = getdate()
insert into s_mms_replication_test (
       bk_hash,
       replication_test_id,
       replication_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_replication_test_inserts.bk_hash,
       #s_mms_replication_test_inserts.replication_test_id,
       #s_mms_replication_test_inserts.replication_date_time,
       case when s_mms_replication_test.s_mms_replication_test_id is null then isnull(#s_mms_replication_test_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_replication_test_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_replication_test_inserts
  left join p_mms_replication_test
    on #s_mms_replication_test_inserts.bk_hash = p_mms_replication_test.bk_hash
   and p_mms_replication_test.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_replication_test
    on p_mms_replication_test.bk_hash = s_mms_replication_test.bk_hash
   and p_mms_replication_test.s_mms_replication_test_id = s_mms_replication_test.s_mms_replication_test_id
 where s_mms_replication_test.s_mms_replication_test_id is null
    or (s_mms_replication_test.s_mms_replication_test_id is not null
        and s_mms_replication_test.dv_hash <> #s_mms_replication_test_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_replication_test @current_dv_batch_id

end
