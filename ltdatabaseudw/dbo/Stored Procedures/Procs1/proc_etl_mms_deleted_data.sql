CREATE PROC [dbo].[proc_etl_mms_deleted_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_DeletedData

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_DeletedData (
       bk_hash,
       DeletedDataID,
       TableName,
       PrimaryKeyID,
       DeletedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(DeletedDataID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       DeletedDataID,
       TableName,
       PrimaryKeyID,
       DeletedDateTime,
       isnull(cast(stage_mms_DeletedData.DeletedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_DeletedData
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_deleted_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_deleted_data (
       bk_hash,
       deleted_data_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_DeletedData.bk_hash,
       stage_hash_mms_DeletedData.DeletedDataID deleted_data_id,
       isnull(cast(stage_hash_mms_DeletedData.DeletedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_DeletedData
  left join h_mms_deleted_data
    on stage_hash_mms_DeletedData.bk_hash = h_mms_deleted_data.bk_hash
 where h_mms_deleted_data_id is null
   and stage_hash_mms_DeletedData.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_deleted_data
if object_id('tempdb..#l_mms_deleted_data_inserts') is not null drop table #l_mms_deleted_data_inserts
create table #l_mms_deleted_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DeletedData.bk_hash,
       stage_hash_mms_DeletedData.DeletedDataID deleted_data_id,
       stage_hash_mms_DeletedData.PrimaryKeyID primary_key_id,
       isnull(cast(stage_hash_mms_DeletedData.DeletedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DeletedData.DeletedDataID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_DeletedData.PrimaryKeyID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DeletedData
 where stage_hash_mms_DeletedData.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_deleted_data records
set @insert_date_time = getdate()
insert into l_mms_deleted_data (
       bk_hash,
       deleted_data_id,
       primary_key_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_deleted_data_inserts.bk_hash,
       #l_mms_deleted_data_inserts.deleted_data_id,
       #l_mms_deleted_data_inserts.primary_key_id,
       case when l_mms_deleted_data.l_mms_deleted_data_id is null then isnull(#l_mms_deleted_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_deleted_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_deleted_data_inserts
  left join p_mms_deleted_data
    on #l_mms_deleted_data_inserts.bk_hash = p_mms_deleted_data.bk_hash
   and p_mms_deleted_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_deleted_data
    on p_mms_deleted_data.bk_hash = l_mms_deleted_data.bk_hash
   and p_mms_deleted_data.l_mms_deleted_data_id = l_mms_deleted_data.l_mms_deleted_data_id
 where l_mms_deleted_data.l_mms_deleted_data_id is null
    or (l_mms_deleted_data.l_mms_deleted_data_id is not null
        and l_mms_deleted_data.dv_hash <> #l_mms_deleted_data_inserts.source_hash)

--calculate hash and lookup to current s_mms_deleted_data
if object_id('tempdb..#s_mms_deleted_data_inserts') is not null drop table #s_mms_deleted_data_inserts
create table #s_mms_deleted_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DeletedData.bk_hash,
       stage_hash_mms_DeletedData.DeletedDataID deleted_data_id,
       stage_hash_mms_DeletedData.TableName table_name,
       stage_hash_mms_DeletedData.DeletedDateTime deleted_date_time,
       isnull(cast(stage_hash_mms_DeletedData.DeletedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DeletedData.DeletedDataID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_DeletedData.TableName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DeletedData.DeletedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DeletedData
 where stage_hash_mms_DeletedData.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_deleted_data records
set @insert_date_time = getdate()
insert into s_mms_deleted_data (
       bk_hash,
       deleted_data_id,
       table_name,
       deleted_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_deleted_data_inserts.bk_hash,
       #s_mms_deleted_data_inserts.deleted_data_id,
       #s_mms_deleted_data_inserts.table_name,
       #s_mms_deleted_data_inserts.deleted_date_time,
       case when s_mms_deleted_data.s_mms_deleted_data_id is null then isnull(#s_mms_deleted_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_deleted_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_deleted_data_inserts
  left join p_mms_deleted_data
    on #s_mms_deleted_data_inserts.bk_hash = p_mms_deleted_data.bk_hash
   and p_mms_deleted_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_deleted_data
    on p_mms_deleted_data.bk_hash = s_mms_deleted_data.bk_hash
   and p_mms_deleted_data.s_mms_deleted_data_id = s_mms_deleted_data.s_mms_deleted_data_id
 where s_mms_deleted_data.s_mms_deleted_data_id is null
    or (s_mms_deleted_data.s_mms_deleted_data_id is not null
        and s_mms_deleted_data.dv_hash <> #s_mms_deleted_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_deleted_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_deleted_data @current_dv_batch_id

end
