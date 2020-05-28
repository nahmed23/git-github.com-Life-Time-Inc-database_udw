﻿CREATE PROC [dbo].[proc_etl_mms_tran_voided] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_TranVoided

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_TranVoided (
       bk_hash,
       TranVoidedID,
       EmployeeID,
       VoidDateTime,
       Comments,
       UTCVoidDateTime,
       VoidDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(TranVoidedID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       TranVoidedID,
       EmployeeID,
       VoidDateTime,
       Comments,
       UTCVoidDateTime,
       VoidDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_TranVoided.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_TranVoided
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_tran_voided @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_tran_voided (
       bk_hash,
       tran_voided_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_TranVoided.bk_hash,
       stage_hash_mms_TranVoided.TranVoidedID tran_voided_id,
       isnull(cast(stage_hash_mms_TranVoided.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_TranVoided
  left join h_mms_tran_voided
    on stage_hash_mms_TranVoided.bk_hash = h_mms_tran_voided.bk_hash
 where h_mms_tran_voided_id is null
   and stage_hash_mms_TranVoided.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_tran_voided
if object_id('tempdb..#l_mms_tran_voided_inserts') is not null drop table #l_mms_tran_voided_inserts
create table #l_mms_tran_voided_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranVoided.bk_hash,
       stage_hash_mms_TranVoided.TranVoidedID tran_voided_id,
       stage_hash_mms_TranVoided.EmployeeID employee_id,
       isnull(cast(stage_hash_mms_TranVoided.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranVoided.TranVoidedID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_TranVoided.EmployeeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranVoided
 where stage_hash_mms_TranVoided.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_tran_voided records
set @insert_date_time = getdate()
insert into l_mms_tran_voided (
       bk_hash,
       tran_voided_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_tran_voided_inserts.bk_hash,
       #l_mms_tran_voided_inserts.tran_voided_id,
       #l_mms_tran_voided_inserts.employee_id,
       case when l_mms_tran_voided.l_mms_tran_voided_id is null then isnull(#l_mms_tran_voided_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_tran_voided_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_tran_voided_inserts
  left join p_mms_tran_voided
    on #l_mms_tran_voided_inserts.bk_hash = p_mms_tran_voided.bk_hash
   and p_mms_tran_voided.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_tran_voided
    on p_mms_tran_voided.bk_hash = l_mms_tran_voided.bk_hash
   and p_mms_tran_voided.l_mms_tran_voided_id = l_mms_tran_voided.l_mms_tran_voided_id
 where l_mms_tran_voided.l_mms_tran_voided_id is null
    or (l_mms_tran_voided.l_mms_tran_voided_id is not null
        and l_mms_tran_voided.dv_hash <> #l_mms_tran_voided_inserts.source_hash)

--calculate hash and lookup to current s_mms_tran_voided
if object_id('tempdb..#s_mms_tran_voided_inserts') is not null drop table #s_mms_tran_voided_inserts
create table #s_mms_tran_voided_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_TranVoided.bk_hash,
       stage_hash_mms_TranVoided.TranVoidedID tran_voided_id,
       stage_hash_mms_TranVoided.VoidDateTime void_date_time,
       stage_hash_mms_TranVoided.Comments comments,
       stage_hash_mms_TranVoided.UTCVoidDateTime utc_void_date_time,
       stage_hash_mms_TranVoided.VoidDateTimeZone void_date_time_zone,
       stage_hash_mms_TranVoided.InsertedDateTime inserted_date_time,
       stage_hash_mms_TranVoided.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_TranVoided.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_TranVoided.TranVoidedID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranVoided.VoidDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_TranVoided.Comments,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranVoided.UTCVoidDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_TranVoided.VoidDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranVoided.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_TranVoided.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_TranVoided
 where stage_hash_mms_TranVoided.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_tran_voided records
set @insert_date_time = getdate()
insert into s_mms_tran_voided (
       bk_hash,
       tran_voided_id,
       void_date_time,
       comments,
       utc_void_date_time,
       void_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_tran_voided_inserts.bk_hash,
       #s_mms_tran_voided_inserts.tran_voided_id,
       #s_mms_tran_voided_inserts.void_date_time,
       #s_mms_tran_voided_inserts.comments,
       #s_mms_tran_voided_inserts.utc_void_date_time,
       #s_mms_tran_voided_inserts.void_date_time_zone,
       #s_mms_tran_voided_inserts.inserted_date_time,
       #s_mms_tran_voided_inserts.updated_date_time,
       case when s_mms_tran_voided.s_mms_tran_voided_id is null then isnull(#s_mms_tran_voided_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_tran_voided_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_tran_voided_inserts
  left join p_mms_tran_voided
    on #s_mms_tran_voided_inserts.bk_hash = p_mms_tran_voided.bk_hash
   and p_mms_tran_voided.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_tran_voided
    on p_mms_tran_voided.bk_hash = s_mms_tran_voided.bk_hash
   and p_mms_tran_voided.s_mms_tran_voided_id = s_mms_tran_voided.s_mms_tran_voided_id
 where s_mms_tran_voided.s_mms_tran_voided_id is null
    or (s_mms_tran_voided.s_mms_tran_voided_id is not null
        and s_mms_tran_voided.dv_hash <> #s_mms_tran_voided_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_tran_voided @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_tran_voided @current_dv_batch_id

end
