CREATE PROC [dbo].[proc_etl_mms_reason_code] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ReasonCode

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ReasonCode (
       bk_hash,
       ReasonCodeID,
       Name,
       Description,
       SortOrder,
       DisplayUIFlag,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ReasonCodeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ReasonCodeID,
       Name,
       Description,
       SortOrder,
       DisplayUIFlag,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ReasonCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ReasonCode
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_reason_code @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_reason_code (
       bk_hash,
       reason_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ReasonCode.bk_hash,
       stage_hash_mms_ReasonCode.ReasonCodeID reason_code_id,
       isnull(cast(stage_hash_mms_ReasonCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ReasonCode
  left join h_mms_reason_code
    on stage_hash_mms_ReasonCode.bk_hash = h_mms_reason_code.bk_hash
 where h_mms_reason_code_id is null
   and stage_hash_mms_ReasonCode.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mms_reason_code
if object_id('tempdb..#s_mms_reason_code_inserts') is not null drop table #s_mms_reason_code_inserts
create table #s_mms_reason_code_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReasonCode.bk_hash,
       stage_hash_mms_ReasonCode.ReasonCodeID reason_code_id,
       stage_hash_mms_ReasonCode.Name name,
       stage_hash_mms_ReasonCode.Description description,
       stage_hash_mms_ReasonCode.SortOrder sort_order,
       stage_hash_mms_ReasonCode.DisplayUIFlag display_ui_flag,
       stage_hash_mms_ReasonCode.InsertedDateTime inserted_date_time,
       stage_hash_mms_ReasonCode.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_ReasonCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReasonCode.ReasonCodeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReasonCode.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReasonCode.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReasonCode.SortOrder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReasonCode.DisplayUIFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReasonCode.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReasonCode.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReasonCode
 where stage_hash_mms_ReasonCode.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_reason_code records
set @insert_date_time = getdate()
insert into s_mms_reason_code (
       bk_hash,
       reason_code_id,
       name,
       description,
       sort_order,
       display_ui_flag,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_reason_code_inserts.bk_hash,
       #s_mms_reason_code_inserts.reason_code_id,
       #s_mms_reason_code_inserts.name,
       #s_mms_reason_code_inserts.description,
       #s_mms_reason_code_inserts.sort_order,
       #s_mms_reason_code_inserts.display_ui_flag,
       #s_mms_reason_code_inserts.inserted_date_time,
       #s_mms_reason_code_inserts.updated_date_time,
       case when s_mms_reason_code.s_mms_reason_code_id is null then isnull(#s_mms_reason_code_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_reason_code_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_reason_code_inserts
  left join p_mms_reason_code
    on #s_mms_reason_code_inserts.bk_hash = p_mms_reason_code.bk_hash
   and p_mms_reason_code.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_reason_code
    on p_mms_reason_code.bk_hash = s_mms_reason_code.bk_hash
   and p_mms_reason_code.s_mms_reason_code_id = s_mms_reason_code.s_mms_reason_code_id
 where s_mms_reason_code.s_mms_reason_code_id is null
    or (s_mms_reason_code.s_mms_reason_code_id is not null
        and s_mms_reason_code.dv_hash <> #s_mms_reason_code_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_reason_code @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_reason_code @current_dv_batch_id

end
