CREATE PROC [dbo].[proc_etl_mms_kids_play_check_in] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_KidsPlayCheckIn

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_KidsPlayCheckIn (
       bk_hash,
       KidsPlayCheckInID,
       ChildCenterUsageID,
       KidsPlayCheckinDateTime,
       UTCKidsPlayCheckinDateTime,
       KidsPlayCheckinDateTimeZone,
       InsertedDatetime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(KidsPlayCheckInID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       KidsPlayCheckInID,
       ChildCenterUsageID,
       KidsPlayCheckinDateTime,
       UTCKidsPlayCheckinDateTime,
       KidsPlayCheckinDateTimeZone,
       InsertedDatetime,
       UpdatedDateTime,
       isnull(cast(stage_mms_KidsPlayCheckIn.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_KidsPlayCheckIn
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_kids_play_check_in @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_kids_play_check_in (
       bk_hash,
       kids_play_check_in_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_KidsPlayCheckIn.bk_hash,
       stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckInID kids_play_check_in_id,
       isnull(cast(stage_hash_mms_KidsPlayCheckIn.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_KidsPlayCheckIn
  left join h_mms_kids_play_check_in
    on stage_hash_mms_KidsPlayCheckIn.bk_hash = h_mms_kids_play_check_in.bk_hash
 where h_mms_kids_play_check_in_id is null
   and stage_hash_mms_KidsPlayCheckIn.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_kids_play_check_in
if object_id('tempdb..#l_mms_kids_play_check_in_inserts') is not null drop table #l_mms_kids_play_check_in_inserts
create table #l_mms_kids_play_check_in_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_KidsPlayCheckIn.bk_hash,
       stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckInID kids_play_check_in_id,
       stage_hash_mms_KidsPlayCheckIn.ChildCenterUsageID child_center_usage_id,
       isnull(cast(stage_hash_mms_KidsPlayCheckIn.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckInID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_KidsPlayCheckIn.ChildCenterUsageID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_KidsPlayCheckIn
 where stage_hash_mms_KidsPlayCheckIn.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_kids_play_check_in records
set @insert_date_time = getdate()
insert into l_mms_kids_play_check_in (
       bk_hash,
       kids_play_check_in_id,
       child_center_usage_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_kids_play_check_in_inserts.bk_hash,
       #l_mms_kids_play_check_in_inserts.kids_play_check_in_id,
       #l_mms_kids_play_check_in_inserts.child_center_usage_id,
       case when l_mms_kids_play_check_in.l_mms_kids_play_check_in_id is null then isnull(#l_mms_kids_play_check_in_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_kids_play_check_in_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_kids_play_check_in_inserts
  left join p_mms_kids_play_check_in
    on #l_mms_kids_play_check_in_inserts.bk_hash = p_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_kids_play_check_in
    on p_mms_kids_play_check_in.bk_hash = l_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.l_mms_kids_play_check_in_id = l_mms_kids_play_check_in.l_mms_kids_play_check_in_id
 where l_mms_kids_play_check_in.l_mms_kids_play_check_in_id is null
    or (l_mms_kids_play_check_in.l_mms_kids_play_check_in_id is not null
        and l_mms_kids_play_check_in.dv_hash <> #l_mms_kids_play_check_in_inserts.source_hash)

--calculate hash and lookup to current s_mms_kids_play_check_in
if object_id('tempdb..#s_mms_kids_play_check_in_inserts') is not null drop table #s_mms_kids_play_check_in_inserts
create table #s_mms_kids_play_check_in_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_KidsPlayCheckIn.bk_hash,
       stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckInID kids_play_check_in_id,
       stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckinDateTime kids_play_check_in_date_time,
       stage_hash_mms_KidsPlayCheckIn.UTCKidsPlayCheckinDateTime utckids_play_check_in_date_time,
       stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckinDateTimeZone kids_play_check_in_date_time_zone,
       stage_hash_mms_KidsPlayCheckIn.InsertedDatetime inserted_date_time,
       stage_hash_mms_KidsPlayCheckIn.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_KidsPlayCheckIn.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckInID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckinDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_KidsPlayCheckIn.UTCKidsPlayCheckinDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_KidsPlayCheckIn.KidsPlayCheckinDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_KidsPlayCheckIn.InsertedDatetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_KidsPlayCheckIn.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_KidsPlayCheckIn
 where stage_hash_mms_KidsPlayCheckIn.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_kids_play_check_in records
set @insert_date_time = getdate()
insert into s_mms_kids_play_check_in (
       bk_hash,
       kids_play_check_in_id,
       kids_play_check_in_date_time,
       utckids_play_check_in_date_time,
       kids_play_check_in_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_kids_play_check_in_inserts.bk_hash,
       #s_mms_kids_play_check_in_inserts.kids_play_check_in_id,
       #s_mms_kids_play_check_in_inserts.kids_play_check_in_date_time,
       #s_mms_kids_play_check_in_inserts.utckids_play_check_in_date_time,
       #s_mms_kids_play_check_in_inserts.kids_play_check_in_date_time_zone,
       #s_mms_kids_play_check_in_inserts.inserted_date_time,
       #s_mms_kids_play_check_in_inserts.updated_date_time,
       case when s_mms_kids_play_check_in.s_mms_kids_play_check_in_id is null then isnull(#s_mms_kids_play_check_in_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_kids_play_check_in_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_kids_play_check_in_inserts
  left join p_mms_kids_play_check_in
    on #s_mms_kids_play_check_in_inserts.bk_hash = p_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_kids_play_check_in
    on p_mms_kids_play_check_in.bk_hash = s_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.s_mms_kids_play_check_in_id = s_mms_kids_play_check_in.s_mms_kids_play_check_in_id
 where s_mms_kids_play_check_in.s_mms_kids_play_check_in_id is null
    or (s_mms_kids_play_check_in.s_mms_kids_play_check_in_id is not null
        and s_mms_kids_play_check_in.dv_hash <> #s_mms_kids_play_check_in_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_kids_play_check_in @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_kids_play_check_in @current_dv_batch_id

end
