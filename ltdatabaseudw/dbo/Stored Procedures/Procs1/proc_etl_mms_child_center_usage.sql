﻿CREATE PROC [dbo].[proc_etl_mms_child_center_usage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ChildCenterUsage

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ChildCenterUsage (
       bk_hash,
       ChildCenterUsageID,
       MemberID,
       ClubID,
       CheckInMemberID,
       CheckInDateTime,
       CheckOutMemberID,
       CheckOutDateTime,
       UTCCheckInDateTime,
       CheckInDateTimeZone,
       UTCCheckOutDateTime,
       CheckOutDateTimeZone,
       InsertedDatetime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ChildCenterUsageID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ChildCenterUsageID,
       MemberID,
       ClubID,
       CheckInMemberID,
       CheckInDateTime,
       CheckOutMemberID,
       CheckOutDateTime,
       UTCCheckInDateTime,
       CheckInDateTimeZone,
       UTCCheckOutDateTime,
       CheckOutDateTimeZone,
       InsertedDatetime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ChildCenterUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ChildCenterUsage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_child_center_usage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_child_center_usage (
       bk_hash,
       child_center_usage_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ChildCenterUsage.bk_hash,
       stage_hash_mms_ChildCenterUsage.ChildCenterUsageID child_center_usage_id,
       isnull(cast(stage_hash_mms_ChildCenterUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ChildCenterUsage
  left join h_mms_child_center_usage
    on stage_hash_mms_ChildCenterUsage.bk_hash = h_mms_child_center_usage.bk_hash
 where h_mms_child_center_usage_id is null
   and stage_hash_mms_ChildCenterUsage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_child_center_usage
if object_id('tempdb..#l_mms_child_center_usage_inserts') is not null drop table #l_mms_child_center_usage_inserts
create table #l_mms_child_center_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ChildCenterUsage.bk_hash,
       stage_hash_mms_ChildCenterUsage.ChildCenterUsageID child_center_usage_id,
       stage_hash_mms_ChildCenterUsage.MemberID member_id,
       stage_hash_mms_ChildCenterUsage.ClubID club_id,
       stage_hash_mms_ChildCenterUsage.CheckInMemberID check_in_member_id,
       stage_hash_mms_ChildCenterUsage.CheckOutMemberID check_out_member_id,
       isnull(cast(stage_hash_mms_ChildCenterUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.ChildCenterUsageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.CheckInMemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.CheckOutMemberID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ChildCenterUsage
 where stage_hash_mms_ChildCenterUsage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_child_center_usage records
set @insert_date_time = getdate()
insert into l_mms_child_center_usage (
       bk_hash,
       child_center_usage_id,
       member_id,
       club_id,
       check_in_member_id,
       check_out_member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_child_center_usage_inserts.bk_hash,
       #l_mms_child_center_usage_inserts.child_center_usage_id,
       #l_mms_child_center_usage_inserts.member_id,
       #l_mms_child_center_usage_inserts.club_id,
       #l_mms_child_center_usage_inserts.check_in_member_id,
       #l_mms_child_center_usage_inserts.check_out_member_id,
       case when l_mms_child_center_usage.l_mms_child_center_usage_id is null then isnull(#l_mms_child_center_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_child_center_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_child_center_usage_inserts
  left join p_mms_child_center_usage
    on #l_mms_child_center_usage_inserts.bk_hash = p_mms_child_center_usage.bk_hash
   and p_mms_child_center_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_child_center_usage
    on p_mms_child_center_usage.bk_hash = l_mms_child_center_usage.bk_hash
   and p_mms_child_center_usage.l_mms_child_center_usage_id = l_mms_child_center_usage.l_mms_child_center_usage_id
 where l_mms_child_center_usage.l_mms_child_center_usage_id is null
    or (l_mms_child_center_usage.l_mms_child_center_usage_id is not null
        and l_mms_child_center_usage.dv_hash <> #l_mms_child_center_usage_inserts.source_hash)

--calculate hash and lookup to current s_mms_child_center_usage
if object_id('tempdb..#s_mms_child_center_usage_inserts') is not null drop table #s_mms_child_center_usage_inserts
create table #s_mms_child_center_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ChildCenterUsage.bk_hash,
       stage_hash_mms_ChildCenterUsage.ChildCenterUsageID child_center_usage_id,
       stage_hash_mms_ChildCenterUsage.CheckInDateTime check_in_date_time,
       stage_hash_mms_ChildCenterUsage.CheckOutDateTime check_out_date_time,
       stage_hash_mms_ChildCenterUsage.UTCCheckInDateTime utc_check_in_date_time,
       stage_hash_mms_ChildCenterUsage.CheckInDateTimeZone check_in_date_time_zone,
       stage_hash_mms_ChildCenterUsage.UTCCheckOutDateTime utc_check_out_date_time,
       stage_hash_mms_ChildCenterUsage.CheckOutDateTimeZone check_out_date_time_zone,
       stage_hash_mms_ChildCenterUsage.InsertedDatetime inserted_date_time,
       stage_hash_mms_ChildCenterUsage.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_ChildCenterUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ChildCenterUsage.ChildCenterUsageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.CheckInDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.CheckOutDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.UTCCheckInDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ChildCenterUsage.CheckInDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.UTCCheckOutDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ChildCenterUsage.CheckOutDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.InsertedDatetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ChildCenterUsage.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ChildCenterUsage
 where stage_hash_mms_ChildCenterUsage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_child_center_usage records
set @insert_date_time = getdate()
insert into s_mms_child_center_usage (
       bk_hash,
       child_center_usage_id,
       check_in_date_time,
       check_out_date_time,
       utc_check_in_date_time,
       check_in_date_time_zone,
       utc_check_out_date_time,
       check_out_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_child_center_usage_inserts.bk_hash,
       #s_mms_child_center_usage_inserts.child_center_usage_id,
       #s_mms_child_center_usage_inserts.check_in_date_time,
       #s_mms_child_center_usage_inserts.check_out_date_time,
       #s_mms_child_center_usage_inserts.utc_check_in_date_time,
       #s_mms_child_center_usage_inserts.check_in_date_time_zone,
       #s_mms_child_center_usage_inserts.utc_check_out_date_time,
       #s_mms_child_center_usage_inserts.check_out_date_time_zone,
       #s_mms_child_center_usage_inserts.inserted_date_time,
       #s_mms_child_center_usage_inserts.updated_date_time,
       case when s_mms_child_center_usage.s_mms_child_center_usage_id is null then isnull(#s_mms_child_center_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_child_center_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_child_center_usage_inserts
  left join p_mms_child_center_usage
    on #s_mms_child_center_usage_inserts.bk_hash = p_mms_child_center_usage.bk_hash
   and p_mms_child_center_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_child_center_usage
    on p_mms_child_center_usage.bk_hash = s_mms_child_center_usage.bk_hash
   and p_mms_child_center_usage.s_mms_child_center_usage_id = s_mms_child_center_usage.s_mms_child_center_usage_id
 where s_mms_child_center_usage.s_mms_child_center_usage_id is null
    or (s_mms_child_center_usage.s_mms_child_center_usage_id is not null
        and s_mms_child_center_usage.dv_hash <> #s_mms_child_center_usage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_child_center_usage @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_child_center_usage @current_dv_batch_id

end
