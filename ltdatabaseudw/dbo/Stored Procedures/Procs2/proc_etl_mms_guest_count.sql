﻿CREATE PROC [dbo].[proc_etl_mms_guest_count] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_GuestCount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_GuestCount (
       bk_hash,
       GuestCountID,
       ClubID,
       GuestCountDate,
       MemberCount,
       NonMemberCount,
       MemberChildCount,
       NonMemberChildCount,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(GuestCountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       GuestCountID,
       ClubID,
       GuestCountDate,
       MemberCount,
       NonMemberCount,
       MemberChildCount,
       NonMemberChildCount,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_GuestCount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_GuestCount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_guest_count @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_guest_count (
       bk_hash,
       guest_count_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_GuestCount.bk_hash,
       stage_hash_mms_GuestCount.GuestCountID guest_count_id,
       isnull(cast(stage_hash_mms_GuestCount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_GuestCount
  left join h_mms_guest_count
    on stage_hash_mms_GuestCount.bk_hash = h_mms_guest_count.bk_hash
 where h_mms_guest_count_id is null
   and stage_hash_mms_GuestCount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_guest_count
if object_id('tempdb..#l_mms_guest_count_inserts') is not null drop table #l_mms_guest_count_inserts
create table #l_mms_guest_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestCount.bk_hash,
       stage_hash_mms_GuestCount.GuestCountID guest_count_id,
       stage_hash_mms_GuestCount.ClubID club_id,
       isnull(cast(stage_hash_mms_GuestCount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.GuestCountID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.ClubID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestCount
 where stage_hash_mms_GuestCount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_guest_count records
set @insert_date_time = getdate()
insert into l_mms_guest_count (
       bk_hash,
       guest_count_id,
       club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_guest_count_inserts.bk_hash,
       #l_mms_guest_count_inserts.guest_count_id,
       #l_mms_guest_count_inserts.club_id,
       case when l_mms_guest_count.l_mms_guest_count_id is null then isnull(#l_mms_guest_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_guest_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_guest_count_inserts
  left join p_mms_guest_count
    on #l_mms_guest_count_inserts.bk_hash = p_mms_guest_count.bk_hash
   and p_mms_guest_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_guest_count
    on p_mms_guest_count.bk_hash = l_mms_guest_count.bk_hash
   and p_mms_guest_count.l_mms_guest_count_id = l_mms_guest_count.l_mms_guest_count_id
 where l_mms_guest_count.l_mms_guest_count_id is null
    or (l_mms_guest_count.l_mms_guest_count_id is not null
        and l_mms_guest_count.dv_hash <> #l_mms_guest_count_inserts.source_hash)

--calculate hash and lookup to current s_mms_guest_count
if object_id('tempdb..#s_mms_guest_count_inserts') is not null drop table #s_mms_guest_count_inserts
create table #s_mms_guest_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestCount.bk_hash,
       stage_hash_mms_GuestCount.GuestCountID guest_count_id,
       stage_hash_mms_GuestCount.GuestCountDate guest_count_date,
       stage_hash_mms_GuestCount.MemberCount member_count,
       stage_hash_mms_GuestCount.NonMemberCount non_member_count,
       stage_hash_mms_GuestCount.MemberChildCount member_child_count,
       stage_hash_mms_GuestCount.NonMemberChildCount non_member_child_count,
       stage_hash_mms_GuestCount.InsertedDateTime inserted_date_time,
       stage_hash_mms_GuestCount.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_GuestCount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.GuestCountID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestCount.GuestCountDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.MemberCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.NonMemberCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.MemberChildCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestCount.NonMemberChildCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestCount.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestCount.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestCount
 where stage_hash_mms_GuestCount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_guest_count records
set @insert_date_time = getdate()
insert into s_mms_guest_count (
       bk_hash,
       guest_count_id,
       guest_count_date,
       member_count,
       non_member_count,
       member_child_count,
       non_member_child_count,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_guest_count_inserts.bk_hash,
       #s_mms_guest_count_inserts.guest_count_id,
       #s_mms_guest_count_inserts.guest_count_date,
       #s_mms_guest_count_inserts.member_count,
       #s_mms_guest_count_inserts.non_member_count,
       #s_mms_guest_count_inserts.member_child_count,
       #s_mms_guest_count_inserts.non_member_child_count,
       #s_mms_guest_count_inserts.inserted_date_time,
       #s_mms_guest_count_inserts.updated_date_time,
       case when s_mms_guest_count.s_mms_guest_count_id is null then isnull(#s_mms_guest_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_guest_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_guest_count_inserts
  left join p_mms_guest_count
    on #s_mms_guest_count_inserts.bk_hash = p_mms_guest_count.bk_hash
   and p_mms_guest_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_guest_count
    on p_mms_guest_count.bk_hash = s_mms_guest_count.bk_hash
   and p_mms_guest_count.s_mms_guest_count_id = s_mms_guest_count.s_mms_guest_count_id
 where s_mms_guest_count.s_mms_guest_count_id is null
    or (s_mms_guest_count.s_mms_guest_count_id is not null
        and s_mms_guest_count.dv_hash <> #s_mms_guest_count_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_guest_count @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_guest_count @current_dv_batch_id

end
