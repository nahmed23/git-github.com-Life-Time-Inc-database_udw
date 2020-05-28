CREATE PROC [dbo].[proc_etl_mms_member_usage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MemberUsage

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MemberUsage (
       bk_hash,
       MemberUsageID,
       ClubID,
       MemberID,
       UsageDateTime,
       UTCUsageDateTime,
       UsageDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       CheckinDelinquentFlag,
       DepartmentID,
       LTFKeyOwnerID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MemberUsageID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MemberUsageID,
       ClubID,
       MemberID,
       UsageDateTime,
       UTCUsageDateTime,
       UsageDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       CheckinDelinquentFlag,
       DepartmentID,
       LTFKeyOwnerID,
       isnull(cast(stage_mms_MemberUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_MemberUsage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_member_usage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_member_usage (
       bk_hash,
       member_usage_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MemberUsage.bk_hash,
       stage_hash_mms_MemberUsage.MemberUsageID member_usage_id,
       isnull(cast(stage_hash_mms_MemberUsage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MemberUsage
  left join h_mms_member_usage
    on stage_hash_mms_MemberUsage.bk_hash = h_mms_member_usage.bk_hash
 where h_mms_member_usage_id is null
   and stage_hash_mms_MemberUsage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_member_usage
if object_id('tempdb..#l_mms_member_usage_inserts') is not null drop table #l_mms_member_usage_inserts
create table #l_mms_member_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MemberUsage.bk_hash,
       stage_hash_mms_MemberUsage.MemberUsageID member_usage_id,
       stage_hash_mms_MemberUsage.ClubID club_id,
       stage_hash_mms_MemberUsage.MemberID member_id,
       stage_hash_mms_MemberUsage.DepartmentID department_id,
       stage_hash_mms_MemberUsage.LTFKeyOwnerID ltf_key_owner_id,
       stage_hash_mms_MemberUsage.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.MemberUsageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.DepartmentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.LTFKeyOwnerID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MemberUsage
 where stage_hash_mms_MemberUsage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_member_usage records
set @insert_date_time = getdate()
insert into l_mms_member_usage (
       bk_hash,
       member_usage_id,
       club_id,
       member_id,
       department_id,
       ltf_key_owner_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_member_usage_inserts.bk_hash,
       #l_mms_member_usage_inserts.member_usage_id,
       #l_mms_member_usage_inserts.club_id,
       #l_mms_member_usage_inserts.member_id,
       #l_mms_member_usage_inserts.department_id,
       #l_mms_member_usage_inserts.ltf_key_owner_id,
       case when l_mms_member_usage.l_mms_member_usage_id is null then isnull(#l_mms_member_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_member_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_member_usage_inserts
  left join p_mms_member_usage
    on #l_mms_member_usage_inserts.bk_hash = p_mms_member_usage.bk_hash
   and p_mms_member_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_member_usage
    on p_mms_member_usage.bk_hash = l_mms_member_usage.bk_hash
   and p_mms_member_usage.l_mms_member_usage_id = l_mms_member_usage.l_mms_member_usage_id
 where l_mms_member_usage.l_mms_member_usage_id is null
    or (l_mms_member_usage.l_mms_member_usage_id is not null
        and l_mms_member_usage.dv_hash <> #l_mms_member_usage_inserts.source_hash)

--calculate hash and lookup to current s_mms_member_usage
if object_id('tempdb..#s_mms_member_usage_inserts') is not null drop table #s_mms_member_usage_inserts
create table #s_mms_member_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MemberUsage.bk_hash,
       stage_hash_mms_MemberUsage.MemberUsageID member_usage_id,
       stage_hash_mms_MemberUsage.UsageDateTime usage_date_time,
       stage_hash_mms_MemberUsage.UTCUsageDateTime utc_usage_date_time,
       stage_hash_mms_MemberUsage.UsageDateTimeZone usage_date_time_zone,
       stage_hash_mms_MemberUsage.InsertedDateTime inserted_date_time,
       stage_hash_mms_MemberUsage.UpdatedDateTime updated_date_time,
       stage_hash_mms_MemberUsage.CheckinDelinquentFlag checkin_delinquent_flag,
       stage_hash_mms_MemberUsage.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.MemberUsageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MemberUsage.UsageDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MemberUsage.UTCUsageDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_MemberUsage.UsageDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MemberUsage.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MemberUsage.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MemberUsage.CheckinDelinquentFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MemberUsage
 where stage_hash_mms_MemberUsage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_member_usage records
set @insert_date_time = getdate()
insert into s_mms_member_usage (
       bk_hash,
       member_usage_id,
       usage_date_time,
       utc_usage_date_time,
       usage_date_time_zone,
       inserted_date_time,
       updated_date_time,
       checkin_delinquent_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_member_usage_inserts.bk_hash,
       #s_mms_member_usage_inserts.member_usage_id,
       #s_mms_member_usage_inserts.usage_date_time,
       #s_mms_member_usage_inserts.utc_usage_date_time,
       #s_mms_member_usage_inserts.usage_date_time_zone,
       #s_mms_member_usage_inserts.inserted_date_time,
       #s_mms_member_usage_inserts.updated_date_time,
       #s_mms_member_usage_inserts.checkin_delinquent_flag,
       case when s_mms_member_usage.s_mms_member_usage_id is null then isnull(#s_mms_member_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_member_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_member_usage_inserts
  left join p_mms_member_usage
    on #s_mms_member_usage_inserts.bk_hash = p_mms_member_usage.bk_hash
   and p_mms_member_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_member_usage
    on p_mms_member_usage.bk_hash = s_mms_member_usage.bk_hash
   and p_mms_member_usage.s_mms_member_usage_id = s_mms_member_usage.s_mms_member_usage_id
 where s_mms_member_usage.s_mms_member_usage_id is null
    or (s_mms_member_usage.s_mms_member_usage_id is not null
        and s_mms_member_usage.dv_hash <> #s_mms_member_usage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_member_usage @current_dv_batch_id

end
