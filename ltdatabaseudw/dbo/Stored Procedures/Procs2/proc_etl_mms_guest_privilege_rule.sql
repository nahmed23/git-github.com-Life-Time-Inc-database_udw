CREATE PROC [dbo].[proc_etl_mms_guest_privilege_rule] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_GuestPrivilegeRule

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_GuestPrivilegeRule (
       bk_hash,
       GuestPrivilegeRuleID,
       NumberOfGuests,
       ValPeriodTypeID,
       LowClubAccessLevel,
       HighClubAccessLevel,
       MembershipStartDate,
       MembershipEndDate,
       InsertedDateTime,
       UpdatedDateTime,
       ValCardLevelID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(GuestPrivilegeRuleID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       GuestPrivilegeRuleID,
       NumberOfGuests,
       ValPeriodTypeID,
       LowClubAccessLevel,
       HighClubAccessLevel,
       MembershipStartDate,
       MembershipEndDate,
       InsertedDateTime,
       UpdatedDateTime,
       ValCardLevelID,
       isnull(cast(stage_mms_GuestPrivilegeRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_GuestPrivilegeRule
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_guest_privilege_rule @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_guest_privilege_rule (
       bk_hash,
       guest_privilege_rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_GuestPrivilegeRule.bk_hash,
       stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID guest_privilege_rule_id,
       isnull(cast(stage_hash_mms_GuestPrivilegeRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_GuestPrivilegeRule
  left join h_mms_guest_privilege_rule
    on stage_hash_mms_GuestPrivilegeRule.bk_hash = h_mms_guest_privilege_rule.bk_hash
 where h_mms_guest_privilege_rule_id is null
   and stage_hash_mms_GuestPrivilegeRule.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_guest_privilege_rule
if object_id('tempdb..#l_mms_guest_privilege_rule_inserts') is not null drop table #l_mms_guest_privilege_rule_inserts
create table #l_mms_guest_privilege_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestPrivilegeRule.bk_hash,
       stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID guest_privilege_rule_id,
       stage_hash_mms_GuestPrivilegeRule.ValPeriodTypeID val_period_type_id,
       isnull(cast(stage_hash_mms_GuestPrivilegeRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.ValPeriodTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestPrivilegeRule
 where stage_hash_mms_GuestPrivilegeRule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_guest_privilege_rule records
set @insert_date_time = getdate()
insert into l_mms_guest_privilege_rule (
       bk_hash,
       guest_privilege_rule_id,
       val_period_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_guest_privilege_rule_inserts.bk_hash,
       #l_mms_guest_privilege_rule_inserts.guest_privilege_rule_id,
       #l_mms_guest_privilege_rule_inserts.val_period_type_id,
       case when l_mms_guest_privilege_rule.l_mms_guest_privilege_rule_id is null then isnull(#l_mms_guest_privilege_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_guest_privilege_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_guest_privilege_rule_inserts
  left join p_mms_guest_privilege_rule
    on #l_mms_guest_privilege_rule_inserts.bk_hash = p_mms_guest_privilege_rule.bk_hash
   and p_mms_guest_privilege_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_guest_privilege_rule
    on p_mms_guest_privilege_rule.bk_hash = l_mms_guest_privilege_rule.bk_hash
   and p_mms_guest_privilege_rule.l_mms_guest_privilege_rule_id = l_mms_guest_privilege_rule.l_mms_guest_privilege_rule_id
 where l_mms_guest_privilege_rule.l_mms_guest_privilege_rule_id is null
    or (l_mms_guest_privilege_rule.l_mms_guest_privilege_rule_id is not null
        and l_mms_guest_privilege_rule.dv_hash <> #l_mms_guest_privilege_rule_inserts.source_hash)

--calculate hash and lookup to current l_mms_guest_privilege_rule_1
if object_id('tempdb..#l_mms_guest_privilege_rule_1_inserts') is not null drop table #l_mms_guest_privilege_rule_1_inserts
create table #l_mms_guest_privilege_rule_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestPrivilegeRule.bk_hash,
       stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID guest_privilege_rule_id,
       stage_hash_mms_GuestPrivilegeRule.ValCardLevelID val_card_level_id,
       isnull(cast(stage_hash_mms_GuestPrivilegeRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.ValCardLevelID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestPrivilegeRule
 where stage_hash_mms_GuestPrivilegeRule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_guest_privilege_rule_1 records
set @insert_date_time = getdate()
insert into l_mms_guest_privilege_rule_1 (
       bk_hash,
       guest_privilege_rule_id,
       val_card_level_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_guest_privilege_rule_1_inserts.bk_hash,
       #l_mms_guest_privilege_rule_1_inserts.guest_privilege_rule_id,
       #l_mms_guest_privilege_rule_1_inserts.val_card_level_id,
       case when l_mms_guest_privilege_rule_1.l_mms_guest_privilege_rule_1_id is null then isnull(#l_mms_guest_privilege_rule_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_guest_privilege_rule_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_guest_privilege_rule_1_inserts
  left join p_mms_guest_privilege_rule
    on #l_mms_guest_privilege_rule_1_inserts.bk_hash = p_mms_guest_privilege_rule.bk_hash
   and p_mms_guest_privilege_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_guest_privilege_rule_1
    on p_mms_guest_privilege_rule.bk_hash = l_mms_guest_privilege_rule_1.bk_hash
   and p_mms_guest_privilege_rule.l_mms_guest_privilege_rule_1_id = l_mms_guest_privilege_rule_1.l_mms_guest_privilege_rule_1_id
 where l_mms_guest_privilege_rule_1.l_mms_guest_privilege_rule_1_id is null
    or (l_mms_guest_privilege_rule_1.l_mms_guest_privilege_rule_1_id is not null
        and l_mms_guest_privilege_rule_1.dv_hash <> #l_mms_guest_privilege_rule_1_inserts.source_hash)

--calculate hash and lookup to current s_mms_guest_privilege_rule
if object_id('tempdb..#s_mms_guest_privilege_rule_inserts') is not null drop table #s_mms_guest_privilege_rule_inserts
create table #s_mms_guest_privilege_rule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestPrivilegeRule.bk_hash,
       stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID guest_privilege_rule_id,
       stage_hash_mms_GuestPrivilegeRule.NumberOfGuests number_of_guests,
       stage_hash_mms_GuestPrivilegeRule.LowClubAccessLevel low_club_access_level,
       stage_hash_mms_GuestPrivilegeRule.HighClubAccessLevel high_club_access_level,
       stage_hash_mms_GuestPrivilegeRule.MembershipStartDate membership_start_date,
       stage_hash_mms_GuestPrivilegeRule.MembershipEndDate membership_end_date,
       stage_hash_mms_GuestPrivilegeRule.InsertedDateTime inserted_date_time,
       stage_hash_mms_GuestPrivilegeRule.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_GuestPrivilegeRule.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.GuestPrivilegeRuleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.NumberOfGuests as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.LowClubAccessLevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestPrivilegeRule.HighClubAccessLevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestPrivilegeRule.MembershipStartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestPrivilegeRule.MembershipEndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestPrivilegeRule.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestPrivilegeRule.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestPrivilegeRule
 where stage_hash_mms_GuestPrivilegeRule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_guest_privilege_rule records
set @insert_date_time = getdate()
insert into s_mms_guest_privilege_rule (
       bk_hash,
       guest_privilege_rule_id,
       number_of_guests,
       low_club_access_level,
       high_club_access_level,
       membership_start_date,
       membership_end_date,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_guest_privilege_rule_inserts.bk_hash,
       #s_mms_guest_privilege_rule_inserts.guest_privilege_rule_id,
       #s_mms_guest_privilege_rule_inserts.number_of_guests,
       #s_mms_guest_privilege_rule_inserts.low_club_access_level,
       #s_mms_guest_privilege_rule_inserts.high_club_access_level,
       #s_mms_guest_privilege_rule_inserts.membership_start_date,
       #s_mms_guest_privilege_rule_inserts.membership_end_date,
       #s_mms_guest_privilege_rule_inserts.inserted_date_time,
       #s_mms_guest_privilege_rule_inserts.updated_date_time,
       case when s_mms_guest_privilege_rule.s_mms_guest_privilege_rule_id is null then isnull(#s_mms_guest_privilege_rule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_guest_privilege_rule_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_guest_privilege_rule_inserts
  left join p_mms_guest_privilege_rule
    on #s_mms_guest_privilege_rule_inserts.bk_hash = p_mms_guest_privilege_rule.bk_hash
   and p_mms_guest_privilege_rule.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_guest_privilege_rule
    on p_mms_guest_privilege_rule.bk_hash = s_mms_guest_privilege_rule.bk_hash
   and p_mms_guest_privilege_rule.s_mms_guest_privilege_rule_id = s_mms_guest_privilege_rule.s_mms_guest_privilege_rule_id
 where s_mms_guest_privilege_rule.s_mms_guest_privilege_rule_id is null
    or (s_mms_guest_privilege_rule.s_mms_guest_privilege_rule_id is not null
        and s_mms_guest_privilege_rule.dv_hash <> #s_mms_guest_privilege_rule_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_guest_privilege_rule @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_guest_privilege_rule @current_dv_batch_id

end
