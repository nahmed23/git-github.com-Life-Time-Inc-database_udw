CREATE PROC [dbo].[proc_etl_ltfmart_employee_goals] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ltfmart_EmployeeGoals

set @insert_date_time = getdate()
insert into dbo.stage_hash_ltfmart_EmployeeGoals (
       bk_hash,
       [ID],
       EmployeeID,
       ClubID,
       ValCompensationPlanID,
       GoalDate,
       NewHireTypeID,
       MALevelID,
       SDHTypeID,
       AppointmentShowGoal,
       MembershipGoal,
       VIPReferralGoal,
       HoursWorked,
       UnitQuota,
       UnitQuotaOverride,
       NDTQuota,
       HasGoal,
       IsASDH,
       IsDeptHead,
       IsNewHire,
       IsPartTime,
       OverrideUnitQuota,
       SalesCompException,
       OverrideUserRole,
       InactiveDate,
       CreateEmpID,
       CreateDateTime,
       ModifyEmpID,
       ModifyDateTime,
       RowVersion,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([ID] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [ID],
       EmployeeID,
       ClubID,
       ValCompensationPlanID,
       GoalDate,
       NewHireTypeID,
       MALevelID,
       SDHTypeID,
       AppointmentShowGoal,
       MembershipGoal,
       VIPReferralGoal,
       HoursWorked,
       UnitQuota,
       UnitQuotaOverride,
       NDTQuota,
       HasGoal,
       IsASDH,
       IsDeptHead,
       IsNewHire,
       IsPartTime,
       OverrideUnitQuota,
       SalesCompException,
       OverrideUserRole,
       InactiveDate,
       CreateEmpID,
       CreateDateTime,
       ModifyEmpID,
       ModifyDateTime,
       RowVersion,
       isnull(cast(stage_ltfmart_EmployeeGoals.GoalDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ltfmart_EmployeeGoals
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ltfmart_employee_goals @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ltfmart_employee_goals (
       bk_hash,
       employee_goals_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ltfmart_EmployeeGoals.bk_hash,
       stage_hash_ltfmart_EmployeeGoals.[ID] employee_goals_id,
       isnull(cast(stage_hash_ltfmart_EmployeeGoals.GoalDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       21,
       @insert_date_time,
       @user
  from stage_hash_ltfmart_EmployeeGoals
  left join h_ltfmart_employee_goals
    on stage_hash_ltfmart_EmployeeGoals.bk_hash = h_ltfmart_employee_goals.bk_hash
 where h_ltfmart_employee_goals_id is null
   and stage_hash_ltfmart_EmployeeGoals.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ltfmart_employee_goals
if object_id('tempdb..#l_ltfmart_employee_goals_inserts') is not null drop table #l_ltfmart_employee_goals_inserts
create table #l_ltfmart_employee_goals_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfmart_EmployeeGoals.bk_hash,
       stage_hash_ltfmart_EmployeeGoals.[ID] employee_goals_id,
       stage_hash_ltfmart_EmployeeGoals.EmployeeID employee_id,
       stage_hash_ltfmart_EmployeeGoals.ClubID club_id,
       stage_hash_ltfmart_EmployeeGoals.ValCompensationPlanID val_compensation_plan_id,
       stage_hash_ltfmart_EmployeeGoals.NewHireTypeID new_hire_type_id,
       stage_hash_ltfmart_EmployeeGoals.MALevelID ma_level_id,
       stage_hash_ltfmart_EmployeeGoals.SDHTypeID sdh_type_id,
       stage_hash_ltfmart_EmployeeGoals.CreateEmpID create_emp_id,
       stage_hash_ltfmart_EmployeeGoals.ModifyEmpID modify_emp_id,
       stage_hash_ltfmart_EmployeeGoals.GoalDate dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.EmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.ValCompensationPlanID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.NewHireTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.MALevelID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.SDHTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.CreateEmpID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.ModifyEmpID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfmart_EmployeeGoals
 where stage_hash_ltfmart_EmployeeGoals.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ltfmart_employee_goals records
set @insert_date_time = getdate()
insert into l_ltfmart_employee_goals (
       bk_hash,
       employee_goals_id,
       employee_id,
       club_id,
       val_compensation_plan_id,
       new_hire_type_id,
       ma_level_id,
       sdh_type_id,
       create_emp_id,
       modify_emp_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ltfmart_employee_goals_inserts.bk_hash,
       #l_ltfmart_employee_goals_inserts.employee_goals_id,
       #l_ltfmart_employee_goals_inserts.employee_id,
       #l_ltfmart_employee_goals_inserts.club_id,
       #l_ltfmart_employee_goals_inserts.val_compensation_plan_id,
       #l_ltfmart_employee_goals_inserts.new_hire_type_id,
       #l_ltfmart_employee_goals_inserts.ma_level_id,
       #l_ltfmart_employee_goals_inserts.sdh_type_id,
       #l_ltfmart_employee_goals_inserts.create_emp_id,
       #l_ltfmart_employee_goals_inserts.modify_emp_id,
       case when l_ltfmart_employee_goals.l_ltfmart_employee_goals_id is null then isnull(#l_ltfmart_employee_goals_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       21,
       #l_ltfmart_employee_goals_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ltfmart_employee_goals_inserts
  left join p_ltfmart_employee_goals
    on #l_ltfmart_employee_goals_inserts.bk_hash = p_ltfmart_employee_goals.bk_hash
   and p_ltfmart_employee_goals.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ltfmart_employee_goals
    on p_ltfmart_employee_goals.bk_hash = l_ltfmart_employee_goals.bk_hash
   and p_ltfmart_employee_goals.l_ltfmart_employee_goals_id = l_ltfmart_employee_goals.l_ltfmart_employee_goals_id
 where l_ltfmart_employee_goals.l_ltfmart_employee_goals_id is null
    or (l_ltfmart_employee_goals.l_ltfmart_employee_goals_id is not null
        and l_ltfmart_employee_goals.dv_hash <> #l_ltfmart_employee_goals_inserts.source_hash)

--calculate hash and lookup to current s_ltfmart_employee_goals
if object_id('tempdb..#s_ltfmart_employee_goals_inserts') is not null drop table #s_ltfmart_employee_goals_inserts
create table #s_ltfmart_employee_goals_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfmart_EmployeeGoals.bk_hash,
       stage_hash_ltfmart_EmployeeGoals.[ID] employee_goals_id,
       stage_hash_ltfmart_EmployeeGoals.GoalDate goal_date,
       stage_hash_ltfmart_EmployeeGoals.AppointmentShowGoal appointment_show_goal,
       stage_hash_ltfmart_EmployeeGoals.MembershipGoal membership_goal,
       stage_hash_ltfmart_EmployeeGoals.VIPReferralGoal vip_referral_goal,
       stage_hash_ltfmart_EmployeeGoals.HoursWorked hours_worked,
       stage_hash_ltfmart_EmployeeGoals.UnitQuota unit_quota,
       stage_hash_ltfmart_EmployeeGoals.UnitQuotaOverride unit_quota_override,
       stage_hash_ltfmart_EmployeeGoals.NDTQuota ndt_quota,
       stage_hash_ltfmart_EmployeeGoals.HasGoal has_goal,
       stage_hash_ltfmart_EmployeeGoals.IsASDH is_as_dh,
       stage_hash_ltfmart_EmployeeGoals.IsDeptHead is_dept_head,
       stage_hash_ltfmart_EmployeeGoals.IsNewHire is_new_hire,
       stage_hash_ltfmart_EmployeeGoals.IsPartTime is_part_time,
       stage_hash_ltfmart_EmployeeGoals.OverrideUnitQuota override_unit_quota,
       stage_hash_ltfmart_EmployeeGoals.SalesCompException sales_comp_exception,
       stage_hash_ltfmart_EmployeeGoals.OverrideUserRole override_user_role,
       stage_hash_ltfmart_EmployeeGoals.InactiveDate inactive_date,
       stage_hash_ltfmart_EmployeeGoals.CreateDateTime create_date_time,
       stage_hash_ltfmart_EmployeeGoals.ModifyDateTime modify_date_time,
       stage_hash_ltfmart_EmployeeGoals.RowVersion row_version,
       stage_hash_ltfmart_EmployeeGoals.GoalDate dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfmart_EmployeeGoals.GoalDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.AppointmentShowGoal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.MembershipGoal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.VIPReferralGoal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.HoursWorked as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.UnitQuota as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.UnitQuotaOverride as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.NDTQuota as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.HasGoal as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.IsASDH as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.IsDeptHead as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.IsNewHire as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.IsPartTime as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.OverrideUnitQuota as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.SalesCompException as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfmart_EmployeeGoals.OverrideUserRole as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfmart_EmployeeGoals.InactiveDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfmart_EmployeeGoals.CreateDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfmart_EmployeeGoals.ModifyDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ltfmart_EmployeeGoals.RowVersion, 2),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfmart_EmployeeGoals
 where stage_hash_ltfmart_EmployeeGoals.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ltfmart_employee_goals records
set @insert_date_time = getdate()
insert into s_ltfmart_employee_goals (
       bk_hash,
       employee_goals_id,
       goal_date,
       appointment_show_goal,
       membership_goal,
       vip_referral_goal,
       hours_worked,
       unit_quota,
       unit_quota_override,
       ndt_quota,
       has_goal,
       is_as_dh,
       is_dept_head,
       is_new_hire,
       is_part_time,
       override_unit_quota,
       sales_comp_exception,
       override_user_role,
       inactive_date,
       create_date_time,
       modify_date_time,
       row_version,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ltfmart_employee_goals_inserts.bk_hash,
       #s_ltfmart_employee_goals_inserts.employee_goals_id,
       #s_ltfmart_employee_goals_inserts.goal_date,
       #s_ltfmart_employee_goals_inserts.appointment_show_goal,
       #s_ltfmart_employee_goals_inserts.membership_goal,
       #s_ltfmart_employee_goals_inserts.vip_referral_goal,
       #s_ltfmart_employee_goals_inserts.hours_worked,
       #s_ltfmart_employee_goals_inserts.unit_quota,
       #s_ltfmart_employee_goals_inserts.unit_quota_override,
       #s_ltfmart_employee_goals_inserts.ndt_quota,
       #s_ltfmart_employee_goals_inserts.has_goal,
       #s_ltfmart_employee_goals_inserts.is_as_dh,
       #s_ltfmart_employee_goals_inserts.is_dept_head,
       #s_ltfmart_employee_goals_inserts.is_new_hire,
       #s_ltfmart_employee_goals_inserts.is_part_time,
       #s_ltfmart_employee_goals_inserts.override_unit_quota,
       #s_ltfmart_employee_goals_inserts.sales_comp_exception,
       #s_ltfmart_employee_goals_inserts.override_user_role,
       #s_ltfmart_employee_goals_inserts.inactive_date,
       #s_ltfmart_employee_goals_inserts.create_date_time,
       #s_ltfmart_employee_goals_inserts.modify_date_time,
       #s_ltfmart_employee_goals_inserts.row_version,
       case when s_ltfmart_employee_goals.s_ltfmart_employee_goals_id is null then isnull(#s_ltfmart_employee_goals_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       21,
       #s_ltfmart_employee_goals_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ltfmart_employee_goals_inserts
  left join p_ltfmart_employee_goals
    on #s_ltfmart_employee_goals_inserts.bk_hash = p_ltfmart_employee_goals.bk_hash
   and p_ltfmart_employee_goals.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ltfmart_employee_goals
    on p_ltfmart_employee_goals.bk_hash = s_ltfmart_employee_goals.bk_hash
   and p_ltfmart_employee_goals.s_ltfmart_employee_goals_id = s_ltfmart_employee_goals.s_ltfmart_employee_goals_id
 where s_ltfmart_employee_goals.s_ltfmart_employee_goals_id is null
    or (s_ltfmart_employee_goals.s_ltfmart_employee_goals_id is not null
        and s_ltfmart_employee_goals.dv_hash <> #s_ltfmart_employee_goals_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ltfmart_employee_goals @current_dv_batch_id

end
