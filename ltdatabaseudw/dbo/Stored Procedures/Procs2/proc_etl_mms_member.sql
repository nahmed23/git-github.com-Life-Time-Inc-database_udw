CREATE PROC [dbo].[proc_etl_mms_member] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_mms_Member where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Member (
       bk_hash,
       MemberID,
       MembershipID,
       EmployerID,
       FirstName,
       MiddleName,
       LastName,
       DOB,
       Gender,
       ActiveFlag,
       HasMessageFlag,
       JoinDate,
       Comment,
       ValMemberTypeID,
       InsertedDateTime,
       ValNamePrefixID,
       ValNameSuffixID,
       EmailAddress,
       CreditCardAccountID,
       ChargeToaccountFlag,
       CWMedicaNumber,
       CWEnrollmentDate,
       CWProgramEnrolledFlag,
       MIPUpdatedDateTime,
       SiebelRow_ID,
       UpdatedDateTime,
       PhotoDeleteDateTime,
       Salesforce_Prospect_ID,
       MemberToken,
       Party_ID,
       LastUpdatedEmployeeID,
       Salesforce_Contact_ID,
       AssessJrMemberDuesFlag,
       CRMContactID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MemberID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MemberID,
       MembershipID,
       EmployerID,
       FirstName,
       MiddleName,
       LastName,
       DOB,
       Gender,
       ActiveFlag,
       HasMessageFlag,
       JoinDate,
       Comment,
       ValMemberTypeID,
       InsertedDateTime,
       ValNamePrefixID,
       ValNameSuffixID,
       EmailAddress,
       CreditCardAccountID,
       ChargeToaccountFlag,
       CWMedicaNumber,
       CWEnrollmentDate,
       CWProgramEnrolledFlag,
       MIPUpdatedDateTime,
       SiebelRow_ID,
       UpdatedDateTime,
       PhotoDeleteDateTime,
       Salesforce_Prospect_ID,
       MemberToken,
       Party_ID,
       LastUpdatedEmployeeID,
       Salesforce_Contact_ID,
       AssessJrMemberDuesFlag,
       CRMContactID,
       isnull(cast(stage_mms_Member.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_Member
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_member @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_member (
       bk_hash,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_Member.bk_hash,
       stage_hash_mms_Member.MemberID member_id,
       isnull(cast(stage_hash_mms_Member.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Member
  left join h_mms_member
    on stage_hash_mms_Member.bk_hash = h_mms_member.bk_hash
 where h_mms_member_id is null
   and stage_hash_mms_Member.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_member
if object_id('tempdb..#l_mms_member_inserts') is not null drop table #l_mms_member_inserts
create table #l_mms_member_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Member.bk_hash,
       stage_hash_mms_Member.MemberID member_id,
       stage_hash_mms_Member.MembershipID membership_id,
       stage_hash_mms_Member.EmployerID employer_id,
       stage_hash_mms_Member.ValMemberTypeID val_member_type_id,
       stage_hash_mms_Member.ValNamePrefixID val_name_prefix_id,
       stage_hash_mms_Member.ValNameSuffixID val_name_suffix_id,
       stage_hash_mms_Member.CreditCardAccountID credit_card_account_id,
       stage_hash_mms_Member.SiebelRow_ID siebel_row_id,
       stage_hash_mms_Member.Salesforce_Prospect_ID salesforce_prospect_id,
       stage_hash_mms_Member.Party_ID party_id,
       stage_hash_mms_Member.LastUpdatedEmployeeID last_updated_employee_id,
       stage_hash_mms_Member.Salesforce_Contact_ID salesforce_contact_id,
       stage_hash_mms_Member.CRMContactID crm_contact_id,
       isnull(cast(stage_hash_mms_Member.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Member.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.EmployerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.ValMemberTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.ValNamePrefixID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.ValNameSuffixID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.CreditCardAccountID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.SiebelRow_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.Salesforce_Prospect_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.Party_ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.Salesforce_Contact_ID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.CRMContactID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Member
 where stage_hash_mms_Member.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_member records
set @insert_date_time = getdate()
insert into l_mms_member (
       bk_hash,
       member_id,
       membership_id,
       employer_id,
       val_member_type_id,
       val_name_prefix_id,
       val_name_suffix_id,
       credit_card_account_id,
       siebel_row_id,
       salesforce_prospect_id,
       party_id,
       last_updated_employee_id,
       salesforce_contact_id,
       crm_contact_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_member_inserts.bk_hash,
       #l_mms_member_inserts.member_id,
       #l_mms_member_inserts.membership_id,
       #l_mms_member_inserts.employer_id,
       #l_mms_member_inserts.val_member_type_id,
       #l_mms_member_inserts.val_name_prefix_id,
       #l_mms_member_inserts.val_name_suffix_id,
       #l_mms_member_inserts.credit_card_account_id,
       #l_mms_member_inserts.siebel_row_id,
       #l_mms_member_inserts.salesforce_prospect_id,
       #l_mms_member_inserts.party_id,
       #l_mms_member_inserts.last_updated_employee_id,
       #l_mms_member_inserts.salesforce_contact_id,
       #l_mms_member_inserts.crm_contact_id,
       case when l_mms_member.l_mms_member_id is null then isnull(#l_mms_member_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_member_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_member_inserts
  left join p_mms_member
    on #l_mms_member_inserts.bk_hash = p_mms_member.bk_hash
   and p_mms_member.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_member
    on p_mms_member.bk_hash = l_mms_member.bk_hash
   and p_mms_member.l_mms_member_id = l_mms_member.l_mms_member_id
 where l_mms_member.l_mms_member_id is null
    or (l_mms_member.l_mms_member_id is not null
        and l_mms_member.dv_hash <> #l_mms_member_inserts.source_hash)

--calculate hash and lookup to current s_mms_member
if object_id('tempdb..#s_mms_member_inserts') is not null drop table #s_mms_member_inserts
create table #s_mms_member_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Member.bk_hash,
       stage_hash_mms_Member.MemberID member_id,
       stage_hash_mms_Member.FirstName first_name,
       stage_hash_mms_Member.MiddleName middle_name,
       stage_hash_mms_Member.LastName last_name,
       stage_hash_mms_Member.DOB dob,
       stage_hash_mms_Member.Gender gender,
       stage_hash_mms_Member.ActiveFlag active_flag,
       stage_hash_mms_Member.HasMessageFlag has_message_flag,
       stage_hash_mms_Member.JoinDate join_date,
       stage_hash_mms_Member.Comment comment,
       stage_hash_mms_Member.InsertedDateTime inserted_date_time,
       stage_hash_mms_Member.EmailAddress email_address,
       stage_hash_mms_Member.ChargeToaccountFlag charge_to_account_flag,
       stage_hash_mms_Member.CWMedicaNumber cw_medica_number,
       stage_hash_mms_Member.CWEnrollmentDate cw_enrollment_date,
       stage_hash_mms_Member.CWProgramEnrolledFlag cw_program_enrolled_flag,
       stage_hash_mms_Member.MIPUpdatedDateTime mip_updated_date_time,
       stage_hash_mms_Member.UpdatedDateTime updated_date_time,
       stage_hash_mms_Member.PhotoDeleteDateTime photo_delete_date_time,
       stage_hash_mms_Member.MemberToken member_token,
       stage_hash_mms_Member.AssessJrMemberDuesFlag assess_jr_member_dues_flag,
       isnull(cast(stage_hash_mms_Member.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Member.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.FirstName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.MiddleName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.LastName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.DOB,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.Gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.ActiveFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.HasMessageFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.JoinDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.Comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.EmailAddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.ChargeToaccountFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Member.CWMedicaNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.CWEnrollmentDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.CWProgramEnrolledFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.MIPUpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Member.PhotoDeleteDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_mms_Member.MemberToken, 2),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Member.AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Member
 where stage_hash_mms_Member.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_member records
set @insert_date_time = getdate()
insert into s_mms_member (
       bk_hash,
       member_id,
       first_name,
       middle_name,
       last_name,
       dob,
       gender,
       active_flag,
       has_message_flag,
       join_date,
       comment,
       inserted_date_time,
       email_address,
       charge_to_account_flag,
       cw_medica_number,
       cw_enrollment_date,
       cw_program_enrolled_flag,
       mip_updated_date_time,
       updated_date_time,
       photo_delete_date_time,
       member_token,
       assess_jr_member_dues_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_member_inserts.bk_hash,
       #s_mms_member_inserts.member_id,
       #s_mms_member_inserts.first_name,
       #s_mms_member_inserts.middle_name,
       #s_mms_member_inserts.last_name,
       #s_mms_member_inserts.dob,
       #s_mms_member_inserts.gender,
       #s_mms_member_inserts.active_flag,
       #s_mms_member_inserts.has_message_flag,
       #s_mms_member_inserts.join_date,
       #s_mms_member_inserts.comment,
       #s_mms_member_inserts.inserted_date_time,
       #s_mms_member_inserts.email_address,
       #s_mms_member_inserts.charge_to_account_flag,
       #s_mms_member_inserts.cw_medica_number,
       #s_mms_member_inserts.cw_enrollment_date,
       #s_mms_member_inserts.cw_program_enrolled_flag,
       #s_mms_member_inserts.mip_updated_date_time,
       #s_mms_member_inserts.updated_date_time,
       #s_mms_member_inserts.photo_delete_date_time,
       #s_mms_member_inserts.member_token,
       #s_mms_member_inserts.assess_jr_member_dues_flag,
       case when s_mms_member.s_mms_member_id is null then isnull(#s_mms_member_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_member_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_member_inserts
  left join p_mms_member
    on #s_mms_member_inserts.bk_hash = p_mms_member.bk_hash
   and p_mms_member.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_member
    on p_mms_member.bk_hash = s_mms_member.bk_hash
   and p_mms_member.s_mms_member_id = s_mms_member.s_mms_member_id
 where s_mms_member.s_mms_member_id is null
    or (s_mms_member.s_mms_member_id is not null
        and s_mms_member.dv_hash <> #s_mms_member_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_member @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_member @current_dv_batch_id
exec dbo.proc_d_mms_member_history @current_dv_batch_id

end
