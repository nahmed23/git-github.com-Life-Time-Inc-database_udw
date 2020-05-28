CREATE PROC [dbo].[proc_etl_mms_company] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Company

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Company (
       bk_hash,
       CompanyID,
       AccountRepInitials,
       CompanyName,
       PrintUsageReportFlag,
       CorporateCode,
       InsertedDateTime,
       StartDate,
       EndDate,
       AccountRepName,
       InitiationFee,
       UpdatedDateTime,
       EnrollmentDiscPercentage,
       MACEnrollmentDiscPercentage,
       InvoiceFlag,
       DollarDiscount,
       AdminFee,
       OverridePercentage,
       EFTAccountNumber,
       UsageReportFlag,
       ReportToEmailAddress,
       UsageReportMemberType,
       SmallBusinessFlag,
       AccountOwner,
       SubsidyMeasurement,
       OpportunityRecordType,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CompanyID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       CompanyID,
       AccountRepInitials,
       CompanyName,
       PrintUsageReportFlag,
       CorporateCode,
       InsertedDateTime,
       StartDate,
       EndDate,
       AccountRepName,
       InitiationFee,
       UpdatedDateTime,
       EnrollmentDiscPercentage,
       MACEnrollmentDiscPercentage,
       InvoiceFlag,
       DollarDiscount,
       AdminFee,
       OverridePercentage,
       EFTAccountNumber,
       UsageReportFlag,
       ReportToEmailAddress,
       UsageReportMemberType,
       SmallBusinessFlag,
       AccountOwner,
       SubsidyMeasurement,
       OpportunityRecordType,
       isnull(cast(stage_mms_Company.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_Company
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_company @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_company (
       bk_hash,
       company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_Company.bk_hash,
       stage_hash_mms_Company.CompanyID company_id,
       isnull(cast(stage_hash_mms_Company.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Company
  left join h_mms_company
    on stage_hash_mms_Company.bk_hash = h_mms_company.bk_hash
 where h_mms_company_id is null
   and stage_hash_mms_Company.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mms_company
if object_id('tempdb..#s_mms_company_inserts') is not null drop table #s_mms_company_inserts
create table #s_mms_company_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Company.bk_hash,
       stage_hash_mms_Company.CompanyID company_id,
       stage_hash_mms_Company.AccountRepInitials account_rep_initials,
       stage_hash_mms_Company.CompanyName company_name,
       stage_hash_mms_Company.PrintUsageReportFlag print_usage_report_flag,
       stage_hash_mms_Company.CorporateCode corporate_code,
       stage_hash_mms_Company.InsertedDateTime inserted_date_time,
       stage_hash_mms_Company.StartDate start_date,
       stage_hash_mms_Company.EndDate end_date,
       stage_hash_mms_Company.AccountRepName account_rep_name,
       stage_hash_mms_Company.InitiationFee initiation_fee,
       stage_hash_mms_Company.UpdatedDateTime updated_date_time,
       stage_hash_mms_Company.EnrollmentDiscPercentage enrollment_disc_percentage,
       stage_hash_mms_Company.MACEnrollmentDiscPercentage mac_enrollment_disc_percentage,
       stage_hash_mms_Company.InvoiceFlag invoice_flag,
       stage_hash_mms_Company.DollarDiscount dollar_discount,
       stage_hash_mms_Company.AdminFee admin_fee,
       stage_hash_mms_Company.OverridePercentage override_percentage,
       stage_hash_mms_Company.EFTAccountNumber eft_account_number,
       stage_hash_mms_Company.UsageReportFlag usage_report_flag,
       stage_hash_mms_Company.ReportToEmailAddress report_to_email_address,
       stage_hash_mms_Company.UsageReportMemberType usage_report_member_type,
       stage_hash_mms_Company.SmallBusinessFlag small_business_flag,
       stage_hash_mms_Company.AccountOwner account_owner,
       stage_hash_mms_Company.SubsidyMeasurement subsidy_measurement,
       stage_hash_mms_Company.OpportunityRecordType opportunity_record_type,
       stage_hash_mms_Company.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Company.CompanyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.AccountRepInitials,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.CompanyName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.PrintUsageReportFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.CorporateCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Company.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Company.StartDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Company.EndDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.AccountRepName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.InitiationFee as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Company.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.EnrollmentDiscPercentage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.MACEnrollmentDiscPercentage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.InvoiceFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.DollarDiscount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.AdminFee as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.OverridePercentage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.EFTAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.UsageReportFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.ReportToEmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.UsageReportMemberType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Company.SmallBusinessFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.AccountOwner,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.SubsidyMeasurement,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Company.OpportunityRecordType,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Company
 where stage_hash_mms_Company.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_company records
set @insert_date_time = getdate()
insert into s_mms_company (
       bk_hash,
       company_id,
       account_rep_initials,
       company_name,
       print_usage_report_flag,
       corporate_code,
       inserted_date_time,
       start_date,
       end_date,
       account_rep_name,
       initiation_fee,
       updated_date_time,
       enrollment_disc_percentage,
       mac_enrollment_disc_percentage,
       invoice_flag,
       dollar_discount,
       admin_fee,
       override_percentage,
       eft_account_number,
       usage_report_flag,
       report_to_email_address,
       usage_report_member_type,
       small_business_flag,
       account_owner,
       subsidy_measurement,
       opportunity_record_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_company_inserts.bk_hash,
       #s_mms_company_inserts.company_id,
       #s_mms_company_inserts.account_rep_initials,
       #s_mms_company_inserts.company_name,
       #s_mms_company_inserts.print_usage_report_flag,
       #s_mms_company_inserts.corporate_code,
       #s_mms_company_inserts.inserted_date_time,
       #s_mms_company_inserts.start_date,
       #s_mms_company_inserts.end_date,
       #s_mms_company_inserts.account_rep_name,
       #s_mms_company_inserts.initiation_fee,
       #s_mms_company_inserts.updated_date_time,
       #s_mms_company_inserts.enrollment_disc_percentage,
       #s_mms_company_inserts.mac_enrollment_disc_percentage,
       #s_mms_company_inserts.invoice_flag,
       #s_mms_company_inserts.dollar_discount,
       #s_mms_company_inserts.admin_fee,
       #s_mms_company_inserts.override_percentage,
       #s_mms_company_inserts.eft_account_number,
       #s_mms_company_inserts.usage_report_flag,
       #s_mms_company_inserts.report_to_email_address,
       #s_mms_company_inserts.usage_report_member_type,
       #s_mms_company_inserts.small_business_flag,
       #s_mms_company_inserts.account_owner,
       #s_mms_company_inserts.subsidy_measurement,
       #s_mms_company_inserts.opportunity_record_type,
       case when s_mms_company.s_mms_company_id is null then isnull(#s_mms_company_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_company_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_company_inserts
  left join p_mms_company
    on #s_mms_company_inserts.bk_hash = p_mms_company.bk_hash
   and p_mms_company.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_company
    on p_mms_company.bk_hash = s_mms_company.bk_hash
   and p_mms_company.s_mms_company_id = s_mms_company.s_mms_company_id
 where s_mms_company.s_mms_company_id is null
    or (s_mms_company.s_mms_company_id is not null
        and s_mms_company.dv_hash <> #s_mms_company_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_company @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_company @current_dv_batch_id

end
