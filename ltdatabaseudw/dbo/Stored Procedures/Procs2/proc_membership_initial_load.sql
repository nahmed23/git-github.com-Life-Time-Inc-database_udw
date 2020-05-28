CREATE PROC [dbo].[proc_membership_initial_load] AS
begin

/*


-- prepare the tables
exec proc_util_reload_dv_object 'mms_membership'
--go
*/

--run the following bcp commands to populate the MMSMembership table from LTFDWStg

--bcp dbo.MMSMembership out \\mndevinf01\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Membership.txt -d LTFDWStg_dev -S mnqasqlvs02\mnqasqlvs02 -T -r "\r" -t "|" -c
--bcp dbo.MMSMembership in \\mndevinf01\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Membership.txt -S aeqadb02.database.windows.net -U "InformaticaUser" -P "Informatic@1" -d lt_udw_dev -r "\r" -t "|" -q -e \\mndevinf01\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Membership.log -c

/*
-- prepare the dv tables
truncate table dbo.h_mms_membership
truncate table dbo.l_mms_membership
truncate table dbo.s_mms_membership_main
truncate table dbo.p_mms_membership
truncate table dbo.stage_mms_Membership
exec proc_util_create_base_records @table_name = 'h_mms_membership'
exec proc_util_create_base_records @table_name = 'l_mms_membership'
exec proc_util_create_base_records @table_name = 's_mms_membership_main'
exec proc_p_mms_membership @current_dv_batch_id = -1
--go
select getdate()
*/

--Select the records from MMSMembership to be staged and inserted into the dv tables
--We only want 1 record per membership for any particular timestamp
--  Do row_number ranking
--    Partition by the following
--      MembershipID
--      Calculated update_insert_date
--        If MMSUpdatedDateTime is null then MMSInsertedDateTime
--        Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--        Else add 27 hours to the date portion of MMSUpdatedDateTime
--    Order by MMSMembershipKey in descending order (this keeps the most recent record where one or more are in LTFDWStg with the same date)
--  Only keep the records with rank = 1
if object_id('tempdb.dbo.#stage_mms_Membership') is not null drop table #stage_mms_Membership
create table dbo.#stage_mms_Membership with (location=user_db, distribution = hash(bk_hash)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) bk_hash,
       x.*,
       row_number() over(partition by MembershipID order by x.update_insert_date) rank2,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.PurchaserID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.AdvisorEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.CompanyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MembershipTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.PromotionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.JrMemberDuesProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.SalesforceProspectID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.QualifiedSalesPromotionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.SalesforceAccountID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.SalesforceOpportunityID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.CRMOpportunityID,'z#@$k%&P')+
--                                         'P%#&z$@k'+isnull(cast(x.PreviousMembershipTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.PriorPlusMembershipTypeID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) l_mms_membership_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.LegacyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.ActivationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.ExpirationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.TotalContractAmount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.Comments,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MandatoryCommentFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValEFTOptionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValEnrollmentTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValTerminationReasonID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValMembershipStatusID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.CancellationRequestDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.CreatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.UTCCreatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.CreatedDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSInsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValMembershipSourceID as varchar(500)),'z#@$k%&P')+
--                                         'P%#&z$@k'+isnull(cast(x.AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSUpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MoneyBackCancelPolicyDays as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.JoinFeePaid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ChildCenterUnrestrictedCheckoutFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValTerminationReasonClubTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.CurrentPrice as varchar(500)),'z#@$k%&P')+
--                                         'P%#&z$@k'+isnull(cast(x.PreviousPrice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.PriorPlusPrice as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) s_mms_membership_main_hash
  from (select row_number() over(partition by MembershipID,
                                              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                               end
                                 order by MMSMembershipKey desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
               end update_insert_date,
               *
          from MMSMembership) x
 where rank1 = 1
--go
select getdate()

/*
select * from h_mms_membership
select * from l_mms_membership
select * from s_mms_membership_main
select * from p_mms_membership
*/

-- Create the h records.
-- Only use records where rank2 = 1 (first record in a series: min(update_insert_date))
-- dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.h_mms_membership(
       h_mms_membership_id, 
       bk_hash,
       membership_id, 
       dv_load_date_time, 
       dv_batch_id, 
       dv_r_load_source_id, 
       dv_inserted_date_time, 
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_id) h_mms_membership_id,
       x.*
  from (select bk_hash,
               MembershipID membership_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
          from dbo.#stage_mms_Membership
         where rank2 = 1) x
--go
select getdate()

-- Create the l records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into l_mms_membership (
       l_mms_membership_id,
       bk_hash,
       membership_id,
       club_id,
       purchaser_id,
       advisor_employee_id,
       company_id,
       membership_type_id,
       promotion_id,
       jr_member_dues_product_id,
       salesforce_prospect_id,
       last_updated_employee_id,
       qualified_sales_promotion_id,
       salesforce_account_id,
       salesforce_opportunity_id,
       crm_opportunity_id,
--       previous_membership_type_id,
       prior_plus_membership_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_id) l_mms_membership_id,
       x.*
  from (select #stage_mms_Membership.bk_hash,
               #stage_mms_Membership.MembershipID membership_id,
               #stage_mms_Membership.ClubID club_id,
               #stage_mms_Membership.PurchaserID purchaser_id,
               #stage_mms_Membership.AdvisorEmployeeID advisor_employee_id,
               #stage_mms_Membership.CompanyID company_id,
               #stage_mms_Membership.MembershipTypeID membership_type_id,
               #stage_mms_Membership.PromotionID promotion_id,
               #stage_mms_Membership.JrMemberDuesProductID jr_member_dues_product_id,
               #stage_mms_Membership.SalesforceProspectID salesforce_prospect_id,
               #stage_mms_Membership.LastUpdatedEmployeeID last_updated_employee_id,
               #stage_mms_Membership.QualifiedSalesPromotionID qualified_sales_promotion_id,
               #stage_mms_Membership.SalesforceAccountID salesforce_account_id,
               #stage_mms_Membership.SalesforceOpportunityID salesforce_opportunity_id,
               #stage_mms_Membership.CRMOpportunityID crm_opportunity_id,
--               #stage_mms_Membership.PreviousMembershipTypeID previous_membership_type_id,
               #stage_mms_Membership.PriorPlusMembershipTypeID prior_plus_membership_type_id,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Membership.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Membership.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Membership.l_mms_membership_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_Membership
          left join dbo.#stage_mms_Membership prior
            on #stage_mms_Membership.MembershipID = prior.MembershipID
           and #stage_mms_Membership.rank2 = prior.rank2 + 1
         where #stage_mms_Membership.l_mms_membership_hash != isnull(prior.l_mms_membership_hash, ''))x
--go
select getdate()

-- Create the s records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into s_mms_membership_main (
       s_mms_membership_main_id,
       bk_hash,
       membership_id,
       legacy_code,
       activation_date,
       expiration_date,
       total_contract_amount,
       comments,
       mandatory_comment_flag,
       val_eft_option_id,
       val_enrollment_type_id,
       val_termination_reason_id,
       val_membership_status_id,
       cancellation_request_date,
       created_date_time,
       utc_created_date_time,
       created_date_time_zone,
       inserted_date_time,
       val_membership_source_id,
--       assess_jr_member_dues_flag,
       updated_date_time,
       money_back_cancel_policy_days,
       join_fee_paid,
       child_center_unrestricted_checkout_flag,
       val_termination_reason_club_type_id,
       current_price,
--       previous_price,
       prior_plus_price,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select row_number() over(order by x.dv_batch_id, x.membership_id) s_mms_membership_main_id,
       x.*
  from (select #stage_mms_Membership.bk_hash,
               #stage_mms_Membership.MembershipID membership_id,
               #stage_mms_Membership.LegacyCode legacy_code,
               #stage_mms_Membership.ActivationDate activation_date,
               #stage_mms_Membership.ExpirationDate expiration_date,
               #stage_mms_Membership.TotalContractAmount total_contract_amount,
               #stage_mms_Membership.Comments comments,
               #stage_mms_Membership.MandatoryCommentFlag mandatory_comment_flag,
               #stage_mms_Membership.ValEFTOptionID val_eft_option_id,
               #stage_mms_Membership.ValEnrollmentTypeID val_enrollment_type_id,
               #stage_mms_Membership.ValTerminationReasonID val_termination_reason_id,
               #stage_mms_Membership.ValMembershipStatusID val_membership_status_id,
               #stage_mms_Membership.CancellationRequestDate cancellation_request_date,
               #stage_mms_Membership.CreatedDateTime created_date_time,
               #stage_mms_Membership.UTCCreatedDateTime utc_created_date_time,
               #stage_mms_Membership.CreatedDateTimeZone created_date_time_zone,
               #stage_mms_Membership.MMSInsertedDateTime inserted_date_time,
               #stage_mms_Membership.ValMembershipSourceID val_membership_source_id,
--               #stage_mms_Membership.AssessJrMemberDuesFlag assess_jr_member_dues_flag,
               #stage_mms_Membership.MMSUpdatedDateTime updated_date_time,
               #stage_mms_Membership.MoneyBackCancelPolicyDays money_back_cancel_policy_days,
               #stage_mms_Membership.JoinFeePaid join_fee_paid,
               #stage_mms_Membership.ChildCenterUnrestrictedCheckoutFlag child_center_unrestricted_checkout_flag,
               #stage_mms_Membership.ValTerminationReasonClubTypeID val_termination_reason_club_type_id,
               #stage_mms_Membership.CurrentPrice current_price,
--               #stage_mms_Membership.PreviousPrice previous_price,
               #stage_mms_Membership.PriorPlusPrice prior_plus_price,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Membership.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Membership.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Membership.s_mms_membership_main_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user,
               0 dv_deleted
          from dbo.#stage_mms_Membership
          left join dbo.#stage_mms_Membership prior
            on #stage_mms_Membership.MembershipID = prior.MembershipID
           and #stage_mms_Membership.rank2 = prior.rank2 + 1
         where #stage_mms_Membership.s_mms_membership_main_hash != isnull(prior.s_mms_membership_main_hash, '')) x
--go
select getdate()

-- Insert history into the staging table
-- Calculate dv_batch_id (convert the following to YYYYMMSSHHMISS)
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
insert dbo.stage_mms_Membership (
       stage_mms_Membership_id, 
       MembershipID, 
       ClubID, 
       PurchaserID, 
       LegacyCode, 
       AdvisorEmployeeID, 
       ActivationDate, 
       ExpirationDate, 
       TotalContractAmount, 
       CompanyID, 
       Comments, 
       MandatoryCommentFlag, 
       ValEFTOptionID, 
       ValEnrollmentTypeID, 
       ValTerminationReasonID, 
       MembershipTypeID, 
       ValMembershipStatusID, 
       CancellationRequestDate, 
       CreatedDateTime, 
       UTCCreatedDateTime, 
       CreatedDateTimeZone, 
       InsertedDateTime, 
       ValMembershipSourceID, 
       AssessJrMemberDuesFlag, -- Populate with NULLS until we remove the BCP code
       UpdatedDateTime, 
       PromotionID, 
       JrMemberDuesProductID, 
       Salesforce_Prospect_ID, 
       MoneyBackCancelPolicyDays, 
       LastUpdatedEmployeeID, 
       QualifiedSalesPromotionID, 
       JoinFeePaid, 
       Salesforce_Account_ID, 
       Salesforce_Opportunity_ID, 
       ChildCenterUnrestrictedCheckoutFlag, 
       ValTerminationReasonClubTypeID, 
       CRMOpportunityID, 
       CurrentPrice, 
       PreviousPrice, -- Populate with NULL until we remove the BCP logic
       PreviousMembershipTypeID, -- Populate with NULL until we remove the BCP logic
       PriorPlusPrice, 
       PriorPlusMembershipTypeID, 
       dv_inserted_date_time, 
       dv_insert_user, 
       dv_batch_id)
select row_number() over(order by x.dv_batch_id, x.MembershipID) stage_mms_Membership_id,
       x.*
  from (select MembershipID, 
               ClubID, 
               PurchaserID, 
               LegacyCode, 
               AdvisorEmployeeID, 
               ActivationDate, 
               ExpirationDate, 
               TotalContractAmount, 
               CompanyID, 
               Comments, 
               MandatoryCommentFlag, 
               ValEFTOptionID, 
               ValEnrollmentTypeID, 
               ValTerminationReasonID, 
               MembershipTypeID, 
               ValMembershipStatusID, 
               CancellationRequestDate, 
               CreatedDateTime, 
               UTCCreatedDateTime, 
               CreatedDateTimeZone, 
               MMSInsertedDateTime, 
               ValMembershipSourceID, 
               null AssessJrMemberDuesFlag, -- Populate with NULL until we remove the BCP logic
               MMSUpdatedDateTime, 
               PromotionID, 
               JrMemberDuesProductID, 
               SalesforceProspectID Salesforce_Prospect_ID, 
               MoneyBackCancelPolicyDays, 
               LastUpdatedEmployeeID, 
               QualifiedSalesPromotionID, 
               JoinFeePaid, 
               SalesforceAccountID Salesforce_Account_ID, 
               SalesforceOpportunityID Salesforce_Opportunity_ID, 
               ChildCenterUnrestrictedCheckoutFlag, 
               ValTerminationReasonClubTypeID, 
               CRMOpportunityID, 
               CurrentPrice, 
               null PreviousPrice, -- Populate with NULL until we remove the BCP logic
               null PreviousMembershipTypeID, -- Populate with NULL until we remove the BCP logic
               PriorPlusPrice, 
               PriorPlusMembershipTypeID, 
               getdate() dv_inserted_date_time,
               suser_sname() dv_insert_user, 
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Membership.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id
          from #stage_mms_Membership) x
--go
select getdate()

/*
-- Make sure there are no duplicates as this will potentially cause the pit table to grow out of control
select membership_id from h_mms_membership group by membership_id having count(*) > 1
select membership_id, dv_load_date_time from l_mms_membership group by membership_id, dv_load_date_time having count(*) > 1
select membership_id, dv_load_date_time from s_mms_membership_main group by membership_id, dv_load_date_time having count(*) > 1
select MembershipID, dv_batch_id from stage_mms_Membership group by MembershipID, dv_batch_id having count(*) > 1
*/

update dv_sequence_number
   set max_sequence_number = (select max(h_mms_membership_id) from h_mms_membership)
 where table_name = 'h_mms_membership'
--go
update dv_sequence_number
   set max_sequence_number = (select max(l_mms_membership_id) from l_mms_membership)
 where table_name = 'l_mms_membership'
--go
update dv_sequence_number
   set max_sequence_number = (select max(s_mms_membership_main_id) from s_mms_membership_main)
 where table_name = 's_mms_membership_main'
-- p_mms_membership is updated by the pit proc
--go
update dv_sequence_number
   set max_sequence_number = (select max(stage_mms_Membership_id) from stage_mms_Membership)
 where table_name = 'stage_mms_Membership'
--go
select getdate()

/*
-- Dev/QA only code to handle bad data in staging
-- Delete any records where the dv_load_date_time for a record is less than the dv_load_date_time for the prior record in a series
--  There don't appear to be any in Dev, but try this in QA and prod too
select count(*)
  from (select MembershipID,
               rank2,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Membership.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_Membership) this
  left join (select MembershipID,
               rank2,
               case when #stage_mms_Membership.rank2 = 1 then
                         case when #stage_mms_Membership.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Membership.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Membership.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Membership.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_Membership) prior
    on this.MembershipID = prior.MembershipID
   and this.rank2 = prior.rank2 + 1
 where this.dv_load_date_time < isnull(prior.dv_load_date_time, 'jan 1, 1900')
*/

-- Populate the pit table
exec proc_p_mms_membership @current_dv_batch_id = -1
--go
select getdate()

/*
-- make sure there are no duplicate dv_load_date_times for a membership_id
select membership_id, dv_load_date_time from p_mms_membership group by membership_id, dv_load_date_time having count(*) > 1

if object_id('tempdb.dbo.#p') is not null drop table #p
create table dbo.#p with (location=user_db, distribution = hash(membership_id)) as
select row_number() over(partition by membership_id order by dv_load_date_time) rank,
       membership_id,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_greatest_satellite_date_time,
       dv_next_greatest_satellite_date_time
  from p_mms_membership

select this.*, prior.*, next.*
  from #p this
  left join #p prior
    on this.membership_id = prior.membership_id
   and this.rank = prior.rank + 1
  left join #p next
    on this.membership_id = next.membership_id
   and this.rank = next.rank - 1
-- make sure this dv_greatest_satellite_date_time is greatest than the prior dv_greatest_satellite_date_time
 where this.dv_greatest_satellite_date_time <= isnull(prior.dv_greatest_satellite_date_time, 'jan 1, 1900')
-- make sure this dv_greatest_satellite_date_time is less than the prior dv_greatest_satellite_date_time
    or this.dv_greatest_satellite_date_time >= isnull(next.dv_greatest_satellite_date_time, 'dec 31, 9999')
-- make sure if this is the last in a series that the dv_next_greatest_satellite_date_time is null
    or (next.rank is null and this.dv_next_greatest_satellite_date_time is not null)
-- make sure if this is not the last in a series that the dv_next_greatest_satellite_date_time is not 'dec 31, 9999'
    or (next.rank is not null and this.dv_next_greatest_satellite_date_time = 'dec 31, 9999')
-- make sure if this is the last in a series that the dv_next_greatest_satellite_date_time is 'dec 31, 9999'
    or (next.rank is null and this.dv_next_greatest_satellite_date_time != 'dec 31, 9999')
-- make sure if this is not the last in a series that the dv_next_greatest_satellite_date_time matches the next dv_greatest_satellite_date_time
    or (next.rank is not null and this.dv_next_greatest_satellite_date_time != next.dv_greatest_satellite_date_time)
*/


end
