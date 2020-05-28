CREATE PROC [dbo].[proc_member_initial_load] AS
begin

--run the following bcp commands to populate the MMSMember table from LTFDWStg

--bcp dbo.MMSMember out \\mndevinf10\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Member.txt -d LTFDWStg_dev -S mnqasqlvs02\mnqasqlvs02 -T -r "\r" -t "|" -c -U InformaticaUser -P ***********
--bcp dbo.MMSMember in \\mndevinf10\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Member.txt -S aeqadb02.database.windows.net -U InformaticaUser -P Informatic@1 -d lt_udw_dev -r "\r" -t "|" -q -e \\mndevinf01\d$\infa_shared_dev\TgtFiles\AzureFiles\stage_mms_Member.log -c
--bcp dbo.Member out \\mndevinf10\d$\infa_shared_dev\TgtFiles\AzureFiles\Member.txt -d mms_dev_edw -S mnqasqlvs01\mnqasqlvs01 -T -r "\r" -t "|" -c -U InformaticaUser -P *********
--bcp dbo.Member in \\mndevinf10\d$\infa_shared_dev\TgtFiles\AzureFiles\Member.txt -S aeqadb02.database.windows.net -U InformaticaUser -P ******** -d lt_udw_dev -r "\r" -t "|" -q -e \\mndevinf01\d$\infa_shared_dev\TgtFiles\AzureFiles\Member.log -c

/*
-- prepare the dv tables
exec proc_util_reload_dv_object 'mms_member'
--go
select getdate()
*/

--Select the records from MMSMember to be staged and inserted into the dv tables
--We only want 1 record per member for any particular timestamp
--  Do row_number ranking
--    Partition by the following
--      MemberID
--      Calculated update_insert_date
--        If MMSUpdatedDateTime is null then MMSInsertedDateTime
--        Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--        Else add 27 hours to the date portion of MMSUpdatedDateTime
--    Order by MMSMemberKey in descending order (this keeps the most recent record where one or more are in LTFDWStg with the same date)
--  Only keep the records with rank = 1

--alter table MMSMember drop column CRMContactID

if object_id('tempdb.dbo.#stage_mms_Member') is not null drop table #stage_mms_Member
create table dbo.#stage_mms_Member with (location=user_db, distribution = hash(bk_hash)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MemberID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) bk_hash,
       x.*,
       row_number() over(partition by x.MemberID order by x.update_insert_date) rank2,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.EmployerID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValMemberTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValNamePrefixID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValNameSuffixID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.CreditCardAccountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.SiebelRowID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.SalesforceProspectID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.PartyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.SalesforceContactID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), x.CRMContactID, 2),'z#@$k%&P'))
                                  ),2
               ) l_mms_member_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.MiddleName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.DOB,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.Gender,'z#@$k%&P')+
                                         --'P%#&z$@k'+isnull(x.SSN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ActiveFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.HasMessageFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.JoinDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.Comment,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSInsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ChargeToaccountFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.CWMedicaNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.CWEnrollmentDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.CWProgramEnrolledFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MIPUpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSUpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.PhotoDeleteDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MemberToken as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P'))
                                  ),2
               ) s_mms_member_hash
  from (select row_number() over(partition by MMSMember.MemberID,
                                            case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                               end
                                 order by MMSMemberKey desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
               end update_insert_date,
               MMSMember.*,
               Member.CRMContactID
          from MMSMember
          left join Member
            on Member.MemberID = MMSMember.MemberID) x
 where rank1 = 1
--go
select getdate()

--select count(*) from MMSMember
--select count(*) from Member
--select count(*) from #stage_mms_Member
--select top 10 * from #stage_mms_Member

/*
select * from h_mms_member
select * from l_mms_member
select * from s_mms_member
select * from p_mms_member
*/

-- Create the h records.
-- Only use records where rank2 = 1 (first record in a series: min(update_insert_date))
-- dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.h_mms_member(
       h_mms_member_id, 
       bk_hash,
       member_id, 
       dv_load_date_time, 
       dv_batch_id, 
       dv_r_load_source_id, 
       dv_inserted_date_time, 
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.member_id) h_mms_member_id,
       x.*
  from (select bk_hash,
               MemberID member_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_Member
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
insert into l_mms_member (
       l_mms_member_id,
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
select row_number() over(order by x.dv_batch_id, x.member_id) l_mms_member_id,
       x.*
  from (select #stage_mms_Member.bk_hash,
               #stage_mms_Member.MemberID member_id,
               #stage_mms_Member.MembershipID membership_id,
               #stage_mms_Member.employerid employer_id,
               #stage_mms_Member.ValMemberTypeID val_member_type_id,
               #stage_mms_Member.ValNamePrefixID val_name_prefix_id,
               #stage_mms_Member.ValNameSuffixID val_name_suffix_id,
               #stage_mms_Member.creditcardaccountid credit_card_account_id,
               #stage_mms_Member.siebelrowid siebel_row_id,
               #stage_mms_Member.salesforceprospectid salesforce_prospect_id,
               #stage_mms_Member.partyid party_id,
               #stage_mms_Member.lastupdatedemployeeid last_updated_employee_id,
               #stage_mms_Member.salesforcecontactid salesforce_contact_id,
               #stage_mms_Member.CRMContactID crm_contact_id,
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Member.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Member.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Member.l_mms_member_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_Member
          left join dbo.#stage_mms_Member prior
            on #stage_mms_Member.MemberID = prior.MemberID
           and #stage_mms_Member.rank2 = prior.rank2 + 1
         where #stage_mms_Member.l_mms_member_hash != isnull(prior.l_mms_member_hash, ''))x
--go
select getdate()


-- Create the s records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into s_mms_member (
       s_mms_member_id,
       bk_hash,
       member_id,
       first_name,
       middle_name,
       last_name,
       dob,
       gender,
       --ssn,
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
select row_number() over(order by x.dv_batch_id, x.member_id) s_mms_member_id,
       x.*
  from (select #stage_mms_Member.bk_hash,
               #stage_mms_Member.MemberID member_id,
               #stage_mms_Member.FirstName first_name, 
               #stage_mms_Member.MiddleName middle_name,        
               #stage_mms_Member.LastName last_name, 
               #stage_mms_Member.DOB dob,
               #stage_mms_Member.Gender gender,
               --#stage_mms_Member.SSN ssn,
               #stage_mms_Member.ActiveFlag active_flag,
               #stage_mms_Member.HasMessageFlag has_message_flag,
               #stage_mms_Member.JoinDate join_date,
               #stage_mms_Member.Comment comment,
               #stage_mms_Member.MMSInsertedDateTime inserted_date_time,
               #stage_mms_Member.EmailAddress email_address,
               #stage_mms_Member.ChargeToaccountFlag charge_to_account_flag,
               #stage_mms_Member.CWMedicaNumber cw_medica_number,
               #stage_mms_Member.CWEnrollmentDate cw_enrollment_date,
               #stage_mms_Member.CWProgramEnrolledFlag cw_program_enrolled_flag,
               #stage_mms_Member.MIPUpdatedDateTime mip_updated_date_time,
               #stage_mms_Member.MMSUpdatedDateTime updated_date_time,
               #stage_mms_Member.PhotoDeleteDateTime photo_delete_date_time,
               #stage_mms_Member.MemberToken member_token,
               #stage_mms_Member.AssessJrMemberDuesFlag assess_jr_member_dues_flag, 
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Member.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Member.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Member.s_mms_member_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
          from dbo.#stage_mms_Member
          left join dbo.#stage_mms_Member prior
            on #stage_mms_Member.MemberID = prior.MemberID
           and #stage_mms_Member.rank2 = prior.rank2 + 1
         where #stage_mms_Member.s_mms_member_hash != isnull(prior.s_mms_member_hash, ''))x
--go
select getdate()

-- Insert history into the staging table
-- Calculate dv_batch_id (convert the following to YYYYMMSSHHMISS)
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
insert dbo.stage_mms_Member (
       stage_mms_Member_id, 
       MemberID,
       MembershipID, 
       EmployerID, 
       FirstName, 
       MiddleName, 
       LastName, 
       DOB,
       Gender,
       --SSN,
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
       dv_inserted_date_time, 
       dv_insert_user, 
       dv_batch_id)
select row_number() over(order by x.dv_batch_id, x.MemberID) stage_mms_Member_id,
       x.*
  from (select MemberID,
               MembershipID, 
               EmployerID, 
               FirstName, 
               MiddleName, 
               LastName, 
               DOB,
               Gender,
               --SSN,
               ActiveFlag,
               HasMessageFlag,
               JoinDate,
               Comment,
               ValMemberTypeID,
               MMSInsertedDateTime,
               ValNamePrefixID,
               ValNameSuffixID,
               EmailAddress,
               CreditCardAccountID,
               ChargeToaccountFlag,
               CWMedicaNumber,
               CWEnrollmentDate,
               CWProgramEnrolledFlag,
               MIPUpdatedDateTime,
               SiebelRowID,
               MMSUpdatedDateTime,
               PhotoDeleteDateTime,
               SalesforceProspectID,
               MemberToken,
               PartyID,
               LastUpdatedEmployeeID,
               SalesforceContactID, 
               AssessJrMemberDuesFlag, 
               CRMContactID,  
               getdate() dv_inserted_date_time,
               suser_sname() dv_insert_user, 
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Member.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id
          from #stage_mms_Member) x
--go
select getdate()

/*
-- Make sure there are no duplicates as this will potentially cause the pit table to grow out of control
select member_id from h_mms_member group by membership_id having count(*) > 1
select member_id, dv_load_date_time from l_mms_member group by member_id, dv_load_date_time having count(*) > 1
select member_id, dv_load_date_time from s_mms_member group by member_id, dv_load_date_time having count(*) > 1
select member_id, dv_load_date_time from p_mms_membership group by member_id, dv_load_date_time having count(*) > 1
select MemberID, dv_batch_id from stage_mms_Member group by MemberID, dv_batch_id having count(*) > 1
*/

update dv_sequence_number
   set max_sequence_number = (select max(h_mms_member_id) from h_mms_member)
 where table_name = 'h_mms_member'
--go
update dv_sequence_number
   set max_sequence_number = (select max(l_mms_member_id) from l_mms_member)
 where table_name = 'l_mms_member'
--go
update dv_sequence_number
   set max_sequence_number = (select max(s_mms_member_id) from s_mms_member)
 where table_name = 's_mms_member'
-- p_mms_membership is updated by the pit proc
--go
update dv_sequence_number
   set max_sequence_number = (select max(stage_mms_Member_id) from stage_mms_Member)
 where table_name = 'stage_mms_Member'
--go
select getdate()

--alter table stage_mms_Member add  dv_inserted_date_time datetime not null
--alter table stage_mms_Member add  dv_insert_user varchar(50) not null


/*
-- Dev/QA only code to handle bad data in staging
-- Delete any records where the dv_load_date_time for a record is less than the dv_load_date_time for the prior record in a series
--  There don't appear to be any in Dev, but try this in QA and prod too
select count(*)
  from (select MemberID,
               rank2,
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Member.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_Member) this
  left join (select MemberID,
               rank2,
               case when #stage_mms_Member.rank2 = 1 then
                         case when #stage_mms_Member.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Member.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_Member.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_Member.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_Member) prior
    on this.MemberID = prior.MemberID
   and this.rank2 = prior.rank2 + 1
 where this.dv_load_date_time < isnull(prior.dv_load_date_time, 'jan 1, 1900')
*/

-- Populate the pit table
exec proc_p_mms_member @current_dv_batch_id = -1
--go
select getdate()


end
