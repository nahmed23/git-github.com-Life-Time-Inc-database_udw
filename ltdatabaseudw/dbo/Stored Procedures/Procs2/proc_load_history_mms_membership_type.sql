CREATE PROC [dbo].[proc_load_history_mms_membership_type] AS
begin

set nocount on
set xact_abort on

/*Select the records from MMSMembershipType to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_MembershipType_History') is not null drop table #stage_mms_MembershipType_History
create table dbo.#stage_mms_MembershipType_History with (location=user_db, distribution = hash(MembershipTypeID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipTypeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ValMembershipTypeGroupID as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(ValCheckInGroupID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ValMembershipTypeFamilyStatusID as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(ValEnrollmentTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ValUnitTypeID as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(MemberCardDesignID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ValWelcomeKitTypeID as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(ValPricingRuleId as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(ValRestrictedGroupId as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(ValPricingMethodId as varchar(500)),'z#@$k%&P') )),2) as l_mms_membership_type_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(AssessDueFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,MMSInsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ShortTermMembershipFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(MaxUnitType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(ExpressMembershipFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,MMSUpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(DisplayName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(WaiveAdminFeeFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(GTASigOverride,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(AllowPartnerProgramFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(MinUnitType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(MinPrimaryAge as varchar(500)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(WaiveEnrollmentFeeFlag as varchar(42)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(WaiveLateFeeFlag as varchar(42)),'z#@$k%&P')+
										 'P%#&z$@k'+isnull(cast(SuppressMembershipCardFlag as varchar(42)),'z#@$k%&P'))),2) as s_mms_membership_type_hash ,
        row_number() over(partition by MembershipTypeID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by MembershipTypeID,
                                              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                else MMSUpdatedDateTime
                                               end
                                 order by [MMSMembershipTypeKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
               end update_insert_date,
               *
          from stage_mms_MembershipType_History) x
where rank1 = 1
		  
/* Create the h records.*/

/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_membership_type(
       bk_hash,
	   membership_type_id,
       dv_load_date_time, 
       dv_batch_id, 
       dv_r_load_source_id, 
       dv_inserted_date_time, 
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
               MembershipTypeID membership_type_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipType_History 
         where rank2 = 1) x
         
/* Create the l records.*/

/* Calculate dv_load_date_time*/

/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/

insert into dbo.l_mms_membership_type (
	   bk_hash,
       membership_type_id,
       product_id,
       val_membership_type_group_id,
       val_check_in_group_id,
       val_membership_type_family_status_id,
       val_enrollment_type_id,
       val_unit_type_id,
       member_card_design_id,
       val_welcome_kit_type_id,
	   val_pricing_rule_id,
	   val_restricted_group_id,
	   val_pricing_method_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
      x.*
  from (select #stage_mms_MembershipType_History.bk_hash bk_hash,
               #stage_mms_MembershipType_History.MembershipTypeID  membership_type_id,
               #stage_mms_MembershipType_History.ProductID product_id,
       #stage_mms_MembershipType_History.ValMembershipTypeGroupID val_membership_type_group_id,
       #stage_mms_MembershipType_History.ValCheckInGroupID val_check_in_group_id,
       #stage_mms_MembershipType_History.ValMembershipTypeFamilyStatusID val_membership_type_family_status_id,
       #stage_mms_MembershipType_History.ValEnrollmentTypeID val_enrollment_type_id,
       #stage_mms_MembershipType_History.ValUnitTypeID val_unit_type_id,
       #stage_mms_MembershipType_History.MemberCardDesignID member_card_design_id,
       #stage_mms_MembershipType_History.ValWelcomeKitTypeID val_welcome_kit_type_id,
	   #stage_mms_MembershipType_History.ValPricingRuleId val_pricing_rule_id,
	   #stage_mms_MembershipType_History.ValRestrictedGroupId val_restricted_group_id,
	   #stage_mms_MembershipType_History.ValPricingMethodId val_pricing_method_id,
              case when #stage_mms_MembershipType_History.rank2 = 1 then
                         case when #stage_mms_MembershipType_History.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipType_History.MMSInsertedDateTime
                          end
                    else isnull(#stage_mms_MembershipType_History.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)) 
                end dv_load_date_time,
               case when #stage_mms_MembershipType_History.rank2 = 1 then
                         case when #stage_mms_MembershipType_History.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipType_History.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipType_History.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipType_History.l_mms_membership_type_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
           from dbo.#stage_mms_MembershipType_History 
		  left join dbo.#stage_mms_MembershipType_History prior
            on #stage_mms_MembershipType_History.MembershipTypeID = prior.MembershipTypeID
           and #stage_mms_MembershipType_History.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipType_History.l_mms_membership_type_hash != isnull(prior.l_mms_membership_type_hash, ''))x
		  

/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_membership_type (
       /*s_mms_membership_type_id,*/
      bk_hash,
       membership_type_id,
       assess_due_flag,
       inserted_date_time,
       short_term_membership_flag,
       max_unit_type,
       express_membership_flag,
       updated_date_time,
       display_name,
       assess_jr_member_dues_flag,
       waive_admin_fee_flag,
       gta_sig_override,
       allow_partner_program_flag,
       min_unit_type,
       min_primary_age,
	   waive_enrollment_fee_flag,
	   waive_late_fee_flag,
	   suppress_membership_card_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_MembershipType_History.bk_hash,
       #stage_mms_MembershipType_History.MembershipTypeID,
       #stage_mms_MembershipType_History.AssessDueFlag,
       #stage_mms_MembershipType_History.MMSInsertedDateTime,
       #stage_mms_MembershipType_History.ShortTermMembershipFlag,
       #stage_mms_MembershipType_History.MaxUnitType,
       #stage_mms_MembershipType_History.ExpressMembershipFlag,
       #stage_mms_MembershipType_History.MMSUpdatedDateTime,
       #stage_mms_MembershipType_History.DisplayName,
       #stage_mms_MembershipType_History.AssessJrMemberDuesFlag,
       #stage_mms_MembershipType_History.WaiveAdminFeeFlag,
       #stage_mms_MembershipType_History.GTASigOverride,
       #stage_mms_MembershipType_History.AllowPartnerProgramFlag,
       #stage_mms_MembershipType_History.MinUnitType,
       #stage_mms_MembershipType_History.MinPrimaryAge,
	   #stage_mms_MembershipType_History.WaiveEnrollmentFeeFlag,
	   #stage_mms_MembershipType_History.WaiveLateFeeFlag,
	   #stage_mms_MembershipType_History.SuppressMembershipCardFlag,
               case when #stage_mms_MembershipType_History.rank2 = 1 then
                         case when #stage_mms_MembershipType_History.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipType_History.MMSInsertedDateTime
                          end
                    else isnull(#stage_mms_MembershipType_History.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107))
                end dv_load_date_time,
               case when #stage_mms_MembershipType_History.rank2 = 1 then
                         case when #stage_mms_MembershipType_History.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipType_History.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipType_History.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipType_History.s_mms_membership_type_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_MembershipType_History
          left join dbo.#stage_mms_MembershipType_History prior
            on #stage_mms_MembershipType_History.MembershipTypeID = prior.MembershipTypeID
           and #stage_mms_MembershipType_History.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipType_History.s_mms_membership_type_hash != isnull(prior.s_mms_membership_type_hash, ''))x

end
