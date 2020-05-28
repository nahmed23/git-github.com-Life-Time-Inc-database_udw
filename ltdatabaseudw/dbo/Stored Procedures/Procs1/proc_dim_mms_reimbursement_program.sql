CREATE PROC [dbo].[proc_dim_mms_reimbursement_program] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_mms_reimbursement_program)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#dim_mms_reimbursement_program') is not null drop table #dim_mms_reimbursement_program
create table dbo.#dim_mms_reimbursement_program with(distribution=hash(dim_mms_reimbursement_program_key), location=user_db, heap) as

select d_mms_reimbursement_program.dim_mms_reimbursement_program_key dim_mms_reimbursement_program_key,  --generate the key in d_mms_pricing_discount
       d_mms_reimbursement_program.reimbursement_program_id reimbursement_program_id,
	   d_mms_reimbursement_program.p_mms_reimbursement_program_id p_mms_reimbursement_program_id,
	   d_mms_reimbursement_program.program_name program_name,
	   d_mms_reimbursement_program.program_active_flag program_active_flag,
	   d_mms_reimbursement_program.dim_mms_company_key  dim_mms_company_key ,
	   isnull(d_mms_subsidy_company_reimbursement_program.subsidy_program_flag,'N') subsidy_program_flag,
	   isnull(d_mms_subsidy_company_reimbursement_program.subsidy_program_description,'') subsidy_program_description,
	   isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_single_membership_dues_flag,'N') subsidy_reimbursement_single_membership_dues_flag,
	   isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_single_membership_dues_amount,0) subsidy_reimbursement_single_membership_dues_amount,
	   isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_single_membership_dues_percentage,0) subsidy_reimbursement_single_membership_dues_percentage,
	   isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_single_membership_dues_include_tax_flag,'N') subsidy_reimbursement_single_membership_dues_include_tax_flag,
	   isnull(couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_couple_membership_dues_flag,'N') subsidy_reimbursement_couple_membership_dues_flag,
	   isnull(couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_couple_membership_dues_amount,0) subsidy_reimbursement_couple_membership_dues_amount,
	   isnull(couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_couple_membership_dues_percentage,0) subsidy_reimbursement_couple_membership_dues_percentage,
	   isnull(couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_couple_membership_dues_include_tax_flag,'N') subsidy_reimbursement_couple_membership_dues_include_tax_flag,
	   isnull(family_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_family_membership_dues_flag,'N') subsidy_reimbursement_family_membership_dues_flag,
	   isnull(family_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_family_membership_dues_amount,0) subsidy_reimbursement_family_membership_dues_amount,
	   isnull(family_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_family_membership_dues_percentage,0) subsidy_reimbursement_family_membership_dues_percentage,
	   isnull(family_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_family_membership_dues_include_tax_flag,'N') subsidy_reimbursement_family_membership_dues_include_tax_flag,
	   isnull(junior_member_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_junior_member_dues_flag,'N') subsidy_reimbursement_junior_member_dues_flag,
	   isnull(junior_member_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_junior_member_dues_amount,0) subsidy_reimbursement_junior_member_dues_amount,
	   isnull(junior_member_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_junior_member_dues_percentage,0) subsidy_reimbursement_junior_member_dues_percentage,
	   isnull(junior_member_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_junior_member_dues_include_tax_flag,'N') subsidy_reimbursement_junior_member_dues_include_tax_flag,
	   isnull(experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_experience_life_magazine_flag,'N') subsidy_reimbursement_experience_life_magazine_flag,
	   isnull(experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_experience_life_magazine_amount,0) subsidy_reimbursement_experience_life_magazine_amount,
	   isnull(experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_experience_life_magazine_percentage,0) subsidy_reimbursement_experience_life_magazine_percentage,
	   isnull(experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.subsidy_reimbursement_experience_life_magazine_include_tax_flag,'N')  subsidy_reimbursement_experience_life_magazine_include_tax_flag,
	   isnull(d_mms_subsidy_rule.subsidy_reimbursement_usage_type_dim_description_key,'-998') subsidy_reimbursement_usage_type_dim_description_key,
	   d_mms_reimbursement_program.program_type_dim_description_key program_type_dim_description_key,
	   d_mms_reimbursement_program.program_processing_type_dim_description_key program_processing_type_dim_description_key,
       case when d_mms_reimbursement_program.dv_load_date_time >= isnull(d_mms_subsidy_rule.dv_load_date_time,'jan 1, 1753')
	        and  d_mms_subsidy_rule.dv_load_date_time >= isnull(d_mms_subsidy_company_reimbursement_program.dv_load_date_time,'jan 1, 1753')
			and  d_mms_subsidy_company_reimbursement_program.dv_load_date_time >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_load_date_time,'jan 1, 1753')
			then d_mms_reimbursement_program.dv_load_date_time
            when d_mms_subsidy_rule.dv_load_date_time >= isnull(d_mms_subsidy_company_reimbursement_program.dv_load_date_time,'jan 1, 1753')
			and  d_mms_subsidy_company_reimbursement_program.dv_load_date_time >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_load_date_time,'jan 1, 1753')
			then d_mms_subsidy_rule.dv_load_date_time
	        when d_mms_subsidy_company_reimbursement_program.dv_load_date_time >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_load_date_time,'jan 1, 1753')
			then d_mms_subsidy_company_reimbursement_program.dv_load_date_time
	        else single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
      case when d_mms_reimbursement_program.dv_batch_id >= isnull(d_mms_subsidy_rule.dv_batch_id,-1)
      and d_mms_reimbursement_program.dv_batch_id >= isnull(d_mms_subsidy_company_reimbursement_program.dv_batch_id,-1)
      and d_mms_reimbursement_program.dv_batch_id >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id,-1)
     then d_mms_reimbursement_program.dv_batch_id
     when d_mms_subsidy_rule.dv_batch_id >= isnull(d_mms_subsidy_company_reimbursement_program.dv_batch_id,-1)
      and d_mms_subsidy_rule.dv_batch_id >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id,-1)
     then d_mms_subsidy_rule.dv_batch_id
     when d_mms_subsidy_company_reimbursement_program.dv_batch_id >= isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id,-1)
     then d_mms_subsidy_company_reimbursement_program.dv_batch_id
     else isnull(single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id,-1) 
	 end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from d_mms_reimbursement_program d_mms_reimbursement_program
  left join d_mms_subsidy_company_reimbursement_program d_mms_subsidy_company_reimbursement_program
    on d_mms_reimbursement_program.reimbursement_program_id = d_mms_subsidy_company_reimbursement_program.reimbursement_program_id
  left join d_mms_subsidy_rule d_mms_subsidy_rule
    on d_mms_subsidy_company_reimbursement_program.subsidy_company_reimbursement_program_id = d_mms_subsidy_rule.subsidy_company_reimbursement_program_id
  left join d_mms_subsidy_rule_reimbursement_type single_membership_dues_d_mms_subsidy_rule_reimbursement_type
    on d_mms_subsidy_rule.subsidy_rule_id = single_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_rule_id
   and 2 = single_membership_dues_d_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id
  left join d_mms_subsidy_rule_reimbursement_type couple_membership_dues_d_mms_subsidy_rule_reimbursement_type
    on d_mms_subsidy_rule.subsidy_rule_id = couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_rule_id
   and 3 = couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id
  left join d_mms_subsidy_rule_reimbursement_type family_membership_dues_d_mms_subsidy_rule_reimbursement_type
    on d_mms_subsidy_rule.subsidy_rule_id = family_membership_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_rule_id
   and 4 = family_membership_dues_d_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id
  left join d_mms_subsidy_rule_reimbursement_type junior_member_dues_d_mms_subsidy_rule_reimbursement_type
    on d_mms_subsidy_rule.subsidy_rule_id = junior_member_dues_d_mms_subsidy_rule_reimbursement_type.subsidy_rule_id
   and 5 = junior_member_dues_d_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id
  left join d_mms_subsidy_rule_reimbursement_type experience_life_magazine_d_mms_subsidy_rule_reimbursement_type
    on d_mms_subsidy_rule.subsidy_rule_id = experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.subsidy_rule_id
   and 6 = experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id
 where d_mms_reimbursement_program.dv_batch_id >= @load_dv_batch_id
    or d_mms_subsidy_company_reimbursement_program.dv_batch_id >= @load_dv_batch_id
    or d_mms_subsidy_rule.dv_batch_id >= @load_dv_batch_id
    or single_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id >= @load_dv_batch_id
    or couple_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id >= @load_dv_batch_id
    or family_membership_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id >= @load_dv_batch_id
    or junior_member_dues_d_mms_subsidy_rule_reimbursement_type.dv_batch_id >= @load_dv_batch_id
    or experience_life_magazine_d_mms_subsidy_rule_reimbursement_type.dv_batch_id >= @load_dv_batch_id

	
	
--delete and re-insert
-- do as a single transaction
--   delete records from the fact table that exist
--   insert records from records from current and missing batches
    begin tran

   delete dbo.dim_mms_reimbursement_program
   where dim_mms_reimbursement_program_key in (select dim_mms_reimbursement_program_key from dbo.#dim_mms_reimbursement_program) 
	  
										insert  dbo.dim_mms_reimbursement_program(  
												dim_mms_reimbursement_program_key,
												reimbursement_program_id,
												program_name,
												program_active_flag,
												dim_mms_company_key ,
												subsidy_program_flag,
												subsidy_program_description,
												subsidy_reimbursement_single_membership_dues_flag,
												subsidy_reimbursement_single_membership_dues_amount,
												subsidy_reimbursement_single_membership_dues_percentage,
												subsidy_reimbursement_single_membership_dues_include_tax_flag,
												subsidy_reimbursement_couple_membership_dues_flag,
												subsidy_reimbursement_couple_membership_dues_amount,
												subsidy_reimbursement_couple_membership_dues_percentage,
												subsidy_reimbursement_couple_membership_dues_include_tax_flag,
												subsidy_reimbursement_family_membership_dues_flag,
												subsidy_reimbursement_family_membership_dues_amount,
												subsidy_reimbursement_family_membership_dues_percentage,
												subsidy_reimbursement_family_membership_dues_include_tax_flag,
												subsidy_reimbursement_junior_member_dues_flag,
												subsidy_reimbursement_junior_member_dues_amount,
												subsidy_reimbursement_junior_member_dues_percentage,
												subsidy_reimbursement_junior_member_dues_include_tax_flag,
												subsidy_reimbursement_experience_life_magazine_flag,
												subsidy_reimbursement_experience_life_magazine_amount,
												subsidy_reimbursement_experience_life_magazine_percentage,
												subsidy_reimbursement_experience_life_magazine_include_tax_flag,
												subsidy_reimbursement_usage_type_dim_description_key,
												program_type_dim_description_key,
												program_processing_type_dim_description_key,
												dv_load_date_time,
												dv_load_end_date_time,
												dv_batch_id,
												dv_inserted_date_time,
												dv_insert_user
												)
											    select 	 
												dim_mms_reimbursement_program_key,
												reimbursement_program_id,
												program_name,
												program_active_flag,
												dim_mms_company_key ,
												subsidy_program_flag,
												subsidy_program_description,
												subsidy_reimbursement_single_membership_dues_flag,
												subsidy_reimbursement_single_membership_dues_amount,
												subsidy_reimbursement_single_membership_dues_percentage,
												subsidy_reimbursement_single_membership_dues_include_tax_flag,
												subsidy_reimbursement_couple_membership_dues_flag,
												subsidy_reimbursement_couple_membership_dues_amount,
												subsidy_reimbursement_couple_membership_dues_percentage,
												subsidy_reimbursement_couple_membership_dues_include_tax_flag,
												subsidy_reimbursement_family_membership_dues_flag,
												subsidy_reimbursement_family_membership_dues_amount,
												subsidy_reimbursement_family_membership_dues_percentage,
												subsidy_reimbursement_family_membership_dues_include_tax_flag,
												subsidy_reimbursement_junior_member_dues_flag,
												subsidy_reimbursement_junior_member_dues_amount,
												subsidy_reimbursement_junior_member_dues_percentage,
												subsidy_reimbursement_junior_member_dues_include_tax_flag,
												subsidy_reimbursement_experience_life_magazine_flag,
												subsidy_reimbursement_experience_life_magazine_amount,
												subsidy_reimbursement_experience_life_magazine_percentage,
												subsidy_reimbursement_experience_life_magazine_include_tax_flag,
												subsidy_reimbursement_usage_type_dim_description_key,
												program_type_dim_description_key,
												program_processing_type_dim_description_key,
												dv_load_date_time,
												dv_load_end_date_time,
												dv_batch_id,
												dv_inserted_date_time,
												dv_insert_user											
												from
												#dim_mms_reimbursement_program 
												

			commit tran
end
