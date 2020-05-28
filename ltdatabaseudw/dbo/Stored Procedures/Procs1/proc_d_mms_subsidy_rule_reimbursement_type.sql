CREATE PROC [dbo].[proc_d_mms_subsidy_rule_reimbursement_type] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_subsidy_rule_reimbursement_type)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_subsidy_rule_reimbursement_type_insert') is not null drop table #p_mms_subsidy_rule_reimbursement_type_insert
create table dbo.#p_mms_subsidy_rule_reimbursement_type_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_subsidy_rule_reimbursement_type.p_mms_subsidy_rule_reimbursement_type_id,
       p_mms_subsidy_rule_reimbursement_type.bk_hash
  from dbo.p_mms_subsidy_rule_reimbursement_type
 where p_mms_subsidy_rule_reimbursement_type.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_subsidy_rule_reimbursement_type.dv_batch_id > @max_dv_batch_id
        or p_mms_subsidy_rule_reimbursement_type.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_subsidy_rule_reimbursement_type.bk_hash,
       p_mms_subsidy_rule_reimbursement_type.bk_hash dim_mms_subsidy_rule_reimbursement_type_key,
       p_mms_subsidy_rule_reimbursement_type.subsidy_rule_reimbursement_type_id subsidy_rule_reimbursement_type_id,
       case when  
	l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=3 then 
 isnull(s_mms_subsidy_rule_reimbursement_type.reimbursement_amount,0) else 0 end subsidy_reimbursement_couple_membership_dues_amount,
       case when  
	l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=3 then 
	case when s_mms_subsidy_rule_reimbursement_type.reimbursement_amount is not null or 
   s_mms_subsidy_rule_reimbursement_type.reimbursement_percentage is not null then 
	'Y' else 'N' end end subsidy_reimbursement_couple_membership_dues_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=3 then 
 case when s_mms_Subsidy_Rule_Reimbursement_Type.Include_Tax_Flag='1' then 'Y' else 'N' end end subsidy_reimbursement_couple_membership_dues_include_tax_flag,
       case when  
	l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=3 then 
 isnull(s_mms_subsidy_rule_reimbursement_type.reimbursement_percentage,0) else 0 end subsidy_reimbursement_couple_membership_dues_percentage,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=6 then 
 isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount,0) else 0 end subsidy_reimbursement_experience_life_magazine_amount,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=6 then 
  case when s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount is not null or 
    s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage is not null then 
	'Y' else 'N' end end subsidy_reimbursement_experience_life_magazine_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=6 then 
	case when s_mms_Subsidy_Rule_Reimbursement_Type.Include_Tax_Flag='1' then 'Y' else 'N' end end subsidy_reimbursement_experience_life_magazine_include_tax_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=6 then 
isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage,0) else 0 end subsidy_reimbursement_experience_life_magazine_percentage,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=4 then 
  isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount,0) else 0 end subsidy_reimbursement_family_membership_dues_amount,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=4 then 
  case when s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount is not null or 
    s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage is not null then 
	'Y' else 'N' end end subsidy_reimbursement_family_membership_dues_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=4 then
  case when s_mms_Subsidy_Rule_Reimbursement_Type.Include_Tax_Flag='1' then 'Y' else 'N' end end subsidy_reimbursement_family_membership_dues_include_tax_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=4 then 
  isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage,0) else 0 end subsidy_reimbursement_family_membership_dues_percentage,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=5 then 
  isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount,0) else 0 end subsidy_reimbursement_junior_member_dues_amount,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=5 then 
  case when s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Amount is not null or 
    s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage is not null then 
	'Y' else 'N' end end subsidy_reimbursement_junior_member_dues_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=5 then 
  case when s_mms_Subsidy_Rule_Reimbursement_Type.Include_Tax_Flag='1' then 'Y' else 'N' end end subsidy_reimbursement_junior_member_dues_include_tax_flag,
       case when  
	l_mms_Subsidy_Rule_Reimbursement_Type.Val_Reimbursement_Type_ID=5 then 
  isnull(s_mms_Subsidy_Rule_Reimbursement_Type.Reimbursement_Percentage,0) else 0 end subsidy_reimbursement_junior_member_dues_percentage,
       case when l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=2 then 
	isnull(s_mms_Subsidy_Rule_Reimbursement_type.reimbursement_amount,0) else 0 end subsidy_reimbursement_single_membership_dues_amount,
       case when  l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=2 
	then 
     case when s_mms_subsidy_rule_reimbursement_type.reimbursement_amount is not null or 
	     s_mms_subsidy_rule_reimbursement_type.reimbursement_percentage is not null then 
	'Y' else 'N' end end subsidy_reimbursement_single_membership_dues_flag,
       case when l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=2 then 
	case when s_mms_subsidy_rule_reimbursement_type.include_tax_flag='1' then 'Y' else 'N' end end subsidy_reimbursement_single_membership_dues_include_tax_flag,
       case when l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id=2 then 
	isnull(s_mms_subsidy_rule_reimbursement_type.reimbursement_percentage,0) else 0 end subsidy_reimbursement_single_membership_dues_percentage,
       l_mms_subsidy_rule_reimbursement_type.subsidy_rule_id subsidy_rule_id,
       l_mms_subsidy_rule_reimbursement_type.val_reimbursement_type_id val_reimbursement_type_id,
       h_mms_subsidy_rule_reimbursement_type.dv_deleted,
       p_mms_subsidy_rule_reimbursement_type.p_mms_subsidy_rule_reimbursement_type_id,
       p_mms_subsidy_rule_reimbursement_type.dv_batch_id,
       p_mms_subsidy_rule_reimbursement_type.dv_load_date_time,
       p_mms_subsidy_rule_reimbursement_type.dv_load_end_date_time
  from dbo.h_mms_subsidy_rule_reimbursement_type
  join dbo.p_mms_subsidy_rule_reimbursement_type
    on h_mms_subsidy_rule_reimbursement_type.bk_hash = p_mms_subsidy_rule_reimbursement_type.bk_hash  join #p_mms_subsidy_rule_reimbursement_type_insert
    on p_mms_subsidy_rule_reimbursement_type.bk_hash = #p_mms_subsidy_rule_reimbursement_type_insert.bk_hash
   and p_mms_subsidy_rule_reimbursement_type.p_mms_subsidy_rule_reimbursement_type_id = #p_mms_subsidy_rule_reimbursement_type_insert.p_mms_subsidy_rule_reimbursement_type_id
  join dbo.l_mms_subsidy_rule_reimbursement_type
    on p_mms_subsidy_rule_reimbursement_type.bk_hash = l_mms_subsidy_rule_reimbursement_type.bk_hash
   and p_mms_subsidy_rule_reimbursement_type.l_mms_subsidy_rule_reimbursement_type_id = l_mms_subsidy_rule_reimbursement_type.l_mms_subsidy_rule_reimbursement_type_id
  join dbo.s_mms_subsidy_rule_reimbursement_type
    on p_mms_subsidy_rule_reimbursement_type.bk_hash = s_mms_subsidy_rule_reimbursement_type.bk_hash
   and p_mms_subsidy_rule_reimbursement_type.s_mms_subsidy_rule_reimbursement_type_id = s_mms_subsidy_rule_reimbursement_type.s_mms_subsidy_rule_reimbursement_type_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_subsidy_rule_reimbursement_type
   where d_mms_subsidy_rule_reimbursement_type.bk_hash in (select bk_hash from #p_mms_subsidy_rule_reimbursement_type_insert)

  insert dbo.d_mms_subsidy_rule_reimbursement_type(
             bk_hash,
             dim_mms_subsidy_rule_reimbursement_type_key,
             subsidy_rule_reimbursement_type_id,
             subsidy_reimbursement_couple_membership_dues_amount,
             subsidy_reimbursement_couple_membership_dues_flag,
             subsidy_reimbursement_couple_membership_dues_include_tax_flag,
             subsidy_reimbursement_couple_membership_dues_percentage,
             subsidy_reimbursement_experience_life_magazine_amount,
             subsidy_reimbursement_experience_life_magazine_flag,
             subsidy_reimbursement_experience_life_magazine_include_tax_flag,
             subsidy_reimbursement_experience_life_magazine_percentage,
             subsidy_reimbursement_family_membership_dues_amount,
             subsidy_reimbursement_family_membership_dues_flag,
             subsidy_reimbursement_family_membership_dues_include_tax_flag,
             subsidy_reimbursement_family_membership_dues_percentage,
             subsidy_reimbursement_junior_member_dues_amount,
             subsidy_reimbursement_junior_member_dues_flag,
             subsidy_reimbursement_junior_member_dues_include_tax_flag,
             subsidy_reimbursement_junior_member_dues_percentage,
             subsidy_reimbursement_single_membership_dues_amount,
             subsidy_reimbursement_single_membership_dues_flag,
             subsidy_reimbursement_single_membership_dues_include_tax_flag,
             subsidy_reimbursement_single_membership_dues_percentage,
             subsidy_rule_id,
             val_reimbursement_type_id,
             deleted_flag,
             p_mms_subsidy_rule_reimbursement_type_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_subsidy_rule_reimbursement_type_key,
         subsidy_rule_reimbursement_type_id,
         subsidy_reimbursement_couple_membership_dues_amount,
         subsidy_reimbursement_couple_membership_dues_flag,
         subsidy_reimbursement_couple_membership_dues_include_tax_flag,
         subsidy_reimbursement_couple_membership_dues_percentage,
         subsidy_reimbursement_experience_life_magazine_amount,
         subsidy_reimbursement_experience_life_magazine_flag,
         subsidy_reimbursement_experience_life_magazine_include_tax_flag,
         subsidy_reimbursement_experience_life_magazine_percentage,
         subsidy_reimbursement_family_membership_dues_amount,
         subsidy_reimbursement_family_membership_dues_flag,
         subsidy_reimbursement_family_membership_dues_include_tax_flag,
         subsidy_reimbursement_family_membership_dues_percentage,
         subsidy_reimbursement_junior_member_dues_amount,
         subsidy_reimbursement_junior_member_dues_flag,
         subsidy_reimbursement_junior_member_dues_include_tax_flag,
         subsidy_reimbursement_junior_member_dues_percentage,
         subsidy_reimbursement_single_membership_dues_amount,
         subsidy_reimbursement_single_membership_dues_flag,
         subsidy_reimbursement_single_membership_dues_include_tax_flag,
         subsidy_reimbursement_single_membership_dues_percentage,
         subsidy_rule_id,
         val_reimbursement_type_id,
         dv_deleted,
         p_mms_subsidy_rule_reimbursement_type_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_subsidy_rule_reimbursement_type)
--Done!
end
