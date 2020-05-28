CREATE PROC [dbo].[proc_d_mms_subsidy_rule] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_subsidy_rule)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_subsidy_rule_insert') is not null drop table #p_mms_subsidy_rule_insert
create table dbo.#p_mms_subsidy_rule_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_subsidy_rule.p_mms_subsidy_rule_id,
       p_mms_subsidy_rule.bk_hash
  from dbo.p_mms_subsidy_rule
 where p_mms_subsidy_rule.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_subsidy_rule.dv_batch_id > @max_dv_batch_id
        or p_mms_subsidy_rule.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_subsidy_rule.bk_hash,
       p_mms_subsidy_rule.bk_hash dim_mms_subsidy_rule_key,
       p_mms_subsidy_rule.subsidy_rule_id subsidy_rule_id,
       l_mms_subsidy_rule.subsidy_company_reimbursement_program_id subsidy_company_reimbursement_program_id,
       case when l_mms_subsidy_rule.val_reimbursement_usage_type_id is null or l_mms_subsidy_rule.subsidy_company_reimbursement_program_id is null then '-998'
    else 'r_mms_val_reimbursement_usage_type_' + convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_subsidy_rule.val_reimbursement_usage_type_id as varchar(500)),'z#@$k%&P'))),2)
   end subsidy_reimbursement_usage_type_dim_description_key,
       h_mms_subsidy_rule.dv_deleted,
       p_mms_subsidy_rule.p_mms_subsidy_rule_id,
       p_mms_subsidy_rule.dv_batch_id,
       p_mms_subsidy_rule.dv_load_date_time,
       p_mms_subsidy_rule.dv_load_end_date_time
  from dbo.h_mms_subsidy_rule
  join dbo.p_mms_subsidy_rule
    on h_mms_subsidy_rule.bk_hash = p_mms_subsidy_rule.bk_hash  join #p_mms_subsidy_rule_insert
    on p_mms_subsidy_rule.bk_hash = #p_mms_subsidy_rule_insert.bk_hash
   and p_mms_subsidy_rule.p_mms_subsidy_rule_id = #p_mms_subsidy_rule_insert.p_mms_subsidy_rule_id
  join dbo.l_mms_subsidy_rule
    on p_mms_subsidy_rule.bk_hash = l_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.l_mms_subsidy_rule_id = l_mms_subsidy_rule.l_mms_subsidy_rule_id
  join dbo.s_mms_subsidy_rule
    on p_mms_subsidy_rule.bk_hash = s_mms_subsidy_rule.bk_hash
   and p_mms_subsidy_rule.s_mms_subsidy_rule_id = s_mms_subsidy_rule.s_mms_subsidy_rule_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_subsidy_rule
   where d_mms_subsidy_rule.bk_hash in (select bk_hash from #p_mms_subsidy_rule_insert)

  insert dbo.d_mms_subsidy_rule(
             bk_hash,
             dim_mms_subsidy_rule_key,
             subsidy_rule_id,
             subsidy_company_reimbursement_program_id,
             subsidy_reimbursement_usage_type_dim_description_key,
             deleted_flag,
             p_mms_subsidy_rule_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_subsidy_rule_key,
         subsidy_rule_id,
         subsidy_company_reimbursement_program_id,
         subsidy_reimbursement_usage_type_dim_description_key,
         dv_deleted,
         p_mms_subsidy_rule_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_subsidy_rule)
--Done!
end
