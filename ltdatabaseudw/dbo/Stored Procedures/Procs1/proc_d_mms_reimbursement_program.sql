CREATE PROC [dbo].[proc_d_mms_reimbursement_program] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_reimbursement_program)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_reimbursement_program_insert') is not null drop table #p_mms_reimbursement_program_insert
create table dbo.#p_mms_reimbursement_program_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_reimbursement_program.p_mms_reimbursement_program_id,
       p_mms_reimbursement_program.bk_hash
  from dbo.p_mms_reimbursement_program
 where p_mms_reimbursement_program.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_reimbursement_program.dv_batch_id > @max_dv_batch_id
        or p_mms_reimbursement_program.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_reimbursement_program.bk_hash,
       p_mms_reimbursement_program.bk_hash dim_mms_reimbursement_program_key,
       p_mms_reimbursement_program.reimbursement_program_id reimbursement_program_id,
       case when l_mms_reimbursement_program.company_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_reimbursement_program.company_id as varchar(500)),'z#@$k%&P'))),2)
   end dim_mms_company_key ,
       case when s_mms_reimbursement_program.active_flag='1' then 
	'Y' else 'N'
	 end program_active_flag,
       s_mms_reimbursement_program.reimbursement_program_name program_name,
       case when l_mms_reimbursement_program.val_reimbursement_program_processing_type_id is null then '-998'
    else 'r_mms_val_reimbursement_program_processing_type_' + convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_reimbursement_program.val_reimbursement_program_processing_type_id as varchar(500)),'z#@$k%&P'))),2)
   end program_processing_type_dim_description_key,
       case when l_mms_reimbursement_program.val_reimbursement_program_type_id is null then '-998' 
    else 'r_mms_val_reimbursement_program_type_' + convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_reimbursement_program.val_reimbursement_program_type_id as varchar(500)),'z#@$k%&P'))),2)
  end program_type_dim_description_key,
       l_mms_reimbursement_program.val_reimbursement_program_processing_type_id val_reimbursement_program_processing_type_id,
       l_mms_reimbursement_program.val_reimbursement_program_type_id val_reimbursement_program_type_id,
       h_mms_reimbursement_program.dv_deleted,
       p_mms_reimbursement_program.p_mms_reimbursement_program_id,
       p_mms_reimbursement_program.dv_batch_id,
       p_mms_reimbursement_program.dv_load_date_time,
       p_mms_reimbursement_program.dv_load_end_date_time
  from dbo.h_mms_reimbursement_program
  join dbo.p_mms_reimbursement_program
    on h_mms_reimbursement_program.bk_hash = p_mms_reimbursement_program.bk_hash  join #p_mms_reimbursement_program_insert
    on p_mms_reimbursement_program.bk_hash = #p_mms_reimbursement_program_insert.bk_hash
   and p_mms_reimbursement_program.p_mms_reimbursement_program_id = #p_mms_reimbursement_program_insert.p_mms_reimbursement_program_id
  join dbo.l_mms_reimbursement_program
    on p_mms_reimbursement_program.bk_hash = l_mms_reimbursement_program.bk_hash
   and p_mms_reimbursement_program.l_mms_reimbursement_program_id = l_mms_reimbursement_program.l_mms_reimbursement_program_id
  join dbo.s_mms_reimbursement_program
    on p_mms_reimbursement_program.bk_hash = s_mms_reimbursement_program.bk_hash
   and p_mms_reimbursement_program.s_mms_reimbursement_program_id = s_mms_reimbursement_program.s_mms_reimbursement_program_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_reimbursement_program
   where d_mms_reimbursement_program.bk_hash in (select bk_hash from #p_mms_reimbursement_program_insert)

  insert dbo.d_mms_reimbursement_program(
             bk_hash,
             dim_mms_reimbursement_program_key,
             reimbursement_program_id,
             dim_mms_company_key ,
             program_active_flag,
             program_name,
             program_processing_type_dim_description_key,
             program_type_dim_description_key,
             val_reimbursement_program_processing_type_id,
             val_reimbursement_program_type_id,
             deleted_flag,
             p_mms_reimbursement_program_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_reimbursement_program_key,
         reimbursement_program_id,
         dim_mms_company_key ,
         program_active_flag,
         program_name,
         program_processing_type_dim_description_key,
         program_type_dim_description_key,
         val_reimbursement_program_processing_type_id,
         val_reimbursement_program_type_id,
         dv_deleted,
         p_mms_reimbursement_program_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_reimbursement_program)
--Done!
end
