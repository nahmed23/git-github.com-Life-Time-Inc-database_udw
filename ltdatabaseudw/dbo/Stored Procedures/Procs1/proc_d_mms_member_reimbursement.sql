CREATE PROC [dbo].[proc_d_mms_member_reimbursement] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_member_reimbursement)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_member_reimbursement_insert') is not null drop table #p_mms_member_reimbursement_insert
create table dbo.#p_mms_member_reimbursement_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member_reimbursement.p_mms_member_reimbursement_id,
       p_mms_member_reimbursement.bk_hash
  from dbo.p_mms_member_reimbursement
 where p_mms_member_reimbursement.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_member_reimbursement.dv_batch_id > @max_dv_batch_id
        or p_mms_member_reimbursement.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member_reimbursement.bk_hash,
       p_mms_member_reimbursement.bk_hash fact_mms_member_reimbursement_key,
       p_mms_member_reimbursement.member_reimbursement_id member_reimbursement_id,
       case when p_mms_member_reimbursement.bk_hash in ('-997','-998','-999') then p_mms_member_reimbursement.bk_hash
            when l_mms_member_reimbursement.member_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member_reimbursement.member_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_member_key,
       case when p_mms_member_reimbursement.bk_hash in ('-997','-998','-999') then p_mms_member_reimbursement.bk_hash
            when l_mms_member_reimbursement.reimbursement_program_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member_reimbursement.reimbursement_program_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_reimbursement_program_key,
       s_mms_member_reimbursement.enrollment_date enrollment_date,
       convert(varchar(8),s_mms_member_reimbursement.enrollment_date,112) enrollment_dim_date_key,
       l_mms_member_reimbursement.member_id member_id,
       l_mms_member_reimbursement.reimbursement_program_id reimbursement_program_id,
       case when p_mms_member_reimbursement.bk_hash in ('-997','-998','-999') then p_mms_member_reimbursement.bk_hash
            when l_mms_member_reimbursement.reimbursement_program_identifier_format_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member_reimbursement.reimbursement_program_identifier_format_id as varchar(500)),'z#@$k%&P'))),2)
        end reimbursement_program_identifier_format_bk_hash,
       l_mms_member_reimbursement.reimbursement_program_identifier_format_id reimbursement_program_identifier_format_id,
       s_mms_member_reimbursement.termination_date termination_date,
       convert(varchar(8),s_mms_member_reimbursement.termination_date,112) termination_dim_date_key,
       h_mms_member_reimbursement.dv_deleted,
       p_mms_member_reimbursement.p_mms_member_reimbursement_id,
       p_mms_member_reimbursement.dv_batch_id,
       p_mms_member_reimbursement.dv_load_date_time,
       p_mms_member_reimbursement.dv_load_end_date_time
  from dbo.h_mms_member_reimbursement
  join dbo.p_mms_member_reimbursement
    on h_mms_member_reimbursement.bk_hash = p_mms_member_reimbursement.bk_hash
  join #p_mms_member_reimbursement_insert
    on p_mms_member_reimbursement.bk_hash = #p_mms_member_reimbursement_insert.bk_hash
   and p_mms_member_reimbursement.p_mms_member_reimbursement_id = #p_mms_member_reimbursement_insert.p_mms_member_reimbursement_id
  join dbo.l_mms_member_reimbursement
    on p_mms_member_reimbursement.bk_hash = l_mms_member_reimbursement.bk_hash
   and p_mms_member_reimbursement.l_mms_member_reimbursement_id = l_mms_member_reimbursement.l_mms_member_reimbursement_id
  join dbo.s_mms_member_reimbursement
    on p_mms_member_reimbursement.bk_hash = s_mms_member_reimbursement.bk_hash
   and p_mms_member_reimbursement.s_mms_member_reimbursement_id = s_mms_member_reimbursement.s_mms_member_reimbursement_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_member_reimbursement
   where d_mms_member_reimbursement.bk_hash in (select bk_hash from #p_mms_member_reimbursement_insert)

  insert dbo.d_mms_member_reimbursement(
             bk_hash,
             fact_mms_member_reimbursement_key,
             member_reimbursement_id,
             dim_mms_member_key,
             dim_mms_reimbursement_program_key,
             enrollment_date,
             enrollment_dim_date_key,
             member_id,
             reimbursement_program_id,
             reimbursement_program_identifier_format_bk_hash,
             reimbursement_program_identifier_format_id,
             termination_date,
             termination_dim_date_key,
             deleted_flag,
             p_mms_member_reimbursement_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_member_reimbursement_key,
         member_reimbursement_id,
         dim_mms_member_key,
         dim_mms_reimbursement_program_key,
         enrollment_date,
         enrollment_dim_date_key,
         member_id,
         reimbursement_program_id,
         reimbursement_program_identifier_format_bk_hash,
         reimbursement_program_identifier_format_id,
         termination_date,
         termination_dim_date_key,
         dv_deleted,
         p_mms_member_reimbursement_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_member_reimbursement)
--Done!
end
