CREATE PROC [dbo].[proc_d_mms_reimbursement_program_identifier_format_part] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_reimbursement_program_identifier_format_part)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_reimbursement_program_identifier_format_part_insert') is not null drop table #p_mms_reimbursement_program_identifier_format_part_insert
create table dbo.#p_mms_reimbursement_program_identifier_format_part_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_reimbursement_program_identifier_format_part.p_mms_reimbursement_program_identifier_format_part_id,
       p_mms_reimbursement_program_identifier_format_part.bk_hash
  from dbo.p_mms_reimbursement_program_identifier_format_part
 where p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_reimbursement_program_identifier_format_part.dv_batch_id > @max_dv_batch_id
        or p_mms_reimbursement_program_identifier_format_part.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_reimbursement_program_identifier_format_part.bk_hash,
       p_mms_reimbursement_program_identifier_format_part.bk_hash reimbursement_program_identifier_format_part_bk_hash,
       p_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_part_id reimbursement_program_identifier_format_part_id,
       case when p_mms_reimbursement_program_identifier_format_part.bk_hash in ('-997','-998','-999')       then p_mms_reimbursement_program_identifier_format_part.bk_hash               when s_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_part_id is null then '-998'      else 's_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_part_id_'+s_mms_reimbursement_program_identifier_format_part.bk_hash  end field_name_dim_description_key,
       s_mms_reimbursement_program_identifier_format_part.field_sequence field_sequence,
       case when p_mms_reimbursement_program_identifier_format_part.bk_hash in ('-997','-998','-999') then p_mms_reimbursement_program_identifier_format_part.bk_hash
            when l_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end reimbursement_program_identifier_format_bk_hash,
       h_mms_reimbursement_program_identifier_format_part.dv_deleted,
       p_mms_reimbursement_program_identifier_format_part.p_mms_reimbursement_program_identifier_format_part_id,
       p_mms_reimbursement_program_identifier_format_part.dv_batch_id,
       p_mms_reimbursement_program_identifier_format_part.dv_load_date_time,
       p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time
  from dbo.h_mms_reimbursement_program_identifier_format_part
  join dbo.p_mms_reimbursement_program_identifier_format_part
    on h_mms_reimbursement_program_identifier_format_part.bk_hash = p_mms_reimbursement_program_identifier_format_part.bk_hash
  join #p_mms_reimbursement_program_identifier_format_part_insert
    on p_mms_reimbursement_program_identifier_format_part.bk_hash = #p_mms_reimbursement_program_identifier_format_part_insert.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.p_mms_reimbursement_program_identifier_format_part_id = #p_mms_reimbursement_program_identifier_format_part_insert.p_mms_reimbursement_program_identifier_format_part_id
  join dbo.l_mms_reimbursement_program_identifier_format_part
    on p_mms_reimbursement_program_identifier_format_part.bk_hash = l_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id = l_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id
  join dbo.s_mms_reimbursement_program_identifier_format_part
    on p_mms_reimbursement_program_identifier_format_part.bk_hash = s_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id = s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_reimbursement_program_identifier_format_part
   where d_mms_reimbursement_program_identifier_format_part.bk_hash in (select bk_hash from #p_mms_reimbursement_program_identifier_format_part_insert)

  insert dbo.d_mms_reimbursement_program_identifier_format_part(
             bk_hash,
             reimbursement_program_identifier_format_part_bk_hash,
             reimbursement_program_identifier_format_part_id,
             field_name_dim_description_key,
             field_sequence,
             reimbursement_program_identifier_format_bk_hash,
             p_mms_reimbursement_program_identifier_format_part_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         reimbursement_program_identifier_format_part_bk_hash,
         reimbursement_program_identifier_format_part_id,
         field_name_dim_description_key,
         field_sequence,
         reimbursement_program_identifier_format_bk_hash,
         p_mms_reimbursement_program_identifier_format_part_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_reimbursement_program_identifier_format_part)
--Done!
end
