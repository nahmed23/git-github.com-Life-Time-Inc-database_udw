CREATE PROC [dbo].[proc_d_medallia_field_answer] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_medallia_field_answer)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_medallia_field_answer_insert') is not null drop table #p_medallia_field_answer_insert
create table dbo.#p_medallia_field_answer_insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_field_answer.p_medallia_field_answer_id,
       p_medallia_field_answer.bk_hash
  from dbo.p_medallia_field_answer
 where p_medallia_field_answer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_medallia_field_answer.dv_batch_id > @max_dv_batch_id
        or p_medallia_field_answer.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_field_answer.bk_hash,
       rtrim(ltrim(p_medallia_field_answer.answer_id)) answer_id,
       s_medallia_field_answer.answer_name answer_name,
       s_medallia_field_answer.answer_type answer_type,
       case when p_medallia_field_answer.bk_hash in('-997', '-998', '-999') then p_medallia_field_answer.bk_hash   
          when p_medallia_field_answer.answer_id is null then '-998'    
          else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast( rtrim(ltrim(p_medallia_field_answer.answer_id))as int) as varchar(500)),'z#@$k%&P'))),2)  end dim_medallia_field_answer_key,
       isnull(h_medallia_field_answer.dv_deleted,0) dv_deleted,
       p_medallia_field_answer.p_medallia_field_answer_id,
       p_medallia_field_answer.dv_batch_id,
       p_medallia_field_answer.dv_load_date_time,
       p_medallia_field_answer.dv_load_end_date_time
  from dbo.h_medallia_field_answer
  join dbo.p_medallia_field_answer
    on h_medallia_field_answer.bk_hash = p_medallia_field_answer.bk_hash
  join #p_medallia_field_answer_insert
    on p_medallia_field_answer.bk_hash = #p_medallia_field_answer_insert.bk_hash
   and p_medallia_field_answer.p_medallia_field_answer_id = #p_medallia_field_answer_insert.p_medallia_field_answer_id
  join dbo.s_medallia_field_answer
    on p_medallia_field_answer.bk_hash = s_medallia_field_answer.bk_hash
   and p_medallia_field_answer.s_medallia_field_answer_id = s_medallia_field_answer.s_medallia_field_answer_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_medallia_field_answer
   where d_medallia_field_answer.bk_hash in (select bk_hash from #p_medallia_field_answer_insert)

  insert dbo.d_medallia_field_answer(
             bk_hash,
             answer_id,
             answer_name,
             answer_type,
             dim_medallia_field_answer_key,
             deleted_flag,
             p_medallia_field_answer_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         answer_id,
         answer_name,
         answer_type,
         dim_medallia_field_answer_key,
         dv_deleted,
         p_medallia_field_answer_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_medallia_field_answer)
--Done!
end
