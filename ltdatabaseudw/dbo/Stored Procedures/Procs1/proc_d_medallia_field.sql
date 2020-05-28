CREATE PROC [dbo].[proc_d_medallia_field] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_medallia_field)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_medallia_field_insert') is not null drop table #p_medallia_field_insert
create table dbo.#p_medallia_field_insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_field.p_medallia_field_id,
       p_medallia_field.bk_hash
  from dbo.p_medallia_field
 where p_medallia_field.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_medallia_field.dv_batch_id > @max_dv_batch_id
        or p_medallia_field.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_field.bk_hash,
       p_medallia_field.name_in_medallia name_in_medallia,
       rtrim(ltrim(l_medallia_field.answer_id)) answer_id,
       s_medallia_field.data_type data_type,
       s_medallia_field.description_question description_question,
       case when p_medallia_field.bk_hash in('-997', '-998', '-999') then p_medallia_field.bk_hash   
          when l_medallia_field.answer_id is null or isnumeric(l_medallia_field.answer_id) = 0 then '-998'    
          else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast( rtrim(ltrim(l_medallia_field.answer_id))as int) as varchar(500)),'z#@$k%&P'))),2)  end dim_medallia_field_answer_key,
       s_medallia_field.examples examples,
       s_medallia_field.name_in_api name_in_api,
       s_medallia_field.single_select single_select,
       s_medallia_field.sr_no sr_no,
       s_medallia_field.variable_name variable_name,
       isnull(h_medallia_field.dv_deleted,0) dv_deleted,
       p_medallia_field.p_medallia_field_id,
       p_medallia_field.dv_batch_id,
       p_medallia_field.dv_load_date_time,
       p_medallia_field.dv_load_end_date_time
  from dbo.h_medallia_field
  join dbo.p_medallia_field
    on h_medallia_field.bk_hash = p_medallia_field.bk_hash
  join #p_medallia_field_insert
    on p_medallia_field.bk_hash = #p_medallia_field_insert.bk_hash
   and p_medallia_field.p_medallia_field_id = #p_medallia_field_insert.p_medallia_field_id
  join dbo.l_medallia_field
    on p_medallia_field.bk_hash = l_medallia_field.bk_hash
   and p_medallia_field.l_medallia_field_id = l_medallia_field.l_medallia_field_id
  join dbo.s_medallia_field
    on p_medallia_field.bk_hash = s_medallia_field.bk_hash
   and p_medallia_field.s_medallia_field_id = s_medallia_field.s_medallia_field_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_medallia_field
   where d_medallia_field.bk_hash in (select bk_hash from #p_medallia_field_insert)

  insert dbo.d_medallia_field(
             bk_hash,
             name_in_medallia,
             answer_id,
             data_type,
             description_question,
             dim_medallia_field_answer_key,
             examples,
             name_in_api,
             single_select,
             sr_no,
             variable_name,
             deleted_flag,
             p_medallia_field_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         name_in_medallia,
         answer_id,
         data_type,
         description_question,
         dim_medallia_field_answer_key,
         examples,
         name_in_api,
         single_select,
         sr_no,
         variable_name,
         dv_deleted,
         p_medallia_field_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_medallia_field)
--Done!
end
