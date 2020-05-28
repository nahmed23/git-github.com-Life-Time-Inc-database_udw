CREATE PROC [dbo].[proc_d_mms_tran_voided] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_tran_voided)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_tran_voided_insert') is not null drop table #p_mms_tran_voided_insert
create table dbo.#p_mms_tran_voided_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_voided.p_mms_tran_voided_id,
       p_mms_tran_voided.bk_hash
  from dbo.p_mms_tran_voided
 where p_mms_tran_voided.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_tran_voided.dv_batch_id > @max_dv_batch_id
        or p_mms_tran_voided.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_voided.bk_hash,
       p_mms_tran_voided.tran_voided_id tran_voided_id,
       isnull(s_mms_tran_voided.comments, '') void_comment,
       case when p_mms_tran_voided.bk_hash in ('-997', '-998', '-999') then p_mms_tran_voided.bk_hash
            when s_mms_tran_voided.void_date_time is null then '-998'
            else convert(varchar, s_mms_tran_voided.void_date_time, 112)
        end void_dim_date_key,
       case when p_mms_tran_voided.bk_hash in ('-997', '-998', '-999') then p_mms_tran_voided.bk_hash
            when l_mms_tran_voided.employee_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_tran_voided.employee_id as varchar(500)),'z#@$k%&P'))),2)
        end void_dim_employee_key,
       case when p_mms_tran_voided.bk_hash in ('-997', '-998', '-999') then p_mms_tran_voided.bk_hash
            when s_mms_tran_voided.void_date_time is null then '-998'
            else '1' + replace(substring(convert(varchar,s_mms_tran_voided.void_date_time,114), 1, 5),':','')
        end void_dim_time_key,
       p_mms_tran_voided.p_mms_tran_voided_id,
       p_mms_tran_voided.dv_batch_id,
       p_mms_tran_voided.dv_load_date_time,
       p_mms_tran_voided.dv_load_end_date_time
  from dbo.p_mms_tran_voided
  join #p_mms_tran_voided_insert
    on p_mms_tran_voided.bk_hash = #p_mms_tran_voided_insert.bk_hash
   and p_mms_tran_voided.p_mms_tran_voided_id = #p_mms_tran_voided_insert.p_mms_tran_voided_id
  join dbo.l_mms_tran_voided
    on p_mms_tran_voided.bk_hash = l_mms_tran_voided.bk_hash
   and p_mms_tran_voided.l_mms_tran_voided_id = l_mms_tran_voided.l_mms_tran_voided_id
  join dbo.s_mms_tran_voided
    on p_mms_tran_voided.bk_hash = s_mms_tran_voided.bk_hash
   and p_mms_tran_voided.s_mms_tran_voided_id = s_mms_tran_voided.s_mms_tran_voided_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_tran_voided
   where d_mms_tran_voided.bk_hash in (select bk_hash from #p_mms_tran_voided_insert)

  insert dbo.d_mms_tran_voided(
             bk_hash,
             tran_voided_id,
             void_comment,
             void_dim_date_key,
             void_dim_employee_key,
             void_dim_time_key,
             p_mms_tran_voided_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         tran_voided_id,
         void_comment,
         void_dim_date_key,
         void_dim_employee_key,
         void_dim_time_key,
         p_mms_tran_voided_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_tran_voided)
--Done!
end
