CREATE PROC [dbo].[proc_d_boss_asi_dept_m] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_dept_m)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_dept_m_insert') is not null drop table #p_boss_asi_dept_m_insert
create table dbo.#p_boss_asi_dept_m_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_dept_m.p_boss_asi_dept_m_id,
       p_boss_asi_dept_m.bk_hash
  from dbo.p_boss_asi_dept_m
 where p_boss_asi_dept_m.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_dept_m.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_dept_m.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_dept_m.bk_hash,
       p_boss_asi_dept_m.dept_m_code department_code,
       isnull(s_boss_asi_dept_m.dept_m_desc, '') department_description,
       p_boss_asi_dept_m.p_boss_asi_dept_m_id,
       p_boss_asi_dept_m.dv_batch_id,
       p_boss_asi_dept_m.dv_load_date_time,
       p_boss_asi_dept_m.dv_load_end_date_time
  from dbo.h_boss_asi_dept_m
  join dbo.p_boss_asi_dept_m
    on h_boss_asi_dept_m.bk_hash = p_boss_asi_dept_m.bk_hash  join #p_boss_asi_dept_m_insert
    on p_boss_asi_dept_m.bk_hash = #p_boss_asi_dept_m_insert.bk_hash
   and p_boss_asi_dept_m.p_boss_asi_dept_m_id = #p_boss_asi_dept_m_insert.p_boss_asi_dept_m_id
  join dbo.l_boss_asi_dept_m
    on p_boss_asi_dept_m.bk_hash = l_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.l_boss_asi_dept_m_id = l_boss_asi_dept_m.l_boss_asi_dept_m_id
  join dbo.s_boss_asi_dept_m
    on p_boss_asi_dept_m.bk_hash = s_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.s_boss_asi_dept_m_id = s_boss_asi_dept_m.s_boss_asi_dept_m_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_dept_m
   where d_boss_asi_dept_m.bk_hash in (select bk_hash from #p_boss_asi_dept_m_insert)

  insert dbo.d_boss_asi_dept_m(
             bk_hash,
             department_code,
             department_description,
             p_boss_asi_dept_m_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         department_code,
         department_description,
         p_boss_asi_dept_m_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_dept_m)
--Done!
end
