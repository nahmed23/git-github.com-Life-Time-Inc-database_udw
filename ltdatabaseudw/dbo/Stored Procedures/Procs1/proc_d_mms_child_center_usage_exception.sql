CREATE PROC [dbo].[proc_d_mms_child_center_usage_exception] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_child_center_usage_exception)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_child_center_usage_exception_insert') is not null drop table #p_mms_child_center_usage_exception_insert
create table dbo.#p_mms_child_center_usage_exception_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id,
       p_mms_child_center_usage_exception.bk_hash
  from dbo.p_mms_child_center_usage_exception
 where p_mms_child_center_usage_exception.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_child_center_usage_exception.dv_batch_id > @max_dv_batch_id
        or p_mms_child_center_usage_exception.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_child_center_usage_exception.bk_hash,
       p_mms_child_center_usage_exception.bk_hash fact_mms_child_center_usage_exception_key,
       p_mms_child_center_usage_exception.child_center_usage_exception_id child_center_usage_exception_id,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.employee_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.employee_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_employee_key,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.val_child_center_usage_exception_id is null then '-998'
             else 'r_mms_val_child_center_usage_exception'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.val_child_center_usage_exception_id as varchar(500)),'z#@$k%&P'))),2)
        end exception_dim_description_key,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.child_center_usage_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.child_center_usage_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_child_center_usage_key,
       p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id,
       p_mms_child_center_usage_exception.dv_batch_id,
       p_mms_child_center_usage_exception.dv_load_date_time,
       p_mms_child_center_usage_exception.dv_load_end_date_time
  from dbo.h_mms_child_center_usage_exception
  join dbo.p_mms_child_center_usage_exception
    on h_mms_child_center_usage_exception.bk_hash = p_mms_child_center_usage_exception.bk_hash  join #p_mms_child_center_usage_exception_insert
    on p_mms_child_center_usage_exception.bk_hash = #p_mms_child_center_usage_exception_insert.bk_hash
   and p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id = #p_mms_child_center_usage_exception_insert.p_mms_child_center_usage_exception_id
  join dbo.l_mms_child_center_usage_exception
    on p_mms_child_center_usage_exception.bk_hash = l_mms_child_center_usage_exception.bk_hash
   and p_mms_child_center_usage_exception.l_mms_child_center_usage_exception_id = l_mms_child_center_usage_exception.l_mms_child_center_usage_exception_id
  join dbo.s_mms_child_center_usage_exception
    on p_mms_child_center_usage_exception.bk_hash = s_mms_child_center_usage_exception.bk_hash
   and p_mms_child_center_usage_exception.s_mms_child_center_usage_exception_id = s_mms_child_center_usage_exception.s_mms_child_center_usage_exception_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_child_center_usage_exception
   where d_mms_child_center_usage_exception.bk_hash in (select bk_hash from #p_mms_child_center_usage_exception_insert)

  insert dbo.d_mms_child_center_usage_exception(
             bk_hash,
             fact_mms_child_center_usage_exception_key,
             child_center_usage_exception_id,
             dim_employee_key,
             exception_dim_description_key,
             fact_mms_child_center_usage_key,
             p_mms_child_center_usage_exception_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_child_center_usage_exception_key,
         child_center_usage_exception_id,
         dim_employee_key,
         exception_dim_description_key,
         fact_mms_child_center_usage_key,
         p_mms_child_center_usage_exception_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_child_center_usage_exception)
--Done!
end
