CREATE PROC [dbo].[proc_d_fact_mms_child_center_usage_exception] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception','proc_d_fact_mms_child_center_usage_exception start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_mms_child_center_usage_exception

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception','#p_mms_child_center_usage_exception_insert',@current_dv_batch_id
if object_id('tempdb..#p_mms_child_center_usage_exception_insert') is not null drop table #p_mms_child_center_usage_exception_insert
create table dbo.#p_mms_child_center_usage_exception_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id,
       p_mms_child_center_usage_exception.bk_hash,
       row_number() over (order by p_mms_child_center_usage_exception_id) row_num
  from dbo.p_mms_child_center_usage_exception
  join #batch_id
    on p_mms_child_center_usage_exception.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_child_center_usage_exception.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_child_center_usage_exception.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_mms_child_center_usage_exception_insert.row_num,
       p_mms_child_center_usage_exception.bk_hash fact_mms_child_center_usage_exception_key,
       p_mms_child_center_usage_exception.child_center_usage_exception_id child_center_usage_exception_id,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.employee_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.employee_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_employee_key,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.val_child_center_usage_exception_id is null then '-998'
             else 'r_mms_val_child_center_usage_exception'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.val_child_center_usage_exception_id as varchar(500)),'z#@$k%&P'))),2)
        end exception_dim_mms_description_key,
       case when p_mms_child_center_usage_exception.bk_hash in ('-997','-998','-999') then p_mms_child_center_usage_exception.bk_hash
             when l_mms_child_center_usage_exception.child_center_usage_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_child_center_usage_exception.child_center_usage_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_child_center_usage_key,
       p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id,
       p_mms_child_center_usage_exception.dv_batch_id,
       p_mms_child_center_usage_exception.dv_load_date_time,
       p_mms_child_center_usage_exception.dv_load_end_date_time
  from dbo.p_mms_child_center_usage_exception
  join #p_mms_child_center_usage_exception_insert
    on p_mms_child_center_usage_exception.p_mms_child_center_usage_exception_id = #p_mms_child_center_usage_exception_insert.p_mms_child_center_usage_exception_id
  join dbo.l_mms_child_center_usage_exception
    on p_mms_child_center_usage_exception.l_mms_child_center_usage_exception_id = l_mms_child_center_usage_exception.l_mms_child_center_usage_exception_id
  join dbo.s_mms_child_center_usage_exception
    on p_mms_child_center_usage_exception.s_mms_child_center_usage_exception_id = s_mms_child_center_usage_exception.s_mms_child_center_usage_exception_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_mms_child_center_usage_exception', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_mms_child_center_usage_exception
       where d_fact_mms_child_center_usage_exception.fact_mms_child_center_usage_exception_key in (select bk_hash from #p_mms_child_center_usage_exception_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_mms_child_center_usage_exception(
                 d_fact_mms_child_center_usage_exception_id,
                 fact_mms_child_center_usage_exception_key,
                 child_center_usage_exception_id,
                 dim_mms_employee_key,
                 exception_dim_mms_description_key,
                 fact_mms_child_center_usage_key,
                 p_mms_child_center_usage_exception_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_mms_child_center_usage_exception_key,
             child_center_usage_exception_id,
             dim_mms_employee_key,
             exception_dim_mms_description_key,
             fact_mms_child_center_usage_key,
             p_mms_child_center_usage_exception_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
       where row_num >= @start
         and row_num < @start+1000000
    commit tran

    set @start = @start+1000000
end

--Done!
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_child_center_usage_exception','proc_d_fact_mms_child_center_usage_exception end',@current_dv_batch_id
end
