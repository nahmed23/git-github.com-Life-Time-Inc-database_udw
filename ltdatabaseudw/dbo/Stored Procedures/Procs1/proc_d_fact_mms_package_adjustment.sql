CREATE PROC [dbo].[proc_d_fact_mms_package_adjustment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_mms_package_adjustment

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_package_adjustment_insert') is not null drop table #p_mms_package_adjustment_insert
create table dbo.#p_mms_package_adjustment_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_package_adjustment.p_mms_package_adjustment_id,
       p_mms_package_adjustment.bk_hash,
       row_number() over (order by p_mms_package_adjustment_id) row_num
  from dbo.p_mms_package_adjustment
  join #batch_id
    on p_mms_package_adjustment.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_package_adjustment.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_package_adjustment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_mms_package_adjustment_insert.row_num,
       p_mms_package_adjustment.bk_hash fact_mms_package_adjustment_key,
       p_mms_package_adjustment.package_adjustment_id package_adjustment_id,
       s_mms_package_adjustment.adjusted_date_time adjusted_date_time,
       s_mms_package_adjustment.comment adjustment_comment,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash
            when l_mms_package_adjustment.employee_id is null then '-998'
            when l_mms_package_adjustment.employee_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_adjustment.employee_id as varchar(500)),'z#@$k%&P'))),2)
        end adjustment_dim_employee_key,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash
            when l_mms_package_adjustment.mms_tran_id is null then '-998'
            when l_mms_package_adjustment.mms_tran_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_adjustment.mms_tran_id as varchar(500)),'z#@$k%&P'))),2)
        end adjustment_mms_tran_key,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash
            when l_mms_package_adjustment.val_package_adjustment_type_id is null then '-998'
            when l_mms_package_adjustment.val_package_adjustment_type_id in (0) then '-998'
            else 'r_mms_val_package_adjustment_type'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_adjustment.val_package_adjustment_type_id as varchar(500)),'z#@$k%&P'))),2)
       end adjustment_type_dim_mms_description_key,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash
            when l_mms_package_adjustment.package_id is null then '-998'
            when l_mms_package_adjustment.package_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_adjustment.package_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_package_key,
       s_mms_package_adjustment.sessions_adjusted number_of_sessions_adjusted,
       s_mms_package_adjustment.amount_adjusted package_adjustment_amount,
       isnull(cast(l_mms_package_adjustment.val_package_adjustment_type_id as int),-998) ref_mms_val_package_adjustment_type_id,
       p_mms_package_adjustment.p_mms_package_adjustment_id,
       p_mms_package_adjustment.dv_batch_id,
       p_mms_package_adjustment.dv_load_date_time,
       p_mms_package_adjustment.dv_load_end_date_time
  from dbo.p_mms_package_adjustment
  join #p_mms_package_adjustment_insert
    on p_mms_package_adjustment.p_mms_package_adjustment_id = #p_mms_package_adjustment_insert.p_mms_package_adjustment_id
  join dbo.l_mms_package_adjustment
    on p_mms_package_adjustment.l_mms_package_adjustment_id = l_mms_package_adjustment.l_mms_package_adjustment_id
  join dbo.s_mms_package_adjustment
    on p_mms_package_adjustment.s_mms_package_adjustment_id = s_mms_package_adjustment.s_mms_package_adjustment_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_mms_package_adjustment', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    begin tran
      delete dbo.d_fact_mms_package_adjustment
       where d_fact_mms_package_adjustment.fact_mms_package_adjustment_key in (select bk_hash from #p_mms_package_adjustment_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_mms_package_adjustment(
                 d_fact_mms_package_adjustment_id,
                 fact_mms_package_adjustment_key,
                 package_adjustment_id,
                 adjusted_date_time,
                 adjustment_comment,
                 adjustment_dim_employee_key,
                 adjustment_mms_tran_key,
                 adjustment_type_dim_mms_description_key,
                 fact_mms_package_key,
                 number_of_sessions_adjusted,
                 package_adjustment_amount,
                 ref_mms_val_package_adjustment_type_id,
                 p_mms_package_adjustment_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_mms_package_adjustment_key,
             package_adjustment_id,
             adjusted_date_time,
             adjustment_comment,
             adjustment_dim_employee_key,
             adjustment_mms_tran_key,
             adjustment_type_dim_mms_description_key,
             fact_mms_package_key,
             number_of_sessions_adjusted,
             package_adjustment_amount,
             ref_mms_val_package_adjustment_type_id,
             p_mms_package_adjustment_id,
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
end
