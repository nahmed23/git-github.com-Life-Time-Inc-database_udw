CREATE PROC [dbo].[proc_d_fact_mms_package_session] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session','proc_d_fact_mms_package_session start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_mms_package_session

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session','#p_mms_package_session_insert',@current_dv_batch_id
if object_id('tempdb..#p_mms_package_session_insert') is not null drop table #p_mms_package_session_insert
create table dbo.#p_mms_package_session_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_package_session.p_mms_package_session_id,
       p_mms_package_session.bk_hash,
       row_number() over (order by p_mms_package_session_id) row_num
  from dbo.p_mms_package_session
  join #batch_id
    on p_mms_package_session.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_package_session.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_package_session.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_mms_package_session_insert.row_num,
       p_mms_package_session.bk_hash fact_mms_package_session_key,
       p_mms_package_session.package_session_id package_session_id,
       s_mms_package_session.created_date_time created_date_time,
       s_mms_package_session.delivered_date_time delivered_date_time,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash
            when l_mms_package_session.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.club_id as varchar(500)),'z#@$k%&P'))),2)
        end delivered_dim_club_key,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash
            when l_mms_package_session.delivered_employee_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.delivered_employee_id as varchar(500)),'z#@$k%&P'))),2)
        end delivered_dim_team_member_key,
       isnull(s_mms_package_session.comment,'') delivered_session_comment,
        isnull(s_mms_package_session.session_price,0) delivered_session_price,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash
            when l_mms_package_session.package_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.package_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_package_key,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash
            when l_mms_package_session.mms_tran_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.mms_tran_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_id,
       p_mms_package_session.p_mms_package_session_id,
       p_mms_package_session.dv_batch_id,
       p_mms_package_session.dv_load_date_time,
       p_mms_package_session.dv_load_end_date_time
  from dbo.p_mms_package_session
  join #p_mms_package_session_insert
    on p_mms_package_session.p_mms_package_session_id = #p_mms_package_session_insert.p_mms_package_session_id
  join dbo.l_mms_package_session
    on p_mms_package_session.l_mms_package_session_id = l_mms_package_session.l_mms_package_session_id
  join dbo.s_mms_package_session
    on p_mms_package_session.s_mms_package_session_id = s_mms_package_session.s_mms_package_session_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_mms_package_session', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_mms_package_session
       where d_fact_mms_package_session.fact_mms_package_session_key in (select bk_hash from #p_mms_package_session_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_mms_package_session(
                 d_fact_mms_package_session_id,
                 fact_mms_package_session_key,
                 package_session_id,
                 created_date_time,
                 delivered_date_time,
                 delivered_dim_club_key,
                 delivered_dim_team_member_key,
                 delivered_session_comment,
                 delivered_session_price,
                 fact_mms_package_key,
                 fact_mms_sales_transaction_id,
                 p_mms_package_session_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_mms_package_session_key,
             package_session_id,
             created_date_time,
             delivered_date_time,
             delivered_dim_club_key,
             delivered_dim_team_member_key,
             delivered_session_comment,
             delivered_session_price,
             fact_mms_package_key,
             fact_mms_sales_transaction_id,
             p_mms_package_session_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package_session','proc_d_fact_mms_package_session end',@current_dv_batch_id
end
