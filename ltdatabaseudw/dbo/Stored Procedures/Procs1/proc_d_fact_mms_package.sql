CREATE PROC [dbo].[proc_d_fact_mms_package] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package','proc_d_fact_mms_package start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_mms_package

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package','#p_mms_package_insert',@current_dv_batch_id
if object_id('tempdb..#p_mms_package_insert') is not null drop table #p_mms_package_insert
create table dbo.#p_mms_package_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_package.p_mms_package_id,
       p_mms_package.bk_hash,
       row_number() over (order by p_mms_package_id) row_num
  from dbo.p_mms_package
  join #batch_id
    on p_mms_package.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_package.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_package.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_mms_package_insert.row_num,
       p_mms_package.bk_hash fact_mms_package_key,
       s_mms_package.package_id package_id,
       isnull(s_mms_package.balance_amount, 0) balance_amount,
       s_mms_package.created_date_time created_date_time,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.club_id is null then '-998'
            when l_mms_package.club_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.club_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.member_id is null then '-998'
            when l_mms_package.member_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.member_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_member_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.membership_id is null then '-998'
            when l_mms_package.membership_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.membership_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.product_id is null then '-998'
            when l_mms_package.product_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.product_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_product_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.tran_item_id is null then '-998'
            when l_mms_package.tran_item_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.tran_item_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_item_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.mms_tran_id is null then '-998'
            when l_mms_package.mms_tran_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.mms_tran_id as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_key,
       isnull(s_mms_package.number_of_sessions, 0) number_of_sessions,
       s_mms_package.package_edit_date_time package_edit_date_time,
       case
                  when s_mms_package.package_edited_flag = 1 then 'Y'
                  else 'N'
              end package_edited_flag,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.employee_id is null then '-998'
            when l_mms_package.employee_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.employee_id as varchar(500)),'z#@$k%&P'))),2)
        end package_entered_dim_team_member_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
            when l_mms_package.val_package_status_id is null then '-998'
            when l_mms_package.val_package_status_id in (0) then '-998'
            else 'r_mms_val_package_status'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package.val_package_status_id as varchar(500)),'z#@$k%&P'))),2)
       end package_status_dim_description_key,
       isnull(s_mms_package.price_per_session, 0) price_per_session,
       isnull(l_mms_package.val_package_status_id,-998) ref_mms_val_package_status_id,
       isnull(s_mms_package.sessions_left, 0) sessions_remaining,
       p_mms_package.p_mms_package_id,
       p_mms_package.dv_batch_id,
       p_mms_package.dv_load_date_time,
       p_mms_package.dv_load_end_date_time
  from dbo.p_mms_package
  join #p_mms_package_insert
    on p_mms_package.p_mms_package_id = #p_mms_package_insert.p_mms_package_id
  join dbo.l_mms_package
    on p_mms_package.l_mms_package_id = l_mms_package.l_mms_package_id
  join dbo.s_mms_package
    on p_mms_package.s_mms_package_id = s_mms_package.s_mms_package_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_mms_package', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_mms_package
       where d_fact_mms_package.fact_mms_package_key in (select bk_hash from #p_mms_package_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_mms_package(
                 d_fact_mms_package_id,
                 fact_mms_package_key,
                 package_id,
                 balance_amount,
                 created_date_time,
                 dim_club_key,
                 dim_mms_member_key,
                 dim_mms_membership_key,
                 dim_mms_product_key,
                 fact_mms_sales_transaction_item_key,
                 fact_mms_sales_transaction_key,
                 number_of_sessions,
                 package_edit_date_time,
                 package_edited_flag,
                 package_entered_dim_team_member_key,
                 package_status_dim_description_key,
                 price_per_session,
                 ref_mms_val_package_status_id,
                 sessions_remaining,
                 p_mms_package_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_mms_package_key,
             package_id,
             balance_amount,
             created_date_time,
             dim_club_key,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_mms_product_key,
             fact_mms_sales_transaction_item_key,
             fact_mms_sales_transaction_key,
             number_of_sessions,
             package_edit_date_time,
             package_edited_flag,
             package_entered_dim_team_member_key,
             package_status_dim_description_key,
             price_per_session,
             ref_mms_val_package_status_id,
             sessions_remaining,
             p_mms_package_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_mms_package','proc_d_fact_mms_package end',@current_dv_batch_id
end
