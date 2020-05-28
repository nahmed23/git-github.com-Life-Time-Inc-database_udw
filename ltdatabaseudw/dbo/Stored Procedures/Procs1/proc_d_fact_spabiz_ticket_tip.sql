CREATE PROC [dbo].[proc_d_fact_spabiz_ticket_tip] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip','proc_d_fact_spabiz_ticket_tip start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_ticket_tip

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip','#p_spabiz_ticket_tip_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ticket_tip_insert') is not null drop table #p_spabiz_ticket_tip_insert
create table dbo.#p_spabiz_ticket_tip_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_ticket_tip.p_spabiz_ticket_tip_id,
       p_spabiz_ticket_tip.bk_hash,
       row_number() over (order by p_spabiz_ticket_tip_id) row_num
  from dbo.p_spabiz_ticket_tip
  join #batch_id
    on p_spabiz_ticket_tip.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ticket_tip.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ticket_tip.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_ticket_tip_insert.row_num,
       p_spabiz_ticket_tip.bk_hash fact_spabiz_ticket_tip_key,
       p_spabiz_ticket_tip.store_number store_number,
       p_spabiz_ticket_tip.ticket_tip_id ticket_tip_id,
       s_spabiz_ticket_tip.amount amount,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ticket_tip.date = convert(date, '18991230', 112) then null
            else s_spabiz_ticket_tip.date
        end created_date_time,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_tip.bk_hash    
              when l_spabiz_ticket_tip.cust_id is null then '-998'       
       	   when l_spabiz_ticket_tip.cust_id = 0 then '-998'       
       	   when l_spabiz_ticket_tip.cust_id = -1 then '-998'
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.cust_id as varchar(500)),'z#@$k%&P'))),2)       
       	   end dim_spabiz_customer_key,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_tip.bk_hash
            when l_spabiz_ticket_tip.shift_id is null then '-998'
            when l_spabiz_ticket_tip.shift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_shift_key,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_tip.bk_hash
            when l_spabiz_ticket_tip.staff_id is null then '-998'
            when l_spabiz_ticket_tip.staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_tip.bk_hash
            when l_spabiz_ticket_tip.store_number is null then '-998'
            when l_spabiz_ticket_tip.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ticket_tip.edit_time edit_date_time,
       case when p_spabiz_ticket_tip.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_tip.bk_hash
            when l_spabiz_ticket_tip.ticket_id is null then '-998'
            when l_spabiz_ticket_tip.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_tip.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       s_spabiz_ticket_tip.paid paid,
       case when s_spabiz_ticket_tip.split = 1 then 'Y'
            else 'N'
        end split_flag,
       's_spabiz_ticket_tip.status_' + convert(varchar,s_spabiz_ticket_tip.status) status_dim_description_key,
       s_spabiz_ticket_tip.status status_id,
       l_spabiz_ticket_tip.cust_id l_spabiz_ticket_tip_cust_id,
       l_spabiz_ticket_tip.shift_id l_spabiz_ticket_tip_shift_id,
       l_spabiz_ticket_tip.staff_id l_spabiz_ticket_tip_staff_id,
       l_spabiz_ticket_tip.ticket_id l_spabiz_ticket_tip_ticket_id,
       s_spabiz_ticket_tip.status s_spabiz_ticket_tip_status,
       p_spabiz_ticket_tip.p_spabiz_ticket_tip_id,
       p_spabiz_ticket_tip.dv_batch_id,
       p_spabiz_ticket_tip.dv_load_date_time,
       p_spabiz_ticket_tip.dv_load_end_date_time
  from dbo.p_spabiz_ticket_tip
  join #p_spabiz_ticket_tip_insert
    on p_spabiz_ticket_tip.p_spabiz_ticket_tip_id = #p_spabiz_ticket_tip_insert.p_spabiz_ticket_tip_id
  join dbo.l_spabiz_ticket_tip
    on p_spabiz_ticket_tip.l_spabiz_ticket_tip_id = l_spabiz_ticket_tip.l_spabiz_ticket_tip_id
  join dbo.s_spabiz_ticket_tip
    on p_spabiz_ticket_tip.s_spabiz_ticket_tip_id = s_spabiz_ticket_tip.s_spabiz_ticket_tip_id
 where l_spabiz_ticket_tip.store_number not in (1,100,999) OR p_spabiz_ticket_tip.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_ticket_tip', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_ticket_tip
       where d_fact_spabiz_ticket_tip.fact_spabiz_ticket_tip_key in (select bk_hash from #p_spabiz_ticket_tip_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_ticket_tip(
                 d_fact_spabiz_ticket_tip_id,
                 fact_spabiz_ticket_tip_key,
                 store_number,
                 ticket_tip_id,
                 amount,
                 created_date_time,
                 dim_spabiz_customer_key,
                 dim_spabiz_shift_key,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 fact_spabiz_ticket_key,
                 paid,
                 split_flag,
                 status_dim_description_key,
                 status_id,
                 l_spabiz_ticket_tip_cust_id,
                 l_spabiz_ticket_tip_shift_id,
                 l_spabiz_ticket_tip_staff_id,
                 l_spabiz_ticket_tip_ticket_id,
                 s_spabiz_ticket_tip_status,
                 p_spabiz_ticket_tip_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_ticket_tip_key,
             store_number,
             ticket_tip_id,
             amount,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_shift_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_ticket_key,
             paid,
             split_flag,
             status_dim_description_key,
             status_id,
             l_spabiz_ticket_tip_cust_id,
             l_spabiz_ticket_tip_shift_id,
             l_spabiz_ticket_tip_staff_id,
             l_spabiz_ticket_tip_ticket_id,
             s_spabiz_ticket_tip_status,
             p_spabiz_ticket_tip_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_tip','proc_d_fact_spabiz_ticket_tip end',@current_dv_batch_id
end
