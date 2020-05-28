CREATE PROC [dbo].[proc_d_fact_spabiz_ticket_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item','proc_d_fact_spabiz_ticket_item start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_ticket_item
 
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item','#p_spabiz_ticket_data_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ticket_data_insert') is not null drop table #p_spabiz_ticket_data_insert
create table dbo.#p_spabiz_ticket_data_insert with(distribution=hash(bk_hash), location=user_db, heap) as
select p_spabiz_ticket_data.p_spabiz_ticket_data_id,
       p_spabiz_ticket_data.bk_hash,
       row_number() over (order by p_spabiz_ticket_data_id) row_num
  from dbo.p_spabiz_ticket_data
  join #batch_id
    on p_spabiz_ticket_data.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ticket_data.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ticket_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
 
-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(fact_spabiz_ticket_item_key), location=user_db, heap) as
select #p_spabiz_ticket_data_insert.row_num,
       p_spabiz_ticket_data.bk_hash fact_spabiz_ticket_item_key,
       p_spabiz_ticket_data.store_number store_number,
       p_spabiz_ticket_data.ticket_data_id ticket_data_id,
       s_spabiz_ticket_data.cost cost,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.group_id is null then '-998'
            when l_spabiz_ticket_data.group_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.group_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_category_key,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.cust_id is null then '-998'
            when l_spabiz_ticket_data.cust_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.cust_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_customer_key,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.data_type is null then '-998'
            when l_spabiz_ticket_data.data_type = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.data_type as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_data_type_key,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.discount_id is null then '-998'
            when l_spabiz_ticket_data.discount_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.discount_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_discount_key,
       s_spabiz_ticket_data.edit_time edit_time,
       s_spabiz_ticket_data.end_time end_time,
       s_spabiz_ticket_data.ext_price ext_price,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.ticket_id is null then '-998'
            when l_spabiz_ticket_data.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.staff_id_1 is null then '-998'
            when l_spabiz_ticket_data.staff_id_1 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.staff_id_1 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end first_dim_spabiz_staff_key,
       s_spabiz_ticket_data.ticket_dis_amt item_discount_amount,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.item_id is null then '-998'
            when l_spabiz_ticket_data.item_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.item_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end item_id_store_number_hash,
       convert(bigint, s_spabiz_ticket_data.line_num) line_number,
       s_spabiz_ticket_data.other_amt other_amount,
       case when s_spabiz_ticket_data.other_qty is null then 0
            else convert(bigint, s_spabiz_ticket_data.other_qty)
        end other_quantity,
       s_spabiz_ticket_data.product_amt product_amount,
       case when s_spabiz_ticket_data.product_qty is null then 0
            else convert(bigint, s_spabiz_ticket_data.product_qty)
        end product_quantity,
       case when s_spabiz_ticket_data.qty is null then 0
            else convert(bigint, s_spabiz_ticket_data.qty)
        end quantity,
       s_spabiz_ticket_data.retail_price retail_price,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.staff_id_2 is null then '-998'
            when l_spabiz_ticket_data.staff_id_2 = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.staff_id_2 as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end second_dim_spabiz_staff_key,
       s_spabiz_ticket_data.service_amt service_amount,
       case when s_spabiz_ticket_data.service_qty is null then 0
            else convert(bigint, s_spabiz_ticket_data.service_qty)
        end service_quantity,
       s_spabiz_ticket_data.start_time start_time,
		's_spabiz_ticket_data.status _'+ convert(varchar,convert(int,s_spabiz_ticket_data.status)) status_dim_description_key,
		convert(int,s_spabiz_ticket_data.status) status_id,
       case when p_spabiz_ticket_data.bk_hash in ('-997','-998','-999') then p_spabiz_ticket_data.bk_hash
            when l_spabiz_ticket_data.sub_group_id is null then '-998'
            when l_spabiz_ticket_data.sub_group_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.sub_group_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end sub_dim_spabiz_category_key,
       convert(bigint, l_spabiz_ticket_data.ticket_id) ticket_id,
       s_spabiz_ticket_data.date ticket_item_date_time,
       s_spabiz_ticket_data.discount_amount ticket_total_discount_amount,
       convert(bigint, l_spabiz_ticket_data.cust_id) l_spabiz_ticket_data_cust_id,
       convert(bigint, l_spabiz_ticket_data.data_type) l_spabiz_ticket_data_data_type,
       convert(bigint, l_spabiz_ticket_data.day_id) l_spabiz_ticket_data_day_id,
       convert(bigint, l_spabiz_ticket_data.discount_id) l_spabiz_ticket_data_discount_id,
       convert(bigint, l_spabiz_ticket_data.group_id) l_spabiz_ticket_data_group_id,
       convert(bigint, l_spabiz_ticket_data.item_id) l_spabiz_ticket_data_item_id,
       convert(bigint, l_spabiz_ticket_data.staff_id_1) l_spabiz_ticket_data_staff_id_1,
       convert(bigint, l_spabiz_ticket_data.staff_id_2) l_spabiz_ticket_data_staff_id_2,
       convert(bigint, l_spabiz_ticket_data.sub_group_id) l_spabiz_ticket_data_sub_group_id,
       p_spabiz_ticket_data.p_spabiz_ticket_data_id p_spabiz_ticket_data_id,
       p_spabiz_ticket_data.dv_batch_id,
       p_spabiz_ticket_data.dv_load_date_time,
       p_spabiz_ticket_data.dv_load_end_date_time
	    from dbo.p_spabiz_ticket_data
  join #p_spabiz_ticket_data_insert
    on p_spabiz_ticket_data.p_spabiz_ticket_data_id = #p_spabiz_ticket_data_insert.p_spabiz_ticket_data_id
   and p_spabiz_ticket_data.bk_hash = #p_spabiz_ticket_data_insert.bk_hash
  join dbo.l_spabiz_ticket_data
    on p_spabiz_ticket_data.l_spabiz_ticket_data_id = l_spabiz_ticket_data.l_spabiz_ticket_data_id
   and p_spabiz_ticket_data.bk_hash = l_spabiz_ticket_data.bk_hash
  join dbo.s_spabiz_ticket_data
    on p_spabiz_ticket_data.s_spabiz_ticket_data_id = s_spabiz_ticket_data.s_spabiz_ticket_data_id
   and p_spabiz_ticket_data.bk_hash = s_spabiz_ticket_data.bk_hash

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_ticket_item', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_ticket_item
      where d_fact_spabiz_ticket_item.fact_spabiz_ticket_item_key in (select bk_hash from #p_spabiz_ticket_data_insert where row_num >= @start and row_num < @start+1000000)
	   
      insert dbo.d_fact_spabiz_ticket_item(
                 d_fact_spabiz_ticket_item_id,
                 fact_spabiz_ticket_item_key,
                 store_number,
                 ticket_data_id,
                 cost,
                 dim_spabiz_category_key,
                 dim_spabiz_customer_key,
                 dim_spabiz_data_type_key,
                 dim_spabiz_discount_key,
                 edit_date_time,
                 end_date_time,
                 ext_price,
                 fact_spabiz_ticket_key,
                 first_dim_spabiz_staff_key,
                 item_discount_amount,
                 item_id_store_number_hash,
                 line_number,
                 other_amount,
                 other_quantity,
                 product_amount,
                 product_quantity,
                 quantity,
                 retail_price,
                 second_dim_spabiz_staff_key,
                 service_amount,
                 service_quantity,
                 start_date_time,
                 status_dim_description_key,
				 status_id,
                 sub_dim_spabiz_category_key,
                 ticket_id,
                 ticket_item_date_time,
                 ticket_total_discount_amount,
                 l_spabiz_ticket_data_cust_id,
                 l_spabiz_ticket_data_data_type,
                 l_spabiz_ticket_data_day_id,
                 l_spabiz_ticket_data_discount_id,
                 l_spabiz_ticket_data_group_id,
                 l_spabiz_ticket_data_item_id,
                 l_spabiz_ticket_data_staff_id_1,
                 l_spabiz_ticket_data_staff_id_2,
                 l_spabiz_ticket_data_sub_group_id,
                 p_spabiz_ticket_data_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_ticket_item_key,
             store_number,
            ticket_data_id,
             cost,
             dim_spabiz_category_key,
             dim_spabiz_customer_key,
             dim_spabiz_data_type_key,
             dim_spabiz_discount_key,
             edit_time,
             end_time,
             ext_price,
             fact_spabiz_ticket_key,
             first_dim_spabiz_staff_key,
             item_discount_amount,
             item_id_store_number_hash,
             line_number,
             other_amount,
             other_quantity,
             product_amount,
             product_quantity,
             quantity,
             retail_price,
             second_dim_spabiz_staff_key,
             service_amount,
             service_quantity,
             start_time,
             status_dim_description_key,
			 status_id,
             sub_dim_spabiz_category_key,
             ticket_id,
             ticket_item_date_time,
             ticket_total_discount_amount,
             l_spabiz_ticket_data_cust_id,
             l_spabiz_ticket_data_data_type,
             l_spabiz_ticket_data_day_id,
             l_spabiz_ticket_data_discount_id,
             l_spabiz_ticket_data_group_id,
             l_spabiz_ticket_data_item_id,
             l_spabiz_ticket_data_staff_id_1,
             l_spabiz_ticket_data_staff_id_2,
             l_spabiz_ticket_data_sub_group_id,
             p_spabiz_ticket_data_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket_item','proc_d_fact_spabiz_ticket_item end',@current_dv_batch_id
end

