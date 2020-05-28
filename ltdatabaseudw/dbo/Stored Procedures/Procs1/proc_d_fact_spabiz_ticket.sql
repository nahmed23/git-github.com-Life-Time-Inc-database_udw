CREATE PROC [dbo].[proc_d_fact_spabiz_ticket] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket','proc_d_fact_spabiz_ticket start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_ticket

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket','#p_spabiz_ticket_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ticket_insert') is not null drop table #p_spabiz_ticket_insert
create table dbo.#p_spabiz_ticket_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_ticket.p_spabiz_ticket_id,
       p_spabiz_ticket.bk_hash,
       row_number() over (order by p_spabiz_ticket_id) row_num
  from dbo.p_spabiz_ticket
  join #batch_id
    on p_spabiz_ticket.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ticket.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ticket.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_ticket_insert.row_num,
       p_spabiz_ticket.bk_hash fact_spabiz_ticket_key,
       p_spabiz_ticket.store_number store_number,
       p_spabiz_ticket.ticket_id ticket_id,
       s_spabiz_ticket.cash_change cash_change,
       s_spabiz_ticket.check_in_time check_in_date_time,
       s_spabiz_ticket.date created_date_time,
       case when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash     
              when l_spabiz_ticket.cust_id is null then '-998'       
       	   when l_spabiz_ticket.cust_id = 0 then '-998' 
              when l_spabiz_ticket.cust_id = -1 then '-998'    
              else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.cust_id as varchar(500)),'z#@$k%&P'))),2)	      
       	   end dim_spabiz_customer_key,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.pay_type_id is null then '-998'
            when l_spabiz_ticket.pay_type_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.pay_type_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_payment_type_key,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.shift_id is null then '-998'
            when l_spabiz_ticket.shift_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_shift_key,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.staff_id is null then '-998'
            when l_spabiz_ticket.staff_id = 0 then '-998'
            when l_spabiz_ticket.staff_id = -1 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.store_number is null then '-998'
            when l_spabiz_ticket.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ticket.discount_product discount_product,
       s_spabiz_ticket.discount_service discount_service,
       s_spabiz_ticket.discount_total discount_total,
       s_spabiz_ticket.edit_time edit_date_time,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.ap_id is null then '-998'
            when l_spabiz_ticket.ap_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.ap_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_appointment_key,
       case
            when s_spabiz_ticket.late = -1 then 'Y'
            else 'N'
        end late,
       case
            when s_spabiz_ticket.note is null then ''
            else s_spabiz_ticket.note
        end note,
       s_spabiz_ticket.sales_gift_total sales_gift_total,
       s_spabiz_ticket.sales_package_total sales_package_total,
       s_spabiz_ticket.sales_product_total sales_product_total,
       s_spabiz_ticket.sales_series_total sales_series_total,
       s_spabiz_ticket.sales_service_total sales_service_total,
       s_spabiz_ticket.sales_subtotal sales_subtotal,
       s_spabiz_ticket.sales_total sales_total,
       's_spabiz_ticket.status_' + convert(varchar,convert(int,s_spabiz_ticket.status)) status_dim_description_key,
       convert(int,s_spabiz_ticket.status) status_id,
       s_spabiz_ticket.tax_total tax_total,
       s_spabiz_ticket.ticket_id_for_day ticket_id_for_day,
       s_spabiz_ticket.ticket_num ticket_number,
       s_spabiz_ticket.tip tip_amount,
       case
            when p_spabiz_ticket.bk_hash in ('-997','-998','-999') then p_spabiz_ticket.bk_hash
            when l_spabiz_ticket.voider_id is null then '-998'
            when l_spabiz_ticket.voider_id = 0 then '-998'
            when l_spabiz_ticket.voider_id = -1 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ticket.voider_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ticket.store_number as varchar(500)),'z#@$k%&P'))),2)
        end voider_dim_spabiz_staff_key,
       l_spabiz_ticket.ap_id l_spabiz_ticket_ap_id,
       l_spabiz_ticket.cust_id l_spabiz_ticket_cust_id,
       l_spabiz_ticket.pay_type_id l_spabiz_ticket_pay_type_id,
       l_spabiz_ticket.shift_id l_spabiz_ticket_shift_id,
       l_spabiz_ticket.staff_id l_spabiz_ticket_staff_id,
       l_spabiz_ticket.voider_id l_spabiz_ticket_voider_id,
       p_spabiz_ticket.p_spabiz_ticket_id,
       p_spabiz_ticket.dv_batch_id,
       p_spabiz_ticket.dv_load_date_time,
       p_spabiz_ticket.dv_load_end_date_time
  from dbo.p_spabiz_ticket
  join #p_spabiz_ticket_insert
    on p_spabiz_ticket.p_spabiz_ticket_id = #p_spabiz_ticket_insert.p_spabiz_ticket_id
  join dbo.l_spabiz_ticket
    on p_spabiz_ticket.l_spabiz_ticket_id = l_spabiz_ticket.l_spabiz_ticket_id
  join dbo.s_spabiz_ticket
    on p_spabiz_ticket.s_spabiz_ticket_id = s_spabiz_ticket.s_spabiz_ticket_id
 where l_spabiz_ticket.store_number not in (1,100,999) OR p_spabiz_ticket.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_ticket', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_ticket
       where d_fact_spabiz_ticket.fact_spabiz_ticket_key in (select bk_hash from #p_spabiz_ticket_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_ticket(
                 d_fact_spabiz_ticket_id,
                 fact_spabiz_ticket_key,
                 store_number,
                 ticket_id,
                 cash_change,
                 check_in_date_time,
                 created_date_time,
                 dim_spabiz_customer_key,
                 dim_spabiz_payment_type_key,
                 dim_spabiz_shift_key,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 discount_product,
                 discount_service,
                 discount_total,
                 edit_date_time,
                 fact_spabiz_appointment_key,
                 late,
                 note,
                 sales_gift_total,
                 sales_package_total,
                 sales_product_total,
                 sales_series_total,
                 sales_service_total,
                 sales_subtotal,
                 sales_total,
                 status_dim_description_key,
                 status_id,
                 tax_total,
                 ticket_id_for_day,
                 ticket_number,
                 tip_amount,
                 voider_dim_spabiz_staff_key,
                 l_spabiz_ticket_ap_id,
                 l_spabiz_ticket_cust_id,
                 l_spabiz_ticket_pay_type_id,
                 l_spabiz_ticket_shift_id,
                 l_spabiz_ticket_staff_id,
                 l_spabiz_ticket_voider_id,
                 p_spabiz_ticket_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_ticket_key,
             store_number,
             ticket_id,
             cash_change,
             check_in_date_time,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_payment_type_key,
             dim_spabiz_shift_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             discount_product,
             discount_service,
             discount_total,
             edit_date_time,
             fact_spabiz_appointment_key,
             late,
             note,
             sales_gift_total,
             sales_package_total,
             sales_product_total,
             sales_series_total,
             sales_service_total,
             sales_subtotal,
             sales_total,
             status_dim_description_key,
             status_id,
             tax_total,
             ticket_id_for_day,
             ticket_number,
             tip_amount,
             voider_dim_spabiz_staff_key,
             l_spabiz_ticket_ap_id,
             l_spabiz_ticket_cust_id,
             l_spabiz_ticket_pay_type_id,
             l_spabiz_ticket_shift_id,
             l_spabiz_ticket_staff_id,
             l_spabiz_ticket_voider_id,
             p_spabiz_ticket_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_ticket','proc_d_fact_spabiz_ticket end',@current_dv_batch_id
end
