CREATE PROC [dbo].[proc_d_dim_spabiz_service_charge_type] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type','proc_d_dim_spabiz_service_charge_type start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_spabiz_service_charge_type

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type','#p_spabiz_service_charge_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_service_charge_insert') is not null drop table #p_spabiz_service_charge_insert
create table dbo.#p_spabiz_service_charge_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_service_charge.p_spabiz_service_charge_id,
       p_spabiz_service_charge.bk_hash,
       row_number() over (order by p_spabiz_service_charge_id) row_num
  from dbo.p_spabiz_service_charge
  join #batch_id
    on p_spabiz_service_charge.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_service_charge.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_service_charge.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_service_charge_insert.row_num,
       p_spabiz_service_charge.bk_hash dim_spabiz_service_charge_type_key,
       p_spabiz_service_charge.service_charge_id service_charge_id,
       p_spabiz_service_charge.store_number store_number,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then null
             else delete_date
        end deleted_date_time,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_service_charge.service_charge_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then p_spabiz_service_charge.bk_hash
            when l_spabiz_service_charge.staff_id is null then '-998'
            when l_spabiz_service_charge.staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_service_charge.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_service_charge.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       s_spabiz_service_charge.edit_time edit_date_time,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_service_charge.enabled = 1 then 'Y'
            else 'N'
        end enabled_flag,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_service_charge.pay_commission = 1 then 'Y'
            else 'N'
        end pay_commission_flag,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then ''
             else l_spabiz_service_charge.quick_id
        end quick_id,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 0
            else s_spabiz_service_charge.amount
        end service_charge_amount,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 'N'
             when s_spabiz_service_charge.computed_on = 1 then 'Y'
             else 'N'
        end service_charge_computed_by_percent_flag,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_service_charge.display_name is null then ''
            else s_spabiz_service_charge.display_name
        end service_charge_display_name,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_service_charge.name is null then ''
            else s_spabiz_service_charge.name
        end service_charge_name,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 0
            else s_spabiz_service_charge.dollar_percent
        end service_charge_percent,
       case when p_spabiz_service_charge.bk_hash in ('-997','-998','-999') then 'N'
             when s_spabiz_service_charge.taxable = 1 then 'Y'
             else 'N'
        end taxable_flag,
       p_spabiz_service_charge.p_spabiz_service_charge_id,
       p_spabiz_service_charge.dv_batch_id,
       p_spabiz_service_charge.dv_load_date_time,
       p_spabiz_service_charge.dv_load_end_date_time
  from dbo.p_spabiz_service_charge
  join #p_spabiz_service_charge_insert
    on p_spabiz_service_charge.p_spabiz_service_charge_id = #p_spabiz_service_charge_insert.p_spabiz_service_charge_id
  join dbo.l_spabiz_service_charge
    on p_spabiz_service_charge.l_spabiz_service_charge_id = l_spabiz_service_charge.l_spabiz_service_charge_id
  join dbo.s_spabiz_service_charge
    on p_spabiz_service_charge.s_spabiz_service_charge_id = s_spabiz_service_charge.s_spabiz_service_charge_id
 where l_spabiz_service_charge.store_number not in (1,100,999) OR p_spabiz_service_charge.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_spabiz_service_charge_type', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_spabiz_service_charge_type
       where d_dim_spabiz_service_charge_type.dim_spabiz_service_charge_type_key in (select bk_hash from #p_spabiz_service_charge_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_spabiz_service_charge_type(
                 d_dim_spabiz_service_charge_type_id,
                 dim_spabiz_service_charge_type_key,
                 service_charge_id,
                 store_number,
                 deleted_date_time,
                 deleted_flag,
                 dim_spabiz_staff_key,
                 edit_date_time,
                 enabled_flag,
                 pay_commission_flag,
                 quick_id,
                 service_charge_amount,
                 service_charge_computed_by_percent_flag,
                 service_charge_display_name,
                 service_charge_name,
                 service_charge_percent,
                 taxable_flag,
                 p_spabiz_service_charge_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_spabiz_service_charge_type_key,
             service_charge_id,
             store_number,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_staff_key,
             edit_date_time,
             enabled_flag,
             pay_commission_flag,
             quick_id,
             service_charge_amount,
             service_charge_computed_by_percent_flag,
             service_charge_display_name,
             service_charge_name,
             service_charge_percent,
             taxable_flag,
             p_spabiz_service_charge_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_service_charge_type','proc_d_dim_spabiz_service_charge_type end',@current_dv_batch_id
end
