CREATE PROC [dbo].[proc_d_dim_spabiz_staff] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff','proc_d_dim_spabiz_staff start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_spabiz_staff

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff','#p_spabiz_staff_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_staff_insert') is not null drop table #p_spabiz_staff_insert
create table dbo.#p_spabiz_staff_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_staff.p_spabiz_staff_id,
       p_spabiz_staff.bk_hash,
       row_number() over (order by p_spabiz_staff_id) row_num
  from dbo.p_spabiz_staff
  join #batch_id
    on p_spabiz_staff.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_staff.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_staff.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_staff_insert.row_num,
       p_spabiz_staff.bk_hash dim_spabiz_staff_key,
       p_spabiz_staff.staff_id staff_id,
       p_spabiz_staff.store_number store_number,
       case when s_spabiz_staff.city is null then ''
            else s_spabiz_staff.city
        end address_city,
       case when s_spabiz_staff.address_1 is null then ''
            else s_spabiz_staff.address_1
        end address_line_1,
       case when s_spabiz_staff.address_2 is null then ''
            else s_spabiz_staff.address_2
        end address_line_2,
       case when s_spabiz_staff.state is null then ''
            else s_spabiz_staff.state
        end adress_state,
       case when s_spabiz_staff.anniversary is null then ''
            else s_spabiz_staff.anniversary
        end anniversary,
       s_spabiz_staff.ap_cycle_count appointment_cycle_count,
       case when s_spabiz_staff.b_day is null then ''
            else s_spabiz_staff.b_day
        end birthday,
       case when s_spabiz_staff.book_name is null then ''
            else s_spabiz_staff.book_name
        end book_name,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.can_use_system = 1 then 'Y'
            else 'N'
        end can_use_system_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when  s_spabiz_staff.clock_in_req= 1 then 'Y'
           else 'N'
       end clock_in_request_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_staff.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_staff.delete_date
        end deleted_date_time,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.staff_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then p_spabiz_staff.bk_hash
            when l_spabiz_staff.ass_commish_id is null then '-998'
            when l_spabiz_staff.ass_commish_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_staff.ass_commish_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_assistant_comission_key,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then p_spabiz_staff.bk_hash
            when l_spabiz_staff.dept_cat is null then '-998'  
            when l_spabiz_staff.dept_cat=0 then '-998' 	 
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_staff.dept_cat as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_category_key,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then l_spabiz_staff.bk_hash
            when l_spabiz_staff.primary_location is null then '-998'
            when l_spabiz_staff.primary_location = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_staff.primary_location as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_primary_store_key,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then l_spabiz_staff.bk_hash
            when l_spabiz_staff.store_number is null then '-998'
            when l_spabiz_staff.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_staff.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.do_not_print_prod = 1 then 'Y'
            else 'N'
        end do_not_print_productivity_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then null
             else s_spabiz_staff.edit_time
        end edit_date_time,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_staff.emp_end_date = convert(date, '18991230', 112) then null
            else s_spabiz_staff.emp_end_date
        end employee_end_date_time,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_staff.emp_start_date = convert(date, '18991230', 112) then null
            else s_spabiz_staff.emp_start_date
        end employee_start_date_time,
       case when s_spabiz_staff.f_l_name is null then ''
            else s_spabiz_staff.f_l_name
        end first_initial_last_name,
       case when s_spabiz_staff.f_name is null then ''
            else s_spabiz_staff.f_name
        end first_last_name,
       case when s_spabiz_staff.first_name is null then ''
            else s_spabiz_staff.first_name
        end first_name,
       's_spabiz_staff.sex_' + convert(varchar,convert(int,s_spabiz_staff.sex)) gender_dim_description_key,
       convert(int,s_spabiz_staff.sex) gender_id,
       case when s_spabiz_staff.tel_home is null then ''
            else s_spabiz_staff.tel_home
        end home_phone_number,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.is_admin = 1 then 'Y'
            else 'N'
        end is_admin_flag,
       case when s_spabiz_staff.last_name is null then ''
            else s_spabiz_staff.last_name
        end last_name,
       case when s_spabiz_staff.mi is null then ''
            else s_spabiz_staff.mi
        end middle_initial,
       case when s_spabiz_staff.tel_mobil is null then ''
            else s_spabiz_staff.tel_mobil
        end mobile_phone_number,
       case when s_spabiz_staff.name is null then ''
            else s_spabiz_staff.name
        end name,
       case when s_spabiz_staff.neill_id is null then ''
                  else s_spabiz_staff.neill_id
        end neill_id,
       case when s_spabiz_staff.tel_pager is null then ''
            else s_spabiz_staff.tel_pager
        end pager_number,
       s_spabiz_staff.pager_type pager_type,
       case when s_spabiz_staff.pop_up_info is null then ''
                  else s_spabiz_staff.pop_up_info
        end pop_up_information,
       case when s_spabiz_staff.zip is null then ''
            else s_spabiz_staff.zip
        end postal_code,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_1 = 1 then 'Y'
            else 'N'
        end print_1_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_2 = 1 then 'Y'
            else 'N'
        end print_2_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_3 = 1 then 'Y'
            else 'N'
        end print_3_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_4 = 1 then 'Y'
            else 'N'
        end print_4_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_5 = 1 then 'Y'
            else 'N'
        end print_5_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_6 = 1 then 'Y'
            else 'N'
        end print_6_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_7 = 1 then 'Y'
            else 'N'
        end print_7_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.print_traveler = 1 then 'Y'
            else 'N'
        end print_traveler_flag,
       case when s_spabiz_staff.quick_id is null then ''
            else s_spabiz_staff.quick_id
        end quick_id,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
             when s_spabiz_staff.type_of = 0 then 'Y'
             else 'N'
        end resource_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
             when s_spabiz_staff.type_of = 1 then 'Y'
             else 'N'
        end staff_flag,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_staff.start_ap_cycle = convert(date, '18991230', 112) then null
            else s_spabiz_staff.start_ap_cycle
        end start_appointment_cycle_date_time,
       s_spabiz_staff.wage wage,
       s_spabiz_staff.wage_per_min wage_per_minute,
       's_spabiz_staff.wage_type_' + convert(varchar,convert(int,s_spabiz_staff.wage_type)) wage_type_dim_description_key,
       convert(int,s_spabiz_staff.wage_type) wage_type_id,
       case when p_spabiz_staff.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_staff.web_book = 1 then 'Y'
            else 'N'
        end web_book_flag,
       case when s_spabiz_staff.tel_work is null then ''
            else s_spabiz_staff.tel_work
        end work_phone_number,
       p_spabiz_staff.p_spabiz_staff_id,
       p_spabiz_staff.dv_batch_id,
       p_spabiz_staff.dv_load_date_time,
       p_spabiz_staff.dv_load_end_date_time
  from dbo.p_spabiz_staff
  join #p_spabiz_staff_insert
    on p_spabiz_staff.p_spabiz_staff_id = #p_spabiz_staff_insert.p_spabiz_staff_id
  join dbo.l_spabiz_staff
    on p_spabiz_staff.l_spabiz_staff_id = l_spabiz_staff.l_spabiz_staff_id
  join dbo.s_spabiz_staff
    on p_spabiz_staff.s_spabiz_staff_id = s_spabiz_staff.s_spabiz_staff_id
 where l_spabiz_staff.store_number not in (1,100,999) OR p_spabiz_staff.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_spabiz_staff', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_spabiz_staff
       where d_dim_spabiz_staff.dim_spabiz_staff_key in (select bk_hash from #p_spabiz_staff_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_spabiz_staff(
                 d_dim_spabiz_staff_id,
                 dim_spabiz_staff_key,
                 staff_id,
                 store_number,
                 address_city,
                 address_line_1,
                 address_line_2,
                 adress_state,
                 anniversary,
                 appointment_cycle_count,
                 birthday,
                 book_name,
                 can_use_system_flag,
                 clock_in_request_flag,
                 deleted_date_time,
                 deleted_flag,
                 dim_spabiz_assistant_comission_key,
                 dim_spabiz_category_key,
                 dim_spabiz_primary_store_key,
                 dim_spabiz_store_key,
                 do_not_print_productivity_flag,
                 edit_date_time,
                 employee_end_date_time,
                 employee_start_date_time,
                 first_initial_last_name,
                 first_last_name,
                 first_name,
                 gender_dim_description_key,
                 gender_id,
                 home_phone_number,
                 is_admin_flag,
                 last_name,
                 middle_initial,
                 mobile_phone_number,
                 name,
                 neill_id,
                 pager_number,
                 pager_type,
                 pop_up_information,
                 postal_code,
                 print_1_flag,
                 print_2_flag,
                 print_3_flag,
                 print_4_flag,
                 print_5_flag,
                 print_6_flag,
                 print_7_flag,
                 print_traveler_flag,
                 quick_id,
                 resource_flag,
                 staff_flag,
                 start_appointment_cycle_date_time,
                 wage,
                 wage_per_minute,
                 wage_type_dim_description_key,
                 wage_type_id,
                 web_book_flag,
                 work_phone_number,
                 p_spabiz_staff_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_spabiz_staff_key,
             staff_id,
             store_number,
             address_city,
             address_line_1,
             address_line_2,
             adress_state,
             anniversary,
             appointment_cycle_count,
             birthday,
             book_name,
             can_use_system_flag,
             clock_in_request_flag,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_assistant_comission_key,
             dim_spabiz_category_key,
             dim_spabiz_primary_store_key,
             dim_spabiz_store_key,
             do_not_print_productivity_flag,
             edit_date_time,
             employee_end_date_time,
             employee_start_date_time,
             first_initial_last_name,
             first_last_name,
             first_name,
             gender_dim_description_key,
             gender_id,
             home_phone_number,
             is_admin_flag,
             last_name,
             middle_initial,
             mobile_phone_number,
             name,
             neill_id,
             pager_number,
             pager_type,
             pop_up_information,
             postal_code,
             print_1_flag,
             print_2_flag,
             print_3_flag,
             print_4_flag,
             print_5_flag,
             print_6_flag,
             print_7_flag,
             print_traveler_flag,
             quick_id,
             resource_flag,
             staff_flag,
             start_appointment_cycle_date_time,
             wage,
             wage_per_minute,
             wage_type_dim_description_key,
             wage_type_id,
             web_book_flag,
             work_phone_number,
             p_spabiz_staff_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_staff','proc_d_dim_spabiz_staff end',@current_dv_batch_id
end
