CREATE PROC [dbo].[proc_d_boss_asi_reserv] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_reserv)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_reserv_insert') is not null drop table #p_boss_asi_reserv_insert
create table dbo.#p_boss_asi_reserv_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_reserv.p_boss_asi_reserv_id,
       p_boss_asi_reserv.bk_hash
  from dbo.p_boss_asi_reserv
 where p_boss_asi_reserv.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_reserv.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_reserv.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_reserv.bk_hash,
       p_boss_asi_reserv.bk_hash dim_boss_reservation_key,
       p_boss_asi_reserv.reservation reservation_id,
       s_boss_asi_reserv.age_high age_high,
       s_boss_asi_reserv.age_low age_low,
       s_boss_asi_reserv.allow_wait_list allow_wait_list,
       s_boss_asi_reserv.billing_count billing_count,
       isnull(s_boss_asi_reserv.comment,'') comment,
       s_boss_asi_reserv.continuous continuous,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.create_date is null then '-998'
       	else convert(varchar, s_boss_asi_reserv.create_date, 112) 
       end created_dim_date_key,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.create_date is null then '-998'
       	else '1' + replace(substring(convert(varchar,s_boss_asi_reserv.create_date,114), 1, 5),':','')
       end created_dim_time_key,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
    when l_boss_asi_reserv.club is null then '-998'
	when l_boss_asi_reserv.resource_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_reserv.club as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_boss_asi_reserv.resource_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_boss_asi_club_res_bk_hash,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.format_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_reserv.format_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_product_format_bk_hash,
       case when charindex('6', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_friday_flag,
       case when charindex('2', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_monday_flag,
       case when charindex('7', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_saturday_flag,
       case when charindex('1', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_sunday_flag,
       case when charindex('5', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_thursday_flag,
       case when charindex('3', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_tuesday_flag,
       case when charindex('4', s_boss_asi_reserv.day_plan_ints) != 0 then 'Y'
       else 'N' end day_of_week_wednesday_flag,
       s_boss_asi_reserv.day_plan_ints day_plan_ints,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.upc_code is null then '-998'    
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ltrim(rtrim(l_boss_asi_reserv.upc_code)) as char(15)),'z#@$k%&P'))),2) end dim_boss_product_key,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.club is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_reserv.club)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_club_key,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.trainer_cust_code is null then '-998'
       	when ltrim(rtrim(l_boss_asi_reserv.trainer_cust_code)) ='' then '-998'
       	when l_boss_asi_reserv.trainer_cust_code = 0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_reserv.trainer_cust_code)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_employee_key,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.mms_product_id is null then '-998' 
       	when ltrim(rtrim(l_boss_asi_reserv.mms_product_id))='' then '-998' 
           when PATINDEX('%[^a-zA-Z0-9]%', ltrim(rtrim(l_boss_asi_reserv.mms_product_id))) > 0 then '-998'     
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_reserv.mms_product_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_product_key,
       h_boss_asi_reserv.dv_deleted dv_deleted_flag,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.end_date is null then '-998'
       	else convert(varchar, s_boss_asi_reserv.end_date, 112) 
       end end_dim_date_key,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.end_date is null then '-998'
       	else '1' + replace(substring(convert(varchar,s_boss_asi_reserv.end_date,114), 1, 5),':','')
       end end_dim_time_key,
       l_boss_asi_reserv.format_id format_id,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.free_date is null then '-998'
       	else convert(varchar, s_boss_asi_reserv.free_date, 112) 
       end free_dim_date_key,
       s_boss_asi_reserv.grace_days grace_days,
       s_boss_asi_reserv.instructor_expense instructor_expense,
       case when p_boss_asi_reserv.bk_hash in ('-997','-998','-999') then 0
            when s_boss_asi_reserv.published_duration > 0 then s_boss_asi_reserv.published_duration
            else datediff(mi, s_boss_asi_reserv.start_date, s_boss_asi_reserv.end_date)%(60*24)
       end length_in_minutes,
       s_boss_asi_reserv.limit limit,
       s_boss_asi_reserv.min_limit limit_minimum,
       case when p_boss_asi_reserv.bk_hash in ('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when l_boss_asi_reserv.link_to is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_reserv.link_to)) as int) as varchar(500)),'z#@$k%&P'))),2) end link_to_dim_boss_reservation_key,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.last_modified is null then '-998'
       	else convert(varchar, s_boss_asi_reserv.last_modified, 112) 
       end modified_dim_date_key,
       s_boss_asi_reserv.non_mbr_price non_member_price,
       isnull(s_boss_asi_reserv.print_desc,'') print_description,
       s_boss_asi_reserv.program_id program_id,
       s_boss_asi_reserv.publish publish_flag,
       s_boss_asi_reserv.status reservation_status,
       s_boss_asi_reserv.reserve_type reservation_type,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.start_date is null then '-998'
       	else convert(varchar, s_boss_asi_reserv.start_date, 112) 
       end start_dim_date_key,
       case when p_boss_asi_reserv.bk_hash in('-997', '-998', '-999') then p_boss_asi_reserv.bk_hash
           when s_boss_asi_reserv.start_date is null then '-998'
       	else '1' + replace(substring(convert(varchar,s_boss_asi_reserv.start_date,114), 1, 5),':','')
       end start_dim_time_key,
       s_boss_asi_reserv.use_for_LT_Bucks use_for_lt_bucks_flag,
       s_boss_asi_reserv.waiver_reqd waiver_required_flag,
       isnull(s_boss_asi_reserv.web_register, 'N') web_register_flag,
       isnull(h_boss_asi_reserv.dv_deleted,0) dv_deleted,
       p_boss_asi_reserv.p_boss_asi_reserv_id,
       p_boss_asi_reserv.dv_batch_id,
       p_boss_asi_reserv.dv_load_date_time,
       p_boss_asi_reserv.dv_load_end_date_time
  from dbo.h_boss_asi_reserv
  join dbo.p_boss_asi_reserv
    on h_boss_asi_reserv.bk_hash = p_boss_asi_reserv.bk_hash
  join #p_boss_asi_reserv_insert
    on p_boss_asi_reserv.bk_hash = #p_boss_asi_reserv_insert.bk_hash
   and p_boss_asi_reserv.p_boss_asi_reserv_id = #p_boss_asi_reserv_insert.p_boss_asi_reserv_id
  join dbo.l_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = l_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.l_boss_asi_reserv_id = l_boss_asi_reserv.l_boss_asi_reserv_id
  join dbo.s_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = s_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.s_boss_asi_reserv_id = s_boss_asi_reserv.s_boss_asi_reserv_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_reserv
   where d_boss_asi_reserv.bk_hash in (select bk_hash from #p_boss_asi_reserv_insert)

  insert dbo.d_boss_asi_reserv(
             bk_hash,
             dim_boss_reservation_key,
             reservation_id,
             age_high,
             age_low,
             allow_wait_list,
             billing_count,
             comment,
             continuous,
             created_dim_date_key,
             created_dim_time_key,
             d_boss_asi_club_res_bk_hash,
             d_boss_product_format_bk_hash,
             day_of_week_friday_flag,
             day_of_week_monday_flag,
             day_of_week_saturday_flag,
             day_of_week_sunday_flag,
             day_of_week_thursday_flag,
             day_of_week_tuesday_flag,
             day_of_week_wednesday_flag,
             day_plan_ints,
             dim_boss_product_key,
             dim_club_key,
             dim_employee_key,
             dim_mms_product_key,
             dv_deleted_flag,
             end_dim_date_key,
             end_dim_time_key,
             format_id,
             free_dim_date_key,
             grace_days,
             instructor_expense,
             length_in_minutes,
             limit,
             limit_minimum,
             link_to_dim_boss_reservation_key,
             modified_dim_date_key,
             non_member_price,
             print_description,
             program_id,
             publish_flag,
             reservation_status,
             reservation_type,
             start_dim_date_key,
             start_dim_time_key,
             use_for_lt_bucks_flag,
             waiver_required_flag,
             web_register_flag,
             deleted_flag,
             p_boss_asi_reserv_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_boss_reservation_key,
         reservation_id,
         age_high,
         age_low,
         allow_wait_list,
         billing_count,
         comment,
         continuous,
         created_dim_date_key,
         created_dim_time_key,
         d_boss_asi_club_res_bk_hash,
         d_boss_product_format_bk_hash,
         day_of_week_friday_flag,
         day_of_week_monday_flag,
         day_of_week_saturday_flag,
         day_of_week_sunday_flag,
         day_of_week_thursday_flag,
         day_of_week_tuesday_flag,
         day_of_week_wednesday_flag,
         day_plan_ints,
         dim_boss_product_key,
         dim_club_key,
         dim_employee_key,
         dim_mms_product_key,
         dv_deleted_flag,
         end_dim_date_key,
         end_dim_time_key,
         format_id,
         free_dim_date_key,
         grace_days,
         instructor_expense,
         length_in_minutes,
         limit,
         limit_minimum,
         link_to_dim_boss_reservation_key,
         modified_dim_date_key,
         non_member_price,
         print_description,
         program_id,
         publish_flag,
         reservation_status,
         reservation_type,
         start_dim_date_key,
         start_dim_time_key,
         use_for_lt_bucks_flag,
         waiver_required_flag,
         web_register_flag,
         dv_deleted,
         p_boss_asi_reserv_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_reserv)
--Done!
end
