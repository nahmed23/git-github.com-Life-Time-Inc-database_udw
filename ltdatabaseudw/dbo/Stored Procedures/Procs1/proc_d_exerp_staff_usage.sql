CREATE PROC [dbo].[proc_d_exerp_staff_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_staff_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_staff_usage_insert') is not null drop table #p_exerp_staff_usage_insert
create table dbo.#p_exerp_staff_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_staff_usage.p_exerp_staff_usage_id,
       p_exerp_staff_usage.bk_hash
  from dbo.p_exerp_staff_usage
 where p_exerp_staff_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_staff_usage.dv_batch_id > @max_dv_batch_id
        or p_exerp_staff_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_staff_usage.bk_hash,
       p_exerp_staff_usage.staff_usage_id staff_usage_id,
       l_exerp_staff_usage.booking_id booking_id,
       l_exerp_staff_usage.center_id center_id,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
            when l_exerp_staff_usage.booking_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_staff_usage.booking_id as varchar(4000)),'z#@$k%&P'))),2)   end d_exerp_booking_bk_hash,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
            when l_exerp_staff_usage.center_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_staff_usage.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
           when l_exerp_staff_usage.person_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_exerp_staff_usage.person_id, PATINDEX('%[0-9]%',l_exerp_staff_usage.person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2)
       end dim_employee_key,
       s_exerp_staff_usage.ets ets,
       l_exerp_staff_usage.person_id person_id,
       s_exerp_staff_usage.salary staff_usage_salary,
       s_exerp_staff_usage.state staff_usage_state,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
            when s_exerp_staff_usage.start_datetime is null then '-998'
         else convert(varchar, s_exerp_staff_usage.start_datetime, 112)    end start_dim_date_key,
       case when p_exerp_staff_usage.bk_hash in ('-997','-998','-999') then p_exerp_staff_usage.bk_hash
        when s_exerp_staff_usage.start_datetime is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_staff_usage.start_datetime,114), 1, 5),':','') end start_dim_time_key,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
            when s_exerp_staff_usage.stop_datetime is null then '-998'
         else convert(varchar, s_exerp_staff_usage.stop_datetime, 112)    end stop_dim_date_key,
       case when p_exerp_staff_usage.bk_hash in ('-997','-998','-999') then p_exerp_staff_usage.bk_hash
        when s_exerp_staff_usage.stop_datetime is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_staff_usage.stop_datetime,114), 1, 5),':','') end stop_dim_time_key,
       case when p_exerp_staff_usage.bk_hash in('-997', '-998', '-999') then p_exerp_staff_usage.bk_hash
           when l_exerp_staff_usage_1.substitute_of_person_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_exerp_staff_usage_1.substitute_of_person_id, PATINDEX('%[0-9]%',l_exerp_staff_usage_1.substitute_of_person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2)
       end substitute_of_dim_employee_key,
       isnull(h_exerp_staff_usage.dv_deleted,0) dv_deleted,
       p_exerp_staff_usage.p_exerp_staff_usage_id,
       p_exerp_staff_usage.dv_batch_id,
       p_exerp_staff_usage.dv_load_date_time,
       p_exerp_staff_usage.dv_load_end_date_time
  from dbo.h_exerp_staff_usage
  join dbo.p_exerp_staff_usage
    on h_exerp_staff_usage.bk_hash = p_exerp_staff_usage.bk_hash
  join #p_exerp_staff_usage_insert
    on p_exerp_staff_usage.bk_hash = #p_exerp_staff_usage_insert.bk_hash
   and p_exerp_staff_usage.p_exerp_staff_usage_id = #p_exerp_staff_usage_insert.p_exerp_staff_usage_id
  join dbo.l_exerp_staff_usage
    on p_exerp_staff_usage.bk_hash = l_exerp_staff_usage.bk_hash
   and p_exerp_staff_usage.l_exerp_staff_usage_id = l_exerp_staff_usage.l_exerp_staff_usage_id
  join dbo.l_exerp_staff_usage_1
    on p_exerp_staff_usage.bk_hash = l_exerp_staff_usage_1.bk_hash
   and p_exerp_staff_usage.l_exerp_staff_usage_1_id = l_exerp_staff_usage_1.l_exerp_staff_usage_1_id
  join dbo.s_exerp_staff_usage
    on p_exerp_staff_usage.bk_hash = s_exerp_staff_usage.bk_hash
   and p_exerp_staff_usage.s_exerp_staff_usage_id = s_exerp_staff_usage.s_exerp_staff_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_staff_usage
   where d_exerp_staff_usage.bk_hash in (select bk_hash from #p_exerp_staff_usage_insert)

  insert dbo.d_exerp_staff_usage(
             bk_hash,
             staff_usage_id,
             booking_id,
             center_id,
             d_exerp_booking_bk_hash,
             d_exerp_center_bk_hash,
             dim_employee_key,
             ets,
             person_id,
             staff_usage_salary,
             staff_usage_state,
             start_dim_date_key,
             start_dim_time_key,
             stop_dim_date_key,
             stop_dim_time_key,
             substitute_of_dim_employee_key,
             deleted_flag,
             p_exerp_staff_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         staff_usage_id,
         booking_id,
         center_id,
         d_exerp_booking_bk_hash,
         d_exerp_center_bk_hash,
         dim_employee_key,
         ets,
         person_id,
         staff_usage_salary,
         staff_usage_state,
         start_dim_date_key,
         start_dim_time_key,
         stop_dim_date_key,
         stop_dim_time_key,
         substitute_of_dim_employee_key,
         dv_deleted,
         p_exerp_staff_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_staff_usage)
--Done!
end
