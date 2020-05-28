CREATE PROC [dbo].[proc_d_exerp_booking] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_booking)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_booking_insert') is not null drop table #p_exerp_booking_insert
create table dbo.#p_exerp_booking_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking.p_exerp_booking_id,
       p_exerp_booking.bk_hash
  from dbo.p_exerp_booking
 where p_exerp_booking.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_booking.dv_batch_id > @max_dv_batch_id
        or p_exerp_booking.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking.bk_hash,
       p_exerp_booking.booking_id booking_id,
       s_exerp_booking_1.age_text age_text,
       s_exerp_booking.name booking_name,
       s_exerp_booking.state booking_state,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.cancel_datetime is null then '-998'
       else convert(varchar, s_exerp_booking.cancel_datetime, 112) end cancel_dim_date_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.cancel_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking.cancel_datetime,114), 1, 5),':','') end cancel_dim_time_key,
       s_exerp_booking.cancel_reason cancel_reason,
       s_exerp_booking.class_capacity class_capacity,
       s_exerp_booking.color color,
       isnull(s_exerp_booking.comment,'') comment,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.creation_datetime is null then '-998'
       else convert(varchar, s_exerp_booking.creation_datetime, 112) end creation_dim_date_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.creation_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking.creation_datetime,114), 1, 5),':','') end creation_dim_time_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash     
         when l_exerp_booking.activity_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_booking.activity_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_activity_bk_hash,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash     
         when l_exerp_booking.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_booking.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       s_exerp_booking.description description,
       s_exerp_booking.ets ets,
       l_exerp_booking.main_booking_id main_booking_id,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash     
         when l_exerp_booking.main_booking_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_booking.main_booking_id as varchar(4000)),'z#@$k%&P'))),2)   end main_d_exerp_booking_bk_hash,
       s_exerp_booking.max_capacity_override max_capacity_override,
       s_exerp_booking_1.maximum_age maximum_age,
       s_exerp_booking_1.maximum_age_unit maximum_age_unit,
       s_exerp_booking_1.minimum_age minimum_age,
       s_exerp_booking_1.minimum_age_unit minimum_age_unit,
       case when s_exerp_booking_1.single_cancellation = 1 then 'Y'     else 'N'     end single_cancellation_flag,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.start_datetime is null then '-998'
       else convert(varchar, s_exerp_booking.start_datetime, 112) end start_dim_date_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.start_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking.start_datetime,114), 1, 5),':','') end start_dim_time_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.stop_datetime is null then '-998'
       else convert(varchar, s_exerp_booking.stop_datetime, 112) end stop_dim_date_key,
       case when p_exerp_booking.bk_hash in ('-997','-998','-999') then p_exerp_booking.bk_hash
       when s_exerp_booking.stop_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking.stop_datetime,114), 1, 5),':','')  end stop_dim_time_key,
       s_exerp_booking_1.strict_age_limit strict_age_limit,
       s_exerp_booking.waiting_list_capacity waiting_list_capacity,
       isnull(h_exerp_booking.dv_deleted,0) dv_deleted,
       p_exerp_booking.p_exerp_booking_id,
       p_exerp_booking.dv_batch_id,
       p_exerp_booking.dv_load_date_time,
       p_exerp_booking.dv_load_end_date_time
  from dbo.h_exerp_booking
  join dbo.p_exerp_booking
    on h_exerp_booking.bk_hash = p_exerp_booking.bk_hash
  join #p_exerp_booking_insert
    on p_exerp_booking.bk_hash = #p_exerp_booking_insert.bk_hash
   and p_exerp_booking.p_exerp_booking_id = #p_exerp_booking_insert.p_exerp_booking_id
  join dbo.l_exerp_booking
    on p_exerp_booking.bk_hash = l_exerp_booking.bk_hash
   and p_exerp_booking.l_exerp_booking_id = l_exerp_booking.l_exerp_booking_id
  join dbo.s_exerp_booking
    on p_exerp_booking.bk_hash = s_exerp_booking.bk_hash
   and p_exerp_booking.s_exerp_booking_id = s_exerp_booking.s_exerp_booking_id
  join dbo.s_exerp_booking_1
    on p_exerp_booking.bk_hash = s_exerp_booking_1.bk_hash
   and p_exerp_booking.s_exerp_booking_1_id = s_exerp_booking_1.s_exerp_booking_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_booking
   where d_exerp_booking.bk_hash in (select bk_hash from #p_exerp_booking_insert)

  insert dbo.d_exerp_booking(
             bk_hash,
             booking_id,
             age_text,
             booking_name,
             booking_state,
             cancel_dim_date_key,
             cancel_dim_time_key,
             cancel_reason,
             class_capacity,
             color,
             comment,
             creation_dim_date_key,
             creation_dim_time_key,
             d_exerp_activity_bk_hash,
             d_exerp_center_bk_hash,
             description,
             ets,
             main_booking_id,
             main_d_exerp_booking_bk_hash,
             max_capacity_override,
             maximum_age,
             maximum_age_unit,
             minimum_age,
             minimum_age_unit,
             single_cancellation_flag,
             start_dim_date_key,
             start_dim_time_key,
             stop_dim_date_key,
             stop_dim_time_key,
             strict_age_limit,
             waiting_list_capacity,
             deleted_flag,
             p_exerp_booking_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         booking_id,
         age_text,
         booking_name,
         booking_state,
         cancel_dim_date_key,
         cancel_dim_time_key,
         cancel_reason,
         class_capacity,
         color,
         comment,
         creation_dim_date_key,
         creation_dim_time_key,
         d_exerp_activity_bk_hash,
         d_exerp_center_bk_hash,
         description,
         ets,
         main_booking_id,
         main_d_exerp_booking_bk_hash,
         max_capacity_override,
         maximum_age,
         maximum_age_unit,
         minimum_age,
         minimum_age_unit,
         single_cancellation_flag,
         start_dim_date_key,
         start_dim_time_key,
         stop_dim_date_key,
         stop_dim_time_key,
         strict_age_limit,
         waiting_list_capacity,
         dv_deleted,
         p_exerp_booking_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_booking)
--Done!
end
