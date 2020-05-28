CREATE PROC [dbo].[proc_dim_fitmetrix_appointment] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_fitmetrix_appointment)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl') is not null drop table #etl
create table dbo.#etl with(distribution=hash(dim_fitmetrix_appointment_key), location=user_db) as
select d_fitmetrix_api_appointments.dim_fitmetrix_appointment_key,
       d_fitmetrix_api_appointments.appointment_id,
       d_fitmetrix_api_appointments.cancelled_flag,
       d_fitmetrix_api_appointments.dim_boss_reservation_key,
	   d_fitmetrix_api_appointments.dim_exerp_booking_key,
       isnull(d_fitmetrix_api_facility_locations.dim_club_key, '-998') dim_club_key,
       isnull(d_fitmetrix_api_instructor.dim_employee_key,'-998') dim_employee_key,
       d_fitmetrix_api_appointments.dim_fitmetrix_activity_key,
       d_fitmetrix_api_appointments.dim_fitmetrix_instructor_key,
       d_fitmetrix_api_appointments.dim_fitmetrix_location_key,
       d_fitmetrix_api_appointments.dim_fitmetrix_location_resource_key,
       d_fitmetrix_api_appointments.end_dim_date_key,
       d_fitmetrix_api_appointments.end_dim_time_key,
       d_fitmetrix_api_appointments.instructor_name,
       d_fitmetrix_api_appointments.max_capacity,
       d_fitmetrix_api_appointments.name,
       d_fitmetrix_api_appointments.start_dim_date_key,
       d_fitmetrix_api_appointments.start_dim_time_key,
       d_fitmetrix_api_appointments.total_booked,
       d_fitmetrix_api_appointments.wait_list_available_flag,
       d_fitmetrix_api_appointments.dv_load_date_time,
       d_fitmetrix_api_appointments.dv_load_end_date_time,
       d_fitmetrix_api_appointments.dv_batch_id
  from d_fitmetrix_api_appointments
  left join d_fitmetrix_api_facility_locations
    on d_fitmetrix_api_appointments.dim_fitmetrix_location_key = d_fitmetrix_api_facility_locations.dim_fitmetrix_location_key
  left join d_fitmetrix_api_instructor
    on d_fitmetrix_api_appointments.dim_fitmetrix_instructor_key = d_fitmetrix_api_instructor.dim_fitmetrix_instructor_key
 where d_fitmetrix_api_appointments.dv_batch_id >= @load_dv_batch_id

/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.dim_fitmetrix_appointment
   where dim_fitmetrix_appointment_key in (select dim_fitmetrix_appointment_key from dbo.#etl) 
   
  insert into dim_fitmetrix_appointment
        (dim_fitmetrix_appointment_key,
         appointment_id,
         cancelled_flag,
         dim_boss_reservation_key,
		 dim_exerp_booking_key,
         dim_club_key,
         dim_employee_key,
         dim_fitmetrix_activity_key,
         dim_fitmetrix_instructor_key,
         dim_fitmetrix_location_key,
         dim_fitmetrix_location_resource_key,
         end_dim_date_key,
         end_dim_time_key,
         instructor_name,
         max_capacity,
         name,
         start_dim_date_key,
         start_dim_time_key,
         total_booked,
         wait_list_available_flag,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_fitmetrix_appointment_key,
         appointment_id,
         cancelled_flag,
         dim_boss_reservation_key,
		 dim_exerp_booking_key,
         dim_club_key,
         dim_employee_key,
         dim_fitmetrix_activity_key,
         dim_fitmetrix_instructor_key,
         dim_fitmetrix_location_key,
         dim_fitmetrix_location_resource_key,
         end_dim_date_key,
         end_dim_time_key,
         instructor_name,
         max_capacity,
         name,
         start_dim_date_key,
         start_dim_time_key,
         total_booked,
         wait_list_available_flag,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl
 
commit tran

end

