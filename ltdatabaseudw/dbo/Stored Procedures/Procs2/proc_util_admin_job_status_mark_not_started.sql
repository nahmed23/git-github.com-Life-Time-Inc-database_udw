CREATE PROC [dbo].[proc_util_admin_job_status_mark_not_started] @job_group [varchar](256),@job_name [varchar](256) AS
begin


  set nocount on
  set xact_abort on

  /* Set the next extract to be two minutes prior to the current time to allow for minor differences between system clocks*/
  declare @next_utc_begin_extract_date_time datetime = (select dateadd(mi,-2,getutcdate()))

  /* Get the year portion of the next extract*/
  declare @year int = (select datepart(yyyy, @next_utc_begin_extract_date_time))

  /* Determine the utc date/time of the switch to daylight saving time for the year of the next extract*/
  declare @utc_date_time_switch_to_daylight_saving_time datetime = (
  select dateadd(hh,8,calendar_date)
    from dbo.dim_date
   where year = @year
     and month_name = 'March'
     and day_of_week_name = 'Sunday'
     and day_number_in_month >= 8
     and day_number_in_month <= 14)

  /* Determine the utc date of the switch back to standard time for the year of the next extract*/
  declare @utc_date_time_switch_back_to_standard_time datetime = (
  select dateadd(hh,7,calendar_date)
    from dbo.dim_date
   where year = @year
     and month_name = 'November'
     and day_of_week_name = 'Sunday'
     and day_number_in_month <= 7)

  /* Convert the UTC next extract date/time to Central time*/
  declare @next_begin_extract_date_time datetime = (select case when @next_utc_begin_extract_date_time < @utc_date_time_switch_to_daylight_saving_time then dateadd(hh,-6,@next_utc_begin_extract_date_time)
                                                         when @next_utc_begin_extract_date_time >= @utc_date_time_switch_back_to_standard_time then dateadd(hh,-6,@next_utc_begin_extract_date_time)
                                                         else dateadd(hh,-5,@next_utc_begin_extract_date_time)
                                                     end)

  /* If the date/time is between 01:00 and 02:00 on the date of the switch back to standard time*/
  /* then set the extract time to 01:00.  This is because the fall back causes the hour to be repeated*/
  /* so we need to be sure to get the records from the second time through that hour.*/
  if convert(date, @next_begin_extract_date_time) = convert(date, @utc_date_time_switch_to_daylight_saving_time)
     and datepart(hh, @next_begin_extract_date_time) = 1
    begin
      set @next_begin_extract_date_time = dateadd(hh, 1, convert(datetime,convert(date, @next_begin_extract_date_time)))
    end

  /* Determine the previous master job status.  This is the dv_job_status record where job_name = @job_name*/
  declare @previous_master_job_status varchar(50) = (select job_status from dbo.dv_job_status where job_group = @job_group and job_name = @job_name)

  /* Update the dv_job_status records where the job_group = @job_group*/
  declare @suser_sname varchar(100) = suser_sname()

  update dbo.dv_job_status
     set job_start_date_time = null,
         job_end_date_time = null,
         job_status = 'Not Started',
         begin_extract_date_time = case when @previous_master_job_status = 'Complete' and job_status = 'Complete' 
                                            and source_name in ('chronotrack','athlinks') then dateadd(day,-10,next_begin_extract_date_time)
	                                    when @previous_master_job_status = 'Complete' and job_status = 'Complete' 
	                                    then next_begin_extract_date_time
                                   else begin_extract_date_time end,
         utc_begin_extract_date_time = case when @previous_master_job_status = 'Complete' and job_status = 'Complete' 
                                                 and source_name in ('chronotrack','athlinks') then dateadd(day,-10,next_utc_begin_extract_date_time)
	                                        when @previous_master_job_status = 'Complete' and job_status = 'Complete' 
	                                        then next_utc_begin_extract_date_time
                                         else utc_begin_extract_date_time end,
         next_begin_extract_date_time = @next_begin_extract_date_time,
         next_utc_begin_extract_date_time = @next_utc_begin_extract_date_time,
         dv_batch_id = replace(replace(replace(convert(varchar, getutcdate(),120 ), '-', ''),' ', ''), ':', ''),
         dv_updated_date_time = getdate(),
         dv_update_user = @suser_sname
   where job_group = @job_group

end

