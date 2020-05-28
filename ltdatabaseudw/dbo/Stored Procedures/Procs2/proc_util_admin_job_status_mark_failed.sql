CREATE PROC [dbo].[proc_util_admin_job_status_mark_failed] @job_group [varchar](256),@job_name [varchar](256) AS
begin

  set xact_abort on
  set nocount on

  -- Get the year portion of the current UTC date/time
  declare @current_utc_date_time datetime = getutcdate()

  -- Get the year portion of the current UTC date/time
  declare @year int = (select datepart(yyyy, @current_utc_date_time))

  -- Determine the utc date/time of the switch to daylight saving time for the year of the current UTC date/time
  declare @utc_date_time_switch_to_daylight_saving_time datetime = (
  select dateadd(hh,8,calendar_date)
    from dbo.dim_date
   where year = @year
     and month_name = 'March'
     and day_of_week_name = 'Sunday'
     and day_number_in_month >= 8
     and day_number_in_month <= 14)

  -- Determine the utc date of the switch back to standard time for the year of the current UTC date/time
  declare @utc_date_time_switch_back_to_standard_time datetime = (
  select dateadd(hh,7,calendar_date)
    from dbo.dim_date
   where year = @year
     and month_name = 'November'
     and day_of_week_name = 'Sunday'
     and day_number_in_month <= 7)

  -- Convert the current UTC date/time to Central time
  declare @current_date_time datetime = (select case when @current_utc_date_time < @utc_date_time_switch_to_daylight_saving_time then dateadd(hh,-6,@current_utc_date_time)
                                                     when @current_utc_date_time >= @utc_date_time_switch_back_to_standard_time then dateadd(hh,-6,@current_utc_date_time)
                                                     else dateadd(hh,-5,@current_utc_date_time)
                                                 end)

  declare @suser_sname varchar(100) = suser_sname()

  update dbo.dv_job_status
     set job_status = case when retry_flag = 1 then 'Failed'
                           else 'Failed and No Retry' end,
         job_end_date_time = @current_date_time,
         dv_updated_date_time = getdate(),
         dv_update_user = @suser_sname
   where job_group = @job_group
     and job_name = @job_name

end
