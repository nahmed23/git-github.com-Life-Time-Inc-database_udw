CREATE PROC [dbo].[proc_util_admin_job_status_set_next_begin_extract_date_time] @batch_id [varchar](256),@job_group [varchar](256) AS
begin

set nocount on
set xact_abort on

  /* deducting 2 hours from last_start_date to make sure of any overlap with the way replication is performed*/
  /* we are just using 6 hours and ignoring daylight/standard time differences*/
  declare @next_utc_begin_extract_date_time datetime = (select dateadd(hh, -2, last_start_date) from stage_spabiz_USER_SCHEDULER_JOBS where job_name = 'MVIEW_REFRESH_ALL' and dv_batch_id = @batch_id)
  declare @next_begin_extract_date_time datetime = (select dateadd(hh, -8, last_start_date) from stage_spabiz_USER_SCHEDULER_JOBS where job_name = 'MVIEW_REFRESH_ALL' and dv_batch_id = @batch_id)

  update dv_job_status
     set next_utc_begin_extract_date_time = @next_utc_begin_extract_date_time,
	     next_begin_extract_date_time = @next_begin_extract_date_time
   where source_name = 'spabiz'
     and job_group =  @job_group

end
