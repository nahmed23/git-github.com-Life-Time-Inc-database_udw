CREATE PROC [dbo].[proc_util_admin_generate_parameter_file] @job_group [varchar](255),@job_name [varchar](255),@blob_container_name [varchar](255) AS
begin

  set nocount on
  set xact_abort on

select '[global]' + char(13) + char(10) +
       '$$begin_extract_date_time=' + convert(varchar, begin_extract_date_time,120) + char(13) + char(10) +
       '$$utc_begin_extract_date_time=' + convert(varchar, utc_begin_extract_date_time,120) + char(13) + char(10) +
       '$$job_start_date_time=' + convert(varchar, job_start_date_time,120) + char(13) + char(10) +
	   '$$blob_container_name=' + @blob_container_name + char(13) + char(10) +
	   '$$job_group=' + @job_group + char(13) + char(10) +
       '$$batch_id=' + convert(varchar, dv_batch_id) + char(13) + char(10) parameters
  from dbo.dv_job_status
 where job_group = @job_group
   and job_name = @job_name

end


