CREATE PROC [dbo].[proc_util_admin_backup_configuration_tables] @dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--delete duplicates due to the workflow running multiple times
delete dv_d_etl_map_backup where dv_batch_id = @dv_batch_id
delete dv_etl_map_backup where dv_batch_id = @dv_batch_id
delete dv_job_dependency_backup where dv_batch_id = @dv_batch_id
delete dv_etl_parameter_backup where dv_batch_id = @dv_batch_id

insert into dbo.dv_d_etl_map_backup
(
  dv_d_etl_map_id,
  target_object,
  target_column,
  data_type,
  source_sql,
  partition_scheme,
  release,
  view_schema,
  dv_inserted_date_time,
  dv_insert_user,
  dv_updated_date_time,
  dv_update_user,
  dv_batch_id,
  dv_backup_date_time,
  distribution_type
)
select dv_d_etl_map_id,
       target_object,
       target_column,
       data_type,
       source_sql,
       partition_scheme,
       release,
       view_schema,
       dv_inserted_date_time,
       dv_insert_user,
       dv_updated_date_time,
       dv_update_user,
       @dv_batch_id,
       getdate(),
       distribution_type
  from dbo.dv_d_etl_map

insert into dbo.dv_etl_map_backup
(
  dv_etl_map_id,
  dv_table,
  dv_column,
  source,
  source_table,
  source_column,
  data_type,
  sort_order,
  business_key_sort_order,
  comments,
  release,
  partition_scheme,
  is_truncated_staging,
  historical_pit_flag,
  dv_inserted_date_time,
  dv_insert_user,
  dv_updated_date_time,
  dv_update_user,
  greatest_satellite_date_time_type,
  dv_batch_id,
  dv_backup_date_time
)
select dv_etl_map_id,
       dv_table,
       dv_column,
       source,
       source_table,
       source_column,
       data_type,
       sort_order,
       business_key_sort_order,
       comments,
       release,
       partition_scheme,
       is_truncated_staging,
       historical_pit_flag,
       dv_inserted_date_time,
       dv_insert_user,
       dv_updated_date_time,
       dv_update_user,
       greatest_satellite_date_time_type,
       @dv_batch_id,
       getdate()
  from dbo.dv_etl_map

insert into dbo.dv_job_dependency_backup
(
  dv_job_dependency_id,
  dv_job_status_id,
  dependent_on_dv_job_status_id,
  dv_inserted_date_time,
  dv_insert_user,
  dv_updated_date_time,
  dv_update_user,
  dv_batch_id,
  dv_backup_date_time
)
select dv_job_dependency_id,
       dv_job_status_id,
       dependent_on_dv_job_status_id,
       dv_inserted_date_time,
       dv_insert_user,
       dv_updated_date_time,
       dv_update_user,
       @dv_batch_id,
       getdate()
  from dbo.dv_job_dependency

insert into dbo.dv_etl_parameter_backup
(
  dv_etl_parameter_id,
  job_group,
  parameter_set,
  parameter_name,
  parameter_value,
  dv_inserted_date_time,
  dv_insert_user,
  dv_updated_date_time,
  dv_update_user,
  dv_batch_id,
  dv_backup_date_time
)
select dv_etl_parameter_id,
       job_group,
       parameter_set,
       parameter_name,
       parameter_value,
       dv_inserted_date_time,
       dv_insert_user,
       dv_updated_date_time,
       dv_update_user,
       @dv_batch_id,
       getdate()
  from dbo.dv_etl_parameter

end
