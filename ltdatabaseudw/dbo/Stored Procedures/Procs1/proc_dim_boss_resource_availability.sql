CREATE PROC [dbo].[proc_dim_boss_resource_availability] @dv_batch_id [varchar](500) AS
  BEGIN
  SET XACT_ABORT ON
  SET NOCOUNT ON

   IF object_id('tempdb..#etl_step1') IS NOT NULL
   DROP TABLE #etl_step1

   CREATE TABLE dbo.#etl_step1
   WITH (
   distribution = HASH (dim_boss_resource_availability_key)
   ,location = user_db
   ) AS
   
   SELECT d_boss_asi_available.bk_hash dim_boss_resource_availability_key
   ,d_boss_asi_available.club_d_boss_asi_club_res_bk_hash as dim_club_key
   ,d_boss_asi_resource.d_boss_asi_resource_id d_boss_asi_resource_id
   ,d_boss_asi_resource.resource_type resource_type                                             
   ,d_boss_asi_club_res.resource resource
   ,d_boss_employees.employee_id employee_id
   ,d_boss_employees.bk_hash dim_employee_key
   ,d_boss_asi_club_res.capacity capacity
   ,d_boss_asi_club_res.status status
   ,d_boss_asi_available.start_dim_date_key start_dim_date_key
   ,d_boss_asi_available.start_dim_time_key start_dim_time_key
   ,d_boss_asi_available.end_dim_date_key end_dim_date_key
   ,d_boss_asi_available.end_dim_time_key end_dim_time_key
   ,CASE 
   WHEN isnull(d_boss_asi_available.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753')
       AND isnull(d_boss_asi_available.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753')
   AND isnull(d_boss_asi_available.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_employees.dv_load_date_time, 'Jan 1, 1753')
   THEN isnull(d_boss_asi_available.dv_load_date_time, 'Jan 1, 1753')
   WHEN isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753')
       AND isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_employees.dv_load_date_time, 'Jan 1, 1753')
   THEN isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753')
       WHEN isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_employees.dv_load_date_time, 'Jan 1, 1753')
   THEN isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753')
   ELSE isnull(d_boss_employees.dv_load_date_time, 'Jan 1, 1753')
   END dv_load_date_time
   ,convert(DATETIME, '99991231', 112) dv_load_end_date_time
   , CASE 
   WHEN isnull(d_boss_asi_available.dv_batch_id, -1) >= isnull(d_boss_asi_resource.dv_batch_id, -1)
       AND isnull(d_boss_asi_available.dv_batch_id, -1) >= isnull(d_boss_asi_club_res.dv_batch_id, -1)
   AND isnull(d_boss_asi_available.dv_batch_id, -1) >= isnull(d_boss_employees.dv_batch_id, -1)
   THEN isnull(d_boss_asi_available.dv_batch_id, -1)
   WHEN isnull(d_boss_asi_resource.dv_batch_id, - 1) >= isnull(d_boss_asi_club_res.dv_batch_id, - 1)
       AND isnull(d_boss_asi_resource.dv_batch_id, - 1) >= isnull(d_boss_employees.dv_batch_id, - 1)
   THEN isnull(d_boss_asi_resource.dv_batch_id, - 1)
   WHEN isnull(d_boss_asi_club_res.dv_batch_id, - 1) >= isnull(d_boss_employees.dv_batch_id, - 1)
   THEN isnull(d_boss_asi_club_res.dv_batch_id, - 1)
   ELSE isnull(d_boss_employees.dv_batch_id, - 1)
   END dv_batch_id
   FROM dbo.d_boss_asi_available
           join dbo.d_boss_asi_club_res
           on d_boss_asi_available.resource_id = d_boss_asi_club_res.resource_id
           join dbo.d_boss_asi_resource on d_boss_asi_club_res.d_boss_asi_resource_bk_hash = d_boss_asi_resource.bk_hash
           left join dbo.d_boss_employees on d_boss_employees.id = d_boss_asi_club_res.empl_id
   /* Delete and re-insert as a single transaction*/
   /*   Delete records from the table that exist*/
   /*   Insert records from records from current and missing batches*/
   BEGIN TRAN
   
   DELETE dbo.dim_boss_resource_availability
   WHERE dim_boss_resource_availability_key IN (
   SELECT dim_boss_resource_availability_key
   FROM dbo.#etl_step1
   )
   
   INSERT INTO dim_boss_resource_availability (
    dim_boss_resource_availability_key
    ,dim_club_key
    ,d_boss_asi_resource_id
    ,resource_type
    ,resource
    ,employee_id
    ,dim_employee_key
    ,capacity
    ,status
    ,start_dim_date_key
    ,start_dim_time_key
    ,end_dim_date_key
    ,end_dim_time_key
    ,dv_load_date_time
    ,dv_load_end_date_time
    ,dv_batch_id
    ,dv_inserted_date_time
    ,dv_insert_user
   )
   SELECT dim_boss_resource_availability_key
     ,dim_club_key
     ,d_boss_asi_resource_id
     ,resource_type
     ,resource
     ,employee_id
     ,dim_employee_key
     ,capacity
     ,status
     ,start_dim_date_key
     ,start_dim_time_key
	 ,end_dim_date_key
     ,end_dim_time_key
     ,dv_load_date_time
     ,dv_load_end_date_time
     ,dv_batch_id
     ,getdate()
     ,suser_sname()
     FROM #etl_step1

  COMMIT TRAN

  END

