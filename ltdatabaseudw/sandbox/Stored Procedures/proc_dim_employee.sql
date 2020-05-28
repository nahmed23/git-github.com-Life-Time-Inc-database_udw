CREATE PROC [sandbox].[proc_dim_employee] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT dim_employee.[employee_id]
     , dim_club.[club_id]
     , dim_employee.[member_id]
     , [active_status_flag] = dim_employee.[active_status]
     , dim_employee.[first_name]
     , dim_employee.[last_name]
     , dim_employee.[middle_name]
     , dim_employee.[hire_date]
     , dim_employee.[termination_date]
     , [business_title]        = CONVERT(varchar(100), dim_emp_job_title.[business_title])
     , [employee_type]         = CONVERT(varchar(30), 'absent')
     , [employment_status]     = LOWER(CONVERT(char(1), dim_employee.[cf_employment_status]))
     , [family]                = CONVERT(varchar(50), dim_emp_job_title.[family])
     , [inserted_date_time]    = dim_employee.[inserted_date_time]
     , [level]                 = CONVERT(varchar(20), dim_emp_job_title.[level])
     , [marketing_title]       = CONVERT(varchar(100), dim_emp_job_title.[marketing_title])
     , [position_type]         = CONVERT(varchar(15), 'absent')
     , [primary_job_code]      = CONVERT(varchar(8), dim_emp_job_title.[job_code])
     , [profile]               = CONVERT(varchar(100), dim_emp_job_title.[profile])
     , [status_effective_date] = CONVERT(datetime, null)
     , [sub_family]            = CONVERT(varchar(100), dim_emp_job_title.[sub_family])
     , [updated_date_time]     = dim_employee.[updated_date_time]
     , [user_name]             = CONVERT(varchar(20), 'absent')
     , [work_email]            = CONVERT(varchar(100), dim_employee.[primary_work_email])
     , [work_phone_number]     = CONVERT(varchar(20), dim_employee.[phone_number])
     , [manager_employee_id]   = dim_employee.[manager_id]
     , [cost_center_id]        = CONVERT(varchar(6), 'absent')
     , [cost_center_name]      = CONVERT(varchar(100), 'absent')
     , [offering_id]           = CONVERT(varchar(10), 'absent')
     , [offering_name]         = CONVERT(varchar(50), 'absent')
     , [supervisory_org_id]    = CONVERT(varchar(50), 'absent')
     , [supervisory_org_name]  = CONVERT(varchar(100), 'absent')
     , [workday_region_id]     = CONVERT(varchar(4), dim_emp_job_title.[workday_region_id])
     , dim_employee.[dim_employee_key]
     , dim_employee.[dim_club_key]
     , [dim_mms_member_key] = CASE WHEN NOT dim_employee.[member_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, dim_employee.[member_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [bk_hash] = dim_employee.[dim_employee_key]
     , [p_udw_employee_id] = 0
     , dim_employee.[dv_load_date_time]
     , dim_employee.[dv_batch_id]
     , [dv_hash] = dim_employee.[dim_employee_key]
  FROM [dbo].[dim_employee]
       INNER JOIN [dbo].[dim_club]
         ON dim_club.[dim_club_key] = dim_employee.[dim_club_key]
       LEFT OUTER JOIN
         ( SELECT [dim_employee_key]
                , [workday_region_id]
                , [business_title]
                , [family]
                , [is_primary_flag]
                , [job_code]
                , [level]
                , [marketing_title]
                , [profile]
                , [sub_family]
                , [dv_load_date_time]
                , [dv_batch_id]
                , RowRank = RANK() OVER (PARTITION BY [dim_employee_key], [is_primary_flag] ORDER BY [dv_load_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY [dim_employee_key], [is_primary_flag] ORDER BY [dv_load_date_time] DESC)
             FROM [dbo].[dim_employee_job_title]
             WHERE (NOT [is_primary_flag] Is Null AND [is_primary_flag] = 'Y')
         ) dim_emp_job_title
         ON dim_emp_job_title.[dim_employee_key] = dim_employee.[dim_employee_key]
            AND dim_emp_job_title.[is_primary_flag] = 'Y'
            AND dim_emp_job_title.RowRank = 1 AND dim_emp_job_title.RowNumber = 1
  WHERE NOT dim_employee.[employee_id] Is Null
    AND ( dim_employee.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND dim_employee.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
ORDER BY dim_employee.[dv_batch_id] ASC, dim_employee.[dv_load_date_time] ASC, ISNULL(dim_employee.[updated_date_time], dim_employee.[inserted_date_time]) ASC;

END
