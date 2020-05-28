CREATE PROC [dbo].[proc_dim_location_attribute] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	
	declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_location_attribute)
    declare @current_dv_batch_id bigint = @dv_batch_id
    declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

	TRUNCATE TABLE dim_location_attribute

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (distribution = HASH (dim_location_attribute_key),location = user_db) 
	AS
	SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(t1.dim_employee_key as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(t5.dim_location_key as varchar(500)),'z#@$k%&P')
								  +'P%#&z$@k'+'UDW.Workday'
								  +'P%#&z$@k'+isnull(cast(t3.bk_hash as varchar(500)),'z#@$k%&P')
								  )
							),2) dim_location_attribute_key
		,t5.dim_location_key
		,t1.dim_employee_key AS business_key
		,'UDW.Workday' business_source_name
		,t3.val_attribute_type_name location_attribute_type_name
		,CASE WHEN t3.val_attribute_type_name LIKE '%email%' THEN t1.primary_work_email 
		      WHEN t3.val_attribute_type_name LIKE '%phone%' THEN t1.phone_number 
		      WHEN t3.val_attribute_type_name LIKE '%name%' THEN t1.employee_name
		      WHEN t3.val_attribute_type_name LIKE '%Job Code%'  THEN t2.job_code 
			  WHEN t3.val_attribute_type_name LIKE '%Employee ID%'  THEN CAST(t1.employee_id AS VARCHAR)
		 END AS attribute_value
		,t3.display_name AS location_attribute_type_display_name
		,t4.val_attribute_type_group_name location_attribute_type_group_name
		,t4.display_name location_attribute_type_group_display_name
		,'Y' AS managed_by_udw_flag
		,CONVERT(VARCHAR(8), cast(t1.dv_load_date_time AS DATE), 112) created_dim_date_key
		,t1.dv_insert_user AS created_by
		,CONVERT(VARCHAR(8), cast(t1.dv_updated_date_time AS DATE), 112) updated_dim_date_key
		,t1.dv_update_user AS updated_by
		,t1.dv_load_date_time
		,t1.dv_batch_id
	FROM dim_employee t1
	JOIN dim_employee_job_title t2 ON t1.dim_employee_key = t2.dim_employee_key
	JOIN d_loc_val_attribute_type t3 ON (
	t2.marketing_title = substring(t3.val_attribute_type_name, 1, 15)
	OR t2.marketing_title = substring(t3.val_attribute_type_name, 1, 23) )
	JOIN d_loc_val_attribute_type_group t4 ON t3.d_loc_val_attribute_type_group_bk_hash = t4.bk_hash
	LEFT JOIN dim_location t5 ON t1.dim_club_key = t5.business_key
	WHERE t2.is_primary_flag = 'Y'
		AND t1.active_status = '1'
		AND t1.cf_employment_status='A'
		AND t5.hierarchy_level=2 
		AND t5.location_type_display_name='Life Time Club'
        AND t5.managed_by_udw_flag='Y'
		AND t2.marketing_title IN ('General Manager','Member Services Manager')
		AND (t3.val_attribute_type_name LIKE '%email%'
		OR t3.val_attribute_type_name LIKE '%phone%'
		OR t3.val_attribute_type_name LIKE '%name%'
		OR t3.val_attribute_type_name LIKE '%Job Code%'
		OR t3.val_attribute_type_name LIKE '%Employee ID%')
	
	UNION
	
	SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(t1.dim_club_key as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(t5.dim_location_key as varchar(500)),'z#@$k%&P')
								  +'P%#&z$@k'+'UDW.MMS'
								  +'P%#&z$@k'+isnull(cast(t3.bk_hash as varchar(500)),'z#@$k%&P')
								  )
							),2) dim_location_attribute_key
		,t5.dim_location_key
		,t1.dim_club_key AS business_key
		,'UDW.MMS' business_source_name
		,t3.val_attribute_type_name location_attribute_type_name
		,t1.phone_number attribute_value
		,t3.display_name AS location_attribute_type_display_name
		,t4.val_attribute_type_group_name location_attribute_type_group_name
		,t4.display_name location_attribute_type_group_display_name
		,'Y' AS managed_by_udw_flag
		,CONVERT(VARCHAR(8), cast(t1.dv_load_date_time AS DATE), 112) created_dim_date_key
		,t1.dv_insert_user AS created_by
		,CONVERT(VARCHAR(8), cast(t1.dv_updated_date_time AS DATE), 112) updated_dim_date_key
		,t1.dv_update_user AS updated_by
		,t1.dv_load_date_time
		,t1.dv_batch_id
	FROM dim_club t1
	CROSS JOIN d_loc_val_attribute_type t3
	JOIN d_loc_val_attribute_type_group t4 ON t3.d_loc_val_attribute_type_group_bk_hash = t4.bk_hash
	LEFT JOIN dim_location t5 ON t1.dim_club_key = t5.business_key
	WHERE t1.dim_club_key NOT IN ('-997','-998','-999')
	    AND t5.hierarchy_level=2 
		AND t5.location_type_display_name='Life Time Club'
        AND t5.managed_by_udw_flag='Y'
		AND t3.val_attribute_type_name LIKE '%club phone%'
		
	UNION
	
		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(t1.dim_club_key as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(t5.dim_location_key as varchar(500)),'z#@$k%&P')
								  +'P%#&z$@k'+'Notes.csv'
								  +'P%#&z$@k'+isnull(cast(t3.bk_hash as varchar(500)),'z#@$k%&P')
								  )
							),2) dim_location_attribute_key
		,t5.dim_location_key
		,t1.dim_club_key AS business_key
		,'Notes.csv' business_source_name
		,t3.val_attribute_type_name location_attribute_type_name
		,t1.notes attribute_value
		,t3.display_name AS location_attribute_type_display_name
		,t4.val_attribute_type_group_name location_attribute_type_group_name
		,t4.display_name location_attribute_type_group_display_name
		,'Y' AS managed_by_udw_flag
		,CONVERT(char(8), GETDATE(), 112) created_dim_date_key
		,'InformaticaUser' AS created_by
		,'-998' as updated_dim_date_key
		,null AS updated_by
		,getdate() dv_load_date_time
		,'-1' dv_batch_id
	FROM stg_attribute_note t1
	CROSS JOIN d_loc_val_attribute_type t3
	JOIN d_loc_val_attribute_type_group t4 ON t3.d_loc_val_attribute_type_group_bk_hash = t4.bk_hash
	LEFT JOIN dim_location t5 ON t1.dim_club_key = t5.business_key and t1.display_name=t5.location_type_display_name
	WHERE t1.dim_club_key NOT IN ('-997','-998','-999')
	    AND t3.val_attribute_type_name ='Location Note'
		AND t1.notes is not null
		AND t5.managed_by_udw_flag='Y'	
		
	UNION
	
	/**************************************APP Managed Data************************************************/	
	
	SELECT CASE WHEN business_key in ('-997','-998','-999') THEN business_key
	       ELSE CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(business_key as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(dim_location_key as varchar(500)),'z#@$k%&P')
								  +'P%#&z$@k'+'lt_dm_loc'
								  +'P%#&z$@k'+isnull(cast(d_loc_val_attribute_type_bk_hash as varchar(500)),'z#@$k%&P')
								  )
							),2) END dim_location_attribute_key,
	  d_loc_attribute.dim_location_key,
      d_loc_attribute.business_key,
	  CASE WHEN business_key in ('-997','-998','-999') THEN NULL
	       ELSE 'lt_dm_loc' END as business_source_name,
	  d_loc_val_attribute_type.val_attribute_type_name location_attribute_type_name,
	  d_loc_attribute.attribute_value,
	  d_loc_val_attribute_type.display_name location_attribute_type_display_name,
	  d_loc_val_attribute_type_group.val_attribute_type_group_name location_attribute_type_group_name,
	  d_loc_val_attribute_type_group.display_name location_attribute_type_group_display_name,
	  d_loc_attribute.managed_by_udw_flag,
	  d_loc_attribute.created_dim_date_key,
	  d_loc_attribute.created_by,
	  d_loc_attribute.updated_dim_date_key,
	  d_loc_attribute.updated_by,
	  CASE WHEN isnull(d_loc_attribute.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_loc_val_attribute_type.dv_load_date_time, 'Jan 1, 1753')
                AND isnull(d_loc_attribute.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_loc_val_attribute_type_group.dv_load_date_time, 'Jan 1, 1753')
           THEN isnull(d_loc_attribute.dv_load_date_time, 'Jan 1, 1753')
   	       WHEN isnull(d_loc_val_attribute_type.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_loc_val_attribute_type_group.dv_load_date_time, 'Jan 1, 1753')
           THEN isnull(d_loc_val_attribute_type.dv_load_date_time, 'Jan 1, 1753')
   	   ELSE isnull(d_loc_val_attribute_type_group.dv_load_date_time, 'Jan 1, 1753')
   	 END dv_load_date_time,
	 CASE WHEN isnull(d_loc_attribute.dv_batch_id, - 1) >= isnull(d_loc_val_attribute_type.dv_batch_id, - 1)
               AND isnull(d_loc_attribute.dv_batch_id, - 1) >= isnull(d_loc_val_attribute_type_group.dv_batch_id, - 1)
          THEN isnull(d_loc_attribute.dv_batch_id, - 1)
   	      WHEN isnull(d_loc_val_attribute_type.dv_batch_id, - 1) >= isnull(d_loc_val_attribute_type_group.dv_batch_id, - 1)
          THEN isnull(d_loc_val_attribute_type.dv_batch_id, - 1)
   	 ELSE isnull(d_loc_val_attribute_type_group.dv_batch_id, - 1)
   	END dv_batch_id
  FROM d_loc_attribute d_loc_attribute
  LEFT JOIN d_loc_val_attribute_type d_loc_val_attribute_type 
  ON d_loc_attribute.d_loc_val_attribute_type_bk_hash=d_loc_val_attribute_type.bk_hash
  JOIN d_loc_val_attribute_type_group d_loc_val_attribute_type_group
  ON d_loc_val_attribute_type.d_loc_val_attribute_type_group_bk_hash=d_loc_val_attribute_type_group.bk_hash
  WHERE d_loc_attribute.managed_by_udw_flag='N' and ( d_loc_attribute.dv_batch_id >= @load_dv_batch_id  
      OR d_loc_val_attribute_type.dv_batch_id >= @load_dv_batch_id  
      OR d_loc_val_attribute_type_group.dv_batch_id >= @load_dv_batch_id
   ) 

/*****************************Steps to handle MSM and GSM Order*******************************************************/
  
  
IF object_id('tempdb..#etl_step2') IS NOT NULL
		DROP TABLE #etl_step2

CREATE TABLE dbo.#etl_step2
 WITH (distribution = HASH (dim_location_attribute_key),location = user_db) 
AS
SELECT dim_location_attribute_key
	,dim_location_key
	,business_key
	,location_attribute_type_name
	,attribute_value
	,business_source_name
	,CASE WHEN attribute_value = '716' THEN 1
		WHEN attribute_value = '252' THEN 2
		WHEN attribute_value = '871' THEN 3
		END AS display_name_gm_order
	,CASE WHEN attribute_value = '1060' THEN 1
		WHEN attribute_value = '1653' THEN 2
		WHEN attribute_value = '2285' THEN 3
		WHEN attribute_value = '2871' THEN 4
		END AS display_name_msm_order
	,managed_by_udw_flag
	,created_dim_date_key
	,created_by
	,updated_dim_date_key
	,updated_by
	,dv_load_date_time
	,dv_batch_id	
FROM #etl_step1
WHERE location_attribute_type_name LIKE '%Manager Job Code'

/*E61EE92C3B833EF1F5DAB5D81ED81266*/

IF object_id('tempdb..#etl_step3') IS NOT NULL
		DROP TABLE #etl_step3
CREATE TABLE dbo.#etl_step3
 WITH (distribution = HASH (dim_location_key),location = user_db) 
AS
SELECT t1.dim_location_key
	,t1.business_key
	,'Display General Manager Flag' AS location_attribute_type_name
	,CASE WHEN t2.display_name_gm_order IS NULL THEN 'N' ELSE 'Y' END AS attribute_value
	/*,t1.attribute_value as old_attribute_value*/
	,t1.business_source_name
	,t1.managed_by_udw_flag
	,t1.created_dim_date_key
	,t1.created_by
	,t1.updated_dim_date_key
	,updated_by
	,t1.dv_load_date_time
	,t1.dv_batch_id
FROM #etl_step2 t1
LEFT JOIN (SELECT dim_location_key
		,location_attribute_type_name
		,MIN(display_name_gm_order) display_name_gm_order
	FROM #etl_step2
	WHERE display_name_gm_order IS NOT NULL
	GROUP BY dim_location_key,location_attribute_type_name
	) t2 ON t1.dim_location_key = t2.dim_location_key
	AND t1.display_name_gm_order = t2.display_name_gm_order
WHERE t1.location_attribute_type_name='General Manager Job Code'
UNION
SELECT t1.dim_location_key
	,t1.business_key
	,'Display Member Services Manager Flag' AS location_attribute_type_name
	,CASE WHEN t2.display_name_msm_order IS NULL THEN 'N' ELSE 'Y' END AS attribute_value
	/*,t1.attribute_value as old_attribute_value*/
	,t1.business_source_name
	,t1.managed_by_udw_flag
	,t1.created_dim_date_key
	,t1.created_by
	,t1.updated_dim_date_key
	,updated_by
	,t1.dv_load_date_time
	,t1.dv_batch_id
FROM #etl_step2 t1
LEFT JOIN (SELECT dim_location_key
		,location_attribute_type_name
		,MIN(display_name_msm_order) display_name_msm_order
	FROM #etl_step2
	WHERE display_name_msm_order IS NOT NULL
	GROUP BY dim_location_key,location_attribute_type_name
	) t2 ON t1.dim_location_key = t2.dim_location_key
	AND t1.display_name_msm_order = t2.display_name_msm_order
WHERE t1.location_attribute_type_name='Member Services Manager Job Code'


IF object_id('tempdb..#etl_step4') IS NOT NULL
		DROP TABLE #etl_step4

CREATE TABLE dbo.#etl_step4
 WITH (distribution = HASH (dim_location_attribute_key),location = user_db) 
AS
SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(business_key as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(dim_location_key as varchar(500)),'z#@$k%&P')
								  +'P%#&z$@k'+'lt_dm_loc'
								  +'P%#&z$@k'+isnull(cast(t2.bk_hash as varchar(500)),'z#@$k%&P')
								  )
							),2) dim_location_attribute_key
    ,t1.dim_location_key
	,t1.business_key
	,t1.business_source_name
	,t2.val_attribute_type_name location_attribute_type_name
	,t1.attribute_value
	,t2.display_name AS location_attribute_type_display_name
	,t3.val_attribute_type_group_name location_attribute_type_group_name
	,t3.display_name location_attribute_type_group_display_name
	,t1.managed_by_udw_flag
	,t1.created_dim_date_key
	,t1.created_by
	,t1.updated_dim_date_key
	,t1.updated_by AS dv_update_user
	,t1.dv_load_date_time
	,t1.dv_batch_id
FROM #etl_step3 t1
JOIN d_loc_val_attribute_type t2 ON t1.location_attribute_type_name = t2.val_attribute_type_name
JOIN d_loc_val_attribute_type_group t3 ON t2.d_loc_val_attribute_type_group_bk_hash = t3.bk_hash

IF object_id('tempdb..#etl_step5') IS NOT NULL
		DROP TABLE #etl_step5

CREATE TABLE dbo.#etl_step5
 WITH (distribution = HASH (dim_location_attribute_key),location = user_db) 
AS
SELECT * FROM #etl_step1
UNION
SELECT * FROM #etl_step4


	DECLARE @dv_inserted_date_time DATETIME = getdate()
	DECLARE @dv_insert_user VARCHAR(50) = suser_sname()

	BEGIN TRAN

	INSERT INTO dim_location_attribute (
		attribute_value
		,business_key
		,business_source_name
		,created_by
		,created_dim_date_key
		,deleted_by
		,deleted_dim_date_key
		,dim_location_attribute_key
		,dim_location_key
		,location_attribute_type_display_name
		,location_attribute_type_group_display_name
		,location_attribute_type_group_name
		,location_attribute_type_name
		,managed_by_udw_flag
		,updated_by
		,updated_dim_date_key 
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT attribute_value
		,business_key
		,business_source_name
		,created_by
		,created_dim_date_key
		,NULL AS deleted_by
		,NULL deleted_dim_date_key
		,dim_location_attribute_key
		,dim_location_key
		,location_attribute_type_display_name
		,location_attribute_type_group_display_name
		,location_attribute_type_group_name
		,location_attribute_type_name
		,managed_by_udw_flag
		,updated_by
		,updated_dim_date_key
		,dv_load_date_time
		,'dec 31, 9999' AS dv_load_end_date_time
		,dv_batch_id
		,@dv_inserted_date_time
		,@dv_insert_user
	FROM #etl_step5

	COMMIT TRAN
END