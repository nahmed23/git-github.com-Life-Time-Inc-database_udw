CREATE VIEW [reporting].[v_exerp_boss_product]
AS select
		  'Exerp' data_source,
		  a.dim_exerp_activity_key as dim_product_key,
		  a.dim_boss_product_key as dim_legacy_product_key,
		  NULL as department_code,
		  a.department as department_description,/* product_description */
		  a.activity_name as product, /*Product*/
		  a.activity_group_name as product_Line,/*Product Line*/
		  a.color,
		  a.external_id as upc_code,
		  'Other' as class_format ,
		  a.sku as product_sku,
		  NULL as product_format_id,
		  NULL as product_format_long_desc,
		  NULL as product_interest_id,
		  NULL as product_interest_long_desc,
		  NULL as product_updated_timestamp
		  from [marketing].[v_dim_exerp_activity] a
  UNION
		SELECT 
		'BOSS' data_source,
		a.dim_boss_product_key as dim_product_key,
		NULL as dim_legacy_product_key,
		a.department_code, 
		a.department_description, 
		LTRIM(RTRIM(a.product_description)) AS product, 
		a.product_line, 
		a.color, 
		a.upc_code, 
		CASE WHEN a.product_description LIKE 'Feature%' THEN 'Feature' WHEN a.product_description LIKE 'EDG%' THEN 'Edge' ELSE 'Other' END AS class_format ,
		a.sku as product_sku,
		a.product_format_id,
        a.product_format_long_desc,
		a.product_interest_id,
        a.product_interest_long_desc,
		a.product_updated_timestamp
		FROM marketing.v_dim_boss_product a;