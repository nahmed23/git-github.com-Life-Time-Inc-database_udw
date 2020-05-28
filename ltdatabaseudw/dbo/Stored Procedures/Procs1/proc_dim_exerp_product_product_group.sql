CREATE PROC [dbo].[proc_dim_exerp_product_product_group] @dv_batch_id [varchar](500) AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

if object_id('tempdb..#etl_step1') is not null
       drop table #etl_step1
    create table dbo.#etl_step1
        with (
               distribution = hash(dim_exerp_product_product_group_key),
               location = user_db
     ) as
SELECT
    d_exerp_product_product_group.bk_hash dim_exerp_product_product_group_key,
    d_exerp_product_product_group.dim_exerp_product_key dim_exerp_product_key,
	d_exerp_product_product_group.product_group_id product_group_id,
	d_exerp_product_product_group.product_id product_id,
	CASE WHEN d_exerp_product.product_group_id = d_exerp_product_product_group.product_group_id
	        THEN 'Y' ELSE 'N' 
		        END primary_product_group_flag,
	d_exerp_product_group.product_group_name product_group_name,
	d_exerp_product_group.external_id product_group_external_id,
	dimension_d_exerp_product_group.product_group_id dimension_product_group_id,
	dimension_d_exerp_product_group.product_group_name dimension_product_group_name,
    parent_d_exerp_product_group.product_group_id parent_product_group_id,
	parent_d_exerp_product_group.product_group_name  parent_product_group_name,
	case when isnull(d_exerp_product_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_product_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_product.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_product_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(parent_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_exerp_product_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(dimension_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                           then isnull(d_exerp_product_product_group.dv_load_date_time,'Jan 1, 1753') 
	     when isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_product.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(parent_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(dimension_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
	                        then isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') 					   
	     when isnull(d_exerp_product.dv_load_date_time,'Jan 1, 1753') >= isnull(parent_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_product.dv_load_date_time,'Jan 1, 1753') >= isnull(dimension_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                            then isnull(d_exerp_product.dv_load_date_time,'Jan 1, 1753')
         when isnull(parent_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') >= isnull(dimension_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
                            then isnull(parent_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')	
                        		else  isnull(dimension_d_exerp_product_group.dv_load_date_time,'Jan 1, 1753') end  dv_load_date_time, 
								
	convert(datetime, '99991231', 112) dv_load_end_date_time,
	
	case when isnull(d_exerp_product_product_group.dv_batch_id,'-1') >= isnull(d_exerp_product_group.dv_batch_id,'-1')
                and isnull(d_exerp_product_product_group.dv_batch_id,'-1') >= isnull(d_exerp_product.dv_batch_id,'-1')
                and isnull(d_exerp_product_product_group.dv_batch_id,'-1') >= isnull(parent_d_exerp_product_group.dv_batch_id,'-1')
				and isnull(d_exerp_product_product_group.dv_batch_id,'-1') >= isnull(dimension_d_exerp_product_group.dv_batch_id,'-1')
                           then isnull(d_exerp_product_product_group.dv_batch_id,'-1') 
	     when isnull(d_exerp_product_group.dv_batch_id,'-1') >= isnull(d_exerp_product.dv_batch_id,'-1')
                and isnull(d_exerp_product_group.dv_batch_id,'-1') >= isnull(parent_d_exerp_product_group.dv_batch_id,'-1')
                and isnull(d_exerp_product_group.dv_batch_id,'-1') >= isnull(dimension_d_exerp_product_group.dv_batch_id,'-1')
	                        then isnull(d_exerp_product_group.dv_batch_id,'-1') 					   
	     when isnull(d_exerp_product.dv_batch_id,'-1') >= isnull(parent_d_exerp_product_group.dv_batch_id,'-1')
                and isnull(d_exerp_product.dv_batch_id,'-1') >= isnull(dimension_d_exerp_product_group.dv_batch_id,'-1')
                            then isnull(d_exerp_product.dv_batch_id,'-1')
         when isnull(parent_d_exerp_product_group.dv_batch_id,'-1') >= isnull(dimension_d_exerp_product_group.dv_batch_id,'-1')
                            then isnull(parent_d_exerp_product_group.dv_batch_id,'-1')	
                        		else  isnull(dimension_d_exerp_product_group.dv_batch_id,'-1') end  dv_batch_id     
	FROM 
	    d_exerp_product_product_group d_exerp_product_product_group
	 
	JOIN 
	    d_exerp_product_group d_exerp_product_group
	    ON d_exerp_product_product_group.d_exerp_product_group_bk_hash = d_exerp_product_group.bk_hash
	 
    LEFT JOIN 
	    d_exerp_product d_exerp_product
	    ON   d_exerp_product.d_exerp_product_group_bk_hash = d_exerp_product_product_group.d_exerp_product_group_bk_hash
	    AND  d_exerp_product.bk_hash = d_exerp_product_product_group.dim_exerp_product_key
	 
	LEFT JOIN 
	    d_exerp_product_group parent_d_exerp_product_group
    	ON parent_d_exerp_product_group.bk_hash  = d_exerp_product_group.parent_d_exerp_product_group_bk_hash
    
	LEFT JOIN 
	    d_exerp_product_group dimension_d_exerp_product_group
	    ON  dimension_d_exerp_product_group.bk_hash = d_exerp_product_group.dimension_d_exerp_product_group_bk_hash
	
BEGIN TRAN
DELETE dbo.dim_exerp_product_product_group
WHERE dim_exerp_product_product_group_key IN (
  SELECT dim_exerp_product_product_group_key
  FROM dbo.#etl_step1
  )
INSERT INTO dim_exerp_product_product_group(
    dim_exerp_product_product_group_key
    ,dim_exerp_product_key
    ,product_group_id
	,product_id
    ,primary_product_group_flag
    ,product_group_name
    ,product_group_external_id
    ,dimension_product_group_id
    ,dimension_product_group_name
    ,parent_product_group_id
    ,parent_product_group_name
    ,dv_load_date_time
    ,dv_load_end_date_time
    ,dv_batch_id
    ,dv_inserted_date_time
    ,dv_insert_user
)
SELECT
    dim_exerp_product_product_group_key
    ,dim_exerp_product_key
    ,product_group_id
	,product_id
    ,primary_product_group_flag
    ,product_group_name
    ,product_group_external_id
    ,dimension_product_group_id
    ,dimension_product_group_name
    ,parent_product_group_id
    ,parent_product_group_name
    ,dv_load_date_time
    ,dv_load_end_date_time
    ,dv_batch_id
    ,getdate()
    ,suser_sname()
 
FROM #etl_step1
COMMIT TRAN
 
END
