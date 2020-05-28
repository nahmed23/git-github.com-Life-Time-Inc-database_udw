CREATE PROC [reporting].[proc_PayrollProductGroupSetupDetail] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 ------- Execution Sample
 ------- Exec [reporting].[proc_PayrollProductGroupSetupDetail]


SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Sales'  AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_mms_product]
WHERE payroll_standard_product_group_flag = 'Y'     
      AND payroll_standard_sales_amount_flag = 'Y'

UNION 

-- query 2

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Service'  AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_mms_product]
WHERE payroll_standard_product_group_flag = 'Y'               
AND	payroll_standard_service_amount_flag = 'Y'

UNION 

-- query 3

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_lt_bucks_group_description + ' Service Quantity' AS PayrollProductGroupNameAndExtractColumnName,
       'myLT BUCK$' AS ProductGroupType,
       payroll_lt_bucks_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_mms_product]
WHERE payroll_lt_bucks_product_group_flag = 'Y'             
AND payroll_lt_bucks_service_quantity_flag = 'Y'


UNION 

-- query 4

SELECT payroll_description AS  CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Sales' AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_cafe_product]
WHERE payroll_standard_product_group_flag = 'Y'                  
AND	payroll_standard_sales_amount_flag = 'Y'

UNION 

-- query 5

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Service' AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_cafe_product]
WHERE payroll_standard_product_group_flag = 'Y'                   
AND	payroll_standard_service_amount_flag = 'Y'

UNION 

-- query 6

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_lt_bucks_group_description + ' Service Quantity' AS PayrollProductGroupNameAndExtractColumnName,
       'myLT BUCK$' AS ProductGroupType,
       payroll_lt_bucks_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_cafe_product]
WHERE payroll_lt_bucks_product_group_flag = 'Y'                  
AND	payroll_lt_bucks_service_quantity_flag = 'Y'

UNION 

-- query 7

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Sales' AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_hybris_product]
WHERE payroll_standard_product_group_flag = 'Y'                
AND	payroll_standard_sales_amount_flag = 'Y'

UNION 

-- query 8

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Service'  AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_hybris_product]
WHERE payroll_standard_product_group_flag = 'Y'        
AND payroll_standard_service_amount_flag = 'Y'

UNION 

-- query 9

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_lt_bucks_group_description + ' Service Quantity' AS PayrollProductGroupNameAndExtractColumnName,
       'myLT BUCK$' AS ProductGroupType,
       payroll_lt_bucks_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_hybris_product] 
WHERE payroll_lt_bucks_product_group_flag = 'Y'           
 AND payroll_lt_bucks_service_quantity_flag = 'Y'

UNION 

-- query 10

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Sales' AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_healthcheckusa_product]
WHERE payroll_standard_product_group_flag = 'Y'                
AND	payroll_standard_sales_amount_flag = 'Y'

UNION 

-- query 11

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_standard_group_description + ' Service'  AS PayrollProductGroupNameAndExtractColumnName,
       'Standard' AS ProductGroupType,
       payroll_standard_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_healthcheckusa_product]
WHERE payroll_standard_product_group_flag = 'Y'        
AND payroll_standard_service_amount_flag = 'Y'

UNION 

-- query 12

SELECT payroll_description AS CommissionableSalesAndServiceGroup,
       payroll_lt_bucks_group_description + ' Service Quantity' AS PayrollProductGroupNameAndExtractColumnName,
       'myLT BUCK$' AS ProductGroupType,
       payroll_lt_bucks_group_sort_order AS PayrollProductGroupSortOrder 
FROM [marketing].[v_dim_healthcheckusa_product]
WHERE payroll_lt_bucks_product_group_flag = 'Y'         
 AND payroll_lt_bucks_service_quantity_flag = 'Y'
 
------ORDER BY CommissionableSalesAndServiceGroup, PayrollProductGroupSortOrder






END
