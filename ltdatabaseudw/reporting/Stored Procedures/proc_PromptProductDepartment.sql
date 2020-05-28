CREATE PROC [reporting].[proc_PromptProductDepartment] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
SET FMTONLY OFF
END

IF OBJECT_ID('tempdb.dbo.#RevenueDepartments', 'U') IS NOT NULL
  DROP TABLE #RevenueDepartments; 
  
  
  
SELECT DISTINCT reporting_department DepartmentName--RevenueReportingDepartmentName
  INTO #RevenueDepartments
  FROM marketing.v_dim_mms_product
 WHERE reporting_department <> ''
UNION ALL
SELECT DISTINCT reporting_department DepartmentName
  FROM marketing.v_dim_hybris_product
 WHERE reporting_department <> ''
UNION ALL
SELECT DISTINCT reporting_department DepartmentName
  FROM marketing.v_dim_cafe_product--vDimCafeProductActive
 WHERE reporting_department <> ''--RevenueReportingDepartmentNameFor(Non)CommissionedSales
UNION ALL
SELECT DISTINCT reporting_department DepartmentName
  FROM marketing.v_dim_magento_product
 WHERE reporting_department <> ''
 UNION ALL
SELECT DISTINCT reporting_department DepartmentName
  FROM marketing.v_dim_healthcheckusa_product
 WHERE reporting_department <> ''
UNION ALL
SELECT 'None Designated'

SELECT DISTINCT DepartmentName DepartmentDescription
  FROM #RevenueDepartments


END
