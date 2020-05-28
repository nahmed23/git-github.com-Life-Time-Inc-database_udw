CREATE PROC [reporting].[proc_PromptMMSDepartmentDescription] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

------ This SP returns 1) description from v_dim_mms_department.
------ Execution Sample:  Exec [reporting].[proc_PromptMMSDepartmentDescription] 

select distinct department.description MMSDepartmentDescription
from [marketing].[v_dim_mms_department] department
where IsNull(department.description,'0') > '0'   ----- to remove null values
and department.description <> 'Test for retry logic'  ----- to remove test department
order by department.description
	   
END

