CREATE PROC [reporting].[proc_PromptTeamMemberAndRoleByClubAndDate] @ReportStartDate [DATETIME],@ReportEndDate [DATETIME],@DimMMSClubIDList [VARCHAR](8000) AS 
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON 

IF 1=0 BEGIN
	SET FMTONLY OFF 
END

IF OBJECT_ID('tempdb.dbo.#DimMMSClubIDList', 'U') IS NOT NULL
	DROP TABLE #DimMMSClubIDList; 
	
----- Create DimLocationKeyList temp table

DECLARE @list_table VARCHAR(8000)
SET		@list_table = 'MMSClubIDList' 

EXEC marketing.proc_parse_pipe_list @DimMMSClubIDList,@list_table


SELECT DimClub.dim_club_key AS DimClubKey 
      INTO [#DimClubKeyList]
      FROM marketing.v_dim_club AS DimClub
INNER JOIN [#MMSClubIDList] 
        ON [#MMSClubIDList].Item = DimClub.club_id

SELECT DISTINCT DimEmployee.dim_employee_key,
                DimClub.Club_Code + '-' + DimEmployee.First_Name + ' ' + DimEmployee.Last_Name AS ClubCodeDashTeamMemberName,
                DimEmployeeRole.Role_Name,
				DimEmployee.employee_id
	  FROM	marketing.v_dim_employee AS DimEmployee
INNER JOIN	marketing.v_dim_employee_bridge_dim_employee_role AS DimEmployeeBridgeDimEmployeeRole 
		ON	DimEmployee.employee_id = DimEmployeeBridgeDimEmployeeRole.employee_id
INNER JOIN	marketing.v_dim_employee_role AS DimEmployeeRole 
		ON	DimEmployeeBridgeDimEmployeeRole.dim_employee_role_key = DimEmployeeRole.dim_employee_role_key
INNER JOIN	marketing.v_dim_club AS DimClub 
		ON	DimEmployee.dim_club_key = DimClub.dim_club_key
INNER JOIN	[#DimClubKeyList] 
		ON  DimClub.dim_club_key = [#DimClubKeyList].DimClubKey
	 WHERE  (DimEmployee.Employee_Active_Flag = 'Y')
	 
DROP TABLE #DimClubKeyList 

END 

