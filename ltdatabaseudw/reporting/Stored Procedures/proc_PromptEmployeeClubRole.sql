CREATE PROC [reporting].[proc_PromptEmployeeClubRole] @ClubIDList [VARCHAR](2000) AS
BEGIN

--DECLARE @ClubIDList VARCHAR(2000) = '151'

IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #clubs;

DECLARE @list_table VARCHAR(500)
SET @list_table = 'Clubs'
EXEC marketing.proc_parse_pipe_list @ClubIDList,@list_table

  IF (SELECT COUNT(*) FROM #Clubs WHERE Item = 0) = 1  -- all clubs option selected
   BEGIN 
    TRUNCATE TABLE #Clubs  
    INSERT INTO #Clubs (Club_id) SELECT club_id FROM marketing.v_Dim_club
   END

Select c.club_name ClubName
	,employee_role.role_name RoleDescription
	,e.employee_id EmployeeID
	,e.first_name FirstName
	,e.last_name LastName
	,e.employee_name EmployeeName
	,c.club_id ClubID
	,2 SortOrder
	,e.hire_date HireDate
	,e.termination_date TerminationDate
	,ISNULL(e.termination_date,DATEADD(YEAR,10,GETDATE())) NonNull_TerminationDate_forCognos

FROM marketing.v_Dim_club c
  JOIN #Clubs cl
    ON c.club_id = cl.Item
  join marketing.v_dim_employee e
    ON e.dim_club_key = c.dim_club_key
  JOIN marketing.v_dim_employee_bridge_dim_employee_role bridge
    on bridge.dim_employee_key = e.dim_employee_key
  JOIN marketing.v_dim_employee_role employee_role
    ON employee_role.dim_employee_role_key = bridge.dim_employee_role_key

UNION

SELECT
	 '' AS ClubName
	,'' AS RoleDescription
	,e.employee_id EmployeeID
	,e.first_name FirstName
	,e.last_name LastName
	,e.employee_name EmployeeName
	,c.club_id ClubID
	,1 SortOrder
	,e.hire_date HireDate
	,e.termination_date TerminationDate
	,ISNULL(e.termination_date,DATEADD(YEAR,10,GETDATE())) NonNull_TerminationDate_forCognos
FROM marketing.v_Dim_club c
  join marketing.v_dim_employee e
    ON e.dim_club_key = c.dim_club_key

WHERE e.employee_id < 0

END
GRANT EXECUTE ON [reporting].[proc_PromptEmployeeClubRole] TO CognosAnalyticsUser