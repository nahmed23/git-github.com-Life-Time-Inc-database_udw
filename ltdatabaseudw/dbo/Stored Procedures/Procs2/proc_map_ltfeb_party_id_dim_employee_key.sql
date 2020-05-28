CREATE PROC [dbo].[proc_map_ltfeb_party_id_dim_employee_key] AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1


	CREATE TABLE dbo.#etl_step1
		WITH (
				distribution = HASH (party_id)
				,location = user_db
				) AS
select    
		 d_ltfeb_party_role.pr_party_id party_id
		,d_ltfeb_party_relationship_role_assignment.assigned_id assigned_id
		,case when d_ltfeb_party_relationship_role_assignment.assigned_id is null then '-998'
				when isnumeric(d_ltfeb_party_relationship_role_assignment.assigned_id) = 0  then '-998'
				else convert(char(32),hashbytes('md5',('P%#&z$@k'+d_ltfeb_party_relationship_role_assignment.assigned_id)),2)
			end dim_employee_key
		,row_number() over(partition by d_ltfeb_party_role.pr_party_id order by d_ltfeb_party_relationship.effective_to_dim_date_key desc) r
	    ,case 
	    	when isnull(d_ltfeb_party_role.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_ltfeb_party_relationship.dv_load_date_time,'Jan 1, 1753') 
				and isnull(d_ltfeb_party_role.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_ltfeb_party_relationship_role_assignment.dv_load_date_time,'Jan 1, 1753')
					then d_ltfeb_party_role.dv_load_date_time
	    	when isnull(d_ltfeb_party_relationship.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_ltfeb_party_relationship_role_assignment.dv_load_date_time,'Jan 1, 1753')
					then d_ltfeb_party_relationship.dv_load_date_time
			else isnull(d_ltfeb_party_relationship_role_assignment.dv_load_date_time,'Jan 1, 1753') 
	     end dv_load_date_time
	    ,case 
	    	when isnull(d_ltfeb_party_role.dv_batch_id,-1) >= isnull(d_ltfeb_party_relationship.dv_batch_id,-1) 
				and isnull(d_ltfeb_party_role.dv_batch_id,-1) >= isnull(d_ltfeb_party_relationship_role_assignment.dv_batch_id,-1)
					then d_ltfeb_party_role.dv_batch_id
	    	when isnull(d_ltfeb_party_relationship.dv_batch_id,-1) >= isnull(d_ltfeb_party_relationship_role_assignment.dv_batch_id,-1)
					then d_ltfeb_party_relationship.dv_batch_id
			else isnull(d_ltfeb_party_relationship_role_assignment.dv_batch_id,-1) 
	     end dv_batch_id
	    ,convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	   from d_ltfeb_party_role
    join d_ltfeb_party_relationship
	  on d_ltfeb_party_role.party_role_id = d_ltfeb_party_relationship.from_party_role_id
    join d_ltfeb_party_relationship_role_assignment
		on d_ltfeb_party_relationship.party_relationship_id = d_ltfeb_party_relationship_role_assignment.party_relationship_id
	where d_ltfeb_party_role.party_role_type = 'Employee' and d_ltfeb_party_relationship_role_assignment.party_relationship_role_type = 'LTF Employee'


	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/

	TRUNCATE TABLE map_ltfeb_party_id_dim_employee_key
	
	BEGIN TRAN

	INSERT INTO map_ltfeb_party_id_dim_employee_key(
		  party_id
        , assigned_id
        , dim_employee_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, dv_inserted_date_time
		, dv_insert_user
		)
	SELECT
		  party_id
        , assigned_id
        , dim_employee_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, getdate()
		, suser_sname()
	FROM #etl_step1
	where r = 1

	COMMIT TRAN
END
