CREATE PROC [dbo].[proc_dim_nmo_hub_task_status] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_nmo_hub_task_status
			)
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
	DECLARE @load_dv_batch_id BIGINT = CASE 
			WHEN @max_dv_batch_id < @current_dv_batch_id
				THEN @max_dv_batch_id
			ELSE @current_dv_batch_id
			END

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (
				distribution = HASH (dim_nmo_hub_task_status_key)
				,location = user_db
				) AS
select   d_nmo_hub_task.dim_nmo_hub_task_status_key dim_nmo_hub_task_status_key
        ,case when d_nmo_hub_task.party_id is null then '-998'
			else d_nmo_hub_task.party_id end party_id 
        ,case when d_nmo_hub_task.creator_party_id is null then '-998' 
			else d_nmo_hub_task.creator_party_id end creator_party_id
        ,case when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null then '-998'
			else map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end dim_mms_member_key
		,case when map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-998'
			else map_ltfeb_party_id_dim_employee_key.dim_employee_key end dim_employee_key
        ,case when d_nmo_hub_task_department.title is null then '-998'
			else d_nmo_hub_task_department.title end department_title
        ,case when d_nmo_hub_task.dim_club_key is null then '-998'
			else d_nmo_hub_task.dim_club_key end dim_club_key
        ,case when d_nmo_hub_task_status.title is null then '-998' 
			else d_nmo_hub_task_status.title end status
        ,case when d_nmo_hub_task_department.activation_dim_date_key is null then '-998'
			else d_nmo_hub_task_department.activation_dim_date_key end activation_dim_date_key
        ,case when d_nmo_hub_task_department.activation_dim_time_key is null then '-998'
			else d_nmo_hub_task_department.activation_dim_time_key end activation_dim_time_key
        ,case when d_nmo_hub_task_department.expiration_dim_date_key is null then '-998'
			else d_nmo_hub_task_department.expiration_dim_date_key end expiration_dim_date_key
        ,case when d_nmo_hub_task_department.expiration_dim_time_key is null then '-998'
			else d_nmo_hub_task_department.expiration_dim_time_key end expiration_dim_time_key
        ,case when d_nmo_hub_task.resolution_dim_date_key is null then '-998'
			else d_nmo_hub_task.resolution_dim_date_key end resolution_dim_date_key
        ,case when d_nmo_hub_task.resolution_dim_time_key is null then '-998'
			else d_nmo_hub_task.resolution_dim_time_key end resolution_dim_time_key
        ,case when d_nmo_hub_task.due_dim_date_key is null then '-998'
			else d_nmo_hub_task.due_dim_date_key end due_dim_date_key
        ,case when d_nmo_hub_task.due_dim_time_key is null then '-998'
			else d_nmo_hub_task.due_dim_time_key end due_dim_time_key
        ,case when d_nmo_hub_task.created_dim_date_key is null then '-998'
			else d_nmo_hub_task.created_dim_date_key end created_dim_date_key
        ,case when d_nmo_hub_task.created_dim_time_key is null then '-998'
			else d_nmo_hub_task.created_dim_time_key end created_dim_time_key
/*        ,case when d_nmo_hub_task.p_nmo_hub_task_id is null then '-998'*/
/*			else d_nmo_hub_task.p_nmo_hub_task_id end p_nmo_hub_task_id */
        ,case when d_nmo_hub_task.bk_hash is null then '-998'
			else d_nmo_hub_task.bk_hash end dim_nmo_hub_task_key 
        ,case when d_nmo_hub_task.dim_nmo_hub_task_department_key is null then '-998'
			else d_nmo_hub_task.dim_nmo_hub_task_department_key end dim_nmo_hub_task_department_key 
        ,case when d_nmo_hub_task.dim_nmo_hub_task_type_key is null then '-998'
			else d_nmo_hub_task.dim_nmo_hub_task_type_key end dim_nmo_hub_task_type_key 
																				
	    ,case 
			when isnull(d_nmo_hub_task.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_nmo_hub_task_department.dv_load_date_time,'Jan 1, 1753') 
				and isnull(d_nmo_hub_task.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_nmo_hub_task_status.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_nmo_hub_task.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_nmo_hub_task.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_nmo_hub_task.dv_load_date_time
	    	when isnull(d_nmo_hub_task_department.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_nmo_hub_task_status.dv_load_date_time,'Jan 1, 1753')
			and isnull(d_nmo_hub_task_department.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
			and isnull(d_nmo_hub_task_department.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_nmo_hub_task_department.dv_load_date_time
		    when isnull(d_nmo_hub_task_status.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
			and isnull(d_nmo_hub_task_status.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_nmo_hub_task_status.dv_load_date_time
			when isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time
			else isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time		  
	    ,case 
	    	when isnull(d_nmo_hub_task.dv_batch_id,-1) >= isnull(d_nmo_hub_task_department.dv_batch_id,-1) 
				and isnull(d_nmo_hub_task.dv_batch_id,-1) >= isnull(d_nmo_hub_task_status.dv_batch_id,-1)
				and isnull(d_nmo_hub_task.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
				and isnull(d_nmo_hub_task.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
					then d_nmo_hub_task.dv_batch_id
	    	when isnull(d_nmo_hub_task_department.dv_batch_id,-1) >= isnull(d_nmo_hub_task_status.dv_batch_id,-1)
			    and isnull(d_nmo_hub_task_department.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
			    and isnull(d_nmo_hub_task_department.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
					then d_nmo_hub_task_department.dv_batch_id
			when isnull(d_nmo_hub_task_status.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
			and isnull(d_nmo_hub_task_status.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				then d_nmo_hub_task_status.dv_batch_id				
			when isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				then map_ltfeb_party_id_dim_mms_member_key.dv_batch_id
			    else isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
	      end dv_batch_id
	    ,convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
from d_nmo_hub_task
left join d_nmo_hub_task_department
     on d_nmo_hub_task.dim_nmo_hub_task_department_key = d_nmo_hub_task_department.bk_hash 
left join d_nmo_hub_task_status
     on d_nmo_hub_task.dim_nmo_hub_task_status_key = d_nmo_hub_task_status.bk_hash
left join map_ltfeb_party_id_dim_mms_member_key
     on d_nmo_hub_task.party_id = map_ltfeb_party_id_dim_mms_member_key.party_id
left join map_ltfeb_party_id_dim_employee_key
     on d_nmo_hub_task.creator_party_id = map_ltfeb_party_id_dim_employee_key.party_id
where (d_nmo_hub_task.dv_batch_id >= @load_dv_batch_id
     or d_nmo_hub_task_department.dv_batch_id >= @load_dv_batch_id
     or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id
     or map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id
     or d_nmo_hub_task_status.dv_batch_id >= @load_dv_batch_id) 
	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_nmo_hub_task_status
	WHERE dim_nmo_hub_task_status_key IN (
			SELECT dim_nmo_hub_task_status_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_nmo_hub_task_status(
         dim_nmo_hub_task_status_key
		,party_id
		,creator_party_id
		,dim_mms_member_key
		,dim_employee_key
		,department_title
		,dim_club_key
		,status
		,activation_dim_date_key
		,activation_dim_time_key
		,expiration_dim_date_key 
		,expiration_dim_time_key
		,resolution_dim_date_key
		,resolution_dim_time_key
		,due_dim_date_key
		,due_dim_time_key
		,created_dim_date_key
		,created_dim_time_key
/*		,p_nmo_hub_task_id*/
		,dim_nmo_hub_task_key
		,dim_nmo_hub_task_department_key
		,dim_nmo_hub_task_type_key
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_nmo_hub_task_status_key
		,party_id
		,creator_party_id
		,dim_mms_member_key
		,dim_employee_key
		,department_title
		,dim_club_key
		,status
		,activation_dim_date_key
		,activation_dim_time_key
		,expiration_dim_date_key 
		,expiration_dim_time_key
		,resolution_dim_date_key
		,resolution_dim_time_key
		,due_dim_date_key
		,due_dim_time_key
		,created_dim_date_key
		,created_dim_time_key
/*		,p_nmo_hub_task_id*/
		,dim_nmo_hub_task_key
		,dim_nmo_hub_task_department_key
		,dim_nmo_hub_task_type_key
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN
			
END

