CREATE PROC [dbo].[proc_fact_guest_usage_summary] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_guest_usage_summary
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
		distribution = HASH (fact_guest_usage_summary_key)
		,location = user_db
	) AS
SELECT 
      d_mms_guest_count.bk_hash fact_guest_usage_summary_key
      ,d_mms_guest_count.guest_count_id guest_count_id
      ,d_mms_guest_count.dim_club_key dim_club_key
      ,d_mms_guest_count.fact_guest_count_dim_date_key fact_mms_guest_count_dim_date_key
      ,d_mms_guest_count.member_child_count member_child_count
      ,d_mms_guest_count.member_count member_count
      ,d_mms_guest_count.non_member_child_count non_member_child_count
      ,d_mms_guest_count.non_member_count non_member_count
	  ,case when d_mms_guest_count.inserted_dim_date_key > dateadd(dd,7,dim_date.next_month_starting_date) then 'Y' else 'N' end data_received_late_flag
	  ,d_mms_guest_count.club_id club_id
	  ,d_mms_guest_count.guest_count_date guest_count_date
	  ,d_mms_guest_count.inserted_date_time inserted_date_time
	  ,d_mms_guest_count.dv_load_date_time dv_load_date_time
      ,d_mms_guest_count.dv_load_end_date_time dv_load_end_date_time
      ,d_mms_guest_count.dv_batch_id dv_batch_id
      ,d_mms_guest_count.dv_inserted_date_time dv_inserted_date_time
      ,d_mms_guest_count.dv_insert_user dv_insert_user
  FROM dbo.d_mms_guest_count
  join dim_date
  on d_mms_guest_count.fact_guest_count_dim_date_key = dim_date.dim_date_key
where d_mms_guest_count.dv_batch_id >= @load_dv_batch_id

/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

DELETE dbo.fact_guest_usage_summary
WHERE fact_guest_usage_summary_key IN (
		SELECT fact_guest_usage_summary_key
		FROM dbo.#etl_step1
		)

INSERT INTO fact_guest_usage_summary (
	fact_guest_usage_summary_key
	,dim_club_key
	,guest_count_id
	,club_id
	,guest_count_date
	,fact_mms_guest_count_dim_date_key
	,member_count
	,non_member_count
	,member_child_count
	,non_member_child_count
	,inserted_date_time
	,data_received_late_flag
    ,dv_load_date_time
    ,dv_load_end_date_time
    ,dv_batch_id
    ,dv_inserted_date_time
    ,dv_insert_user
	)
SELECT 
	fact_guest_usage_summary_key
	,dim_club_key
	,guest_count_id
	,club_id
	,guest_count_date
	,fact_mms_guest_count_dim_date_key
	,member_count
	,non_member_count
	,member_child_count
	,non_member_child_count
	,inserted_date_time
	,data_received_late_flag
    ,dv_load_date_time
    ,dv_load_end_date_time
    ,dv_batch_id
    ,dv_inserted_date_time
    ,dv_insert_user
from #etl_step1

COMMIT TRAN
END
