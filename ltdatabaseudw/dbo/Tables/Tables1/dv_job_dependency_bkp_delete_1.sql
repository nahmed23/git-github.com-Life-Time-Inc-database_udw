CREATE TABLE [dbo].[dv_job_dependency_bkp_delete_1] (
    [dv_job_dependency_id]          BIGINT       IDENTITY (1, 1) NOT NULL,
    [dv_job_status_id]              BIGINT       NOT NULL,
    [dependent_on_dv_job_status_id] BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

