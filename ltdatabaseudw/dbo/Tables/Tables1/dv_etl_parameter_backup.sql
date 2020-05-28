CREATE TABLE [dbo].[dv_etl_parameter_backup] (
    [dv_etl_parameter_id]   BIGINT        NOT NULL,
    [job_group]             VARCHAR (256) NOT NULL,
    [parameter_set]         VARCHAR (256) NOT NULL,
    [parameter_name]        VARCHAR (50)  NOT NULL,
    [parameter_value]       VARCHAR (256) NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL,
    [dv_batch_id]           BIGINT        NOT NULL,
    [dv_backup_date_time]   DATETIME      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

