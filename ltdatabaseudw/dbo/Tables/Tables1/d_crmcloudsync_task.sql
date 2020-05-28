CREATE TABLE [dbo].[d_crmcloudsync_task] (
    [d_crmcloudsync_task_id]                   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)      NOT NULL,
    [fact_crm_task_key]                        VARCHAR (32)   NULL,
    [activity_id]                              VARCHAR (36)   NULL,
    [created_dim_date_key]                     VARCHAR (8)    NULL,
    [created_dim_time_key]                     INT            NULL,
    [created_on]                               DATETIME       NULL,
    [dim_crm_owner_key]                        VARCHAR (32)   NULL,
    [ltf_task_type_name]                       NVARCHAR (255) NULL,
    [regarding_object_dim_crm_system_user_key] VARCHAR (32)   NULL,
    [regarding_object_type_code]               NVARCHAR (64)  NULL,
    [scheduled_start]                          DATETIME       NULL,
    [scheduled_start_dim_date_key]             VARCHAR (8)    NULL,
    [scheduled_start_dim_time_key]             INT            NULL,
    [status_code]                              INT            NULL,
    [status_code_name]                         NVARCHAR (255) NULL,
    [p_crmcloudsync_task_id]                   BIGINT         NOT NULL,
    [deleted_flag]                             INT            NULL,
    [dv_load_date_time]                        DATETIME       NULL,
    [dv_load_end_date_time]                    DATETIME       NULL,
    [dv_batch_id]                              BIGINT         NOT NULL,
    [dv_inserted_date_time]                    DATETIME       NOT NULL,
    [dv_insert_user]                           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                     DATETIME       NULL,
    [dv_update_user]                           VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

