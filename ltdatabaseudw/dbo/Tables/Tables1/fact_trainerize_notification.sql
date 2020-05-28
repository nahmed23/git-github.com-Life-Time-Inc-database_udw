CREATE TABLE [dbo].[fact_trainerize_notification] (
    [fact_trainerize_notification_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [created_dim_date_key]             VARCHAR (8)     NULL,
    [created_dim_time_key]             INT             NULL,
    [fact_trainerize_notification_key] VARCHAR (32)    NULL,
    [from_dim_employee_key]            VARCHAR (32)    NULL,
    [message]                          NVARCHAR (4000) NULL,
    [message_type]                     VARCHAR (1)     NULL,
    [notification_id]                  INT             NULL,
    [received_dim_date_key]            VARCHAR (8)     NULL,
    [received_dim_time_key]            INT             NULL,
    [source_id]                        NVARCHAR (50)   NULL,
    [source_thread_id]                 NVARCHAR (50)   NULL,
    [source_type]                      INT             NULL,
    [status]                           VARCHAR (1)     NULL,
    [subject]                          NVARCHAR (4000) NULL,
    [to_dim_mms_member_key]            VARCHAR (32)    NULL,
    [updated_dim_date_key]             VARCHAR (8)     NULL,
    [updated_dim_time_key]             INT             NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_trainerize_notification_key]));

