CREATE TABLE [dbo].[fact_affinitech_accuracy_audit] (
    [fact_affinitech_accuracy_audit_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [Accuracy]                           FLOAT (53)    NULL,
    [Count]                              INT           NULL,
    [date]                               DATE          NULL,
    [fact_affinitech_accuracy_audit_key] VARCHAR (32)  NULL,
    [studio]                             VARCHAR (255) NULL,
    [transactions]                       FLOAT (53)    NULL,
    [dv_load_date_time]                  DATETIME      NULL,
    [dv_load_end_date_time]              DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NOT NULL,
    [dv_inserted_date_time]              DATETIME      NOT NULL,
    [dv_insert_user]                     VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]               DATETIME      NULL,
    [dv_update_user]                     VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_affinitech_accuracy_audit_key]));

