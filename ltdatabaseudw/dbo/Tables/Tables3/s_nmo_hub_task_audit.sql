CREATE TABLE [dbo].[s_nmo_hub_task_audit] (
    [s_nmo_hub_task_audit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [id]                      INT             NULL,
    [operation]               NVARCHAR (4000) NULL,
    [field]                   NVARCHAR (4000) NULL,
    [old_value]               NVARCHAR (4000) NULL,
    [new_value]               NVARCHAR (4000) NULL,
    [modified_name]           NVARCHAR (60)   NULL,
    [created_date]            DATETIME        NULL,
    [updated_date]            DATETIME        NULL,
    [dv_load_date_time]       DATETIME        NOT NULL,
    [dv_r_load_source_id]     BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL,
    [dv_hash]                 CHAR (32)       NOT NULL,
    [dv_deleted]              BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

