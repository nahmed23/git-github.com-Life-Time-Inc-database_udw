CREATE TABLE [dbo].[d_nmo_hub_task_audit] (
    [d_nmo_hub_task_audit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [hub_task_audit_id]       INT             NULL,
    [created_date]            DATETIME        NULL,
    [created_dim_date_key]    VARCHAR (8)     NULL,
    [created_dim_time_key]    INT             NULL,
    [dim_nmo_hub_task_key]    VARCHAR (32)    NULL,
    [field]                   NVARCHAR (4000) NULL,
    [modified_name]           NVARCHAR (60)   NULL,
    [modified_party_id]       INT             NULL,
    [new_value]               NVARCHAR (4000) NULL,
    [old_value]               NVARCHAR (4000) NULL,
    [operation]               NVARCHAR (4000) NULL,
    [updated_date]            DATETIME        NULL,
    [updated_dim_date_key]    VARCHAR (8)     NULL,
    [updated_dim_time_key]    INT             NULL,
    [p_nmo_hub_task_audit_id] BIGINT          NOT NULL,
    [deleted_flag]            INT             NULL,
    [dv_load_date_time]       DATETIME        NULL,
    [dv_load_end_date_time]   DATETIME        NULL,
    [dv_batch_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

