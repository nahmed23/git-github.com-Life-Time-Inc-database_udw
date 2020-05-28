CREATE TABLE [dbo].[s_nmo_hub_task] (
    [s_nmo_hub_task_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [id]                    INT             NULL,
    [title]                 NVARCHAR (255)  NULL,
    [description]           NVARCHAR (4000) NULL,
    [priority]              INT             NULL,
    [creator_name]          NVARCHAR (60)   NULL,
    [assignee_name]         NVARCHAR (60)   NULL,
    [due_date]              DATETIME        NULL,
    [resolution_date]       DATETIME        NULL,
    [created_date]          DATETIME        NULL,
    [updated_date]          DATETIME        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_deleted]            BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

