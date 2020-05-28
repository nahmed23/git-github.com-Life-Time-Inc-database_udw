CREATE TABLE [dbo].[stage_hash_ec_PlanItems] (
    [stage_hash_ec_PlanItems_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [PlanItemId]                 INT             NULL,
    [SourceId]                   NVARCHAR (50)   NULL,
    [ItemType]                   INT             NULL,
    [Date]                       DATETIME        NULL,
    [Name]                       NVARCHAR (4000) NULL,
    [Description]                NVARCHAR (4000) NULL,
    [Completed]                  BIT             NULL,
    [SourceType]                 INT             NULL,
    [PlanId]                     INT             NULL,
    [CreatedDate]                DATETIME        NULL,
    [UpdatedDate]                DATETIME        NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

