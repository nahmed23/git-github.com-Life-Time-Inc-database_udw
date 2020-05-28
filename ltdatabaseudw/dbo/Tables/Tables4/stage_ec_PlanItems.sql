CREATE TABLE [dbo].[stage_ec_PlanItems] (
    [stage_ec_PlanItems_id] BIGINT          NOT NULL,
    [PlanItemId]            INT             NULL,
    [SourceId]              NVARCHAR (50)   NULL,
    [ItemType]              INT             NULL,
    [Date]                  DATETIME        NULL,
    [Name]                  NVARCHAR (4000) NULL,
    [Description]           NVARCHAR (4000) NULL,
    [Completed]             BIT             NULL,
    [SourceType]            INT             NULL,
    [PlanId]                INT             NULL,
    [CreatedDate]           DATETIME        NULL,
    [UpdatedDate]           DATETIME        NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

