CREATE TABLE [dbo].[stage_mms_ProductTier] (
    [stage_mms_ProductTier_id] BIGINT        NOT NULL,
    [ProductTierID]            INT           NULL,
    [Description]              VARCHAR (250) NULL,
    [DisplayText]              VARCHAR (50)  NULL,
    [ProductID]                INT           NULL,
    [ValProductTierTypeID]     INT           NULL,
    [SortOrder]                INT           NULL,
    [DisplayUIFlag]            BIT           NULL,
    [InsertedDateTime]         DATETIME      NULL,
    [UpdatedDateTime]          DATETIME      NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

