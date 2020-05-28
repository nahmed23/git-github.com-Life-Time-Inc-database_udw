CREATE TABLE [dbo].[stage_mms_LTFResource] (
    [stage_mms_LTFResource_id] BIGINT       NOT NULL,
    [LTFResourceID]            INT          NULL,
    [Identifier]               VARCHAR (50) NULL,
    [Name]                     VARCHAR (50) NULL,
    [ValResourceTypeID]        INT          NULL,
    [InsertedDateTime]         DATETIME     NULL,
    [UpdatedDateTime]          DATETIME     NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

