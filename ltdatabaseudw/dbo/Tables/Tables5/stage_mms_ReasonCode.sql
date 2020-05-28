CREATE TABLE [dbo].[stage_mms_ReasonCode] (
    [stage_mms_ReasonCode_id] BIGINT       NOT NULL,
    [ReasonCodeID]            INT          NULL,
    [Name]                    VARCHAR (15) NULL,
    [Description]             VARCHAR (50) NULL,
    [SortOrder]               INT          NULL,
    [DisplayUIFlag]           BIT          NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

