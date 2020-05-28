CREATE TABLE [dbo].[stage_mms_ValCommissionable] (
    [stage_mms_ValCommissionable_id] BIGINT       NOT NULL,
    [ValCommissionableID]            INT          NULL,
    [Description]                    VARCHAR (50) NULL,
    [SortOrder]                      INT          NULL,
    [InsertedDateTime]               DATETIME     NULL,
    [UpdatedDateTime]                DATETIME     NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

