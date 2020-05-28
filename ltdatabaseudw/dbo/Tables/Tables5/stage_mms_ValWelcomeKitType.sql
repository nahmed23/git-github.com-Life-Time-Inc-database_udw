CREATE TABLE [dbo].[stage_mms_ValWelcomeKitType] (
    [stage_mms_ValWelcomeKitType_id] BIGINT       NOT NULL,
    [ValWelcomeKitTypeID]            INT          NULL,
    [Description]                    VARCHAR (50) NULL,
    [InsertedDateTime]               DATETIME     NULL,
    [UpdatedDateTime]                DATETIME     NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

