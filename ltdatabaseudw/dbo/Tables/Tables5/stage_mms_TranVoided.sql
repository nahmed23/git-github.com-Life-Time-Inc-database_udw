CREATE TABLE [dbo].[stage_mms_TranVoided] (
    [stage_mms_TranVoided_id] BIGINT       NOT NULL,
    [TranVoidedID]            INT          NULL,
    [EmployeeID]              INT          NULL,
    [VoidDateTime]            DATETIME     NULL,
    [Comments]                VARCHAR (50) NULL,
    [UTCVoidDateTime]         DATETIME     NULL,
    [VoidDateTimeZone]        VARCHAR (4)  NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

