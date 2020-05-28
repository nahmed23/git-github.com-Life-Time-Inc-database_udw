CREATE TABLE [dbo].[stage_mms_PackageAdjustment] (
    [stage_mms_PackageAdjustment_id] BIGINT         NOT NULL,
    [PackageAdjustmentID]            INT            NULL,
    [PackageID]                      INT            NULL,
    [AdjustedDateTime]               DATETIME       NULL,
    [UTCAdjustedDateTime]            DATETIME       NULL,
    [AdjustedDateTimeZone]           VARCHAR (4)    NULL,
    [EmployeeID]                     INT            NULL,
    [MMSTranID]                      INT            NULL,
    [SessionsAdjusted]               SMALLINT       NULL,
    [AmountAdjusted]                 NUMERIC (9, 4) NULL,
    [Comment]                        VARCHAR (250)  NULL,
    [ValPackageAdjustmentTypeID]     SMALLINT       NULL,
    [InsertedDateTime]               DATETIME       NULL,
    [UpdatedDateTime]                DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

