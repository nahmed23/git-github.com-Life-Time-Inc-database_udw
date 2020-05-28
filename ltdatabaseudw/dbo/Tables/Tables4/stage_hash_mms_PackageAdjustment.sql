CREATE TABLE [dbo].[stage_hash_mms_PackageAdjustment] (
    [stage_hash_mms_PackageAdjustment_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [PackageAdjustmentID]                 INT            NULL,
    [PackageID]                           INT            NULL,
    [AdjustedDateTime]                    DATETIME       NULL,
    [UTCAdjustedDateTime]                 DATETIME       NULL,
    [AdjustedDateTimeZone]                VARCHAR (4)    NULL,
    [EmployeeID]                          INT            NULL,
    [MMSTranID]                           INT            NULL,
    [SessionsAdjusted]                    SMALLINT       NULL,
    [AmountAdjusted]                      NUMERIC (9, 4) NULL,
    [Comment]                             VARCHAR (250)  NULL,
    [ValPackageAdjustmentTypeID]          SMALLINT       NULL,
    [InsertedDateTime]                    DATETIME       NULL,
    [UpdatedDateTime]                     DATETIME       NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

