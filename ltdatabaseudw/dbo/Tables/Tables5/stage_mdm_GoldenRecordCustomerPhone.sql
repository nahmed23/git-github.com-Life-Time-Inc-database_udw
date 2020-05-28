CREATE TABLE [dbo].[stage_mdm_GoldenRecordCustomerPhone] (
    [stage_mdm_GoldenRecordCustomerPhone_id] BIGINT         NOT NULL,
    [PhoneType]                              CHAR (40)      NULL,
    [Phone]                                  CHAR (40)      NULL,
    [LoadDateTime]                           DATETIME2 (7)  NULL,
    [EntityID]                               NVARCHAR (128) NULL,
    [InsertedDateTime]                       DATETIME2 (7)  NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

