CREATE TABLE [dbo].[stage_mdm_GoldenRecordCustomerIDS] (
    [stage_mdm_GoldenRecordCustomerIDS_id] BIGINT         NOT NULL,
    [LoadDateTime]                         DATETIME2 (7)  NULL,
    [EntityID]                             NVARCHAR (128) NULL,
    [SourceCode]                           VARCHAR (128)  NULL,
    [SourceID]                             VARCHAR (128)  NULL,
    [InsertedDateTime]                     DATETIME2 (7)  NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

