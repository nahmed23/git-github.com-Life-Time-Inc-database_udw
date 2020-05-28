CREATE TABLE [dbo].[stage_mdm_GoldenRecordCustomerEmail] (
    [stage_mdm_GoldenRecordCustomerEmail_id] BIGINT         NOT NULL,
    [EmailType]                              CHAR (40)      NULL,
    [Email]                                  CHAR (128)     NULL,
    [LoadDateTime]                           DATETIME2 (7)  NULL,
    [EntityID]                               NVARCHAR (128) NULL,
    [InsertedDateTime]                       DATETIME       NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

