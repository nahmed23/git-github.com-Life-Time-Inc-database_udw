CREATE TABLE [dbo].[stage_mdm_GoldenRecordCustomerLinkage] (
    [stage_mdm_GoldenRecordCustomerLinkage_id] BIGINT        NOT NULL,
    [LoadDateTime]                             DATETIME      NULL,
    [RowNumber]                                INT           NULL,
    [SourceCode]                               VARCHAR (128) NULL,
    [SourceID]                                 VARCHAR (128) NULL,
    [EventDateTime]                            DATETIME      NULL,
    [EventType]                                VARCHAR (128) NULL,
    [CurrentEntityID]                          BIGINT        NULL,
    [PreviousEntityID]                         BIGINT        NULL,
    [dv_batch_id]                              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

