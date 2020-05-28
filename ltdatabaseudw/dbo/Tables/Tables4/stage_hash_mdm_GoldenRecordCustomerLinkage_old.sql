CREATE TABLE [dbo].[stage_hash_mdm_GoldenRecordCustomerLinkage_old] (
    [stage_hash_mdm_GoldenRecordCustomerLinkage_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)     NOT NULL,
    [LoadDateTime]                                  DATETIME      NULL,
    [RowNumber]                                     INT           NULL,
    [SourceCode]                                    VARCHAR (128) NULL,
    [SourceID]                                      VARCHAR (128) NULL,
    [EventDateTime]                                 DATETIME      NULL,
    [EventType]                                     VARCHAR (128) NULL,
    [CurrentEntityID]                               BIGINT        NULL,
    [PreviousEntityID]                              BIGINT        NULL,
    [dv_load_date_time]                             DATETIME      NOT NULL,
    [dv_inserted_date_time]                         DATETIME      NOT NULL,
    [dv_insert_user]                                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                          DATETIME      NULL,
    [dv_update_user]                                VARCHAR (50)  NULL,
    [dv_batch_id]                                   BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

