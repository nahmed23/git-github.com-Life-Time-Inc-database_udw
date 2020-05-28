CREATE TABLE [dbo].[stage_mms_ValTerminationReason] (
    [stage_mms_ValTerminationReason_id] BIGINT       NOT NULL,
    [ValTerminationReasonID]            INT          NULL,
    [Description]                       VARCHAR (50) NULL,
    [SortOrder]                         INT          NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [DisplayUIFlag]                     BIT          NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

