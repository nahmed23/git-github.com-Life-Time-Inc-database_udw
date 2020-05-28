CREATE TABLE [dbo].[stage_mms_WebOrderMMSTran] (
    [stage_mms_WebOrderMMSTran_id] BIGINT   NOT NULL,
    [WebOrderMMSTranID]            INT      NULL,
    [WebOrderID]                   INT      NULL,
    [MMSTranID]                    INT      NULL,
    [InsertedDateTime]             DATETIME NULL,
    [UpdatedDateTime]              DATETIME NULL,
    [dv_batch_id]                  BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

