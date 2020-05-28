CREATE TABLE [dbo].[stage_hybris_pgrels] (
    [stage_hybris_pgrels_id] BIGINT         NOT NULL,
    [hjmpTS]                 BIGINT         NULL,
    [createdTS]              DATETIME       NULL,
    [modifiedTS]             DATETIME       NULL,
    [TypePkString]           BIGINT         NULL,
    [OwnerPkString]          BIGINT         NULL,
    [PK]                     BIGINT         NULL,
    [languagepk]             BIGINT         NULL,
    [Qualifier]              NVARCHAR (255) NULL,
    [SourcePK]               BIGINT         NULL,
    [TargetPK]               BIGINT         NULL,
    [SequenceNumber]         INT            NULL,
    [RSequenceNumber]        INT            NULL,
    [aCLTS]                  BIGINT         NULL,
    [propTS]                 BIGINT         NULL,
    [dv_batch_id]            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

