CREATE TABLE [dbo].[stage_hybris_oeperel] (
    [stage_hybris_oeperel_id] BIGINT         NOT NULL,
    [hjmpTS]                  BIGINT         NULL,
    [TypePkString]            BIGINT         NULL,
    [OwnerPkString]           BIGINT         NULL,
    [modifiedTS]              DATETIME       NULL,
    [createdTS]               DATETIME       NULL,
    [PK]                      BIGINT         NULL,
    [RSequenceNumber]         INT            NULL,
    [TargetPK]                BIGINT         NULL,
    [SequenceNumber]          INT            NULL,
    [SourcePK]                BIGINT         NULL,
    [Qualifier]               NVARCHAR (255) NULL,
    [languagepk]              BIGINT         NULL,
    [aCLTS]                   BIGINT         NULL,
    [propTS]                  BIGINT         NULL,
    [dv_batch_id]             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

