CREATE TABLE [dbo].[stage_hybris_cat2prodrel] (
    [stage_hybris_cat2prodrel_id] BIGINT         NOT NULL,
    [hjmpTS]                      BIGINT         NULL,
    [TypePkString]                BIGINT         NULL,
    [PK]                          BIGINT         NULL,
    [createdTS]                   DATETIME       NULL,
    [modifiedTS]                  DATETIME       NULL,
    [OwnerPkString]               BIGINT         NULL,
    [aCLTS]                       INT            NULL,
    [propTS]                      INT            NULL,
    [Qualifier]                   NVARCHAR (255) NULL,
    [SourcePK]                    BIGINT         NULL,
    [TargetPK]                    BIGINT         NULL,
    [RSequenceNumber]             INT            NULL,
    [SequenceNumber]              INT            NULL,
    [languagepk]                  BIGINT         NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

