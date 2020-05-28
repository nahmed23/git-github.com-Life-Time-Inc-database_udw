CREATE TABLE [dbo].[stage_hybris_enumerationvalues] (
    [stage_hybris_enumerationvalues_id] BIGINT         NOT NULL,
    [hjmpTS]                            BIGINT         NULL,
    [createdTS]                         DATETIME       NULL,
    [modifiedTS]                        DATETIME       NULL,
    [TypePkString]                      BIGINT         NULL,
    [OwnerPkString]                     BIGINT         NULL,
    [PK]                                BIGINT         NULL,
    [Code]                              NVARCHAR (255) NULL,
    [codeLowerCase]                     NVARCHAR (255) NULL,
    [SequenceNumber]                    INT            NULL,
    [p_extensionname]                   NVARCHAR (255) NULL,
    [p_icon]                            BIGINT         NULL,
    [aCLTS]                             BIGINT         NULL,
    [propTS]                            BIGINT         NULL,
    [Editable]                          TINYINT        NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

