CREATE TABLE [dbo].[stage_hybris_currencies] (
    [stage_hybris_currencies_id] BIGINT          NOT NULL,
    [hjmpTS]                     BIGINT          NULL,
    [createdTS]                  DATETIME        NULL,
    [modifiedTS]                 DATETIME        NULL,
    [TypePkString]               BIGINT          NULL,
    [OwnerPkString]              BIGINT          NULL,
    [PK]                         BIGINT          NULL,
    [p_active]                   TINYINT         NULL,
    [p_isocode]                  NVARCHAR (255)  NULL,
    [p_base]                     TINYINT         NULL,
    [p_conversion]               DECIMAL (26, 6) NULL,
    [p_digits]                   INT             NULL,
    [p_symbol]                   NVARCHAR (255)  NULL,
    [aCLTS]                      BIGINT          NULL,
    [propTS]                     BIGINT          NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

