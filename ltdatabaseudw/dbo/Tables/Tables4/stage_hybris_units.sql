CREATE TABLE [dbo].[stage_hybris_units] (
    [stage_hybris_units_id] BIGINT          NOT NULL,
    [hjmpTS]                BIGINT          NULL,
    [createdTS]             DATETIME        NULL,
    [modifiedTS]            DATETIME        NULL,
    [TypePkString]          BIGINT          NULL,
    [OwnerPkString]         BIGINT          NULL,
    [PK]                    BIGINT          NULL,
    [p_code]                NVARCHAR (255)  NULL,
    [p_conversion]          DECIMAL (26, 6) NULL,
    [p_unittype]            NVARCHAR (255)  NULL,
    [aCLTS]                 BIGINT          NULL,
    [propTS]                BIGINT          NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

