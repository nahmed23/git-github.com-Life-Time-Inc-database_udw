CREATE TABLE [dbo].[stage_hybris_stocklevels] (
    [stage_hybris_stocklevels_id] BIGINT         NOT NULL,
    [hjmpTS]                      BIGINT         NULL,
    [TypePkString]                BIGINT         NULL,
    [PK]                          BIGINT         NULL,
    [createdTS]                   DATETIME       NULL,
    [modifiedTS]                  DATETIME       NULL,
    [OwnerPkString]               BIGINT         NULL,
    [aCLTS]                       INT            NULL,
    [propTS]                      INT            NULL,
    [p_preorder]                  INT            NULL,
    [p_treatnegativeaszero]       TINYINT        NULL,
    [p_overselling]               INT            NULL,
    [p_maxstocklevelhistorycount] INT            NULL,
    [p_instockstatus]             BIGINT         NULL,
    [p_available]                 INT            NULL,
    [p_productcode]               NVARCHAR (255) NULL,
    [p_reserved]                  INT            NULL,
    [p_warehouse]                 BIGINT         NULL,
    [p_maxpreorder]               INT            NULL,
    [p_releasedate]               DATETIME       NULL,
    [p_nextdeliverytime]          DATETIME       NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

