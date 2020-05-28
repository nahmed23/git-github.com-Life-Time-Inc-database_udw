CREATE TABLE [dbo].[stage_hybris_promotionresult] (
    [stage_hybris_promotionresult_id] BIGINT          NOT NULL,
    [hjmpTS]                          BIGINT          NULL,
    [createdTS]                       DATETIME        NULL,
    [modifiedTS]                      DATETIME        NULL,
    [TypePkString]                    BIGINT          NULL,
    [OwnerPkString]                   BIGINT          NULL,
    [PK]                              BIGINT          NULL,
    [p_promotion]                     BIGINT          NULL,
    [p_certainty]                     DECIMAL (26, 6) NULL,
    [p_custom]                        NVARCHAR (255)  NULL,
    [p_order]                         BIGINT          NULL,
    [p_moduleversion]                 BIGINT          NULL,
    [p_ruleversion]                   BIGINT          NULL,
    [aCLTS]                           BIGINT          NULL,
    [propTS]                          BIGINT          NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

