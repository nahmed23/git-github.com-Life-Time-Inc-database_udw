CREATE TABLE [dbo].[stage_hybris_promotionlp] (
    [stage_hybris_promotionlp_id] BIGINT          NOT NULL,
    [ITEMPK]                      BIGINT          NULL,
    [ITEMTYPEPK]                  BIGINT          NULL,
    [LANGPK]                      BIGINT          NULL,
    [p_name]                      NVARCHAR (255)  NULL,
    [p_messagefired]              NVARCHAR (4000) NULL,
    [p_messagecouldhavefired]     NVARCHAR (4000) NULL,
    [p_messageproductnothreshold] NVARCHAR (4000) NULL,
    [p_messagethresholdnoproduct] NVARCHAR (4000) NULL,
    [p_promotiondescription]      VARCHAR (300)   NULL,
    [createdTS]                   DATETIME        NULL,
    [modifiedTS]                  DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

