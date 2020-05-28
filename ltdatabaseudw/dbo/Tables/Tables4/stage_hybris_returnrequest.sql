CREATE TABLE [dbo].[stage_hybris_returnrequest] (
    [stage_hybris_returnrequest_id] BIGINT         NOT NULL,
    [hjmpTS]                        BIGINT         NULL,
    [createdTS]                     DATETIME       NULL,
    [modifiedTS]                    DATETIME       NULL,
    [TypePkString]                  BIGINT         NULL,
    [OwnerPkString]                 BIGINT         NULL,
    [PK]                            BIGINT         NULL,
    [p_code]                        NVARCHAR (255) NULL,
    [p_rma]                         NVARCHAR (255) NULL,
    [p_replacementorder]            BIGINT         NULL,
    [p_currency]                    BIGINT         NULL,
    [p_status]                      BIGINT         NULL,
    [p_returnlabel]                 BIGINT         NULL,
    [p_trackingid]                  NVARCHAR (255) NULL,
    [p_returnwarehouse]             BIGINT         NULL,
    [p_orderpos]                    INT            NULL,
    [p_order]                       BIGINT         NULL,
    [p_refunddeliverycost]          TINYINT        NULL,
    [aCLTS]                         BIGINT         NULL,
    [propTS]                        BIGINT         NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

