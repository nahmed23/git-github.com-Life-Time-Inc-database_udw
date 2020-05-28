CREATE TABLE [dbo].[stage_hybris_ltfrefundentry] (
    [stage_hybris_ltfrefundentry_id] BIGINT          NOT NULL,
    [hjmpTS]                         BIGINT          NULL,
    [TypePkString]                   BIGINT          NULL,
    [PK]                             BIGINT          NULL,
    [createdTS]                      DATETIME        NULL,
    [modifiedTS]                     DATETIME        NULL,
    [OwnerPkString]                  BIGINT          NULL,
    [aCLTS]                          INT             NULL,
    [propTS]                         INT             NULL,
    [p_refundeddate]                 DATETIME        NULL,
    [p_reason]                       BIGINT          NULL,
    [p_amount]                       DECIMAL (30, 8) NULL,
    [p_refundnote]                   NVARCHAR (4000) NULL,
    [p_refundstatus]                 BIGINT          NULL,
    [p_orderentries]                 BIGINT          NULL,
    [p_refundpaytype]                BIGINT          NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

