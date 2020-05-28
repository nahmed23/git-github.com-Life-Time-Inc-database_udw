CREATE TABLE [dbo].[stage_hybris_returnentry] (
    [stage_hybris_returnentry_id] BIGINT          NOT NULL,
    [hjmpTS]                      BIGINT          NULL,
    [createdTS]                   DATETIME        NULL,
    [modifiedTS]                  DATETIME        NULL,
    [TypePkString]                BIGINT          NULL,
    [OwnerPkString]               BIGINT          NULL,
    [PK]                          BIGINT          NULL,
    [p_orderentry]                BIGINT          NULL,
    [p_expectedquantity]          BIGINT          NULL,
    [p_receivedquantity]          BIGINT          NULL,
    [p_reacheddate]               DATETIME        NULL,
    [p_status]                    BIGINT          NULL,
    [p_action]                    BIGINT          NULL,
    [p_notes]                     NVARCHAR (255)  NULL,
    [p_returnrequestpos]          INT             NULL,
    [p_returnrequest]             BIGINT          NULL,
    [aCLTS]                       BIGINT          NULL,
    [propTS]                      BIGINT          NULL,
    [p_reason]                    BIGINT          NULL,
    [p_amount]                    DECIMAL (30, 8) NULL,
    [p_refundeddate]              DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

