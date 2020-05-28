CREATE TABLE [dbo].[stage_hybris_paymnttrnsctentries] (
    [stage_hybris_paymnttrnsctentries_id] BIGINT          NOT NULL,
    [hjmpTS]                              BIGINT          NULL,
    [createdTS]                           DATETIME        NULL,
    [modifiedTS]                          DATETIME        NULL,
    [TypePkString]                        BIGINT          NULL,
    [OwnerPkString]                       BIGINT          NULL,
    [PK]                                  BIGINT          NULL,
    [p_type]                              BIGINT          NULL,
    [p_amount]                            DECIMAL (30, 8) NULL,
    [p_currency]                          BIGINT          NULL,
    [p_time]                              DATETIME        NULL,
    [p_transactionstatus]                 NVARCHAR (255)  NULL,
    [p_transactionstatusdetails]          NVARCHAR (255)  NULL,
    [p_requesttoken]                      NVARCHAR (255)  NULL,
    [p_requestid]                         NVARCHAR (255)  NULL,
    [p_subscriptionid]                    NVARCHAR (255)  NULL,
    [p_code]                              NVARCHAR (255)  NULL,
    [p_versionid]                         NVARCHAR (255)  NULL,
    [p_paymenttransaction]                BIGINT          NULL,
    [aCLTS]                               BIGINT          NULL,
    [propTS]                              BIGINT          NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

