CREATE TABLE [dbo].[stage_hybris_paymenttransactions] (
    [stage_hybris_paymenttransactions_id] BIGINT          NOT NULL,
    [hjmpTS]                              BIGINT          NULL,
    [TypePkString]                        BIGINT          NULL,
    [PK]                                  BIGINT          NULL,
    [createdTS]                           DATETIME        NULL,
    [modifiedTS]                          DATETIME        NULL,
    [OwnerPkString]                       BIGINT          NULL,
    [aCLTS]                               INT             NULL,
    [propTS]                              INT             NULL,
    [p_versionid]                         NVARCHAR (255)  NULL,
    [p_code]                              NVARCHAR (255)  NULL,
    [p_currency]                          BIGINT          NULL,
    [p_requestid]                         NVARCHAR (255)  NULL,
    [p_order]                             BIGINT          NULL,
    [p_paymentprovider]                   NVARCHAR (255)  NULL,
    [p_requesttoken]                      NVARCHAR (255)  NULL,
    [p_info]                              BIGINT          NULL,
    [p_plannedamount]                     DECIMAL (30, 8) NULL,
    [p_autherrorcode]                     NVARCHAR (255)  NULL,
    [p_kountresponsecode]                 NVARCHAR (255)  NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

