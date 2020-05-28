CREATE TABLE [dbo].[stage_hybris_consignments] (
    [stage_hybris_consignments_id] BIGINT         NOT NULL,
    [hjmpTS]                       BIGINT         NULL,
    [TypePkString]                 BIGINT         NULL,
    [PK]                           BIGINT         NULL,
    [createdTS]                    DATETIME       NULL,
    [modifiedTS]                   DATETIME       NULL,
    [OwnerPkString]                BIGINT         NULL,
    [aCLTS]                        INT            NULL,
    [propTS]                       INT            NULL,
    [p_trackingid]                 NVARCHAR (255) NULL,
    [p_status]                     BIGINT         NULL,
    [p_shippingdate]               DATETIME       NULL,
    [p_nameddeliverydate]          DATETIME       NULL,
    [p_code]                       NVARCHAR (255) NULL,
    [p_carrier]                    NVARCHAR (255) NULL,
    [p_warehouse]                  BIGINT         NULL,
    [p_shippingaddress]            BIGINT         NULL,
    [p_order]                      BIGINT         NULL,
    [p_deliverymode]               BIGINT         NULL,
    [p_deliverypointofservice]     BIGINT         NULL,
    [p_trackingmessage]            NVARCHAR (255) NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

