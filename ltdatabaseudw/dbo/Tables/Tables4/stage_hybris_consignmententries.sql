CREATE TABLE [dbo].[stage_hybris_consignmententries] (
    [stage_hybris_consignmententries_id] BIGINT   NOT NULL,
    [hjmpTS]                             BIGINT   NULL,
    [TypePkString]                       BIGINT   NULL,
    [PK]                                 BIGINT   NULL,
    [createdTS]                          DATETIME NULL,
    [modifiedTS]                         DATETIME NULL,
    [OwnerPkString]                      BIGINT   NULL,
    [aCLTS]                              INT      NULL,
    [propTS]                             INT      NULL,
    [p_consignment]                      BIGINT   NULL,
    [p_quantity]                         BIGINT   NULL,
    [p_orderentry]                       BIGINT   NULL,
    [p_shippedquantity]                  BIGINT   NULL,
    [dv_batch_id]                        BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

