CREATE TABLE [dbo].[stage_lt_bucks_CartDetails] (
    [stage_lt_bucks_CartDetails_id] BIGINT      NOT NULL,
    [cdetail_id]                    INT         NULL,
    [cdetail_cart]                  INT         NULL,
    [cdetail_poption]               INT         NULL,
    [cdetail_club]                  INT         NULL,
    [cdetail_expiration_date]       DATETIME    NULL,
    [cdetail_transactionkey]        INT         NULL,
    [cdetail_package]               INT         NULL,
    [cdetail_deliverydate]          DATETIME    NULL,
    [cdetail_assembly_cart]         INT         NULL,
    [cdetail_campaign_detail]       INT         NULL,
    [cdetail_qtyExpandCart]         INT         NULL,
    [cdetail_reservation]           INT         NULL,
    [cdetail_assigned_member]       VARCHAR (9) NULL,
    [LastModifiedTimestamp]         DATETIME    NULL,
    [cdetail_service_expired]       BIT         NULL,
    [dv_batch_id]                   BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

