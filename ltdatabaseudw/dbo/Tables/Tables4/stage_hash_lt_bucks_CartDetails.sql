CREATE TABLE [dbo].[stage_hash_lt_bucks_CartDetails] (
    [stage_hash_lt_bucks_CartDetails_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)    NOT NULL,
    [cdetail_id]                         INT          NULL,
    [cdetail_cart]                       INT          NULL,
    [cdetail_poption]                    INT          NULL,
    [cdetail_club]                       INT          NULL,
    [cdetail_expiration_date]            DATETIME     NULL,
    [cdetail_transactionkey]             INT          NULL,
    [cdetail_package]                    INT          NULL,
    [cdetail_deliverydate]               DATETIME     NULL,
    [cdetail_assembly_cart]              INT          NULL,
    [cdetail_campaign_detail]            INT          NULL,
    [cdetail_qtyExpandCart]              INT          NULL,
    [cdetail_reservation]                INT          NULL,
    [cdetail_assigned_member]            VARCHAR (9)  NULL,
    [LastModifiedTimestamp]              DATETIME     NULL,
    [cdetail_service_expired]            BIT          NULL,
    [dv_load_date_time]                  DATETIME     NOT NULL,
    [dv_inserted_date_time]              DATETIME     NOT NULL,
    [dv_insert_user]                     VARCHAR (50) NOT NULL,
    [dv_updated_date_time]               DATETIME     NULL,
    [dv_update_user]                     VARCHAR (50) NULL,
    [dv_batch_id]                        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

