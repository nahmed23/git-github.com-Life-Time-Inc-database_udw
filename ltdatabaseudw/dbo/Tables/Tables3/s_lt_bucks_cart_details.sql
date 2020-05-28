CREATE TABLE [dbo].[s_lt_bucks_cart_details] (
    [s_lt_bucks_cart_details_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [cdetail_id]                 INT          NULL,
    [cdetail_expiration_date]    DATETIME     NULL,
    [cdetail_delivery_date]      DATETIME     NULL,
    [cdetail_campaign_detail]    INT          NULL,
    [cdetail_reservation]        INT          NULL,
    [cdetail_assigned_member]    VARCHAR (9)  NULL,
    [last_modified_timestamp]    DATETIME     NULL,
    [cdetail_service_expired]    BIT          NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_lt_bucks_cart_details]
    ON [dbo].[s_lt_bucks_cart_details]([bk_hash] ASC, [s_lt_bucks_cart_details_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_lt_bucks_cart_details]([dv_batch_id] ASC);

