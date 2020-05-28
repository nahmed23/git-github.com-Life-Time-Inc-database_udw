CREATE TABLE [dbo].[l_hybris_promotion] (
    [l_hybris_promotion_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [type_pk_string]        BIGINT       NULL,
    [owner_pk_string]       BIGINT       NULL,
    [promotion_pk]          BIGINT       NULL,
    [p_promotion_group]     BIGINT       NULL,
    [p_product_banner]      BIGINT       NULL,
    [p_partner_product]     BIGINT       NULL,
    [p_discount_product]    BIGINT       NULL,
    [p_gift_product]        BIGINT       NULL,
    [p_free_voucher]        BIGINT       NULL,
    [p_delivery_mode]       BIGINT       NULL,
    [p_rule]                BIGINT       NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_promotion]
    ON [dbo].[l_hybris_promotion]([bk_hash] ASC, [l_hybris_promotion_id] ASC);

