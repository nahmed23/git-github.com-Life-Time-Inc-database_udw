CREATE TABLE [dbo].[d_mms_membership_product_tier_history] (
    [d_mms_membership_product_tier_history_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [dim_membership_bridge_dim_mms_product_tier_key] CHAR (32)    NULL,
    [membership_product_tier_id]                     INT          NULL,
    [effective_date_time]                            DATETIME     NULL,
    [expiration_date_time]                           DATETIME     NULL,
    [dim_mms_membership_key]                         CHAR (32)    NULL,
    [dim_mms_product_tier_key]                       CHAR (32)    NULL,
    [p_mms_membership_product_tier_id]               BIGINT       NOT NULL,
    [deleted_flag]                                   INT          NULL,
    [dv_load_date_time]                              DATETIME     NULL,
    [dv_load_end_date_time]                          DATETIME     NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL,
    [dv_inserted_date_time]                          DATETIME     NOT NULL,
    [dv_insert_user]                                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                           DATETIME     NULL,
    [dv_update_user]                                 VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

