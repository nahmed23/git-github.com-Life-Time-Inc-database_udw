CREATE TABLE [dbo].[s_hybris_affiliate_entry_details] (
    [s_hybris_affiliate_entry_details_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [hjmpts]                              BIGINT       NULL,
    [affiliate_entry_details_pk]          BIGINT       NULL,
    [created_ts]                          DATETIME     NULL,
    [modified_ts]                         DATETIME     NULL,
    [acl_ts]                              INT          NULL,
    [prop_ts]                             INT          NULL,
    [ltf_affval_end_time]                 DATETIME     NULL,
    [ltf_purchase_flag]                   TINYINT      NULL,
    [ltf_aff_val_start_time]              DATETIME     NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_r_load_source_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_hash]                             CHAR (32)    NOT NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_affiliate_entry_details]
    ON [dbo].[s_hybris_affiliate_entry_details]([bk_hash] ASC, [s_hybris_affiliate_entry_details_id] ASC);

