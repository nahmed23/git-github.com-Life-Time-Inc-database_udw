CREATE TABLE [dbo].[s_hybris_promotion_lp] (
    [s_hybris_promotion_lp_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [item_pk]                        BIGINT          NULL,
    [lang_pk]                        BIGINT          NULL,
    [p_name]                         NVARCHAR (255)  NULL,
    [p_message_fired]                NVARCHAR (4000) NULL,
    [p_message_could_have_fired]     NVARCHAR (4000) NULL,
    [p_message_product_no_threshold] NVARCHAR (4000) NULL,
    [p_message_threshold_no_product] NVARCHAR (4000) NULL,
    [p_promotion_description]        VARCHAR (300)   NULL,
    [created_ts]                     DATETIME        NULL,
    [modified_ts]                    DATETIME        NULL,
    [dv_load_date_time]              DATETIME        NOT NULL,
    [dv_r_load_source_id]            BIGINT          NOT NULL,
    [dv_inserted_date_time]          DATETIME        NOT NULL,
    [dv_insert_user]                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL,
    [dv_hash]                        CHAR (32)       NOT NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_promotion_lp]
    ON [dbo].[s_hybris_promotion_lp]([bk_hash] ASC, [s_hybris_promotion_lp_id] ASC);

