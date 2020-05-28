CREATE TABLE [dbo].[s_hybris_stock_levels] (
    [s_hybris_stock_levels_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [hjmpts]                          BIGINT       NULL,
    [stock_levels_pk]                 BIGINT       NULL,
    [created_ts]                      DATETIME     NULL,
    [modified_ts]                     DATETIME     NULL,
    [acl_ts]                          INT          NULL,
    [prop_ts]                         INT          NULL,
    [p_preorder]                      INT          NULL,
    [p_treat_negative_as_zero]        TINYINT      NULL,
    [p_over_selling]                  INT          NULL,
    [p_max_stock_level_history_count] INT          NULL,
    [p_available]                     INT          NULL,
    [p_reserved]                      INT          NULL,
    [p_warehouse]                     BIGINT       NULL,
    [p_max_pre_order]                 INT          NULL,
    [p_release_date]                  DATETIME     NULL,
    [p_next_delivery_time]            DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_r_load_source_id]             BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_hash]                         CHAR (32)    NOT NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_stock_levels]
    ON [dbo].[s_hybris_stock_levels]([bk_hash] ASC, [s_hybris_stock_levels_id] ASC);

