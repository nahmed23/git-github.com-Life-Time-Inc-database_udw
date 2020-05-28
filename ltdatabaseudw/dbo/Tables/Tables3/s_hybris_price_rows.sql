CREATE TABLE [dbo].[s_hybris_price_rows] (
    [s_hybris_price_rows_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [hjmpts]                 BIGINT          NULL,
    [created_ts]             DATETIME        NULL,
    [modified_ts]            DATETIME        NULL,
    [price_rows_pk]          BIGINT          NULL,
    [p_start_time]           DATETIME        NULL,
    [p_end_time]             DATETIME        NULL,
    [p_user_match_qualifier] BIGINT          NULL,
    [p_product_id]           NVARCHAR (255)  NULL,
    [p_match_value]          INT             NULL,
    [p_min_qtd]              BIGINT          NULL,
    [p_net]                  TINYINT         NULL,
    [p_price]                DECIMAL (26, 6) NULL,
    [p_unit_factor]          INT             NULL,
    [p_give_away_price]      TINYINT         NULL,
    [acl_ts]                 BIGINT          NULL,
    [prop_ts]                BIGINT          NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_price_rows]
    ON [dbo].[s_hybris_price_rows]([bk_hash] ASC, [s_hybris_price_rows_id] ASC);

