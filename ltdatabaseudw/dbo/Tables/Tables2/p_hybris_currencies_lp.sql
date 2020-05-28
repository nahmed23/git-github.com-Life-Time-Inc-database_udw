CREATE TABLE [dbo].[p_hybris_currencies_lp] (
    [p_hybris_currencies_lp_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [item_pk]                              BIGINT       NULL,
    [lang_pk]                              BIGINT       NULL,
    [l_hybris_currencies_lp_id]            BIGINT       NULL,
    [s_hybris_currencies_lp_id]            BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_hybris_currencies_lp]
    ON [dbo].[p_hybris_currencies_lp]([bk_hash] ASC, [p_hybris_currencies_lp_id] ASC);

