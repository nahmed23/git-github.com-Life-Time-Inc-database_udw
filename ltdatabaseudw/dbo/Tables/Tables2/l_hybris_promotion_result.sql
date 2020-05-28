CREATE TABLE [dbo].[l_hybris_promotion_result] (
    [l_hybris_promotion_result_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [type_pk_string]               BIGINT       NULL,
    [owner_pk_string]              BIGINT       NULL,
    [promotion_result_pk]          BIGINT       NULL,
    [p_promotion]                  BIGINT       NULL,
    [p_order]                      BIGINT       NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_promotion_result]
    ON [dbo].[l_hybris_promotion_result]([bk_hash] ASC, [l_hybris_promotion_result_id] ASC);

