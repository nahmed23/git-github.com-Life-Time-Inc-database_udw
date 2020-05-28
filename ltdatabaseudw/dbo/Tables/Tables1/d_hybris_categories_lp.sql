CREATE TABLE [dbo].[d_hybris_categories_lp] (
    [d_hybris_categories_lp_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [d_hybris_categories_lp_key] CHAR (32)      NULL,
    [item_pk]                    BIGINT         NULL,
    [lang_pk]                    BIGINT         NULL,
    [item_type_pk]               BIGINT         NULL,
    [p_description]              VARCHAR (8000) NULL,
    [p_name]                     NVARCHAR (255) NULL,
    [p_hybris_categories_lp_id]  BIGINT         NOT NULL,
    [dv_load_date_time]          DATETIME       NULL,
    [dv_load_end_date_time]      DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

